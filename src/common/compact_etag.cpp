/*
 * Copyright 2025 The Ossfs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "compact_etag.h"

#include <charconv>
#include <new>

#include "common/logger.h"

namespace OssFileSystem {

namespace {

// Returns 0..15 for a hex character, -1 otherwise.
int hex_digit_value(char c) noexcept {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

// Decodes exactly 32 hex chars into 16 bytes. Both cases are accepted.
bool decode_hex32(std::string_view hex, std::byte *out) noexcept {
  if (hex.size() != CompactEtag::kHex32Len) return false;
  for (size_t i = 0; i < CompactEtag::kHex32Len / 2; ++i) {
    int hi = hex_digit_value(hex[i * 2]);
    int lo = hex_digit_value(hex[i * 2 + 1]);
    if (hi < 0 || lo < 0) return false;
    out[i] = static_cast<std::byte>((hi << 4) | lo);
  }
  return true;
}

// Canonical textual form of the hex kinds uses uppercase hex (OSS semantics).
char encode_hex_digit(uint8_t v) noexcept {
  if (v < 10) return static_cast<char>('0' + v);
  return static_cast<char>('A' + (v - 10));
}

// Re-encodes 16 bytes as 32 uppercase hex chars.
void encode_hex32(const std::byte *in, char *out) noexcept {
  for (size_t i = 0; i < CompactEtag::kHex32Len / 2; ++i) {
    uint8_t byte = static_cast<uint8_t>(in[i]);
    out[i * 2] = encode_hex_digit(byte >> 4);
    out[i * 2 + 1] = encode_hex_digit(byte & 0x0f);
  }
}

// Fold only hexadecimal letters; every other byte compares exactly.
bool equal_hex_case_fold(const char *a, const char *b, size_t n) noexcept {
  for (size_t i = 0; i < n; i++) {
    char x = a[i];
    char y = b[i];
    if (x >= 'a' && x <= 'f') x = static_cast<char>(x - 'a' + 'A');
    if (y >= 'a' && y <= 'f') y = static_cast<char>(y - 'a' + 'A');
    if (x != y) return false;
  }
  return true;
}

int decimal_digits(uint16_t v) noexcept {
  int digits = 1;
  while (v >= 10) {
    v /= 10;
    digits++;
  }
  return digits;
}

// Parses a canonical multipart part count (1..5 digits, no leading zeros,
// value in [0, kMaxPartCount]); anything else falls back to kOther.
bool parse_part_count(std::string_view digits, uint16_t *out) noexcept {
  if (digits.empty() || digits.size() > CompactEtag::kMaxPartCountDigits) {
    return false;
  }
  if (digits.front() == '0' && digits.size() > 1) return false;
  uint32_t value = 0;
  for (char c : digits) {
    if (c < '0' || c > '9') return false;
    value = value * 10 + static_cast<uint32_t>(c - '0');
  }
  if (value > CompactEtag::kMaxPartCount) return false;
  *out = static_cast<uint16_t>(value);
  return true;
}

}  // namespace

CompactEtag::CompactEtag() noexcept = default;

CompactEtag::CompactEtag(std::string_view raw) {
  assign(raw);
}

CompactEtag::CompactEtag(const CompactEtag &other) {
  *this = other;
}

CompactEtag::CompactEtag(CompactEtag &&other) noexcept
    : storage_(other.storage_) {
  other.storage_.fill(std::byte{0});
}

CompactEtag &CompactEtag::operator=(const CompactEtag &other) {
  if (this == &other) return *this;
  if (other.kind() == Kind::kOther) {
    // Allocate the new blob before releasing the old one, so a failed
    // allocation leaves this object unchanged.
    const HeapValue *src = other.heap_value();
    void *mem = ::operator new(sizeof(HeapValue) + src->size);
    auto *blob = new (mem) HeapValue{src->size};
    memcpy(blob->data, src->data, src->size);
    release_heap();
    storage_.fill(std::byte{0});
    memcpy(storage_.data() + kDataOff, &blob, sizeof(blob));
    storage_[kKindOff] = static_cast<std::byte>(Kind::kOther);
  } else {
    release_heap();
    storage_ = other.storage_;
  }
  return *this;
}

CompactEtag &CompactEtag::operator=(CompactEtag &&other) noexcept {
  if (this == &other) return *this;
  release_heap();
  storage_ = other.storage_;
  other.storage_.fill(std::byte{0});
  return *this;
}

CompactEtag::~CompactEtag() {
  release_heap();
}

void CompactEtag::assign(std::string_view raw) {
  if (raw.empty()) {
    clear();
    return;
  }

  // Strong exception guarantee: build the next state (incl. any heap blob)
  // first, then release the old value and commit.
  std::array<std::byte, kStorageSize> next{};
  Kind next_kind = Kind::kOther;

  if (raw.size() >= 2 && raw.front() == '"' && raw.back() == '"') {
    std::string_view inner = raw.substr(1, raw.size() - 2);
    uint16_t part_count_value = 0;
    if (inner.size() == kHex32Len &&
        decode_hex32(inner, next.data() + kDataOff)) {
      next_kind = Kind::kNormal;
    } else if (inner.size() > kHex32Len + 1 && inner[kHex32Len] == '-' &&
               parse_part_count(inner.substr(kHex32Len + 1),
                                &part_count_value) &&
               decode_hex32(inner.substr(0, kHex32Len),
                            next.data() + kDataOff)) {
      memcpy(next.data() + kPartCountOff, &part_count_value,
             sizeof(part_count_value));
      next_kind = Kind::kMultipart;
    }
  }

  if (next_kind == Kind::kOther) {
    LOG_WARN("Nonstandard OSS ETag, length `, etag `", raw.size(), raw);
    void *mem = ::operator new(sizeof(HeapValue) + raw.size());
    auto *blob = new (mem) HeapValue{raw.size()};
    memcpy(blob->data, raw.data(), raw.size());
    next.fill(std::byte{0});
    memcpy(next.data() + kDataOff, &blob, sizeof(blob));
  }
  next[kKindOff] = static_cast<std::byte>(next_kind);

  release_heap();
  storage_ = next;
}

void CompactEtag::clear() noexcept {
  release_heap();
  storage_.fill(std::byte{0});
}

bool CompactEtag::empty() const noexcept {
  return kind() == Kind::kEmpty;
}

size_t CompactEtag::size() const noexcept {
  switch (kind()) {
    case Kind::kEmpty:
      return 0;
    case Kind::kNormal:
      return kHex32Len + 2;
    case Kind::kMultipart:
      return kHex32Len + 1 + static_cast<size_t>(decimal_digits(part_count())) +
             2;
    case Kind::kOther:
      return heap_value()->size;
  }
  return 0;
}

bool CompactEtag::equals(const CompactEtag &other) const noexcept {
  Kind k = kind();
  if (k != other.kind()) return false;
  if (k == Kind::kOther) {
    const HeapValue *a = heap_value();
    const HeapValue *b = other.heap_value();
    return a->size == b->size && equal_hex_case_fold(a->data, b->data, a->size);
  }
  // Inline kinds are built on zeroed storage, so a full-range compare is
  // exact (unused bytes are always zero).
  return storage_ == other.storage_;
}

bool CompactEtag::equals(std::string_view raw) const noexcept {
  if (raw.size() != size()) return false;
  switch (kind()) {
    case Kind::kEmpty:
      // size() == 0, so raw is empty as well.
      return true;
    case Kind::kOther:
      return equal_hex_case_fold(heap_value()->data, raw.data(), raw.size());
    case Kind::kNormal:
    case Kind::kMultipart: {
      if (raw.front() != '"' || raw.back() != '"') return false;
      raw = raw.substr(1, raw.size() - 2);
      char encoded[kHex32Len + 1 + kMaxPartCountDigits];
      size_t len = encode_hex_to(encoded);
      return equal_hex_case_fold(encoded, raw.data(), len);
    }
  }
  return false;
}

size_t CompactEtag::encode_hex_to(char *buf) const noexcept {
  char *p = buf;
  encode_hex32(storage_.data() + kDataOff, p);
  p += kHex32Len;
  if (kind() == Kind::kMultipart) {
    *p++ = '-';
    p = std::to_chars(p, p + kMaxPartCountDigits, part_count()).ptr;
  }
  return static_cast<size_t>(p - buf);
}

void CompactEtag::append_to(std::string *output) const {
  switch (kind()) {
    case Kind::kEmpty:
      return;
    case Kind::kNormal:
    case Kind::kMultipart: {
      char encoded[kHex32Len + 1 + kMaxPartCountDigits];
      size_t len = encode_hex_to(encoded);
      output->push_back('"');
      output->append(encoded, len);
      output->push_back('"');
      return;
    }
    case Kind::kOther: {
      const HeapValue *value = heap_value();
      output->append(value->data, value->size);
      return;
    }
  }
}

std::string CompactEtag::to_string() const {
  std::string result;
  result.reserve(size());
  append_to(&result);
  return result;
}

void CompactEtag::release_heap() noexcept {
  if (kind() == Kind::kOther) {
    ::operator delete(heap_value());
  }
}

}  // namespace OssFileSystem
