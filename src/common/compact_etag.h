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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

class CompactEtagTest;

namespace OssFileSystem {

// CompactEtag is a 20-byte, format-adaptive value type for OSS ETags.
// ETag kinds are named after the string syntax they match, never after MD5
// semantics.
//
// Storage kinds:
//   kEmpty:      no ETag (HDFS, unfinished upload).
//   kNormal:     quoted 32-hex OSS ETag, decoded into 16 bytes.
//   kMultipart:  quoted 32-hex OSS ETag plus a decimal part count (0..10000).
//   kOther:      every other non-empty representation, stored verbatim in
//                one heap blob.
//
// OSS instructs clients to compare etags case-insensitively and returns
// uppercase hex etags
// (https://help.aliyun.com/zh/oss/developer-reference/compatibility-with-amazon-s3).
// Hex letters a-f/A-F compare case-insensitively in every representation; all
// other bytes compare exactly. Compressed hex values serialize in uppercase,
// while other values preserve their original bytes.
//
// There is intentionally NO implicit conversion to or from std::string /
// std::string_view: the compressed representation has no stable ASCII view,
// and hidden formatting/parsing would introduce allocations on hot paths.
// Raw text enters only via explicit CompactEtag(raw) / assign(raw), and text
// comes out only via explicit to_string()/append_to().
class CompactEtag {
 public:
  CompactEtag() noexcept;
  explicit CompactEtag(std::string_view raw);

  CompactEtag(const CompactEtag &other);
  CompactEtag(CompactEtag &&other) noexcept;
  CompactEtag &operator=(const CompactEtag &other);
  CompactEtag &operator=(CompactEtag &&other) noexcept;
  ~CompactEtag();

  // Parse and own raw etag text.
  // Strong exception guarantee: on allocation failure the current value is
  // left unchanged.
  void assign(std::string_view raw);
  void clear() noexcept;

  bool empty() const noexcept;
  // Length of the textual representation, i.e. to_string().size().
  size_t size() const noexcept;

  // Allocation-free and never throws; never builds a temporary CompactEtag.
  bool equals(const CompactEtag &other) const noexcept;
  // Compare against transient raw etag text (e.g. a GET response etag that
  // is only checked once and never stored).
  bool equals(std::string_view raw) const noexcept;

  void append_to(std::string *output) const;
  std::string to_string() const;

  static constexpr size_t kHex32Len = 32;
  static constexpr size_t kMaxPartCountDigits = 5;
  // OSS allows at most 10000 multipart parts.
  static constexpr uint16_t kMaxPartCount = 10000;

 private:
  enum class Kind : uint8_t {
    kEmpty = 0,
    kNormal,
    kMultipart,
    kOther,
  };

  // Heap blob for kOther: a single allocation carrying both the size
  // header and the original ETag bytes.
  struct HeapValue {
    size_t size;
    char data[0];
  };

  // Storage layout (20 bytes, 4-byte aligned):
  //   byte 0..15:  decoded normal/multipart hex / HeapValue*
  //   byte 16..17: multipart part count
  //   byte 18:     reserved
  //   byte 19:     Kind
  static constexpr size_t kStorageSize = 20;
  static constexpr size_t kDataOff = 0;
  static constexpr size_t kPartCountOff = 16;
  static constexpr size_t kKindOff = 19;

  static_assert(kDataOff + kHex32Len / 2 <= kPartCountOff,
                "decoded hex must not overlap the part count");
  static_assert(kDataOff + sizeof(HeapValue *) <= kPartCountOff,
                "heap pointer must not overlap the part count");
  static_assert(kPartCountOff + sizeof(uint16_t) <= kKindOff,
                "part count must not overlap the kind byte");
  static_assert(kKindOff < kStorageSize,
                "kind byte must fit in CompactEtag storage");

  Kind kind() const noexcept {
    return static_cast<Kind>(static_cast<uint8_t>(storage_[kKindOff]));
  }

  // Multi-byte fields are accessed through memcpy to avoid unaligned access
  // and strict-aliasing issues.
  uint16_t part_count() const noexcept {
    uint16_t value;
    memcpy(&value, storage_.data() + kPartCountOff, sizeof(value));
    return value;
  }
  HeapValue *heap_value() const noexcept {
    HeapValue *value;
    memcpy(&value, storage_.data() + kDataOff, sizeof(value));
    return value;
  }

  void release_heap() noexcept;

  // buf must hold kHex32Len + 1 + kMaxPartCountDigits bytes.
  size_t encode_hex_to(char *buf) const noexcept;

  alignas(uint32_t) std::array<std::byte, kStorageSize> storage_{};

  friend class ::CompactEtagTest;
};

static_assert(sizeof(CompactEtag) == 20, "CompactEtag must stay 20 bytes");
static_assert(alignof(CompactEtag) == alignof(uint32_t),
              "CompactEtag alignment must match uint32_t");
static_assert(std::is_nothrow_move_constructible<CompactEtag>::value,
              "CompactEtag move must be noexcept");
static_assert(std::is_nothrow_move_assignable<CompactEtag>::value,
              "CompactEtag move assignment must be noexcept");

}  // namespace OssFileSystem
