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

#include <gtest/gtest.h>

#include <numeric>
#include <string>

#include "common/iov_utils.h"

namespace {

// Filled with increasing bytes so an offset error shows up as wrong content.
class ScatteredBuffer {
 public:
  explicit ScatteredBuffer(const std::vector<size_t> &lens) {
    size_t total = std::accumulate(lens.begin(), lens.end(), size_t{0});
    storage_.resize(total);
    for (size_t i = 0; i < total; i++) {
      storage_[i] = static_cast<char>(i & 0xff);
    }
    size_t off = 0;
    for (size_t len : lens) {
      segs_.push_back({storage_.data() + off, len});
      off += len;
    }
  }

  const std::vector<iovec> &segs() const {
    return segs_;
  }

  char at(size_t off) const {
    return storage_[off];
  }

 private:
  std::string storage_;
  std::vector<iovec> segs_;
};

size_t view_bytes(const std::vector<iovec> &view) {
  size_t total = 0;
  for (const auto &seg : view) total += seg.iov_len;
  return total;
}

std::string view_content(const std::vector<iovec> &view) {
  std::string out;
  for (const auto &seg : view) {
    out.append(static_cast<const char *>(seg.iov_base), seg.iov_len);
  }
  return out;
}

std::string expected_content(const ScatteredBuffer &buf, size_t off,
                             size_t len) {
  std::string out;
  for (size_t i = 0; i < len; i++) out.push_back(buf.at(off + i));
  return out;
}

}  // namespace

TEST(IovUtilsTest, build_view_covers_whole_uniform_buffer) {
  ScatteredBuffer buf({4, 4, 4});

  auto view = build_iov_view(buf.segs(), 0, 12);
  ASSERT_EQ(view.size(), 3U);
  ASSERT_EQ(view_bytes(view), 12U);
  ASSERT_EQ(view_content(view), expected_content(buf, 0, 12));
}

TEST(IovUtilsTest, build_view_handles_partial_last_segment) {
  ScatteredBuffer buf({4, 2});

  auto full = build_iov_view(buf.segs(), 0, 6);
  ASSERT_EQ(full.size(), 2U);
  ASSERT_EQ(full[1].iov_len, 2U);
  ASSERT_EQ(view_content(full), expected_content(buf, 0, 6));

  auto tail = build_iov_view(buf.segs(), 5, 1);
  ASSERT_EQ(tail.size(), 1U);
  ASSERT_EQ(view_content(tail), expected_content(buf, 5, 1));
}

TEST(IovUtilsTest, build_view_starts_inside_a_segment) {
  ScatteredBuffer buf({4, 4, 4});

  auto view = build_iov_view(buf.segs(), 2, 9);
  ASSERT_EQ(view.size(), 3U);
  ASSERT_EQ(view[0].iov_len, 2U);
  ASSERT_EQ(view[1].iov_len, 4U);
  ASSERT_EQ(view[2].iov_len, 3U);
  ASSERT_EQ(view_content(view), expected_content(buf, 2, 9));
}

TEST(IovUtilsTest, build_view_on_segment_boundaries) {
  ScatteredBuffer buf({4, 4, 4});

  for (size_t off : {size_t{0}, size_t{4}, size_t{8}}) {
    auto view = build_iov_view(buf.segs(), off, 4);
    ASSERT_EQ(view.size(), 1U) << "offset " << off;
    ASSERT_EQ(view_bytes(view), 4U) << "offset " << off;
    ASSERT_EQ(view_content(view), expected_content(buf, off, 4))
        << "offset " << off;
  }
}

TEST(IovUtilsTest, build_view_within_one_segment) {
  ScatteredBuffer buf({8, 8});

  auto view = build_iov_view(buf.segs(), 3, 2);
  ASSERT_EQ(view.size(), 1U);
  ASSERT_EQ(view_content(view), expected_content(buf, 3, 2));
}

TEST(IovUtilsTest, build_view_zero_length_keeps_request_shape) {
  ScatteredBuffer buf({4, 4});

  auto view = build_iov_view(buf.segs(), 0, 0);
  ASSERT_EQ(view.size(), 1U);
  ASSERT_EQ(view[0].iov_len, 0U);
  ASSERT_EQ(view[0].iov_base, buf.segs().front().iov_base);

  ASSERT_TRUE(build_iov_view({}, 0, 0).empty());
}

TEST(IovUtilsTest, memcpy_to_and_from_iov_round_trip) {
  ScatteredBuffer buf({4, 2, 4});
  auto view = build_iov_view(buf.segs(), 1, 8);

  std::string src(8, '\0');
  for (size_t i = 0; i < src.size(); i++) src[i] = static_cast<char>('a' + i);
  memcpy_to_iov(view.data(), iovcnt(view), src.data(), src.size());
  ASSERT_EQ(view_content(view), src);

  std::string dst(8, '\0');
  memcpy_from_iov(dst.data(), view.data(), iovcnt(view), dst.size());
  ASSERT_EQ(dst, src);
}

TEST(IovUtilsTest, crc64_over_view_matches_contiguous) {
  ScatteredBuffer buf({4, 2, 4});
  auto view = build_iov_view(buf.segs(), 0, 10);

  std::string flat = expected_content(buf, 0, 10);
  ASSERT_EQ(crc64ecma_iov(view, 10, 0), crc64ecma(flat.data(), 10, 0));

  ASSERT_EQ(crc64ecma_iov(view, 5, 0), crc64ecma(flat.data(), 5, 0));
  ASSERT_EQ(crc64ecma_iov(view, 0, 12345ULL), 12345ULL);
}
