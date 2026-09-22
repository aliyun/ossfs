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

// Arithmetic over a scatter list of buffers whose segments are logically
// contiguous: byte N of the buffer is the N-th byte of the concatenated
// segments.

#pragma once

#include <photon/common/checksum/crc64ecma.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/uio.h>

#include <algorithm>
#include <vector>

#include "common/macros.h"

// A view over [offset, offset + len). len == 0 yields a single zero-length
// iovec.
inline std::vector<iovec> build_iov_view(const std::vector<iovec> &buf,
                                         size_t offset, size_t len) {
  std::vector<iovec> view;
  if (len == 0) {
    if (!buf.empty()) view.push_back({buf.front().iov_base, 0});
    return view;
  }

  size_t remain = len;
  size_t seg_begin = 0;
  for (const auto &seg : buf) {
    if (remain == 0) break;
    size_t seg_end = seg_begin + seg.iov_len;
    if (offset < seg_end) {
      size_t skip = offset > seg_begin ? offset - seg_begin : 0;
      size_t take = std::min(seg.iov_len - skip, remain);
      view.push_back({static_cast<char *>(seg.iov_base) + skip, take});
      offset += take;
      remain -= take;
    }
    seg_begin = seg_end;
  }
  RELEASE_ASSERT(remain == 0);
  return view;
}

inline int iovcnt(const std::vector<iovec> &view) {
  return static_cast<int>(view.size());
}

// 'len' may be shorter than the view.
inline uint64_t crc64ecma_iov(const std::vector<iovec> &view, size_t len,
                              uint64_t crc) {
  for (const auto &seg : view) {
    if (len == 0) break;
    size_t n = std::min(seg.iov_len, len);
    crc = crc64ecma(seg.iov_base, n, crc);
    len -= n;
  }
  return crc;
}

inline void memcpy_to_iov(const struct iovec *iov, int iovcnt, const void *src,
                          size_t len) {
  size_t copied = 0;
  for (int i = 0; i < iovcnt && copied < len; i++) {
    size_t to_copy = std::min(iov[i].iov_len, len - copied);
    memcpy(iov[i].iov_base, static_cast<const char *>(src) + copied, to_copy);
    copied += to_copy;
  }
  RELEASE_ASSERT(copied == len);
}

inline void memcpy_from_iov(void *dst, const struct iovec *iov, int iovcnt,
                            size_t len) {
  size_t copied = 0;
  for (int i = 0; i < iovcnt && copied < len; i++) {
    size_t to_copy = std::min(iov[i].iov_len, len - copied);
    memcpy(static_cast<char *>(dst) + copied, iov[i].iov_base, to_copy);
    copied += to_copy;
  }
  RELEASE_ASSERT(copied == len);
}
