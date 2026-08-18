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

#include <stddef.h>
#include <string.h>
#include <unistd.h>

#include "common/fuse.h"
#include "common/macros.h"

inline size_t fuse_bufv_size(const struct fuse_bufvec *bufv) {
  size_t size = 0;
  for (size_t i = 0; i < bufv->count; i++) {
    size += bufv->buf[i].size;
  }
  return size;
}

inline const struct fuse_buf *fuse_bufvec_current(struct fuse_bufvec *bufv) {
  if (bufv->idx < bufv->count)
    return &bufv->buf[bufv->idx];
  else
    return NULL;
}

inline int fuse_bufvec_advance(struct fuse_bufvec *bufv, size_t len) {
  const struct fuse_buf *buf = fuse_bufvec_current(bufv);

  if (!buf) return 0;

  bufv->off += len;
  RELEASE_ASSERT(bufv->off <= buf->size);
  if (bufv->off == buf->size) {
    RELEASE_ASSERT(bufv->idx < bufv->count);
    bufv->idx++;
    if (bufv->idx == bufv->count) return 0;
    bufv->off = 0;
  }

  return 1;
}

// Copy data from a FD-backed fuse_buf to dst.
inline ssize_t fuse_buf_to_buf_copy(char *dst, const struct fuse_buf *src,
                                    size_t src_off, size_t len) {
  ssize_t res = 0;
  size_t copied = 0;

  while (copied < len) {
    if (src->flags & FUSE_BUF_FD_SEEK) {
      res = pread(src->fd, dst + copied, len, src->pos + src_off);
    } else {
      res = read(src->fd, dst + copied, len);
    }
    if (res == -1) {
      if (!copied) return -errno;
      break;
    }
    if (res == 0) break;

    copied += res;
    if (!(src->flags & FUSE_BUF_FD_RETRY)) break;

    src_off += res;
    len -= res;
  }

  return copied;
}

// Copy data from a fuse_bufvec to dst.
inline ssize_t fuse_bufvec_to_buf_copy(char *dst, struct fuse_bufvec *srcv,
                                       size_t len) {
  size_t copied = 0;

  while (copied < len) {
    const struct fuse_buf *src = fuse_bufvec_current(srcv);

    size_t src_len = src->size - srcv->off;
    size_t copy_len = std::min(src_len, len - copied);
    ssize_t res = 0;

    if (src->flags & FUSE_BUF_IS_FD) {
      res = fuse_buf_to_buf_copy(dst + copied, src, srcv->off, copy_len);
      if (res < 0) {
        if (!copied) return res;
        break;
      }
    } else {
      memcpy(dst + copied, (char *)src->mem + srcv->off, copy_len);
      res = copy_len;
    }

    copied += res;
    if (!fuse_bufvec_advance(srcv, res)) break;
  }

  return copied;
}

inline ssize_t fuse_read_bufvec_full(char *dst, struct fuse_bufvec *srcv,
                                     size_t len) {
  size_t total = 0;
  while (total < len) {
    ssize_t n = fuse_bufvec_to_buf_copy(dst + total, srcv, len - total);
    if (n < 0) return n;
    if (n == 0) return -EIO;
    total += n;
  }
  return static_cast<ssize_t>(total);
}
