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

#include <photon/common/iovector.h>
#include <photon/common/range-lock.h>
#include <sys/uio.h>

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <utility>

class BlockCacheTest;

namespace OssFileSystem {

struct RangeBuffer {
  off_t offset;
  size_t count;
  IOVector buffer;
};

class ICacheStore {
 public:
  virtual ~ICacheStore() = default;

  virtual ssize_t pread(char *buf, off_t offset, size_t count) = 0;
  virtual ssize_t pin(off_t offset, size_t count, void **buf) = 0;
  virtual void unpin(off_t offset) = 0;
  virtual std::pair<off_t, size_t> query_refill_range(off_t offset,
                                                      size_t count) = 0;
  virtual void drop() = 0;

  // Acquires a writable cache buffer for range [offset, offset + count).
  // On success, populates 'buffer' that the caller can fill with data.
  virtual int acquire_write_buffer(RangeBuffer &range_buffer) = 0;
  // Releases the resources associated with the previously prepared buffer.
  // If 'evict' is true, any staged data is discarded and not retained in the
  // cache.
  virtual void release_write_buffer(const RangeBuffer &range_buffer,
                                    bool evict = false) = 0;
};

struct CacheHandle;
class ICache {
 public:
  virtual ~ICache() = default;

  virtual size_t block_size() const = 0;
  virtual CacheHandle *get(std::string_view name, std::string_view etag,
                           off_t actual_size = 0) = 0;
  virtual size_t capacity() = 0;

  virtual size_t try_expand_blocks(uint64_t count, uint64_t max_capacity,
                                   bool ignore_limit) = 0;
  virtual void release(CacheHandle *h, uint64_t count) = 0;
};

struct CacheHandle {
 public:
  CacheHandle(ICacheStore *s, RangeLock *l) : cache_store(s), range_lock(l) {}
  virtual ~CacheHandle() = default;

  RangeLock *get_range_lock() {
    return range_lock;
  }

  ssize_t pread(char *buf, off_t offset, size_t count) {
    return cache_store->pread(buf, offset, count);
  }

  ssize_t pin(off_t offset, size_t count, void **buf) {
    return cache_store->pin(offset, count, buf);
  }

  void unpin(off_t offset) {
    cache_store->unpin(offset);
  }

  std::pair<off_t, size_t> query_refill_range(off_t offset, size_t count) {
    return cache_store->query_refill_range(offset, count);
  }

  void drop() {
    cache_store->drop();
  }

  int acquire_write_buffer(RangeBuffer &range_buffer) {
    return cache_store->acquire_write_buffer(range_buffer);
  }

  void release_write_buffer(const RangeBuffer &range_buffer,
                            bool evict = false) {
    cache_store->release_write_buffer(range_buffer, evict);
  }

  virtual void on_read_success(off_t offset, size_t prefetch_buffer_size) {}

 protected:
  ICacheStore *cache_store = nullptr;
  RangeLock *range_lock = nullptr;

  friend class ::BlockCacheTest;
};

}  // namespace OssFileSystem
