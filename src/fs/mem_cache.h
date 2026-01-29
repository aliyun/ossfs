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

#include <photon/common/range-lock.h>
#include <photon/thread/thread.h>
#include <sys/uio.h>

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/logger.h"

namespace OssFileSystem {

// SpinRWLock does not implement read-write priority. Currently, in the external
// cache filling implementation, there is another RangeLock protecting the block
// to be filled. The RangeLock includes logic for queuing and waking up, so the
// write lock here will not be starved.
class SpinRWLock {
 public:
  bool try_read_lock() {
    int expected;
    do {
      expected = lock_cnt.load(std::memory_order_acquire);
      if (expected < 0) {
        return false;
      }
    } while (!lock_cnt.compare_exchange_weak(expected, expected + 1,
                                             std::memory_order_acquire,
                                             std::memory_order_relaxed));
    return true;
  }

  void unlock_read() {
    lock_cnt.fetch_sub(1, std::memory_order_release);
  }

  bool try_write_lock() {
    int expected = 0;
    return lock_cnt.compare_exchange_strong(expected, -1,
                                            std::memory_order_acquire);
  }

  void unlock_write() {
    lock_cnt.store(0, std::memory_order_release);
  }

  bool is_locked() {
    return lock_cnt.load(std::memory_order_acquire) != 0;
  }

 private:
  std::atomic<int> lock_cnt = {0};
};

enum BlockLockType { READ = 0, WRITE = 1 };

struct BlockInfo {
  char *mem = nullptr;

  SpinRWLock lock;

  uint32_t valid_off = 0;
  uint32_t valid_size = 0;

  uint64_t generation = 0;
};

class BlockCache {
 public:
  BlockCache(uint64_t block_size = 1048576) : block_size_(block_size) {
    meta_store_ = std::make_unique<MetaStore>(this, block_size);
  }

  ssize_t pread(char *buf, off_t offset, size_t count);

  // Try to allocate blocks for range [offset, offset + count) and
  // lock it if successful.
  int try_lock_blocks(uint64_t offset, uint64_t count,
                      std::vector<iovec> &blocks);

  // Unlock the block. If evict is true, the block will be evicted.
  // Evict flag is used for the case that writing block data failed
  // and we rollback metadata for those blocks.
  void unlock_blocks(uint64_t offset, uint64_t count, bool evict = false);

  // The pin operation returns a buffer pointer pointing to the
  // corresponding offset and increments the reference count to
  // prevent modification when success. If cache miss or the range
  // crosses the block boundary, -ENOENT will be returned.
  ssize_t pin(off_t offset, size_t count, void **buf);

  // Release the block and decrease the reference count.
  void unpin(off_t offset);

  // Only collect memory blocks pointer which should be managed outside.
  void expand_blocks(const std::vector<char *> &blocks);

  // Query the range of blocks that need to be refilled.
  // Returns (start, end). (0, 0) means no need to refill.
  std::pair<uint64_t, uint64_t> query_refill_range(off_t offset, size_t count);

  size_t block_size() {
    return block_size_;
  }

  void set_latest_read_off(off_t offset);

  // Drop all existing cache blocks by incrementing the generation number,
  // which causes subsequent accesses to reload the data.
  void drop() {
    increment_generation();
  }

 private:
  class MetaStore {
   public:
    MetaStore(BlockCache *cache, uint64_t block_size)
        : cache_(cache), block_size_(block_size) {}
    ~MetaStore();
    void get_locked_block(uint64_t block_id, BlockInfo **info);
    int get_and_lock_block(uint64_t block_id, BlockInfo **info,
                           BlockLockType lock_type,
                           std::function<int(BlockInfo **)> alloc_fn = nullptr);
    int evict_one_lock_held(BlockInfo **info, uint64_t latest_read_block_id);

   private:
    BlockCache *cache_ = nullptr;

    std::map<uint64_t, BlockInfo *> data_;
    photon::spinlock lock_;

    const uint64_t block_size_ = 0;

    uint64_t generation_ = 1;

    friend class BlockCache;
  };

  class BlockPool {
   public:
    int alloc_one_block(char **ptr);
    void expand(const std::vector<char *> &blocks);

   private:
    photon::spinlock lock_;
    std::vector<char *> free_list_;
  };

  int alloc_block(BlockInfo **info);
  void rollback_locked_block_range(uint64_t start_block_id, int count);

  void increment_generation();

  BlockPool block_pool_;
  std::unique_ptr<MetaStore> meta_store_;

  std::atomic<uint64_t> latest_read_block_id_ = {0};
  const uint64_t block_size_ = 0;
};

// BlockCacheManager provides a centralized management mechanism for block
// caches, including allocation/deallocation of cache blocks, reference counting
// for cache lifecycle management, and thread-safe access control. It supports
// dynamic expansion  of cache capacity and integrates with external memory
// pools for memory block management.
class BlockCacheManager {
 public:
  BlockCacheManager(size_t block_size) : block_size_(block_size){};
  ~BlockCacheManager();

  // Get the cache instance and increase the reference count.
  std::pair<BlockCache *, RangeLock *> get();

  // Try to expand cache blocks by allocating 'count' blocks via alloc_cb
  // callback, and the total number of blocks should not exceed 'max_capacity'
  // after expansion. Returns the number of blocks actually allocated.
  size_t try_expand_blocks(
      std::function<std::vector<char *>(uint64_t)> alloc_cb, uint64_t count,
      uint64_t max_capacity);

  // Decrease the reference count and release the cache instance if
  // the reference count is 0.
  void release(std::function<void(const std::vector<char *> &)> release_cb,
               uint64_t count);

  // Return total block count.
  size_t capacity();

  size_t block_size() const {
    return block_size_;
  }

 private:
  const size_t block_size_ = 1048576;
  RangeLock range_lock_;

  std::mutex mtx_;
  BlockCache *cache_ = nullptr;
  std::vector<char *> blocks_;
  size_t num_used_blocks_ = 0;
  uint64_t ref_cnt_ = 0;
};

}  // namespace OssFileSystem
