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

#include <photon/thread/thread.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "cache.h"
#include "mem_pool.h"

class BlockCacheStoreTest;

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

class BlockCacheStore;
struct BlockCacheHandle;

class BlockCacheStore : public ICacheStore {
 public:
  BlockCacheStore(uint64_t block_size = 1048576) : block_size_(block_size) {
    meta_store_ = std::make_unique<MetaStore>(this);
  }

  ssize_t pread(char *buf, off_t offset, size_t count) override;

  // The pin operation returns a buffer pointer pointing to the
  // corresponding offset and increments the reference count to
  // prevent modification when success. If cache miss or the range
  // crosses the block boundary, -ENOENT will be returned.
  ssize_t pin(off_t offset, size_t count, void **buf) override;

  // Release the block and decrease the reference count.
  void unpin(off_t offset) override;

  // Only collect memory blocks pointer which should be managed outside.
  void expand_blocks(const std::vector<char *> &blocks);

  // Return total blocks managed by this cache (MetaStore + BlockPool
  // free_list).
  size_t total_blocks();

  // Shrink and return up to 'count' blocks from cache.
  // May return fewer blocks if not enough are available (free_list exhausted
  // and no evictable blocks in MetaStore).
  // Returns a pair: {memory blocks to deallocate, BlockInfo objects to delete}.
  // The caller is responsible for freeing both outside of any locks.
  std::pair<std::vector<char *>, std::vector<BlockInfo *>> shrink_blocks(
      size_t count);

  // Drain all blocks when ref_cnt reaches 0 (final cleanup).
  // Returns a pair: {memory blocks to deallocate, BlockInfo objects to delete}.
  // The caller is responsible for freeing both outside of any locks.
  std::pair<std::vector<char *>, std::vector<BlockInfo *>> drain_all_blocks();

  // Query the range of blocks that need to be refilled.
  // Returns (start, end). (0, 0) means no need to refill.
  std::pair<off_t, size_t> query_refill_range(off_t offset,
                                              size_t count) override;

  int acquire_write_buffer(RangeBuffer &range_buffer) override {
    return try_lock_blocks(range_buffer.offset, range_buffer.count,
                           range_buffer.buffer);
  }

  void release_write_buffer(const RangeBuffer &range_buffer,
                            bool evict = false) override {
    unlock_blocks(range_buffer.offset, range_buffer.count, evict);
  }

  void update_retention_range(BlockCacheHandle *h, off_t offset, size_t count);

  void clear_retention_range(BlockCacheHandle *h) {
    retention_manager_.remove_range(h);
  }

  // Drop all existing cache blocks by incrementing the generation number,
  // which causes subsequent accesses to reload the data.
  void drop() override {
    increment_generation();
  }

 private:
  struct RetentionManager {
    struct Range {
      uint64_t start;
      uint64_t end;
      BlockCacheHandle *h;
    };

    photon::spinlock lock;
    std::vector<Range> ranges;

    size_t size();
    void update_range(BlockCacheHandle *h, uint64_t start, uint64_t end);
    void remove_range(BlockCacheHandle *h);
    std::optional<uint64_t> get_max_covered_end(uint64_t current_point);
    uint64_t get_min_start();
    uint64_t get_max_end();
  };

  class MetaStore {
   public:
    MetaStore(BlockCacheStore *cache) : cache_(cache) {}
    ~MetaStore();
    void get_locked_block(uint64_t block_id, BlockInfo **info);
    int get_and_lock_block(uint64_t block_id, BlockInfo **info,
                           BlockLockType lock_type,
                           std::function<int(BlockInfo **)> alloc_fn = nullptr);
    int evict_one_lock_held(BlockInfo **info);

   private:
    BlockCacheStore *cache_ = nullptr;

    std::map<uint64_t, BlockInfo *> data_;
    photon::spinlock lock_;

    uint64_t generation_ = 1;

    friend class BlockCacheStore;
  };

  class BlockPool {
   public:
    int alloc_one_block(char **ptr);
    void expand(const std::vector<char *> &blocks);

    // Get the number of free blocks in the pool.
    size_t get_free_block_count() {
      SCOPED_LOCK(lock_);
      return free_list_.size();
    }

    // Extract up to 'count' free blocks from the pool.
    std::vector<char *> extract_free_blocks(size_t count);

    // Clear and return all free blocks from the pool.
    std::vector<char *> clear_all();

   private:
    photon::spinlock lock_;
    std::vector<char *> free_list_;
  };

  int alloc_block(BlockInfo **info);
  void rollback_locked_block_range(uint64_t start_block_id, int count);

  void increment_generation();

  // Try to allocate blocks for range [offset, offset + count) and
  // lock it if successful.
  int try_lock_blocks(uint64_t offset, uint64_t count, IOVector &blocks);

  // Unlock the block. If evict is true, the block will be evicted.
  // Evict flag is used for the case that writing block data failed
  // and we rollback metadata for those blocks.
  void unlock_blocks(uint64_t offset, uint64_t count, bool evict = false);

  BlockPool block_pool_;
  std::unique_ptr<MetaStore> meta_store_;
  RetentionManager retention_manager_;

  const uint64_t block_size_ = 0;

  friend class BlockCacheHandle;
  friend class ::BlockCacheStoreTest;
};

struct BlockCacheHandle : public CacheHandle {
 public:
  BlockCacheHandle(BlockCacheStore *cache_store, RangeLock *range_lock)
      : CacheHandle(cache_store, range_lock) {}

  ~BlockCacheHandle() {
    static_cast<BlockCacheStore *>(cache_store)->clear_retention_range(this);
  }

  void on_read_success(off_t offset, size_t prefetch_buffer_size) override {
    static_cast<BlockCacheStore *>(cache_store)
        ->update_retention_range(this, offset, prefetch_buffer_size);
  }

 private:
  int64_t retention_index = -1;

  friend class BlockCacheStore::RetentionManager;
};

// BlockCache provides a centralized management mechanism for block
// caches, including allocation/deallocation of cache blocks, reference counting
// for cache lifecycle management, and thread-safe access control. It supports
// dynamic expansion  of cache capacity and integrates with external memory
// pools for memory block management.
class BlockCache : public ICache {
 public:
  BlockCache(std::shared_ptr<FixedBlockMemoryPool> block_pool)
      : block_pool_(std::move(block_pool)) {}
  ~BlockCache();

  size_t block_size() const override {
    return block_pool_->block_size();
  }

  // Get the cache instance and increase the reference count.
  CacheHandle *get(std::string_view name = "", std::string_view etag = "",
                   off_t actual_size = 0) override;

  // Try to expand cache blocks by allocating 'count' blocks in buffer_pool_,
  // and the total number of blocks should not exceed 'max_capacity'
  // after expansion. Returns the number of blocks actually allocated.
  size_t try_expand_blocks(uint64_t count, uint64_t max_capacity,
                           bool ignore_limit = false) override;

  // Decrease the reference count and release the cache instance if
  // the reference count is 0.
  void release(CacheHandle *h, uint64_t count) override;

  // Return total block count.
  size_t capacity() override;

 private:
  RangeLock range_lock_;

  std::mutex mtx_;
  BlockCacheStore *cache_store_ = nullptr;
  uint64_t ref_cnt_ = 0;

  std::shared_ptr<FixedBlockMemoryPool> block_pool_;
};

}  // namespace OssFileSystem
