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

#include "mem_cache.h"

#include <photon/common/alog-stdstring.h>
#include <photon/thread/thread.h>

#include <bitset>

#include "common/macros.h"

namespace OssFileSystem {

int BlockCache::BlockPool::alloc_one_block(char **ptr) {
  SCOPED_LOCK(lock_);
  if (free_list_.empty()) {
    return -ENOSPC;
  }

  *ptr = free_list_.back();
  free_list_.pop_back();
  return 0;
}

void BlockCache::BlockPool::expand(const std::vector<char *> &blocks) {
  SCOPED_LOCK(lock_);
  free_list_.insert(free_list_.end(), blocks.begin(), blocks.end());
}

void BlockCache::MetaStore::get_locked_block(uint64_t block_id,
                                             BlockInfo **info) {
  SCOPED_LOCK(lock_);
  auto it = data_.find(block_id);
  RELEASE_ASSERT(it != data_.end());
  *info = it->second;
  RELEASE_ASSERT(it->second->lock.is_locked());
}

int BlockCache::MetaStore::get_and_lock_block(
    uint64_t block_id, BlockInfo **info, BlockLockType lock_type,
    std::function<int(BlockInfo **)> alloc_fn) {
  SCOPED_LOCK(lock_);

  auto it = data_.find(block_id);
  if (it == data_.end()) {
    if (lock_type == WRITE) {
      assert(alloc_fn);
      if (alloc_fn(info) != 0) {
        if (evict_one_lock_held(info, cache_->latest_read_block_id_) != 0) {
          return -ENOSPC;
        }
      }
      bool locked = (*info)->lock.try_write_lock();
      (*info)->generation = generation_;
      RELEASE_ASSERT_WITH_MSG(locked, "block lock failed");
      data_[block_id] = *info;
      return 0;
    }
    return -ENOENT;
  }

  *info = it->second;
  BlockInfo *block_info = *info;
  switch (lock_type) {
    case READ:
      if (block_info->generation < generation_) return -ENOENT;
      if (!block_info->lock.try_read_lock()) return -EAGAIN;
      break;
    case WRITE:
      if (!block_info->lock.try_write_lock()) return -EAGAIN;
      block_info->generation = generation_;
      break;
  }

  return 0;
}

BlockCache::MetaStore::~MetaStore() {
  for (auto &it : data_) {
    delete it.second;
  }
}

int BlockCache::MetaStore::evict_one_lock_held(BlockInfo **info,
                                               uint64_t latest_read_block_id) {
  // Simple evict strategy:
  // 1. try find the first block in [0, latest_read_block_id).
  // 2. if there is no useable block in step 1, find the last
  //    block in [latest_read_block_id, max_blocks).
  auto it = data_.begin();
  while (it != data_.end() && it->first < latest_read_block_id) {
    // we only lock block under lock_, so no one can lock it here.
    if (!it->second->lock.is_locked()) {
      *info = it->second;
      data_.erase(it);
      return 0;
    }

    ++it;
  }

  if (it == data_.end()) return -ENOSPC;

  auto rit = data_.rbegin();
  while (rit != data_.rend() && rit->first >= latest_read_block_id) {
    // we only lock block under lock_, so no one can lock it here.
    if (!rit->second->lock.is_locked()) {
      *info = rit->second;
      data_.erase(rit->first);
      return 0;
    }

    ++rit;
  }

  return -ENOSPC;
}

void BlockCache::expand_blocks(const std::vector<char *> &blocks) {
  block_pool_.expand(blocks);
}

int BlockCache::alloc_block(BlockInfo **info) {
  char *block_buf = nullptr;
  int r = block_pool_.alloc_one_block(&block_buf);
  if (r == 0) {
    BlockInfo *block_info = new BlockInfo;
    block_info->mem = block_buf;
    *info = block_info;
    return 0;
  }

  return -ENOSPC;
}

ssize_t BlockCache::pread(char *buf, off_t offset, size_t count) {
  size_t read = 0;
  uint64_t block_id = offset / block_size_;
  uint64_t block_off = offset % block_size_;

  BlockInfo *block_info = nullptr;
  while (read < count) {
    if (meta_store_->get_and_lock_block(block_id, &block_info,
                                        BlockLockType::READ) != 0) {
      return -ENOENT;
    }

    ssize_t read_size = std::min(count - read, block_size_ - block_off);

    if (block_info->valid_off > block_off ||
        block_info->valid_off + block_info->valid_size <
            block_off + read_size) {
      block_info->lock.unlock_read();
      return -ENOENT;
    }

    if (buf != nullptr) {
      memcpy(buf + read, block_info->mem + block_off, read_size);
    }

    block_info->lock.unlock_read();
    read += read_size;
    block_off = 0;
    block_id++;
  }

  return read;
}

ssize_t BlockCache::pin(off_t offset, size_t count, void **buf) {
  uint64_t block_id = offset / block_size_;
  uint64_t block_off = offset % block_size_;
  BlockInfo *block_info = nullptr;

  if (meta_store_->get_and_lock_block(block_id, &block_info,
                                      BlockLockType::READ) != 0) {
    return -ENOENT;
  }

  if (block_info->valid_off > block_off ||
      block_info->valid_off + block_info->valid_size < block_off + count) {
    block_info->lock.unlock_read();
    return -ENOENT;
  }

  *buf = block_info->mem + block_off;
  return count;
}

void BlockCache::unpin(off_t offset) {
  uint64_t block_id = offset / block_size_;
  BlockInfo *block_info = nullptr;

  meta_store_->get_locked_block(block_id, &block_info);
  block_info->lock.unlock_read();
}

void BlockCache::rollback_locked_block_range(uint64_t start_block_id,
                                             int count) {
  BlockInfo *block_info = nullptr;
  for (int i = 0; i < count; i++) {
    meta_store_->get_locked_block(start_block_id + i, &block_info);
    block_info->valid_size = 0;
    block_info->lock.unlock_write();
  }
}

int BlockCache::try_lock_blocks(uint64_t offset, uint64_t count,
                                std::vector<iovec> &blocks) {
  BlockInfo *block_info = nullptr;
  size_t written = 0;
  uint64_t block_id = offset / block_size_;
  uint64_t block_off = offset % block_size_;
  uint64_t start_block_id = block_id;

  while (written < count) {
    int r = meta_store_->get_and_lock_block(
        block_id, &block_info, BlockLockType::WRITE,
        [this](BlockInfo **info) { return this->alloc_block(info); });
    if (r < 0) {
      rollback_locked_block_range(start_block_id, block_id - start_block_id);
      return r;
    }

    ssize_t write_size = std::min(count - written, block_size_ - block_off);

    blocks.push_back({block_info->mem + block_off, (size_t)write_size});
    block_info->valid_off = block_off;
    block_info->valid_size = write_size;

    written += write_size;
    block_off = 0;
    block_id++;
  }

  return 0;
}

void BlockCache::unlock_blocks(uint64_t offset, uint64_t count, bool evict) {
  BlockInfo *block_info = nullptr;

  size_t written = 0;
  uint64_t block_id = offset / block_size_;
  uint64_t block_off = offset % block_size_;

  while (written < count) {
    meta_store_->get_locked_block(block_id, &block_info);
    ssize_t write_size = std::min(count - written, block_size_ - block_off);

    if (evict) {
      block_info->valid_size = 0;
    }

    block_info->lock.unlock_write();
    written += write_size;
    block_id++;
    block_off = 0;
  }
}

void BlockCache::set_latest_read_off(off_t offset) {
  latest_read_block_id_ = offset / block_size_;
}

void BlockCache::increment_generation() {
  SCOPED_LOCK(meta_store_->lock_);
  meta_store_->generation_++;
}

std::pair<uint64_t, uint64_t> BlockCache::query_refill_range(off_t offset,
                                                             size_t count) {
  size_t read = 0;
  uint64_t block_id = offset / block_size_;
  uint64_t block_off = offset % block_size_;

  bool need_refill = false;
  uint64_t left_refill_block_id = std::numeric_limits<uint64_t>::max();
  uint64_t right_refill_block_id = 0;

  // Currently, there will not be a large number of blocks. We
  // can also make a better implementation by searching from both ends
  // to the middle and fastly break when the left and right are found.
  BlockInfo *block_info = nullptr;
  while (read < count) {
    ssize_t read_size = std::min(count - read, block_size_ - block_off);
    int r = meta_store_->get_and_lock_block(block_id, &block_info,
                                            BlockLockType::READ);
    if (r == 0) {
      // Not enough data in the block.
      if (block_info->valid_off > block_off ||
          block_info->valid_off + block_info->valid_size <
              block_off + read_size) {
        left_refill_block_id = std::min(block_id, left_refill_block_id);
        right_refill_block_id = std::max(block_id, right_refill_block_id);
        need_refill = true;
      }
      block_info->lock.unlock_read();
    } else if (r == -ENOENT) {
      left_refill_block_id = std::min(block_id, left_refill_block_id);
      right_refill_block_id = std::max(block_id, right_refill_block_id);
      need_refill = true;
    } else if (r == -EAGAIN) {
      // Empty body, and -EAGAIN means someone is writing in the block.
    }
    read += read_size;
    block_off = 0;
    block_id++;
  }

  if (!need_refill) return std::make_pair(0, 0);

  // New writes to a block do not merge with existing data, so we
  // return the whole block range to be refilled.
  return std::make_pair(
      left_refill_block_id * block_size_,
      (right_refill_block_id - left_refill_block_id + 1) * block_size_);
}

BlockCacheManager::~BlockCacheManager() {
  RELEASE_ASSERT(ref_cnt_ == 0);
  RELEASE_ASSERT(cache_ == nullptr);
  RELEASE_ASSERT(num_used_blocks_ == 0);
}

std::pair<BlockCache *, RangeLock *> BlockCacheManager::get() {
  std::lock_guard<std::mutex> l(mtx_);
  if (cache_ == nullptr) {
    cache_ = new BlockCache(block_size_);
  }
  ++ref_cnt_;
  return {cache_, &range_lock_};
}

size_t BlockCacheManager::try_expand_blocks(
    std::function<std::vector<char *>(uint64_t)> alloc_cb, uint64_t count,
    uint64_t max_capacity) {
  RELEASE_ASSERT(cache_ != nullptr);

  size_t allocated_blocks = 0;
  std::lock_guard<std::mutex> l(mtx_);
  if (num_used_blocks_ >= max_capacity) return 0;

  // Allocate from the unused blocks.
  RELEASE_ASSERT(blocks_.size() >= num_used_blocks_);
  auto free = blocks_.size() - num_used_blocks_;
  if (free >= count) {
    num_used_blocks_ += count;
    return count;
  }

  count -= free;
  allocated_blocks += free;
  num_used_blocks_ += free;

  // This occurs when someone expanded the blocks_ with a larger max_capacity
  // before, skip expansion.
  if (max_capacity <= blocks_.size()) return allocated_blocks;

  // Allocate from the alloc_cb.
  if (count > 0) {
    count = std::min(count, max_capacity - blocks_.size());
    auto new_blocks = alloc_cb(count);
    if (new_blocks.size() > 0) {
      cache_->expand_blocks(new_blocks);
      blocks_.insert(blocks_.end(), new_blocks.begin(), new_blocks.end());
      allocated_blocks += new_blocks.size();
      num_used_blocks_ += new_blocks.size();
    }
  }

  return allocated_blocks;
}

void BlockCacheManager::release(
    std::function<void(const std::vector<char *> &)> release_cb,
    uint64_t count) {
  BlockCache *cache_ptr = nullptr;
  std::vector<char *> old_blocks;
  {
    std::lock_guard<std::mutex> l(mtx_);
    RELEASE_ASSERT(ref_cnt_ > 0);
    RELEASE_ASSERT(cache_ != nullptr);
    RELEASE_ASSERT(num_used_blocks_ >= count);
    num_used_blocks_ -= count;
    if (--ref_cnt_ == 0) {
      cache_ptr = cache_;
      cache_ = nullptr;
      blocks_.swap(old_blocks);
    }
  }

  if (cache_ptr != nullptr) {
    delete cache_ptr;
    release_cb(old_blocks);
  }
}

size_t BlockCacheManager::capacity() {
  std::lock_guard<std::mutex> l(mtx_);
  return blocks_.size();
}

}  // namespace OssFileSystem
