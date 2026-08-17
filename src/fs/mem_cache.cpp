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

int BlockCacheStore::BlockPool::alloc_one_block(char **ptr) {
  SCOPED_LOCK(lock_);
  if (free_list_.empty()) {
    return -ENOSPC;
  }

  *ptr = free_list_.back();
  free_list_.pop_back();
  return 0;
}

void BlockCacheStore::BlockPool::expand(const std::vector<char *> &blocks) {
  SCOPED_LOCK(lock_);
  free_list_.insert(free_list_.end(), blocks.begin(), blocks.end());
}

std::vector<char *> BlockCacheStore::BlockPool::extract_free_blocks(
    size_t count) {
  SCOPED_LOCK(lock_);
  size_t take = std::min(count, free_list_.size());
  if (take == 0) {
    return {};
  }
  auto start_it = free_list_.end() - take;
  std::vector<char *> blocks(start_it, free_list_.end());
  free_list_.erase(start_it, free_list_.end());
  return blocks;
}

std::vector<char *> BlockCacheStore::BlockPool::clear_all() {
  std::vector<char *> blocks;
  SCOPED_LOCK(lock_);
  free_list_.swap(blocks);
  return blocks;
}

size_t BlockCacheStore::RetentionManager::size() {
  SCOPED_LOCK(lock);
  return ranges.size();
}

void BlockCacheStore::RetentionManager::update_range(BlockCacheHandle *h,
                                                     uint64_t start,
                                                     uint64_t end) {
  SCOPED_LOCK(lock);
  if (unlikely(h->retention_index == -1)) {
    h->retention_index = ranges.size();
    ranges.push_back({start, end, h});
  } else {
    RELEASE_ASSERT(h->retention_index >= 0 &&
                   static_cast<size_t>(h->retention_index) < ranges.size());
    ranges[h->retention_index] = {start, end, h};
  }
}

void BlockCacheStore::RetentionManager::remove_range(BlockCacheHandle *h) {
  SCOPED_LOCK(lock);

  int64_t index = h->retention_index;
  if (index == -1) return;

  RELEASE_ASSERT(ranges.size() >= 1 && static_cast<int64_t>(ranges.size()) <=
                                           std::numeric_limits<int64_t>::max());
  int64_t last = static_cast<int64_t>(ranges.size()) - 1;
  if (index != last) {
    ranges[index] = ranges[last];
    ranges[index].h->retention_index = index;
  }
  ranges.pop_back();
  h->retention_index = -1;
}

std::optional<uint64_t> BlockCacheStore::RetentionManager::get_max_covered_end(
    uint64_t current_point) {
  SCOPED_LOCK(lock);

  if (ranges.empty()) {
    return std::nullopt;
  }

  uint64_t max_end = 0;
  bool covered = false;
  bool changed = true;

  // Counter to track the number of comparison steps in this query.
  // Used for adaptive optimization to detect worst-case scenarios.
  uint64_t current_scan_steps = 0;

  // --- Algorithm: Iterative Expansion (BFS-like on 1D ranges) ---
  // Since we cannot merge ranges from different owners (to support O(1) removal
  // by owner), overlapping ranges from different owners may exist as separate
  // entries. We must iteratively expand the coverage window [current_point,
  // max_end] until no further ranges can extend it.
  //
  // Complexity Analysis:
  // - Best Case (Sorted Ranges): O(N). The loop runs once because all connected
  //   ranges are adjacent in the vector. We scan linearly and break early.
  // - Worst Case (Reverse Sorted Chain): O(N^2). If ranges form a chain [N,
  // N+1], [N-1, N]...
  //   stored in reverse order, we might need N passes, each scanning up to N
  //   elements.
  // - Practical Case (N <= 64): Even in the theoretical worst case, 64^2 = 4096
  // operations
  //   is negligible (< 1 microsecond) on modern CPUs due to cache locality.
  while (changed) {
    changed = false;
    for (const auto &r : ranges) {
      current_scan_steps++;

      if (!covered) {
        // Phase 1: Find the initial range that strictly covers the query point.
        if (r.start <= current_point && r.end >= current_point) {
          covered = true;
          max_end = r.end;
          changed = true;
        }
      } else {
        // Phase 2: Expand the current coverage window.
        // Check if the current range 'r' overlaps or touches the known
        // [current_point, max_end].
        if (r.start <= max_end + 1 && r.end > max_end) {
          max_end = r.end;
          changed = true;
        }
      }
    }
  }

  // --- Adaptive Strategy with Lower Bound ---
  // Formula: threshold = max(Min_Cost_Limit, N^2 / 4)
  //
  // Reasoning:
  // 1. Min_Cost_Limit (e.g., 256):
  //    Sorting has a fixed overhead (~300ns). For small N (e.g., N=8), the
  //    query is already fast (~50ns). Triggering sort would be 6x slower than
  //    just running the "slow" query! We only want to sort when the saved query
  //    time > sort cost.
  //
  // 2. N^2 / 4:
  //    For larger N, we want to be proactive. If steps exceed ~25% of
  //    worst-case, it indicates significant disorder.

  const size_t n = ranges.size();
  // Use a conservative lower bound. 256 steps is roughly the break-even point
  // where the cost of sorting equals the cumulative savings of optimized
  // queries.
  constexpr uint64_t kMinSortThreshold = 256;

  uint64_t dynamic_threshold = (n * n) / 4;
  uint64_t threshold = (dynamic_threshold > kMinSortThreshold)
                           ? dynamic_threshold
                           : kMinSortThreshold;

  if (current_scan_steps >= threshold) {
    auto now = std::chrono::steady_clock::now();
    std::sort(ranges.begin(), ranges.end(),
              [](const Range &a, const Range &b) { return a.start < b.start; });

    for (size_t i = 0; i < ranges.size(); ++i) {
      ranges[i].h->retention_index = static_cast<int64_t>(i);
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now() - now)
                       .count();
    LOG_DEBUG(
        "[`] Sorted retention ranges due to high scan steps (`), cost ` ns",
        this, current_scan_steps, elapsed);
  }

  return covered ? std::optional<uint64_t>(max_end) : std::nullopt;
}

uint64_t BlockCacheStore::RetentionManager::get_min_start() {
  SCOPED_LOCK(lock);
  uint64_t min_start = std::numeric_limits<uint64_t>::max();
  for (const auto &r : ranges) {
    if (r.start < min_start) {
      min_start = r.start;
    }
  }
  return min_start;
}

uint64_t BlockCacheStore::RetentionManager::get_max_end() {
  SCOPED_LOCK(lock);
  uint64_t max_end = 0;
  for (const auto &r : ranges) {
    if (r.end > max_end) {
      max_end = r.end;
    }
  }
  return max_end;
}

void BlockCacheStore::MetaStore::get_locked_block(uint64_t block_id,
                                                  BlockInfo **info) {
  SCOPED_LOCK(lock_);
  auto it = data_.find(block_id);
  RELEASE_ASSERT(it != data_.end());
  *info = it->second;
  RELEASE_ASSERT(it->second->lock.is_locked());
}

int BlockCacheStore::MetaStore::get_and_lock_block(
    uint64_t block_id, BlockInfo **info, BlockLockType lock_type,
    std::function<int(BlockInfo **)> alloc_fn) {
  SCOPED_LOCK(lock_);

  auto it = data_.find(block_id);
  if (it == data_.end()) {
    if (lock_type == WRITE) {
      assert(alloc_fn);
      if (alloc_fn(info) != 0) {
        if (evict_one_lock_held(info) != 0) {
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

BlockCacheStore::MetaStore::~MetaStore() {
  for (auto &it : data_) {
    delete it.second;
  }
}

int BlockCacheStore::MetaStore::evict_one_lock_held(BlockInfo **info) {
  // Eviction Strategy (4-Level Priority):
  //
  // We maintain a set of retention ranges. Blocks outside these ranges are
  // evicted first. Within retention ranges, we prefer evicting blocks not
  // covered by any active range, falling back to covered ones only when
  // necessary.
  //
  // Block ID Space Layout:
  // 0 ........... min_start ............. max_end .......... INF
  // |                 |                      |                |
  // |  [Priority 1]   |   [Priority 3/4]     | [Priority 2]   |
  // |  Left Side      |   Retention Window   |  Right Side    |
  // |  (Evict First)  |                      | (Evict Second) |
  //                   |                      |
  //                   |                      |
  //               min_start               max_end
  //          (min of all ranges)     (max of all ranges)
  //
  // Detailed Priority Order:
  //
  // P1: [0, min_start) - Left of all retention ranges
  //     Traverse from beginning, evict first unlocked block.
  //
  // P2: [max_end, INF) - Right of all retention ranges
  //     Traverse from end, evict first unlocked block.
  //
  // P3: [min_start, max_end) - Inside retention window, NOT covered
  //     For each unlocked block, check coverage via get_max_covered_end().
  //     If not covered (nullopt), evict immediately.
  //     Optimization: Skip to upper_bound(covered_end) to avoid checking
  //     multiple blocks within the same retention range.
  //
  // P4: Fallback - Inside retention window, covered but unlocked
  //     If P3 finds no candidate, evict the first covered block we saw.
  //     This is a last resort as it may impact active operations.
  //
  // Returns: 0 on success (block evicted), -ENOSPC if no evictable block found.
  auto &retention_mgr = cache_->retention_manager_;

  auto lit = data_.begin();
  if (retention_mgr.size() > 0) {
    auto min_start = retention_mgr.get_min_start();
    while (lit != data_.end() && lit->first < min_start) {
      if (!lit->second->lock.is_locked()) {
        *info = lit->second;
        data_.erase(lit);
        return 0;
      }

      ++lit;
    }

    if (lit == data_.end()) return -ENOSPC;
  }

  auto rit = data_.rbegin();
  auto max_end = retention_mgr.get_max_end();
  while (rit != data_.rend() && rit->first > max_end) {
    if (!rit->second->lock.is_locked()) {
      *info = rit->second;
      data_.erase(std::prev(rit.base()));
      return 0;
    }

    ++rit;
  }

  std::optional<uint64_t> fallback_candidate;
  auto upper_it = rit.base();
  auto it = lit;
  while (it != data_.end() && it != upper_it) {
    if (upper_it != data_.end() && it->first >= upper_it->first) {
      break;
    }

    if (it->second->lock.is_locked()) {
      ++it;
      continue;
    }

    auto max_covered_end = retention_mgr.get_max_covered_end(it->first);
    if (!max_covered_end.has_value()) {
      *info = it->second;
      data_.erase(it);
      return 0;
    }

    if (!fallback_candidate.has_value()) {
      fallback_candidate = std::optional<uint64_t>(it->first);
    }

    it = data_.upper_bound(max_covered_end.value());
  }

  if (fallback_candidate.has_value()) {
    auto it = data_.find(fallback_candidate.value());
    *info = it->second;
    data_.erase(it);
    return 0;
  }

  return -ENOSPC;
}

void BlockCacheStore::expand_blocks(const std::vector<char *> &blocks) {
  block_pool_.expand(blocks);
}

size_t BlockCacheStore::total_blocks() {
  SCOPED_LOCK(meta_store_->lock_);
  return meta_store_->data_.size() + block_pool_.get_free_block_count();
}

std::pair<std::vector<char *>, std::vector<BlockInfo *>>
BlockCacheStore::shrink_blocks(size_t count) {
  std::vector<char *> blocks_to_free;
  std::vector<BlockInfo *> infos_to_delete;

  auto free_blocks = block_pool_.extract_free_blocks(count);
  size_t taken = free_blocks.size();
  blocks_to_free.insert(blocks_to_free.end(), free_blocks.begin(),
                        free_blocks.end());

  if (taken < count) {
    size_t need_evict = count - taken;
    blocks_to_free.reserve(count);
    infos_to_delete.reserve(need_evict);
    SCOPED_LOCK(meta_store_->lock_);
    BlockInfo *info = nullptr;
    for (size_t i = 0; i < need_evict; ++i) {
      if (meta_store_->evict_one_lock_held(&info) != 0) {
        break;
      }
      blocks_to_free.push_back(info->mem);
      infos_to_delete.push_back(info);
    }
  }

  return {blocks_to_free, infos_to_delete};
}

std::pair<std::vector<char *>, std::vector<BlockInfo *>>
BlockCacheStore::drain_all_blocks() {
  std::vector<char *> blocks_to_free;
  std::vector<BlockInfo *> infos_to_delete;

  blocks_to_free.reserve(meta_store_->data_.size() +
                         block_pool_.get_free_block_count());
  infos_to_delete.reserve(meta_store_->data_.size());

  // Collect all BlockInfo from MetaStore.
  for (auto &it : meta_store_->data_) {
    RELEASE_ASSERT(!it.second->lock.is_locked());
    blocks_to_free.push_back(it.second->mem);
    infos_to_delete.push_back(it.second);
  }
  meta_store_->data_.clear();

  // Add all free blocks from BlockPool.
  auto free_blocks = block_pool_.clear_all();
  blocks_to_free.insert(blocks_to_free.end(), free_blocks.begin(),
                        free_blocks.end());

  return {blocks_to_free, infos_to_delete};
}

int BlockCacheStore::alloc_block(BlockInfo **info) {
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

ssize_t BlockCacheStore::pread(char *buf, off_t offset, size_t count) {
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

ssize_t BlockCacheStore::pin(off_t offset, size_t count, void **buf) {
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

void BlockCacheStore::unpin(off_t offset) {
  uint64_t block_id = offset / block_size_;
  BlockInfo *block_info = nullptr;

  meta_store_->get_locked_block(block_id, &block_info);
  block_info->lock.unlock_read();
}

void BlockCacheStore::rollback_locked_block_range(uint64_t start_block_id,
                                                  int count) {
  BlockInfo *block_info = nullptr;
  for (int i = 0; i < count; i++) {
    meta_store_->get_locked_block(start_block_id + i, &block_info);
    block_info->valid_size = 0;
    block_info->lock.unlock_write();
  }
}

int BlockCacheStore::try_lock_blocks(uint64_t offset, uint64_t count,
                                     IOVector &blocks) {
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

void BlockCacheStore::unlock_blocks(uint64_t offset, uint64_t count,
                                    bool evict) {
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

void BlockCacheStore::update_retention_range(BlockCacheHandle *h, off_t offset,
                                             size_t count) {
  if (count > 0) {
    uint64_t start_block_id = offset / block_size_;
    uint64_t end_block_id = (offset + count - 1) / block_size_;
    retention_manager_.update_range(h, start_block_id, end_block_id);
  }
}

void BlockCacheStore::increment_generation() {
  SCOPED_LOCK(meta_store_->lock_);
  meta_store_->generation_++;
}

std::pair<off_t, size_t> BlockCacheStore::query_refill_range(off_t offset,
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

BlockCache::~BlockCache() {
  RELEASE_ASSERT(ref_cnt_ == 0);
  RELEASE_ASSERT(cache_store_ == nullptr);
}

CacheHandle *BlockCache::get(std::string_view /* name */,
                             std::string_view /* etag */, size_t /* size */) {
  std::lock_guard<std::mutex> l(mtx_);
  if (cache_store_ == nullptr) {
    cache_store_ = new BlockCacheStore(block_pool_->block_size());
  }
  ++ref_cnt_;
  return new BlockCacheHandle(cache_store_, &range_lock_);
}

size_t BlockCache::try_expand_blocks(uint64_t count, uint64_t max_capacity,
                                     bool ignore_limit) {
  RELEASE_ASSERT(block_pool_ != nullptr);
  RELEASE_ASSERT(cache_store_ != nullptr);

  std::lock_guard<std::mutex> l(mtx_);
  size_t curr_total = cache_store_->total_blocks();
  if (curr_total >= max_capacity) return 0;

  size_t need_blocks = std::min(count, max_capacity - curr_total);
  auto new_blocks = block_pool_->try_allocate(need_blocks, ignore_limit);

  if (new_blocks.size() > 0) {
    cache_store_->expand_blocks(new_blocks);
  }

  return new_blocks.size();
}

void BlockCache::release(CacheHandle *h, uint64_t count) {
  RELEASE_ASSERT(block_pool_ != nullptr);

  delete h;

  BlockCacheStore *cache_ptr = nullptr;
  std::vector<char *> blocks_to_dealloc;
  std::vector<BlockInfo *> infos_to_delete;

  {
    std::lock_guard<std::mutex> l(mtx_);
    RELEASE_ASSERT(ref_cnt_ > 0);
    RELEASE_ASSERT(cache_store_ != nullptr);

    if (--ref_cnt_ == 0) {
      cache_ptr = cache_store_;
      cache_store_ = nullptr;
    } else {
      // Reserve at least 1 block when multiple handles share the cache.
      size_t total_blocks = cache_store_->total_blocks();
      size_t actual_shrink =
          std::min(count, total_blocks > 1 ? total_blocks - 1 : 0);
      if (actual_shrink > 0) {
        std::tie(blocks_to_dealloc, infos_to_delete) =
            cache_store_->shrink_blocks(actual_shrink);
      }
    }
  }

  if (cache_ptr != nullptr) {
    std::tie(blocks_to_dealloc, infos_to_delete) =
        cache_ptr->drain_all_blocks();
    delete cache_ptr;
  }

  for (auto *info : infos_to_delete) {
    delete info;
  }

  if (!blocks_to_dealloc.empty()) {
    block_pool_->deallocate(blocks_to_dealloc);
  }
}

size_t BlockCache::capacity() {
  std::lock_guard<std::mutex> l(mtx_);
  return cache_store_ ? cache_store_->total_blocks() : 0;
}

}  // namespace OssFileSystem
