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

#include <random>

#include "fs/mem_cache.h"
#include "test_suite.h"

static std::unique_ptr<BlockCacheHandle> create_test_handle(
    BlockCacheStore *cache) {
  static RangeLock dummy_lock;
  return std::make_unique<BlockCacheHandle>(cache, &dummy_lock);
}

class BlockCacheStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    block_size_ = 4096;
    cache_ = std::make_unique<BlockCacheStore>(block_size_);
    rng_.seed(std::random_device{}());
  }

  void verify_retention_range() {
    auto h1 = create_test_handle(cache_.get());
    auto h2 = create_test_handle(cache_.get());
    auto h3 = create_test_handle(cache_.get());

    // single range
    cache_->update_retention_range(h1.get(), block_size_ * 1, block_size_ * 2);
    ASSERT_EQ(cache_->retention_manager_.size(), 1ULL);
    ASSERT_EQ(cache_->retention_manager_.get_min_start(), 1ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_end(), 2ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(0).has_value());
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(1).value(), 2ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(4).has_value());

    // update range to the next
    cache_->update_retention_range(h1.get(), block_size_ * 2, block_size_ * 2);
    ASSERT_EQ(cache_->retention_manager_.size(), 1ULL);
    ASSERT_EQ(cache_->retention_manager_.get_min_start(), 2ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_end(), 3ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(0).has_value());
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(2).value(), 3ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(4).has_value());

    // add another range
    cache_->update_retention_range(h2.get(), block_size_ * 5, block_size_ * 4);
    ASSERT_EQ(cache_->retention_manager_.size(), 2ULL);
    ASSERT_EQ(cache_->retention_manager_.get_min_start(), 2ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_end(), 8ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(0).has_value());
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(2).value(), 3ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(4).has_value());
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(5).value(), 8ULL);

    // add overlapped range
    cache_->update_retention_range(h3.get(), block_size_ * 4, block_size_ * 2);
    ASSERT_EQ(cache_->retention_manager_.size(), 3ULL);
    ASSERT_EQ(cache_->retention_manager_.get_min_start(), 2ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_end(), 8ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(0).has_value());
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(2).value(), 8ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(4).value(), 8ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(5).value(), 8ULL);

    cache_->clear_retention_range(h1.get());
    ASSERT_EQ(cache_->retention_manager_.size(), 2ULL);
    ASSERT_EQ(cache_->retention_manager_.get_min_start(), 4ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_end(), 8ULL);
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(0).has_value());
    ASSERT_FALSE(cache_->retention_manager_.get_max_covered_end(2).has_value());
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(4).value(), 8ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(5).value(), 8ULL);

    cache_->clear_retention_range(h2.get());
    ASSERT_EQ(cache_->retention_manager_.size(), 1ULL);
    ASSERT_EQ(cache_->retention_manager_.get_min_start(), 4ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_end(), 5ULL);
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(4).value(), 5ULL);

    cache_->clear_retention_range(h3.get());
    ASSERT_EQ(cache_->retention_manager_.size(), 0ULL);
  }

  void verify_retention_range_adaptive_resort() {
    std::vector<std::unique_ptr<BlockCacheHandle>> handles;
    handles.reserve(1024);
    // check get_max_covered_end latency in the worst case
    for (int i = 1024; i >= 1; i--) {
      auto h = create_test_handle(cache_.get());
      cache_->update_retention_range(h.get(), block_size_ * i, block_size_);
      handles.push_back(std::move(h));
    }

    auto now = std::chrono::system_clock::now();
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(1), 1024);
    auto old_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now() - now)
                           .count();
    LOG_INFO("latency: ` ns", old_latency);

    now = std::chrono::system_clock::now();
    ASSERT_EQ(cache_->retention_manager_.get_max_covered_end(1), 1024);
    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::system_clock::now() - now)
                       .count();
    LOG_INFO("latency: ` ns", latency);

    ASSERT_LT(latency, old_latency);
  }

  void verify_eviction() {
    // fake empty blocks
    std::vector<char *> blocks;
    for (int i = 1023; i >= 0; i--) {
      blocks.push_back(reinterpret_cast<char *>(i));
    }
    cache_->expand_blocks(blocks);

    ASSERT_EQ(fill_cache_range(0, block_size_ * 1024), 0);
    auto h1 = create_test_handle(cache_.get());
    auto h2 = create_test_handle(cache_.get());
    auto h3 = create_test_handle(cache_.get());
    auto h4 = create_test_handle(cache_.get());
    auto h5 = create_test_handle(cache_.get());
    auto h6 = create_test_handle(cache_.get());

    // set retention ranges:
    // [37, 124] [65, 127] [129, 244] [256, 439] [439, 777] [456, 665]
    cache_->update_retention_range(h1.get(), 37 * block_size_,
                                   (1 + 124 - 37) * block_size_);
    cache_->update_retention_range(h2.get(), 439 * block_size_,
                                   (1 + 777 - 439) * block_size_);
    cache_->update_retention_range(h3.get(), 129 * block_size_,
                                   (1 + 244 - 129) * block_size_);
    cache_->update_retention_range(h4.get(), 256 * block_size_,
                                   (1 + 439 - 256) * block_size_);
    cache_->update_retention_range(h5.get(), 65 * block_size_,
                                   (1 + 127 - 65) * block_size_);
    cache_->update_retention_range(h6.get(), 456 * block_size_,
                                   (1 + 665 - 456) * block_size_);

    for (auto &range : cache_->retention_manager_.ranges) {
      LOG_INFO("range: [` - `]", range.start, range.end);
    }

    std::vector<char *> expected_evicted_blocks;
    // P1
    for (int i = 0; i < 37; i++) {
      expected_evicted_blocks.push_back(reinterpret_cast<char *>(i));
    }

    // P2
    for (int i = 1023; i >= 778; i--) {
      expected_evicted_blocks.push_back(reinterpret_cast<char *>(i));
    }

    // P3-1
    expected_evicted_blocks.push_back(reinterpret_cast<char *>(128));

    // P3-2
    for (int i = 245; i <= 255; i++) {
      expected_evicted_blocks.push_back(reinterpret_cast<char *>(i));
    }

    // P4
    for (int i = 37; i <= 127; i++) {
      expected_evicted_blocks.push_back(reinterpret_cast<char *>(i));
    }
    for (int i = 129; i <= 244; i++) {
      expected_evicted_blocks.push_back(reinterpret_cast<char *>(i));
    }
    for (int i = 256; i <= 777; i++) {
      expected_evicted_blocks.push_back(reinterpret_cast<char *>(i));
    }

    // verify
    for (auto &block : expected_evicted_blocks) {
      BlockInfo *evicted_block = nullptr;
      int r = cache_->meta_store_->evict_one_lock_held(&evicted_block);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(evicted_block->mem, block);
      // Free the evicted block to avoid memory leak
      delete evicted_block;
    }

    ASSERT_EQ(fill_cache_range(0, block_size_ * 1), -ENOSPC);
  };

  void verify_eviction_with_random_ranges() {
    const uint64_t kMaxBlockId = 15000;
    const int kNumRanges = 128;
    const int kTotalBlocks = 16384;

    // 1. Prepare resource pool
    std::vector<char *> blocks;
    for (int i = kTotalBlocks - 1; i >= 0; i--) {
      blocks.push_back(reinterpret_cast<char *>(i));
    }
    cache_->expand_blocks(blocks);

    // 2. Fill all blocks
    ASSERT_EQ(fill_cache_range(0, kTotalBlocks * block_size_), 0);

    // 3. Randomly generate Retention Ranges
    std::vector<std::unique_ptr<BlockCacheHandle>> handles;
    handles.reserve(kNumRanges);

    for (int i = kNumRanges - 1; i >= 0; i--) {
      auto h = create_test_handle(cache_.get());
      auto [s, e] = generate_random_range(kMaxBlockId, 200);
      cache_->update_retention_range(h.get(), s * block_size_,
                                     (e - s + 1) * block_size_);
      handles.push_back(std::move(h));
    }

    // 4. Get boundaries
    uint64_t global_min_start = cache_->retention_manager_.get_min_start();
    uint64_t global_max_end = cache_->retention_manager_.get_max_end();

    LOG_INFO("Global Min Start: `", global_min_start);
    LOG_INFO("Global Max End: `", global_max_end);

    // 5. Perform multiple evictions
    int total_evictions = 0;
    int last_priority = 1;

    while (true) {
      BlockInfo *evicted_info = nullptr;
      int r = cache_->meta_store_->evict_one_lock_held(&evicted_info);

      if (r == -ENOSPC) break;

      ASSERT_EQ(r, 0);
      ASSERT_NE(evicted_info, nullptr);

      uint64_t evicted_bid = reinterpret_cast<uint64_t>(evicted_info->mem);
      total_evictions++;

      // Free the evicted block to avoid memory leak
      delete evicted_info;

      auto covered_end_opt =
          cache_->retention_manager_.get_max_covered_end(evicted_bid);
      bool is_covered_by_any = covered_end_opt.has_value() &&
                               (evicted_bid <= covered_end_opt.value());

      bool is_p1 = (evicted_bid < global_min_start);
      bool is_p2 = (evicted_bid > global_max_end);
      bool is_p3 = (!is_p1 && !is_p2 && !is_covered_by_any);
      int current_priority = is_p1 ? 1 : (is_p2 ? 2 : (is_p3 ? 3 : 4));

      ASSERT_GE(current_priority, last_priority)
          << "Priority violation: Evicted P" << current_priority << " after P"
          << last_priority << ". Block ID: " << evicted_bid;

      last_priority = current_priority;
    }

    LOG_INFO("Random Eviction Test Passed. Total evictions: `",
             total_evictions);
    ASSERT_GT(total_evictions, 0);
  };

 private:
  int fill_cache_range(off_t offset, size_t count) {
    IOVector blocks;
    int r = cache_->try_lock_blocks(offset, count, blocks);
    if (r < 0) {
      return r;
    }

    cache_->unlock_blocks(offset, count);
    return 0;
  }

  // Helper: Generate a random range [start, end] (unit: block_id)
  std::pair<uint64_t, uint64_t> generate_random_range(uint64_t max_id,
                                                      uint64_t max_len) {
    std::uniform_int_distribution<uint64_t> dist_start(0, max_id);
    uint64_t start = dist_start(rng_);
    // Ensure length is at least 1 and does not exceed max_id
    std::uniform_int_distribution<uint64_t> dist_len(
        1, std::min(std::max(1UL, max_id - start + 1), max_len));
    uint64_t len = dist_len(rng_);
    return {start, start + len - 1};
  }

  void clear_cache() {
    // BlockCacheStore doesn't have a clear method, blocks are managed by
    // BlockPool. The leaked blocks are from eviction - they need to be freed
    // by caller.
  }

  uint64_t block_size_;
  std::unique_ptr<BlockCacheStore> cache_;
  std::mt19937_64 rng_;
};

TEST_F(BlockCacheStoreTest, verify_retention_range) {
  verify_retention_range();
};

TEST_F(BlockCacheStoreTest, verify_retention_range_adaptive_resort) {
  verify_retention_range_adaptive_resort();
};

TEST_F(BlockCacheStoreTest, verify_eviction) {
  verify_eviction();
};

TEST_F(BlockCacheStoreTest, verify_eviction_with_random_ranges) {
  verify_eviction_with_random_ranges();
}
