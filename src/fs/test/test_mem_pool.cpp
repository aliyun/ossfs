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

#include "fs/mem_pool.h"
#include "fs/shared_data_pool.h"
#include "test_suite.h"

class FixedMemoryPoolTest : public Ossfs2TestSuite {
 protected:
  void verify_allocate() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 10;
    const size_t max_cached_blocks = 5;

    FixedBlockMemoryPool pool(block_size, pool_capacity, max_cached_blocks, 0);

    // Test allocating a single block
    auto blocks1 = pool.allocate(1);
    ASSERT_EQ(blocks1.size(), 1ULL);
    ASSERT_NE(blocks1[0], nullptr);
    ASSERT_EQ(pool.used_blocks(), 1ULL);

    // Test allocating multiple blocks
    auto blocks2 = pool.allocate(3);
    ASSERT_EQ(blocks2.size(), 3ULL);
    ASSERT_EQ(pool.used_blocks(), 4ULL);

    // Test that allocated blocks are not nullptr
    for (auto block : blocks2) {
      ASSERT_NE(block, nullptr);
    }

    // Test allocating more blocks than available in capacity
    // allocate should ignore capacity limits, so this should succeed
    auto blocks3 =
        pool.allocate(pool_capacity - 4);  // 6 more blocks to reach capacity
    ASSERT_EQ(blocks3.size(), pool_capacity - 4);
    ASSERT_EQ(pool.used_blocks(), pool_capacity);

    // Attempting to allocate beyond capacity should still work since allocate
    // ignores limits
    auto blocks4 = pool.allocate(
        5);  // This would exceed capacity but should still allocate
    ASSERT_EQ(blocks4.size(), 5ULL);
    ASSERT_EQ(pool.used_blocks(), pool_capacity + 5);

    // Deallocate some blocks to test deallocation
    auto old_used = pool.used_blocks();
    std::vector<char *> to_deallocate = {blocks1[0], blocks2[0]};
    pool.deallocate(to_deallocate);
    ASSERT_EQ(pool.used_blocks(), old_used - 2);

    pool.deallocate({blocks2[1], blocks2[2]});
    pool.deallocate(blocks3);
    pool.deallocate(blocks4);
  }

  void verify_try_allocate() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 5;
    const size_t max_cached_blocks = 3;

    FixedBlockMemoryPool pool(block_size, pool_capacity, max_cached_blocks, 0);

    // Test try_allocate with capacity limits respected
    auto blocks1 = pool.try_allocate(3);
    ASSERT_EQ(blocks1.size(), 3ULL);
    ASSERT_EQ(pool.used_blocks(), 3ULL);

    // Test try_allocate that would exceed capacity - should return fewer blocks
    auto blocks2 = pool.try_allocate(
        5);  // Request 5, but only 2 available within capacity
    ASSERT_EQ(blocks2.size(),
              2ULL);  // Only 2 blocks should be allocated due to capacity limit
    ASSERT_EQ(pool.used_blocks(), 5ULL);  // Total used is now at capacity

    // Further allocation attempt should return empty vector since capacity is
    // reached
    auto blocks3 = pool.try_allocate(1);
    ASSERT_EQ(blocks3.size(),
              0ULL);  // No blocks allocated due to capacity limit
    ASSERT_EQ(pool.used_blocks(), 5ULL);

    // Deallocate some blocks to allow further allocation
    std::vector<char *> to_deallocate = {blocks1[0],
                                         blocks1[1]};  // Deallocate 2 blocks
    pool.deallocate(to_deallocate);
    ASSERT_EQ(pool.used_blocks(),
              3ULL);  // After deallocating 2 blocks from 5, should have 3 used

    // Now try allocation again - should succeed up to available capacity
    auto blocks4 = pool.try_allocate(2);
    ASSERT_EQ(blocks4.size(), 2ULL);
    ASSERT_EQ(pool.used_blocks(), 5ULL);  // Back to full capacity

    // Test try_allocate with ignore_limit = true (should behave like allocate)
    auto blocks5 = pool.try_allocate(3, true);  // ignore limit
    ASSERT_EQ(blocks5.size(),
              3ULL);  // Should allocate 3 more blocks even beyond capacity
    ASSERT_EQ(pool.used_blocks(), 8ULL);  // Total is now beyond capacity

    // Free allocated blocks
    pool.deallocate({blocks1[2]});
    pool.deallocate(blocks2);
    pool.deallocate(blocks3);
    pool.deallocate(blocks4);
    pool.deallocate(blocks5);
  }

  void verify_purger() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 5;
    const size_t max_cached_blocks = 3;

    FixedBlockMemoryPool pool(block_size, pool_capacity, max_cached_blocks,
                              2000);

    ASSERT_EQ(pool.used_blocks(), 0ULL);
    for (int i = 0; i < 100; i++) {
      auto blocks = pool.allocate(3);
      ASSERT_EQ(blocks.size(), 3ULL);
      ASSERT_EQ(pool.used_blocks(), 3ULL);
      pool.deallocate(blocks);

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      ASSERT_EQ(pool.used_blocks(), 0ULL);
      ASSERT_EQ(pool.cached_block_list_.size(), 3ULL);
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_EQ(pool.cached_block_list_.size(), 0ULL);
  }

  void verify_get_usage_ratio() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 10;
    const size_t max_cached_blocks = 5;

    // Test empty pool
    FixedBlockMemoryPool pool1(block_size, pool_capacity, max_cached_blocks, 0);
    ASSERT_DOUBLE_EQ(pool1.get_usage_ratio(), 0.0);

    // Test half-full pool
    FixedBlockMemoryPool pool2(block_size, pool_capacity, max_cached_blocks, 0);
    auto blocks = pool2.allocate(5);
    ASSERT_EQ(pool2.used_blocks(), 5ULL);
    ASSERT_DOUBLE_EQ(pool2.get_usage_ratio(), 0.5);
    pool2.deallocate(blocks);

    // Test full pool
    FixedBlockMemoryPool pool3(block_size, pool_capacity, max_cached_blocks, 0);
    auto blocks2 = pool3.allocate(pool_capacity);
    ASSERT_EQ(pool3.used_blocks(), pool_capacity);
    ASSERT_DOUBLE_EQ(pool3.get_usage_ratio(), 1.0);
    pool3.deallocate(blocks2);

    // Test pool with usage exceeding capacity
    FixedBlockMemoryPool pool4(block_size, pool_capacity, max_cached_blocks, 0);
    auto blocks3 = pool4.allocate(pool_capacity * 2);
    ASSERT_EQ(pool4.used_blocks(), pool_capacity * 2);
    ASSERT_DOUBLE_EQ(pool4.get_usage_ratio(), 2.0);
    pool4.deallocate(blocks3);

    // Test pool with zero capacity (unlimited)
    FixedBlockMemoryPool pool5(block_size, std::numeric_limits<size_t>::max(),
                               max_cached_blocks, 0);
    auto blocks4 = pool5.allocate(5);
    ASSERT_DOUBLE_EQ(pool5.get_usage_ratio(), 0.0);
    pool5.deallocate(blocks4);
  }

  void verify_read_quota_independent_of_writes() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 10;
    const size_t read_quota_blocks = 4;

    SharedDataBufferPool pool(block_size, pool_capacity, pool_capacity, 0,
                              read_quota_blocks);

    auto write_blocks = pool.allocate_write(pool_capacity * 3);
    ASSERT_EQ(pool.write_used_blocks(), pool_capacity * 3);
    ASSERT_EQ(pool.used_blocks(), pool_capacity * 3);

    auto blocks = pool.try_allocate(read_quota_blocks);
    ASSERT_EQ(blocks.size(), read_quota_blocks);
    ASSERT_TRUE(pool.try_allocate(1).empty());

    pool.deallocate(blocks);
    pool.deallocate_write(write_blocks);
    ASSERT_EQ(pool.used_blocks(), 0ULL);
  }

  void verify_write_never_blocked_by_reads() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 10;
    const size_t read_quota_blocks = 8;

    SharedDataBufferPool pool(block_size, pool_capacity, pool_capacity, 0,
                              read_quota_blocks);

    auto blocks = pool.try_allocate(read_quota_blocks);
    ASSERT_EQ(blocks.size(), read_quota_blocks);
    ASSERT_TRUE(pool.try_allocate(1).empty());
    ASSERT_EQ(pool.cached_block_list_.size(), 0ULL);

    auto write_blocks = pool.allocate_write(pool_capacity);
    ASSERT_EQ(write_blocks.size(), pool_capacity);
    ASSERT_EQ(pool.write_used_blocks(), pool_capacity);
    ASSERT_EQ(pool.used_blocks(), read_quota_blocks + pool_capacity);

    pool.deallocate_write(write_blocks);
    pool.deallocate(blocks);
    ASSERT_EQ(pool.used_blocks(), 0ULL);
  }

  void verify_read_quota_cap() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 12;
    const size_t read_quota_blocks = 8;

    SharedDataBufferPool pool(block_size, pool_capacity, pool_capacity, 0,
                              read_quota_blocks);

    auto blocks = pool.try_allocate(pool_capacity);
    ASSERT_EQ(blocks.size(), read_quota_blocks);
    ASSERT_TRUE(pool.try_allocate(1).empty());

    auto write_blocks = pool.allocate_write(4);
    ASSERT_EQ(write_blocks.size(), 4ULL);
    ASSERT_EQ(pool.used_blocks(), 12ULL);

    ASSERT_TRUE(pool.try_allocate(1).empty());

    auto fg = pool.try_allocate(1, true);
    ASSERT_EQ(fg.size(), 1ULL);
    pool.deallocate(fg);

    pool.deallocate(blocks);
    auto again = pool.try_allocate(3);
    ASSERT_EQ(again.size(), 3ULL);

    pool.deallocate(again);
    pool.deallocate_write(write_blocks);
    ASSERT_EQ(pool.used_blocks(), 0ULL);
  }

  void verify_usage_ratio_excludes_write_share() {
    const size_t block_size = 1024;
    const size_t pool_capacity = 12;
    const size_t read_quota_blocks = 8;

    SharedDataBufferPool pool(block_size, pool_capacity, pool_capacity, 0,
                              read_quota_blocks);
    ASSERT_DOUBLE_EQ(pool.read_usage_ratio(), 0.0);

    auto reads = pool.try_allocate(read_quota_blocks / 2);
    ASSERT_EQ(reads.size(), read_quota_blocks / 2);
    ASSERT_DOUBLE_EQ(pool.read_usage_ratio(), 0.5);

    auto writes = pool.allocate_write(pool_capacity);
    ASSERT_EQ(pool.used_blocks(), read_quota_blocks / 2 + pool_capacity);
    ASSERT_DOUBLE_EQ(pool.read_usage_ratio(), 0.5);

    auto more = pool.try_allocate(read_quota_blocks - read_quota_blocks / 2);
    ASSERT_EQ(more.size(), read_quota_blocks - read_quota_blocks / 2);
    ASSERT_DOUBLE_EQ(pool.read_usage_ratio(), 1.0);

    pool.deallocate(more);
    pool.deallocate(reads);
    pool.deallocate_write(writes);
    ASSERT_EQ(pool.used_blocks(), 0ULL);

    SharedDataBufferPool unlimited(
        block_size, std::numeric_limits<size_t>::max(), pool_capacity, 0,
        std::numeric_limits<size_t>::max());
    auto any = unlimited.allocate(4);
    ASSERT_EQ(any.size(), 4ULL);
    ASSERT_DOUBLE_EQ(unlimited.read_usage_ratio(), 0.0);
    unlimited.deallocate(any);
  }

  // Regression: unlimited read (read_quota == SIZE_MAX) with concurrent write
  // borrowing must not overflow the bounded-read limit. Before the fix,
  // read_quota_blocks_ + write_used_ wrapped to a tiny value and starved reads.
  void verify_unlimited_read_not_starved_by_writes() {
    const size_t block_size = 1024;
    const size_t max_cached_blocks = 16;
    SharedDataBufferPool pool(block_size, std::numeric_limits<size_t>::max(),
                              max_cached_blocks, 0,
                              std::numeric_limits<size_t>::max());

    auto writes = pool.allocate_write(8);
    ASSERT_EQ(writes.size(), 8ULL);

    // Bounded read (ignore_limit = false) stays unbounded under unlimited read.
    auto reads = pool.try_allocate(100);
    ASSERT_EQ(reads.size(), 100ULL);

    pool.deallocate(reads);
    pool.deallocate_write(writes);
    ASSERT_EQ(pool.used_blocks(), 0ULL);
  }
};

TEST_F(FixedMemoryPoolTest, verify_allocate) {
  verify_allocate();
};

TEST_F(FixedMemoryPoolTest, verify_try_allocate) {
  verify_try_allocate();
}

TEST_F(FixedMemoryPoolTest, verify_purger) {
  verify_purger();
}

TEST_F(FixedMemoryPoolTest, verify_get_usage_ratio) {
  verify_get_usage_ratio();
}

TEST_F(FixedMemoryPoolTest, verify_read_quota_independent_of_writes) {
  verify_read_quota_independent_of_writes();
}

TEST_F(FixedMemoryPoolTest, verify_write_never_blocked_by_reads) {
  verify_write_never_blocked_by_reads();
}

TEST_F(FixedMemoryPoolTest, verify_read_quota_cap) {
  verify_read_quota_cap();
}

TEST_F(FixedMemoryPoolTest, verify_usage_ratio_excludes_write_share) {
  verify_usage_ratio_excludes_write_share();
}

TEST_F(FixedMemoryPoolTest, verify_unlimited_read_not_starved_by_writes) {
  verify_unlimited_read_not_starved_by_writes();
}
