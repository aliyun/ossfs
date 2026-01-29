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
#include "test_suite.h"

TEST(FixedMemoryPoolTest, allocate) {
  const size_t block_size = 1024;
  const size_t pool_capacity = 10;
  const size_t max_cached_blocks = 5;

  FixedBlockMemoryPool pool(block_size, pool_capacity, max_cached_blocks);

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
  auto blocks4 =
      pool.allocate(5);  // This would exceed capacity but should still allocate
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
};

TEST(FixedMemoryPoolTest, try_allocate) {
  const size_t block_size = 1024;
  const size_t pool_capacity = 5;
  const size_t max_cached_blocks = 3;

  FixedBlockMemoryPool pool(block_size, pool_capacity, max_cached_blocks);

  // Test try_allocate with capacity limits respected
  auto blocks1 = pool.try_allocate(3);
  ASSERT_EQ(blocks1.size(), 3ULL);
  ASSERT_EQ(pool.used_blocks(), 3ULL);

  // Test try_allocate that would exceed capacity - should return fewer blocks
  auto blocks2 =
      pool.try_allocate(5);  // Request 5, but only 2 available within capacity
  ASSERT_EQ(blocks2.size(),
            2ULL);  // Only 2 blocks should be allocated due to capacity limit
  ASSERT_EQ(pool.used_blocks(), 5ULL);  // Total used is now at capacity

  // Further allocation attempt should return empty vector since capacity is
  // reached
  auto blocks3 = pool.try_allocate(1);
  ASSERT_EQ(blocks3.size(), 0ULL);  // No blocks allocated due to capacity limit
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
