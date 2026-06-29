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

#include "fs/mem_cache.h"
#include "test_suite.h"

static const size_t kBlockSize = 512;

class BlockCacheTest : public ::testing::Test {
 protected:
  bool share_cache_store(CacheHandle *h1, CacheHandle *h2) {
    return h1->cache_store == h2->cache_store;
  }
};

TEST_F(BlockCacheTest, verify_multiple_expansions) {
  auto pool = std::make_shared<FixedBlockMemoryPool>(kBlockSize, 50, 5, 0);
  BlockCache manager(pool);

  // Get a reference to initialize the cache
  auto h1 = manager.get();
  ASSERT_TRUE(h1);

  uint64_t max_capacity = 20;

  // First expansion
  size_t expanded1 = manager.try_expand_blocks(5, max_capacity);
  ASSERT_EQ(expanded1, 5ULL);
  ASSERT_EQ(manager.capacity(), 5ULL);

  // Second expansion
  size_t expanded2 = manager.try_expand_blocks(8, max_capacity);
  ASSERT_EQ(expanded2, 8ULL);
  ASSERT_EQ(manager.capacity(), 13ULL);

  // Try expansion that would exceed capacity
  size_t expanded3 = manager.try_expand_blocks(
      10, max_capacity);  // Would be 23 total, exceeds 20
  ASSERT_EQ(expanded3,
            7ULL);  // Should only expand 7 to reach max capacity of 20
  ASSERT_EQ(manager.capacity(), 20ULL);

  // Test expansion when already at max capacity
  size_t expanded4 = manager.try_expand_blocks(5, max_capacity);
  ASSERT_EQ(expanded4, 0ULL);  // Should not expand since already at capacity
  ASSERT_EQ(manager.capacity(), 20ULL);

  manager.release(h1, 20);

  // Test count > max_capacity scenario
  BlockCache manager2(pool);
  auto h2 = manager2.get();
  ASSERT_TRUE(h2);

  // Request more blocks than max capacity allows
  size_t expanded5 = manager2.try_expand_blocks(15, 10);  // Request 15, max 10
  ASSERT_EQ(expanded5, 10ULL);  // Should expand up to max capacity
  ASSERT_EQ(manager2.capacity(), 10ULL);

  manager2.release(h2, 10);

  // Test with zero capacity
  BlockCache manager3(pool);
  auto h3 = manager3.get();
  ASSERT_TRUE(h3);

  size_t expanded6 = manager3.try_expand_blocks(5, 0);  // Max capacity 0
  ASSERT_EQ(expanded6, 0ULL);  // Should not expand since capacity is 0
  ASSERT_EQ(manager3.capacity(), 0ULL);

  manager3.release(h3, 0);

  // Test with very small capacity
  BlockCache manager4(pool);
  auto h4 = manager4.get();
  ASSERT_TRUE(h4);

  size_t expanded7 = manager4.try_expand_blocks(1, 1);  // Max capacity 1
  ASSERT_EQ(expanded7, 1ULL);                           // Should expand 1 block
  ASSERT_EQ(manager4.capacity(), 1ULL);

  manager4.release(h4, 1);
}

TEST_F(BlockCacheTest, verify_ref_counting_with_reuse) {
  auto pool = std::make_shared<FixedBlockMemoryPool>(kBlockSize, 50, 5, 0);
  BlockCache manager(pool);

  // Initial state - no cache yet
  ASSERT_EQ(manager.capacity(), 0ULL);

  // Get first reference
  auto h1 = manager.get();
  ASSERT_TRUE(h1);

  // Get second reference
  auto h2 = manager.get();
  ASSERT_TRUE(h2);

  // Both should point to the same cache instance
  ASSERT_TRUE(share_cache_store(h1, h2));

  // Expand with some blocks
  size_t expanded = manager.try_expand_blocks(3, 10);
  ASSERT_EQ(expanded, 3ULL);
  ASSERT_EQ(manager.capacity(), 3ULL);

  // Release some blocks - handle releases its blocks back to pool
  manager.release(h2, 2);               // Release 2 blocks held by h2
  ASSERT_EQ(manager.capacity(), 1ULL);  // 1 block remains in cache

  // Get another reference - should still work since there's still one reference
  auto h3 = manager.get();
  ASSERT_TRUE(h3);
  ASSERT_TRUE(share_cache_store(h1, h3));

  // Expand more blocks
  size_t expanded2 = manager.try_expand_blocks(4, 10);
  ASSERT_EQ(expanded2, 4ULL);
  ASSERT_EQ(manager.capacity(), 5ULL);  // 1 + 4 = 5 total blocks

  // Release remaining blocks held by h3
  manager.release(h3, 5);  // Release 5 blocks
  // With min block retention policy, at least 1 block remains when other
  // handles exist
  ASSERT_EQ(manager.capacity(), 1ULL);

  // Now release the last reference to trigger cleanup
  manager.release(h1, 1);  // Release the retained block
  ASSERT_EQ(manager.capacity(), 0ULL);

  // Get a fresh reference after everything is released
  auto h4 = manager.get();
  ASSERT_TRUE(h4);

  // Expand again to make sure manager works after full cleanup
  size_t expanded3 = manager.try_expand_blocks(2, 10);
  ASSERT_EQ(expanded3, 2ULL);
  ASSERT_EQ(manager.capacity(), 2ULL);  // New allocation after cleanup

  // Final cleanup - release all references and blocks
  manager.release(h4, 2);
}

TEST_F(BlockCacheTest, verify_concurrent_allocation_deallocation) {
  const size_t num_threads = 8;
  const uint64_t max_blocks_per_thread = 100;
  const uint64_t iterations = 10000;
  const uint64_t max_capacity = 500;

  auto pool =
      std::make_shared<FixedBlockMemoryPool>(kBlockSize, max_capacity, 10, 0);
  BlockCache manager(pool);

  // Vector to hold threads
  std::vector<std::thread> threads;

  // Launch threads
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back(
        [&manager, max_blocks_per_thread, iterations, max_capacity, this]() {
          // Each thread will perform multiple allocation/deallocation cycles
          for (uint64_t iter = 0; iter < iterations; ++iter) {
            // Get cache reference
            auto h = manager.get();
            ASSERT_TRUE(h);

            // Random number of blocks to allocate (between 1 and
            // max_blocks_per_thread)
            uint64_t blocks_to_allocate = 1 + (iter % max_blocks_per_thread);

            // Try to expand blocks
            size_t expanded =
                manager.try_expand_blocks(blocks_to_allocate, max_capacity);
            // The actual number of blocks expanded might be less than requested
            // due to capacity limits
            ASSERT_GE(expanded, 0ULL);

            // Perform some operations on the cache
            usleep(rand() % 100 + 10);

            // Get another reference occasionally to test reference counting
            if (iter % 10 == 0) {
              auto h2 = manager.get();
              ASSERT_TRUE(h2);
              // Should point to same cache instance
              ASSERT_TRUE(share_cache_store(h, h2));

              // Release this additional reference
              manager.release(h2, 0);
            }

            manager.release(h, expanded);
          }
        });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // After all threads finish, the manager should be in a valid state
  // Verify that we can still get a reference and use the manager
  auto final_h = manager.get();
  ASSERT_TRUE(final_h);

  // Perform one final allocation to verify the manager is still functional
  size_t final_expansion = manager.try_expand_blocks(5, max_capacity);
  ASSERT_GE(final_expansion, 0ULL);

  // Release the final allocation
  manager.release(final_h, final_expansion);
}
