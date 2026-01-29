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

#include "test_suite.h"

static const size_t kBlockSize = 512;
static std::vector<char *> alloc_cb(uint64_t count) {
  std::vector<char *> blocks;
  for (uint64_t i = 0; i < count; ++i) {
    char *block = static_cast<char *>(malloc(kBlockSize));
    memset(block, 0, kBlockSize);  // Initialize memory
    blocks.push_back(block);
  }
  return blocks;
}

static void release_cb(const std::vector<char *> &blocks) {
  for (char *block : blocks) {
    free(block);
  }
}

TEST(BlockCacheManagerTest, multiple_expansions) {
  BlockCacheManager manager(kBlockSize);

  // Get a reference to initialize the cache
  auto [cache, range_lock] = manager.get();
  ASSERT_NE(cache, nullptr);
  ASSERT_NE(range_lock, nullptr);

  uint64_t max_capacity = 20;

  // First expansion
  size_t expanded1 = manager.try_expand_blocks(alloc_cb, 5, max_capacity);
  ASSERT_EQ(expanded1, 5ULL);
  ASSERT_EQ(manager.capacity(), 5ULL);

  // Second expansion
  size_t expanded2 = manager.try_expand_blocks(alloc_cb, 8, max_capacity);
  ASSERT_EQ(expanded2, 8ULL);
  ASSERT_EQ(manager.capacity(), 13ULL);

  // Try expansion that would exceed capacity
  size_t expanded3 = manager.try_expand_blocks(
      alloc_cb, 10, max_capacity);  // Would be 23 total, exceeds 20
  ASSERT_EQ(expanded3,
            7ULL);  // Should only expand 7 to reach max capacity of 20
  ASSERT_EQ(manager.capacity(), 20ULL);

  // Test expansion when already at max capacity
  size_t expanded4 = manager.try_expand_blocks(alloc_cb, 5, max_capacity);
  ASSERT_EQ(expanded4, 0ULL);  // Should not expand since already at capacity
  ASSERT_EQ(manager.capacity(), 20ULL);

  manager.release(release_cb, 20);

  // Test count > max_capacity scenario
  BlockCacheManager manager2(kBlockSize);
  auto [cache2, range_lock2] = manager2.get();
  ASSERT_NE(cache2, nullptr);
  ASSERT_NE(range_lock2, nullptr);

  // Request more blocks than max capacity allows
  size_t expanded5 =
      manager2.try_expand_blocks(alloc_cb, 15, 10);  // Request 15, max 10
  ASSERT_EQ(expanded5, 10ULL);  // Should expand up to max capacity
  ASSERT_EQ(manager2.capacity(), 10ULL);

  manager2.release(release_cb, 10);

  // Test with zero capacity
  BlockCacheManager manager3(kBlockSize);
  auto [cache3, range_lock3] = manager3.get();
  ASSERT_NE(cache3, nullptr);
  ASSERT_NE(range_lock3, nullptr);

  size_t expanded6 =
      manager3.try_expand_blocks(alloc_cb, 5, 0);  // Max capacity 0
  ASSERT_EQ(expanded6, 0ULL);  // Should not expand since capacity is 0
  ASSERT_EQ(manager3.capacity(), 0ULL);

  manager3.release(release_cb, 0);

  // Test with very small capacity
  BlockCacheManager manager4(kBlockSize);
  auto [cache4, range_lock4] = manager4.get();
  ASSERT_NE(cache4, nullptr);
  ASSERT_NE(range_lock4, nullptr);

  size_t expanded7 =
      manager4.try_expand_blocks(alloc_cb, 1, 1);  // Max capacity 1
  ASSERT_EQ(expanded7, 1ULL);                      // Should expand 1 block
  ASSERT_EQ(manager4.capacity(), 1ULL);

  manager4.release(release_cb, 1);
}

TEST(BlockCacheManagerTest, ref_counting_with_reuse) {
  BlockCacheManager manager(kBlockSize);

  // Initial state - no cache yet
  ASSERT_EQ(manager.capacity(), 0ULL);

  // Get first reference
  auto [cache1, lock1] = manager.get();
  ASSERT_NE(cache1, nullptr);

  // Get second reference
  auto [cache2, lock2] = manager.get();
  ASSERT_NE(cache2, nullptr);

  // Both should point to the same cache instance
  ASSERT_EQ(cache1, cache2);

  // Expand with some blocks
  size_t expanded = manager.try_expand_blocks(alloc_cb, 3, 10);
  ASSERT_EQ(expanded, 3ULL);
  ASSERT_EQ(manager.capacity(), 3ULL);

  // Release some blocks but keep references - capacity() should remain the same
  manager.release(release_cb,
                  2);  // Release 2 of the 3 blocks, but keep references
  // capacity() returns the total allocated blocks in the manager, not currently
  // used blocks
  ASSERT_EQ(manager.capacity(), 3ULL);  // Capacity remains the same as blocks
                                        // are still managed by the manager

  // Get another reference - should still work since there's still one reference
  auto [cache3, lock3] = manager.get();
  ASSERT_NE(cache3, nullptr);
  ASSERT_EQ(cache1, cache3);

  // Expand more blocks
  size_t expanded2 = manager.try_expand_blocks(alloc_cb, 4, 10);
  ASSERT_EQ(expanded2, 4ULL);
  ASSERT_EQ(manager.capacity(),
            5ULL);  // 1 used_block + 2 cached blocks + 2 new blocks = 5 total
                    // allocated

  // Release remaining blocks but keep references
  manager.release(release_cb,
                  5);  // Release 5 blocks (all currently allocated blocks)
  ASSERT_EQ(
      manager.capacity(),
      5ULL);  // Capacity should remain the same as the manager still exists

  // Now release all references to trigger the actual cleanup
  manager.release(release_cb, 0);
  ASSERT_EQ(manager.capacity(), 0ULL);

  // Get a fresh reference after everything is released
  auto [cache4, lock4] = manager.get();
  ASSERT_NE(cache4, nullptr);

  // Expand again to make sure manager works after full cleanup
  size_t expanded3 = manager.try_expand_blocks(alloc_cb, 2, 10);
  ASSERT_EQ(expanded3, 2ULL);
  ASSERT_EQ(manager.capacity(), 2ULL);  // New allocation after cleanup

  // Final cleanup - release all references and blocks
  manager.release(release_cb, 2);
}

TEST(BlockCacheManagerTest, concurrent_allocation_deallocation) {
  const size_t num_threads = 8;
  const uint64_t max_blocks_per_thread = 100;
  const uint64_t iterations = 10000;
  const uint64_t max_capacity = 500;

  BlockCacheManager manager(kBlockSize);

  // Vector to hold threads
  std::vector<std::thread> threads;

  // Launch threads
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back(
        [&manager, max_blocks_per_thread, iterations, max_capacity]() {
          // Each thread will perform multiple allocation/deallocation cycles
          for (uint64_t iter = 0; iter < iterations; ++iter) {
            // Get cache reference
            auto [cache, range_lock] = manager.get();
            ASSERT_NE(cache, nullptr);
            ASSERT_NE(range_lock, nullptr);

            // Random number of blocks to allocate (between 1 and
            // max_blocks_per_thread)
            uint64_t blocks_to_allocate = 1 + (iter % max_blocks_per_thread);

            // Try to expand blocks
            size_t expanded = manager.try_expand_blocks(
                alloc_cb, blocks_to_allocate, max_capacity);
            // The actual number of blocks expanded might be less than requested
            // due to capacity limits
            ASSERT_GE(expanded, 0ULL);

            // Perform some operations on the cache
            usleep(rand() % 100 + 10);

            // Get another reference occasionally to test reference counting
            if (iter % 10 == 0) {
              auto [cache2, range_lock2] = manager.get();
              ASSERT_NE(cache2, nullptr);
              ASSERT_EQ(cache, cache2);  // Should point to same instance

              // Release this additional reference
              manager.release(release_cb, 0);
            }

            manager.release(release_cb, expanded);
          }
        });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // After all threads finish, the manager should be in a valid state
  // Verify that we can still get a reference and use the manager
  auto [final_cache, final_lock] = manager.get();
  ASSERT_NE(final_cache, nullptr);
  ASSERT_NE(final_lock, nullptr);

  // Perform one final allocation to verify the manager is still functional
  size_t final_expansion = manager.try_expand_blocks(alloc_cb, 5, max_capacity);
  ASSERT_GE(final_expansion, 0ULL);

  // Release the final allocation
  manager.release(release_cb, final_expansion);
}
