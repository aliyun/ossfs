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

#include <vector>

namespace OssFileSystem {
// A memory pool for fixed-size memory blocks.
// This class manages a pool of fixed-size memory blocks to reduce the overhead
// of frequent memory allocations and deallocations. It maintains a cache of
// freed blocks for reuse and enforces a soft limit on the pool capacity.
class FixedBlockMemoryPool {
 public:
  FixedBlockMemoryPool(size_t block_size, size_t pool_capacity,
                       size_t max_cached_blocks);
  ~FixedBlockMemoryPool();

  // Allocates a specified number of memory blocks, ignoring pool capacity
  // limits.
  std::vector<char *> allocate(size_t count);

  std::vector<char *> try_allocate(size_t count, bool ignore_limit = false);

  void deallocate(const std::vector<char *> &addresses);

  size_t used_blocks();

  FixedBlockMemoryPool(const FixedBlockMemoryPool &) = delete;
  FixedBlockMemoryPool &operator=(const FixedBlockMemoryPool &) = delete;
  FixedBlockMemoryPool(FixedBlockMemoryPool &&) = delete;
  FixedBlockMemoryPool &operator=(FixedBlockMemoryPool &&) = delete;

 private:
  char *expand_one();

  const size_t block_size_ = 0;

  // Maximum number of blocks in the pool. Set to
  // std::numeric_limits<size_t>::max() means unlimited.
  const size_t pool_capacity_ = 0;

  // Maximum number of free blocks to retain in the pool for reuse.
  // Excess blocks are returned to the OS during deallocation.
  const size_t max_cached_blocks_ = 0;

  photon::spinlock lock_;
  std::vector<char *> cached_block_list_;
  size_t used_ = 0;
};

};  // namespace OssFileSystem
