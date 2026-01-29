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

#include "mem_pool.h"

#include <cstdlib>
#include <cstring>

#include "common/fault_injector.h"
#include "common/macros.h"

namespace OssFileSystem {
FixedBlockMemoryPool::FixedBlockMemoryPool(size_t block_size,
                                           size_t pool_capacity,
                                           size_t max_cached_blocks)
    : block_size_(block_size),
      pool_capacity_(pool_capacity),
      max_cached_blocks_(max_cached_blocks) {}
FixedBlockMemoryPool::~FixedBlockMemoryPool() {
  for (char *block : cached_block_list_) {
    free(block);
  }
}

std::vector<char *> FixedBlockMemoryPool::allocate(size_t count) {
  return try_allocate(count, true);
}

std::vector<char *> FixedBlockMemoryPool::try_allocate(size_t count,
                                                       bool ignore_limit) {
  const size_t limit =
      ignore_limit ? std::numeric_limits<size_t>::max() : pool_capacity_;
  SCOPED_LOCK(lock_);

  std::vector<char *> addresses;
  for (size_t i = 0; i < count; ++i) {
    if (used_ >= limit) break;
    ++used_;
    if (cached_block_list_.empty()) {
      addresses.push_back(expand_one());
      continue;
    }
    addresses.push_back(cached_block_list_.back());
    cached_block_list_.pop_back();
  }
  return addresses;
}

void FixedBlockMemoryPool::deallocate(const std::vector<char *> &addresses) {
  size_t free_cnt = 0;
  {
    SCOPED_LOCK(lock_);
    for (char *addr : addresses) {
      if (cached_block_list_.size() < max_cached_blocks_) {
        cached_block_list_.push_back(addr);
        free_cnt++;

        if (ENABLE_TESTS()) {
          memset(addr, 0, block_size_);
        }
      } else {
        break;
      }
    }
    used_ -= addresses.size();
  }
  if (free_cnt < addresses.size()) {
    for (size_t i = free_cnt; i < addresses.size(); ++i) {
      free(addresses[i]);
    }
  }
}

size_t FixedBlockMemoryPool::used_blocks() {
  SCOPED_LOCK(lock_);
  return used_;
}

char *FixedBlockMemoryPool::expand_one() {
  char *ptr = nullptr;
  int r = posix_memalign(reinterpret_cast<void **>(&ptr), 4096, block_size_);
  RELEASE_ASSERT(r == 0);
  return ptr;
}

};  // namespace OssFileSystem
