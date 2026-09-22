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

#include <limits>
#include <vector>

#include "mem_pool.h"

class FixedMemoryPoolTest;

namespace OssFileSystem {
// One fixed-block pool shared by the read cache and the write buffers.
//
// Write blocks must be returned through deallocate_write(): the base
// deallocate() would leave write_used_ stale.
class SharedDataBufferPool : public FixedBlockMemoryPool {
 public:
  SharedDataBufferPool(size_t block_size, size_t pool_capacity,
                       size_t max_cached_blocks, uint64_t purge_interval_ms,
                       size_t read_quota_blocks);

  // Bounded by read_quota_blocks_; ignore_limit lifts that bound.
  std::vector<char *> try_allocate(size_t count,
                                   bool ignore_limit = false) override;

  // Read share against the read quota, not against the whole pool.
  double read_usage_ratio();

  // Exempt from the read quota and from the capacity.
  std::vector<char *> allocate_write(size_t count);

  void deallocate_write(const std::vector<char *> &addresses);

  size_t write_used_blocks();

 private:
  const size_t read_quota_blocks_ = std::numeric_limits<size_t>::max();
  size_t write_used_ = 0;

  friend class ::FixedMemoryPoolTest;
};

};  // namespace OssFileSystem
