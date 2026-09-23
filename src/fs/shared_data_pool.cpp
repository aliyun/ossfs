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

#include "shared_data_pool.h"

#include <photon/common/utility.h>

#include "common/logger.h"
#include "common/macros.h"

namespace OssFileSystem {
SharedDataBufferPool::SharedDataBufferPool(size_t block_size,
                                           size_t pool_capacity,
                                           size_t max_cached_blocks,
                                           uint64_t purge_interval_ms,
                                           size_t read_quota_blocks)
    : FixedBlockMemoryPool(block_size, pool_capacity, max_cached_blocks,
                           purge_interval_ms),
      read_quota_blocks_(read_quota_blocks) {}

std::vector<char *> SharedDataBufferPool::try_allocate(size_t count,
                                                       bool ignore_limit) {
  SCOPED_LOCK(lock_);
  // Unlimited read (read_quota_blocks_ == SIZE_MAX) plus held write buffers
  // would wrap the bounded-read limit around to a tiny value and starve reads;
  // sat_add saturates to SIZE_MAX instead of overflowing.
  const size_t limit = ignore_limit
                           ? std::numeric_limits<size_t>::max()
                           : photon::sat_add(read_quota_blocks_, write_used_);
  return take_blocks_locked(count, limit);
}

std::vector<char *> SharedDataBufferPool::allocate_write(size_t count) {
  std::vector<char *> addresses;
  size_t used_snapshot = 0;
  size_t write_snapshot = 0;
  {
    SCOPED_LOCK(lock_);
    addresses = take_blocks_locked(count, std::numeric_limits<size_t>::max());
    write_used_ += addresses.size();
    used_snapshot = used_;
    write_snapshot = write_used_;
  }

  if (pool_capacity_ != std::numeric_limits<size_t>::max() &&
      used_snapshot > pool_capacity_) {
    // clang-format off
    LOG_EVERY_N(100, ALOG_WARN,
        "write path holds ` blocks, pool used ` exceeds capacity `, memory is over budget",
        write_snapshot, used_snapshot, pool_capacity_);
    // clang-format on
  }
  return addresses;
}

// write_used_ and the block return must happen under one lock.
void SharedDataBufferPool::deallocate_write(
    const std::vector<char *> &addresses) {
  size_t free_cnt = 0;
  {
    SCOPED_LOCK(lock_);
    RELEASE_ASSERT(write_used_ >= addresses.size());
    write_used_ -= addresses.size();
    free_cnt = return_blocks_locked(addresses);
  }
  free_excess(addresses, free_cnt);
}

size_t SharedDataBufferPool::write_used_blocks() {
  SCOPED_LOCK(lock_);
  return write_used_;
}

double SharedDataBufferPool::read_usage_ratio() {
  if (read_quota_blocks_ == 0 ||
      read_quota_blocks_ == std::numeric_limits<size_t>::max()) {
    return 0.0;
  }
  SCOPED_LOCK(lock_);
  return static_cast<double>(used_ - write_used_) / read_quota_blocks_;
}

};  // namespace OssFileSystem
