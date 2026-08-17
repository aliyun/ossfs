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

#include <unistd.h>

#include <cstdint>
#include <string>
#include <unordered_set>

#include "common/macros.h"

namespace OssFileSystem {

// All callers hold the owning inode's wlock externally.
class ChunkMap {
 public:
  explicit ChunkMap(uint64_t chunk_size) : chunk_size_(chunk_size) {}

  uint64_t chunk_size() const {
    return chunk_size_;
  }

  bool is_dirty(uint64_t cid) const {
    return dirty_chunks_.count(cid) != 0;
  }

  // Range: [begin, end). Returns true if any dirty chunk overlaps the range.
  bool is_range_dirty(uint64_t begin, uint64_t end) const {
    auto start_chunk = begin / chunk_size_;
    auto end_chunk = (end + chunk_size_ - 1) / chunk_size_;
    for (auto cid = start_chunk; cid < end_chunk; ++cid) {
      if (is_dirty(cid)) return true;
    }
    return false;
  }

  void mark_dirty(uint64_t cid) {
    dirty_chunks_.insert(cid);
  }

  size_t dirty_chunk_count() const {
    return dirty_chunks_.size();
  }

  void erase_above_chunk(uint64_t new_size) {
    const uint64_t threshold = (new_size + chunk_size_ - 1) / chunk_size_;
    for (auto it = dirty_chunks_.begin(); it != dirty_chunks_.end();) {
      it = (*it >= threshold) ? dirty_chunks_.erase(it) : std::next(it);
    }
  }

  void clear() {
    dirty_chunks_.clear();
  }

 private:
  const uint64_t chunk_size_;
  std::unordered_set<uint64_t> dirty_chunks_;
};

// All access to RandomWriteContext is serialized by the owning inode's
// lock; no internal synchronization needed.
struct RandomWriteContext {
  explicit RandomWriteContext(uint64_t chunk_size) : chunks(chunk_size) {}
  RandomWriteContext(const RandomWriteContext &) = delete;
  RandomWriteContext &operator=(const RandomWriteContext &) = delete;
  ~RandomWriteContext() {
    if (staging_fd >= 0) ::close(staging_fd);
  }

  int staging_fd = -1;
  ChunkMap chunks;
  uint64_t remote_size = 0;
  std::string upload_path;  // refreshed on first dirty write after rename
  int ref_count = 0;
  // Cached st_blocks bytes of the staging file. Every site that mutates the
  // staging file (under the inode wlock) refreshes it, so "old" values for
  // disk-usage accounting read the cache instead of issuing an extra fstat.
  int64_t staging_disk_bytes = 0;
};

}  // namespace OssFileSystem
