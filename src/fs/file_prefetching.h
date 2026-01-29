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

#include <photon/photon.h>
#include <photon/thread/thread.h>
#include <sys/types.h>

#include <atomic>

namespace OssFileSystem {

// photon::http_client only supports less than 28(32 - 4) iovcnt in 0.8 version.
// Our block_size is 1MB, so we limit the max prefetch size to 16MB
// to avoid potential stack overflow.
static const uint64_t kMaxPrefetchSizePerRequest = 16 * 1024 * 1024;

class OssFs;
class FileInode;
class OssAdapter;

// Derived Must be a subclass of Reader and provide:
//  - size_t get_prefetch_alignment();
//  - bool has_enough_space(size_t);
//  - void try_expand_prefetch_window(size_t);
//  - ssize_t bg_try_refill_range(OssAdapter*, off_t, size_t);
template <typename Derived>
class EnableFilePrefetching {
 public:
  struct PrefetchContext {
    EnableFilePrefetching *prefetcher;
    off_t offset = 0;
    int num = 0;
  };

  EnableFilePrefetching(OssFs *fs);
  ~EnableFilePrefetching();

  void do_prefetch(size_t remote_size, off_t offset, size_t count);

  void wait_prefetch_done();

  inline static size_t get_prefetch_buffer_size(size_t prefetch_window_size) {
    return prefetch_window_size / 2 * 3;
  }
  inline static size_t get_prefetch_window_size(size_t prefetch_buffer_size) {
    return prefetch_buffer_size / 3 * 2;
  }

 protected:
  Derived *derived() {
    return static_cast<Derived *>(this);
  }

  ssize_t do_prefetch_range(OssAdapter *oss_client, off_t offset, size_t count);

  void reset_prefetch(off_t remote_size, off_t offset);
  void adjust_next_prefetch_off(off_t remote_size, off_t offset);
  void schedule_prefetch(off_t remote_size);

  static void *prefetch_tsk(void *args);
  static void *do_prefetch_tsk(void *args);

  OssFs *fs_ = nullptr;

  photon::mutex prefetch_mutex_;

  // For sequential read detection.
  static const uint32_t kOffsetDeviation = 2 * 1024 * 1024;
  off_t next_read_off_ = 0;
  int seq_read_cnt_ = 0;

  // For prefetching tasks.
  bool is_prefetching_scheduled_ = false;
  size_t prefetch_window_size_ = 0;
  size_t prefetch_chunk_size_ = 0;
  off_t next_prefetch_off_ = 0;
  size_t next_prefetch_size_ = 0;
  std::atomic<uint64_t> running_download_tasks_ = {0};
};

};  // namespace OssFileSystem
