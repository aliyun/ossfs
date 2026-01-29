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

#include "file_prefetching.h"

#include "file.h"
#include "fs.h"

namespace OssFileSystem {

template <typename Derived>
EnableFilePrefetching<Derived>::EnableFilePrefetching(OssFs *fs)
    : fs_(fs),
      prefetch_chunk_size_(fs_->options_.prefetch_chunk_size),
      next_prefetch_size_(fs_->options_.prefetch_chunk_size),
      running_download_tasks_(0) {}

template <typename Derived>
EnableFilePrefetching<Derived>::~EnableFilePrefetching() {
  wait_prefetch_done();
}

template <typename Derived>
void EnableFilePrefetching<Derived>::do_prefetch(size_t remote_size,
                                                 off_t offset, size_t count) {
  photon::scoped_lock lock(prefetch_mutex_);

  // Avoid prefetching when reading data in the same range.
  if (static_cast<size_t>(next_read_off_) == offset + count) {
    return;
  }

  // For a file with a small size, always prefetch all data.
  if (static_cast<size_t>(remote_size) <= prefetch_chunk_size_) {
    if (offset == 0) {
      derived()->try_expand_prefetch_window(remote_size);
      if (derived()->has_enough_space(remote_size)) {
        GET_BACKGROUND_OSS_CLIENT_AND_DO_SYNC_FUNC(fs_, do_prefetch_range, 0,
                                                   remote_size);
      }
    }
    return;
  }

  if (unlikely(next_read_off_ + kOffsetDeviation < offset ||
               offset + kOffsetDeviation < next_read_off_)) {
    seq_read_cnt_ = 0;
    reset_prefetch(remote_size, offset);
  } else {
    seq_read_cnt_++;
  }

  next_read_off_ = offset + count;

  if (seq_read_cnt_ >= fs_->options_.seq_read_detect_count) {
    adjust_next_prefetch_off(remote_size, offset);
    schedule_prefetch(remote_size);
  }
}

template <typename Derived>
void EnableFilePrefetching<Derived>::adjust_next_prefetch_off(off_t remote_size,
                                                              off_t offset) {
  bool left = prefetch_window_size_ > 0 &&
              static_cast<size_t>(offset) +
                      get_prefetch_buffer_size(prefetch_window_size_) <
                  static_cast<size_t>(next_prefetch_off_);
  bool right = is_prefetching_scheduled_ && offset > next_prefetch_off_;
  if (unlikely(left || right)) {
    reset_prefetch(remote_size, offset);
  }
}

template <typename Derived>
void EnableFilePrefetching<Derived>::reset_prefetch(off_t remote_size,
                                                    off_t offset) {
  is_prefetching_scheduled_ = false;
  auto alignment = prefetch_chunk_size_;
  next_prefetch_off_ =
      std::min(align_up(offset, alignment), (uint64_t)remote_size);
  next_prefetch_size_ = prefetch_chunk_size_;
  LOG_DEBUG("prefetching reset file: `, next_prefetch_off: `, offset: `",
            static_cast<Derived *>(this)->get_path(), next_prefetch_off_,
            offset);
}

//
//  Prefecthing algorithm:
//  +--------------------+---------------------+---------------------+
//  |        prefetch window(prefetching)      | next prefetch range |
//  +--------------------+---------------------+---------------------+
//   ^
//   |
//  read offset
//
//  next prefetch range = min(prefetch chunk size * prefech concurrency,
//                            prefetch window / 2)
//
//  New prefetching tasks will be generated when we have less data than
//  prefetch window. After prefetching tasks generated, the prefetch window will
//  slide to next like this:
//  +--------------------+---------------------+---------------------+
//  |                    |         prefetch window(prefetching)      |
//  +--------------------+---------------------+---------------------+
//            ^
//            |
//       read offset
//
//  So we need at least prefetch window size / 2 * 3 memory buffer.
//

template <typename Derived>
void EnableFilePrefetching<Derived>::schedule_prefetch(off_t remote_size) {
  derived()->try_expand_prefetch_window(remote_size);
  if (prefetch_window_size_ == 0) return;

  if (is_prefetching_scheduled_ &&
      next_read_off_ + prefetch_window_size_ <=
          static_cast<size_t>(next_prefetch_off_)) {
    return;
  }

  is_prefetching_scheduled_ = true;
  auto start = next_prefetch_off_;

  auto running_tsks = running_download_tasks_.load();
  uint64_t max_tsks = std::min(
      static_cast<uint64_t>(fs_->options_.prefetch_concurrency_per_file),
      prefetch_window_size_ / 2 / prefetch_chunk_size_);
  if (max_tsks <= running_tsks) return;

  max_tsks = max_tsks - running_tsks;
  size_t remain_prefetch_size = remote_size - next_prefetch_off_;

  if (remain_prefetch_size == 0) return;

  size_t prefetch_size =
      std::min(next_prefetch_size_, max_tsks * prefetch_chunk_size_);
  prefetch_size = std::min(prefetch_window_size_ / 2, prefetch_size);
  prefetch_size = std::min(prefetch_size, remain_prefetch_size);
  next_prefetch_off_ += prefetch_size;
  uint32_t num = prefetch_size / prefetch_chunk_size_;

  if (num == 0) return;

  running_download_tasks_.fetch_add(num);
  auto ctx = new PrefetchContext;
  ctx->prefetcher = this;
  ctx->offset = start;
  ctx->num = num;

  auto th = photon::thread_create(prefetch_tsk, ctx);
  photon::thread_migrate(
      th,
      ctx->prefetcher->fs_->bg_vcpu_env_.bg_oss_client_env->get_vcpu_next());

  next_prefetch_size_ =
      std::min(next_prefetch_size_ * 2, fs_->max_prefetch_size_per_handle_);
}

template <typename Derived>
void *EnableFilePrefetching<Derived>::prefetch_tsk(void *args) {
  auto ctx = (PrefetchContext *)args;
  auto start = ctx->offset;
  if (IS_FAULT_INJECTION_ENABLED(FI_First_Prefetch_Delay)) {
    if (start == 0) photon::thread_sleep(1);
  }
  for (int i = 0; i < ctx->num; i++) {
    ctx->prefetcher->fs_->prefetch_sem_->wait(1);
    auto sub_ctx = new PrefetchContext;
    sub_ctx->prefetcher = ctx->prefetcher;
    sub_ctx->offset = start;
    start += ctx->prefetcher->prefetch_chunk_size_;

    // TODO: use photon::threadpool with pooled_stack_allocator
    auto th = photon::thread_create(do_prefetch_tsk, sub_ctx);
    photon::thread_migrate(
        th,
        ctx->prefetcher->fs_->bg_vcpu_env_.bg_oss_client_env->get_vcpu_next());
  }

  delete ctx;
  return nullptr;
}

template <typename Derived>
void *EnableFilePrefetching<Derived>::do_prefetch_tsk(void *args) {
  auto ctx = (PrefetchContext *)args;
  thread_local auto back_fs =
      ctx->prefetcher->fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client();
  ctx->prefetcher->do_prefetch_range(back_fs, ctx->offset,
                                     ctx->prefetcher->prefetch_chunk_size_);
  ctx->prefetcher->fs_->prefetch_sem_->signal(1);

  ctx->prefetcher->running_download_tasks_.fetch_sub(1);

  delete ctx;
  return nullptr;
}

template <typename Derived>
ssize_t EnableFilePrefetching<Derived>::do_prefetch_range(
    OssAdapter *oss_client, off_t offset, size_t count) {
  uint64_t end = photon::sat_add(offset, count);
  auto alignment = derived()->get_prefetch_alignment();

  if (offset % alignment != 0) {
    offset = offset / alignment * alignment;
  }

  if (end % alignment != 0) {
    end = photon::sat_add(end, alignment - 1) / alignment * alignment;
  }

  uint64_t remain = end - offset;
  ssize_t read = 0;
  while (remain > 0) {
    off_t min = std::min(kMaxPrefetchSizePerRequest, remain);
    remain -= min;
    auto ret = derived()->bg_try_refill_range(oss_client, offset,
                                              static_cast<size_t>(min));
    if (ret < 0) {
      return ret;
    }
    read += ret;
    // Reach the end of the file.
    if (ret < min) {
      return read;
    }
    offset += ret;
  }
  return read;
}

template <typename Derived>
void EnableFilePrefetching<Derived>::wait_prefetch_done() {
  while (running_download_tasks_.load() > 0) {
    AUTO_USLEEP(3000);
  }
}

template class EnableFilePrefetching<OssCachedReader>;

};  // namespace OssFileSystem
