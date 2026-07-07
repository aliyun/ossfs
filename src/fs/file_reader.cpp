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

#include "file_reader.h"

#include <photon/common/iovector.h>

#include "error_codes.h"
#include "file.h"
#include "fs.h"
#include "mem_cache.h"
#include "metric/metrics.h"

namespace OssFileSystem {

OssCachedReader::OssCachedReader(OssFs *fs, std::string_view path,
                                 FileInode *inode,
                                 std::shared_ptr<ICache> cache,
                                 CacheHandle *cache_handle)
    : EnableFilePrefetching<OssCachedReader>::EnableFilePrefetching(fs),
      cache_(std::move(cache)),
      cache_handle_(cache_handle),
      remote_size_(inode->attr.size),
      mtime_(inode->attr.mtime),
      inode_(inode),
      path_(path) {
  const size_t block_size = cache_->block_size();
  size_t init_num_blocks =
      (fs_->options_.cache_refill_unit + block_size - 1) / block_size;

  // Only allow min_reserved_buffer_size_per_file to be set to 0 or 1 MB
  // currently.
  if (fs_->active_file_handles_.fetch_add(1) <
      fs_->options_.max_total_reserved_buffer_count) {
    if (fs_->options_.min_reserved_buffer_size_per_file > 0 &&
        get_remote_size() > 0) {
      try_realloc_cache_blocks(init_num_blocks);
    }
  }
}

OssCachedReader::~OssCachedReader() {
  wait_prefetch_done();

  cache_->release(cache_handle_, total_blocks_);

  fs_->active_file_handles_.fetch_sub(1);
}

off_t OssCachedReader::get_remote_size() {
  SCOPED_LOCK(attr_lock_);
  return remote_size_;
}

void OssCachedReader::set_remote_size(off_t size) {
  SCOPED_LOCK(attr_lock_);
  remote_size_ = size;
}

bool OssCachedReader::refresh_attr_if_needed_and_invoke(
    std::function<void()> &&callback) {
  SCOPED_LOCK(attr_lock_);

  // The goal is to support readers seeing appended data after the attr cache
  // expires. ETag-only changes are ignored, as they would require kernel cache
  // invalidation (not implemented) when mtime and size remain unchanged.
  // If a relevant change (e.g., mtime or size) is detected, caller must
  // invalidate any cached data associated with this file handle within the
  // callback.
  if (unlikely(inode_->attr.size != static_cast<uint64_t>(remote_size_) ||
               inode_->attr.mtime.tv_sec != mtime_.tv_sec ||
               inode_->attr.mtime.tv_nsec != mtime_.tv_nsec)) {
    remote_size_ = inode_->attr.size;
    mtime_ = inode_->attr.mtime;
    callback();
    return true;
  }
  return false;
}

ssize_t OssCachedReader::do_pread(void *buf, size_t count, off_t offset,
                                  size_t refill_unit) {
again:
  ssize_t r = 0;
  {
    ScopedRangeLock rl(*range_lock(), offset, count);
    r = cache_handle_->pread(static_cast<char *>(buf), offset, count);
  }

  if (r > 0) {
    RELEASE_ASSERT((size_t)r == count);
    return r;
  }

  auto refill_offset = align_down(offset, refill_unit);
  auto refill_end = align_up(offset + count, refill_unit);
  auto refill_size =
      std::min(get_remote_size() - refill_offset, refill_end - refill_offset);
  r = GET_BACKGROUND_OBJ_STORE_AND_PERFORM(
      fs_, do_refill_range, refill_offset, refill_size, count,
      static_cast<char *>(buf), offset, false);
  if (r == -EAGAIN) {
    AUTO_USLEEP(100);
    goto again;
  } else if (r == -ENOSPC) {
    // Fall back to reading from OSS directly.
    iovec iov{buf, count};
    IOVector input(&iov, 1);
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, get_object_range, path_,
                                       input.iovec(), input.iovcnt(), offset);
    if (r < 0) {
      LOG_ERROR("fail to read ` from oss, offset:`, size:`, r: `", path_,
                offset, count, r);
    }
  } else if (r < 0) {
    LOG_ERROR("fail to read file: `, nodeid: `, offset: `, count: `, r: `",
              path_, inode_->nodeid, offset, count, r);
  }

  return r;
}

ssize_t OssCachedReader::pread_rlocked(void *buf, size_t count, off_t offset) {
  if (fs_->options_.enable_appendable_object) {
    if (inode_->is_dirty) {
      auto r = read_from_dirty_inode(buf, count, offset);
      if (r != -E_NO_DIRTY_DATA) return r;
    } else {
      set_remote_size(inode_->attr.size);
    }
  }
  return -E_CONTINUE_READ;
}

ssize_t OssCachedReader::pread(void *buf, size_t count, off_t offset) {
  off_t remote_size = get_remote_size();
  if (unlikely(offset >= (int64_t)remote_size)) {
    return 0;
  }

  count = std::min(count, (size_t)remote_size - offset);
  do_prefetch(remote_size, offset, count);

  auto ret = do_pread(buf, count, offset, fs_->options_.cache_refill_unit);
  if (ret > 0) {
    cache_handle_->on_read_success(
        offset, get_prefetch_buffer_size(prefetch_window_size_));
  }
  return ret;
}

ssize_t OssCachedReader::pin_rlocked(off_t offset, size_t count, void **buf) {
  // Fallback to pread().
  if (inode_->is_dirty) return -ENOTSUP;
  if (refresh_attr_if_needed_and_invoke([&]() { cache_handle_->drop(); })) {
    return -ENOENT;
  }
  return -E_CONTINUE_PIN;
}

ssize_t OssCachedReader::pin(off_t offset, size_t count, void **buf) {
  off_t remote_size = get_remote_size();
  if (unlikely(offset >= remote_size)) {
    return 0;
  }

  count = std::min(count, (size_t)remote_size - offset);
  do_prefetch(remote_size, offset, count);

  ssize_t ret = cache_handle_->pin(offset, count, buf);
  if (ret > 0) {
    cache_handle_->on_read_success(
        offset, get_prefetch_buffer_size(prefetch_window_size_));
  }

  return ret;
}

void OssCachedReader::unpin(off_t offset) {
  cache_handle_->unpin(offset);
}

ssize_t OssCachedReader::bg_try_refill_range(IObjStore *obj_store, off_t offset,
                                             size_t count) {
  auto remote_size = get_remote_size();
  if (offset >= remote_size) return 0;
  if (offset + static_cast<off_t>(count) > remote_size) {
    count = remote_size - offset;
  }

again:
  auto res = cache_handle_->query_refill_range(offset, count);
  if (0 == res.second) return count;

  ssize_t ret = do_refill_range(obj_store, res.first, res.second, count,
                                nullptr, 0, true);
  if (ret == -EAGAIN) {
    AUTO_USLEEP(100);
    goto again;
  }

  if (ret < 0) {
    LOG_WARN(
        "[file=`] bg_try_refill_range ` failed, ret : `, count : `, offset : `",
        this, path_, ret, count, offset);
  }
  return ret;
}

ssize_t OssCachedReader::do_refill_range(IObjStore *obj_store,
                                         uint64_t refill_off,
                                         uint64_t refill_size, size_t count,
                                         char *input, off_t offset,
                                         bool from_bg_prefetch) {
  ssize_t ret = 0;
  FAULT_INJECTION(FaultInjectionId::FI_Do_Refill_Range_Delay,
                  []() { AUTO_USLEEP(1'000'000); });
  off_t remote_size = get_remote_size();
  if (refill_off >= static_cast<uint64_t>(remote_size)) {
    LOG_DEBUG("refill_off(`) >= remote_size(`), skip refill", refill_off,
              remote_size);
    return 0;
  } else if (refill_off + refill_size > static_cast<uint64_t>(remote_size)) {
    refill_size = remote_size - refill_off;
  }

  if (!from_bg_prefetch) fs_->prefetch_sem_->wait(1);
  DEFER({
    if (!from_bg_prefetch) fs_->prefetch_sem_->signal(1);
  });

  ret = range_lock()->try_lock_wait(refill_off, refill_size);
  if (ret < 0) return -EAGAIN;

  DEFER({ range_lock()->unlock(refill_off, refill_size); });

  RangeBuffer range_buffer;
  range_buffer.offset = refill_off;
  range_buffer.count = refill_size;
  ret = cache_handle_->acquire_write_buffer(range_buffer);
  if (ret < 0) return ret;

  DECLARE_METRIC_LATENCY(refill_range, Metric::kInternalMetrics);

  auto &buffer = range_buffer.buffer;
  auto start_time = std::chrono::steady_clock::now();
  ret = obj_store->get_object_range(path_, buffer.iovec(), buffer.iovcnt(),
                                    refill_off);
  auto end_time = std::chrono::steady_clock::now();

  uint64_t lat = std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                     .count();
  fs_->update_max_oss_rw_lat(lat);

  if (ret < 0) {
    cache_handle_->release_write_buffer(range_buffer, true);
    // clang-format off
    LOG_ERROR(
        "src file ` read failed, read : `, expectRead : `, remote_size : `, offset : `, r: `",
        path_, ret, refill_size, remote_size, refill_off, ret);
    // clang-format on
    return ret;
  }

  if (input) {
    RELEASE_ASSERT(offset >= static_cast<off_t>(refill_off));

    iovector_view tail_buffer;
    tail_buffer.iovcnt = 0;
    size_t tail_size = refill_size - (offset - refill_off);
    buffer.slice(tail_size, offset - refill_off, &tail_buffer);
    ret = tail_buffer.memcpy_to(input, count);
    RELEASE_ASSERT(ret == static_cast<ssize_t>(count));
  }

  cache_handle_->release_write_buffer(range_buffer);
  return count;
}

bool OssCachedReader::has_enough_space(size_t size) {
  size_t block_size = cache_->block_size();
  return cache_->capacity() >= (size + block_size - 1) / block_size;
}

// Memory-aware prefetch control based on pool usage.
// Layer 1 (0 - low):    Full window, aggressive expansion (2x)
// Layer 2 (low - high): Reduced window, moderate expansion (1.5x)
// Layer 3 (high+):      Minimum window, conservative expansion (1.25x)
size_t OssCachedReader::get_dynamic_max_window(size_t configured_max,
                                               double pool_usage) const {
  if (pool_usage < kPoolUsageLowThreshold) {
    return configured_max;
  } else if (pool_usage < kPoolUsageHighThreshold) {
    // Linear reduction: low -> 100%, high -> kMinWindowRatio
    double ratio =
        1.0 - (pool_usage - kPoolUsageLowThreshold) /
                  (kPoolUsageHighThreshold - kPoolUsageLowThreshold) *
                  (1.0 - kMinWindowRatio);
    return static_cast<size_t>(configured_max * ratio);
  } else {
    return prefetch_chunk_size_ * 4;
  }
}

double OssCachedReader::get_dynamic_expansion_factor(double pool_usage) const {
  if (pool_usage < kPoolUsageLowThreshold) {
    return kExpansionFactorAtLowUsage;
  } else if (pool_usage < kPoolUsageHighThreshold) {
    return kExpansionFactorAtMedUsage;
  } else {
    return kExpansionFactorAtHighUsage;
  }
}

// By default, each file handle attempts to allocate prefetch chunk memory from
// global memory pool before generating prefetch tasks if the current allocation
// is insufficient. This allocated memory is released when the file handle is
// closed. File handles that cannot obtain sufficient prefetch chunk memory will
// experience degraded performance due to the lack of prefetching capabilities.
void OssCachedReader::try_expand_prefetch_window(off_t remain_prefetch_size) {
  if (is_prefetch_too_far_ahead()) return;

  const size_t block_size = cache_->block_size();

  auto pool_usage = fs_->download_buffers_->get_usage_ratio();
  auto configured_max_window =
      std::min(fs_->max_prefetch_window_size_per_handle_,
               static_cast<size_t>(remain_prefetch_size));
  auto max_prefetch_window_size = align_up(
      get_dynamic_max_window(configured_max_window, pool_usage), block_size);

  if (prefetch_window_size_ < max_prefetch_window_size) {
    double expansion_factor = get_dynamic_expansion_factor(pool_usage);

    auto target_prefetch_windows_size = align_up(
        std::min(static_cast<size_t>(prefetch_window_size_ * expansion_factor),
                 max_prefetch_window_size),
        block_size);
    if (target_prefetch_windows_size == 0) {
      target_prefetch_windows_size =
          std::min(prefetch_chunk_size_ * 4, max_prefetch_window_size);
    }

    size_t target_total_buffer_size =
        get_prefetch_buffer_size(target_prefetch_windows_size);
    target_total_buffer_size = std::min(
        target_total_buffer_size, static_cast<size_t>(remain_prefetch_size));
    size_t target_total_blocks =
        (target_total_buffer_size + block_size - 1) / block_size;
    target_total_blocks = std::max(target_total_blocks, total_blocks_);

    auto allocated_blocks = try_realloc_cache_blocks(target_total_blocks, true);
    if (allocated_blocks > 0) {
      auto old = prefetch_window_size_.load();
      prefetch_window_size_ =
          std::min(get_prefetch_window_size(total_blocks_ * block_size),
                   max_prefetch_window_size);
      LOG_DEBUG("[file=`] ` expand prefetch_window_size: ` to `", this, path_,
                old, prefetch_window_size_.load());
    }
  }
}

size_t OssCachedReader::try_realloc_cache_blocks(uint64_t new_total_blocks,
                                                 bool from_bg_prefetch) {
  RELEASE_ASSERT(new_total_blocks >= total_blocks_);
  const size_t block_size = cache_->block_size();
  uint64_t new_num_blocks = new_total_blocks - total_blocks_;
  if (new_num_blocks == 0) return 0;

  uint64_t max_blocks = (get_remote_size() + block_size - 1) / block_size;
  size_t allocated_blocks =
      cache_->try_expand_blocks(new_num_blocks, max_blocks, !from_bg_prefetch);
  total_blocks_ += allocated_blocks;
  return allocated_blocks;
}

ssize_t OssCachedReader::read_from_dirty_inode(void *buf, size_t count,
                                               off_t offset) {
  const size_t buffer_size = fs_->options_.upload_buffer_size;

  auto dirty_fh = inode_->dirty_fh;
  RELEASE_ASSERT(dirty_fh);

  if (dirty_fh->get_is_immutable()) {
    return -E_NO_DIRTY_DATA;
  }

  // Update the size of the clean part of this file.
  off_t remote_size = dirty_fh->calc_remote_size();
  set_remote_size(remote_size);

  const size_t real_size = inode_->attr.size;
  if (unlikely(offset >= (int64_t)real_size)) {
    return 0;
  }

  count = std::min(count, (size_t)real_size - offset);

  // No dirty data, fall back to reading from OSS.
  if (offset + count <= static_cast<size_t>(remote_size))
    return -E_NO_DIRTY_DATA;

  size_t dirty_buffer_index = real_size / buffer_size;
  size_t buffer_index = offset / buffer_size;
  off_t buffer_offset = offset % buffer_size;

  size_t read = 0;
  for (; buffer_index <= dirty_buffer_index; buffer_index++) {
    ssize_t r = 0;
    off_t read_off = buffer_index * buffer_size + buffer_offset;
    size_t read_size = std::min(count - read, buffer_size - buffer_offset);

    if (dirty_buffer_index == buffer_index) {
      if (remote_size > read_off) {
        size_t remote_read_size = remote_size - read_off;
        r = do_pread(static_cast<char *>(buf) + read, remote_read_size,
                     read_off, fs_->options_.cache_block_size);
        if (r < 0) {
          // clang-format off
          LOG_ERROR(
              "read ` from remote file failed, read : `, expectRead : `, remote_size : `, offset : `",
              path_, r, remote_read_size, remote_size, read_off);
          // clang-format on
          return r;
        }
        read += r;
        read_off += r;
        read_size -= r;
      }

      RELEASE_ASSERT(read_size == count - read);
      r = dirty_fh->pread_from_upload_buffer(static_cast<char *>(buf) + read,
                                             read_size, read_off);
      RELEASE_ASSERT(r == static_cast<ssize_t>(read_size));
    } else {
      r = do_pread(static_cast<char *>(buf) + read, read_size, read_off,
                   fs_->options_.cache_block_size);
      if (r < 0) {
        // clang-format off
        LOG_ERROR(
            "read from remote file failed, read : `, expectRead : `, remote_size : `, offset : `",
            r, read_size, remote_size, read_off);
        // clang-format on
        return r;
      }
    }

    read += r;
    if (buffer_offset != 0) buffer_offset = 0;
  }

  return read;
}

int OssCachedReader::close() {
  wait_prefetch_done();
  return 0;
}

uint64_t OssCachedReader::get_prefetch_alignment() {
  return fs_->options_.cache_block_size;
}

OssDirectReader::OssDirectReader(OssFs *fs, std::string_view path,
                                 FileInode *inode)
    : fs_(fs), inode_(inode), path_(path) {}

// TODO: support read dirty file when enable_appendable_object is true
ssize_t OssDirectReader::pread_rlocked(void *buf, size_t count, off_t offset) {
  file_size_ = inode_->attr.size;
  return -E_CONTINUE_READ;
}

ssize_t OssDirectReader::pread(void *buf, size_t count, off_t offset) {
  auto file_size = file_size_.load();
  if (unlikely(offset >= file_size)) {
    return 0;
  }

  count = std::min(count, (size_t)file_size - offset);

  iovec iov{buf, count};
  IOVector input(&iov, 1);
  ssize_t ret = PERFORM_BACKGROUND_OBJ_REQUEST(
      fs_, get_object_range, path_, input.iovec(), input.iovcnt(), offset);
  if (ret < 0) {
    LOG_ERROR("fail to read ` from oss, offset:`, size:`, ret: `", path_,
              offset, count, ret);
  }

  return ret;
}

std::unique_ptr<IReader> create_oss_reader(OssFs *fs, std::string_view path,
                                           FileInode *inode,
                                           bool cache_enabled) {
  if (cache_enabled) {
    auto cache = inode->cache;
    if (cache == nullptr) {
      cache = std::make_shared<BlockCache>(fs->get_download_buffers());
    }
    auto cache_handle = cache->get(path, inode->etag, inode->attr.size);
    if (cache_handle) {
      return std::make_unique<OssCachedReader>(fs, path, inode,
                                               std::move(cache), cache_handle);
    }
  }
  return std::make_unique<OssDirectReader>(fs, path, inode);
}

}  // namespace OssFileSystem
