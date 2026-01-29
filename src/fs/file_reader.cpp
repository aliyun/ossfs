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
#include "metric/metrics.h"

namespace OssFileSystem {

OssCachedReader::OssCachedReader(OssFs *fs, std::string_view path,
                                 FileInode *inode)
    : EnableFilePrefetching<OssCachedReader>::EnableFilePrefetching(fs),
      remote_size_(inode->attr.size),
      mtime_(inode->attr.mtime),
      inode_(inode),
      path_(path) {
  const size_t block_size = fs_->options_.cache_block_size;
  size_t init_num_blocks =
      (fs_->options_.cache_refill_unit + block_size - 1) / block_size;

  if (inode_->cache_manager) {
    cache_manager_ = inode_->cache_manager;
  } else {
    cache_manager_ = std::make_shared<BlockCacheManager>(block_size);
  }

  std::tie(cache_, range_lock_) = cache_manager_->get();

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

  cache_manager_->release(
      [this](const std::vector<char *> &blocks) {
        fs_->download_buffers_->deallocate(blocks);
      },
      total_blocks_);

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
    ScopedRangeLock rl(*range_lock_, offset, count);
    r = cache_->pread(static_cast<char *>(buf), offset, count);
  }

  if (r > 0) {
    RELEASE_ASSERT((size_t)r == count);
    return r;
  }

  auto refill_offset = align_down(offset, refill_unit);
  auto refill_end = align_up(offset + count, refill_unit);
  auto refill_size =
      std::min(get_remote_size() - refill_offset, refill_end - refill_offset);
  r = GET_BACKGROUND_OSS_CLIENT_AND_DO_SYNC_FUNC(
      fs_, do_refill_range, refill_offset, refill_size, count,
      static_cast<char *>(buf), offset, false);
  if (r == -EAGAIN) {
    AUTO_USLEEP(100);
    goto again;
  } else if (r == -ENOSPC) {
    // Fall back to reading from OSS directly.
    iovec iov{buf, count};
    IOVector input(&iov, 1);
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_get_object_range, path_,
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
    set_latest_read_off(offset);
  }
  return ret;
}

ssize_t OssCachedReader::pin_rlocked(off_t offset, size_t count, void **buf) {
  // Fallback to pread().
  if (inode_->is_dirty) return -ENOTSUP;
  if (refresh_attr_if_needed_and_invoke([&]() { cache_->drop(); })) {
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

  ssize_t ret = cache_->pin(offset, count, buf);
  if (ret > 0) {
    set_latest_read_off(offset);
  }

  return ret;
}

void OssCachedReader::unpin(off_t offset) {
  cache_->unpin(offset);
}

ssize_t OssCachedReader::bg_try_refill_range(OssAdapter *oss_client,
                                             off_t offset, size_t count) {
  auto remote_size = get_remote_size();
  if (offset >= remote_size) return 0;
  if (offset + static_cast<off_t>(count) > remote_size) {
    count = remote_size - offset;
  }

again:
  auto res = cache_->query_refill_range(offset, count);
  if (0 == res.second) return count;

  ssize_t ret = do_refill_range(oss_client, res.first, res.second, count,
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

ssize_t OssCachedReader::do_refill_range(OssAdapter *oss_client,
                                         uint64_t refill_off,
                                         uint64_t refill_size, size_t count,
                                         char *input, off_t offset,
                                         bool from_bg_prefetch) {
  ssize_t ret = 0;
  off_t remote_size = get_remote_size();
  if (refill_off + refill_size > static_cast<uint64_t>(remote_size)) {
    refill_size = remote_size - refill_off;
  }

  if (!from_bg_prefetch) fs_->prefetch_sem_->wait(1);
  DEFER({
    if (!from_bg_prefetch) fs_->prefetch_sem_->signal(1);
  });

  ret = range_lock_->try_lock_wait(refill_off, refill_size);
  if (ret < 0) return -EAGAIN;

  DEFER({ range_lock_->unlock(refill_off, refill_size); });

  std::vector<iovec> blocks;
  ret = cache_->try_lock_blocks(refill_off, refill_size, blocks);
  if (ret < 0) {
    return ret;
  }

  DECLARE_METRIC_LATENCY(refill_range, Metric::kInternalMetrics);

  auto start_time = std::chrono::steady_clock::now();
  ret = oss_client->oss_get_object_range(path_, &blocks[0], blocks.size(),
                                         refill_off);
  auto end_time = std::chrono::steady_clock::now();

  uint64_t lat = std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                     .count();
  fs_->update_max_oss_rw_lat(lat);

  if (ret < 0) {
    cache_->unlock_blocks(refill_off, refill_size, true);
    // clang-format off
    LOG_ERROR(
        "src file ` read failed, read : `, expectRead : `, remote_size : `, offset : `, r: `",
        path_, ret, refill_size, remote_size, refill_off, ret);
    // clang-format on
    return ret;
  }

  if (input) {
    RELEASE_ASSERT(offset >= (off_t)refill_off);

    // Find the first block.
    int i = 0;
    size_t block_off = offset - refill_off;
    while (block_off > 0) {
      if (blocks[i].iov_len <= block_off) {
        block_off -= blocks[i].iov_len;
        i++;
      } else {
        break;
      }
    }

    size_t read = 0;
    while (read < count) {
      size_t read_size = std::min(blocks[i].iov_len - block_off, count - read);
      memcpy(input + read, static_cast<char *>(blocks[i].iov_base) + block_off,
             read_size);

      block_off = 0;
      read += read_size;
      i++;
    }
  }

  cache_->unlock_blocks(refill_off, refill_size);
  return count;
}

bool OssCachedReader::has_enough_space(size_t size) {
  return cache_manager_->capacity() * cache_->block_size() >= size;
}

// By default, each file handle attempts to allocate prefetch chunk memory from
// global memory pool before generating prefetch tasks if the current allocation
// is insufficient. This allocated memory is released when the file handle is
// closed. File handles that cannot obtain sufficient prefetch chunk memory will
// experience degraded performance due to the lack of prefetching capabilities.
void OssCachedReader::try_expand_prefetch_window(off_t remote_size) {
  const size_t block_size = cache_->block_size();
  auto max_prefetch_window_size =
      align_up(std::min(fs_->max_prefetch_window_size_per_handle_,
                        static_cast<size_t>(remote_size)),
               block_size);

  if (prefetch_window_size_ < max_prefetch_window_size) {
    auto target_prefetch_windows_size =
        std::min(prefetch_window_size_ * 2, max_prefetch_window_size);
    if (target_prefetch_windows_size == 0) {
      target_prefetch_windows_size =
          std::min(prefetch_chunk_size_ * 4, max_prefetch_window_size);
    }

    size_t target_total_buffer_size =
        get_prefetch_buffer_size(target_prefetch_windows_size);
    target_total_buffer_size =
        std::min(target_total_buffer_size, static_cast<size_t>(remote_size));
    size_t target_total_blocks =
        (target_total_buffer_size + block_size - 1) / block_size;
    target_total_blocks = std::max(target_total_blocks, total_blocks_);

    auto allocated_blocks = try_realloc_cache_blocks(target_total_blocks, true);
    if (allocated_blocks > 0) {
      auto old = prefetch_window_size_;
      prefetch_window_size_ =
          std::min(get_prefetch_window_size(total_blocks_ * block_size),
                   max_prefetch_window_size);
      LOG_DEBUG("[file=`] ` expand prefetch_window_size: ` to `", this, path_,
                old, prefetch_window_size_);
    }
  }
}

size_t OssCachedReader::try_realloc_cache_blocks(uint64_t new_total_blocks,
                                                 bool from_bg_prefetch) {
  RELEASE_ASSERT(new_total_blocks >= total_blocks_);
  const size_t block_size = cache_manager_->block_size();
  uint64_t new_num_blocks = new_total_blocks - total_blocks_;
  if (new_num_blocks == 0) return 0;

  uint64_t max_blocks = (get_remote_size() + block_size - 1) / block_size;
  size_t allocated_blocks = cache_manager_->try_expand_blocks(
      [this, from_bg_prefetch](uint64_t num_blocks) {
        return fs_->download_buffers_->try_allocate(num_blocks,
                                                    !from_bg_prefetch);
      },
      new_num_blocks, max_blocks);
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

void OssCachedReader::set_latest_read_off(off_t offset) {
  cache_->set_latest_read_off(offset);
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
  ssize_t ret = DO_SYNC_BACKGROUND_OSS_REQUEST(
      fs_, oss_get_object_range, path_, input.iovec(), input.iovcnt(), offset);
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
    return std::make_unique<OssCachedReader>(fs, path, inode);
  } else {
    return std::make_unique<OssDirectReader>(fs, path, inode);
  }
}

}  // namespace OssFileSystem