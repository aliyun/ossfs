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

#include "file_hdfs.h"

#include <fcntl.h>

#include <cerrno>

#include "common/fault_injector.h"
#include "common/fuse_buf_utils.h"
#include "common/logger.h"
#include "file.h"
#include "fs.h"

namespace OssFileSystem {

HdfsFileHandle::HdfsFileHandle(OssFs *fs, std::string_view path,
                               FileInode *inode, int flags, mode_t mode)
    : fs_(fs), inode_(inode), reader_path_(path), flags_(flags), mode_(mode) {}

HdfsFileHandle::~HdfsFileHandle() {
  if (reader_) {
    delete reader_;
    reader_ = nullptr;
  }
  if (writer_) {
    delete writer_;
    writer_ = nullptr;
  }
}

int HdfsFileHandle::open() {
  return open_with_flags(reader_path_, flags_);
}

int HdfsFileHandle::open_with_flags(std::string_view cur_path, int flags) {
  closed_ = false;

  if (reader_ || writer_) {
    return 0;
  }

  int access_mode = flags & O_ACCMODE;
  bool need_writer = false;
  bool need_reader = false;

  switch (access_mode) {
    case O_RDONLY:
      need_reader = true;
      break;
    case O_WRONLY:
      need_writer = true;
      break;
    case O_RDWR:
    default:
      need_writer = true;
      need_reader = true;
      break;
  }

  // Create path always opens a writer (even for O_RDONLY)
  // to ensure the file exists on the backend.
  if (flags & O_CREAT) {
    need_writer = true;
  }

  // Writer is opened first so the file is created on the backend.
  if (need_writer) {
    // For O_RDONLY|O_CREAT, change access mode to O_WRONLY so open_object
    // opens a writer. O_CREAT/O_TRUNC/etc are preserved.
    int writer_flags =
        (access_mode == O_RDONLY) ? ((flags & ~O_ACCMODE) | O_WRONLY) : flags;

    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsOpen_WriterFail, [&] { fi_err = true; });
    if (fi_err) return -EIO;

    RawObjHandle *raw_handle = nullptr;
    int ret = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, open_object, cur_path,
                                             writer_flags, mode_, &raw_handle);
    if (ret < 0) {
      LOG_ERROR("Failed to open HDFS WRITER: `, flags: `, ret: `", cur_path,
                writer_flags, ret);
      return ret;
    }
    writer_ = raw_handle;
    write_offset_ = 0;

    // jdo_open with OVERWRITE synchronously truncates the backend file to 0.
    // Reset the cached size so getattr / ftruncate see the real size.
    if (flags & O_TRUNC) {
      inode_->attr.size = 0;
      inode_->invalidate_data_cache = true;
    }

    if (flags & O_APPEND) {
      ssize_t pos = writer_->tell(cur_path);
      if (pos >= 0) {
        write_offset_ = pos;
      }
    }
    max_write_end_ = write_offset_;
  }

  if (need_reader) {
    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsOpen_ReaderFail, [&] { fi_err = true; });
    if (fi_err) {
      if (writer_) {
        delete writer_;
        writer_ = nullptr;
      }
      return -EIO;
    }

    RawObjHandle *raw_handle = nullptr;
    int ret = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, open_object, cur_path,
                                             O_RDONLY, mode_, &raw_handle);
    if (ret < 0) {
      LOG_ERROR("Failed to open HDFS READER: `, ret: `", cur_path, ret);
      if (writer_) {
        delete writer_;
        writer_ = nullptr;
      }
      return ret;
    }
    reader_ = raw_handle;
    read_offset_ = 0;
    reader_path_.assign(cur_path.data(), cur_path.size());
  }

  return 0;
}

// Close backend streams only, keeping the flock state intact.
int HdfsFileHandle::close_streams(std::string_view cur_path) {
  if (closed_) {
    return 0;
  }

  int ret = 0;

  if (writer_) {
    if (is_writing_) {
      ret = writer_->close(cur_path);
      if (ret == 0) {
        commit_written_size();
      }
      FAULT_INJECTION(FI_HdfsClose_WriterFail, [&] { ret = -EIO; });
      if (ret < 0) {
        // File was deleted while open — ignore close error, same as
        // OSS mode's complete_upload returning 0 for stale inodes.
        if (inode_->is_stale) {
          LOG_INFO("ignore close error for deleted HDFS file: `, ret: `",
                   cur_path, ret);
          ret = 0;
        } else {
          LOG_ERROR("Failed to close HDFS writer: `, ret: `", cur_path, ret);
        }
      }
      is_writing_ = false;
      RELEASE_ASSERT(inode_->hdfs_dirty_count > 0);
      if (--inode_->hdfs_dirty_count == 0) {
        inode_->is_dirty = false;
      }
      inode_->invalidate_data_cache = true;
      inode_->attr_time = 0;
    } else {
      int r = writer_->close(cur_path);
      FAULT_INJECTION(FI_HdfsClose_IdleWriterFail, [&] { r = -EIO; });
      if (r < 0) {
        LOG_WARN("Failed to close idle HDFS writer: `, ret: `", cur_path, r);
      }
    }
    delete writer_;
    writer_ = nullptr;
  }

  if (reader_) {
    int r = reader_->close(cur_path);
    FAULT_INJECTION(FI_HdfsClose_ReaderFail, [&] { r = -EIO; });
    if (r < 0) {
      LOG_WARN("Failed to close HDFS reader: `, ret: `", cur_path, r);
    }
    delete reader_;
    reader_ = nullptr;
  }

  closed_ = true;
  return ret;
}

void HdfsFileHandle::release_flock(std::string_view cur_path) {
  if (!holds_flock_) {
    return;
  }
  // Empty path = stale inode (external delete; local unlink keeps a hidden
  // inode). The lock is gone with the object, so skip rather than resolve ""
  // against the bucket root.
  if (cur_path.empty()) {
    LOG_WARN(
        "skip flock release on empty path (stale inode), nodeid: `, owner: `",
        inode_->nodeid, flock_owner_);
    holds_flock_ = false;
    flock_owner_ = 0;
    return;
  }
  int lr = PERFORM_BACKGROUND_OBJ_REQUEST(
      fs_, set_lock, cur_path, static_cast<int64_t>(0), static_cast<int64_t>(0),
      static_cast<int16_t>(LockType::UnLock), static_cast<int64_t>(getpid()),
      flock_owner_);
  if (lr < 0) {
    LOG_WARN("Failed to release flock on close: `, owner: `, ret: `", cur_path,
             flock_owner_, lr);
  }
  holds_flock_ = false;
  flock_owner_ = 0;
}

int HdfsFileHandle::close_with_path(std::string_view cur_path) {
  int ret = close_streams(cur_path);

  // BSD semantics: flock is bound to the fd, not the backend streams, so
  // release it here unconditionally. The lock is released either by an
  // explicit LOCK_UN or when all duplicate fds are closed.
  // See: https://man7.org/linux/man-pages/man2/flock.2.html.
  release_flock(cur_path);

  return ret;
}

int HdfsFileHandle::close() {
  // No-arg override only satisfies the base interface; HDFS callers go through
  // close_with_path() (see OssFs::release). Fall back to reader_path_.
  return close_with_path(reader_path_);
}

void HdfsFileHandle::release() {
  delete this;
}

int HdfsFileHandle::fsync() {
  return fdatasync();
}

int HdfsFileHandle::fdatasync() {
  if (!(flags_ & O_WRONLY) && !(flags_ & O_RDWR)) {
    return 0;
  }

  // A stale inode may not resolve a path; skip the flush in that case.
  if (inode_->is_stale) {
    LOG_INFO("skip flush for deleted HDFS file, nodeid: `", inode_->nodeid);
    return 0;
  }

  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);
  if (!writer_ || !is_writing_) {
    return 0;
  }

  int ret = writer_->flush(ref.inode_path);
  FAULT_INJECTION(FI_HdfsFdatasync_FlushFail, [&] { ret = -EIO; });
  if (ret < 0) {
    LOG_ERROR("Failed to fdatasync HDFS file: `, ret: `", ref.inode_path, ret);
  } else {
    commit_written_size();
  }
  return ret;
}

int HdfsFileHandle::ftruncate(off_t) {
  // Unreachable in HDFS mode: OssFs routes through ftruncate_with_path().
  RELEASE_ASSERT(false);
  return -ENOTSUP;
}

// Caller must hold inode_lock (write lock) and pass the fresh inode path.
int HdfsFileHandle::ftruncate_with_path(std::string_view cur_path,
                                        off_t target_size) {
  off_t current_size =
      std::max(static_cast<off_t>(inode_->attr.size), max_write_end_);

  if (current_size > target_size) {
    // Shrink path: close -> truncate -> reopen. The fd stays open, so only
    // streams are closed and the flock survives. If truncate_object fails,
    // writer_/reader_ remain null and the handle is broken; subsequent I/O
    // must check them and return -EBADF.
    int r = close_streams(cur_path);
    if (r < 0) return r;

    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, truncate_object, cur_path,
                                       target_size);
    if (r < 0) return r;

    inode_->attr.size = target_size;

    // Strip one-shot flags: replaying O_TRUNC would destroy the just
    // truncated file.
    r = open_with_flags(cur_path, flags_ & ~(O_TRUNC | O_CREAT));
    if (r < 0) return r;
  } else if (current_size < target_size) {
    if (!writer_) {
      return -EBADF;
    }

    off_t extend_len = target_size - current_size;
    int r = writer_->fallocate(cur_path, current_size, extend_len);
    if (r < 0) return r;

    inode_->attr.size = target_size;
  }

  return 0;
}

int HdfsFileHandle::fallocate(off_t, off_t) {
  // Unreachable in HDFS mode: OssFs routes through fallocate_with_path().
  RELEASE_ASSERT(false);
  return -ENOTSUP;
}

int HdfsFileHandle::fallocate_with_path(std::string_view cur_path, off_t offset,
                                        off_t length) {
  off_t current_size =
      std::max(static_cast<off_t>(inode_->attr.size), max_write_end_);
  off_t new_end = offset + length;

  if (new_end > current_size) {
    if (!writer_) {
      return -EBADF;
    }
    off_t extend_len = new_end - current_size;
    int r = writer_->fallocate(cur_path, current_size, extend_len);
    if (r < 0) return r;
    inode_->attr.size = new_end;
    inode_->invalidate_data_cache = true;
    inode_->attr_time = 0;
  }

  return 0;
}

ssize_t HdfsFileHandle::pread(void *buf, size_t count, off_t offset) {
  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::shared_lock<std::shared_mutex> guard(inode_->inode_lock);
  std::lock_guard<std::mutex> lock(mutex_);

  // The SDK reader stream is bound to the path it was opened at; after a rename
  // that path is stale, so rebuild the reader at the fresh inode path.
  if ((flags_ & O_ACCMODE) != O_WRONLY && ref.inode &&
      ref.inode_path != reader_path_) {
    LOG_WARN("HDFS reader needs rebuild after rename: `", ref.inode_path);
    int rr = rebuild_reader_after_rename(ref.inode_path);
    if (rr < 0) return rr;
  }

  if (!reader_) return -EBADF;

  if (offset != read_offset_) {
    int ret = reader_->seek(ref.inode_path, offset);
    FAULT_INJECTION(FI_HdfsPread_SeekFail, [&] { ret = -EIO; });
    if (ret < 0) {
      LOG_ERROR("Failed to seek HDFS file: `, offset: `, ret: `",
                ref.inode_path, offset, ret);
      return ret;
    }
    read_offset_ = offset;
  }

  // Use read() instead of pread() to enable SDK sequential read prefetch.
  size_t total_read = 0;
  while (total_read < count) {
    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsPread_ReadFail, [&] { fi_err = true; });
    if (fi_err) return -EIO;
    ssize_t ret = reader_->read(ref.inode_path, (char *)buf + total_read,
                                count - total_read);
    if (ret < 0) {
      LOG_ERROR("Failed to read HDFS file: `, offset: `, count: `, ret: `",
                ref.inode_path, offset, count, ret);
      return ret;
    }
    if (ret == 0) break;
    read_offset_ += ret;
    total_read += ret;
  }

  return total_read;
}

int HdfsFileHandle::rebuild_reader_after_rename(std::string_view new_path) {
  if (reader_) {
    int r = reader_->close(reader_path_);
    if (r < 0) {
      LOG_WARN("Failed to close stale HDFS reader: `, ret: `", reader_path_, r);
    }
    delete reader_;
    reader_ = nullptr;
  }

  if (IS_FAULT_INJECTION_ENABLED(FI_HdfsReaderRebuild_ReopenFail)) {
    return -EIO;
  }

  RawObjHandle *raw_handle = nullptr;
  int ret = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, open_object, new_path, O_RDONLY,
                                           mode_, &raw_handle);
  if (ret < 0) {
    LOG_ERROR("Failed to reopen HDFS READER after rename: `, ret: `", new_path,
              ret);
    return ret;
  }

  reader_ = raw_handle;
  reader_path_.assign(new_path.data(), new_path.size());
  read_offset_ = 0;
  LOG_INFO("Rebuilt HDFS reader after rename: `", reader_path_);
  return 0;
}

int HdfsFileHandle::seek_writer_to_offset(std::string_view path, off_t offset) {
  if ((flags_ & O_APPEND) || write_offset_ == offset) {
    return 0;
  }
  ssize_t r = writer_->seek(path, offset);
  FAULT_INJECTION(FI_HdfsWrite_SeekFail, [&] { r = -EIO; });
  if (r < 0) {
    LOG_ERROR("Failed to seek HDFS file: `, offset: `, ret: `", path, offset,
              r);
    return r;
  }
  write_offset_ = offset;
  return 0;
}

void HdfsFileHandle::finalize_write(ssize_t total_written) {
  write_offset_ += total_written;
  // Track the high-water mark; the cursor retreats on random writes.
  if (write_offset_ > max_write_end_) {
    max_write_end_ = write_offset_;
  }
  if (!is_writing_) {
    is_writing_ = true;
    inode_->hdfs_dirty_count++;
    inode_->is_dirty = true;
  }
  // The inode size is deferred to commit_written_size (flush/close): the
  // backend cannot serve data still buffered in the writer stream.
}

void HdfsFileHandle::commit_written_size() {
  if (max_write_end_ > static_cast<off_t>(inode_->attr.size)) {
    inode_->attr.size = max_write_end_;
  }
}

ssize_t HdfsFileHandle::pwrite(const void *buf, size_t count, off_t offset) {
  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);
  if (!writer_) return -EBADF;

  if (count == 0) return 0;

  int seek_ret = seek_writer_to_offset(ref.inode_path, offset);
  if (seek_ret < 0) return seek_ret;

  ssize_t written = writer_->write(ref.inode_path, buf, count);
  FAULT_INJECTION(FI_HdfsWrite_ShortWrite, [&] {
    if (written > 0) written = count - 1;
  });
  if (written < 0) return written;

  // Advance the cached offset by whatever actually entered the stream, keeping
  // it in lockstep with the stream cursor even on a short write.
  finalize_write(written);
  if (static_cast<size_t>(written) < count) {
    LOG_ERROR("HDFS short write: count=` written=`, data loss! path: `", count,
              written, ref.inode_path);
    return -EIO;
  }
  return written;
}

ssize_t HdfsFileHandle::pin(off_t offset, size_t count, void **buf) {
  return -ENOTSUP;
}

void HdfsFileHandle::unpin(off_t offset) {}

ssize_t HdfsFileHandle::write_buf(struct fuse_bufvec *bufv, off_t offset) {
  // Each chunk that enters the stream advances the cached write offset right
  // away (finalize_write), keeping it in lockstep with the stream cursor; a
  // chunk error therefore needs no cursor fix-up on the failure path.
  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);
  if (!writer_) return -EBADF;

  size_t total = fuse_bufv_size(bufv);
  if (total == 0) return 0;

  int seek_ret = seek_writer_to_offset(ref.inode_path, offset);
  if (seek_ret < 0) return seek_ret;

  RELEASE_ASSERT(fs_->data_buffers_ != nullptr);
  size_t block_size = fs_->data_buffers_->block_size();
  auto tmp_vec = fs_->data_buffers_->allocate_write(1);
  char *tmp = tmp_vec.front();
  DEFER(fs_->data_buffers_->deallocate_write(tmp_vec));

  ssize_t total_written = 0;
  size_t remaining = total;

  while (remaining > 0) {
    size_t chunk = std::min(block_size, remaining);

    ssize_t copied = fuse_read_bufvec_full(tmp, bufv, chunk);
    if (copied < 0) return copied;

    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsWriteBuf_ReadError, [&] { fi_err = true; });
    if (fi_err) return -EIO;

    ssize_t written = writer_->write(ref.inode_path, tmp, copied);
    if (written < 0) return written;
    finalize_write(written);
    if (written < copied) {
      LOG_ERROR("HDFS short write: copied=` written=`, data loss! path: `",
                copied, written, ref.inode_path);
      return -EIO;
    }

    total_written += written;
    remaining -= copied;
  }

  return total_written;
}

}  // namespace OssFileSystem
