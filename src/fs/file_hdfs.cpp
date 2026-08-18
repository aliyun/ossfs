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
    : fs_(fs), inode_(inode), path_(path), flags_(flags), mode_(mode) {}

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
  closed_ = false;

  if (reader_ || writer_) {
    return 0;
  }

  int access_mode = flags_ & O_ACCMODE;
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
  if (flags_ & O_CREAT) {
    need_writer = true;
  }

  // Pass original flags_ to open_object; the store layer handles
  // POSIX-to-JDO flag conversion.
  // Writer is opened first so the file is created on the backend.
  if (need_writer) {
    // For O_RDONLY|O_CREAT, change access mode to O_WRONLY so open_object
    // opens a writer. O_CREAT/O_TRUNC/etc are preserved.
    int writer_flags =
        (access_mode == O_RDONLY) ? ((flags_ & ~O_ACCMODE) | O_WRONLY) : flags_;

    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsOpen_WriterFail, [&] { fi_err = true; });
    if (fi_err) return -EIO;

    RawObjHandle *raw_handle = nullptr;
    int ret = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, open_object, path_,
                                             writer_flags, mode_, &raw_handle);
    if (ret < 0) {
      LOG_ERROR("Failed to open HDFS WRITER: `, flags: `, ret: `", path_,
                writer_flags, ret);
      return ret;
    }
    writer_ = raw_handle;
    write_offset_ = 0;
    if (flags_ & O_APPEND) {
      ssize_t pos = writer_->tell();
      if (pos >= 0) {
        write_offset_ = pos;
      }
    }
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
    int ret = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, open_object, path_, O_RDONLY,
                                             mode_, &raw_handle);
    if (ret < 0) {
      LOG_ERROR("Failed to open HDFS READER: `, ret: `", path_, ret);
      if (writer_) {
        delete writer_;
        writer_ = nullptr;
      }
      return ret;
    }
    reader_ = raw_handle;
    read_offset_ = 0;
  }

  return 0;
}

int HdfsFileHandle::close() {
  if (closed_) {
    return 0;
  }

  int ret = 0;

  if (writer_) {
    if (is_writing_) {
      ret = writer_->close();
      FAULT_INJECTION(FI_HdfsClose_WriterFail, [&] { ret = -EIO; });
      if (ret < 0) {
        // File was deleted while open — ignore close error, same as
        // OSS mode's complete_upload returning 0 for stale inodes.
        if (inode_->is_stale) {
          LOG_INFO("ignore close error for deleted HDFS file: `, ret: `", path_,
                   ret);
          ret = 0;
        } else {
          LOG_ERROR("Failed to close HDFS writer: `, ret: `", path_, ret);
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
      int r = writer_->close();
      FAULT_INJECTION(FI_HdfsClose_IdleWriterFail, [&] { r = -EIO; });
      if (r < 0) {
        LOG_WARN("Failed to close idle HDFS writer: `, ret: `", path_, r);
      }
    }
    delete writer_;
    writer_ = nullptr;
  }

  if (reader_) {
    int r = reader_->close();
    FAULT_INJECTION(FI_HdfsClose_ReaderFail, [&] { r = -EIO; });
    if (r < 0) {
      LOG_WARN("Failed to close HDFS reader: `, ret: `", path_, r);
    }
    delete reader_;
    reader_ = nullptr;
  }

  // Release flock if still held. BSD semantics: the lock is released
  // either by an explicit LOCK_UN operation on any of these duplicate
  // file descriptors, or when all such file descriptors have been
  // closed. See: https://man7.org/linux/man-pages/man2/flock.2.html.
  if (holds_flock_) {
    int lr = PERFORM_BACKGROUND_OBJ_REQUEST(
        fs_, set_lock, path_, static_cast<int64_t>(0), static_cast<int64_t>(0),
        static_cast<int16_t>(LockType::UnLock), static_cast<int64_t>(getpid()),
        flock_owner_);
    if (lr < 0) {
      LOG_WARN("Failed to release flock on close: `, owner: `, ret: `", path_,
               flock_owner_, lr);
    }
    holds_flock_ = false;
    flock_owner_ = 0;
  }

  closed_ = true;
  return ret;
}

void HdfsFileHandle::release() {
  delete this;
}

int HdfsFileHandle::fsync() {
  return fdatasync();
}

int HdfsFileHandle::fdatasync() {
  if (!writer_ || !is_writing_) {
    return 0;
  }

  if (!(flags_ & O_WRONLY) && !(flags_ & O_RDWR)) {
    return 0;
  }

  // File was deleted while open — skip flush.
  if (inode_->is_stale) {
    LOG_INFO("skip flush for deleted HDFS file: `", path_);
    return 0;
  }

  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);

  int ret = writer_->flush();
  FAULT_INJECTION(FI_HdfsFdatasync_FlushFail, [&] { ret = -EIO; });
  if (ret < 0) {
    LOG_ERROR("Failed to fdatasync HDFS file: `, ret: `", path_, ret);
  }
  return ret;
}

// Caller must hold inode_lock (write lock).
int HdfsFileHandle::ftruncate(off_t target_size) {
  off_t current_size =
      std::max(static_cast<off_t>(inode_->attr.size), write_offset_);

  if (current_size > target_size) {
    // Shrink path: close -> truncate -> reopen.
    // If truncate_object fails after close(), writer_/reader_ remain null.
    // The handle is effectively broken; subsequent I/O calls must check
    // writer_/reader_ validity and return -EBADF gracefully.
    int r = close();
    if (r < 0) return r;

    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, truncate_object, path_,
                                       target_size);
    if (r < 0) return r;

    inode_->attr.size = target_size;

    r = open();
    if (r < 0) return r;
  } else if (current_size < target_size) {
    if (!writer_) {
      return -EBADF;
    }

    off_t extend_len = target_size - current_size;
    int r = writer_->fallocate(current_size, extend_len);
    if (r < 0) return r;

    inode_->attr.size = target_size;
  }

  return 0;
}

int HdfsFileHandle::fallocate(off_t offset, off_t length) {
  off_t current_size =
      std::max(static_cast<off_t>(inode_->attr.size), write_offset_);
  off_t new_end = offset + length;

  if (new_end > current_size) {
    if (!writer_) {
      return -EBADF;
    }
    off_t extend_len = new_end - current_size;
    int r = writer_->fallocate(current_size, extend_len);
    if (r < 0) return r;
    inode_->attr.size = new_end;
  }

  return 0;
}

ssize_t HdfsFileHandle::pread(void *buf, size_t count, off_t offset) {
  if (!reader_) return -EBADF;

  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::shared_lock<std::shared_mutex> guard(inode_->inode_lock);
  std::lock_guard<std::mutex> lock(mutex_);

  if (offset != read_offset_) {
    int ret = reader_->seek(offset);
    FAULT_INJECTION(FI_HdfsPread_SeekFail, [&] { ret = -EIO; });
    if (ret < 0) {
      LOG_ERROR("Failed to seek HDFS file: `, offset: `, ret: `", path_, offset,
                ret);
      return ret;
    }
    read_offset_ = offset;
  }

  // Use read() instead of pread() to enable SDK sequential read prefetch
  size_t total_read = 0;
  while (total_read < count) {
    ssize_t ret = reader_->read((char *)buf + total_read, count - total_read);
    FAULT_INJECTION(FI_HdfsPread_ReadFail, [&] { ret = -EIO; });
    if (ret < 0) {
      LOG_ERROR("Failed to read HDFS file: `, offset: `, count: `, ret: `",
                path_, offset, count, ret);
      return ret;
    }
    if (ret == 0) break;
    total_read += ret;
  }

  read_offset_ += total_read;
  return total_read;
}

int HdfsFileHandle::seek_writer_to_offset(off_t offset) {
  if ((flags_ & O_APPEND) || write_offset_ == offset) {
    return 0;
  }
  ssize_t r = writer_->seek(offset);
  FAULT_INJECTION(FI_HdfsWrite_SeekFail, [&] { r = -EIO; });
  if (r < 0) {
    LOG_ERROR("Failed to seek HDFS file: `, offset: `, ret: `", path_, offset,
              r);
    return r;
  }
  write_offset_ = offset;
  return 0;
}

void HdfsFileHandle::finalize_write(ssize_t total_written) {
  write_offset_ += total_written;
  if (!is_writing_) {
    is_writing_ = true;
    inode_->hdfs_dirty_count++;
    inode_->is_dirty = true;
  }
  if (write_offset_ > static_cast<off_t>(inode_->attr.size)) {
    inode_->attr.size = write_offset_;
  }
}

ssize_t HdfsFileHandle::pwrite(const void *buf, size_t count, off_t offset) {
  if (!writer_) return -EBADF;

  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);

  if (count == 0) return 0;

  int seek_ret = seek_writer_to_offset(offset);
  if (seek_ret < 0) return seek_ret;

  ssize_t written = writer_->write(buf, count);
  if (written < 0) return written;
  if (static_cast<size_t>(written) < count) return -EIO;

  finalize_write(written);
  return written;
}

ssize_t HdfsFileHandle::pin(off_t offset, size_t count, void **buf) {
  return -ENOTSUP;
}

void HdfsFileHandle::unpin(off_t offset) {}

ssize_t HdfsFileHandle::write_buf(struct fuse_bufvec *bufv, off_t offset) {
  // Design note: write_buf uses all-or-nothing semantics.
  // If any chunk fails, we return error immediately without updating
  // write_offset_ to reflect partial progress. This keeps the state
  // consistent with the caller's view (either full success or full failure).
  // Subsequent writes may fail due to stream misalignment, but this is
  // acceptable since the file is likely in an error state anyway.
  if (!writer_) return -EBADF;

  InodeRef ref = fs_->get_inode_ref(inode_->nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);

  size_t total = fuse_bufv_size(bufv);
  if (total == 0) return 0;

  int seek_ret = seek_writer_to_offset(offset);
  if (seek_ret < 0) return seek_ret;

  size_t block_size = fs_->upload_buffers_->block_size();
  auto tmp_vec = fs_->upload_buffers_->allocate(1);
  char *tmp = tmp_vec.front();
  DEFER(fs_->upload_buffers_->deallocate(tmp_vec));

  ssize_t total_written = 0;
  size_t remaining = total;

  while (remaining > 0) {
    size_t chunk = std::min(block_size, remaining);

    ssize_t copied = fuse_read_bufvec_full(tmp, bufv, chunk);
    if (copied < 0) return copied;

    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsWriteBuf_ReadError, [&] { fi_err = true; });
    if (fi_err) return -EIO;

    ssize_t written = writer_->write(tmp, copied);
    if (written < 0) return written;
    if (written < copied) {
      LOG_ERROR("HDFS short write: copied=` written=`, data loss! path: `",
                copied, written, path_);
      return -EIO;
    }

    total_written += written;
    remaining -= copied;
  }

  finalize_write(total_written);
  return total_written;
}

}  // namespace OssFileSystem
