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

#include "file.h"

#include <fcntl.h>
#include <unistd.h>

#include "common/fuse.h"
#include "common/fuse_buf_utils.h"
#include "common/macros.h"
#include "error_codes.h"
#include "fs.h"

namespace OssFileSystem {

OssFileHandle::OssFileHandle(OssFs *fs, std::string_view path, FileInode *inode,
                             std::unique_ptr<IReader> reader,
                             std::unique_ptr<IWriter> writer)
    : fs_(fs),
      inode_(inode),
      ref_cnt_(1),
      reader_(std::move(reader)),
      writer_(std::move(writer)) {
  RELEASE_ASSERT(reader_ != nullptr);
}

int OssFileHandle::fdatasync() {
  if (writer_ == nullptr) return 0;

  auto ref = fs_->get_inode_ref(inode_->nodeid,
                                OssFs::InodeRefPathType::kPathTypeRead);
  DEFER(fs_->return_inode_ref(ref));

  // We don't care whether the inode is stale or not here as if
  // the inode is stale, we will not flush any data anyway.
  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);
  return fdatasync_lock_held();
}

int OssFileHandle::close() {
  if (closed_) return -EBADF;
  closed_ = true;

  reader_->close();
  int r = fdatasync_lock_held();
  if (writer_) writer_->close();

  // Detach to avoid a dangling dirty_fh; never set in Random mode.
  if (fs_->write_mode() != WriteMode::Random && inode_->dirty_fh == this) {
    inode_->dirty_fh = nullptr;
  }

  return r;
}

ssize_t OssFileHandle::pwrite(const void *buf, size_t count, off_t offset) {
  return do_write(count, offset, buf, nullptr);
}

ssize_t OssFileHandle::write_buf(struct fuse_bufvec *bufv, off_t offset) {
  return do_write(fuse_bufv_size(bufv), offset, nullptr, bufv);
}

ssize_t OssFileHandle::do_write(size_t count, off_t offset, const void *buf,
                                struct fuse_bufvec *bufv) {
  if (writer_ == nullptr) return -ENOTSUP;
  assert(buf != nullptr || bufv != nullptr);

  if (unlikely(count == 0)) return 0;

  bool grab_path = false;

retry_with_path_lock:
  InodeRef ref;
  std::string *wpath = nullptr;
  if (grab_path) {
    ref = fs_->get_inode_ref(inode_->nodeid,
                             OssFs::InodeRefPathType::kPathTypeRead);
    wpath = &ref.inode_path;
  }
  DEFER(fs_->return_inode_ref(ref));

  std::unique_lock<std::shared_mutex> l(inode_->inode_lock);
  ssize_t r = writer_->pwrite(count, offset, buf, bufv, wpath);
  if (r == -E_READ_PATH_NEEDED) {
    grab_path = true;
    goto retry_with_path_lock;
  }

  if (r > 0 && fs_->write_mode() != WriteMode::Random) {
    if (!inode_->dirty_fh) {
      inode_->dirty_fh = this;
    }
  }

  return r;
}

int OssFileHandle::open() {
  if (!writer_) return 0;
  auto r = writer_->open();
  if (r < 0) return r;
  if (fs_->write_mode() != WriteMode::Random && writer_->get_is_dirty() &&
      !inode_->dirty_fh) {
    inode_->dirty_fh = this;
  }
  return 0;
}

ssize_t OssFileHandle::pread(void *buf, size_t count, off_t offset) {
  bool path_refreshed = false;

retry_with_path_lock:
  // Resolve the current path.
  InodeRef ref;
  if (path_refreshed) {
    ref = fs_->get_inode_ref(inode_->nodeid,
                             OssFs::InodeRefPathType::kPathTypeRead);
    if (!ref.inode) return -ESTALE;  // inode unlinked
    reader_->set_path(ref.inode_path);
  }
  DEFER(fs_->return_inode_ref(ref));

  ssize_t r;
  {
    std::shared_lock<std::shared_mutex> l(inode_->inode_lock);
    if (fs_->write_mode() == WriteMode::Streaming && writer_) {
      if (writer_->get_is_dirty()) {
        LOG_EVERY_N(1000, ALOG_ERROR,
                    "Fail to read dirty file: `, offset: `, count: `",
                    get_path(), offset, count);
        return -EBUSY;
      }
    }
    r = reader_->pread_rlocked(buf, count, offset);
  }
  // Inode rlock released.
  if (is_refill_verify_error(r)) {
    // Retry once with the refreshed path.
    if (!path_refreshed) {
      path_refreshed = true;
      goto retry_with_path_lock;
    }
    // Retry exhausted: trust the error. The anchored path really does not
    // exist, or the object was replaced externally.
    return (r == -E_REFILL_PATH_ENOENT) ? -ENOENT : -EIO;
  }
  if (r != -E_CONTINUE_READ) return r;
  return reader_->pread(buf, count, offset);
}

ssize_t OssFileHandle::pin(off_t offset, size_t count, void **buf) {
  ssize_t r = 0;
  DEFER({
    if (r > 0) ref_cnt_.fetch_add(1);
  });

  {
    std::shared_lock<std::shared_mutex> l(inode_->inode_lock);
    r = reader_->pin_rlocked(offset, count, buf);
    if (r != -E_CONTINUE_PIN) return r;
  }
  r = reader_->pin(offset, count, buf);
  return r;
}

IFileHandleFuseLL *create_oss_file_handle(OssFs *fs, const std::string &path,
                                          FileInode *inode, int flags) {
  std::unique_ptr<IWriter> writer;
  if ((flags & O_ACCMODE) != O_RDONLY) {
    writer = create_oss_writer(fs, path, inode, flags);
  }
  auto reader = create_oss_reader(fs, path, inode, fs->enable_prefetching());
  return new OssFileHandle(fs, path, inode, std::move(reader),
                           std::move(writer));
}

};  // namespace OssFileSystem
