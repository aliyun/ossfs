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

#include "common/filesystem.h"
#include "file_reader.h"
#include "file_writer.h"
#include "inode.h"
#include "test/class_declarations.h"

namespace OssFileSystem {

class OssFs;

class OssFileHandle : public IFileHandleFuseLL {
 public:
  int fsync() override {
    return fdatasync();
  }
  int fdatasync() override;
  int close() override;
  int open() override;

  ssize_t pread(void *buf, size_t count, off_t offset) override;
  ssize_t pin(off_t offset, size_t count, void **buf) override;

  void unpin(off_t offset) override {
    reader_->unpin(offset);
    release();
  }

  void release() override {
    if (ref_cnt_.fetch_sub(1) == 1) {
      // It is safe to delete this object here because FUSE will not use this
      // file handle anymore after the fuse_release operation. The ref_cnt_
      // mechanism ensures that no other threads are accessing this object
      // when the count reaches zero.
      delete this;
    }
  }

  int fdatasync_lock_held() {
    return writer_ ? writer_->flush() : 0;
  }

  // It's not supported to open the same file multiple times for writing.
  ssize_t pwrite(const void *buf, size_t count, off_t offset) override;
  ssize_t write_buf(struct fuse_bufvec *bufv, off_t offset) override;

  FileInode *get_inode() {
    return inode_;
  }

  std::string_view get_path() {
    return reader_->get_path();
  }

  bool get_is_immutable() {
    return writer_ ? writer_->get_is_immutable() : true;
  }

  // The following functions need inode's rlock.
  ssize_t pread_from_upload_buffer(void *buf, size_t count, off_t offset) {
    return writer_ ? writer_->pread_from_upload_buffer(buf, count, offset) : 0;
  }
  size_t calc_remote_size() {
    return writer_ ? writer_->calc_remote_size() : inode_->attr.size;
  }

  OssFileHandle(OssFs *fs, std::string_view path, FileInode *inode,
                std::unique_ptr<IReader> reader,
                std::unique_ptr<IWriter> writer);

 private:
  bool get_is_dirty() {
    return writer_ ? writer_->get_is_dirty() : false;
  }

  virtual ~OssFileHandle() = default;

  ssize_t do_write(size_t count, off_t offset, const void *buf,
                   struct fuse_bufvec *bufv);

  OssFs *fs_ = nullptr;
  FileInode *inode_ = nullptr;

  bool closed_ = false;

  std::atomic<uint64_t> ref_cnt_ = {0};

  std::unique_ptr<IReader> reader_;
  std::unique_ptr<IWriter> writer_;

  DECLARE_TEST_FRIENDS_CLASSES;
};

IFileHandleFuseLL *create_oss_file_handle(OssFs *fs, const std::string &path,
                                          FileInode *inode, int flags);

};  // namespace OssFileSystem
