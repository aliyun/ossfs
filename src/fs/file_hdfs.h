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
#include "inode.h"
#include "oss/obj_store.h"
#include "test/class_declarations.h"

namespace OssFileSystem {

class OssFs;

// HdfsFileHandle: File handle for HDFS backend using RawObjHandle (stream IO).
class HdfsFileHandle : public IFileHandleFuseLL {
 public:
  HdfsFileHandle(OssFs *fs, std::string_view path, FileInode *inode, int flags,
                 mode_t mode = 0777);
  ~HdfsFileHandle() override;

  int open() override;
  int close() override;
  void release() override;

  int fsync() override;
  int fdatasync() override;

  int ftruncate(off_t target_size) override;
  int fallocate(off_t offset, off_t length) override;

  ssize_t pread(void *buf, size_t count, off_t offset) override;
  ssize_t pwrite(const void *buf, size_t count, off_t offset) override;

  ssize_t pin(off_t offset, size_t count, void **buf) override;
  void unpin(off_t offset) override;

  ssize_t write_buf(struct fuse_bufvec *bufv, off_t offset) override;

  void *get_inode() override {
    return inode_;
  }

  std::string get_path() {
    return path_;
  }

  // flock state tracking for release-on-close.
  void set_flock_held(uint64_t owner) {
    holds_flock_ = true;
    flock_owner_ = owner;
  }
  void clear_flock_held() {
    holds_flock_ = false;
    flock_owner_ = 0;
  }
  bool holds_flock() const {
    return holds_flock_;
  }
  uint64_t flock_owner() const {
    return flock_owner_;
  }

 private:
  int seek_writer_to_offset(off_t offset);

  void finalize_write(ssize_t total_written);

  OssFs *fs_ = nullptr;
  FileInode *inode_ = nullptr;
  std::string path_;
  int flags_ = 0;
  mode_t mode_ = 0777;

  std::mutex mutex_;
  RawObjHandle *reader_ = nullptr;
  RawObjHandle *writer_ = nullptr;

  bool closed_ = false;
  bool is_writing_ = false;
  bool holds_flock_ = false;
  uint64_t flock_owner_ = 0;

  off_t read_offset_ = 0;
  off_t write_offset_ = 0;

  DECLARE_TEST_FRIENDS_CLASSES;
};

}  // namespace OssFileSystem
