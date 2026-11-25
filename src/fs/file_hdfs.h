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

  // HDFS-specific variants taking the caller-resolved fresh path; used by
  // callers that already hold an InodeRef (setattr/fallocate/release).
  int ftruncate_with_path(std::string_view cur_path, off_t target_size);
  int fallocate_with_path(std::string_view cur_path, off_t offset,
                          off_t length);
  int close_with_path(std::string_view cur_path);

  ssize_t pread(void *buf, size_t count, off_t offset) override;
  ssize_t pwrite(const void *buf, size_t count, off_t offset) override;

  ssize_t pin(off_t offset, size_t count, void **buf) override;
  void unpin(off_t offset) override;

  ssize_t write_buf(struct fuse_bufvec *bufv, off_t offset) override;

  void *get_inode() override {
    return inode_;
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
  int open_with_flags(std::string_view cur_path, int flags);

  // Close backend streams only, keeping the flock state intact.
  int close_streams(std::string_view cur_path);

  void release_flock(std::string_view cur_path);

  int seek_writer_to_offset(std::string_view path, off_t offset);

  // Close the stale reader stream and reopen it at the post-rename path.
  int rebuild_reader_after_rename(std::string_view new_path);

  void finalize_write(ssize_t total_written);

  void commit_written_size();

  OssFs *fs_ = nullptr;
  FileInode *inode_ = nullptr;
  // Path the reader stream is currently opened at. Used ONLY to detect rename
  // staleness in pread; never pass it to a path-resolving backend op -- fresh
  // paths come from the caller's InodeRef via the *_with_path methods.
  std::string reader_path_;
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
  // High-water mark of bytes written; write_offset_ retreats on random writes.
  off_t max_write_end_ = 0;

  DECLARE_TEST_FRIENDS_CLASSES;
};

}  // namespace OssFileSystem
