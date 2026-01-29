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

#include <photon/fs/filesystem.h>
#include <stdint.h>

#include <string_view>

struct fuse_bufvec;
struct fuse_buf;
struct fuse_session;

//
// Fuse ensures that no further operations are performed
// on a file handle after it has been released. Implement a
// custom delete function within the release method to facilitate
// better lifecycle management and good performance.
//
// The fuse_release of file handle might be executed before
// the unpin().
//
class IFileHandleFuseLL {
 public:
  virtual int open() = 0;
  virtual int close() = 0;
  virtual void release() = 0;

  virtual int fsync() = 0;
  virtual int fdatasync() = 0;

  virtual ssize_t pread(void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset) = 0;

  virtual ssize_t pin(off_t offset, size_t count, void **buf) = 0;
  virtual void unpin(off_t offset) = 0;

  virtual ssize_t write_buf(struct fuse_bufvec *bufv, off_t offset) = 0;

 protected:
  virtual ~IFileHandleFuseLL() {}
};

class IFileSystemFuseLL {
 public:
  IFileSystemFuseLL() {}
  virtual ~IFileSystemFuseLL() {}

  void set_fuse_session(struct fuse_session *fuse_se) {
    fuse_se_ = fuse_se;
  }

  virtual int lookup(uint64_t parent, std::string_view name, uint64_t *nodeid,
                     struct stat *stbuf) = 0;
  virtual int forget(uint64_t nodeid, uint64_t nlookup) = 0;
  virtual int getattr(uint64_t nodeid, struct stat *stbuf) = 0;
  virtual int setattr(uint64_t nodeid, struct stat *stbuf, int to_set) = 0;
  virtual int statfs(struct statvfs *stbuf) = 0;
  virtual int rename(uint64_t old_parent, std::string_view old_name,
                     uint64_t new_parent, std::string_view new_name,
                     unsigned int flags) = 0;
  virtual int unlink(uint64_t parent, std::string_view name) = 0;

  virtual int open(uint64_t nodeid, int flags, void **fh,
                   bool *keep_page_cache) = 0;
  virtual int creat(uint64_t parent, std::string_view name, int flags,
                    mode_t mode, uid_t uid, gid_t gid, mode_t umask,
                    uint64_t *nodeid, struct stat *stbuf, void **fh) = 0;
  virtual int release(uint64_t nodeid, void *fh) = 0;

  virtual int opendir(uint64_t nodeid, void **dh) = 0;
  virtual int readdir(uint64_t nodeid, off_t off, void *dh,
                      int (*filler)(void *ctx, uint64_t nodeid,
                                    const char *name, const struct stat *stbuf,
                                    off_t off),
                      void *filler_ctx, int (*is_interrupted)(void *ctx),
                      bool readdirplus, void *interrupted_ctx) = 0;
  virtual int releasedir(uint64_t nodeid, void *dh) = 0;
  virtual int mkdir(uint64_t parent, std::string_view name, mode_t mode,
                    uid_t uid, gid_t gid, mode_t umask, uint64_t *nodeid,
                    struct stat *stbuf) = 0;
  virtual int rmdir(uint64_t parent, std::string_view name) = 0;

  virtual int symlink(uint64_t parent, std::string_view name,
                      std::string_view link, uid_t uid, gid_t gid,
                      uint64_t *nodeid, struct stat *stbuf) = 0;
  virtual ssize_t readlink(uint64_t nodeid, char *buf, size_t size) = 0;

  struct fuse_session *fuse_se_ = nullptr;
};
