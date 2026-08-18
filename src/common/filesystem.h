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
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Reserved uid/gid for unresolved names (nobody user).
// Used by backend when username/groupname resolution fails.
// Also used by FUSE adapter to replace backend reserved values with req.
// context.
constexpr uid_t kReservedUnresolvedUid = 99;
constexpr gid_t kReservedUnresolvedGid = 99;

// Permission bits mask (lower 9 bits: rwxrwxrwx).
constexpr mode_t kPermMask = 0777;

// Test-only mapping table for uid/gid <-> username/groupname.
// Used by FAULT_INJECTION(FI_Hdfs_UserGroup_Mapping) to mock NSS lookups
// in unit tests without requiring real system users/groups.
struct UserGroupMapping {
  std::unordered_map<uid_t, std::string> uid_to_name;
  std::unordered_map<std::string, uid_t> name_to_uid;
  std::unordered_map<gid_t, std::string> gid_to_name;
  std::unordered_map<std::string, gid_t> name_to_gid;
  // gid -> list of usernames in that group (simulates gr_mem)
  std::unordered_map<gid_t, std::vector<std::string>> group_members;
};
extern UserGroupMapping g_test_user_mapping;

// Resolve uid to username string. Returns empty string on failure.
// Uses FI_Hdfs_UserGroup_Mapping for test mocking when enabled.
std::string uid_to_username(uid_t uid);

// Resolve gid to groupname string. Returns empty string on failure.
// Uses FI_Hdfs_UserGroup_Mapping for test mocking when enabled.
std::string gid_to_groupname(gid_t gid);

// HDFS permission check. Uses OR check for loose pre-checking.
// Supplementary group membership is checked via getgrgid_r + gr_mem.
int check_hdfs_access(const struct stat *stbuf, int mask, uid_t current_uid,
                      gid_t current_gid);

// Permission check operation types.
// Covers all operations that VFS default_permissions would check.
// The actual permission enforcement strategy is backend-specific:
// some backends may implement all checks at the store layer,
// while others rely on VFS default_permissions for most operations.
enum class PermOp : int {
  // File content access.
  Open,       // open() - R/W based on flags
  Truncate,   // truncate() - W_OK on file
  Ftruncate,  // ftruncate() - W_OK on file
  // Metadata modification.
  Chmod,      // chmod() - owner or CAP_FOWNER
  Chown,      // chown() - CAP_CHOWN
  Utimensat,  // utimensat() - owner/W_OK/EPERM
  Setxattr,   // setxattr() - owner check
  // Directory operations (parent dir W_OK+X_OK)
  Mkdir,
  Rmdir,
  Mknod,
  Create,  // open(O_CREAT)
  Link,
  Symlink,
  // Removal / rename
  Unlink,
  Rename,
};

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

  virtual int ftruncate(off_t length) = 0;
  virtual int fallocate(off_t offset, off_t length) = 0;

  virtual void *get_inode() = 0;

 protected:
  virtual ~IFileHandleFuseLL() {}
};

class IFileSystemFuseLL {
 public:
  IFileSystemFuseLL() = default;
  virtual ~IFileSystemFuseLL() = default;

  void set_fuse_session(struct fuse_session *fuse_se) {
    fuse_se_ = fuse_se;
  }

  virtual int lookup(uint64_t parent, std::string_view name, uint64_t *nodeid,
                     struct stat *stbuf) = 0;
  virtual int forget(uint64_t nodeid, uint64_t nlookup) = 0;
  virtual int getattr(uint64_t nodeid, struct stat *stbuf) = 0;
  virtual int setattr(uint64_t nodeid, struct stat *stbuf, int to_set,
                      struct fuse_file_info *fi = nullptr, uid_t caller_uid = 0,
                      gid_t caller_gid = 0) = 0;
  virtual int statfs(struct statvfs *stbuf) = 0;
  virtual int rename(uint64_t old_parent, std::string_view old_name,
                     uint64_t new_parent, std::string_view new_name,
                     unsigned int flags) = 0;
  virtual int unlink(uint64_t parent, std::string_view name,
                     uid_t caller_uid = 0, gid_t caller_gid = 0) = 0;

  virtual int open(uint64_t nodeid, int flags, void **fh,
                   bool *keep_page_cache) = 0;
  virtual int creat(uint64_t parent, std::string_view name, int flags,
                    mode_t mode, uid_t uid, gid_t gid, mode_t umask,
                    uint64_t *nodeid, struct stat *stbuf, void **fh) = 0;
  virtual ssize_t read(uint64_t nodeid, void *fh, size_t size, off_t off,
                       std::function<void(void *buf, size_t size)> read_cb) = 0;
  virtual ssize_t write(uint64_t nodeid, void *fh, const char *buf, size_t size,
                        off_t off) = 0;
  virtual ssize_t write_buf(uint64_t nodeid, void *fh, struct fuse_bufvec *bufv,
                            off_t off) = 0;
  virtual int fsync(uint64_t nodeid, void *fh, bool datasync) = 0;
  virtual int flush(uint64_t nodeid, void *fh) = 0;
  virtual int release(uint64_t nodeid, void *fh) = 0;

  virtual int opendir(uint64_t nodeid, struct fuse_file_info *fi) = 0;
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

  // TODO: Mark functions below as pure virtual.
  virtual int mknod(uint64_t parent, std::string_view name, mode_t mode,
                    uid_t uid, gid_t gid, uint64_t *nodeid,
                    struct stat *stbuf) {
    return -ENOSYS;
  }

  virtual int access(uint64_t nodeid, int mask, uid_t caller_uid,
                     gid_t caller_gid) {
    return -ENOSYS;
  }

  virtual int fallocate(uint64_t nodeid, off_t offset, off_t length, void *fh) {
    return -ENOSYS;
  }

  virtual int flock(uint64_t nodeid, void *fh, int op, uint64_t lock_owner) {
    return -ENOSYS;
  }

  virtual int setxattr(uint64_t nodeid, const char *name, const char *value,
                       size_t size, int flags) {
    return -ENOSYS;
  }
  virtual int getxattr(uint64_t nodeid, const char *name, char *value,
                       size_t size) {
    return -ENOSYS;
  }
  virtual int listxattr(uint64_t nodeid, char *list, size_t size) {
    return -ENOSYS;
  }
  virtual int removexattr(uint64_t nodeid, const char *name) {
    return -ENOSYS;
  }

  struct fuse_session *fuse_se_ = nullptr;
};
