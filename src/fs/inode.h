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

// clang-format off
// photon/common/string-keyed.h needs this file in gcc 10, remove
// this after upgraded photon
#include <stdlib.h>
// clang-format on

#include <fcntl.h>
#include <photon/common/range-lock.h>
#include <stdint.h>
#include <time.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "cache.h"
#include "common/logger.h"
#include "common/macros.h"
#include "random_write_context.h"

namespace OssFileSystem {

class OssFileHandle;
static constexpr int64_t kMountPointNodeId = 1;

enum InodeType : uint8_t {
  kFile = 0,
  kDir = 1,
  kSymlink = 2,
};

struct Attribute {
  uint64_t size;
  struct timespec mtime;

  static blksize_t DEFAULT_BLKSIZE;

  Attribute() : size(0), mtime{} {}

  Attribute(uint64_t file_size, struct timespec file_mtime)
      : size(file_size), mtime(file_mtime) {}

  bool operator==(const struct Attribute &attr) const {
    if (size != attr.size || mtime.tv_nsec != attr.mtime.tv_nsec ||
        mtime.tv_sec != attr.mtime.tv_sec) {
      return false;
    }

    return true;
  }

  static mode_t get_file_type_bits(InodeType type) {
    switch (type) {
      case InodeType::kDir:
        return S_IFDIR;
      case InodeType::kSymlink:
        return S_IFLNK;
      default:
        return S_IFREG;
    }
  }

  static mode_t get_default_full_mode(InodeType type) {
    switch (type) {
      case InodeType::kDir:
        return S_IFDIR | DEFAULT_DIR_MODE;
      case InodeType::kSymlink:
        return S_IFLNK | 0777;
      default:
        return S_IFREG | DEFAULT_FILE_MODE;
    }
  }

  static mode_t build_mode(InodeType type, mode_t perm) {
    return get_file_type_bits(type) | perm;
  }

  static void set_default_gid_uid(gid_t gid, uid_t uid) {
    DEFAULT_GID = gid;
    DEFAULT_UID = uid;
  }

  static void set_default_mode(mode_t dir_mode, mode_t file_mode) {
    DEFAULT_DIR_MODE = dir_mode;
    DEFAULT_FILE_MODE = file_mode;
  }

  static gid_t DEFAULT_GID;
  static uid_t DEFAULT_UID;
  static mode_t DEFAULT_DIR_MODE;
  static mode_t DEFAULT_FILE_MODE;
};

// Posix extended attributes (mode/uid/gid/atime).
struct PosixExtAttr {
  mode_t mode = 0;
  uid_t uid = 0;
  gid_t gid = 0;
  struct timespec atime = {};
};

struct Inode {
  const uint64_t nodeid;
  std::string name;
  struct Attribute attr;
  const InodeType type = InodeType::kFile;

  // Once is_stale is set to true, it won't be false again.
  // If an inode is stale, its parent-child relationship will be remained.
  std::atomic<bool> is_stale = ATOMIC_VAR_INIT(false);

  // Renamed to ".fuse_hiddenXXX" by hide_inode; removed on last release.
  bool is_hidden = false;

  // pathlock == 0: no lock
  // pathlock == PATHLOCK_WRITE(-1): held write lock
  // pathlock > 0: held read lock
  // pathlock < -1(PATHLOCK_WAIT_OFFSET + n): held read lock and some write op
  // is waiting
  // Placed here to fill the alignment hole before attr_time.
  int pathlock = 0;  // access only in global map lock scope

  time_t attr_time = 0;

  // The authoritative source is the libfuse documentation, which states that
  // any op that returns fuse_reply_entry fuse_reply_create implicitly
  // increments lookup_cnt: create, lookup, mkdir, symlink, link, readdirplus
  // root node implicitly increments at first.
  uint64_t lookup_cnt = 0;
  int open_ref_cnt = 0;

  // increment: get_inode_ref
  // decrement: return_inode_ref or modify it with global map lock held
  int ref_ctr = 0;

  // For reverse lookup, such as get_full_path.
  uint64_t parent_nodeid = 0;
  Inode *parent = nullptr;

  std::shared_mutex inode_lock;

  std::unique_ptr<PosixExtAttr> posix_ext;

  Inode(uint64_t file_ino, std::string_view file_name, uint64_t file_size,
        struct timespec file_mtime, InodeType type, uint64_t parent_id,
        Inode *parent_node)
      : nodeid(file_ino),
        name(file_name.data(), file_name.size()),
        attr(file_size, file_mtime),
        type(type),
        attr_time(time(nullptr)),
        parent_nodeid(parent_id),
        parent(parent_node) {}

  virtual ~Inode() = default;

  virtual bool is_attr_valid(uint64_t timeout) const {
    return attr_time > 0 && ::difftime(time(nullptr), attr_time) < timeout;
  }

  virtual bool can_be_invalidated() const;

  bool is_dir() const {
    return type == InodeType::kDir;
  }
  bool is_file() const {
    return type == InodeType::kFile;
  }
  bool is_symlink() const {
    return type == InodeType::kSymlink;
  }

  void increment_lookupcnt() {
    lookup_cnt++;
  }

  void decrement_lookupcnt(uint64_t n) {
    RELEASE_ASSERT_WITH_MSG(lookup_cnt >= n,
                            "nodeid: `, lookup_cnt: `, which should be >= `",
                            nodeid, lookup_cnt, n);
    lookup_cnt -= n;
  }

  void update_attr(uint64_t file_size, struct timespec file_mtime,
                   struct timespec file_atime = {});
  void fill_statbuf(struct stat *stbuf) const;

  PosixExtAttr *ensure_posix_ext() {
    if (!posix_ext) posix_ext = std::make_unique<PosixExtAttr>();
    return posix_ext.get();
  }

  mode_t get_mode() const {
    return posix_ext ? posix_ext->mode : Attribute::get_default_full_mode(type);
  }
  uid_t get_uid() const {
    return posix_ext ? posix_ext->uid : Attribute::DEFAULT_UID;
  }
  gid_t get_gid() const {
    return posix_ext ? posix_ext->gid : Attribute::DEFAULT_GID;
  }
  struct timespec get_atime() const {
    return posix_ext ? posix_ext->atime : attr.mtime;
  }
  struct timespec get_mtime() const {
    return attr.mtime;
  }

  void set_mode(mode_t mode) {
    if (posix_ext) posix_ext->mode = mode;
  }
  void set_uid(uid_t uid) {
    if (posix_ext) posix_ext->uid = uid;
  }
  void set_gid(gid_t gid) {
    if (posix_ext) posix_ext->gid = gid;
  }
  void set_atime(struct timespec atime) {
    if (posix_ext) posix_ext->atime = atime;
  }
  void set_mtime(struct timespec mtime) {
    attr.mtime = mtime;
  }

  static InodeType dirent_type_to_inode_type(unsigned char dtype);
  static InodeType mode_to_inode_type(mode_t mode);
  static std::string inode_type_to_string(InodeType type);
};  // struct Inode

struct FileInode final : public Inode {
  bool is_dirty = false;
  bool invalidate_data_cache = false;

  // Per-mode write runtime state sharing one slot (write mode is fixed per
  // mount): dirty_fh (Streaming/Appendable), rw_ctx (Random, owning),
  // hdfs_dirty_count (HDFS).
  union {
    OssFileHandle *dirty_fh = nullptr;
    int64_t hdfs_dirty_count;
    RandomWriteContext *rw_ctx;
  };

  std::string etag;

  std::shared_ptr<ICache> cache;

  FileInode(uint64_t file_ino, std::string_view file_name, uint64_t file_size,
            struct timespec file_mtime, InodeType type, bool is_dirty,
            uint64_t parent_id, Inode *parent_node, std::string_view etag)
      : Inode(file_ino, file_name, file_size, file_mtime, type, parent_id,
              parent_node),
        is_dirty(is_dirty),
        invalidate_data_cache(false),
        dirty_fh(nullptr),
        etag(etag) {}

  bool is_attr_valid(uint64_t timeout) const override {
    if (is_dirty) return true;

    return Inode::is_attr_valid(timeout);
  }

  void invalidate_data_cache_if_needed(const struct stat *stbuf,
                                       std::string_view remote_etag);

  bool is_dirty_file() const {
    return is_dirty;
  }

  bool is_data_changed(const struct stat *stbuf,
                       std::string_view remote_etag) const;
};  // FileInode

struct DirInode final : public Inode {
  std::unordered_map<std::string_view, Inode *> children;
  std::chrono::steady_clock::time_point kernel_readdir_cache_time = {};

  DirInode(uint64_t file_ino, std::string_view file_name,
           struct timespec file_mtime, uint64_t parent_id, Inode *parent_node)
      : Inode(file_ino, file_name, 0, file_mtime, InodeType::kDir, parent_id,
              parent_node) {}

  bool can_be_invalidated() const override;
  bool is_dir_empty(std::string *child_name = nullptr) const;
  void add_child_node(Inode *inode);
  void add_child_node_directly(Inode *inode);
  Inode *find_child_node(std::string_view name) const;
  void erase_child_node(std::string_view name, const uint64_t noid);

  bool is_kernel_readdir_cache_valid(int64_t timeout) const {
    auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - kernel_readdir_cache_time)
            .count();
    return elapsed < timeout;
  }

  void update_kernel_readdir_cache_status() {
    kernel_readdir_cache_time = std::chrono::steady_clock::now();
  }

  bool is_children_empty() const {
    return children.empty();
  }
};  // DirInode

struct InodeRef {
  Inode *inode = nullptr;
  std::string inode_path;
  bool acquire_write_path = false;
};

struct ParentRef {
  DirInode *parent = nullptr;
  Inode *inode = nullptr;
  std::string parent_path;
};

struct ParentRef2 {
  ParentRef ref1;
  ParentRef ref2;
};

struct DentryView {
  uint64_t parent;
  uint64_t nodeid;
  std::string_view name;
};

inline std::string inode_number_to_string(uint64_t inode) {
  return std::to_string(inode);
}

}  // namespace OssFileSystem
