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

#include "fs.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <linux/fs.h>
#include <photon/common/iovector.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <queue>
#include <thread>

#include "common/fuse.h"
#include "common/macros.h"
#include "common/utils.h"
#include "error_codes.h"
#include "file.h"

#define GET_INODE_REF_ONLY_WITH_RET(id)                            \
  auto ref = get_inode_ref((id), InodeRefPathType::kPathTypeNone); \
  DEFER(return_inode_ref(ref));                                    \
  if (!ref.inode) return -ESTALE;

#define GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(id)         \
  auto ref = get_inode_ref((id), InodeRefPathType::kPathTypeRead); \
  DEFER(return_inode_ref(ref));                                    \
  if (!ref.inode) return -ESTALE;

#define GET_PARENT_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(pid, name) \
  auto ref = get_inode_ref((pid), (name));                         \
  DEFER(return_inode_ref(ref));                                    \
  if (!ref.parent) return -ESTALE;

#define GET_PARENT_REF2_AND_LOCK_PATH_IF_NEEDED_WITH_RET(pid1, name1, pid2, \
                                                         name2)             \
  auto ref = get_inode_ref((pid1), (name1), (pid2), (name2));               \
  DEFER(return_inode_ref(ref));                                             \
  if (!ref.ref1.parent || !ref.ref2.parent || !ref.ref1.inode) return -ESTALE;

namespace OssFileSystem {

const uint64_t TEMP_NODEID = std::numeric_limits<uint64_t>::max();

std::atomic<uint64_t> InodeManager::g_nodeid_ = ATOMIC_VAR_INIT(0);
std::atomic<uint64_t> NegativeCache::create_cache_hit_cnt_ = ATOMIC_VAR_INIT(0);
std::atomic<uint64_t> NegativeCache::lookup_cache_hit_cnt_ = ATOMIC_VAR_INIT(0);

gid_t Attribute::DEFAULT_GID = 0;
uid_t Attribute::DEFAULT_UID = 0;
mode_t Attribute::DEFAULT_DIR_MODE = 0755;
mode_t Attribute::DEFAULT_FILE_MODE = 0644;

OssFs::OssFs(const OssFsOptions &options, BackgroundVCpuEnv bg_vcpu_env)
    : options_(options),
      bg_vcpu_env_(bg_vcpu_env),
      prefetch_sem_(
          std::make_unique<photon::semaphore>(options_.prefetch_concurrency)),
      upload_sem_(
          std::make_unique<photon::semaphore>(options_.upload_concurrency)),
      upload_copy_sem_(std::make_unique<photon::semaphore>(
          options_.upload_copy_concurrency)),
      rename_sem_(
          std::make_unique<photon::semaphore>(options_.rename_dir_concurrency)),
      total_create_cnt_(0) {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  mp_inode_ = create_new_inode(kMountPointNodeId, "", 0, now, InodeType::kDir,
                               false, 0, nullptr, "");
  mp_inode_->increment_lookupcnt();
  add_new_inode_to_global_map(mp_inode_);
  InodeManager::init(1);

  Attribute::set_default_gid_uid(options_.gid, options_.uid);
  Attribute::set_default_mode(options_.dir_mode, options_.file_mode);

  init_prefetch_options();
  upload_buffers_ = new FixedBlockMemoryPool(options_.upload_buffer_size,
                                             options_.upload_concurrency + 4,
                                             options_.upload_concurrency + 4);
  if (options_.cache_type == CacheType::kFhCache && enable_prefetching()) {
    size_t blocks_per_prefetch_chunk =
        (options_.prefetch_chunk_size + options_.cache_block_size - 1) /
        options_.cache_block_size;
    size_t cached_block_count =
        blocks_per_prefetch_chunk * options_.prefetch_concurrency * 3;
    size_t pool_capcacity = cached_block_count;

    // Override if user specified.
    if (options_.prefetch_chunks > 0) {
      cached_block_count = blocks_per_prefetch_chunk * options_.prefetch_chunks;
      pool_capcacity = cached_block_count;
    } else if (options_.prefetch_chunks < 0) {
      // Unlimited mode.
      pool_capcacity = std::numeric_limits<size_t>::max();
    }

    download_buffers_ = new FixedBlockMemoryPool(
        options_.cache_block_size, pool_capcacity, cached_block_count);
  }

  if (enable_staged_cache()) {
    // We don't evict inodes when inserting, but inside the eviction background
    // thread.
    staged_inodes_cache_ = new StagedInodeCache(options_.attr_timeout);
  }

  if (enabled_negative_cache()) {
    negative_cache_ = new NegativeCache(options_.oss_negative_cache_timeout,
                                        options_.oss_negative_cache_size);
  }
}

OssFs::~OssFs() {
  is_stopping_ = true;

#define JOIN_AND_DELETE_THREAD(th) \
  if (th) {                        \
    th->join();                    \
    delete th;                     \
    th = nullptr;                  \
  }
#define DELETE_VAR(var) \
  if (var) {            \
    delete var;         \
    var = nullptr;      \
  }

  JOIN_AND_DELETE_THREAD(creds_refresh_th_);
  JOIN_AND_DELETE_THREAD(uds_server_th_);
  JOIN_AND_DELETE_THREAD(reverse_invalidate_th_);
  JOIN_AND_DELETE_THREAD(health_check_th_);
  JOIN_AND_DELETE_THREAD(transmission_control_th_);

  DELETE_VAR(creds_provider_);
  DELETE_VAR(staged_inodes_cache_);
  DELETE_VAR(negative_cache_);

  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    LOG_INFO("Remained inodes number: `", global_inodes_map_.size());
    for (auto &it : global_inodes_map_) {
      delete it.second;
    }
    global_inodes_map_.clear();
  }

  if (options_.cache_type == CacheType::kFhCache) {
    DELETE_VAR(download_buffers_);
  }
  DELETE_VAR(upload_buffers_);

#undef JOIN_AND_DELETE_THREAD
#undef DELETE_VAR
}

// ********************* metadata related apis *********************
// parent's rlock + inode's wlock -> release -> parent's wlock + inode's wlock
int OssFs::lookup(uint64_t parent, std::string_view name, uint64_t *nodeid,
                  struct stat *stbuf) {
  if (name.size() > kOssfsMaxFileNameLength) return -ENAMETOOLONG;

  // Consult VFS code (namei.c): path_lookupat() -> link_path_walk() ->
  // walk_component() -> handle_dots().
  // "." and ".." are handled by VFS so fuse daemon will not receive a lookup
  // request with name of "." or "..". The only exception is when ossfs2
  // mountpath is exported as an NFS share, but this needs the flag
  // FUSE_CAP_EXPORT_SUPPORT being set, which is not set by us now.
  DirInode *parent_inode = nullptr;
  int r = 0;

  {
    GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent);
    RELEASE_ASSERT_WITH_MSG(ref.inode->is_dir(),
                            "lookup: parent inode: `, is not a directory",
                            parent);
    parent_inode = static_cast<DirInode *>(ref.inode);

    std::string full_path(ref.inode_path);
    if (full_path.back() != '/') full_path.append("/");
    full_path.append(name.data(), name.size());

    r = lookup_with_inode_ref(parent_inode, name, full_path, false, nullptr,
                              stbuf);
    if (r != -E_WRITE_PATH_NEEDED) {
      if (r == 0) *nodeid = stbuf->st_ino;
      return r;
    }
  }

  GET_PARENT_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent, name);
  parent_inode = ref.parent;
  Inode *wlocked_inode = ref.inode;

  std::string full_path(ref.parent_path);
  if (full_path.back() != '/') full_path.append("/");
  full_path.append(name.data(), name.size());
  r = lookup_with_inode_ref(parent_inode, name, full_path, true, wlocked_inode,
                            stbuf);
  if (r == 0) *nodeid = stbuf->st_ino;
  return r;
}

// parent's wlock (if parent exists) + inode's wlock
int OssFs::forget(uint64_t nodeid, uint64_t nlookup) {
  if (nodeid == kMountPointNodeId) return 0;

  if (IS_FAULT_INJECTION_ENABLED(FI_Forget_Delay)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  if (!enable_staged_cache()) {
    try_invalidate_inode(nodeid, nlookup, true);
    return 0;
  }
  return forget_and_insert_to_staged_cache(nodeid, nlookup);
}

// inode's rlock -> release -> inode's wlock
int OssFs::getattr(uint64_t nodeid, struct stat *stbuf) {
  if (nodeid == mp_inode_->nodeid) {
    std::shared_lock<std::shared_mutex> mprl(mp_inode_->inode_lock);
    mp_inode_->fill_statbuf(stbuf);
    return 0;
  }

  bool acquire_write_path_lock = false;

retry_with_write_path_lock:
  int r = 0;
  auto path_type = acquire_write_path_lock ? InodeRefPathType::kPathTypeWrite
                                           : InodeRefPathType::kPathTypeRead;
  auto ref = get_inode_ref(nodeid, path_type);
  if (!ref.inode) return -ESTALE;
  DEFER(return_inode_ref(ref));

  Inode *inode = ref.inode;
  const auto &full_path = ref.inode_path;

  // Return true: no need to keep going.
  auto fill_if_not_need_check = [&]() -> bool {
    if (inode->is_stale) {
      r = -ESTALE;
      return true;
    }

    if (inode->is_attr_valid(options_.attr_timeout)) {
      inode->fill_statbuf(stbuf);
      r = 0;
      return true;
    }

    return false;
  };

  {
    std::shared_lock<std::shared_mutex> l(inode->inode_lock);
    if (fill_if_not_need_check()) {
      // cache hit
      return r;
    }
  }

  // cache miss
  std::unique_lock<std::shared_mutex> wl(inode->inode_lock);
  if (fill_if_not_need_check()) {
    return r;
  }

  std::string remote_etag;
  r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_stat, full_path, stbuf,
                                     &remote_etag);
  if (r == 0) {
    InodeType new_type = Inode::mode_to_inode_type(stbuf->st_mode);
    if (inode->type != new_type) {
      LOG_ERROR("inode type changed for ` from ` to `", full_path,
                Inode::inode_type_to_string(inode->type),
                Inode::inode_type_to_string(new_type));
      r = -ENOENT;  // just go through the reverse delete process
    }
  }

  if (r < 0) {
    LOG_ERROR(
        "[getattr] fail to stat from cloud. nodeid `, path ` with error `",
        nodeid, full_path, r);
    if (r == -ENOENT) {
      if (inode->is_dir() && options_.allow_rename_dir &&
          options_.allow_mark_dir_stale_recursively &&
          !static_cast<DirInode *>(inode)->is_dir_empty()) {
        if (!acquire_write_path_lock) {
          acquire_write_path_lock = true;
          goto retry_with_write_path_lock;
        }

        mark_inode_stale_if_needed(inode, true);
        LOG_DEBUG("mark inode stale recursively for dir ` `", nodeid,
                  full_path);
      } else {
        mark_inode_stale_if_needed(inode, false);
      }
    }
    return r;
  }

  invalidate_data_cache_if_needed(inode, stbuf, remote_etag);
  update_inode_etag(inode, remote_etag);
  inode->update_attr(stbuf->st_size, stbuf->st_mtim);
  inode->fill_statbuf(stbuf);
  return 0;
}

// inode's wlock
// Only mtime and truncation to 0 are supported.
int OssFs::setattr(uint64_t nodeid, struct stat *stbuf, int to_set) {
  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  Inode *inode = ref.inode;

  std::unique_lock<std::shared_mutex> l(inode->inode_lock);
  if (inode->is_stale) return -ESTALE;

  if (to_set & FUSE_SET_ATTR_MTIME) {
    inode->update_attr(inode->attr.size, stbuf->st_mtim);
  } else if (to_set & FUSE_SET_ATTR_SIZE) {
    if (stbuf->st_size != 0) {
      LOG_WARN("nodeid ` truncate to non-zero is not supported.", nodeid);
      return -ENOTSUP;
    }
    if (inode->is_dir()) return -EISDIR;

    if (inode->attr.size == 0) goto exit;

    if (static_cast<FileInode *>(inode)->is_dirty_file()) return -EBUSY;

    int r = truncate_inode_data(inode, ref.inode_path, 0);
    if (r < 0) return r;
  }

exit:
  // FUSE needs refill stat buffer.
  inode->fill_statbuf(stbuf);
  return 0;
}

int OssFs::statfs(struct statvfs *stbuf) {
  memset(stbuf, 0, sizeof(struct statvfs));
  stbuf->f_bsize = 0x2000;           // Filesystem block size
  stbuf->f_frsize = stbuf->f_bsize;  // Fragment size

  stbuf->f_blocks =
      kMaxFsSize / stbuf->f_bsize;               // Size of fs in f_frsize units
  stbuf->f_bfree = kMaxFsSize / stbuf->f_bsize;  // Number of free blocks
  stbuf->f_bavail =
      stbuf->f_bfree;  // Number of free blocks for unprivileged users

  stbuf->f_files = kMaxFsInodes;    // Number of inodes
  stbuf->f_ffree = stbuf->f_files;  // Number of free inodes
  stbuf->f_favail =
      stbuf->f_ffree;  // Number of free inodes for unprivileged users

  stbuf->f_namemax = kOssfsMaxFileNameLength;

  return 0;
}

// src_parent's wlock + dst_parent's wlock
// src_file's wlock
int OssFs::rename(uint64_t old_parent, std::string_view old_name,
                  uint64_t new_parent, std::string_view new_name,
                  unsigned int flags) {
  LOG_INFO("rename. from `, ` to `, ` flags `", old_parent, old_name,
           new_parent, new_name, flags);
  if (flags & RENAME_EXCHANGE) return -ENOTSUP;
  if (new_name.size() > kOssfsMaxFileNameLength) return -ENAMETOOLONG;

  GET_PARENT_REF2_AND_LOCK_PATH_IF_NEEDED_WITH_RET(old_parent, old_name,
                                                   new_parent, new_name);
  if (old_parent == new_parent) {
    RELEASE_ASSERT(ref.ref1.parent == ref.ref2.parent);
  }

  DirInode *o_parent = ref.ref1.parent;
  DirInode *n_parent = ref.ref2.parent;

  // In case of deadlock, lock the 2 parent locks in the order of nodeid.
  Inode *first_lock_node = o_parent, *sec_lock_node = n_parent;
  if (o_parent->nodeid > n_parent->nodeid) {
    first_lock_node = n_parent;
    sec_lock_node = o_parent;
  }

  std::unique_lock<std::shared_mutex> fwl(first_lock_node->inode_lock);
  std::unique_lock<std::shared_mutex> swl(sec_lock_node->inode_lock,
                                          std::defer_lock);
  if (first_lock_node != sec_lock_node) swl.lock();
  if (first_lock_node->is_stale || sec_lock_node->is_stale) {
    return -ESTALE;
  }

  // check src
  Inode *src_node = o_parent->find_child_node(old_name);
  RELEASE_ASSERT(src_node == ref.ref1.inode);

  std::unique_lock<std::shared_mutex> scwl(src_node->inode_lock);
  if (src_node->is_dir() && !options_.allow_rename_dir) {
    LOG_ERROR("src `, ` is a dir, rename is not supported", old_parent,
              old_name);
    return -ENOTSUP;
  }
  if (src_node->is_stale) return -ESTALE;

  // check dst
  Inode *dst_node = n_parent->find_child_node(new_name);
  DEFER(if (dst_node) dst_node->inode_lock.unlock());
  if (dst_node) {
    dst_node->inode_lock.lock();
    if (dst_node->is_dir() && !(dst_node->is_stale) &&
        !static_cast<DirInode *>(dst_node)->is_dir_empty()) {
      LOG_ERROR("dst `, ` not empty, rename is not supported", new_parent,
                new_name);
      return -ENOTEMPTY;
    }
    if (!dst_node->is_stale && (flags & RENAME_NOREPLACE)) {
      LOG_ERROR("dst `, ` already exists, rename is not supported", new_parent,
                new_name);
      return -EEXIST;
    }
  }

  auto src_path = ref.ref1.parent_path;
  auto dst_path = ref.ref2.parent_path;
  if (src_path.back() != '/') src_path.append("/");
  if (dst_path.back() != '/') dst_path.append("/");
  src_path.append(old_name.data(), old_name.size());
  dst_path.append(new_name.data(), new_name.size());

  // Check if the destination node exists.
  // It's possible we have no local inode created but the dir/file exists
  // remotely.
  if (!dst_node && (flags & RENAME_NOREPLACE)) {
    struct stat st;
    std::string unused_etag;
    int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_stat, dst_path, &st,
                                           &unused_etag);
    if (r == 0) {
      return -EEXIST;
    } else if (r != -ENOENT) {
      LOG_ERROR("fail to stat from cloud, path ` with error `", dst_path, r);
      return r;
    }
    // If we are here, we'll be sure that no dst_path(file or dir) exists on the
    // cloud.
  } else if (src_node->is_dir() || (dst_node && dst_node->is_dir())) {
    bool is_empty = false;
    int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_is_dir_empty, dst_path,
                                           is_empty);
    if (r != 0) {
      LOG_ERROR("fail to list dir `, with error: `", dst_path, r);
      return r;
    }
    if (!is_empty) {
      LOG_ERROR("dir ` is not empty in cloud, cannot rename", dst_path);
      return -ENOTEMPTY;
    }
  }

  std::vector<FileInode *> dirty_inodes;
  if (!src_node->is_dir()) {
    FileInode *src_file_node = static_cast<FileInode *>(src_node);
    if (src_file_node->is_dirty_file()) {
      dirty_inodes.push_back(src_file_node);
      LOG_INFO("rename for ` which is dirty inodes", src_path);
    }
  } else {
    // If this is a dir, we want to make sure all the dirty files are flushed,
    // so we can copy the written data.
    auto all_dirty_nodeids = get_dirty_nodeids();
    // Grab global lock to make it exclusive with forget operations,
    // as the inodes do not in the dir can be forgotten at the same time.
    std::lock_guard<std::mutex> l(inodes_map_lck_);

    for (auto nodeid : all_dirty_nodeids) {
      auto it = global_inodes_map_.find(nodeid);
      if (it == global_inodes_map_.end()) continue;

      Inode *inode = it->second;
      while (inode && inode->nodeid != kMountPointNodeId) {
        if (inode->is_stale) break;
        if (inode == src_node) {
          dirty_inodes.push_back(static_cast<FileInode *>(it->second));
          break;
        }
        inode = inode->parent;
      }
    }
    LOG_INFO("rename for ` found ` dirty inodes, total ` dirty nodes", src_path,
             dirty_inodes.size(), all_dirty_nodeids.size());
  }

  // All the inodes are dirty, so they must be valid now. Due to the
  // path lock, they could not become clean during the rename process.
  for (auto &dinode : dirty_inodes) {
    std::unique_lock<std::shared_mutex> wl(dinode->inode_lock, std::defer_lock);
    if (dinode != src_node /*we are renaming a dirty file*/) wl.lock();
    auto file = dinode->dirty_fh;
    RELEASE_ASSERT(file);
    int r = file->fdatasync_lock_held();
    if (r < 0) {
      LOG_ERROR("fail to fdatasync dirty file `, with error: `", dinode->nodeid,
                r);
      return r;
    }
  }

  int r = 0;
  if (src_node->is_dir()) {
    r = rename_dir(src_path, dst_path);
  } else {
    r = rename_file(src_path, dst_path);
  }
  if (r < 0) {
    LOG_ERROR("fail to rename from ` to ` on the cloud", src_path, dst_path);
    return r;
  }

  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    if (o_parent != n_parent) {
      src_node->parent_nodeid = new_parent;
      src_node->parent = n_parent;
    }

    // Erase before the src_node->name changes to make sure the child map
    // has a valid key of string view type.
    o_parent->erase_child_node(old_name, src_node->nodeid);
    src_node->name = new_name;

    // Mark dst as stale and overwrite the dst then.
    if (dst_node) {
      dst_node->is_stale = true;
      n_parent->erase_child_node(new_name, dst_node->nodeid);
    }

    n_parent->add_child_node_directly(src_node);
  }

  // Suppose a/c is in the negative cache. rename a/b -> a/c.
  // Its inode's attr_time is not updated, so if we don't erase the a/c from
  // the neg cache, it will be possible that the rename just finished, but the
  // inode's attr timeouts, and the following lookup for a/c returns -ENOENT.
  // And this is the same case for the descendants of the dst_dir of
  // rename_dir as well.
  if (negative_cache_) {
    negative_cache_->erase(dst_path);
    if (src_node->is_dir()) {
      negative_cache_->erase_by_prefix(dst_path + "/");
    }
  }

  // Even though src_node's lookup_cnt is not incremented, we do get a new inode
  // for new_parent/n_name now. So we still need to remove it from the staged
  // cache.
  rm_from_staged_cache_if_needed(new_parent, new_name);

  return 0;
}

// parent's rlock + inode's wlock (for the whole func)
// No need to delete the inode inside. FUSE kernel will handle it later
// (forget).
int OssFs::unlink(uint64_t parent, std::string_view name) {
  LOG_INFO("unlink. parent: `, name `", parent, name);

  GET_PARENT_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent, name);
  DirInode *parent_inode = ref.parent;

  std::shared_lock<std::shared_mutex> pl(parent_inode->inode_lock);
  if (parent_inode->is_stale) return -ESTALE;

  Inode *child = parent_inode->find_child_node(name);
  if (child == nullptr) {
    LOG_ERROR("no child named ` for dir `", name, parent);
    return -ENOENT;
  }

  std::unique_lock<std::shared_mutex> cl(child->inode_lock);
  if (child->is_stale) {
    LOG_ERROR("unlink: stale child ` of `", name, parent);
    return -ESTALE;
  }

  auto full_path = ref.parent_path;
  if (full_path.back() != '/') full_path.append("/");
  full_path.append(name.data(), name.size());

  int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_delete_object, full_path);
  if (r < 0 && r != -ENOENT) {
    LOG_ERROR("fail to delete ` on the cloud", full_path);
    return r;
  }

  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    child->is_stale = true;
  }

  return 0;
}

// ********************* RW related apis *********************
// parent's wlock
int OssFs::creat(uint64_t parent, std::string_view name, int flags, mode_t mode,
                 uid_t uid, gid_t gid, mode_t umask, uint64_t *nodeid,
                 struct stat *stbuf, void **fh) {
  LOG_INFO("create. parent: `, name: `, flags: `, append: `", parent, name,
           flags, (flags & O_APPEND) > 0);
  return create_internal(parent, name, flags, nodeid, stbuf, fh,
                         InodeType::kFile, "");
}

// inode's wlock
int OssFs::open(uint64_t nodeid, int flags, void **fh, bool *keep_page_cache) {
  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);

  RELEASE_ASSERT_WITH_MSG(!ref.inode->is_dir(), "open: nodeid ` is a directory",
                          nodeid);
  FileInode *inode = static_cast<FileInode *>(ref.inode);
  std::unique_lock<std::shared_mutex> l(inode->inode_lock);
  if (inode->is_stale) return -ESTALE;

  const auto &full_path = ref.inode_path;

  // TODO: we also head object when try to append to non-zero file, merge
  // them in one head request.
  if (options_.close_to_open && !inode->is_dirty) {
    struct stat stbuf = {};
    std::string remote_etag;
    int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_stat, full_path, &stbuf,
                                           &remote_etag);
    if (r < 0) {
      LOG_ERROR("fail to open ` on the cloud with r `", full_path, r);
      if (r == -ENOENT) {
        if (inode->open_ref_cnt == 0) {
          std::lock_guard<std::mutex> l(inodes_map_lck_);
          inode->is_stale = true;
        }
      }
      return r;
    }

    if (inode->is_data_changed(&stbuf, remote_etag)) {
      inode->invalidate_data_cache = true;
    }

    inode->etag = remote_etag;
    inode->update_attr(stbuf.st_size, stbuf.st_mtim);
  }

  if (inode->invalidate_data_cache) {
    evict_inode_cache(inode);
  }

  if (flags & O_TRUNC) {
    // Currently we don't allow a file being written by more than one handle.
    if (inode->is_dirty && inode->attr.size != 0) {
      LOG_ERROR("file ` is being written, cannot be truncated", nodeid);
      return -EBUSY;
    }
  }

  auto oss_fh = create_oss_file_handle(this, full_path, inode, flags);
  auto r = oss_fh->open();
  if (r < 0) {
    oss_fh->release();
    return r;
  }

  *fh = oss_fh;
  inode->open_ref_cnt++;

  *keep_page_cache = !inode->invalidate_data_cache;
  inode->invalidate_data_cache = false;

  // clang-format off
  LOG_INFO(
      "open file: `, nodeid: `, size: `, flags: `, read_only: `, truncate: `, append: `",
      full_path, nodeid, inode->attr.size, flags, (flags & O_ACCMODE) == O_RDONLY,
      (flags & O_TRUNC) > 0, (flags & O_APPEND) > 0);
  // clang-format on

  return 0;
}

// inode's wlock
int OssFs::release(uint64_t nodeid, void *fh) {
  // This is special, we don't care if we get the path lock or not as the
  // inode itself could be stale in release.
  auto ref = get_inode_ref(nodeid, InodeRefPathType::kPathTypeRead);
  DEFER(return_inode_ref(ref));

  // No need to hold path lock as fh is valid definitely.
  OssFileHandle *oss_fh = static_cast<OssFileHandle *>(fh);
  RELEASE_ASSERT(oss_fh);
  FileInode *inode = oss_fh->get_inode();

  int r = 0;
  {
    std::unique_lock<std::shared_mutex> l(inode->inode_lock);
    inode->open_ref_cnt--;

    r = oss_fh->close();
    if (r < 0) {
      LOG_ERROR("fail to close file ` due to error `", oss_fh->get_path(), r);
      inode->invalidate_data_cache = true;
    } else {
      LOG_INFO("release file: `, nodeid: `", oss_fh->get_path(), nodeid);
    }
  }

  oss_fh->release();
  return r;
}

// ********************* dir related apis *********************
// dir's wlock
int OssFs::opendir(uint64_t nodeid, void **dh) {
  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  Inode *dir_inode = ref.inode;
  {
    std::unique_lock<std::shared_mutex> wl(dir_inode->inode_lock);
    if (dir_inode->is_stale) return -ESTALE;
    dir_inode->open_ref_cnt++;
  }

  *dh = new OssDirHandle(this, static_cast<DirInode *>(dir_inode),
                         ref.inode_path, options_.readdir_remember_count);
  LOG_INFO("open dir: ` nodeid: `", ref.inode_path, nodeid);
  if (*dh == nullptr) {
    return -ENOSPC;
  }

  return 0;
}

// dir's wlock: readdirplus
// dir's rlock: readdir
int OssFs::readdir(uint64_t nodeid, off_t off, void *dh,
                   int (*filler)(void *ctx, uint64_t nodeid, const char *name,
                                 const struct stat *stbuf, off_t off),
                   void *filler_ctx, int (*is_interrupted)(void *ctx),
                   bool readdirplus, void *interrupted_ctx) {
  int r = 0;

  // FUSE kernel does not increment the lookup_cnt for ./.. when readdirplus.
  // consult: fuse_direntplus_link() in readdir.c
  auto fill_dotdot_stat = [&](uint64_t parent_nodeid) -> int {
    struct stat st2 = {};

    // If current node is the root node, fill .. with itself instead.
    // in that case, ll -a will get the real stat of the parent of the
    // mountpath in the outside filesystem.
    if (parent_nodeid == kMountPointNodeId) {
      std::shared_lock<std::shared_mutex> l(mp_inode_->inode_lock);
      mp_inode_->fill_statbuf(&st2);
      return filler(filler_ctx, kMountPointNodeId, "..", &st2, 2);
    }

    GET_INODE_REF_ONLY_WITH_RET(parent_nodeid);
    Inode *parent_inode = ref.inode;

    std::shared_lock<std::shared_mutex> pl(parent_inode->inode_lock);
    // Ignore stale parent, just fill buf as long as it exists.
    parent_inode->fill_statbuf(&st2);
    return filler(filler_ctx, parent_inode->nodeid, "..", &st2, 2);
  };

  if (off == 0) {
    // off 0: fill ., ..
    GET_INODE_REF_ONLY_WITH_RET(nodeid);
    Inode *inode = ref.inode;

    uint64_t parent_nodeid;
    struct stat st1 = {};
    {
      std::shared_lock<std::shared_mutex> l(inode->inode_lock);
      if (inode->is_stale) {
        LOG_ERROR("readdir: ` is stale", nodeid);
        return -ESTALE;
      }

      inode->fill_statbuf(&st1);
      if ((r = filler(filler_ctx, nodeid, ".", &st1, 1)) != 0) {
        return r;
      }

      parent_nodeid = inode->parent_nodeid;
      if (inode->nodeid == kMountPointNodeId) {
        parent_nodeid = kMountPointNodeId;
      }
    }

    if ((r = fill_dotdot_stat(parent_nodeid)) != 0) {
      return r;
    }
  } else if (unlikely(off == 1)) {
    // off 1: fill ..
    GET_INODE_REF_ONLY_WITH_RET(nodeid);
    Inode *inode = ref.inode;

    uint64_t parent_nodeid;
    {
      std::shared_lock<std::shared_mutex> l(inode->inode_lock);
      if (inode->is_stale) {
        LOG_ERROR("readdir: ` is stale", nodeid);
        return -ESTALE;
      }

      parent_nodeid = inode->parent_nodeid;
      if (inode->nodeid == kMountPointNodeId) {
        parent_nodeid = kMountPointNodeId;
      }
    }

    if ((r = fill_dotdot_stat(parent_nodeid)) != 0) {
      return r;
    }
  }

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  // is_dir and nodeid are constant attrs of an inode, and thus readdir(nodeid)
  // being sent means the inode specified by nodeid must be a dir_inode.
  RELEASE_ASSERT_WITH_MSG(ref.inode->is_dir(), "readdir: nodeid ` is not a dir",
                          nodeid);
  DirInode *parent_inode = static_cast<DirInode *>(ref.inode);

  OssDirHandle *odh = static_cast<OssDirHandle *>(dh);
  off_t start = (off >= 2) ? (off - 2) : 0;
  if (odh->get_full_path() != ref.inode_path) {
    // renamed_dir() is called between two readdir() calls.
    // odh->full_path is changed and odh->last_marker is invalid now.
    return -ESTALE;
  }

  if (readdirplus) {
    std::unique_lock<std::shared_mutex> pwl(parent_inode->inode_lock);
    if (parent_inode->is_stale) return -ESTALE;

    r = seek_dir_plus(parent_inode, odh, start, is_interrupted,
                      interrupted_ctx);
    if (r != 0) return r;

    r = readdir_fill_plus(parent_inode, odh, filler, filler_ctx);
  } else {
    std::shared_lock<std::shared_mutex> prl(parent_inode->inode_lock);
    if (parent_inode->is_stale) return -ESTALE;

    std::lock_guard<std::mutex> lk(odh->dir_lock_);

    r = seek_dir(parent_inode, odh, start, is_interrupted, interrupted_ctx);
    if (r != 0) return r;

    if (IS_FAULT_INJECTION_ENABLED(FI_Readdir_Delay_Noplus)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    r = readdir_fill(parent_inode, odh, filler, filler_ctx);
  }

  return r;
}

int OssFs::releasedir(uint64_t nodeid, void *dh) {
  // This must be valid and we don't need path lock to protect anything.
  OssDirHandle *odh = static_cast<OssDirHandle *>(dh);
  auto dir_inode = odh->inode();
  LOG_INFO("release dir: ` nodeid: `", odh->get_full_path(), nodeid);

  {
    std::unique_lock<std::shared_mutex> wl(dir_inode->inode_lock);
    RELEASE_ASSERT_WITH_MSG(dir_inode->open_ref_cnt > 0,
                            "dir nodeid `, open_ref_cnt `, which should be > 0",
                            nodeid, dir_inode->open_ref_cnt);
    dir_inode->open_ref_cnt--;
  }

  std::unordered_set<uint64_t> redundant_set;
  {
    std::lock_guard<std::mutex> lk(odh->dir_lock_);
    odh->get_pending_fill_nodeids(redundant_set);
  }

  for (auto it = redundant_set.begin(); it != redundant_set.end(); ++it) {
    try_invalidate_inode(*it, 1 /*nlookup*/, false /*recursive*/);
  }

  delete odh;
  return 0;
}

int OssFs::mkdir(uint64_t parent, std::string_view name, mode_t mode, uid_t uid,
                 gid_t gid, mode_t umask, uint64_t *nodeid,
                 struct stat *stbuf) {
  LOG_INFO("mkdir. parent: `, name: `", parent, name);
  return create_internal(parent, name, 0, nodeid, stbuf, nullptr,
                         InodeType::kDir, "");
}

// parent's rlock + inode's wlock
// No need to delete the inode inside. FUSE kernel will handle it later
// (forget).
int OssFs::rmdir(uint64_t parent, std::string_view name) {
  LOG_INFO("rmdir. parent: `, name `", parent, name);

  GET_PARENT_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent, name);
  DirInode *parent_inode = ref.parent;

  std::shared_lock<std::shared_mutex> pl(parent_inode->inode_lock);
  if (parent_inode->is_stale) return -ESTALE;

  auto child = parent_inode->find_child_node(name);
  if (child == nullptr) {
    LOG_ERROR("no child named ` for dir `", name, parent);
    return -ENOENT;
  }

  // It's possible that parent/name becomes a file instead of a dir
  // before parent inode is locked.
  if (!child->is_dir()) {
    LOG_ERROR("parent: `, name: `, is not a dir", parent, name);
    return -ENOTDIR;
  }

  std::unique_lock<std::shared_mutex> l(child->inode_lock);
  if (child->is_stale) {
    return -ESTALE;
  }

  std::string child_name;
  if (!(static_cast<DirInode *>(child)->is_dir_empty(&child_name))) {
    // clang-format off
    LOG_ERROR(
        "fail to remove nonempty dir named ` of parent `, whose children includes: `",
        name, parent, child_name);
    // clang-format on
    return -ENOTEMPTY;
  }

  auto full_path = ref.parent_path;
  if (full_path.back() != '/') full_path.append("/");
  full_path.append(name.data(), name.size());

  // It's OK this dir has been deleted from cloud, 404 will not be returned.
  int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_delete_object,
                                         add_backslash(full_path));
  if (r < 0) {
    LOG_ERROR("fail to delete dir ` on the cloud with error code `", full_path,
              r);
    return r;
  }

  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    child->is_stale = true;
  }
  return 0;
}

int OssFs::symlink(uint64_t parent, std::string_view name,
                   std::string_view link, uid_t uid, gid_t gid,
                   uint64_t *nodeid, struct stat *stbuf) {
  LOG_INFO("symlink. parent: `, name: `, link: `", parent, name, link);

  if (!options_.enable_symlink) return -ENOTSUP;

  // Reject absolute path.
  if (!link.empty() && link.front() == '/') return -EINVAL;

  return create_internal(parent, name, 0, nodeid, stbuf, nullptr,
                         InodeType::kSymlink, link);
}

ssize_t OssFs::readlink(uint64_t nodeid, char *buf, size_t size) {
  if (!options_.enable_symlink) return -ENOTSUP;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::shared_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;

  const auto &full_path = ref.inode_path;
  std::string target;
  int r =
      DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_get_symlink, full_path, target);
  if (r < 0) {
    return r;
  }

  auto write_size = std::min(size, target.size());
  memcpy(buf, target.c_str(), write_size);
  return write_size;
}

// ********************* internal functions *********************
int OssFs::lookup_try_local_attr_cache(
    DirInode *parent_inode, std::string_view name, const std::string &full_path,
    struct stat *stbuf, struct Attribute *old_attr, std::string *old_etag) {
  Inode *child_inode = parent_inode->find_child_node(name);
  if (child_inode != nullptr) {
    std::unique_lock<std::shared_mutex> cl(child_inode->inode_lock);
    if (!(child_inode->is_stale)) {
      // Case 1: cache hit (is dirty or not expired).
      if (child_inode->is_attr_valid(options_.attr_timeout)) {
        // Exclusive with forget (if (lookup_cnt == 0)
        // staged_inodes_cache_->insert()).
        increment_inode_lookupcnt(child_inode, parent_inode->nodeid, name);
        child_inode->fill_statbuf(stbuf);
        return 0;
      }

      if (old_attr) *old_attr = child_inode->attr;
      if (old_etag) *old_etag = std::string(get_inode_etag(child_inode));
    }
  }

  if (negative_cache_ && negative_cache_->exists(full_path)) {
    NegativeCache::lookup_cache_hit_cnt_++;
    LOG_EVERY_N(1000, ALOG_INFO, "Lookup: negative cache hit `",
                NegativeCache::lookup_cache_hit_cnt_.load());
    return -ENOENT;
  }

  return -E_CONTINUE_LOOKUP;
}

int OssFs::lookup_get_remote_attr(DirInode *parent_inode, std::string_view name,
                                  const std::string &full_path,
                                  struct stat *stbuf, std::string *remote_etag,
                                  time_t *attr_time) {
  int r = 0;
  if (lookup_from_staged_cache_if_enabled(parent_inode->nodeid, name, stbuf,
                                          remote_etag, attr_time)) {
    r = -E_LOOKUP_FROM_STAGED_CACHE;
  } else {
    if (IS_FAULT_INJECTION_ENABLED(FI_Lookup_Oss_Failure)) {
      return -EIO;
    }

    FAULT_INJECTION(FI_Lookup_Delay_Before_Getting_OSS_Response, []() {
      std::this_thread::sleep_for(std::chrono::milliseconds(2 * 1000));
    });

    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_stat, full_path, stbuf,
                                       remote_etag);
    *attr_time = time(0);
  }

  if (IS_FAULT_INJECTION_ENABLED(FI_Lookup_Delay_After_Getting_Remote_attr)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2 * 1000));
  }
  return r;
}

int OssFs::lookup_update_local_cache(
    DirInode *parent_inode, std::string_view name, bool acquire_write_path_lock,
    Inode *wlocked_inode, const std::string &full_path,
    const struct Attribute &old_attr, const std::string &old_etag,
    int req_status, struct stat *stbuf, const std::string &remote_etag) {
  Inode *child_inode = parent_inode->find_child_node(name);
  int r = req_status;
  if (r == -E_LOOKUP_FROM_STAGED_CACHE) {
    if (child_inode) {
      std::unique_lock<std::shared_mutex> cl(child_inode->inode_lock);

      if (!(child_inode->is_stale)) {
        increment_inode_lookupcnt(child_inode, parent_inode->nodeid, name);
        child_inode->fill_statbuf(stbuf);
        return 0;
      }
    }
    return -E_CONTINUE_LOOKUP;
  }

  // Update inode accoding to the OSS response.
  if (child_inode) {
    std::unique_lock<std::shared_mutex> cl(child_inode->inode_lock);
    if (!(child_inode->is_stale)) {  // active child
      // !!! Needs to handle file/dir change here. Mark the old inode as stale
      // and create a new one. FUSE kernel can handle this.
      if (r == 0) {
        InodeType new_type = Inode::mode_to_inode_type(stbuf->st_mode);
        if (child_inode->type != new_type) {
          LOG_ERROR("inode type changed for ` from ` to `", full_path,
                    Inode::inode_type_to_string(child_inode->type),
                    Inode::inode_type_to_string(new_type));
          r = -ENOENT;  // just go through the reverse delete process
        }
      }
      if (r < 0) {
        // Case 2: local inode is expired, and the file does not exist on OSS,
        //         try to mark this active child as stale.
        if (r == -ENOENT) {
          if (old_attr == child_inode->attr &&
              old_etag == get_inode_etag(child_inode)) {
            if (child_inode->is_dir() && options_.allow_rename_dir &&
                options_.allow_mark_dir_stale_recursively &&
                !static_cast<DirInode *>(child_inode)->is_dir_empty()) {
              if (!acquire_write_path_lock) {
                return -E_WRITE_PATH_NEEDED;
              }
              // acquire_write_path_lock == true
              // We need to make sure the write path lock is held in the case of
              // dir.
              if (wlocked_inode) {
                RELEASE_ASSERT(wlocked_inode == child_inode);
                mark_inode_stale_if_needed(child_inode, true);
                LOG_DEBUG("mark inode stale recursively for dir `:` `",
                          parent_inode->nodeid, name, full_path);
              } else {
                // Do nothing. This means that the child inode is newly
                // created after we get the path lock, so we can just ignore
                // this new inode!!!
              }
            } else {
              mark_inode_stale_if_needed(child_inode, false /*no effect*/);
            }
          }

          if (negative_cache_) negative_cache_->insert(full_path);
        }

        LOG_ERROR("[lookup] fail to stat from cloud. path ` with error `",
                  full_path, r);
        return r;
      }

      // Case 3: update the active child's attr.
      increment_inode_lookupcnt(child_inode, parent_inode->nodeid, name);
      if (old_attr == child_inode->attr &&
          old_etag == get_inode_etag(child_inode)) {
        invalidate_data_cache_if_needed(child_inode, stbuf, remote_etag);
        update_inode_etag(child_inode, remote_etag);

        child_inode->update_attr(stbuf->st_size, stbuf->st_mtim);
      }

      child_inode->fill_statbuf(stbuf);
      return 0;
    } else {
      // The child is stale and it's found in the cloud. we need to create its
      // inode locally.
    }
  }  // if child inode is not null

  if (r < 0) {
    // Since OSS returned error, we should remove the staged cache in case the
    // next lookup hits the staged cache, causing users' misunderstanding.
    rm_from_staged_cache_if_needed(parent_inode->nodeid, name);

    if (r != -ENOENT) {
      LOG_ERROR("[lookup] fail to stat from cloud. path ` with error `",
                full_path, r);
    } else {
      if (negative_cache_) negative_cache_->insert(full_path);
    }
    return r;
  }

  return -E_CONTINUE_LOOKUP;
}

// With parent's wlock held outside.
void OssFs::lookup_create_new_inode(DirInode *parent_inode,
                                    std::string_view name,
                                    const std::string &remote_etag,
                                    const uint64_t allocated_nodeid,
                                    struct stat *stbuf, time_t *attr_time) {
  bool is_dir = S_ISDIR(stbuf->st_mode);
  if (is_dir) {
    // st_mtim is always 1970.01.01 when using photon OSS SDK.
    // We set it to now.
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    stbuf->st_mtim = now;
  }

  uint64_t parent = parent_inode->nodeid;
  Inode *child_inode = create_new_inode(
      allocated_nodeid, name, stbuf->st_size, stbuf->st_mtim,
      Inode::mode_to_inode_type(stbuf->st_mode), false, parent, parent_inode,
      remote_etag);  // new node, no need to lock

  // A new inode may be created and forgotten and inserted to the staged cache
  // after lookup_try_local_attr_cache and before lookup_update_local_cache, so
  // we also need to try to remove it from the staged cache.
  increment_inode_lookupcnt(child_inode, parent, name);
  child_inode->fill_statbuf(stbuf);

  if (attr_time) child_inode->attr_time = *attr_time;

  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    // If this is the case that we are overwriting an existing node with same
    // name, it's possbile we will break the path which has been acquired.
    add_new_inode_to_global_map(child_inode);
    parent_inode->add_child_node(child_inode);
  }
}

int OssFs::lookup_with_inode_ref(DirInode *parent_inode, std::string_view name,
                                 const std::string &full_path,
                                 bool with_write_path_lock,
                                 Inode *wlocked_inode, struct stat *stbuf) {
  struct Attribute old_attr;
  std::string old_etag;
  std::string remote_etag;
  time_t attr_time = 0;

  int r = 0;
  {
    std::shared_lock<std::shared_mutex> pl(parent_inode->inode_lock);
    if (parent_inode->is_stale) return -ESTALE;
    r = lookup_try_local_attr_cache(parent_inode, name, full_path, stbuf,
                                    &old_attr, &old_etag);
    if (r != -E_CONTINUE_LOOKUP) return r;
  }

  r = lookup_get_remote_attr(parent_inode, name, full_path, stbuf, &remote_etag,
                             &attr_time);
  bool lookup_from_staged_cache = (r == -E_LOOKUP_FROM_STAGED_CACHE);

  std::unique_lock<std::shared_mutex> pl(parent_inode->inode_lock);
  if (parent_inode->is_stale) {
    LOG_ERROR("parent ` becomes stale", parent_inode->nodeid);
    return -ESTALE;
  }

  r = lookup_update_local_cache(parent_inode, name, with_write_path_lock,
                                wlocked_inode, full_path, old_attr, old_etag, r,
                                stbuf, remote_etag);
  if (r != -E_CONTINUE_LOOKUP) return r;

  // Child inode does not exist, or is stale. When reaching here, the
  // child inode must be stale or not exist, and we need to create a new inode.
  uint64_t allocated_nodeid =
      lookup_from_staged_cache ? stbuf->st_ino : InodeManager::next();
  lookup_create_new_inode(parent_inode, name, remote_etag, allocated_nodeid,
                          stbuf, &attr_time);
  return 0;
}

// With inode's wlock held outside.
void OssFs::increment_inode_lookupcnt(Inode *inode, uint64_t parent_nodeid,
                                      std::string_view name) {
  if (inode->lookup_cnt == 0) {
    rm_from_staged_cache_if_needed(parent_nodeid, name);
  }

  inode->increment_lookupcnt();
}

// Common logic for create, mkdir and symlink.
int OssFs::create_internal(uint64_t parent, std::string_view name, int flags,
                           uint64_t *nodeid, struct stat *stbuf, void **fh,
                           InodeType type, std::string_view link) {
  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent);
  if (name.size() > kOssfsMaxFileNameLength) return -ENAMETOOLONG;

  RELEASE_ASSERT_WITH_MSG(ref.inode->is_dir(),
                          "create: parent inode: ` is not a directory", parent);
  DirInode *parent_inode = static_cast<DirInode *>(ref.inode);
  int r = 0;

  std::unique_lock<std::shared_mutex> pwl(parent_inode->inode_lock);
  if (parent_inode->is_stale) return -ESTALE;

  Inode *child = parent_inode->find_child_node(name);
  if (child != nullptr) {
    std::shared_lock<std::shared_mutex> crl(child->inode_lock);
    if (!(child->is_stale)) {
      LOG_ERROR("fail to create a new inode. nodeid `, ` already exists",
                child->nodeid, name);
      return -EEXIST;
    }
    // It's OK that the stale child is erased from the
    // parent_inode->children, since it still is in the
    // global map waiting for an upcoming forget req.
    parent_inode->erase_child_node(name, child->nodeid);
  }

  auto full_path = ref.inode_path;  // parent path
  if (full_path.back() != '/') full_path.append("/");
  full_path.append(name.data(), name.size());

  // Not found in negative cache, look it up on the cloud.
  if (!negative_cache_ || !negative_cache_->exists(full_path)) {
    if (negative_cache_) {
      LOG_WARN("parent: `, full_path `, negative cache miss", parent,
               full_path);
    }

    if (exists_in_staged_cache(parent, name)) return -EEXIST;

    // Not exists at local, or is expired.
    struct stat st;
    std::string unused_etag;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_stat, full_path, &st,
                                       &unused_etag);
    if (r == 0) {
      return -EEXIST;
    } else if (r != -ENOENT) {
      LOG_ERROR("fail to stat from cloud, path ` with error `", full_path, r);
      return r;
    }
  } else {
    NegativeCache::create_cache_hit_cnt_++;
  }

  total_create_cnt_++;
  LOG_EVERY_N(1000, ALOG_INFO, "total create cnt: `, negative cache hit cnt: `",
              total_create_cnt_.load(),
              NegativeCache::create_cache_hit_cnt_.load());

  size_t file_size = 0;
  if (type == InodeType::kDir) {
    iovec iov{nullptr, 0};
    uint64_t expected_crc64 = 0;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_put_object,
                                       add_backslash(full_path), &iov, 1,
                                       &expected_crc64);
    if (r < 0) {
      LOG_ERROR("fail to mkdir from cloud. path ` with error `", full_path, r);
      return r;
    }
  } else if (type == InodeType::kSymlink) {
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_put_symlink, full_path, link);
    if (r < 0) {
      LOG_ERROR("fail to create symlink from cloud. path ` link ` with error `",
                full_path, link, r);
      return r;
    }
    file_size = r;
  } else {
    // Empty body. The upload of the regular file is delayed and performed in
    // flush().
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  Inode *child_inode;
  child_inode = create_new_inode(InodeManager::next(), name, file_size, now,
                                 type, false, parent, parent_inode, "");

  // A create request is equivalent to mknod + open, so we need to open the
  // regular file here.
  if (type == InodeType::kFile) {
    FileInode *child_file_inode = static_cast<FileInode *>(child_inode);

    auto oss_fh = create_oss_file_handle(this, full_path, child_file_inode,
                                         flags | O_CREAT);
    r = oss_fh->open();  // this will never return error
    RELEASE_ASSERT(r == 0);

    *fh = oss_fh;
    child_file_inode->open_ref_cnt++;
  }

  *nodeid = child_inode->nodeid;
  LOG_DEBUG("create nodeid `", *nodeid);

  memset(stbuf, 0, sizeof(struct stat));
  child_inode->fill_statbuf(stbuf);

  // Staged cache may just be timeout, erase it.
  increment_inode_lookupcnt(child_inode, parent, name);

  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    add_new_inode_to_global_map(child_inode);
    parent_inode->add_child_node_directly(child_inode);
  }

  if (negative_cache_) negative_cache_->erase(full_path);

  return 0;
}

int OssFs::get_one_list_results(std::string_view full_path,
                                std::vector<OssDirent> &results,
                                std::string &marker) {
  results.clear();
  return DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_list_dir, full_path, results,
                                        &marker);
}

// Mint a new inode if it doesn't exist based on the given results.
// Not increment lookup_cnt.
void OssFs::construct_inodes_if_needed(DirInode *parent_inode,
                                       OssDirHandle *dh) {
  std::vector<OssDirent> ents;
  dh->get_cur_list_res(ents);
  for (size_t i = dh->get_list_pos(); i < ents.size(); i++) {
    const auto &oss_ent = ents[i];

    uint64_t allocated_nodeid = 0;
    struct stat st;
    memset(&st, 0, sizeof(st));
    InodeType inode_type = Inode::dirent_type_to_inode_type(oss_ent.type());
    st.st_mode = Attribute::get_mode(inode_type);
    st.st_size = oss_ent.size();
    st.st_mtim = timespec{oss_ent.mtime(), 0};

    Inode *child_inode = parent_inode->find_child_node(oss_ent.name());
    // Update the attr of the existing non-stale children.
    if (child_inode != nullptr) {
      std::unique_lock<std::shared_mutex> cl(child_inode->inode_lock);
      if (child_inode->type != inode_type) {
        LOG_ERROR("inode type changed for `:` from ` to `",
                  parent_inode->nodeid, oss_ent.name(),
                  Inode::inode_type_to_string(child_inode->type),
                  Inode::inode_type_to_string(inode_type));
        mark_inode_stale_if_needed(child_inode, false /*non recursively*/);
      }

      if (!(child_inode->is_stale)) {
        try_update_inode_attr_from_list(child_inode, &st, oss_ent.etag());
        // Add an extra lookup_cnt to prevent this child_inode from being
        // forgot. Make sure inodes with extra lookup_cnt are all in the
        // pending_fill_nodeids_. Make sure one child_inode's lookup are not
        // incremented extraly twice.
        if (dh->insert_pending_fill_nodeids(child_inode->nodeid)) {
          // There are both a file and a non-empty dir in the bucket, and the
          // dir's inode is constructed first. When the file's inode is to be
          // constructed, the dir's inode will not be marked stale since it's
          // non-empty. In this case, this child_inode may be inserted twice,
          // and we need to avoid its lookup_cnt from being incremented twice.
          increment_inode_lookupcnt(child_inode, parent_inode->nodeid,
                                    oss_ent.name());
        }

        continue;
      }

      parent_inode->erase_child_node(child_inode->name, child_inode->nodeid);
    } else {
      struct stat stbuf;
      memset(&stbuf, 0, sizeof(stbuf));
      if (lookup_from_staged_cache_if_enabled(parent_inode->nodeid,
                                              oss_ent.name(), &stbuf)) {
        // Reuse nodeid.
        bool is_type_changed = S_ISDIR(stbuf.st_mode) ^ S_ISDIR(st.st_mode);
        if (!is_type_changed) {
          allocated_nodeid = stbuf.st_ino;
        }
      }
    }

    if (inode_type == InodeType::kDir) {
      // st_mtim is always 1970.01.01 when using photon OSS SDK.
      // Set it to now.
      struct timespec now;
      clock_gettime(CLOCK_REALTIME, &now);
      st.st_mtim = now;
    }

    if (allocated_nodeid == 0) {
      allocated_nodeid = InodeManager::next();
    }

    auto new_child_node = create_new_inode(
        allocated_nodeid, oss_ent.name(), st.st_size, st.st_mtim, inode_type,
        false, parent_inode->nodeid, parent_inode, oss_ent.etag());
    new_child_node->fill_statbuf(&st);
    {
      std::lock_guard<std::mutex> l(inodes_map_lck_);
      add_new_inode_to_global_map(new_child_node);
      parent_inode->add_child_node_directly(new_child_node);
    }

    // Add an extra lookup_cnt to prevent this child_inode from being forgot.
    // Inodes with an extra lookup_cnt are all in the pending_fill_nodeids_.
    // One child_inode's lookup_cnt are not incremented twice.
    if (dh->insert_pending_fill_nodeids(new_child_node->nodeid)) {
      increment_inode_lookupcnt(new_child_node, parent_inode->nodeid,
                                oss_ent.name());
    }
  }
}

// Only increment the lookupcnt if this inode exists and is not stale.
int OssFs::remember_inode_if_needed_with_fill(
    DirInode *parent_inode, const char *name, off_t offset, OssDirHandle *odh,
    int (*filler)(void *ctx, uint64_t nodeid, const char *name,
                  const struct stat *stbuf, off_t off),
    void *filler_ctx) {
  int r = 0;
  std::string_view name_view{name};
  Inode *child_inode = parent_inode->find_child_node(name_view);

  if (child_inode != nullptr) {
    std::unique_lock<std::shared_mutex> cl(child_inode->inode_lock);
    if (!(child_inode->is_stale)) {
      struct stat stbuf;
      memset(&stbuf, 0, sizeof(struct stat));
      child_inode->fill_statbuf(&stbuf);
      r = filler(filler_ctx, child_inode->nodeid, name, &stbuf, offset);
      if (r == 0) {
        if (!odh->erase_pending_fill_nodeids(child_inode->nodeid)) {
          // Normally, the lookup_cnt of the inode's in pending_fill_nodeids_
          // have been incremented before. But if there are both a file and a
          // dir on the cloud with the same name, this entry will be filled
          // twice. And for the second filling time, this inode is already not
          // in the pending_fill_nodeids_, so we should increment its lookup_cnt
          // to keep it consistent with the kernel lookup_cnt.
          increment_inode_lookupcnt(child_inode, parent_inode->nodeid,
                                    name_view);
        }
      }
      return r;
    }  // if child_inode is not stale
  }    // if child inode is not null

  // child inode became stale or was destroyed during readdir
  LOG_INFO("parent `, child name ` is stale, can't be filled, skip it",
           parent_inode->nodeid, name_view);
  return -ESTALE;
}

// with parent's rlock held
int OssFs::get_dirty_children(DirInode *parent_inode,
                              std::string_view full_path,
                              std::map<estring, OssDirent> &dirty_children_) {
  dirty_children_.clear();
  for (auto &cit : parent_inode->children) {
    Inode *child = cit.second;
    {
      std::shared_lock<std::shared_mutex> cl(child->inode_lock);
      if (child->is_stale || child->is_dir() ||
          !static_cast<FileInode *>(child)->is_dirty) {
        continue;
      }

      // For readdirplus, these attributes are not used actually. Attributes are
      // obtained from the Inodes when being filled.
      dirty_children_.emplace(
          cit.first,
          OssDirent(child->name, child->attr.size, child->attr.mtime.tv_sec,
                    DT_REG, get_inode_etag(child)));
    }
  }

  return 0;
}

// with parent's wlock held
int OssFs::refresh_dir_plus(DirInode *parent_inode, OssDirHandle *odh) {
  int r = 0;
  // We save current dirty children to a temporary set, in case a
  // currently dirty child becomes clean and is in the list result (filled
  // twice)
  std::map<estring, OssDirent> dirty_children;
  if ((r = get_dirty_children(parent_inode, odh->get_full_path(),
                              dirty_children)) != 0) {
    return r;
  }

  // Inodes are constructed right after next() and refresh_dir() (ListObj sent)
  // rather than constructed after being filled. Consider this case:
  // 1. We get 100 entries (already exist locally) by ListObj.
  // 2. Current readdir() only succeeds to fill 25 entries.
  // 3. Unlink one of the rest 75 files.
  // 4. The following readdir() comes, and tries to fill and construct inodes
  // for the
  //    rest 75 files, causing a new inode created for the file unlinked in
  //    step 3.
  if ((r = odh->refresh_dir(dirty_children)) != 0) {
    return r;
  }

  // refresh_dir will trigger listobj
  construct_inodes_if_needed(parent_inode, odh);

  if (IS_FAULT_INJECTION_ENABLED(FI_Readdir_Delay_After_Construct_Inodes)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  return 0;
}

// parent's rlock inside
int OssFs::refresh_dir(DirInode *parent_inode, OssDirHandle *odh) {
  int r = 0;
  // We save current dirty children to a temporary set, in case a
  // currently dirty child becomes clean and is in the list result (filled
  // twice)
  std::map<estring, OssDirent> dirty_children;
  if ((r = get_dirty_children(parent_inode, odh->get_full_path(),
                              dirty_children)) != 0) {
    return r;
  }

  return odh->refresh_dir(dirty_children);
}

// Refresh the dir handle and move to the target offset step by step.
int OssFs::seek_dir_plus(DirInode *parent_inode, OssDirHandle *odh,
                         off_t target_offset, int (*is_interrupted)(void *ctx),
                         void *interrupted_ctx) {
  if (target_offset == 0) {
    return refresh_dir_plus(parent_inode, odh);
  }

  int r = 0;
  bool is_offset_tuned = false;
  if (!odh->out_of_order(target_offset, &is_offset_tuned)) {
    if (is_offset_tuned) {
      // Since the inodes in last_response could have been forgotten, so it's
      // necessary to construct inodes for them.
      construct_inodes_if_needed(parent_inode, odh);
    }

    return r;
  }

  // clang-format off
  LOG_WARN(
      "readdir out of order! dir nodeid: `, dir path: `, current offset: `, offset in handle: `",
      parent_inode->nodeid, odh->get_full_path(), target_offset,
      odh->telldir());
  // clang-format on

  r = refresh_dir_plus(parent_inode, odh);
  if (r < 0) return r;

  auto ent = odh->get();
  while (ent) {
    if (is_interrupted && is_interrupted(interrupted_ctx)) {
      LOG_WARN("readdir interrupted! path: `, current offset: `",
               odh->get_full_path(), odh->telldir() + 2);
      return -EINTR;
    }

    odh->increment_fill_cnt();
    bool need_construct_inodes = false;
    r = odh->next(&need_construct_inodes);
    if (r < 0) {
      return r;
    } else if (r == 0) {  // reaches the end
      return -EINVAL;
    }

    if (need_construct_inodes) {
      construct_inodes_if_needed(parent_inode, odh);
    }

    if (odh->telldir() == target_offset) {
      return 0;
    }

    ent = odh->get();
  }

  return -EINVAL;
}

// Refresh the dir handle and move to the target offset step by step.
int OssFs::seek_dir(DirInode *parent_inode, OssDirHandle *odh,
                    off_t target_offset, int (*is_interrupted)(void *ctx),
                    void *interrupted_ctx) {
  if (target_offset == 0) {
    return refresh_dir(parent_inode, odh);
  }

  if (!odh->out_of_order(target_offset)) {
    return 0;
  }

  // clang-format off
  LOG_WARN(
      "readdir out of order! dir nodeid: `, dir path: `, current offset: `, offset in handle: `",
      parent_inode->nodeid, odh->get_full_path(), target_offset,
      odh->telldir());
  // clang-format on

  int r = refresh_dir(parent_inode, odh);
  if (r < 0) return r;

  auto ent = odh->get();
  while (ent) {
    if (is_interrupted && is_interrupted(interrupted_ctx)) {
      LOG_WARN("readdir interrupted! path: `, current offset: `",
               odh->get_full_path(), odh->telldir() + 2);
      return -EINTR;
    }

    odh->increment_fill_cnt();
    r = odh->next();
    if (r < 0) {
      return r;
    } else if (r == 0) {  // reaches the end
      return -EINVAL;
    }

    if (odh->telldir() == target_offset) {
      return 0;
    }

    ent = odh->get();
  }

  return -EINVAL;
}

// with parent's wlock held outdise
int OssFs::readdir_fill_plus(DirInode *parent_inode, OssDirHandle *odh,
                             int (*filler)(void *ctx, uint64_t nodeid,
                                           const char *name,
                                           const struct stat *stbuf, off_t off),
                             void *filler_ctx) {
  int r = 0;
  auto dent = odh->get();
  while (dent) {
    // Offset means the next one to fill, so +1 below is required!
    // +3 here includes . and ..
    r = remember_inode_if_needed_with_fill(parent_inode, dent->name_cstr(),
                                           odh->telldir() + 3, odh, filler,
                                           filler_ctx);
    if (r == 0) {
      // Filling succeeded.
      odh->increment_fill_cnt();
    } else if (r != -ESTALE) {
      return r;
    }
    // If filler returns -ESTALE, just continue to try to fill the next one.

    bool need_construct_inodes = false;
    if ((r = odh->next(&need_construct_inodes)) != 1) {
      break;
    }

    if (need_construct_inodes) {
      construct_inodes_if_needed(parent_inode, odh);

      if (IS_FAULT_INJECTION_ENABLED(FI_Readdir_Delay_After_Construct_Inodes)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }

    dent = odh->get();
  }

  return r;
}

int OssFs::readdir_fill(DirInode *parent_inode, OssDirHandle *odh,
                        int (*filler)(void *ctx, uint64_t nodeid,
                                      const char *name,
                                      const struct stat *stbuf, off_t off),
                        void *filler_ctx) {
  int r = 0;
  auto dent = odh->get();
  while (dent) {
    // Offset means the next one to fill, so +1 below is required!
    // +3 here includes . and ..
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode =
        Attribute::get_mode(Inode::dirent_type_to_inode_type(dent->type()));
    st.st_size = dent->size();
    st.st_mtim = timespec{dent->mtime(), 0};
    st.st_ino = TEMP_NODEID;
    r = filler(filler_ctx, TEMP_NODEID, dent->name_cstr(), &st,
               odh->telldir() + 3);

    if (r == 0) {
      odh->increment_fill_cnt();
    } else {
      return r;
    }

    if ((r = odh->next()) != 1) {
      break;
    }

    dent = odh->get();
  }
  return r;
}

void OssFs::try_update_inode_attr_from_list(Inode *inode, struct stat *stbuf,
                                            std::string_view remote_etag) {
  assert(inode);
  assert(stbuf);

  if (!inode->is_dir() && static_cast<FileInode *>(inode)->is_dirty) {
    return;
  }

  invalidate_data_cache_if_needed(inode, stbuf, remote_etag);
  update_inode_etag(inode, remote_etag);
  inode->update_attr(stbuf->st_size, stbuf->st_mtim);
}

// [WARNING] MUST NOT be inside any lock.
int OssFs::try_invalidate_inode(uint64_t nodeid, uint64_t nlookup,
                                bool recursive) {
  if (nodeid == kMountPointNodeId) {
    return 0;
  }

  Inode *inode = nullptr;
  {
    auto ref = get_inode_ref(nodeid, InodeRefPathType::kPathTypeNone);
    inode = ref.inode;
  }
  if (!inode) return -ESTALE;

  uint64_t parent_nodeid;
  {
    if (nlookup != 0) {
      std::unique_lock<std::shared_mutex> cl(inode->inode_lock);
      inode->decrement_lookupcnt(nlookup);
      if (inode->lookup_cnt > 0) {
        std::lock_guard<std::mutex> l(inodes_map_lck_);
        inode->ref_ctr--;
        return 0;
      }
      parent_nodeid = inode->parent_nodeid;
    } else {
      std::shared_lock<std::shared_mutex> cl(inode->inode_lock);
      if (inode->lookup_cnt > 0) {
        std::lock_guard<std::mutex> l(inodes_map_lck_);
        inode->ref_ctr--;
        return 0;
      }
      parent_nodeid = inode->parent_nodeid;
    }
  }

  // During the lock-released period,
  // 1. both the parent and the inode could become stale;
  // 2. and the parent inode could be destroyed
  Inode *parent_inode = nullptr;
  {
    auto ref = get_inode_ref(parent_nodeid, InodeRefPathType::kPathTypeNone);
    parent_inode = ref.inode;
  }
  // CASE 1: parent is destroyed, no need to dissolve parent-child
  // relationship. This inode was stale when creating a file with the same
  // name, so it has no relationship with its original parent, and its
  // original parent inode might be destroyed.
  if (!parent_inode) {
    bool need_delete_inode = false;
    std::unique_lock<std::shared_mutex> cl(inode->inode_lock);
    {
      std::lock_guard<std::mutex> l(inodes_map_lck_);
      inode->ref_ctr--;  // restore the ref incremented above
      if (inode->can_be_invalidated()) {
        // wait tree lock here?
        remove_inode_from_global_map(inode->nodeid);
        need_delete_inode = true;
      }
    }

    cl.unlock();
    // [WARNING] DO NOT delete inode inside inode->lock
    if (need_delete_inode) delete inode;
    return 0;
  }

  std::unique_lock<std::shared_mutex> pl(parent_inode->inode_lock);
  std::unique_lock<std::shared_mutex> cl(inode->inode_lock);
  // CASE 2: parent exists (no matter the parent and the inode are stale or
  // not)
  bool need_delete_inode = false;
  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    inode->ref_ctr--;  // restore the ref incremented above
    if (inode->can_be_invalidated()) {
      remove_inode_from_global_map(inode->nodeid);

      // Once parent_nodeid is specified, parent_inode->is_dir becomes invariant
      RELEASE_ASSERT_WITH_MSG(parent_inode->is_dir(),
                              "try_invalidate_inode: parent ` should be a dir",
                              parent_inode->nodeid);
      // The nodeid below is neccessary: in case this inode was stale, and a new
      // file with the same name was created, and the parent_inode mis-erased
      // the new child.
      static_cast<DirInode *>(parent_inode)
          ->erase_child_node(inode->name,
                             inode->nodeid);  // no lock inside
      need_delete_inode = true;
    }
    parent_inode->ref_ctr--;
    if (parent_inode->ref_ctr != 0) recursive = false;
  }

  cl.unlock();
  parent_nodeid = parent_inode->nodeid;
  pl.unlock();
  // [WARNING] DO NOT delete inode inside inode->lock
  if (need_delete_inode) {
    delete inode;

    if (recursive) {
      // Recursively invalidate the ancestors if needed.
      // We pass parent's nodeid instead of parent inode, so no need to
      // worry that parent inode might be deleted by other threads
      try_invalidate_inode(parent_nodeid, 0, true);
    }
  }

  return 0;
}

Inode *OssFs::create_new_inode(uint64_t nodeid, std::string_view name,
                               uint64_t size, struct timespec mtime,
                               InodeType type, bool is_dirty,
                               uint64_t parent_nodeid, Inode *parent_node,
                               std::string_view remote_etag) {
  Inode *inode = nullptr;
  if (type == InodeType::kDir) {
    inode = new DirInode(nodeid, name, mtime, parent_nodeid, parent_node);
  } else {
    inode = new FileInode(
        nodeid, name, size, mtime, type, is_dirty, parent_nodeid, parent_node,
        remote_etag, options_.bind_cache_to_inode, options_.cache_block_size);
  }
  if (inode == nullptr) {
    LOG_ERROR("fail to create a new inode.");
    return nullptr;
  }

  return inode;
}

int OssFs::forget_and_insert_to_staged_cache(uint64_t nodeid,
                                             uint64_t nlookup) {
  auto ref = get_inode_ref(
      nodeid, InodeRefPathType::kPathTypeNone);  // only inode reference here
  auto inode = ref.inode;
  if (!inode) return -ESTALE;

  std::unique_lock<std::shared_mutex> l(inode->inode_lock);
  RELEASE_ASSERT_WITH_MSG(inode->lookup_cnt >= nlookup,
                          "nodeid: `, lookup_cnt: `, which should be >= `",
                          nodeid, inode->lookup_cnt, nlookup);
  inode->decrement_lookupcnt(nlookup);

  auto restore_refctr = [&]() {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    inode->ref_ctr--;
  };

  if (inode->is_stale || !inode->is_attr_valid(options_.attr_timeout)) {
    restore_refctr();

    if (inode->lookup_cnt > 0) {
      return 0;
    }

    l.unlock();
    return try_invalidate_inode(nodeid, 0, true);
  }

  // We need to make sure that {remove from staged cache, increment lookupcnt}
  // (in lookup and readdirplus) and {if(lookup_cnt==0) insert_lru}
  // (in forget) are mutually exclusive, so hold inode's lock here.
  bool is_referenced = (inode->lookup_cnt > 0);
  if (!is_referenced) {
    // Only inodes with lookup_cnt == 0 are inserted.
    staged_inodes_cache_->insert(
        inode->parent_nodeid, inode->name, inode->attr.size, inode->attr.mtime,
        get_inode_etag(inode), inode->nodeid, inode->type, inode->attr_time);
  }

  restore_refctr();
  l.unlock();

  if (is_referenced) return 0;

  return try_invalidate_inode(nodeid, 0, true);
}

// Should be called before anywhere inode->lookup_cnt++.
void OssFs::rm_from_staged_cache_if_needed(uint64_t parent,
                                           std::string_view name) {
  if (!enable_staged_cache()) return;

  uint64_t parent_nodeid = parent;
  staged_inodes_cache_->erase(parent_nodeid, name);
}

bool OssFs::lookup_from_staged_cache_if_enabled(uint64_t parent_nodeid,
                                                std::string_view name,
                                                struct stat *stbuf,
                                                std::string *remote_etag,
                                                time_t *attr_time) {
  if (!enable_staged_cache()) return false;

  std::unique_ptr<StagedInodeCache::CacheEntry> entry;

  if (staged_inodes_cache_->find_and_erase(parent_nodeid, name, &entry)) {
    RELEASE_ASSERT(entry.get());
    if (stbuf) {
      stbuf->st_size = entry->size;
      stbuf->st_mtim = entry->mtime;
      stbuf->st_mode = Attribute::get_mode(entry->type);
      stbuf->st_ino = entry->nodeid;
    }

    if (attr_time) *attr_time = entry->attr_time;

    if (remote_etag) *remote_etag = entry->etag;

    return true;
  }

  return false;
}

bool OssFs::exists_in_staged_cache(uint64_t parent_nodeid,
                                   std::string_view name) {
  if (!enable_staged_cache()) return false;

  return staged_inodes_cache_->exists(parent_nodeid, name);
}

int OssFs::init() {
  srand(time(nullptr));

  transmission_control_th_ =
      new std::thread(&OssFs::transmission_control, this);
  health_check_th_ = new std::thread(&OssFs::run_health_check, this);
  reverse_invalidate_th_ =
      new std::thread(&OssFs::run_reverse_invalidate, this);

  if (options_.enable_admin_server) {
    std::promise<bool> uds_server_running;
    uds_server_th_ = new std::thread(&OssFs::start_uds_server, this,
                                     std::ref(uds_server_running));
    if (!uds_server_running.get_future().get()) {
      LOG_ERROR("Failed to start uds server");
    }
  }

  if (!options_.ram_role.empty()) {
    creds_provider_ = new_ram_role_creds_provider(options_.ram_role);
  } else if (!options_.credential_process.empty()) {
    creds_provider_ = new_process_creds_provider(options_.credential_process);
  }

  int r = 0;
  if (creds_provider_) {
    std::promise<int> result_promise;
    creds_refresh_th_ = new std::thread(&OssFs::start_creds_refresher, this,
                                        std::ref(result_promise));
    r = result_promise.get_future().get();
  } else {
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_check_bucket);
  }

  if (r < 0) {
    LOG_ERROR("fail to check bucket with error `", r);
  }

  return r;
}

void OssFs::evict_inode_cache(FileInode *inode) {
  if (inode->cache_manager) {
    const uint64_t cache_block_size = inode->cache_manager->block_size();
    inode->cache_manager =
        std::make_shared<BlockCacheManager>(cache_block_size);
  }
}

// See details in file.cpp schedule_prefetch().
void OssFs::init_prefetch_options() {
  // 1. Calculate the prefetch concurrency.
  if (options_.prefetch_chunks > 0 && enable_prefetching()) {
    // We use three times of prefetch_concurrency as the prefetch buffer size.
    // Adjust the prefetch_concurrency if prefethc_chunk_size * prefetch_chunks
    // is smaller than default.
    uint32_t old = options_.prefetch_concurrency;
    options_.prefetch_concurrency =
        std::min(options_.prefetch_chunks / 3,
                 static_cast<int>(options_.prefetch_concurrency));
    options_.prefetch_concurrency = std::max(options_.prefetch_concurrency, 1U);
    if (options_.prefetch_concurrency != old) {
      LOG_INFO("reset prefetch_concurrency with prefetch_chunks `, from ` to `",
               options_.prefetch_chunks, old, options_.prefetch_concurrency);
      prefetch_sem_ =
          std::make_unique<photon::semaphore>(options_.prefetch_concurrency);
    }
  }

  options_.prefetch_concurrency_per_file = std::min(
      options_.prefetch_concurrency_per_file, options_.prefetch_concurrency);

  // 2. Calculate prefetch window size of single file handle.
  max_prefetch_size_per_handle_ =
      static_cast<size_t>(options_.prefetch_concurrency_per_file) *
      options_.prefetch_chunk_size;
  max_prefetch_window_size_per_handle_ = max_prefetch_size_per_handle_ * 2;
}

int OssFs::truncate_inode_data(Inode *inode, std::string_view full_path,
                               size_t to_size) {
  if (inode->is_dir()) return -EISDIR;
  FileInode *file_inode = static_cast<FileInode *>(inode);

  // Currently, only truncation to 0 is allowed.
  RELEASE_ASSERT_WITH_MSG(to_size == 0, "to_size == `, which should be 0",
                          to_size);

  struct stat stbuf = {};

  auto background_env =
      bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next();
  int r = background_env.executor->perform([&]() {
    auto oss_client = background_env.oss_client;
    iovec iov{nullptr, 0};
    uint64_t expected_crc64 = 0;

    ssize_t ret = 0;
    if (options_.enable_appendable_object) {
      ret = oss_client->oss_delete_object(full_path);
      if (ret < 0) {
        LOG_ERROR("Failed to unlink file: `, nodeid: ` r: `", full_path,
                  file_inode->nodeid, ret);
        return ret;
      }

      ret =
          oss_client->oss_append_object(full_path, &iov, 1, 0, &expected_crc64);
    } else {
      ret = oss_client->oss_put_object(full_path, &iov, 1, &expected_crc64);
    }

    if (ret < 0) {
      LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", full_path,
                file_inode->nodeid, ret);
      return ret;
    }

    std::string unused_etag;
    int stat_r = oss_client->oss_stat(full_path, &stbuf, &unused_etag);
    return static_cast<ssize_t>(stat_r);
  });

  if (r < 0) {
    return r;
  }

  file_inode->invalidate_data_cache = true;
  file_inode->etag.clear();
  file_inode->update_attr(0, stbuf.st_mtim);
  return 0;
}

bool OssFs::mark_dir_stale_recursively(DirInode *dir_inode,
                                       uint64_t &walked_cnt,
                                       uint64_t &marked_cnt) {
  walked_cnt++;
  if (dir_inode->is_stale) return true;

  bool all_children_stale = true;
  for (auto &it : dir_inode->children) {
    walked_cnt++;
    auto inode = it.second;
    if (inode->open_ref_cnt != 0) {
      if (all_children_stale) all_children_stale = false;
      continue;
    }
    if (!inode->is_stale) {
      if (inode->is_dir()) {
        if (!mark_dir_stale_recursively(static_cast<DirInode *>(inode),
                                        walked_cnt, marked_cnt)) {
          if (all_children_stale) all_children_stale = false;
        }
      } else {
        inode->is_stale = true;
        marked_cnt++;
      }
    }
  }

  if (dir_inode->open_ref_cnt == 0 && all_children_stale) {
    dir_inode->is_stale = true;
    marked_cnt++;
  }
  return dir_inode->is_stale;
}

// Inode lock is already held and the caller should have write
// path lock held before calling this function.
// Write-path-lock enables us to access the children safely.
// Global map lock makes sure the global directory view unchanged
// during the process of the function.
void OssFs::mark_inode_stale_if_needed(Inode *inode, bool recursively) {
  if (inode->open_ref_cnt != 0 || inode->is_stale) return;
  if (!inode->is_dir()) {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    inode->is_stale = true;
    return;
  }

  // is dir for the following
  DirInode *dir_inode = static_cast<DirInode *>(inode);
  if (!recursively) {
    if (dir_inode->is_dir_empty()) {
      std::lock_guard<std::mutex> l(inodes_map_lck_);
      inode->is_stale = true;
    }
    return;
  }

  // Try to mark dir stale recursively, the caller need to have write path
  // lock held for this operation. Do a simple dfs walk through to mark
  // non-opened file/dir as stale. If one dir has opened children/grandchildren,
  // it will not be marked.
  uint64_t walked_cnt = 0, marked_cnt = 0;
  std::lock_guard<std::mutex> l(inodes_map_lck_);
  auto before = std::chrono::steady_clock::now();
  mark_dir_stale_recursively(dir_inode, walked_cnt, marked_cnt);
  auto after = std::chrono::steady_clock::now();
  auto cost =
      std::chrono::duration_cast<std::chrono::microseconds>(after - before);
  // clang-format off
  LOG_DEBUG(
      "mark_dir_stale_recursively: walked_cnt: `, marked_cnt: `, walk_time: `us",
      walked_cnt, marked_cnt, cost.count());
  // clang-format on
}

int OssFs::rename_file(std::string_view old_path, std::string_view new_path) {
  int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_rename_object, old_path,
                                         new_path,
                                         options_.set_mime_for_rename_dst);
  if (r != 0) {
    LOG_ERROR("fail to rename file from ` to ` with error: `", old_path,
              new_path, r);
  }
  return r;
}

struct RenameContext {
  enum RenameTaskType {
    kRenameTaskInvalid = 0,
    kRenameTaskCopy = 1,
    kRenameTaskDelete = 2
  };
  RenameTaskType task_type = kRenameTaskInvalid;
  OssFs *fs = nullptr;

  size_t obj_index = -1;
  const std::vector<std::string> *list_results = nullptr;
  std::string_view old_parent_path;
  std::string_view new_parent_path;

  std::atomic<uint64_t> *running_tasks_cnt = nullptr;
  std::atomic<int> *job_status = nullptr;
};

int OssFs::rename_dir(std::string_view old_path, std::string_view new_path) {
  std::vector<std::string> list_results;
  auto checker = [&]() -> bool {
    // TODO: check if the file length is valid after copying.
    return list_results.size() <= options_.rename_dir_limit;
  };

  estring old_obj_parent, new_obj_parent;
  old_obj_parent.appends(old_path, "/");
  new_obj_parent.appends(new_path, "/");
  bool is_dir_obj = false;

  auto before = std::chrono::steady_clock::now();

  // We are listing all the objects with old_path/ specified as the prefix.
  int r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_list_dir_descendants,
                                         old_obj_parent, list_results, checker,
                                         &is_dir_obj);
  if (r != 0) {
    LOG_ERROR("fail to list objects with prefix ` r = `", old_path, r);
    return r;
  }
  if (!checker()) {
    LOG_ERROR("trying to rename ` files one time, stop.", list_results.size());
    return -EMFILE;
  }

  auto after = std::chrono::steady_clock::now();
  auto cost =
      std::chrono::duration_cast<std::chrono::microseconds>(after - before);
  LOG_DEBUG("it cost ` us to list ` objs under `", cost.count(),
            list_results.size(), old_obj_parent);

  if (is_dir_obj) {
    // Copy old_parent_path/ to new_parent_path/.
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_copy_object, old_obj_parent,
                                       new_obj_parent, true);
    if (r != 0) {
      LOG_ERROR("fail to copy ` to ` with r `", old_obj_parent, new_obj_parent,
                r);
      return r;
    }
  }

  std::atomic<int> job_status{0};
  auto task_types = {RenameContext::kRenameTaskCopy,
                     RenameContext::kRenameTaskDelete};

  for (auto task_type : task_types) {
    std::atomic<uint64_t> running_tasks_cnt{0};
    LOG_INFO("starting to submit rename tasks for ` total file cnt `",
             task_type == RenameContext::kRenameTaskCopy ? "copy" : "delete",
             list_results.size());
    before = after;
    for (size_t i = 0; i < list_results.size();) {
      if (job_status.load() != 0) {
        LOG_ERROR(
            "stop index ` ` tasks for rename job under `", i,
            task_type == RenameContext::kRenameTaskCopy ? "copy" : "delete",
            old_obj_parent);
        break;
      }

      if ((i + 1) % 10000 == 0) {
        LOG_DEBUG(
            "submitted ` ` tasks for rename job under `", i + 1,
            task_type == RenameContext::kRenameTaskCopy ? "copy" : "delete",
            old_obj_parent);
      }

      rename_sem_->wait(1);
      running_tasks_cnt.fetch_add(1);

      auto ctx = new RenameContext;
      ctx->task_type = task_type;
      ctx->fs = this;
      ctx->obj_index = i;
      ctx->list_results = &list_results;
      ctx->old_parent_path = old_obj_parent;
      ctx->new_parent_path = new_obj_parent;

      if (task_type == RenameContext::kRenameTaskCopy) {
        i++;
      } else {
        i += 1000;  // we do delete in batch modes
      }

      ctx->running_tasks_cnt = &running_tasks_cnt;
      ctx->job_status = &job_status;

      auto th = photon::thread_create(do_rename_task, ctx);
      photon::thread_migrate(th,
                             bg_vcpu_env_.bg_oss_client_env->get_vcpu_next());
    }

    while (running_tasks_cnt.load() > 0) {
      AUTO_USLEEP(10000);
    }

    after = std::chrono::steady_clock::now();
    cost =
        std::chrono::duration_cast<std::chrono::microseconds>(after - before);
    LOG_DEBUG("it cost ` us to do rename ` ` objs under `", cost.count(),
              (task_type == RenameContext::kRenameTaskCopy ? "copy" : "delete"),
              list_results.size(), old_obj_parent);

    r = job_status.load();
    if (r != 0) {
      LOG_ERROR(
          "fail to ` objects under ` with r `",
          (task_type == RenameContext::kRenameTaskCopy ? "copy" : "delete"),
          old_obj_parent, r);
      return r;
    }
  }

  if (is_dir_obj) {
    // Delete old_parent_path/.
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(this, oss_delete_object, old_obj_parent);
    if (r != 0) {
      LOG_ERROR("fail to delete ` with r `", old_obj_parent, r);
      return r;
    }
  }

  return 0;
}

void *OssFs::do_rename_task(void *arg) {
  auto ctx = (RenameContext *)arg;
  thread_local auto oss_client =
      ctx->fs->bg_vcpu_env_.bg_oss_client_env->get_oss_client();

  int r = 0;

  if (ctx->task_type == RenameContext::kRenameTaskCopy) {
    estring src_obj_path, dst_obj_path;
    src_obj_path.appends(ctx->old_parent_path,
                         ctx->list_results->at(ctx->obj_index));
    dst_obj_path.appends(ctx->new_parent_path,
                         ctx->list_results->at(ctx->obj_index));
    r = oss_client->oss_copy_object(src_obj_path, dst_obj_path);
  } else {
    auto start_it = ctx->list_results->begin() + ctx->obj_index;
    auto end_it = (ctx->obj_index + 1000) < ctx->list_results->size()
                      ? (ctx->list_results->begin() + ctx->obj_index + 1000)
                      : ctx->list_results->end();
    std::vector<std::string_view> batch_objs(start_it,
                                             end_it);  // [start_it, end_it)
    if (end_it != ctx->list_results->end()) {
      LOG_DEBUG("rename from ` trying to delete objs from ` to `",
                ctx->old_parent_path, *start_it, *end_it);
    } else {
      LOG_DEBUG("rename from ` trying to delete objs from ` to the end",
                ctx->old_parent_path, *start_it);
    }
    r = oss_client->oss_delete_objects_under_dir(ctx->old_parent_path,
                                                 batch_objs);
  }
  if (r < 0) {
    LOG_ERROR(
        "Failed to do rename ` task from ` to ` name ` r `",
        (ctx->task_type == RenameContext::kRenameTaskCopy ? "copy" : "delete"),
        ctx->old_parent_path, ctx->new_parent_path,
        ctx->list_results->at(ctx->obj_index), r);

    int old_v = 0, new_v = r;
    ctx->job_status->compare_exchange_strong(old_v, new_v);
    RELEASE_ASSERT(ctx->job_status->load() != 0);
  }

  ctx->running_tasks_cnt->fetch_sub(1);
  ctx->fs->rename_sem_->signal(1);

  delete ctx;
  return nullptr;
}

void OssFs::update_max_oss_rw_lat(uint64_t latency_us) {
  if (!options_.enable_transmission_control) return;

  tc_lock_.lock();
  max_oss_rw_lat_us_ = std::max(max_oss_rw_lat_us_, latency_us);
  tc_lock_.unlock();
}

uint64_t OssFs::transmission_control() {
  if (!options_.enable_transmission_control) return 0;
  if (!enable_prefetching()) return 0;

  INIT_PHOTON();

  uint32_t curr_prefetch_concurrency = options_.prefetch_concurrency;
  int already_locked = 0;
  const uint64_t MAX_LAT_THRESHOLD = options_.tc_max_latency_threshold_us;
  const uint32_t MIN_PREFETCH_CONCURRENCY =
      std::min(options_.prefetch_concurrency, static_cast<uint32_t>(4));

  LOG_INFO("Start transmission control");
  while (!is_stopping_) {
    tc_lock_.lock();
    if (max_oss_rw_lat_us_ > MAX_LAT_THRESHOLD) {
      curr_prefetch_concurrency =
          std::max(curr_prefetch_concurrency / 2, MIN_PREFETCH_CONCURRENCY);
    } else if (max_oss_rw_lat_us_ < MAX_LAT_THRESHOLD / 2) {
      curr_prefetch_concurrency = std::min(curr_prefetch_concurrency + 4,
                                           options_.prefetch_concurrency);
    }

    max_oss_rw_lat_us_ = 0;
    tc_lock_.unlock();

    int remain = options_.prefetch_concurrency - curr_prefetch_concurrency;
    int need_lock = remain - already_locked;

    if (need_lock > 0) {
      LOG_INFO("Try to reduce ` max prefetch concurrency, current `", need_lock,
               curr_prefetch_concurrency);
      for (int i = 0; i < need_lock; i++) {
        prefetch_sem_->wait(1);
      }
    } else if (need_lock < 0) {
      LOG_INFO("Try to add ` max prefetch concurrency, current `", -need_lock,
               curr_prefetch_concurrency);
      for (int i = 0; i < -need_lock; i++) {
        prefetch_sem_->signal(1);
      }
    }

    already_locked += need_lock;

    AUTO_USLEEP(100000);
  }

  prefetch_sem_->signal(already_locked);
  LOG_INFO("Stop transmission control");
  return 0;
}

int OssFs::for_each_evictable_inodes(
    uint64_t threshold, std::function<int(const DentryView &)> cb) {
  struct SortableDentry {
    SortableDentry(uint64_t parent, uint64_t nodeid, bool is_dir, bool is_empty,
                   const std::string &name)
        : parent(parent),
          nodeid(nodeid),
          is_dir(is_dir),
          is_empty(is_empty),
          name(name) {}
    uint64_t parent;
    uint64_t nodeid;
    bool is_dir;
    bool is_empty;
    std::string name;
  };

  struct SortableDentryComparator {
    bool operator()(const SortableDentry &lhs, const SortableDentry &rhs) {
      if (lhs.is_dir != rhs.is_dir) {
        // file < dir
        return rhs.is_dir;
      } else if (lhs.is_dir) {
        if (lhs.is_empty != rhs.is_empty) {
          // empty dir < non-empty dir
          return lhs.is_empty;
        } else if (!lhs.is_empty) {
          return lhs.nodeid > rhs.nodeid;
        }
      }

      return lhs.nodeid < rhs.nodeid;
    }
  };

  typedef std::priority_queue<SortableDentry, std::vector<SortableDentry>,
                              SortableDentryComparator>
      SortableDentryPriorityQueue;

  std::unique_lock<std::mutex> im_lock(inodes_map_lck_);
  if (enable_staged_cache()) {
    if (global_inodes_map_.size() + staged_inodes_cache_->size() <= threshold) {
      return 0;
    }

    uint64_t staged_inodes_left = 0;
    if (global_inodes_map_.size() < threshold) {
      staged_inodes_left = threshold - global_inodes_map_.size();
    }

    auto start = std::chrono::steady_clock::now();
    uint64_t evicted_cnt = staged_inodes_cache_->evict_keys(staged_inodes_left);
    auto reclaim_cost_time_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    LOG_INFO("evicted ` staged node, costed: ` us", evicted_cnt,
             reclaim_cost_time_us);

    if (staged_inodes_left > 0) {
      im_lock.unlock();
      malloc_trim(0);
      return 0;
    }

    // It's OK if staged_inodes_cache_'s size is changed after evict_keys.
    // Handle it the next time.
  }

  uint64_t scan_cost_time_us = 0;
  if (global_inodes_map_.size() <= threshold) {
    return 0;
  }

  uint64_t reclaim_count = global_inodes_map_.size() - threshold;
  auto start = std::chrono::steady_clock::now();
  uint64_t non_empty_dir_cnt = 0;

  std::vector<SortableDentry> underlying_container;
  underlying_container.reserve(reclaim_count + 1);

  SortableDentryPriorityQueue sorted_dentries(SortableDentryComparator(),
                                              std::move(underlying_container));

  for (const auto &it : global_inodes_map_) {
    if (is_stopping_) break;
    auto inode = it.second;
    if (inode->nodeid == kMountPointNodeId) continue;

    if (inode->is_dir()) {
      DirInode *dir_inode = static_cast<DirInode *>(inode);
      sorted_dentries.emplace(dir_inode->parent_nodeid, dir_inode->nodeid, true,
                              dir_inode->children.size() == 0, dir_inode->name);
      if (dir_inode->children.size() > 0) non_empty_dir_cnt++;
    } else {
      sorted_dentries.emplace(inode->parent_nodeid, inode->nodeid, false, false,
                              inode->name);
    }

    if (sorted_dentries.size() > reclaim_count) {
      if (sorted_dentries.top().is_dir && !sorted_dentries.top().is_empty) {
        non_empty_dir_cnt--;
      }

      sorted_dentries.pop();

      // We have already collected enough inodes, so we can stop.
      if (non_empty_dir_cnt == 0) {
        break;
      }
    }
  }
  im_lock.unlock();

  scan_cost_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

  start = std::chrono::steady_clock::now();
  while (!sorted_dentries.empty() && !is_stopping_) {
    auto dentry = sorted_dentries.top();
    sorted_dentries.pop();

    int r = cb({dentry.parent, dentry.nodeid, dentry.name});
    if (r == -ENOSYS || r == -EBADF) {
      return r;
    }
  }

  auto end = std::chrono::steady_clock::now();
  LOG_INFO(
      "drop_cached_inodes scan cost ` us, callback cost ` us, reclaim_count: `",
      scan_cost_time_us,
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count(),
      reclaim_count);

  malloc_trim(0);
  return 0;
}

int OssFs::reverse_invalidate_kernel_entry(const DentryView &dentry) {
  if (fuse_se_ == nullptr) return -ENOTCONN;

  auto start = std::chrono::steady_clock::now();
  int r = fuse_lowlevel_notify_inval_entry(
      fuse_se_, dentry.parent, dentry.name.data(), dentry.name.length());
  int error_number = errno;
  auto time_cost_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

  // logging slow requests
  if (time_cost_us > 1000000) {
    LOG_WARN("fuse_lowlevel_notify_inval_entry (`, `, `), r `, errno `, cost `",
             dentry.parent, dentry.name, dentry.nodeid, r, error_number,
             time_cost_us);
  }

  if (r != 0) {
    switch (error_number) {
      // already unmount
      case EBADF:
        LOG_WARN("fuse mountpoint is already unmounted!");
        break;
      // not supported
      case ENOSYS:
        LOG_WARN("fuse_lowlevel_notify_inval_entry are not supported!");
        break;
      case ENOENT:
        return 0;
      default:
        LOG_INFO("fuse_lowlevel_notify_inval_entry (`, `, `), r `, errno `",
                 dentry.parent, dentry.name, dentry.nodeid, r, error_number);
        break;
    }
    return -error_number;
  }

  return 0;
}

void OssFs::run_reverse_invalidate() {
  INIT_PHOTON();

  auto thread_func = [&]() {
    uint64_t interval = options_.inode_cache_eviction_interval_ms * 1000;

    if (!IS_FAULT_INJECTION_ENABLED(FI_Reverse_Invalidate_Inode_Forcefully)) {
      if (fuse_se_ == nullptr) return interval;
    }

    thread_local bool support_fuse_invalidate = true;
    if (!support_fuse_invalidate) return interval;

    int r = for_each_evictable_inodes(
        get_inode_eviction_threshold(), [this](const DentryView &dentry) {
          return this->reverse_invalidate_kernel_entry(dentry);
        });

    // ENOSYS: kernel does not support fuse_lowlevel_notify_inval_entry
    // EBADF: already unmounted
    if (r == -ENOSYS || r == -EBADF) {
      support_fuse_invalidate = false;
    }

    return interval;
  };

  if (get_inode_eviction_threshold() > 0 &&
      options_.inode_cache_eviction_interval_ms > 0) {
    photon::Timer timer(options_.inode_cache_eviction_interval_ms * 1000,
                        thread_func, true);
    while (!is_stopping_) AUTO_USLEEP(100000);
  }
}

void OssFs::run_health_check() {
  INIT_PHOTON();

  uint64_t interval = 30000000;
  auto thread_func = [&]() {
    uint64_t inode_count = 0;
    {
      std::lock_guard<std::mutex> lock(inodes_map_lck_);
      inode_count = global_inodes_map_.size();
    }

    LOG_INFO("[SystemInfo] inode count: `, memory: ` KiByte.", inode_count,
             get_physical_memory_KiB());
    LOG_INFO("[SystemInfo] active file handles: `",
             active_file_handles_.load());
    if (enable_staged_cache()) {
      staged_inodes_cache_->print_staged_cache_status();
    }

    return interval;
  };

  LOG_INFO("Start health check");

  photon::Timer timer(interval, thread_func, true);
  while (!is_stopping_) AUTO_USLEEP(100000);
}

void OssFs::update_creds(const OssCredentials &creds) {
  auto ctxs = bg_vcpu_env_.bg_oss_client_env->get_all_env_cxts();
  for (auto &ctx : ctxs) {
    ctx.executor->perform([&]() {
      ctx.oss_client->set_credentials(
          {creds.accessKeyId, creds.accessKeySecret, creds.securityToken});
    });
  }
}

int OssFs::validate_creds(const OssCredentials &creds) {
  auto options = DO_SYNC_BACKGROUND_OSS_REQUEST(this, get_options);
  std::unique_ptr<OssAdapter> oss_client =
      std::unique_ptr<OssAdapter>(new_oss_client("", "", options));
  oss_client->set_credentials(
      {creds.accessKeyId, creds.accessKeySecret, creds.securityToken});
  int r = oss_client->oss_check_bucket();
  if (r != 0) {
    LOG_ERROR("Fail to check bucket with ak ` error `", creds.accessKeyId, r);
  }
  return r;
}

std::pair<int, uint64_t> OssFs::do_refresh_creds() {
  int r = -EINVAL;
  auto info =
      creds_provider_->refresh_credentials([&](const OssCredentials &creds) {
        r = validate_creds(creds);
        return r == 0;
      });
  if (info.creds != nullptr) {
    update_creds(*info.creds);
  }
  return {r, info.next_refresh_interval_us};
}

uint64_t OssFs::refresh_creds() {
  return do_refresh_creds().second;
}

void OssFs::start_creds_refresher(std::promise<int> &result_promise) {
  INIT_PHOTON();

  auto [result, next] = do_refresh_creds();
  result_promise.set_value(result);

  photon::Timer timer(next, {this, &OssFs::refresh_creds}, true);
  while (!is_stopping_) AUTO_USLEEP(100000);
}

}  // namespace OssFileSystem
