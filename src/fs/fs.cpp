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
#include <photon/common/utility.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <queue>
#include <thread>

#include "common/fuse.h"
#include "common/macros.h"
#include "common/utils.h"
#include "disk_cache.h"
#include "error_codes.h"
#include "file.h"
#include "file_hdfs.h"
#include "mem_cache.h"
#include "metric/metrics.h"
#include "random_write_context.h"

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

std::atomic<uint64_t> NegativeCache::create_cache_hit_cnt_ = ATOMIC_VAR_INIT(0);
std::atomic<uint64_t> NegativeCache::lookup_cache_hit_cnt_ = ATOMIC_VAR_INIT(0);

gid_t Attribute::DEFAULT_GID = 0;
uid_t Attribute::DEFAULT_UID = 0;
mode_t Attribute::DEFAULT_DIR_MODE = 0755;
mode_t Attribute::DEFAULT_FILE_MODE = 0644;
blksize_t Attribute::DEFAULT_BLKSIZE = 4096;  // Default to OSS mode

OssFs::OssFs(const OssFsOptions &options, BackgroundVCpuEnv bg_vcpu_env,
             std::unique_ptr<IIdManager> id_manager)
    : options_(options),
      write_mode_(compute_write_mode(options)),
      bg_vcpu_env_(bg_vcpu_env),
      id_manager_(std::move(id_manager)),
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

  Attribute::set_default_gid_uid(options_.gid, options_.uid);
  Attribute::set_default_mode(options_.dir_mode, options_.file_mode);

  init_prefetch_options();
  upload_buffers_ = std::make_unique<FixedBlockMemoryPool>(
      options_.upload_buffer_size, options_.upload_concurrency + 4,
      options_.upload_concurrency + 4, options_.mempool_purge_interval_ms);
  // Derive base part size: aligned to chunk_size so parts never split chunks.
  random_write_base_part_size_ =
      align_up(options_.upload_buffer_size, options_.random_write_chunk_size);
  if (enable_prefetching()) {
    size_t blocks_per_prefetch_chunk =
        (options_.prefetch_chunk_size + options_.cache_block_size - 1) /
        options_.cache_block_size;
    size_t cached_block_count =
        blocks_per_prefetch_chunk * options_.prefetch_concurrency * 3;
    size_t pool_capcacity = cached_block_count;
    uint64_t purge_interval_ms = options_.mempool_purge_interval_ms;

    // Override if user specified.
    if (options_.prefetch_chunks > 0) {
      cached_block_count = blocks_per_prefetch_chunk * options_.prefetch_chunks;
      pool_capcacity = cached_block_count;
    } else if (options_.prefetch_chunks < 0) {
      // Unlimited mode.
      pool_capcacity = std::numeric_limits<size_t>::max();
    }

    if (options_.memory_data_cache_size > 0) {
      purge_interval_ms = 0;
    }

    download_buffers_ = std::make_shared<FixedBlockMemoryPool>(
        options_.cache_block_size, pool_capcacity, cached_block_count,
        purge_interval_ms);
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

  if (enable_staged_cache()) {
    LOG_INFO("Remained staged inodes number: `", staged_inodes_cache_->size());
  }

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

  FAULT_INJECTION(FI_Forget_Delay, []() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  });

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
  r = PERFORM_BACKGROUND_OBJ_REQUEST(this, stat, full_path, stbuf,
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
  inode->set_mode(stbuf->st_mode);
  inode->set_uid(stbuf->st_uid);
  inode->set_gid(stbuf->st_gid);
  inode->update_attr(stbuf->st_size, stbuf->st_mtim, stbuf->st_atim);
  resync_randwrite_remote_size(inode);
  inode->fill_statbuf(stbuf);
  return 0;
}

// inode's wlock.
// Routes to hdfs_setattr in HDFS mode; OSS only supports mtime and
// truncation to 0.
int OssFs::setattr(uint64_t nodeid, struct stat *stbuf, int to_set,
                   struct fuse_file_info *fi, uid_t caller_uid,
                   gid_t caller_gid) {
  if (is_hdfs_mode()) {
    return hdfs_setattr(nodeid, stbuf, to_set, fi, caller_uid, caller_gid);
  }

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  Inode *inode = ref.inode;

  std::unique_lock<std::shared_mutex> l(inode->inode_lock);
  if (inode->is_stale) return -ESTALE;

  if (to_set & FUSE_SET_ATTR_MTIME) {
    inode->update_attr(inode->attr.size, stbuf->st_mtim);
  } else if (to_set & FUSE_SET_ATTR_SIZE) {
    if (inode->is_dir()) return -EISDIR;

    if (write_mode() == WriteMode::Random) {
      RELEASE_ASSERT(stbuf->st_size >= 0);
      uint64_t new_size = static_cast<uint64_t>(stbuf->st_size);
      if (new_size != inode->attr.size) {
        int r = random_write_truncate(static_cast<FileInode *>(inode),
                                      ref.inode_path, new_size);
        if (r < 0) return r;
      }
    } else {
      if (stbuf->st_size != 0) {
        LOG_WARN("nodeid ` truncate to non-zero is not supported.", nodeid);
        return -ENOTSUP;
      }

      if (inode->attr.size == 0) goto exit;

      if (static_cast<FileInode *>(inode)->is_dirty_file()) {
        LOG_ERROR("nodeid ` is dirty, cannot be truncated", nodeid);
        return -EBUSY;
      }

      int r = truncate_inode_data(inode, ref.inode_path, 0);
      if (r < 0) return r;
    }
  }

exit:
  // FUSE needs refill stat buffer.
  inode->fill_statbuf(stbuf);
  return 0;
}

int OssFs::hdfs_setattr(uint64_t nodeid, struct stat *stbuf, int to_set,
                        struct fuse_file_info *fi, uid_t caller_uid,
                        gid_t caller_gid) {
  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  Inode *inode = ref.inode;

  std::unique_lock<std::shared_mutex> l(inode->inode_lock);
  if (inode->is_stale) return -ESTALE;

  // setattr processes multiple flags sequentially. If any operation fails,
  // subsequent operations are skipped but completed operations are NOT rolled
  // back (partial failure). Order matches libfuse's fuse_lib_setattr
  // (lib/fuse.c:2787): chmod -> chown -> truncate -> utimens.

  // chmod.
  if (to_set & FUSE_SET_ATTR_MODE) {
    int r = do_hdfs_setattr_mode(nodeid, ref.inode_path, stbuf->st_mode, inode,
                                 caller_uid, caller_gid);
    if (r < 0) return r;
  }

  // chown.
  if (to_set & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
    int r = do_hdfs_setattr_uid_gid(nodeid, ref.inode_path, stbuf, to_set,
                                    inode, caller_uid, caller_gid);
    if (r < 0) return r;
  }

  // truncate.
  if (to_set & FUSE_SET_ATTR_SIZE) {
    int r;
    if (fi && fi->fh) {
      r = do_hdfs_ftruncate(nodeid, inode, stbuf->st_size, fi, caller_uid,
                            caller_gid);
    } else {
      r = do_hdfs_setattr_size(nodeid, inode, ref.inode_path, stbuf->st_size,
                               caller_uid, caller_gid);
    }
    if (r < 0) return r;
  }

  // utimensat.
  if (to_set & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)) {
    int r = do_hdfs_setattr_times(inode, ref.inode_path, stbuf, to_set,
                                  caller_uid, caller_gid);
    if (r < 0) return r;
  }

  // Refresh attributes from backend so the setattr reply carries real values
  // (equivalent to libfuse high-level API's automatic getattr after setattr).
  // If stat fails, propagate the error.
  int stat_ret = PERFORM_BACKGROUND_OBJ_REQUEST(this, stat, ref.inode_path,
                                                stbuf, nullptr);
  if (stat_ret < 0) return stat_ret;

  inode->set_mode(stbuf->st_mode);
  inode->set_uid(stbuf->st_uid);
  inode->set_gid(stbuf->st_gid);
  inode->update_attr(stbuf->st_size, stbuf->st_mtim, stbuf->st_atim);
  inode->fill_statbuf(stbuf);
  return 0;
}

int OssFs::do_hdfs_setattr_mode(uint64_t nodeid, std::string_view path,
                                mode_t mode, Inode *inode, uid_t caller_uid,
                                gid_t caller_gid) {
  int r = check_permission(PermOp::Chmod, inode, caller_uid, caller_gid);
  if (r < 0) return r;

  r = PERFORM_BACKGROUND_OBJ_REQUEST(this, set_permission, path,
                                     mode & kPermMask);
  if (r < 0) {
    LOG_ERROR("Failed to chmod, path: `, mode: 0o`, error: `", path,
              OCT(mode & kPermMask), r);
    return r;
  }

  inode->set_mode((inode->get_mode() & ~kPermMask) | (mode & kPermMask));
  LOG_INFO("chmod success, nodeid: `, path: `, mode: 0o`", nodeid, path,
           OCT(mode & kPermMask));
  return 0;
}

int OssFs::do_hdfs_setattr_uid_gid(uint64_t nodeid, std::string_view path,
                                   const struct stat *stbuf, int to_set,
                                   Inode *inode, uid_t caller_uid,
                                   gid_t caller_gid) {
  int r = check_permission(PermOp::Chown, inode, caller_uid, caller_gid);
  if (r < 0) return r;

  uid_t uid = (to_set & FUSE_SET_ATTR_UID) ? stbuf->st_uid : inode->get_uid();
  gid_t gid = (to_set & FUSE_SET_ATTR_GID) ? stbuf->st_gid : inode->get_gid();

  // Convert FUSE flags to store layer flags.
  int store_to_set = 0;
  if (to_set & FUSE_SET_ATTR_UID) store_to_set |= IObjStore::kSetUid;
  if (to_set & FUSE_SET_ATTR_GID) store_to_set |= IObjStore::kSetGid;

  r = PERFORM_BACKGROUND_OBJ_REQUEST(this, set_owner, path, uid, gid,
                                     store_to_set);
  if (r < 0) {
    LOG_ERROR("Failed to chown, path: `, uid: `, gid: `, error: `", path, uid,
              gid, r);
    return r;
  }

  if (to_set & FUSE_SET_ATTR_UID) inode->set_uid(stbuf->st_uid);
  if (to_set & FUSE_SET_ATTR_GID) inode->set_gid(stbuf->st_gid);
  LOG_INFO("chown success, nodeid: `, path: `, uid: `, gid: `", nodeid, path,
           uid, gid);
  return 0;
}

int OssFs::check_permission(PermOp op, Inode *inode, uid_t uid, gid_t gid) {
  if (!is_hdfs_mode()) return 0;

  struct stat stbuf;
  inode->fill_statbuf(&stbuf);
  return PERFORM_BACKGROUND_OBJ_REQUEST(this, check_permission, op, &stbuf, uid,
                                        gid);
}

int OssFs::do_hdfs_setattr_times(Inode *inode, std::string_view path,
                                 const struct stat *stbuf, int to_set,
                                 uid_t caller_uid, gid_t caller_gid) {
  // Save original timestamps for no-op detection.
  struct timespec old_atime = inode->get_atime();
  struct timespec old_mtime = inode->get_mtime();

  // clock_gettime once to ensure both NOW timestamps use the same value.
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  // Compute target timestamps.
  struct timespec new_atime = old_atime;
  struct timespec new_mtime = old_mtime;
  if (to_set & FUSE_SET_ATTR_ATIME) {
    new_atime = (to_set & FUSE_SET_ATTR_ATIME_NOW) ? now : stbuf->st_atim;
  }
  if (to_set & FUSE_SET_ATTR_MTIME) {
    new_mtime = (to_set & FUSE_SET_ATTR_MTIME_NOW) ? now : stbuf->st_mtim;
  }

  // Convert to milliseconds first (HDFS only supports ms precision).
  int64_t new_atime_ms = new_atime.tv_sec * 1000 + new_atime.tv_nsec / 1000000;
  int64_t new_mtime_ms = new_mtime.tv_sec * 1000 + new_mtime.tv_nsec / 1000000;
  int64_t old_atime_ms = old_atime.tv_sec * 1000 + old_atime.tv_nsec / 1000000;
  int64_t old_mtime_ms = old_mtime.tv_sec * 1000 + old_mtime.tv_nsec / 1000000;

  // Permission check: non-owner non-root.
  if (caller_uid != 0 && caller_uid != inode->get_uid()) {
    bool atime_now = (to_set & FUSE_SET_ATTR_ATIME_NOW) != 0;
    bool mtime_now = (to_set & FUSE_SET_ATTR_MTIME_NOW) != 0;
    if (atime_now && mtime_now) {
      int r =
          check_permission(PermOp::Utimensat, inode, caller_uid, caller_gid);
      if (r < 0) return r;
    } else if (new_atime_ms == old_atime_ms && new_mtime_ms == old_mtime_ms) {
      // no-op: timestamps unchanged.
    } else {
      return -EPERM;
    }
  }

  // Only call RPC if milliseconds actually changed.
  if (new_atime_ms != old_atime_ms || new_mtime_ms != old_mtime_ms) {
    int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, set_times, path, new_mtime_ms,
                                           new_atime_ms);
    if (r < 0) return r;

    inode->set_atime(new_atime);
    inode->set_mtime(new_mtime);
    LOG_INFO("utimensat success, path: `, atime_ms: `, mtime_ms: `", path,
             new_atime_ms, new_mtime_ms);
  }
  return 0;
}

int OssFs::do_hdfs_setattr_size(uint64_t nodeid, Inode *inode,
                                std::string_view full_path, off_t target_size,
                                uid_t caller_uid, gid_t caller_gid) {
  if (inode->is_dir()) return -EISDIR;

  int r = check_permission(PermOp::Truncate, inode, caller_uid, caller_gid);
  if (r < 0) return r;

  FileInode *file_inode = static_cast<FileInode *>(inode);
  off_t current_size = static_cast<off_t>(inode->attr.size);

  if (target_size == current_size) return 0;

  if (target_size > current_size) {
    RawObjHandle *raw_handle = nullptr;
    int open_ret = PERFORM_BACKGROUND_OBJ_REQUEST(
        this, open_object, full_path, O_WRONLY, (mode_t)0777, &raw_handle);
    if (open_ret < 0) {
      LOG_ERROR("Failed to open for fallocate, nodeid: `, ret: `", nodeid,
                open_ret);
      return open_ret;
    }

    DEFER({
      if (raw_handle) {
        int close_ret = raw_handle->close();
        if (close_ret < 0) {
          LOG_ERROR("Failed to close raw handle, nodeid: `, r: `", nodeid,
                    close_ret);
        }
        delete raw_handle;
      }
    });

    off_t extend_len = target_size - current_size;
    r = raw_handle->fallocate(current_size, extend_len);
    if (r < 0) {
      LOG_ERROR("Failed to fallocate, nodeid: `, offset: `, len: `, r: `",
                nodeid, current_size, extend_len, r);
      return r;
    }

    inode->attr.size = target_size;
    clock_gettime(CLOCK_REALTIME, &inode->attr.mtime);
    return 0;
  }

  r = PERFORM_BACKGROUND_OBJ_REQUEST(this, truncate_object, full_path,
                                     target_size);
  if (r < 0) {
    LOG_ERROR("Failed to truncate, nodeid: `, size: `, r: `", nodeid,
              target_size, r);
    return r;
  }

  file_inode->invalidate_data_cache = true;
  file_inode->attr.size = target_size;
  clock_gettime(CLOCK_REALTIME, &inode->attr.mtime);
  LOG_INFO("truncate success, nodeid: `, path: `, target_size: `", nodeid,
           full_path, target_size);
  return 0;
}

int OssFs::do_hdfs_ftruncate(uint64_t nodeid, Inode *inode, off_t target_size,
                             struct fuse_file_info *fi, uid_t caller_uid,
                             gid_t caller_gid) {
  if (!fi || !fi->fh) {
    LOG_ERROR("ftruncate called without file handle, nodeid: `", nodeid);
    return -EBADF;
  }

  int r = check_permission(PermOp::Ftruncate, inode, caller_uid, caller_gid);
  if (r < 0) return r;

  auto *handle = reinterpret_cast<IFileHandleFuseLL *>(fi->fh);
  r = handle->ftruncate(target_size);
  if (r < 0) return r;

  // size is already updated by handle->ftruncate().
  // Invalidate caches so next getattr fetches fresh mtime/size from backend.
  auto *file_inode = static_cast<FileInode *>(inode);
  file_inode->invalidate_data_cache = true;
  inode->attr_time = 0;
  LOG_INFO("ftruncate success, nodeid: `, target_size: `", nodeid, target_size);
  return 0;
}

int OssFs::fallocate(uint64_t nodeid, off_t offset, off_t length, void *fh) {
  if (!is_hdfs_mode()) return -ENOTSUP;

  if (length > 0 && offset > std::numeric_limits<off_t>::max() - length) {
    return -EFBIG;
  }
  off_t new_end = offset + length;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  Inode *inode = ref.inode;

  std::unique_lock<std::shared_mutex> l(inode->inode_lock);
  if (inode->is_stale) return -ESTALE;

  auto *handle = static_cast<IFileHandleFuseLL *>(fh);
  int r = handle->fallocate(offset, length);
  if (r < 0) return r;

  if (new_end > static_cast<off_t>(inode->attr.size)) {
    inode->attr.size = new_end;
    auto *file_inode = static_cast<FileInode *>(inode);
    file_inode->invalidate_data_cache = true;
    inode->attr_time = 0;
  }
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

int OssFs::flush_dirty_inodes_for_rename(Inode *src_node,
                                         std::string_view src_path) {
  if (is_hdfs_mode()) return 0;

  std::vector<FileInode *> dirty_inodes;
  if (!src_node->is_dir()) {
    FileInode *src_file_node = static_cast<FileInode *>(src_node);
    if (src_file_node->is_dirty_file()) {
      dirty_inodes.push_back(src_file_node);
      LOG_INFO("rename for ` which is dirty inodes", src_path);
    }
  } else {
    auto all_dirty_nodeids = get_dirty_nodeids();
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

  // dirty_fh and rw_ctx alias one union slot; the write mode picks the member.
  const bool random_mode = (write_mode() == WriteMode::Random);
  for (auto &dinode : dirty_inodes) {
    std::unique_lock<std::shared_mutex> wl(dinode->inode_lock, std::defer_lock);
    if (dinode != src_node) wl.lock();
    if (!random_mode && dinode->dirty_fh) {
      auto file = dinode->dirty_fh;
      int r = file->fdatasync_lock_held();
      if (r < 0) {
        LOG_ERROR("fail to fdatasync dirty file `, with error: `",
                  dinode->nodeid, r);
        return r;
      }
    } else if (dinode->is_dirty && dinode->rw_ctx) {
      // Random mode: flush via a transient writer (see random_write_truncate).
      auto writer = create_oss_writer(this, dinode->rw_ctx->upload_path, dinode,
                                      /*flags=*/0);
      int r = writer->open();
      if (r < 0) {
        LOG_ERROR("rename: transient writer open failed, nodeid `, r `",
                  dinode->nodeid, r);
        return r;
      }
      DEFER(writer->close());
      r = writer->flush();
      if (r < 0) {
        LOG_ERROR("rename: transient writer flush failed, nodeid `, r `",
                  dinode->nodeid, r);
        return r;
      }
    }
  }
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

  return do_rename_locked(o_parent, n_parent, src_node, old_name, new_name,
                          new_parent, ref.ref1.parent_path,
                          ref.ref2.parent_path, flags);
}

// Caller must hold the unique inode_lock of both parents (locked in nodeid
// order) and the unique inode_lock of src_node.
int OssFs::do_rename_locked(DirInode *o_parent, DirInode *n_parent,
                            Inode *src_node, std::string_view old_name,
                            std::string_view new_name, uint64_t new_parent,
                            std::string_view src_parent_path,
                            std::string_view dst_parent_path,
                            unsigned int flags) {
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

  std::string src_path(src_parent_path);
  std::string dst_path(dst_parent_path);
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
    int r =
        PERFORM_BACKGROUND_OBJ_REQUEST(this, stat, dst_path, &st, &unused_etag);
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
    int r =
        PERFORM_BACKGROUND_OBJ_REQUEST(this, is_dir_empty, dst_path, is_empty);
    if (r != 0) {
      LOG_ERROR("fail to list dir `, with error: `", dst_path, r);
      return r;
    }
    if (!is_empty) {
      LOG_ERROR("dir ` is not empty in cloud, cannot rename", dst_path);
      return -ENOTEMPTY;
    }
  }

  int r = flush_dirty_inodes_for_rename(src_node, src_path);
  if (r < 0) return r;

  // In random-write mode, hide an opened dst instead of overwriting it.
  bool need_hide = write_mode() == WriteMode::Random && dst_node &&
                   !dst_node->is_stale && dst_node->is_file() &&
                   dst_node->open_ref_cnt > 0;
  if (need_hide) {
    r = hide_inode(n_parent, dst_node, dst_parent_path);
    if (r < 0) return r;
  }

  if (src_node->is_dir()) {
    r = rename_dir(src_path, dst_path, dst_node != nullptr);
  } else {
    r = rename_file(src_path, dst_path, dst_node != nullptr && !need_hide);
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
    if (dst_node && !need_hide) {
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

// Rename a file to ".fuse_hiddenXXX" (libfuse naming: nodeid + seq) under
// the same parent. Retries with a new seq on -EEXIST, -EBUSY after that.
// Caller must hold the unique inode_lock of parent and src_node.
int OssFs::hide_inode(DirInode *parent, Inode *src_node,
                      std::string_view parent_path) {
  RELEASE_ASSERT(!src_node->is_dir());
  RELEASE_ASSERT(src_node->parent == parent);
  RELEASE_ASSERT(src_node->open_ref_cnt > 0);

  constexpr int kMaxRetry = 10;
  for (int i = 0; i < kMaxRetry; ++i) {
    char hidden_name[48];
    snprintf(hidden_name, sizeof(hidden_name), ".fuse_hidden%08x%08x%08x",
             (unsigned int)(src_node->nodeid >> 32),
             (unsigned int)(src_node->nodeid & 0xffffffff),
             hidden_inode_seq_.fetch_add(1));
    LOG_INFO("hide_inode. parent: `, name: `, hidden_name: `", parent->nodeid,
             src_node->name, hidden_name);
    int r = do_rename_locked(parent, parent, src_node, src_node->name,
                             hidden_name, parent->nodeid, parent_path,
                             parent_path, RENAME_NOREPLACE);
    if (r == 0) {
      src_node->is_hidden = true;
      return 0;
    }
    if (r != -EEXIST) return r;
    LOG_WARN("hidden name ` already exists under `, retry", hidden_name,
             parent->nodeid);
  }

  LOG_ERROR("fail to hide ` under ` after ` retries", src_node->name,
            parent->nodeid, kMaxRetry);
  return -EBUSY;
}

void OssFs::delete_hidden_inode(FileInode *inode, std::string_view inode_path) {
  DirInode *parent = static_cast<DirInode *>(inode->parent);
  std::unique_lock<std::shared_mutex> pl(parent->inode_lock);
  std::unique_lock<std::shared_mutex> il(inode->inode_lock);

  if (inode->open_ref_cnt != 0) return;

  if (inode_path.empty()) {
    LOG_ERROR("fail to delete hidden object, nodeid: `, empty path",
              inode->nodeid);
    return;
  }

  // Hold the parent lock until the delete lands so a concurrent readdirplus
  // cannot fill the hidden entry with this inode while the object is being
  // deleted, and a rename/create against the hidden name cannot take over
  // the path first.
  int dr = PERFORM_BACKGROUND_OBJ_REQUEST(this, delete_object, inode_path);
  if (dr < 0 && dr != -ENOENT) {
    LOG_ERROR("fail to delete hidden object `, error: `", inode_path, dr);
    return;
  }

  {
    std::lock_guard<std::mutex> ml(inodes_map_lck_);
    inode->is_stale = true;
  }
  LOG_INFO("deleted hidden object `, nodeid: `", inode_path, inode->nodeid);
}

void OssFs::mark_ghost_children_stale(DirInode *parent_inode) {
  for (auto &cit : parent_inode->children) {
    Inode *child = cit.second;
    // Only files are reaped; a dir's local state may not be fully uploaded.
    if (!child->is_file()) continue;

    std::unique_lock<std::shared_mutex> cl(child->inode_lock);
    if (child->is_stale || child->open_ref_cnt != 0) continue;
    // A dirty file may not be visible in the remote listing yet.
    if (static_cast<FileInode *>(child)->is_dirty) continue;

    LOG_INFO("mark ghost child ` of dir ` stale after empty remote listing",
             cit.first, parent_inode->nodeid);
    {
      std::lock_guard<std::mutex> l(inodes_map_lck_);
      child->is_stale = true;
    }
  }
}

// parent's wlock + inode's wlock (for the whole func)
// No need to delete the inode inside. FUSE kernel will handle it later
// (forget).
int OssFs::unlink(uint64_t parent, std::string_view name, uid_t caller_uid,
                  gid_t caller_gid) {
  LOG_INFO("unlink. parent: `, name `", parent, name);

  GET_PARENT_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent, name);
  DirInode *parent_inode = ref.parent;

  std::unique_lock<std::shared_mutex> pl(parent_inode->inode_lock);
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

  if (is_hdfs_mode()) {
    // HDFS-specific: check permission on the child inode rather than parent
    // directory. HDFS NameNode enforces the real permission on unlink RPC;
    // this pre-check is a fast-path rejection to avoid unnecessary RPCs.
    int r = check_permission(PermOp::Unlink, child, caller_uid, caller_gid);
    if (r < 0) return r;
  }

  // In random-write mode, hide an opened file instead of deleting it; the
  // hidden object is removed on the last release.
  if (write_mode() == WriteMode::Random && child->is_file() &&
      child->open_ref_cnt > 0) {
    return hide_inode(parent_inode, child, ref.parent_path);
  }

  auto full_path = ref.parent_path;
  if (full_path.back() != '/') full_path.append("/");
  full_path.append(name.data(), name.size());

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, delete_object, full_path);
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
                         InodeType::kFile, "", mode & kPermMask, uid, gid);
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
    int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, stat, full_path, &stbuf,
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
    inode->update_attr(stbuf.st_size, stbuf.st_mtim, stbuf.st_atim);
    resync_randwrite_remote_size(inode);
  }

  if (inode->invalidate_data_cache) {
    evict_inode_cache(inode);
  }

  // Random write + prefetching needs shared cache so mark_clean() can drop it.
  if (inode->open_ref_cnt == 0 && enable_prefetching() &&
      (options_.share_fd_read_buffer || write_mode() == WriteMode::Random)) {
    inode->cache = create_inode_cache();
  }

  if (flags & O_TRUNC) {
    if (write_mode() != WriteMode::Random && inode->is_dirty &&
        inode->attr.size != 0) {
      LOG_ERROR("file ` is being written, cannot be truncated", nodeid);
      return -EBUSY;
    }
  }

  auto file_handle = create_file_handle(full_path, inode, flags,
                                        inode->get_mode() & kPermMask);
  auto r = file_handle->open();
  if (r < 0) {
    file_handle->release();
    return r;
  }

  *fh = file_handle;
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
  IFileHandleFuseLL *handle = static_cast<IFileHandleFuseLL *>(fh);
  RELEASE_ASSERT(handle);
  FileInode *inode = static_cast<FileInode *>(handle->get_inode());

  int r = 0;
  bool delete_hidden = false;
  {
    std::unique_lock<std::shared_mutex> l(inode->inode_lock);
    inode->open_ref_cnt--;

    if (inode->open_ref_cnt == 0 && inode->is_hidden) {
      delete_hidden = true;
    }

    r = handle->close();
    if (r < 0) {
      LOG_ERROR("fail to close file, nodeid: `, error: `", nodeid, r);
      inode->invalidate_data_cache = true;
    } else {
      LOG_INFO("release file, nodeid: `", nodeid);
    }

    if (inode->open_ref_cnt == 0) {
      if (inode->cache) inode->cache.reset();
    }
  }

  // Delete the hidden object only after close(), which may flush data to it.
  if (delete_hidden) {
    delete_hidden_inode(inode, ref.inode_path);
  }

  handle->release();
  return r;
}

// ********************* dir related apis *********************
// dir's wlock
int OssFs::opendir(uint64_t nodeid, struct fuse_file_info *fi) {
  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  RELEASE_ASSERT(ref.inode->is_dir());
  DirInode *dir_inode = static_cast<DirInode *>(ref.inode);
  {
    std::unique_lock<std::shared_mutex> wl(dir_inode->inode_lock);
    if (dir_inode->is_stale) {
      return -ESTALE;
    }

    if (options_.kernel_readdir_cache_timeout > 0) {
      fi->cache_readdir = 1;
      if (dir_inode->is_kernel_readdir_cache_valid(
              options_.kernel_readdir_cache_timeout)) {
        fi->keep_cache = 1;
      } else {
        dir_inode->update_kernel_readdir_cache_status();
      }
    }
    dir_inode->open_ref_cnt++;
  }

  auto dh = new OssDirHandle(this, static_cast<DirInode *>(dir_inode),
                             ref.inode_path, options_.readdir_remember_count);
  LOG_INFO("open dir: ` nodeid: `", ref.inode_path, nodeid);

  fi->fh = reinterpret_cast<uint64_t>(dh);

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

    // The listing ran from the start to the end without any entry: the
    // remote dir is confirmed empty, so reap ghost file children left by
    // remote deletions.
    if (r == 0 && start == 0 && odh->telldir() == 0) {
      mark_ghost_children_stale(parent_inode);
    }
  } else {
    std::shared_lock<std::shared_mutex> prl(parent_inode->inode_lock);
    if (parent_inode->is_stale) return -ESTALE;

    std::lock_guard<std::mutex> lk(odh->dir_lock_);

    r = seek_dir(parent_inode, odh, start, is_interrupted, interrupted_ctx);
    if (r != 0) return r;

    FAULT_INJECTION(FI_Readdir_Delay_Noplus, []() {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    });

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
                         InodeType::kDir, "", mode & kPermMask, uid, gid);
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
  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, delete_object,
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
  if (link.empty()) return -EINVAL;

  // Normalize link to handle '.', '..', consecutive '/', etc.
  std::string normalized_link =
      std::filesystem::path(link).lexically_normal().string();
  std::string effective_link = normalized_link;

  auto starts_with_mountpoint = [this](std::string_view path) {
    // Normalized Mount point path(e.g., "/mnt/ossfs")
    auto mp = options_.mountpoint;
    if (mp.empty() || path.size() <= mp.size() + 1) return false;
    return path.substr(0, mp.size()) == mp && path[mp.size()] == '/';
  };

  if (normalized_link.front() == '/') {
    if (!starts_with_mountpoint(normalized_link)) {
      LOG_WARN("Absolute symlink target not under mountpoint: `",
               normalized_link);
      return -EINVAL;
    }

    // Strip mountpoint: "/mnt/ossfs2/a/c/file" -> "a/c/file"
    std::string mount_rel =
        std::string(normalized_link.substr(options_.mountpoint.size() + 1));

    // Make relative to symlink's parent directory.
    GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(parent);
    auto parent_rel = std::filesystem::path(ref.inode_path.substr(1));
    effective_link = std::filesystem::path(mount_rel)
                         .lexically_relative(parent_rel)
                         .string();
  }

  return create_internal(parent, name, 0, nodeid, stbuf, nullptr,
                         InodeType::kSymlink, effective_link, 0777, uid, gid);
}

ssize_t OssFs::readlink(uint64_t nodeid, char *buf, size_t size) {
  if (!options_.enable_symlink) return -ENOTSUP;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::shared_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;

  const auto &full_path = ref.inode_path;
  std::string target;
  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, get_symlink, full_path, target);
  if (r < 0) {
    return r;
  }

  auto write_size = std::min(size, target.size());
  memcpy(buf, target.c_str(), write_size);
  return write_size;
}

int OssFs::mknod(uint64_t parent, std::string_view name, mode_t mode, uid_t uid,
                 gid_t gid, uint64_t *nodeid, struct stat *stbuf) {
  LOG_INFO("mknod. parent: `, name: `, mode: 0o`", parent, name, OCT(mode));

  if (!is_hdfs_mode()) return -ENOSYS;

  // Only regular file (S_IFREG or 0) is supported.
  mode_t type = mode & S_IFMT;
  if (type != 0 && type != S_IFREG) {
    LOG_WARN("mknod: unsupported type 0o`", OCT(type));
    return -ENOTSUP;
  }

  // mknod: fh=nullptr triggers immediate backend file creation, no handle.
  return create_internal(parent, name, 0, nodeid, stbuf, nullptr,
                         InodeType::kFile, "", mode & kPermMask, uid, gid);
}

int OssFs::flock(uint64_t nodeid, void *fh, int op, uint64_t lock_owner) {
  if (!is_hdfs_mode()) return -ENOTSUP;

  int real_op = op & ~LOCK_NB;

  // LOCK_UN is always allowed (it's used to release locks).
  // LOCK_EX and LOCK_SH require LOCK_NB (blocking mode not supported yet).
  if (real_op != LOCK_UN && !(op & LOCK_NB)) {
    return -ENOTSUP;
  }

  int16_t type;
  if (real_op == LOCK_EX) {
    type = static_cast<int16_t>(LockType::WrLock);
  } else if (real_op == LOCK_SH) {
    type = static_cast<int16_t>(LockType::RdLock);
  } else if (real_op == LOCK_UN) {
    type = static_cast<int16_t>(LockType::UnLock);
  } else {
    return -ENOTSUP;
  }

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::unique_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;
  const auto &full_path = ref.inode_path;

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(
      this, set_lock, full_path, static_cast<int64_t>(0),
      static_cast<int64_t>(0), type, static_cast<int64_t>(getpid()),
      lock_owner);

  // Track flock state on the handle for release-on-close.
  if (r == 0 && fh) {
    auto *handle = static_cast<HdfsFileHandle *>(fh);
    if (real_op == LOCK_UN) {
      handle->clear_flock_held();
    } else {
      handle->set_flock_held(lock_owner);
    }
  }
  return r;
}

int OssFs::setxattr(uint64_t nodeid, const char *name, const char *value,
                    size_t size, int flags) {
  if (!is_hdfs_mode() || !options_.enable_xattr) return -ENOTSUP;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::unique_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;
  const auto &full_path = ref.inode_path;

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, set_xattr, full_path, name,
                                         value, size, flags);
  LOG_DEBUG("setxattr, nodeid: `, path: `, name: `, size: `, flags: `, r: `",
            nodeid, full_path, name, size, flags, r);
  return r;
}

int OssFs::getxattr(uint64_t nodeid, const char *name, char *value,
                    size_t size) {
  if (!is_hdfs_mode() || !options_.enable_xattr) return -ENOTSUP;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::shared_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;
  const auto &full_path = ref.inode_path;

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, get_xattr, full_path, name,
                                         value, size);
  LOG_DEBUG("getxattr, nodeid: `, path: `, name: `, size: `, r: `", nodeid,
            full_path, name, size, r);
  return r;
}

int OssFs::listxattr(uint64_t nodeid, char *list, size_t size) {
  if (!is_hdfs_mode() || !options_.enable_xattr) return -ENOTSUP;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::shared_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;
  const auto &full_path = ref.inode_path;

  int r =
      PERFORM_BACKGROUND_OBJ_REQUEST(this, list_xattr, full_path, list, size);
  LOG_DEBUG("listxattr, nodeid: `, path: `, size: `, r: `", nodeid, full_path,
            size, r);
  return r;
}

int OssFs::removexattr(uint64_t nodeid, const char *name) {
  if (!is_hdfs_mode() || !options_.enable_xattr) return -ENOTSUP;

  GET_INODE_REF_AND_LOCK_PATH_IF_NEEDED_WITH_RET(nodeid);
  std::unique_lock<std::shared_mutex> l(ref.inode->inode_lock);
  if (ref.inode->is_stale) return -ESTALE;
  const auto &full_path = ref.inode_path;

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, remove_xattr, full_path, name);
  LOG_DEBUG("removexattr, nodeid: `, path: `, name: `, r: `", nodeid, full_path,
            name, r);
  return r;
}

ssize_t OssFs::read(uint64_t nodeid, void *fh, size_t size, off_t off,
                    std::function<void(void *buf, size_t size)> read_cb) {
  auto file_handle = static_cast<IFileHandleFuseLL *>(fh);
  void *mem = nullptr;
  auto r = file_handle->pin(off, size, &mem);
  if (r > 0) {
    read_cb(mem, r);
    file_handle->unpin(off);
    return r;
  }

  DECLARE_METRIC_LATENCY(pread, Metric::MetricsType::kInternalMetrics);
  bool from_pool = false;
  if (size <= options_.cache_block_size && download_buffers_ != nullptr) {
    mem = static_cast<void *>(download_buffers_->allocate(1).front());
    from_pool = true;
  } else {
    r = posix_memalign(&mem, 4096, size);
    if (r != 0) {
      return -ENOMEM;
    }
  }

  r = file_handle->pread(mem, size, off);
  if (r >= 0) {
    read_cb(mem, r);
  }

  if (from_pool) {
    std::vector<char *> ptr_vec{static_cast<char *>(mem)};
    download_buffers_->deallocate(ptr_vec);
  } else {
    free(mem);
  }
  return r;
}

ssize_t OssFs::write(uint64_t nodeid, void *fh, const char *buf, size_t size,
                     off_t off) {
  auto file_handle = static_cast<IFileHandleFuseLL *>(fh);
  return file_handle->pwrite(buf, size, off);
}

ssize_t OssFs::write_buf(uint64_t nodeid, void *fh, struct fuse_bufvec *bufv,
                         off_t off) {
  auto file_handle = static_cast<IFileHandleFuseLL *>(fh);
  return file_handle->write_buf(bufv, off);
}

int OssFs::fsync(uint64_t nodeid, void *fh, bool datasync) {
  auto file_handle = static_cast<IFileHandleFuseLL *>(fh);
  int result = datasync ? file_handle->fdatasync() : file_handle->fsync();
  return result;
}

int OssFs::flush(uint64_t nodeid, void *fh) {
  auto file_handle = static_cast<IFileHandleFuseLL *>(fh);
  return file_handle->fsync();
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
    FAULT_INJECTION(FI_Lookup_Oss_Failure, [&]() { r = -EIO; });
    if (r < 0) return r;

    FAULT_INJECTION(FI_Lookup_Delay_Before_Getting_OSS_Response, []() {
      std::this_thread::sleep_for(std::chrono::milliseconds(2 * 1000));
    });

    r = PERFORM_BACKGROUND_OBJ_REQUEST(this, stat, full_path, stbuf,
                                       remote_etag);
    *attr_time = time(0);
  }

  FAULT_INJECTION(FI_Lookup_Delay_After_Getting_Remote_attr, []() {
    std::this_thread::sleep_for(std::chrono::milliseconds(2 * 1000));
  });
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

        child_inode->set_mode(stbuf->st_mode);
        child_inode->set_uid(stbuf->st_uid);
        child_inode->set_gid(stbuf->st_gid);
        child_inode->update_attr(stbuf->st_size, stbuf->st_mtim,
                                 stbuf->st_atim);
        resync_randwrite_remote_size(child_inode);
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
  if (is_dir && !is_hdfs_mode()) {
    // OSS SDK returns epoch 0 for dir mtime. Use current time instead.
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    stbuf->st_mtim = now;
  }

  uint64_t parent = parent_inode->nodeid;
  Inode *child_inode = create_new_inode(
      allocated_nodeid, name, stbuf->st_size, stbuf->st_mtim,
      Inode::mode_to_inode_type(stbuf->st_mode), false, parent, parent_inode,
      remote_etag, stbuf->st_mode & kPermMask, stbuf->st_uid, stbuf->st_gid);

  // A new inode may be created and forgotten and inserted to the staged cache
  // after lookup_try_local_attr_cache and before lookup_update_local_cache, so
  // we also need to try to remove it from the staged cache.
  increment_inode_lookupcnt(child_inode, parent, name);
  child_inode->update_attr(stbuf->st_size, stbuf->st_mtim, stbuf->st_atim);
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
      lookup_from_staged_cache ? stbuf->st_ino : id_manager_->next_id();
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
                           InodeType type, std::string_view link, mode_t mode,
                           uid_t uid, gid_t gid) {
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
    r = PERFORM_BACKGROUND_OBJ_REQUEST(this, stat, full_path, &st,
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
    r = PERFORM_BACKGROUND_OBJ_REQUEST(this, put_object,
                                       add_backslash(full_path), &iov, 1,
                                       &expected_crc64, mode & kPermMask);
    if (r < 0) {
      LOG_ERROR("fail to mkdir from cloud. path ` with error `", full_path, r);
      return r;
    }
  } else if (type == InodeType::kSymlink) {
    r = PERFORM_BACKGROUND_OBJ_REQUEST(this, put_symlink, full_path, link);
    if (r < 0) {
      LOG_ERROR("fail to create symlink from cloud. path ` link ` with error `",
                full_path, link, r);
      return r;
    }
    file_size = r;
  } else {
    // kFile
    if (fh == nullptr) {
      // mknod: create empty file on backend immediately.
      iovec iov{nullptr, 0};
      uint64_t expected_crc64 = 0;
      r = PERFORM_BACKGROUND_OBJ_REQUEST(this, put_object, full_path, &iov, 1,
                                         &expected_crc64, mode & kPermMask);
      if (r < 0) {
        LOG_ERROR("mknod: put_object failed, path `, r: `", full_path, r);
        return r;
      }
    }
    // creat (fh != nullptr): empty body, upload delayed to flush().
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  Inode *child_inode;
  child_inode = create_new_inode(id_manager_->next_id(), name, file_size, now,
                                 type, false, parent, parent_inode, "",
                                 mode & kPermMask, uid, gid);

  // A create request is equivalent to mknod + open, so we need to open the
  // regular file here. For mknod (fh == nullptr), skip file handle creation.
  if (type == InodeType::kFile && fh != nullptr) {
    FileInode *child_file_inode = static_cast<FileInode *>(child_inode);

    auto file_handle = create_file_handle(full_path, child_file_inode,
                                          flags | O_CREAT, mode & kPermMask);
    r = file_handle->open();  // this will never return error.
    if (r < 0) {
      LOG_ERROR("fail to open file after create. path ` with error `",
                full_path, r);
      file_handle->release();
      delete child_inode;
      return r;
    }

    *fh = file_handle;
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

  if (options_.hdfs_set_owner_on_create && is_hdfs_mode() && (uid || gid)) {
    int r2 =
        PERFORM_BACKGROUND_OBJ_REQUEST(this, set_owner, full_path, uid, gid,
                                       IObjStore::kSetUid | IObjStore::kSetGid);
    if (r2 < 0) {
      LOG_WARN("set_owner on create failed, path: `, uid: `, gid: `, r: `",
               full_path, uid, gid, r2);
    }
  }

  return 0;
}

int OssFs::get_one_list_results(std::string_view full_path,
                                std::vector<ObjDirent> &results,
                                std::string &marker) {
  results.clear();
  return PERFORM_BACKGROUND_OBJ_REQUEST(this, list_dir, full_path, results,
                                        &marker);
}

// Mint a new inode if it doesn't exist based on the given results.
// Not increment lookup_cnt.
void OssFs::construct_inodes_if_needed(DirInode *parent_inode,
                                       OssDirHandle *dh) {
  std::vector<ObjDirent> ents;
  dh->get_cur_list_res(ents);
  for (size_t i = dh->get_list_pos(); i < ents.size(); i++) {
    const auto &oss_ent = ents[i];

    uint64_t allocated_nodeid = 0;
    struct stat st;
    memset(&st, 0, sizeof(st));
    InodeType inode_type = Inode::dirent_type_to_inode_type(oss_ent.type());
    st.st_mode = Attribute::get_default_full_mode(inode_type);
    st.st_size = oss_ent.size();
    st.st_mtim = oss_ent.mtime();

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
        try_update_inode_attr_from_list(child_inode, &st, oss_ent.etag(),
                                        oss_ent.mode(), oss_ent.uid(),
                                        oss_ent.gid());
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

    if (inode_type == InodeType::kDir && !is_hdfs_mode()) {
      // OSS SDK returns epoch 0 for dir mtime. Use current time instead.
      struct timespec now;
      clock_gettime(CLOCK_REALTIME, &now);
      st.st_mtim = now;
    }

    if (allocated_nodeid == 0) {
      allocated_nodeid = id_manager_->next_id();
    }

    auto new_child_node = create_new_inode(
        allocated_nodeid, oss_ent.name(), st.st_size, st.st_mtim, inode_type,
        false, parent_inode->nodeid, parent_inode, oss_ent.etag(),
        oss_ent.mode(), oss_ent.uid(), oss_ent.gid());
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
                              std::map<estring, ObjDirent> &dirty_children_) {
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
          cit.first, ObjDirent(child->name, child->attr.size, child->attr.mtime,
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
  std::map<estring, ObjDirent> dirty_children;
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

  FAULT_INJECTION(FI_Readdir_Delay_Before_Construct, []() {
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  });

  // refresh_dir will trigger listobj
  construct_inodes_if_needed(parent_inode, odh);

  FAULT_INJECTION(FI_Readdir_Delay_After_Construct_Inodes, []() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  });

  return 0;
}

// parent's rlock inside
int OssFs::refresh_dir(DirInode *parent_inode, OssDirHandle *odh) {
  int r = 0;
  // We save current dirty children to a temporary set, in case a
  // currently dirty child becomes clean and is in the list result (filled
  // twice)
  std::map<estring, ObjDirent> dirty_children;
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
      // TODO: constructing from the cached listing snapshot may resurrect
      // locally deleted children as ghost inodes if a deletion landed after
      // the snapshot. Fix with a listing-freshness guard in a future version.
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

      FAULT_INJECTION(FI_Readdir_Delay_After_Construct_Inodes, []() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      });
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
    st.st_mode = Attribute::get_default_full_mode(
        Inode::dirent_type_to_inode_type(dent->type()));
    st.st_size = dent->size();
    st.st_mtim = dent->mtime();
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
                                            std::string_view remote_etag,
                                            mode_t perm, uid_t uid, gid_t gid) {
  assert(inode);
  assert(stbuf);

  if (!inode->is_dir() && static_cast<FileInode *>(inode)->is_dirty) {
    return;
  }

  invalidate_data_cache_if_needed(inode, stbuf, remote_etag);
  update_inode_etag(inode, remote_etag);
  inode->set_mode((inode->get_mode() & ~kPermMask) | (perm & kPermMask));
  inode->set_uid(uid);
  inode->set_gid(gid);
  inode->update_attr(stbuf->st_size, stbuf->st_mtim, stbuf->st_atim);
  resync_randwrite_remote_size(inode);
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
      // The nodeid below is necessary: in case this inode was stale, and a new
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
                               std::string_view remote_etag, mode_t perm,
                               uid_t uid, gid_t gid) {
  Inode *inode = nullptr;
  if (type == InodeType::kDir) {
    inode = new DirInode(nodeid, name, mtime, parent_nodeid, parent_node);
  } else {
    inode = new FileInode(nodeid, name, size, mtime, type, is_dirty,
                          parent_nodeid, parent_node, remote_etag);
  }
  if (inode == nullptr) {
    LOG_ERROR("fail to create a new inode.");
    return nullptr;
  }

  if (is_hdfs_mode()) {
    inode->ensure_posix_ext();
    inode->set_mode(Attribute::build_mode(type, perm & kPermMask));
    inode->set_uid(uid);
    inode->set_gid(gid);
    inode->set_atime(mtime);
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
      stbuf->st_mode = Attribute::get_default_full_mode(entry->type);
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
    creds_provider_ = new_process_creds_provider(
        options_.credential_process, options_.credential_refresh_interval);
  }

  int r = 0;
  {
    auto t0 = std::chrono::steady_clock::now();
    if (creds_provider_) {
      std::promise<int> result_promise;
      creds_refresh_th_ = new std::thread(&OssFs::start_creds_refresher, this,
                                          std::ref(result_promise));
      r = result_promise.get_future().get();
    } else {
      r = PERFORM_BACKGROUND_OBJ_REQUEST(this, check_bucket);
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    LOG_INFO("[MountTiming] bucket validation completed in ` us", elapsed);
  }

  if (r < 0) {
    LOG_ERROR("fail to check bucket with error `", r);
    return r;
  }

  if (is_hdfs_mode()) {
    init_hdfs_root_inode();
    Attribute::DEFAULT_BLKSIZE = 512;

    if (options_.max_inode_cache_count > 0 && options_.attr_timeout > 0) {
      LOG_WARN("Staged inode cache disabled in HDFS mode (TODO: PosixExtAttr)");
      return -EINVAL;
    }
  }

  return r;
}

void OssFs::init_hdfs_root_inode() {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  mp_inode_->ensure_posix_ext();
  mp_inode_->set_mode(Attribute::build_mode(InodeType::kDir, 0755));
  mp_inode_->set_uid(kReservedUnresolvedUid);
  mp_inode_->set_gid(kReservedUnresolvedGid);
  mp_inode_->set_atime(now);
}

IFileHandleFuseLL *OssFs::create_file_handle(const std::string &path,
                                             FileInode *inode, int flags,
                                             mode_t mode) {
  // Dispatch to appropriate file handle based on storage backend type.
  switch (get_backend_type()) {
    case IObjStore::StorageBackend::kHDFS:
      return new HdfsFileHandle(this, path, inode, flags, mode);
    case IObjStore::StorageBackend::kOSS:
    default:
      return create_oss_file_handle(this, path, inode, flags);
  }
}

int OssFs::access(uint64_t nodeid, int mask, uid_t caller_uid,
                  gid_t caller_gid) {
  // OSS mode: kernel handles permissions via default_permissions.
  if (!is_hdfs_mode()) {
    return 0;
  }

  // HDFS mode: perform custom permission check.
  struct stat stbuf;
  int r = getattr(nodeid, &stbuf);
  if (r < 0) return r;
  return check_hdfs_access(&stbuf, mask, caller_uid, caller_gid);
}

std::shared_ptr<ICache> OssFs::create_inode_cache() {
  std::shared_ptr<ICache> cache = nullptr;
  switch (options_.cache_type) {
    case CacheType::kFhCache:
      cache = std::make_shared<BlockCache>(download_buffers_);
      break;
    case CacheType::kDiskCache:
      cache = std::make_shared<DiskCache>(bg_vcpu_env_.bg_disk_cache_env,
                                          download_buffers_);
      break;
    default:
      std::abort();
  }
  return cache;
}

void OssFs::evict_inode_cache(FileInode *inode) {
  if (inode->cache) {
    inode->cache = create_inode_cache();
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

  auto background_env = bg_vcpu_env_.bg_obj_store_env->get_obj_store_env_next();
  int r = background_env.executor->perform([&]() {
    auto obj_store = background_env.obj_store;
    iovec iov{nullptr, 0};
    uint64_t expected_crc64 = 0;

    ssize_t ret = 0;
    if (write_mode() == WriteMode::Appendable) {
      ret = obj_store->delete_object(full_path);
      if (ret < 0) {
        LOG_ERROR("Failed to unlink file: `, nodeid: ` r: `", full_path,
                  file_inode->nodeid, ret);
        return ret;
      }

      ret = obj_store->append_object(full_path, &iov, 1, 0, &expected_crc64);
    } else {
      ret = obj_store->put_object(full_path, &iov, 1, &expected_crc64);
    }

    if (ret < 0) {
      LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", full_path,
                file_inode->nodeid, ret);
      return ret;
    }

    std::string unused_etag;
    int stat_r = obj_store->stat(full_path, &stbuf, &unused_etag);
    return static_cast<ssize_t>(stat_r);
  });

  if (r < 0) {
    return r;
  }

  file_inode->invalidate_data_cache = true;
  file_inode->etag.clear();
  file_inode->update_attr(0, stbuf.st_mtim, stbuf.st_atim);
  return 0;
}

// Realign the remote_size snapshot after a remote attr.size refresh; a stale
// one makes flush zero-fill [remote_size, attr.size) over remote data.
void OssFs::resync_randwrite_remote_size(Inode *inode) {
  if (write_mode() != WriteMode::Random || inode->is_dir()) return;
  auto *file_inode = static_cast<FileInode *>(inode);
  if (file_inode->is_dirty || !file_inode->rw_ctx) return;

  auto *ctx = file_inode->rw_ctx;
  if (ctx->remote_size != static_cast<uint64_t>(file_inode->attr.size)) {
    LOG_INFO("resync remote_size ` -> `, nodeid `, path `", ctx->remote_size,
             file_inode->attr.size, file_inode->nodeid, ctx->upload_path);
    ctx->remote_size = file_inode->attr.size;
  }
}

// Inode wlock + path rlock held outside.
int OssFs::random_write_truncate(FileInode *inode, std::string_view full_path,
                                 uint64_t new_size) {
  const auto max_size = options_.random_write_max_file_size;
  if (new_size > max_size) {
    LOG_ERROR("truncate exceeds max file size `: file: ` nodeid `, new_size `",
              max_size, full_path, inode->nodeid, new_size);
    return -EFBIG;
  }

  // With no open dirty writer to flush this truncate later, flush it now so
  // the file's dirty/clean status does not change.
  const bool needs_sync_flush = (inode->rw_ctx == nullptr || !inode->is_dirty);

  // Creates a transient writer for the file.
  auto writer = create_oss_writer(this, full_path, inode, /*flags=*/0);

  int r = writer->open();
  if (r < 0) {
    LOG_ERROR("random truncate: open failed, nodeid `, r `", inode->nodeid, r);
    return r;
  }
  DEFER(writer->close());

  r = writer->truncate(new_size);
  if (r < 0) return r;

  if (needs_sync_flush) {
    r = writer->flush();
    if (r < 0) {
      LOG_ERROR("random truncate: flush failed, nodeid `, r `", inode->nodeid,
                r);
    }
  }
  return r;
}

int OssFs::staging_disk_avail(int staging_fd, uint64_t *avail_out) {
  // Free bytes of the staging filesystem from one fstatvfs call.
  auto query_avail = [](int fd, uint64_t *out) -> int {
    struct statvfs vfs;
    if (::fstatvfs(fd, &vfs) < 0) {
      int r = -errno;
      LOG_ERROR("fstatvfs failed for staging file fd `: `", fd, r);
      return r;
    }
    *out = static_cast<uint64_t>(vfs.f_bavail) *
           static_cast<uint64_t>(vfs.f_frsize);
    return 0;
  };

  // Compensate a snapshot with staging growth since it was taken.
  auto effective = [&](uint64_t avail, uint64_t usage_snap) -> uint64_t {
    uint64_t usage_now = staging_disk_usage_.load(std::memory_order_relaxed);
    uint64_t growth = usage_now > usage_snap ? usage_now - usage_snap : 0;
    return avail > growth ? avail - growth : 0;
  };

  // Refresh and publish the snapshot. usage_now is sampled BEFORE fstatvfs
  // to keep the estimate conservative: bytes flushed during the fstatvfs
  // call may be absent from its result but are counted in usage_now, so the
  // growth compensation slightly understates avail. Sampling after fstatvfs
  // would overstate avail instead, which could let a budget check pass on
  // an almost-full disk.
  auto refresh_cache = [&]() -> int {
    uint64_t usage_now = staging_disk_usage_.load(std::memory_order_relaxed);
    uint64_t avail = 0;
    int r = query_avail(staging_fd, &avail);
    if (r < 0) return r;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    SCOPED_LOCK(staging_avail_lock_);
    staging_avail_bytes_ = avail;
    staging_avail_usage_snap_ = usage_now;
    staging_avail_ts_ns_ =
        static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;
    *avail_out = effective(staging_avail_bytes_, staging_avail_usage_snap_);
    return 0;
  };

  struct timespec now_ts;
  clock_gettime(CLOCK_MONOTONIC, &now_ts);
  uint64_t now_ns =
      static_cast<uint64_t>(now_ts.tv_sec) * 1000000000ULL + now_ts.tv_nsec;
  {
    SCOPED_LOCK(staging_avail_lock_);
    if (staging_avail_ts_ns_ != 0 &&
        now_ns - staging_avail_ts_ns_ < staging_avail_refresh_ns_) {
      *avail_out = effective(staging_avail_bytes_, staging_avail_usage_snap_);
      return 0;
    }
  }

  // Cache stale: elect one refresher to publish the new snapshot. Losers
  // still need an accurate value (the stale snapshot may miss disk space
  // consumed by others, e.g. after a long idle period), so they run their
  // own fstatvfs without touching the cache.
  if (!staging_avail_refreshing_.exchange(true)) {
    int r = refresh_cache();
    staging_avail_refreshing_.store(false);
    return r;
  }
  return query_avail(staging_fd, avail_out);
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

int OssFs::rename_file(std::string_view old_path, std::string_view new_path,
                       bool dst_exists) {
  int r = PERFORM_BACKGROUND_OBJ_REQUEST(
      this, rename_object, old_path, new_path, options_.set_mime_for_rename_dst,
      dst_exists);
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

int OssFs::rename_dir(std::string_view old_path, std::string_view new_path,
                      bool dst_exists) {
  if (is_hdfs_mode()) {
    return do_rename_dir(old_path, new_path, dst_exists);
  }
  return rename_dir_copy_delete(old_path, new_path);
}

int OssFs::do_rename_dir(std::string_view old_path, std::string_view new_path,
                         bool dst_exists) {
  estring old_obj_parent, new_obj_parent;
  old_obj_parent.appends(old_path, "/");
  new_obj_parent.appends(new_path, "/");

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(this, rename_dir, old_obj_parent,
                                         new_obj_parent, dst_exists);
  if (r != 0) {
    LOG_ERROR("fail to atomically rename directory ` to ` r = `", old_path,
              new_path, r);
    return r;
  }

  return 0;
}

int OssFs::rename_dir_copy_delete(std::string_view old_path,
                                  std::string_view new_path) {
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
  int r =
      PERFORM_BACKGROUND_OBJ_REQUEST(this, list_dir_descendants, old_obj_parent,
                                     list_results, checker, &is_dir_obj);
  if (r != 0) {
    LOG_ERROR("fail to list objects with prefix ` r = `", old_path, r);
    return r;
  }
  if (!checker()) {
    LOG_ERROR("trying to rename ` files one time, stop.", list_results.size());
    return -E2BIG;
  }

  auto after = std::chrono::steady_clock::now();
  auto cost =
      std::chrono::duration_cast<std::chrono::microseconds>(after - before);
  LOG_DEBUG("it cost ` us to list ` objs under `", cost.count(),
            list_results.size(), old_obj_parent);

  if (is_dir_obj) {
    // Copy old_parent_path/ to new_parent_path/.
    r = PERFORM_BACKGROUND_OBJ_REQUEST(this, copy_object, old_obj_parent,
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
                             bg_vcpu_env_.bg_obj_store_env->get_vcpu_next());
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
    r = PERFORM_BACKGROUND_OBJ_REQUEST(this, delete_object, old_obj_parent);
    if (r != 0) {
      LOG_ERROR("fail to delete ` with r `", old_obj_parent, r);
      return r;
    }
  }

  return 0;
}

void *OssFs::do_rename_task(void *arg) {
  auto ctx = (RenameContext *)arg;
  thread_local auto obj_store =
      ctx->fs->bg_vcpu_env_.bg_obj_store_env->get_obj_store();

  int r = 0;

  if (ctx->task_type == RenameContext::kRenameTaskCopy) {
    estring src_obj_path, dst_obj_path;
    src_obj_path.appends(ctx->old_parent_path,
                         ctx->list_results->at(ctx->obj_index));
    dst_obj_path.appends(ctx->new_parent_path,
                         ctx->list_results->at(ctx->obj_index));
    r = obj_store->copy_object(src_obj_path, dst_obj_path);
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
    r = obj_store->delete_objects_under_dir(ctx->old_parent_path, batch_objs);
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

void OssFs::update_max_refill_range_lat(uint64_t latency_us) {
  if (!options_.enable_transmission_control) return;

  tc_lock_.lock();
  max_refill_range_lat_us_ = std::max(max_refill_range_lat_us_, latency_us);
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
    if (max_refill_range_lat_us_ > MAX_LAT_THRESHOLD) {
      curr_prefetch_concurrency =
          std::max(curr_prefetch_concurrency / 2, MIN_PREFETCH_CONCURRENCY);
    } else if (max_refill_range_lat_us_ < MAX_LAT_THRESHOLD / 2) {
      curr_prefetch_concurrency = std::min(curr_prefetch_concurrency + 4,
                                           options_.prefetch_concurrency);
    }

    max_refill_range_lat_us_ = 0;
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

  bool reverse_invalidate_forcefully = false;

  auto thread_func = [&]() {
    uint64_t interval = options_.inode_cache_eviction_interval_ms * 1000;

    FAULT_INJECTION(FI_Reverse_Invalidate_Inode_Forcefully,
                    [&]() { reverse_invalidate_forcefully = true; });
    if (!reverse_invalidate_forcefully) {
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
    if (write_mode() == WriteMode::Random) {
      uint64_t usage = staging_disk_usage_.load(std::memory_order_relaxed);
      // clang-format off
      LOG_INFO(
          "[SystemInfo] staging disk usage: ` bytes (` MiB), free space threshold: ` bytes",
          usage, usage >> 20, options_.temp_dir_free_bytes);
      // clang-format on
    }
    if (enable_staged_cache()) {
      staged_inodes_cache_->print_staged_cache_status();
    }

    return interval;
  };

  LOG_INFO("Start health check");

  photon::Timer timer(interval, thread_func, true);
  while (!is_stopping_) AUTO_USLEEP(100000);
}

void OssFs::update_creds(const ObjCredentials &creds) {
  auto ctxs = bg_vcpu_env_.bg_obj_store_env->get_all_env_cxts();
  for (auto &ctx : ctxs) {
    ctx.executor->perform([&]() {
      ctx.obj_store->set_credentials(
          {creds.accessKeyId, creds.accessKeySecret, creds.securityToken});
    });
  }
}

int OssFs::validate_creds(const ObjCredentials &creds, bool allow_auto_create) {
  auto options = PERFORM_BACKGROUND_OBJ_REQUEST(this, get_options);
  std::unique_ptr<IObjStore> obj_store =
      std::unique_ptr<IObjStore>(new_oss_store("", "", options));
  obj_store->set_credentials(
      {creds.accessKeyId, creds.accessKeySecret, creds.securityToken});
  int r = obj_store->check_bucket(allow_auto_create);
  if (r != 0) {
    LOG_ERROR("Fail to check bucket with ak ` error `", creds.accessKeyId, r);
  }
  return r;
}

std::pair<int, uint64_t> OssFs::do_refresh_creds(bool allow_auto_create) {
  int r = -EINVAL;
  auto info =
      creds_provider_->refresh_credentials([&](const ObjCredentials &creds) {
        r = validate_creds(creds, allow_auto_create);
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

  auto [result, next] = do_refresh_creds(true);
  result_promise.set_value(result);

  photon::Timer timer(next, {this, &OssFs::refresh_creds}, true);
  while (!is_stopping_) AUTO_USLEEP(100000);
}

}  // namespace OssFileSystem
