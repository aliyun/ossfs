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

#include <photon/thread/timer.h>

#include <map>
#include <mutex>
#include <unordered_map>

#include "bg_vcpu_env.h"
#include "common/fault_injector.h"
#include "common/filesystem.h"
#include "common/logger.h"
#include "common/lru_map.h"
#include "credentials/creds_provider.h"
#include "dir.h"
#include "fs/id_manager.h"
#include "inode.h"
#include "mem_pool.h"
#include "negative_cache.h"
#include "staged_inode_cache.h"
#include "test/class_declarations.h"

// Call obj_store's methods in background threads (OSS) or directly in the
// current thread (HDFS, since HDFS SDK manages its own thread pool).
#define PERFORM_BACKGROUND_OBJ_REQUEST(__fs, __func, ...)                   \
  ({                                                                        \
    auto __r = (__fs)->bypass_executor()                                    \
                   ? (__fs)->get_obj_store_direct()->__func(__VA_ARGS__)    \
                   : [&]() {                                                \
                       auto __bg_env_ctx =                                  \
                           (__fs)                                           \
                               ->bg_vcpu_env()                              \
                               .bg_obj_store_env->get_obj_store_env_next(); \
                       return __bg_env_ctx.executor->perform([&]() {        \
                         IObjStore *__c = __bg_env_ctx.obj_store;           \
                         return __c->__func(__VA_ARGS__);                   \
                       });                                                  \
                     }();                                                   \
    __r;                                                                    \
  })

// Call common functions which use obj_store pointer as the first argument
// in background threads (OSS) or directly in the current thread (HDFS).
#define GET_BACKGROUND_OBJ_STORE_AND_PERFORM(__fs, __func, ...)             \
  ({                                                                        \
    auto __r = (__fs)->bypass_executor()                                    \
                   ? __func((__fs)->get_obj_store_direct(), __VA_ARGS__)    \
                   : [&]() {                                                \
                       auto __bg_env_ctx =                                  \
                           (__fs)                                           \
                               ->bg_vcpu_env()                              \
                               .bg_obj_store_env->get_obj_store_env_next(); \
                       return __bg_env_ctx.executor->perform([&]() {        \
                         IObjStore *__c = __bg_env_ctx.obj_store;           \
                         return __func(__c, __VA_ARGS__);                   \
                       });                                                  \
                     }();                                                   \
    __r;                                                                    \
  })

#define AUTO_USLEEP(us)                                           \
  do {                                                            \
    if (photon::CURRENT) {                                        \
      photon::thread_usleep(us);                                  \
    } else {                                                      \
      std::this_thread::sleep_for(std::chrono::microseconds(us)); \
    }                                                             \
  } while (0)

namespace OssFileSystem {

enum class CacheType : uint8_t {
  // kFhCache mode binds memory cache data to file handles.
  kFhCache = 0,
  kDiskCache = 1,
};

// How writes are persisted to OSS. The three modes are mutually exclusive and
// derived from configuration (temp_dir / enable_appendable_object).
enum class WriteMode : uint8_t {
  // Streaming: buffer and upload the whole object (MultipartUpload/PutObject).
  Streaming = 0,
  // Appendable: use AppendObject to append data incrementally.
  Appendable = 1,
  // Random: stage data on local disk (temp_dir) to support random writes.
  Random = 2,
};

struct OssFsOptions {
  static int apply_mem_limit(OssFsOptions *fs_options, uint64_t total_mem_limit,
                             double rw_ratio);

  static int validate_random_write(const OssFsOptions &opts);

  CacheType cache_type = CacheType::kFhCache;
  IObjStore::StorageBackend storage_backend = IObjStore::StorageBackend::kOSS;

  bool share_fd_read_buffer = false;
  uint64_t cache_refill_unit = 1024 * 1024;
  uint64_t cache_block_size = 1048576;
  uint64_t memory_data_cache_size = 0;

  uint64_t attr_timeout = 60;
  bool close_to_open = false;
  bool allow_mark_dir_stale_recursively = false;

  int32_t prefetch_chunks = 0;
  uint64_t prefetch_chunk_size = 8 * 1024 * 1024;
  uint32_t prefetch_concurrency = 256;
  uint32_t prefetch_concurrency_per_file = 64;
  uint64_t min_reserved_buffer_size_per_file = 1048576;
  uint64_t max_total_reserved_buffer_count = 256;
  uint64_t upload_buffer_size = 8 * 1024 * 1024;
  uint32_t upload_concurrency = 64;
  uint32_t upload_copy_concurrency = 64;
  int seq_read_detect_count = 3;

  bool enable_appendable_object = false;
  uint64_t appendable_object_autoswitch_threshold = 128 * 1024 * 1024;

  // ── Random write (disk-staged) ──
  std::string temp_dir;
  uint64_t random_write_chunk_size = 2 * 1024 * 1024;
  uint64_t random_write_max_file_size = 100ULL * 1024 * 1024 * 1024;  // 100 GiB
  uint64_t temp_dir_free_bytes = 1ULL * 1024 * 1024 * 1024;           // 1 GiB

  uint32_t readdir_remember_count = 100;
  int64_t kernel_readdir_cache_timeout = 0;

  bool allow_rename_dir = true;
  uint64_t rename_dir_limit = 2000000;
  int32_t rename_dir_concurrency = 128;
  bool set_mime_for_rename_dst = false;

  bool enable_crc64 = true;
  bool enable_transmission_control = false;
  uint64_t tc_max_latency_threshold_us = 2000000;

  gid_t gid = 0;
  uid_t uid = 0;
  mode_t dir_mode = 0755;
  mode_t file_mode = 0644;
  bool readonly = false;

  uint64_t inode_cache_eviction_threshold = 0;
  uint64_t inode_cache_eviction_interval_ms = 0;
  uint64_t max_inode_cache_count = 0;

  uint64_t oss_negative_cache_timeout = 0;
  uint64_t oss_negative_cache_size = 0;

  std::string ram_role;
  std::string credential_process;
  uint64_t credential_refresh_interval =
      0;  // 0 means use default expiration-based strategy.

  bool enable_admin_server = true;
  bool enable_symlink = false;
  bool enable_xattr = true;
  // Explicitly set_owner on HDFS after create/mkdir/mknod. Default off.
  bool hdfs_set_owner_on_create = false;

  // Normalized Mount point path(e.g., "/mnt/ossfs"), used to convert absolute
  // symlink targets to mount-internal relative paths. It could be empty when
  // mount with fuse device fd.
  std::string mountpoint;

  uint64_t mempool_purge_interval_ms = 5000;
};

struct LockQueueElement;
class OssFs : public IFileSystemFuseLL {
 public:
  OssFs(const OssFsOptions &options, BackgroundVCpuEnv bg_vcpu_env,
        std::unique_ptr<IIdManager> id_manager = create_heap_id_manager());
  ~OssFs();

  int init();
  int lookup(uint64_t parent, std::string_view name, uint64_t *nodeid,
             struct stat *stbuf) override;
  int forget(uint64_t nodeid, uint64_t nlookup) override;
  int getattr(uint64_t nodeid, struct stat *stbuf) override;
  int setattr(uint64_t nodeid, struct stat *stbuf, int to_set,
              struct fuse_file_info *fi = nullptr, uid_t caller_uid = 0,
              gid_t caller_gid = 0) override;
  int statfs(struct statvfs *stbuf) override;
  int rename(uint64_t old_parent, std::string_view old_name,
             uint64_t new_parent, std::string_view new_name,
             unsigned int flags) override;
  int unlink(uint64_t parent, std::string_view name, uid_t caller_uid = 0,
             gid_t caller_gid = 0) override;

  int open(uint64_t nodeid, int flags, void **fh,
           bool *keep_page_cache) override;
  int creat(uint64_t parent, std::string_view name, int flags, mode_t mode,
            uid_t uid, gid_t gid, mode_t umask, uint64_t *nodeid,
            struct stat *stbuf, void **fh) override;
  ssize_t read(uint64_t nodeid, void *fh, size_t size, off_t off,
               std::function<void(void *buf, size_t size)> read_cb) override;
  ssize_t write(uint64_t nodeid, void *fh, const char *buf, size_t size,
                off_t off) override;
  ssize_t write_buf(uint64_t nodeid, void *fh, struct fuse_bufvec *bufv,
                    off_t off) override;
  int fsync(uint64_t nodeid, void *fh, bool datasync) override;
  int flush(uint64_t nodeid, void *fh) override;
  int release(uint64_t nodeid, void *fh) override;

  int opendir(uint64_t nodeid, struct fuse_file_info *fi) override;
  int readdir(uint64_t nodeid, off_t off, void *dh,
              int (*filler)(void *ctx, uint64_t nodeid, const char *name,
                            const struct stat *stbuf, off_t off),
              void *filler_ctx, int (*is_interrupted)(void *ctx),
              bool readdirplus, void *interrupted_ctx) override;
  int releasedir(uint64_t nodeid, void *dh) override;

  int mkdir(uint64_t parent, std::string_view name, mode_t mode, uid_t uid,
            gid_t gid, mode_t umask, uint64_t *nodeid,
            struct stat *stbuf) override;
  int rmdir(uint64_t parent, std::string_view name) override;

  int symlink(uint64_t parent, std::string_view name, std::string_view link,
              uid_t uid, gid_t gid, uint64_t *nodeid,
              struct stat *stbuf) override;
  ssize_t readlink(uint64_t nodeid, char *buf, size_t size) override;

  int fallocate(uint64_t nodeid, off_t offset, off_t length, void *fh) override;

  int mknod(uint64_t parent, std::string_view name, mode_t mode, uid_t uid,
            gid_t gid, uint64_t *nodeid, struct stat *stbuf) override;

  int flock(uint64_t nodeid, void *fh, int op, uint64_t lock_owner) override;

  int setxattr(uint64_t nodeid, const char *name, const char *value,
               size_t size, int flags) override;
  int getxattr(uint64_t nodeid, const char *name, char *value,
               size_t size) override;
  int listxattr(uint64_t nodeid, char *list, size_t size) override;
  int removexattr(uint64_t nodeid, const char *name) override;

  int get_one_list_results(std::string_view full_path,
                           std::vector<ObjDirent> &results,
                           std::string &marker);

  CacheType get_cache_type() {
    return options_.cache_type;
  }

  IObjStore::StorageBackend get_backend_type() const {
    return options_.storage_backend;
  }

  bool is_hdfs_mode() const {
    return get_backend_type() == IObjStore::StorageBackend::kHDFS;
  }

  bool bypass_executor() const {
    return is_hdfs_mode();
  }

  // Get objstore pointer for direct calls (HDFS bypasses executor).
  IObjStore *get_obj_store_direct() {
    return bg_vcpu_env_.bg_obj_store_env->obj_stores[0];
  }

  // Create file handle based on storage backend type
  IFileHandleFuseLL *create_file_handle(const std::string &path,
                                        FileInode *inode, int flags,
                                        mode_t mode = 0777);

  int access(uint64_t nodeid, int mask, uid_t caller_uid, gid_t caller_gid);

  // Check permission via store's policy. Returns 0 if permitted.
  int check_permission(PermOp op, Inode *inode, uid_t uid, gid_t gid);

  bool enable_prefetching() {
    return options_.prefetch_concurrency > 0;
  }

  static WriteMode compute_write_mode(const OssFsOptions &opts) {
    if (!opts.temp_dir.empty()) return WriteMode::Random;
    if (opts.enable_appendable_object) return WriteMode::Appendable;
    return WriteMode::Streaming;
  }

  WriteMode write_mode() const {
    return write_mode_;
  }

  std::shared_ptr<FixedBlockMemoryPool> get_download_buffers() {
    return download_buffers_;
  }

  const OssFsOptions &get_options() const {
    return options_;
  }

  // Base multipart part size for random-write flush.
  uint64_t random_write_base_part_size() const {
    return random_write_base_part_size_;
  }

  BackgroundVCpuEnv &bg_vcpu_env() {
    return bg_vcpu_env_;
  }

 private:
  void add_new_inode_to_global_map(Inode *inode) {
    global_inodes_map_[inode->nodeid] = inode;
  }

  void remove_inode_from_global_map(uint64_t nodeid) {
    global_inodes_map_.erase(nodeid);
  }

  void increment_inode_lookupcnt(Inode *inode, uint64_t parent_nodeid,
                                 std::string_view name);

  enum class InodeRefPathType : uint8_t {
    kPathTypeNone = 0,
    kPathTypeRead = 1,
    kPathTypeWrite = 2,
  };
  const InodeRef get_inode_ref(uint64_t nodeid, InodeRefPathType path_type);
  void return_inode_ref(const InodeRef &ref);

  // parent path wlock and inode path rlock if it presents
  const ParentRef get_inode_ref(uint64_t parent, std::string_view name);
  void return_inode_ref(const ParentRef &ref);

  const ParentRef2 get_inode_ref(uint64_t parent1, std::string_view name1,
                                 uint64_t parent2, std::string_view name2);
  void return_inode_ref(const ParentRef2 &ref);

  // lookup related
  int lookup_try_local_attr_cache(DirInode *parent_inode, std::string_view name,
                                  const std::string &full_path,
                                  struct stat *stbuf,
                                  struct Attribute *old_attr = nullptr,
                                  std::string *old_etag = nullptr);
  int lookup_get_remote_attr(DirInode *parent_inode, std::string_view name,
                             const std::string &full_path, struct stat *stbuf,
                             std::string *remote_etag, time_t *attr_time);
  int lookup_update_local_cache(DirInode *parent_inode, std::string_view name,
                                bool acquire_write_path_lock,
                                Inode *wlocked_inode,
                                const std::string &full_path,
                                const struct Attribute &old_attr,
                                const std::string &old_etag, int req_status,
                                struct stat *stbuf,
                                const std::string &remote_etag);
  void lookup_create_new_inode(DirInode *parent_inode, std::string_view name,
                               const std::string &etag,
                               const uint64_t allocated_nodeid,
                               struct stat *stbuf, time_t *attr_time);
  int lookup_with_inode_ref(DirInode *parent_inode, std::string_view name,
                            const std::string &full_path,
                            bool with_write_path_lock, Inode *wlocked_inode,
                            struct stat *stbuf);

  int create_internal(uint64_t parent, std::string_view name, int flags,
                      uint64_t *nodeid, struct stat *stbuf, void **fh,
                      InodeType type, std::string_view link, mode_t mode,
                      uid_t uid, gid_t gid);

  // readdir related
  void construct_inodes_if_needed(DirInode *parent_inode, OssDirHandle *dh);
  // Mark local file children of a dir stale after a from-start listing
  // confirmed the remote dir is empty; caller holds the parent's write lock.
  void mark_ghost_children_stale(DirInode *parent_inode);

  int remember_inode_if_needed_with_fill(
      DirInode *parent_inode, const char *name, off_t offset, OssDirHandle *dh,
      int (*filler)(void *ctx, uint64_t nodeid, const char *name,
                    const struct stat *stbuf, off_t off),
      void *filler_ctx);
  int get_dirty_children(DirInode *parent_inode, std::string_view full_path,
                         std::map<estring, ObjDirent> &dirty_children);
  int refresh_dir_plus(DirInode *parent_inode, OssDirHandle *odh);
  int refresh_dir(DirInode *parent_inode, OssDirHandle *odh);
  int seek_dir_plus(DirInode *parent_inode, OssDirHandle *odh,
                    off_t target_offset, int (*is_interrupted)(void *ctx),
                    void *interrupted_ctx);
  int seek_dir(DirInode *parent_inode, OssDirHandle *odh, off_t target_offset,
               int (*is_interrupted)(void *ctx), void *interrupted_ctx);
  int readdir_fill_plus(DirInode *parent_inode, OssDirHandle *odh,
                        int (*filler)(void *ctx, uint64_t nodeid,
                                      const char *name,
                                      const struct stat *stbuf, off_t off),
                        void *filler_ctx);
  int readdir_fill(DirInode *parent_inode, OssDirHandle *odh,
                   int (*filler)(void *ctx, uint64_t nodeid, const char *name,
                                 const struct stat *stbuf, off_t off),
                   void *filler_ctx);

  void try_update_inode_attr_from_list(Inode *inode, struct stat *stbuf,
                                       std::string_view remote_etag,
                                       mode_t perm = 0, uid_t uid = 0,
                                       gid_t gid = 0);

  std::shared_ptr<ICache> create_inode_cache();
  void evict_inode_cache(FileInode *inode);

  int try_invalidate_inode(uint64_t nodeid, uint64_t nlookup, bool recursive);

  int truncate_inode_data(Inode *inode, std::string_view full_path,
                          size_t to_size);

  int random_write_truncate(FileInode *inode, std::string_view full_path,
                            uint64_t new_size);

  // Requires the inode's wlock.
  void resync_randwrite_remote_size(Inode *inode);

  Inode *create_new_inode(uint64_t file_nodeid, std::string_view file_name,
                          uint64_t file_size, struct timespec file_mtime,
                          InodeType type, bool is_dirty, uint64_t parent_nodeid,
                          Inode *parent_node, std::string_view remote_etag,
                          mode_t perm = 0, uid_t uid = 0, gid_t gid = 0);

  // Staged cache related
  bool enable_staged_cache() const {
    return options_.max_inode_cache_count > 0 && options_.attr_timeout > 0;
  }

  bool enabled_negative_cache() const {
    return options_.oss_negative_cache_timeout > 0 &&
           options_.oss_negative_cache_size > 0;
  }

  int forget_and_insert_to_staged_cache(uint64_t nodeid, uint64_t nlookup);
  void rm_from_staged_cache_if_needed(uint64_t parent, std::string_view name);
  bool lookup_from_staged_cache_if_enabled(uint64_t parent_nodeid,
                                           std::string_view name,
                                           struct stat *stbuf = nullptr,
                                           std::string *remote_etag = nullptr,
                                           time_t *attr_time = nullptr);
  bool exists_in_staged_cache(uint64_t parent_nodeid, std::string_view name);

  uint64_t get_inode_eviction_threshold() {
    return std::max(options_.max_inode_cache_count,
                    options_.inode_cache_eviction_threshold);
  }

  void init_prefetch_options();

  bool mark_dir_stale_recursively(DirInode *dir_inode, uint64_t &walked_cnt,
                                  uint64_t &marked_cnt);
  void mark_inode_stale_if_needed(Inode *inode, bool recursively);

  int flush_dirty_inodes_for_rename(Inode *src_node, std::string_view src_path);
  // Finish the rename after the caller holds the unique inode_lock of both
  // parents and the unique inode_lock of src_node.
  int do_rename_locked(DirInode *o_parent, DirInode *n_parent, Inode *src_node,
                       std::string_view old_name, std::string_view new_name,
                       uint64_t new_parent, std::string_view src_parent_path,
                       std::string_view dst_parent_path, unsigned int flags);
  // Caller must hold the unique inode_lock of the parent dir and src_node.
  int hide_inode(DirInode *parent, Inode *src_node,
                 std::string_view parent_path);
  // Delete the object of a hidden inode whose last handle was closed, under
  // the parent's and the inode's write locks, then mark the inode stale.
  void delete_hidden_inode(FileInode *inode, std::string_view inode_path);
  int rename_file(std::string_view oldpath, std::string_view newpath,
                  bool dst_exists);
  int rename_dir(std::string_view oldpath, std::string_view newpath,
                 bool dst_exists);
  int do_rename_dir(std::string_view oldpath, std::string_view newpath,
                    bool dst_exists);
  int rename_dir_copy_delete(std::string_view oldpath,
                             std::string_view newpath);
  static void *do_rename_task(void *arg);

  uint64_t transmission_control();
  void update_max_refill_range_lat(uint64_t latency_us);

  std::vector<uint64_t> get_dirty_nodeids() {
    std::lock_guard<std::mutex> l(dirty_nodeids_lock_);
    return {dirty_nodeids_.begin(), dirty_nodeids_.end()};
  }

  void add_dirty_nodeid(uint64_t nodeid) {
    std::lock_guard<std::mutex> l(dirty_nodeids_lock_);
    dirty_nodeids_.insert(nodeid);
  }

  void erase_dirty_nodeid(uint64_t nodeid) {
    std::lock_guard<std::mutex> l(dirty_nodeids_lock_);
    dirty_nodeids_.erase(nodeid);
  }

  void invalidate_data_cache_if_needed(Inode *inode, const struct stat *stbuf,
                                       std::string_view remote_etag) {
    if (!inode->is_file()) return;
    static_cast<FileInode *>(inode)->invalidate_data_cache_if_needed(
        stbuf, remote_etag);
  }

  void update_inode_etag(Inode *inode, std::string_view remote_etag) {
    if (!inode->is_file()) return;
    static_cast<FileInode *>(inode)->etag.assign(remote_etag.data(),
                                                 remote_etag.size());
  }

  std::string_view get_inode_etag(Inode *inode) {
    if (!inode->is_file()) return {};
    return static_cast<FileInode *>(inode)->etag;
  }

  void run_health_check();
  void run_reverse_invalidate();
  void start_uds_server(std::promise<bool> &fut);
  std::string process_uds_request(std::string_view action,
                                  std::string_view param);

  int for_each_evictable_inodes(uint64_t threshold,
                                std::function<int(const DentryView &)> cb);
  int reverse_invalidate_kernel_entry(const DentryView &dentry);

  void start_creds_refresher(std::promise<int> &result_promise);
  uint64_t refresh_creds();
  std::pair<int, uint64_t> do_refresh_creds(bool allow_auto_create = false);
  void update_creds(const ObjCredentials &creds);
  int validate_creds(const ObjCredentials &creds,
                     bool allow_auto_create = false);

  void init_hdfs_root_inode();

  // HDFS setattr family.
  int hdfs_setattr(uint64_t nodeid, struct stat *stbuf, int to_set,
                   struct fuse_file_info *fi, uid_t caller_uid,
                   gid_t caller_gid);
  int do_hdfs_setattr_mode(uint64_t nodeid, std::string_view path, mode_t mode,
                           Inode *inode, uid_t caller_uid, gid_t caller_gid);
  int do_hdfs_setattr_uid_gid(uint64_t nodeid, std::string_view path,
                              const struct stat *stbuf, int to_set,
                              Inode *inode, uid_t caller_uid, gid_t caller_gid);
  int do_hdfs_setattr_times(Inode *inode, std::string_view path,
                            const struct stat *stbuf, int to_set,
                            uid_t caller_uid, gid_t caller_gid);
  int do_hdfs_setattr_size(uint64_t nodeid, Inode *inode,
                           std::string_view full_path, off_t target_size,
                           uid_t caller_uid, gid_t caller_gid);
  int do_hdfs_ftruncate(uint64_t nodeid, Inode *inode, off_t target_size,
                        struct fuse_file_info *fi, uid_t caller_uid,
                        gid_t caller_gid);

  void staging_disk_usage_update(int64_t old_bytes, int64_t new_bytes) {
    if (old_bytes < 0 || new_bytes < 0 || old_bytes == new_bytes) return;
    if (new_bytes > old_bytes) {
      staging_disk_usage_.fetch_add(new_bytes - old_bytes,
                                    std::memory_order_relaxed);
    } else {
      // Clamp at 0 so the subtraction can never underflow the unsigned
      // counter.
      uint64_t delta = static_cast<uint64_t>(old_bytes - new_bytes);
      uint64_t cur = staging_disk_usage_.load(std::memory_order_relaxed);
      while (!staging_disk_usage_.compare_exchange_weak(
          cur, cur > delta ? cur - delta : 0, std::memory_order_relaxed)) {
      }
    }
  }

  uint64_t staging_reserved_add(uint64_t bytes) {
    return staging_reserved_bytes_.fetch_add(bytes) + bytes;
  }

  void staging_reserved_sub(uint64_t bytes) {
    staging_reserved_bytes_.fetch_sub(bytes);
  }

  // Effective free bytes of the staging filesystem; a real fstatvfs runs at
  // most once per staging_avail_refresh_ns_ to refresh the cache, and a
  // concurrent caller hitting a stale cache runs its own fstatvfs without
  // publishing. Returns 0 or -errno.
  int staging_disk_avail(int staging_fd, uint64_t *avail_out);

  static inline constexpr uint64_t kMaxFsSize =
      std::numeric_limits<uint64_t>::max();  // 16 EB;
  static inline constexpr uint64_t kMaxFsInodes =
      std::numeric_limits<uint64_t>::max() / 1024;

  OssFsOptions options_;

  const WriteMode write_mode_;

  BackgroundVCpuEnv bg_vcpu_env_;

  Inode *mp_inode_ = nullptr;

  StagedInodeCache *staged_inodes_cache_ = nullptr;
  NegativeCache *negative_cache_ = nullptr;

  std::unique_ptr<IIdManager> id_manager_;

  // key: nodeid. Stores all inodes (active and stale)
  std::map<uint64_t, Inode *> global_inodes_map_;
  // lock the global map and inodes' ref_ctr
  std::mutex inodes_map_lck_;

  LockQueueElement *path_lockq_ = nullptr;

  std::unique_ptr<photon::semaphore> prefetch_sem_;

  // for one file handle
  size_t max_prefetch_size_per_handle_ = 0;
  size_t max_prefetch_window_size_per_handle_ = 0;

  std::atomic<bool> is_stopping_ = {false};

  // for uploading to oss
  std::unique_ptr<photon::semaphore> upload_sem_;
  std::unique_ptr<photon::semaphore> upload_copy_sem_;

  // for renaming directory
  std::unique_ptr<photon::semaphore> rename_sem_;

  // Global seq for generating ".fuse_hiddenXXX" names in hide_inode.
  std::atomic<uint32_t> hidden_inode_seq_ = ATOMIC_VAR_INIT(0);

  std::unique_ptr<FixedBlockMemoryPool> upload_buffers_;
  std::shared_ptr<FixedBlockMemoryPool> download_buffers_;

  // Derived from upload_buffer_size at construction, aligned to chunk_size.
  uint64_t random_write_base_part_size_ = 0;

  // tracking all the dirty inodes
  std::unordered_set<uint64_t> dirty_nodeids_;
  std::mutex dirty_nodeids_lock_;

  std::thread *transmission_control_th_ = nullptr;
  photon::spinlock tc_lock_;
  uint64_t max_refill_range_lat_us_ = 0;

  std::thread *health_check_th_ = nullptr;
  std::thread *reverse_invalidate_th_ = nullptr;
  std::thread *uds_server_th_ = nullptr;

  CredentialsProvider *creds_provider_ = nullptr;
  std::thread *creds_refresh_th_ = nullptr;

  std::atomic<uint64_t> total_create_cnt_ = ATOMIC_VAR_INIT(0);
  std::atomic<uint64_t> active_file_handles_ = ATOMIC_VAR_INIT(0);

  // Random-write staging disk usage, for health-check log and the avail-cache
  // compensation in staging_disk_avail; memory_order_relaxed is OK.
  std::atomic<uint64_t> staging_disk_usage_ = ATOMIC_VAR_INIT(0);

  // Used for the random-write mode disk space check.
  std::atomic<uint64_t> staging_reserved_bytes_ = ATOMIC_VAR_INIT(0);

  // fstatvfs throttle interval; non-const so unit tests can override it.
  uint64_t staging_avail_refresh_ns_ = 100 * 1000 * 1000;  // 100ms

  // Cached disk-avail snapshot for the staging (temp_dir) filesystem.
  photon::spinlock staging_avail_lock_;
  uint64_t staging_avail_ts_ns_ = 0;  // monotonic time of last refresh
  uint64_t staging_avail_bytes_ =
      std::numeric_limits<uint64_t>::max();  // f_bavail*f_frsize at refresh
  uint64_t staging_avail_usage_snap_ = 0;    // staging_disk_usage_ at refresh
  std::atomic<bool> staging_avail_refreshing_ = ATOMIC_VAR_INIT(false);

  friend class OssWriter;
  friend class OssSeqWriter;
  friend class OssStreamingWriter;
  friend class OssAppendableWriter;
  friend class OssRandomWriter;
  friend class OssCachedReader;
  friend class OssDirectReader;
  friend class OssFileHandle;
  friend class HdfsFileHandle;

  template <typename T>
  friend class EnableFilePrefetching;

  DECLARE_TEST_FRIENDS_CLASSES;
};

}  // namespace OssFileSystem
