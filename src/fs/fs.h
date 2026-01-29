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
#include "inode.h"
#include "mem_pool.h"
#include "negative_cache.h"
#include "staged_inode_cache.h"
#include "test/class_declarations.h"

// Call photon OSS SDK's methods in background threads.
#define DO_SYNC_BACKGROUND_OSS_REQUEST(__fs, __func, ...)                \
  ({                                                                     \
    auto __bg_env_ctx =                                                  \
        __fs->bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next(); \
    auto __r = __bg_env_ctx.executor->perform([&]() {                    \
      OssAdapter *__c = __bg_env_ctx.oss_client;                         \
      return __c->__func(__VA_ARGS__);                                   \
    });                                                                  \
    __r;                                                                 \
  })

// Call common functions which use oss_client pointer as the first argument
// in background threads.
#define GET_BACKGROUND_OSS_CLIENT_AND_DO_SYNC_FUNC(__fs, __func, ...)    \
  ({                                                                     \
    auto __bg_env_ctx =                                                  \
        __fs->bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next(); \
    auto __r = __bg_env_ctx.executor->perform([&]() {                    \
      OssAdapter *__c = __bg_env_ctx.oss_client;                         \
      return __func(__c, __VA_ARGS__);                                   \
    });                                                                  \
    __r;                                                                 \
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
};

struct OssFsOptions {
  static int apply_mem_limit(OssFsOptions *fs_options, uint64_t total_mem_limit,
                             double rw_ratio);

  std::string mountpath;

  CacheType cache_type = CacheType::kFhCache;
  bool bind_cache_to_inode = false;
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

  uint32_t readdir_remember_count = 100;

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

  bool enable_admin_server = true;
  bool enable_symlink = false;
};

struct LockQueueElement;
class OssFs : public IFileSystemFuseLL {
 public:
  OssFs(const OssFsOptions &options, BackgroundVCpuEnv bg_vcpu_env);
  ~OssFs();

  int init();
  int lookup(uint64_t parent, std::string_view name, uint64_t *nodeid,
             struct stat *stbuf) override;
  int forget(uint64_t nodeid, uint64_t nlookup) override;
  int getattr(uint64_t nodeid, struct stat *stbuf) override;
  int setattr(uint64_t nodeid, struct stat *stbuf, int to_set) override;
  int statfs(struct statvfs *stbuf) override;
  int rename(uint64_t old_parent, std::string_view old_name,
             uint64_t new_parent, std::string_view new_name,
             unsigned int flags) override;
  int unlink(uint64_t parent, std::string_view name) override;

  int open(uint64_t nodeid, int flags, void **fh,
           bool *keep_page_cache) override;
  int creat(uint64_t parent, std::string_view name, int flags, mode_t mode,
            uid_t uid, gid_t gid, mode_t umask, uint64_t *nodeid,
            struct stat *stbuf, void **fh) override;
  int release(uint64_t nodeid, void *fh) override;

  int opendir(uint64_t nodeid, void **dh) override;
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

  int get_one_list_results(std::string_view full_path,
                           std::vector<OssDirent> &results,
                           std::string &marker);

  CacheType get_cache_type() {
    return options_.cache_type;
  }

  bool enable_prefetching() {
    return options_.prefetch_concurrency > 0;
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
                      InodeType type, std::string_view link);

  // readdir related
  void construct_inodes_if_needed(DirInode *parent_inode, OssDirHandle *dh);

  int remember_inode_if_needed_with_fill(
      DirInode *parent_inode, const char *name, off_t offset, OssDirHandle *dh,
      int (*filler)(void *ctx, uint64_t nodeid, const char *name,
                    const struct stat *stbuf, off_t off),
      void *filler_ctx);
  int get_dirty_children(DirInode *parent_inode, std::string_view full_path,
                         std::map<estring, OssDirent> &dirty_children);
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
                                       std::string_view remote_etag);

  void evict_inode_cache(FileInode *inode);

  int try_invalidate_inode(uint64_t nodeid, uint64_t nlookup, bool recursive);

  int truncate_inode_data(Inode *inode, std::string_view full_path,
                          size_t to_size);

  Inode *create_new_inode(uint64_t file_nodeid, std::string_view file_name,
                          uint64_t file_size, struct timespec file_mtime,
                          InodeType type, bool is_dirty, uint64_t parent_nodeid,
                          Inode *parent_node, std::string_view remote_etag);

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

  int rename_file(std::string_view oldpath, std::string_view newpath);
  int rename_dir(std::string_view oldpath, std::string_view newpath);
  static void *do_rename_task(void *arg);

  uint64_t transmission_control();
  void update_max_oss_rw_lat(uint64_t latency_us);

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
  std::pair<int, uint64_t> do_refresh_creds();
  void update_creds(const OssCredentials &creds);
  int validate_creds(const OssCredentials &creds);

  static inline constexpr uint64_t kMaxFsSize =
      std::numeric_limits<uint64_t>::max();  // 16 EB;
  static inline constexpr uint64_t kMaxFsInodes =
      std::numeric_limits<uint64_t>::max() / 1024;

  OssFsOptions options_;

  BackgroundVCpuEnv bg_vcpu_env_;

  Inode *mp_inode_ = nullptr;

  StagedInodeCache *staged_inodes_cache_ = nullptr;
  NegativeCache *negative_cache_ = nullptr;

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

  FixedBlockMemoryPool *upload_buffers_ = nullptr;
  FixedBlockMemoryPool *download_buffers_ = nullptr;

  // tracking all the dirty inodes
  std::unordered_set<uint64_t> dirty_nodeids_;
  std::mutex dirty_nodeids_lock_;

  std::thread *transmission_control_th_ = nullptr;
  photon::spinlock tc_lock_;
  uint64_t max_oss_rw_lat_us_ = 0;

  std::thread *health_check_th_ = nullptr;
  std::thread *reverse_invalidate_th_ = nullptr;
  std::thread *uds_server_th_ = nullptr;

  CredentialsProvider *creds_provider_ = nullptr;
  std::thread *creds_refresh_th_ = nullptr;

  std::atomic<uint64_t> total_create_cnt_ = ATOMIC_VAR_INIT(0);
  std::atomic<uint64_t> active_file_handles_ = ATOMIC_VAR_INIT(0);

  friend class OssWriter;
  friend class OssCachedReader;
  friend class OssDirectReader;
  friend class OssFileHandle;

  template <typename T>
  friend class EnableFilePrefetching;

  DECLARE_TEST_FRIENDS_CLASSES;
};

}  // namespace OssFileSystem
