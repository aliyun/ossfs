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

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <functional>
#include <string>
#include <vector>

#include "common/filesystem.h"
#include "common/utils.h"
#include "oss_patched.h"

namespace OssFileSystem {

static constexpr int kOssfsMaxFileNameLength = 255;
static constexpr int kOssMaxObjectKeyLength = 1023;

static constexpr std::string_view kOssObjectTypeNormal = "Normal";
static constexpr std::string_view kOssObjectTypeMultipart = "Multipart";
static constexpr std::string_view kOssObjectTypeAppendable = "Appendable";
static constexpr std::string_view kOssObjectTypeSymlink = "Symlink";

using ObjCredentials = photon::objstore_from_photon::CredentialParameters;
using ObjHeaderMeta = photon::objstore_from_photon::ObjectHeaderMeta;

// JindoSDK option keys.
constexpr const char kHdfsSdkOptEndpoint[] = "fs.oss.endpoint";
constexpr const char kHdfsSdkOptAccessKeyId[] = "fs.oss.accessKeyId";
constexpr const char kHdfsSdkOptAccessKeySecret[] = "fs.oss.accessKeySecret";
constexpr const char kHdfsSdkOptRandomWriteSyncInterval[] =
    "fs.oss.randomwrite.sync.interval.millisecond";
constexpr const char kHdfsSdkOptLoggerAppender[] = "logger.appender";
constexpr const char kHdfsSdkOptLoggerDir[] = "logger.dir";

struct ObjDirent {
  ObjDirent(std::string_view name, uint64_t size, struct timespec mtime,
            unsigned char type, std::string_view etag, mode_t mode = 0,
            uid_t uid = 0, gid_t gid = 0, struct timespec atime = {})
      : size_(size),
        mtime_(mtime),
        atime_(atime),
        type_(type),
        etag_(etag),
        mode_(mode),
        uid_(uid),
        gid_(gid) {
    auto len = std::min(name.size(), sizeof(name_) - 1);
    memcpy(name_, name.data(), len);
    name_[len] = '\0';
    name_size_ = len;
  }

  bool is_dir() const {
    return type_ == DT_DIR;
  }

  std::string_view name() const {
    return {name_, name_size_};
  }

  const char *name_cstr() const {
    return name_;
  }

  uint64_t size() const {
    return size_;
  }

  struct timespec mtime() const {
    return mtime_;
  }

  struct timespec atime() const {
    return atime_;
  }

  unsigned char type() const {
    return type_;
  }

  std::string_view etag() const {
    return etag_;
  }

  mode_t mode() const {
    return mode_;
  }

  uid_t uid() const {
    return uid_;
  }

  gid_t gid() const {
    return gid_;
  }

 private:
  uint64_t size_ = 0;
  struct timespec mtime_ = {};
  struct timespec atime_ = {};
  unsigned char type_ = 0;  // same as d_type in dirent
  std::string etag_;
  char name_[255 + 1] = {};  // kMaxFileNameLength
  uint16_t name_size_ = 0;

  // Permission fields (only populated by HDFS backend)
  mode_t mode_ = 0;
  uid_t uid_ = 0;
  gid_t gid_ = 0;
};

using ObjectList = std::vector<ObjDirent>;

// Lock type constants (aligned with JDO_LOCK_TYPE_*).
enum class LockType : int16_t {
  RdLock = 0,
  WrLock = 1,
  UnLock = 2,
};

// RawObjHandle: Low-level stream interface for object store IO operations.
// Provides sequential and random access read/write capabilities.
// Caller is responsible for managing the lifecycle (delete when done).
class RawObjHandle {
 public:
  virtual ~RawObjHandle() = default;

  // Sequential read from current position.
  virtual ssize_t read(void *buffer, size_t length) = 0;

  // Random read from specified offset.
  virtual ssize_t pread(void *buffer, size_t length, off_t offset) = 0;

  // Sequential write at current position.
  virtual ssize_t write(const void *buffer, size_t length) = 0;

  // Flush buffered data to storage.
  virtual int flush() = 0;

  // Close the stream and release resources.
  virtual int close() = 0;

  // Get file length.
  virtual ssize_t get_length() = 0;

  // Get current position.
  virtual ssize_t tell() = 0;

  // Seek to position.
  virtual ssize_t seek(off_t offset) = 0;

  // Allocate space for the file.
  virtual int fallocate(off_t offset, off_t length) = 0;
};

bool is_hdfs_endpoint(std::string_view endpoint);

struct ObjStoreOptions : public photon::objstore_from_photon::ClientOptions {
  std::string prefix;
  bool use_list_obj_v2 = true;
  bool use_auth_cache = false;
  bool enable_symlink = false;
  bool auto_create_bucket = false;
  std::string agentic_bucket;
  std::string hdfs_client_options;

  // Append a key=value to hdfs_client_options (comma-separated).
  // If overwrite is false (default) and the key already exists, skip.
  void append_hdfs_client_option(const std::string &key, const std::string &val,
                                 bool overwrite = false) {
    if (!overwrite) {
      auto prefix = key + "=";
      for (auto token : split_string(hdfs_client_options, ",")) {
        if (token.size() >= prefix.size() &&
            token.compare(0, prefix.size(), prefix) == 0)
          return;
      }
    }
    auto kv = key + "=" + val;
    hdfs_client_options =
        hdfs_client_options.empty() ? kv : hdfs_client_options + "," + kv;
  }

  virtual ~ObjStoreOptions() = default;
};

class IObjStore {
 public:
  enum class StorageBackend {
    kOSS,   // Standard OSS object store
    kHDFS,  // OSS-HDFS via Jindo SDK
  };

  virtual ~IObjStore() = default;

  // Get storage backend type
  virtual StorageBackend get_backend_type() const = 0;

  virtual void set_credentials(ObjCredentials &&creds) = 0;

  virtual const ObjStoreOptions &get_options() const = 0;

  virtual int head_object(std::string_view path, ObjHeaderMeta &meta) = 0;

  virtual ssize_t get_object_range(std::string_view path,
                                   const struct iovec *iov, int iovcnt,
                                   off_t offset,
                                   std::string *response_etag = nullptr) = 0;

  virtual ssize_t get_object_range_to_fd(std::string_view path, int fd,
                                         off_t fd_offset, off_t obj_offset,
                                         size_t count) {
    return -ENOSYS;
  }

  virtual ssize_t put_object(std::string_view path, const struct iovec *iov,
                             int iovcnt, uint64_t *expected_crc64 = nullptr,
                             mode_t mode = 0755) = 0;

  virtual ssize_t put_object_from_fd(std::string_view path, int fd,
                                     off_t offset, size_t count,
                                     uint64_t *expected_crc64 = nullptr,
                                     std::string *etag = nullptr) {
    return -ENOSYS;
  }

  // Open a raw object handle for read/write operations (HDFS stream IO).
  // Caller is responsible for deleting the returned handle.
  // Returns 0 on success (with *out_handle set), negative errno on failure.
  virtual int open_object(std::string_view path, int flags, mode_t mode,
                          RawObjHandle **out_handle) = 0;

  virtual int copy_object(std::string_view src_path, std::string_view dst_path,
                          bool overwrite = false, bool set_mime = false) = 0;

  virtual int rename_object(std::string_view src_path,
                            std::string_view dst_path, bool set_mime = false,
                            bool dst_exists = false) = 0;

  virtual int rename_dir(std::string_view src_path, std::string_view dst_path,
                         bool dst_exists = false) = 0;

  virtual int delete_object(std::string_view path) = 0;

  virtual int stat(std::string_view path, struct stat *buf,
                   std::string *etag) = 0;

  virtual int list_dir(std::string_view path, ObjectList &results,
                       std::string *context = nullptr) = 0;

  // allow_auto_create: when true (mount-time check), a missing bucket is
  // auto-created if auto_create_bucket is enabled. Periodic credential
  // revalidation passes false so it never (re)creates the bucket.
  virtual int check_bucket(bool allow_auto_create = true) = 0;

  // Delete the configured bucket. Mainly used by tests to clean up buckets
  // created via auto_create_bucket. Default: not implemented.
  virtual int delete_bucket() {
    return -ENOSYS;
  }

  virtual int is_dir_empty(std::string_view path, bool &is_empty) = 0;

  virtual int get_symlink(std::string_view path, std::string &target) = 0;
  virtual ssize_t put_symlink(std::string_view path,
                              std::string_view target) = 0;

  virtual ssize_t append_object(std::string_view path, const struct iovec *iov,
                                int iovcnt, off_t position,
                                uint64_t *expected_crc64 = nullptr) = 0;

  virtual int init_multipart_upload(std::string_view path, void **context) = 0;

  virtual ssize_t upload_part(void *context, const struct iovec *iov,
                              int iovcnt, int part_number,
                              uint64_t *expected_crc64 = nullptr) = 0;

  virtual ssize_t upload_part_from_fd(void *context, int fd, off_t offset,
                                      size_t count, int part_number,
                                      uint64_t *expected_crc64 = nullptr) {
    return -ENOSYS;
  }

  virtual int upload_part_copy(void *context, off_t offset, size_t count,
                               int part_number,
                               uint64_t *crc64_out = nullptr) = 0;

  virtual int complete_multipart_upload(void *context, uint64_t *expected_crc64,
                                        std::string *etag = nullptr) = 0;

  virtual int abort_multipart_upload(void *context) = 0;

  virtual int delete_objects_under_dir(
      std::string_view path, const std::vector<std::string_view> &objects) = 0;

  virtual int list_dir_descendants(std::string_view path,
                                   std::vector<std::string> &results,
                                   std::function<bool()> checker = nullptr,
                                   bool *is_dirobj = nullptr) = 0;

  // Truncate object to specified size.
  // Each backend implements its own strategy.
  virtual int truncate_object(std::string_view path, size_t to_size) = 0;

  // Change file permissions.
  // Returns 0 on success, negative error code on failure.
  virtual int set_permission(std::string_view path, mode_t mode) {
    return -ENOSYS;
  };

  // Change file owner and group.
  // to_set flags indicate which fields are explicitly requested:
  //   kSetUid - change uid (uid parameter is the requested value)
  //   kSetGid - change gid (gid parameter is the requested value)
  // When a flag is NOT set, the corresponding parameter is ignored
  // (caller means "don't change").
  // Returns 0 on success, -EINVAL if requested uid/gid cannot be resolved,
  // or other negative error code on failure.
  static constexpr int kSetUid = 1 << 0;
  static constexpr int kSetGid = 1 << 1;
  virtual int set_owner(std::string_view path, uid_t uid, gid_t gid,
                        int to_set) {
    return -ENOSYS;
  };

  // File lock operations.
  // Returns 0 on success, negative error code on failure.
  virtual int set_lock(std::string_view path, int64_t offset, int64_t length,
                       int16_t type, int64_t pid, uint64_t owner) {
    return -ENOSYS;
  };
  virtual int get_lock(std::string_view path, int64_t &offset, int64_t &length,
                       int16_t &type, int64_t &pid, uint64_t owner) {
    return -ENOSYS;
  };

  // Check if an operation is permitted for the given caller.
  // file_stat: the target file/dir's stat (uid/gid/mode).
  //            For dir ops (mkdir/create/etc), this is the *parent* dir's stat.
  // caller_uid/caller_gid: from fuse_context
  // Returns 0 if permitted, negative errno otherwise.
  // Default: return 0 (no-op, for backends that rely on VFS
  // default_permissions).
  virtual int check_permission(PermOp op, const struct stat *file_stat,
                               uid_t caller_uid, gid_t caller_gid) {
    return 0;
  }

  virtual int set_times(std::string_view path, int64_t mtime_ms,
                        int64_t atime_ms) {
    return -ENOSYS;
  }

  // Extended attributes.
  virtual int set_xattr(std::string_view path, const char *name,
                        const char *value, size_t size, int flags) {
    return -ENOSYS;
  };
  virtual int get_xattr(std::string_view path, const char *name, char *value,
                        size_t size) {
    return -ENOSYS;
  };
  virtual int list_xattr(std::string_view path, char *list, size_t size) {
    return -ENOSYS;
  };
  virtual int remove_xattr(std::string_view path, const char *name) {
    return -ENOSYS;
  };
};

IObjStore *new_obj_store(const char *key, const char *key_secret,
                         const ObjStoreOptions &options);

}  // namespace OssFileSystem
