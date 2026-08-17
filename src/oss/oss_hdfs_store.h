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

#include <fcntl.h>

#include <string>
#include <unordered_map>

#include "jdo_common.h"
#include "obj_store.h"

namespace OssFileSystem {

// Map JindoSDK error codes to POSIX errno values.
// Defined in oss_hdfs_store.cpp, exposed for unit testing.
int jdo_error_code_to_posix(int jdo_error_code);

class OssHdfsStore : public IObjStore {
 public:
  OssHdfsStore(const char *key, const char *key_secret,
               const ObjStoreOptions &options);
  ~OssHdfsStore();

  StorageBackend get_backend_type() const override {
    return StorageBackend::kHDFS;
  }

  void set_credentials(ObjCredentials &&creds) override;

  const ObjStoreOptions &get_options() const override {
    return opts_;
  }

  int head_object(std::string_view path, ObjHeaderMeta &meta) override {
    return -ENOTSUP;
  }

  int open_object(std::string_view path, int flags, mode_t mode,
                  RawObjHandle **out_handle) override;

  ssize_t get_object_range(std::string_view path, const struct iovec *iov,
                           int iovcnt, off_t offset,
                           std::string * /*response_etag*/ = nullptr) override {
    return -ENOTSUP;
  }

  ssize_t put_object(std::string_view path, const struct iovec *iov, int iovcnt,
                     uint64_t *expected_crc64 = nullptr,
                     mode_t mode = 0755) override;

  ssize_t append_object(std::string_view path, const struct iovec *iov,
                        int iovcnt, off_t position,
                        uint64_t *expected_crc64 = nullptr) override {
    return -ENOTSUP;
  }

  int copy_object(std::string_view src_path, std::string_view dst_path,
                  bool overwrite = false, bool set_mime = false) override {
    return -ENOTSUP;
  }

  int rename_object(std::string_view src_path, std::string_view dst_path,
                    bool set_mime = false, bool dst_exists = false) override;

  int rename_dir(std::string_view src_path, std::string_view dst_path,
                 bool dst_exists = false) override;

  int delete_object(std::string_view path) override;

  int stat(std::string_view path, struct stat *buf, std::string *etag) override;

  int list_dir(std::string_view path, ObjectList &results,
               std::string *context = nullptr) override;

  int check_bucket(bool allow_auto_create = true) override;

  int is_dir_empty(std::string_view path, bool &is_empty) override;

  int get_symlink(std::string_view path, std::string &target) override;

  ssize_t put_symlink(std::string_view path, std::string_view target) override;

  int init_multipart_upload(std::string_view path, void **context) override {
    return -ENOTSUP;
  }

  ssize_t upload_part(void *context, const struct iovec *iov, int iovcnt,
                      int part_number,
                      uint64_t *expected_crc64 = nullptr) override {
    return -ENOTSUP;
  }

  int upload_part_copy(void *context, off_t offset, size_t count,
                       int part_number,
                       uint64_t *crc64_out = nullptr) override {
    return -ENOTSUP;
  }

  int complete_multipart_upload(void *context, uint64_t *expected_crc64,
                                std::string *etag = nullptr) override {
    return -ENOTSUP;
  }

  int abort_multipart_upload(void *context) override {
    return -ENOTSUP;
  }

  int delete_objects_under_dir(
      std::string_view path,
      const std::vector<std::string_view> &objects) override {
    return -ENOTSUP;
  }

  int list_dir_descendants(std::string_view path,
                           std::vector<std::string> &results,
                           std::function<bool()> checker = nullptr,
                           bool *is_dirobj = nullptr) override {
    return -ENOTSUP;
  }

  int truncate_object(std::string_view path, size_t to_size) override;

  int set_permission(std::string_view path, mode_t mode) override;
  int set_owner(std::string_view path, uid_t uid, gid_t gid,
                int to_set) override;

  int set_lock(std::string_view path, int64_t offset, int64_t length,
               int16_t type, int64_t pid, uint64_t owner) override;
  int get_lock(std::string_view path, int64_t &offset, int64_t &length,
               int16_t &type, int64_t &pid, uint64_t owner) override;

  int check_permission(PermOp op, const struct stat *file_stat,
                       uid_t caller_uid, gid_t caller_gid) override;

  int set_times(std::string_view path, int64_t mtime_ms,
                int64_t atime_ms) override;

  int set_xattr(std::string_view path, const char *name, const char *value,
                size_t size, int flags) override;
  int get_xattr(std::string_view path, const char *name, char *value,
                size_t size) override;
  int list_xattr(std::string_view path, char *list, size_t size) override;
  int remove_xattr(std::string_view path, const char *name) override;

 private:
  // Helper methods for put_object.
  int do_mkdir(std::string_view path, mode_t mode);
  int do_create_file(std::string_view path, mode_t mode);

  class OssHdfsRawObjHandle : public RawObjHandle {
   public:
    OssHdfsRawObjHandle(JdoStore_t store, JdoIOContext_t io_ctx,
                        std::string hdfs_path);
    ~OssHdfsRawObjHandle() override;

    ssize_t read(void *buffer, size_t length) override;
    ssize_t pread(void *buffer, size_t length, off_t offset) override;
    ssize_t write(const void *buffer, size_t length) override;
    int flush() override;
    int close() override;
    ssize_t get_length() override;
    ssize_t tell() override;
    ssize_t seek(off_t offset) override;
    int fallocate(off_t offset, off_t length) override;

   private:
    JdoStore_t store_;
    JdoIOContext_t io_ctx_ = nullptr;
    std::string path_;
    bool closed_ = false;
  };

  int init_jindosdk();
  void rebuild_store(const char *key, const char *key_secret);
  void apply_sdk_options();
  void set_sdk_option(const std::string &key, const std::string &val);
  void load_sdk_config_file(const std::string &path);

  int do_list_dir(std::string_view path, ObjectList &results, bool recursive,
                  int max_keys = 0, std::string *marker = nullptr);

  ObjStoreOptions opts_;
  JdoOptions_t jdo_opts_ = nullptr;
  JdoStore_t jdo_store_ = nullptr;
  std::string uri_;
  bool sdk_initialized_ = false;
};

IObjStore *new_oss_hdfs_store(const char *key, const char *key_secret,
                              const ObjStoreOptions &options);

};  // namespace OssFileSystem