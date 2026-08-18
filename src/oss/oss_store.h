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
#include <photon/thread/thread.h>
#include <sys/stat.h>

#include <functional>

#include "obj_store.h"
#include "oss_patched.h"

namespace OssFileSystem {

using namespace photon::objstore_from_photon;

class RawObjHandle;

//
// OssStore implements IObjStore for OSS object store backend.
// Translates filesystem paths to OSS object paths and manages credentials.
//
// fs path starting with '/', such as /a/b/c, /a/d, a/b/d/
// obj path with no '/' prepended, such as a/b/c, a/d, a/b/d/
//
class OssStore : public IObjStore {
 public:
  OssStore(const ObjStoreOptions &options, Authenticator *auth);

  ~OssStore() override = default;

  StorageBackend get_backend_type() const override {
    return StorageBackend::kOSS;
  }

  // IObjStore interface implementation
  void set_credentials(ObjCredentials &&creds) override;

  const ObjStoreOptions &get_options() const override {
    return opts_;
  }

  int head_object(std::string_view path, ObjHeaderMeta &meta) override;

  ssize_t get_object_range(std::string_view path, const struct iovec *iov,
                           int iovcnt, off_t offset,
                           std::string *response_etag = nullptr) override;

  ssize_t get_object_range_to_fd(std::string_view path, int fd, off_t fd_offset,
                                 off_t obj_offset, size_t count) override;

  ssize_t put_object(std::string_view path, const struct iovec *iov, int iovcnt,
                     uint64_t *expected_crc64 = nullptr,
                     mode_t mode = 0755) override;

  int open_object(std::string_view path, int flags, mode_t mode,
                  RawObjHandle **out_handle) override {
    return -ENOTSUP;
  }

  ssize_t put_object_from_fd(std::string_view path, int fd, off_t offset,
                             size_t count, uint64_t *expected_crc64 = nullptr,
                             std::string *etag = nullptr) override;

  ssize_t append_object(std::string_view path, const struct iovec *iov,
                        int iovcnt, off_t position,
                        uint64_t *expected_crc64 = nullptr) override;

  int copy_object(std::string_view src_path, std::string_view dst_path,
                  bool overwrite = false, bool set_mime = false) override;

  int rename_object(std::string_view src_path, std::string_view dst_path,
                    bool set_mime = false, bool dst_exists = false) override;

  int rename_dir(std::string_view src_path, std::string_view dst_path,
                 bool dst_exists = false) override;

  int delete_object(std::string_view path) override;

  int stat(std::string_view path, struct stat *buf, std::string *etag) override;

  int list_dir(std::string_view path, ObjectList &results,
               std::string *context = nullptr) override;

  int check_bucket(bool allow_auto_create = true) override;

  int delete_bucket() override;

  int is_dir_empty(std::string_view path, bool &is_empty) override;

  int init_multipart_upload(std::string_view path, void **context) override;

  ssize_t upload_part(void *context, const struct iovec *iov, int iovcnt,
                      int part_number,
                      uint64_t *expected_crc64 = nullptr) override;

  ssize_t upload_part_from_fd(void *context, int fd, off_t offset, size_t count,
                              int part_number,
                              uint64_t *expected_crc64 = nullptr) override;

  int upload_part_copy(void *context, off_t offset, size_t count,
                       int part_number, uint64_t *crc64_out = nullptr) override;

  int complete_multipart_upload(void *context, uint64_t *expected_crc64,
                                std::string *etag = nullptr) override;

  int abort_multipart_upload(void *context) override;

  int delete_objects_under_dir(
      std::string_view path,
      const std::vector<std::string_view> &objects) override;

  int list_dir_descendants(std::string_view path,
                           std::vector<std::string> &results,
                           std::function<bool()> checker = nullptr,
                           bool *is_dirobj = nullptr) override;

  int get_symlink(std::string_view path, std::string &target) override;
  ssize_t put_symlink(std::string_view path, std::string_view target) override;

  int truncate_object(std::string_view path, size_t to_size) override;

  int set_permission(std::string_view path, mode_t mode) override;
  int set_owner(std::string_view path, uid_t uid, gid_t gid,
                int to_set) override;

  int set_lock(std::string_view, int64_t, int64_t, int16_t, int64_t,
               uint64_t) override {
    return -ENOTSUP;
  }
  int get_lock(std::string_view, int64_t &, int64_t &, int16_t &, int64_t &,
               uint64_t) override {
    return -ENOTSUP;
  }

  int set_xattr(std::string_view, const char *, const char *, size_t,
                int) override {
    return -ENOTSUP;
  }
  int get_xattr(std::string_view, const char *, char *, size_t) override {
    return -ENOTSUP;
  }
  int list_xattr(std::string_view, char *, size_t) override {
    return -ENOTSUP;
  }
  int remove_xattr(std::string_view, const char *) override {
    return -ENOTSUP;
  }

 private:
  int oss_stat_file(std::string_view path, struct stat *buf, std::string *etag);

  int oss_stat_dir(std::string_view path, struct stat *buf);

  int oss_list_objects(std::string_view prefix, ListObjectsCallback cb,
                       bool delimiter, int max_keys = 0,
                       std::string *marker = nullptr);

  size_t symlink_size_with_root_backtrack(size_t oss_symlink_target_size,
                                          int symlink_parent_depth);
  bool is_oss_symlink_target_valid(std::string_view target,
                                   bool validate_components);

  ObjStoreOptions opts_;
  std::unique_ptr<Client> oss_client_;
};

IObjStore *new_oss_store(const char *key, const char *key_secret,
                         const ObjStoreOptions &opts);

}  // namespace OssFileSystem
