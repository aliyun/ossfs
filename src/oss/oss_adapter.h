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

#include "oss_patched.h"

namespace OssFileSystem {

using namespace photon::objstore_from_photon;
using OssCredentials = CredentialParameters;

static constexpr int kOssfsMaxFileNameLength = 255;
static constexpr int kOssMaxObjectKeyLength = 1023;

static constexpr std::string_view kOssObjectTypeNormal = "Normal";
static constexpr std::string_view kOssObjectTypeMultipart = "Multipart";
static constexpr std::string_view kOssObjectTypeAppendable = "Appendable";
static constexpr std::string_view kOssObjectTypeSymlink = "Symlink";

struct OssAdapterOptions : public ClientOptions {
  std::string prefix;
  bool use_list_obj_v2 = true;
  bool use_auth_cache = false;
  bool enable_symlink = false;
};

struct OssDirent {
  OssDirent(std::string_view name, uint64_t size, time_t mtime,
            unsigned char type, std::string_view etag);
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
  time_t mtime() const {
    return mtime_;
  }
  unsigned char type() const {
    return type_;
  }
  std::string_view etag() const {
    return etag_;
  }

 private:
  uint64_t size_ = 0;
  time_t mtime_ = 0;
  unsigned char type_ = 0;  // same as d_type in dirent
  std::string etag_;
  char name_[kOssfsMaxFileNameLength + 1] = {};
  uint16_t name_size_ = 0;
};

using ObjectList = std::vector<OssDirent>;

//
// class OssAdapter will get requests with the path from local fs's view, and
// translate them to oss's view. One typical case is to prepend some prefix to
// the fs path, or merge several buckets into one fs.
// Also it manages the ways to refresh oss credentials.
//
// fs path starting with '/', such as /a/b/c 、/a/d 、a/b/d/
// obj path with no '/' prepended, such as a/b/c 、a/d 、a/b/d/
//
class OssAdapter {
 public:
  OssAdapter(const OssAdapterOptions &options, Authenticator *auth);

  ~OssAdapter() = default;

  void set_credentials(CredentialParameters &&creds);

  OssAdapterOptions get_options() const {
    return opts_;
  }

  int oss_head_object(std::string_view path, ObjectHeaderMeta &meta);

  ssize_t oss_get_object_range(std::string_view path, const struct iovec *iov,
                               int iovcnt, off_t offset);

  ssize_t oss_put_object(std::string_view path, const struct iovec *iov,
                         int iovcnt, uint64_t *expected_crc64 = nullptr);

  ssize_t oss_append_object(std::string_view path, const struct iovec *iov,
                            int iovcnt, off_t position,
                            uint64_t *expected_crc64 = nullptr);

  int oss_copy_object(std::string_view src_path, std::string_view dst_path,
                      bool overwrite = false, bool set_mime = false);

  int oss_init_multipart_upload(std::string_view path, void **context);

  ssize_t oss_upload_part(void *context, const struct iovec *iov, int iovcnt,
                          int part_number, uint64_t *expected_crc64 = nullptr);

  int oss_upload_part_copy(void *context, off_t offset, size_t count,
                           int part_number);

  int oss_complete_multipart_upload(void *context, uint64_t *expected_crc64);

  int oss_abort_multipart_upload(void *context);

  int oss_rename_object(std::string_view src_path, std::string_view dst_path,
                        bool set_mime = false);

  int oss_delete_objects_under_dir(
      std::string_view path, const std::vector<std::string_view> &objects);

  int oss_delete_object(std::string_view path);

  // This function will check both file and dir.
  int oss_stat(std::string_view path, struct stat *buf, std::string *etag);

  int oss_list_dir(std::string_view path, ObjectList &results,
                   std::string *context = nullptr);

  int oss_check_bucket();

  int oss_is_dir_empty(std::string_view path, bool &is_empty);

  int oss_list_dir_descendants(std::string_view path,
                               std::vector<std::string> &results,
                               std::function<bool()> checker = nullptr,
                               bool *is_dirobj = nullptr);

  int oss_get_symlink(std::string_view path, std::string &target);
  ssize_t oss_put_symlink(std::string_view path, std::string_view target);

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

  OssAdapterOptions opts_;
  std::unique_ptr<Client> oss_client_;
};

OssAdapter *new_oss_client(const char *key, const char *key_secret,
                           const OssAdapterOptions &opts);

}  // namespace OssFileSystem
