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

#include <functional>
#include <string>
#include <vector>

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

struct ObjDirent {
  ObjDirent(std::string_view name, uint64_t size, time_t mtime,
            unsigned char type, std::string_view etag)
      : size_(size), mtime_(mtime), type_(type), etag_(etag) {
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
  char name_[255 + 1] = {};  // kMaxFileNameLength
  uint16_t name_size_ = 0;
};

using ObjectList = std::vector<ObjDirent>;

struct ObjStoreOptions : public photon::objstore_from_photon::ClientOptions {
  std::string prefix;
  bool use_list_obj_v2 = true;
  bool use_auth_cache = false;
  bool enable_symlink = false;
  virtual ~ObjStoreOptions() = default;
};

class IObjStore {
 public:
  virtual ~IObjStore() = default;

  virtual void set_credentials(ObjCredentials &&creds) = 0;

  virtual const ObjStoreOptions &get_options() const = 0;

  virtual int head_object(std::string_view path, ObjHeaderMeta &meta) = 0;

  virtual ssize_t get_object_range(std::string_view path,
                                   const struct iovec *iov, int iovcnt,
                                   off_t offset) = 0;

  virtual ssize_t put_object(std::string_view path, const struct iovec *iov,
                             int iovcnt,
                             uint64_t *expected_crc64 = nullptr) = 0;

  virtual int copy_object(std::string_view src_path, std::string_view dst_path,
                          bool overwrite = false, bool set_mime = false) = 0;

  virtual int rename_object(std::string_view src_path,
                            std::string_view dst_path,
                            bool set_mime = false) = 0;

  virtual int delete_object(std::string_view path) = 0;

  virtual int stat(std::string_view path, struct stat *buf,
                   std::string *etag) = 0;

  virtual int list_dir(std::string_view path, ObjectList &results,
                       std::string *context = nullptr) = 0;

  virtual int check_bucket() = 0;

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

  virtual int upload_part_copy(void *context, off_t offset, size_t count,
                               int part_number) = 0;

  virtual int complete_multipart_upload(void *context,
                                        uint64_t *expected_crc64) = 0;

  virtual int abort_multipart_upload(void *context) = 0;

  virtual int delete_objects_under_dir(
      std::string_view path, const std::vector<std::string_view> &objects) = 0;

  virtual int list_dir_descendants(std::string_view path,
                                   std::vector<std::string> &results,
                                   std::function<bool()> checker = nullptr,
                                   bool *is_dirobj = nullptr) = 0;
};

}  // namespace OssFileSystem
