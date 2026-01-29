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

#include "oss_adapter.h"

#include <photon/common/iovector.h>
#include <photon/thread/timer.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>

#include <filesystem>

#include "common/fault_injector.h"
#include "common/logger.h"
#include "common/utils.h"
#include "metric/metrics.h"

namespace OssFileSystem {

#define DO_OSS_CALL(func, ...)                               \
  ({                                                         \
    DECLARE_METRIC_LATENCY(oss_##func, Metric::kOssMetrics); \
    auto ret = oss_client_->func(__VA_ARGS__);               \
    if (ret < 0) {                                           \
      int saved_errno = errno;                               \
      if (unlikely(saved_errno == 0)) {                      \
        LOG_WARN("Got unexpected errno 0");                  \
        saved_errno = EFAULT;                                \
      }                                                      \
      ret = -saved_errno;                                    \
    }                                                        \
    ret;                                                     \
  })

#define MAKE_CCL estring::make_conditional_cat_list

static constexpr mode_t kOssDirMode = (S_IFDIR | 0755);
static constexpr mode_t kOssFileMode = (S_IFREG | 0755);
static constexpr mode_t kOssSymlinkMode = (S_IFLNK | 0777);

static bool is_obj_name_valid(std::string_view obj) {
  if (obj.size() == 0) return false;
  if (obj == "." || obj == "..") return false;
  // We must do this check as d_name is a 256 size array.
  if (obj.size() > kOssfsMaxFileNameLength) return false;
  return true;
}

static bool starts_with_dotdot(const std::filesystem::path &path) {
  auto it = path.begin();
  return it != path.end() && *it == "..";
}

OssDirent::OssDirent(std::string_view name, uint64_t size, time_t mtime,
                     unsigned char type, std::string_view etag)
    : size_(size), mtime_(mtime), type_(type), etag_(etag) {
  auto len = std::min(name.size(), sizeof(name_) - 1);
  memcpy(name_, name.data(), len);
  name_[len] = '\0';
  name_size_ = len;
}

OssAdapter::OssAdapter(const OssAdapterOptions &options, Authenticator *auth)
    : opts_(options), oss_client_(new_oss_client(options, auth)) {
  // Remove the leading && tailing '/' to make it easier to convert an fs path
  // into an object path.
  if (opts_.prefix.size() && opts_.prefix.back() == '/')
    opts_.prefix.pop_back();
  if (opts_.prefix.size() && opts_.prefix.front() == '/')
    opts_.prefix = opts_.prefix.substr(1);
}

void OssAdapter::set_credentials(CredentialParameters &&creds) {
  oss_client_->set_credentials(std::move(creds));
}

int OssAdapter::oss_head_object(std::string_view path, ObjectHeaderMeta &meta) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(head_object, path.substr(1), meta);

  estring obj_path;
  obj_path.appends(opts_.prefix, path);
  return DO_OSS_CALL(head_object, obj_path, meta);
}

ssize_t OssAdapter::oss_get_object_range(std::string_view path,
                                         const struct iovec *iov, int iovcnt,
                                         off_t offset) {
  auto time_before_req = std::chrono::steady_clock::now();

  iovector_view view((struct iovec *)iov, iovcnt);
  auto cnt = view.sum();

  ssize_t r = 0;

  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(get_object_range, path.substr(1), iov, iovcnt, offset);
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(get_object_range, obj_path, iov, iovcnt, offset);
  }

  // Got partial data means oss object has been truncated to smaller size
  // after file open, return error to caller and let it to reopen it.
  if (r > 0 && r != static_cast<ssize_t>(cnt)) {
    LOG_ERROR("Got unexpected size of object `, offset `, expected `, got `",
              path, offset, cnt, r);
    return -EINVAL;
  }

  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_read, Metric::kOssMetrics, time_before_req,
                                 r);
  }
  return r;
}

ssize_t OssAdapter::oss_put_object(std::string_view path,
                                   const struct iovec *iov, int iovcnt,
                                   uint64_t *expected_crc64) {
  auto time_before_req = std::chrono::steady_clock::now();
  ssize_t r = 0;
  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(put_object, path.substr(1), iov, iovcnt, expected_crc64);
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(put_object, obj_path, iov, iovcnt, expected_crc64);
  }

  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_write, Metric::kOssMetrics,
                                 time_before_req, r);
  }
  return r;
}

ssize_t OssAdapter::oss_append_object(std::string_view path,
                                      const struct iovec *iov, int iovcnt,
                                      off_t position,
                                      uint64_t *expected_crc64) {
  auto time_before_req = std::chrono::steady_clock::now();
  ssize_t r = 0;

  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(append_object, path.substr(1), iov, iovcnt, position,
                    expected_crc64);
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(append_object, obj_path, iov, iovcnt, position,
                    expected_crc64);
  }

  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_write, Metric::kOssMetrics,
                                 time_before_req, r);
  }
  return r;
}

int OssAdapter::oss_copy_object(std::string_view src_path,
                                std::string_view dst_path, bool overwrite,
                                bool set_mime) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(copy_object, src_path.substr(1), dst_path.substr(1),
                       overwrite, set_mime);

  estring src_obj_path, dst_obj_path;
  src_obj_path.appends(opts_.prefix, src_path);
  dst_obj_path.appends(opts_.prefix, dst_path);
  return DO_OSS_CALL(copy_object, src_obj_path, dst_obj_path, overwrite,
                     set_mime);
}

int OssAdapter::oss_init_multipart_upload(std::string_view path,
                                          void **context) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(init_multipart_upload, path.substr(1), context);

  estring obj_path;
  obj_path.appends(opts_.prefix, path);
  return DO_OSS_CALL(init_multipart_upload, obj_path, context);
}

ssize_t OssAdapter::oss_upload_part(void *context, const struct iovec *iov,
                                    int iovcnt, int part_number,
                                    uint64_t *expected_crc64) {
  auto time_before_req = std::chrono::steady_clock::now();
  ssize_t r = DO_OSS_CALL(upload_part, context, iov, iovcnt, part_number,
                          expected_crc64);
  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_write, Metric::kOssMetrics,
                                 time_before_req, r);
  }
  return r;
}

int OssAdapter::oss_upload_part_copy(void *context, off_t offset, size_t count,
                                     int part_number) {
  return DO_OSS_CALL(upload_part_copy, context, offset, count, part_number);
}

int OssAdapter::oss_complete_multipart_upload(void *context,
                                              uint64_t *expected_crc64) {
  return DO_OSS_CALL(complete_multipart_upload, context, expected_crc64);
}

int OssAdapter::oss_abort_multipart_upload(void *context) {
  return DO_OSS_CALL(abort_multipart_upload, context);
}

int OssAdapter::oss_rename_object(std::string_view src_path,
                                  std::string_view dst_path, bool set_mime) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(rename_object, src_path.substr(1), dst_path.substr(1),
                       set_mime);
  estring src_obj_path, dst_obj_path;
  src_obj_path.appends(opts_.prefix, src_path);
  dst_obj_path.appends(opts_.prefix, dst_path);
  return DO_OSS_CALL(rename_object, src_obj_path, dst_obj_path, set_mime);
}

int OssAdapter::oss_delete_objects_under_dir(
    std::string_view path, const std::vector<std::string_view> &objects) {
  estring dir_prefix;
  dir_prefix.appends(MAKE_CCL(!opts_.prefix.empty(), opts_.prefix, "/"),
                     path.substr(1), MAKE_CCL(path.back() != '/', "/"));

  return DO_OSS_CALL(delete_objects, objects, dir_prefix);
}

int OssAdapter::oss_delete_object(std::string_view path) {
  if (opts_.prefix.empty()) return DO_OSS_CALL(delete_object, path.substr(1));

  estring obj_path;
  obj_path.appends(opts_.prefix, path);
  return DO_OSS_CALL(delete_object, obj_path);
}

// oss stat function will check both file and dir
int OssAdapter::oss_stat(std::string_view path, struct stat *buf,
                         std::string *etag) {
  int r = oss_stat_file(path, buf, etag);
  if (r == 0 || r != -ENOENT) return r;
  return oss_stat_dir(path, buf);
}

int OssAdapter::oss_list_dir(std::string_view path, ObjectList &results,
                             std::string *context) {
  estring dir_prefix;
  dir_prefix.appends(MAKE_CCL(!opts_.prefix.empty(), opts_.prefix, "/"),
                     path.substr(1), MAKE_CCL(path.back() != '/', "/"));
  int dir_depth = std::count(path.begin(), path.end(), '/');

  results.reserve(opts_.max_list_ret_cnt);
  auto callback = [&](const ListObjectsCBParameters &params) {
    auto name = params.key.substr(dir_prefix.size());
    if (params.is_com_prefix) {
      if (name.size() > 0 && name.back() == '/') name.remove_suffix(1);
      if (is_obj_name_valid(name)) {
        results.emplace_back(name, 0, 0, DT_DIR, "");
      } else {
        LOG_WARN("skipped dir obj ` in list results under prefix `", params.key,
                 dir_prefix);
      }
    } else {
      if (is_obj_name_valid(name)) {
        unsigned char type =
            (opts_.enable_symlink && params.type == kOssObjectTypeSymlink)
                ? DT_LNK
                : DT_REG;
        size_t file_size = params.size;
        if (type == DT_LNK) {
          // Compute relative symlink size.
          file_size = symlink_size_with_root_backtrack(file_size, dir_depth);
        }
        results.emplace_back(name, file_size, params.mtime, type, params.etag);
      } else if (name.size() > 0) {
        LOG_WARN("skipped file obj ` in list results under prefix `",
                 params.key, dir_prefix);
      }
    }

    if (IS_FAULT_INJECTION_ENABLED(FI_Readdir_list_Delay)) {
      photon::thread_usleep(1000 * 300);
    }

    return 0;
  };

  // List only once if context is provided.
  return oss_list_objects(dir_prefix, callback, true /*delimiter*/,
                          0 /*default max-keys*/, context);
}

int OssAdapter::oss_check_bucket() {
  auto noop = [](const ListObjectsCBParameters &) { return 0; };
  std::string marker;  // provide marker to do one time list only
  if (opts_.prefix.empty())
    return oss_list_objects({}, noop, false, 1, &marker);

  estring obj_path;
  obj_path.appends(opts_.prefix, "/");
  return oss_list_objects(obj_path, noop, false, 1, &marker);
}

int OssAdapter::oss_is_dir_empty(std::string_view path, bool &is_empty) {
  estring dir_prefix;
  dir_prefix.appends(MAKE_CCL(!opts_.prefix.empty(), opts_.prefix, "/"),
                     path.substr(1), MAKE_CCL(path.back() != '/', "/"));

  is_empty = true;
  auto callback = [&](const ListObjectsCBParameters &params) {
    auto name = params.key.substr(dir_prefix.size());
    if (name.empty()) return 0;  // just ignore dir/ itself
    is_empty = false;
    errno = EINTR;
    return -1;
  };

  std::string context;
  int r = 0, ret_cnt = 2;
  do {
    r = oss_list_objects(dir_prefix, callback, false /*no delimiter*/, ret_cnt,
                         &context);
    if (r < 0) break;
    ret_cnt = std::min(ret_cnt * 4, 100);
  } while (!context.empty());

  if (r == -EINTR && !is_empty) {
    return 0;
  }
  return r;
}

int OssAdapter::oss_list_dir_descendants(std::string_view path,
                                         std::vector<std::string> &results,
                                         std::function<bool()> checker,
                                         bool *is_dirobj) {
  estring dir_prefix;
  dir_prefix.appends(MAKE_CCL(!opts_.prefix.empty(), opts_.prefix, "/"),
                     path.substr(1), MAKE_CCL(path.back() != '/', "/"));

  if (is_dirobj) *is_dirobj = false;
  bool checker_alarmed = false;
  auto callback = [&](const ListObjectsCBParameters &params) {
    if (checker && !checker()) {
      checker_alarmed = true;
      errno = EINTR;
      return -1;
    }
    auto name = params.key.substr(dir_prefix.size());
    if (!name.empty()) {
      results.emplace_back(name);
    } else if (is_dirobj) {
      *is_dirobj = true;
    }
    return 0;
  };

  int r = oss_list_objects(dir_prefix, callback, false /*no delimiter*/);
  if (checker_alarmed && r == -EINTR) r = 0;
  return r;
}

int OssAdapter::oss_stat_file(std::string_view path, struct stat *buf,
                              std::string *etag) {
  ObjectMeta meta;
  int r = 0;
  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(get_object_meta, path.substr(1), meta);
    if (r < 0) return r;
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(get_object_meta, obj_path, meta);
    if (r < 0) return r;
  }

  if (buf) {
    buf->st_size = meta.size;
    buf->st_mtime = meta.mtime;
    buf->st_atim = buf->st_mtim;
    buf->st_ctim = buf->st_mtim;
    if (path.back() == '/') {
      // For object with '/' suffix, it's always treated as a directory.
      buf->st_mode = kOssDirMode;
    } else {
      if (opts_.enable_symlink && meta.type == kOssObjectTypeSymlink) {
        // The path is start with '/'.
        int dir_depth = std::count(path.begin(), path.end(), '/') - 1;
        buf->st_size =
            symlink_size_with_root_backtrack(buf->st_size, dir_depth);
        buf->st_mode = kOssSymlinkMode;
      } else {
        buf->st_mode = kOssFileMode;
      }
    }
  }

  if (etag) {
    *etag = meta.etag;
  }
  return 0;
}

int OssAdapter::oss_stat_dir(std::string_view path, struct stat *buf) {
  bool existed = false;
  auto callback = [&](const ListObjectsCBParameters &params) {
    existed = true;
    errno = EINTR;
    return -1;
  };

  estring dir_prefix;
  dir_prefix.appends(MAKE_CCL(!opts_.prefix.empty(), opts_.prefix, "/"),
                     path.substr(1), MAKE_CCL(path.back() != '/', "/"));

  std::string context;
  int r = 0, ret_cnt = 1;
  do {
    r = oss_list_objects(dir_prefix, callback, false /*no delimiter*/, ret_cnt,
                         &context);
    if (r < 0) break;
    ret_cnt = std::min(ret_cnt * 4, 100);
  } while (!context.empty());

  if (existed) {
    if (buf) {
      buf->st_mode = kOssDirMode;
      buf->st_size = 0;
    }
    return 0;
  }
  if (r == 0) {
    r = -ENOENT;
  }
  return r;
}

int OssAdapter::oss_get_symlink(std::string_view path, std::string &target) {
  if (!opts_.enable_symlink) return -EOPNOTSUPP;

  int r = 0;
  std::string original_oss_target;
  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(get_symlink, path.substr(1), original_oss_target);
  } else {
    r = DO_OSS_CALL(get_symlink, opts_.prefix + path, original_oss_target);
  }

  if (r < 0) return r;

  if (!opts_.prefix.empty()) {
    if (!is_subdir(opts_.prefix, original_oss_target)) {
      LOG_ERROR("skipped invalid symlink target ` -> `", path,
                original_oss_target);
      return -EINVAL;
    }
    target = original_oss_target.substr(opts_.prefix.size() + 1);
  } else {
    target = original_oss_target;
  }

  if (!is_oss_symlink_target_valid(target, true)) {
    LOG_ERROR("skipped invalid symlink target ` -> `", path,
              original_oss_target);
    return -EINVAL;
  }

  // For oss symlink, we backtrack the path to the root and concat the target.
  int dir_depth = std::count(path.begin(), path.end(), '/') - 1;

  // The maximum length of an OSS object key is 1023 characters.
  // Therefore, the maximum depth of a valid file system path is limited to 512.
  // This is defensive code; theoretically, the link length should never
  // exceed PATH_MAX.
  if (dir_depth * 3 + target.size() > PATH_MAX) {
    LOG_ERROR("skipped too long symlink target ` -> `", path,
              original_oss_target);
    return -ENAMETOOLONG;
  }

  std::string result;
  for (int i = 0; i < dir_depth; i++) {
    result.append("../");
  }
  target.insert(0, result);

  return 0;
}

ssize_t OssAdapter::oss_put_symlink(std::string_view path,
                                    std::string_view target) {
  if (!opts_.enable_symlink) return -EOPNOTSUPP;

  std::filesystem::path target_path(target);
  std::filesystem::path base(path.substr(1));
  auto normalized = (base.parent_path() / target_path).lexically_normal();
  auto oss_target = normalized.string();

  LOG_DEBUG("put oss symlink ` -> `(`)", path, oss_target, target);

  if (starts_with_dotdot(normalized) ||
      !is_oss_symlink_target_valid(oss_target, false)) {
    LOG_ERROR("invalid oss symlink target ` -> `(`)", path, oss_target, target);
    return -EINVAL;
  }

  int r = 0;
  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(put_symlink, path.substr(1), oss_target);
  } else {
    r = DO_OSS_CALL(put_symlink, opts_.prefix + path,
                    opts_.prefix + "/" + oss_target);
  }

  if (r < 0) return r;

  int dir_depth = std::count(path.begin(), path.end(), '/') - 1;
  return symlink_size_with_root_backtrack(oss_target.size(), dir_depth);
}

int OssAdapter::oss_list_objects(std::string_view prefix,
                                 ListObjectsCallback cb, bool delimiter,
                                 int max_keys, std::string *context) {
  ListObjectsParameters params;
  params.max_keys = max_keys;
  params.slash_delimiter = delimiter;
  params.ver = opts_.use_list_obj_v2 ? 2 : 1;
  return DO_OSS_CALL(list_objects, prefix, cb, params, context);
}

size_t OssAdapter::symlink_size_with_root_backtrack(
    size_t oss_symlink_target_size, int symlink_parent_depth) {
  size_t res = oss_symlink_target_size;
  if (opts_.prefix.empty()) {
    res = oss_symlink_target_size + symlink_parent_depth * 3;
  } else {
    if (oss_symlink_target_size > opts_.prefix.size()) {
      res = oss_symlink_target_size + symlink_parent_depth * 3 -
            opts_.prefix.size() - 1;
    }
  }
  return res;
}

bool OssAdapter::is_oss_symlink_target_valid(std::string_view target,
                                             bool validate_components) {
  if (target.front() == '/') return false;
  if (target.empty()) return false;
  size_t real_size = opts_.prefix.empty()
                         ? target.size()
                         : target.size() + opts_.prefix.size() + 1;
  if (real_size > kOssMaxObjectKeyLength) return false;

  if (validate_components) {
    auto components = split_string(target, "/");
    for (size_t i = 0; i < components.size(); i++) {
      // Allow empty component at the end.
      if (i != components.size() - 1 && components[i].empty()) return false;
      if (components[i] == "." || components[i] == "..") {
        return false;
      }
    }
  }
  return true;
}

OssAdapter *new_oss_client(const char *key, const char *key_secret,
                           const OssAdapterOptions &options) {
  auto auth = new_basic_oss_authenticator({key, key_secret});
  if (options.use_auth_cache) {
    auth = new_cached_oss_authenticator(auth);
  }
  return new OssAdapter(options, auth);
}

}  // namespace OssFileSystem
