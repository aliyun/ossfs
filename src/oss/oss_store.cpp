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

#include "oss_store.h"

#include <photon/common/checksum/crc64ecma.h>
#include <photon/common/iovector.h>
#include <photon/thread/timer.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <memory>

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

namespace {
struct FdWriterCtx {
  int fd;
  off_t base_offset;
  size_t count;
  uint64_t *crc64_acc = nullptr;
};

// BodyWriter callback: pread from fd, write to output IStream.
// Photon SDK returns error if the returned val != content-length.
// Consistent with BodyWriteStream::writev: return -1 if the written bytes !=
// expected bytes.
ssize_t fd_body_writer_cb(void *ctx_ptr, IStream *output) {
  auto *ctx = static_cast<FdWriterCtx *>(ctx_ptr);
  // The HTTP layer re-invokes this callback on retry; reset the accumulator
  // so the CRC always reflects exactly the body of the final attempt.
  if (ctx->crc64_acc) *ctx->crc64_acc = 0;
  constexpr size_t kBufSize = 128 * 1024;
  char buf[kBufSize];
  size_t remaining = ctx->count;
  off_t offset = 0;  // offset relative to ctx->base_offset
  while (remaining > 0) {
    size_t to_read = std::min(remaining, kBufSize);
    ssize_t n;
    do {
      n = ::pread(ctx->fd, buf, to_read, ctx->base_offset + offset);
    } while (n < 0 && errno == EINTR);
    if (n <= 0) return -1;
    if (ctx->crc64_acc) {
      *ctx->crc64_acc = crc64ecma(buf, n, *ctx->crc64_acc);
    }
    FAULT_INJECTION(FI_Modify_Staging_Data,
                    [&]() { buf[0] = static_cast<char>(buf[0] + 1); });
    ssize_t w = output->write(buf, n);
    // Simulate a connection reset mid-body: the chunk has already been
    // CRC-accumulated above, and this failure forces an HTTP-layer retry.
    FAULT_INJECTION(FI_Upload_BodyWriter_Partial_Fail, [&]() {
      w = -1;
      errno = ECONNRESET;
    });
    if (w != n) return -1;
    offset += n;
    remaining -= n;
  }
  return ctx->count;
}

struct FdReaderCtx {
  int fd;
  off_t base_offset;
  size_t count;
};

// BodyReader callback: read from input IStream, pwrite to fd.
// Photon SDK returns error if the returned val != content-length.
// Consistent with BodyReadStream::readv: return the actual bytes read if
// short-read.
ssize_t fd_body_reader_cb(void *ctx_ptr, IStream *input) {
  auto *ctx = static_cast<FdReaderCtx *>(ctx_ptr);
  constexpr size_t kBufSize = 128 * 1024;
  char buf[kBufSize];
  size_t total = 0;
  while (total < ctx->count) {
    size_t to_read = std::min(ctx->count - total, kBufSize);
    ssize_t n = input->read(buf, to_read);
    if (n < 0) return n;
    if (n == 0) break;
    ssize_t w;
    do {
      w = ::pwrite(ctx->fd, buf, n, ctx->base_offset + total);
    } while (w < 0 && errno == EINTR);
    if (w < 0) return w;
    total += static_cast<size_t>(w);
    if (w != n) break;
  }
  return static_cast<ssize_t>(total);
}
}  // namespace

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

OssStore::OssStore(const ObjStoreOptions &options, Authenticator *auth)
    : opts_(options), oss_client_(new_oss_client(options, auth)) {}

void OssStore::set_credentials(ObjCredentials &&creds) {
  oss_client_->set_credentials(std::move(creds));
}

int OssStore::head_object(std::string_view path, ObjHeaderMeta &meta) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(head_object, path.substr(1), meta);

  estring obj_path;
  obj_path.appends(opts_.prefix, path);
  return DO_OSS_CALL(head_object, obj_path, meta);
}

ssize_t OssStore::get_object_range(std::string_view path,
                                   const struct iovec *iov, int iovcnt,
                                   off_t offset, std::string *response_etag) {
  auto time_before_req = std::chrono::steady_clock::now();

  iovector_view view((struct iovec *)iov, iovcnt);
  auto cnt = view.sum();

  ssize_t r = 0;
  ObjectHeaderMeta meta;
  ObjectHeaderMeta *meta_ptr = response_etag ? &meta : nullptr;

  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(get_object_range, path.substr(1), iov, iovcnt, offset,
                    meta_ptr);
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(get_object_range, obj_path, iov, iovcnt, offset, meta_ptr);
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
    if (response_etag && meta.has_etag()) {
      *response_etag = meta.etag;
    }
  }
  return r;
}

ssize_t OssStore::get_object_range_to_fd(std::string_view path, int fd,
                                         off_t fd_offset, off_t obj_offset,
                                         size_t count) {
  auto time_before_req = std::chrono::steady_clock::now();

  FdReaderCtx ctx{fd, fd_offset, count};
  BodyReader reader{&ctx, &fd_body_reader_cb};

  ssize_t r = 0;
  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(get_object_range, path.substr(1), obj_offset, count,
                    reader);
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(get_object_range, obj_path, obj_offset, count, reader);
  }

  if (r > 0 && r != static_cast<ssize_t>(count)) {
    LOG_ERROR("Got unexpected size of object `, offset `, expected `, got `",
              path, obj_offset, count, r);
    return -EINVAL;
  }

  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_read_to_disk, Metric::kOssMetrics,
                                 time_before_req, r);
  }
  return r;
}

ssize_t OssStore::put_object(std::string_view path, const struct iovec *iov,
                             int iovcnt, uint64_t *expected_crc64,
                             mode_t /*mode*/) {
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

ssize_t OssStore::put_object_from_fd(std::string_view path, int fd,
                                     off_t offset, size_t count,
                                     uint64_t *expected_crc64,
                                     std::string *etag) {
  auto time_before_req = std::chrono::steady_clock::now();
  FdWriterCtx ctx{fd, offset, count, expected_crc64};
  BodyWriter writer{&ctx, &fd_body_writer_cb};
  ObjectUploadOptions opts;
  opts.expected_crc64 = expected_crc64;
  opts.etag = etag;
  ssize_t r = 0;
  if (opts_.prefix.empty()) {
    r = DO_OSS_CALL(put_object, path.substr(1), count, writer, opts);
  } else {
    estring obj_path;
    obj_path.appends(opts_.prefix, path);
    r = DO_OSS_CALL(put_object, obj_path, count, writer, opts);
  }

  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_write_from_disk, Metric::kOssMetrics,
                                 time_before_req, r);
  }
  return r;
}

ssize_t OssStore::append_object(std::string_view path, const struct iovec *iov,
                                int iovcnt, off_t position,
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

int OssStore::copy_object(std::string_view src_path, std::string_view dst_path,
                          bool overwrite, bool set_mime) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(copy_object, src_path.substr(1), dst_path.substr(1),
                       overwrite, set_mime);

  estring src_obj_path, dst_obj_path;
  src_obj_path.appends(opts_.prefix, src_path);
  dst_obj_path.appends(opts_.prefix, dst_path);
  return DO_OSS_CALL(copy_object, src_obj_path, dst_obj_path, overwrite,
                     set_mime);
}

int OssStore::init_multipart_upload(std::string_view path, void **context) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(init_multipart_upload, path.substr(1), context);

  estring obj_path;
  obj_path.appends(opts_.prefix, path);
  return DO_OSS_CALL(init_multipart_upload, obj_path, context);
}

ssize_t OssStore::upload_part(void *context, const struct iovec *iov,
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

ssize_t OssStore::upload_part_from_fd(void *context, int fd, off_t offset,
                                      size_t count, int part_number,
                                      uint64_t *expected_crc64) {
  FdWriterCtx ctx{fd, offset, count, expected_crc64};
  BodyWriter writer{&ctx, &fd_body_writer_cb};
  ObjectUploadOptions opts;
  opts.expected_crc64 = expected_crc64;
  auto time_before_req = std::chrono::steady_clock::now();
  ssize_t r =
      DO_OSS_CALL(upload_part, context, count, part_number, writer, opts);
  if (r > 0) {
    REPORT_ALL_METRIC_SUCCESSFUL(oss_write_from_disk, Metric::kOssMetrics,
                                 time_before_req, r);
  }
  return r;
}

int OssStore::upload_part_copy(void *context, off_t offset, size_t count,
                               int part_number, uint64_t *crc64_out) {
  int r = DO_OSS_CALL(upload_part_copy, context, offset, count, part_number,
                      std::string_view{}, crc64_out);
  // Simulate a copy-part response without the CRC64 header: photon leaves
  // the sentinel untouched in that case.
  FAULT_INJECTION(FI_RandomWrite_Copy_Part_No_Crc, [&]() {
    if (r == 0 && crc64_out) *crc64_out = ~0ULL;
  });
  return r;
}

int OssStore::complete_multipart_upload(void *context, uint64_t *expected_crc64,
                                        std::string *etag) {
  ObjectUploadOptions opts;
  opts.expected_crc64 = expected_crc64;
  opts.etag = etag;
  return DO_OSS_CALL(complete_multipart_upload, context, opts);
}

int OssStore::abort_multipart_upload(void *context) {
  return DO_OSS_CALL(abort_multipart_upload, context);
}

int OssStore::rename_object(std::string_view src_path,
                            std::string_view dst_path, bool set_mime,
                            bool /*dst_exists*/) {
  if (opts_.prefix.empty())
    return DO_OSS_CALL(rename_object, src_path.substr(1), dst_path.substr(1),
                       set_mime);
  estring src_obj_path, dst_obj_path;
  src_obj_path.appends(opts_.prefix, src_path);
  dst_obj_path.appends(opts_.prefix, dst_path);
  return DO_OSS_CALL(rename_object, src_obj_path, dst_obj_path, set_mime);
}

int OssStore::delete_objects_under_dir(
    std::string_view path, const std::vector<std::string_view> &objects) {
  estring dir_prefix;
  dir_prefix.appends(MAKE_CCL(!opts_.prefix.empty(), opts_.prefix, "/"),
                     path.substr(1), MAKE_CCL(path.back() != '/', "/"));

  return DO_OSS_CALL(delete_objects, objects, dir_prefix);
}

int OssStore::delete_object(std::string_view path) {
  if (opts_.prefix.empty()) return DO_OSS_CALL(delete_object, path.substr(1));

  estring obj_path;
  obj_path.appends(opts_.prefix, path);
  return DO_OSS_CALL(delete_object, obj_path);
}

// oss stat function will check both file and dir
int OssStore::stat(std::string_view path, struct stat *buf, std::string *etag) {
  int r = oss_stat_file(path, buf, etag);
  if (r == 0 || r != -ENOENT) return r;
  return oss_stat_dir(path, buf);
}

int OssStore::list_dir(std::string_view path, ObjectList &results,
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
        results.emplace_back(name, 0, timespec{}, DT_DIR, "");
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
        results.emplace_back(name, file_size, timespec{params.mtime, 0}, type,
                             params.etag);
      } else if (name.size() > 0) {
        LOG_WARN("skipped file obj ` in list results under prefix `",
                 params.key, dir_prefix);
      }
    }

    FAULT_INJECTION(FI_Readdir_list_Delay,
                    []() { photon::thread_usleep(1000 * 300); });

    return 0;
  };

  // List only once if context is provided.
  return oss_list_objects(dir_prefix, callback, true /*delimiter*/,
                          0 /*default max-keys*/, context);
}

int OssStore::check_bucket(bool allow_auto_create) {
  auto noop = [](const ListObjectsCBParameters &) { return 0; };
  std::string marker;  // provide marker to do one time list only
  estring obj_path;
  if (!opts_.prefix.empty()) obj_path.appends(opts_.prefix, "/");

  int r = oss_list_objects(obj_path, noop, false, 1, &marker);
  FAULT_INJECTION(FI_Check_Bucket_List_Not_Found, [&]() { r = -ENOENT; });
  if (r == -ENOENT && opts_.auto_create_bucket && allow_auto_create) {
    LOG_INFO("bucket ` not found, auto creating (agentic_bucket=`)",
             opts_.bucket, opts_.agentic_bucket);
    int cr = DO_OSS_CALL(put_bucket, opts_.agentic_bucket);
    // -ENOTSUP maps from HTTP 409 (bucket already exists), treat as success.
    if (cr < 0 && cr != -ENOTSUP) {
      LOG_ERROR("failed to auto create bucket `, error `", opts_.bucket, cr);
      return cr;
    }
    return 0;
  }
  return r;
}

int OssStore::delete_bucket() {
  return DO_OSS_CALL(delete_bucket);
}

int OssStore::is_dir_empty(std::string_view path, bool &is_empty) {
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

int OssStore::list_dir_descendants(std::string_view path,
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

int OssStore::oss_stat_file(std::string_view path, struct stat *buf,
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

int OssStore::oss_stat_dir(std::string_view path, struct stat *buf) {
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

int OssStore::get_symlink(std::string_view path, std::string &target) {
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

ssize_t OssStore::put_symlink(std::string_view path, std::string_view target) {
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

int OssStore::oss_list_objects(std::string_view prefix, ListObjectsCallback cb,
                               bool delimiter, int max_keys,
                               std::string *context) {
  ListObjectsParameters params;
  params.max_keys = max_keys;
  params.slash_delimiter = delimiter;
  params.ver = opts_.use_list_obj_v2 ? 2 : 1;
  return DO_OSS_CALL(list_objects, prefix, cb, params, context);
}

size_t OssStore::symlink_size_with_root_backtrack(
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

bool OssStore::is_oss_symlink_target_valid(std::string_view target,
                                           bool validate_components) {
  if (target.empty()) return false;
  if (target.front() == '/') return false;
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

int OssStore::truncate_object(std::string_view path, size_t to_size) {
  // OSS only supports truncate to 0.
  if (to_size != 0) {
    LOG_WARN("OSS only supports truncate to 0, path: `, to_size: `", path,
             to_size);
    return -ENOTSUP;
  }

  // Write empty object (equivalent to delete and create empty file).
  // Only supports Normal object.
  iovec iov{nullptr, 0};
  uint64_t expected_crc64 = 0;
  ssize_t ret = put_object(path, &iov, 1, &expected_crc64);
  if (ret < 0) {
    LOG_ERROR("Failed to truncate OSS object to 0, path: `, ret: `", path, ret);
    return ret;
  }

  return 0;
}

IObjStore *new_oss_store(const char *key, const char *key_secret,
                         const ObjStoreOptions &options) {
  auto auth = new_basic_oss_authenticator({key, key_secret});
  if (options.use_auth_cache) {
    auth = new_cached_oss_authenticator(auth);
  }
  return new OssStore(options, auth);
}

int OssStore::rename_dir(std::string_view src_path, std::string_view dst_path,
                         bool /*dst_exists*/) {
  LOG_ERROR("rename_dir is not supported in OSS mode");
  return -ENOTSUP;
}

int OssStore::set_permission(std::string_view path, mode_t mode) {
  LOG_WARN("OSS does not support chmod. path: `", path);
  return -ENOTSUP;
}

int OssStore::set_owner(std::string_view path, uid_t uid, gid_t gid,
                        int to_set) {
  LOG_WARN("OSS does not support chown. path: `", path);
  return -ENOTSUP;
}

}  // namespace OssFileSystem
