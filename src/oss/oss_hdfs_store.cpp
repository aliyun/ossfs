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

#include "oss_hdfs_store.h"

#include <grp.h>
#include <jdo_api.h>
#include <jdo_defines.h>
#include <jdo_error.h>
#include <jdo_file_status.h>
#include <jdo_list_dir_result.h>
#include <jdo_lock_info.h>
#include <jdo_option_keys.h>
#include <jdo_options.h>
#include <jdo_xattr.h>
#include <jdo_xattr_list.h>
#include <photon/thread/timer.h>
#include <pwd.h>
#include <sys/xattr.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <unordered_set>

#include "common/fault_injector.h"
#include "common/filesystem.h"
#include "common/logger.h"
#include "common/utils.h"
#include "oss/jdo_sdk_loader.h"

namespace OssFileSystem {

static uid_t username_to_uid(std::string_view username) {
  if (username.empty()) return kReservedUnresolvedUid;

  uid_t mapped = (uid_t)-1;
  FAULT_INJECTION(FI_Hdfs_UserGroup_Mapping, [&] {
    auto it = g_test_user_mapping.name_to_uid.find(std::string(username));
    if (it != g_test_user_mapping.name_to_uid.end()) mapped = it->second;
  });
  if (mapped != (uid_t)-1) return mapped;

  struct passwd pwd;
  struct passwd *result = nullptr;
  char buf[4096];

  if (getpwnam_r(std::string(username).c_str(), &pwd, buf, sizeof(buf),
                 &result) == 0 &&
      result) {
    return pwd.pw_uid;
  }

  return kReservedUnresolvedUid;
}

static gid_t groupname_to_gid(std::string_view groupname) {
  if (groupname.empty()) return kReservedUnresolvedGid;

  gid_t mapped = (gid_t)-1;
  FAULT_INJECTION(FI_Hdfs_UserGroup_Mapping, [&] {
    auto it = g_test_user_mapping.name_to_gid.find(std::string(groupname));
    if (it != g_test_user_mapping.name_to_gid.end()) mapped = it->second;
  });
  if (mapped != (gid_t)-1) return mapped;

  struct group grp;
  struct group *result = nullptr;
  char buf[4096];

  if (getgrnam_r(std::string(groupname).c_str(), &grp, buf, sizeof(buf),
                 &result) == 0 &&
      result) {
    return grp.gr_gid;
  }

  return kReservedUnresolvedGid;
}

#define START_CALL(fs, op, path)                        \
  JdoHandleCtx_t ctx = JindoSDK::createHandleCtx1(fs);  \
  auto _hdfs_start_ = std::chrono::steady_clock::now(); \
  [[maybe_unused]] const char *_hdfs_op_ = op;          \
  [[maybe_unused]] std::string_view _hdfs_path_ = path

#define START_CALL2(fs, stream, op, path)                      \
  JdoHandleCtx_t ctx = JindoSDK::createHandleCtx2(fs, stream); \
  auto _hdfs_start_ = std::chrono::steady_clock::now();        \
  [[maybe_unused]] const char *_hdfs_op_ = op;                 \
  [[maybe_unused]] std::string_view _hdfs_path_ = path

#define END_CALL()                                                           \
  auto _hdfs_elapsed_us_ =                                                   \
      std::chrono::duration_cast<std::chrono::microseconds>(                 \
          std::chrono::steady_clock::now() - _hdfs_start_)                   \
          .count();                                                          \
  auto error_code = JindoSDK::getHandleCtxErrorCode(ctx);                    \
  FAULT_INJECTION(FI_OssError_Call_Failed,                                   \
                  [&] { error_code = JDO_IO_ERROR; });                       \
  std::string error_msg;                                                     \
  const char *msg = JindoSDK::getHandleCtxErrorMsg(ctx);                     \
  if (msg != nullptr) {                                                      \
    error_msg.assign(msg);                                                   \
  }                                                                          \
  JindoSDK::freeHandleCtx(ctx);                                              \
  LOG_DEBUG("[HDFS] ` path: `, ec: `, elapsed: `us", _hdfs_op_, _hdfs_path_, \
            error_code, _hdfs_elapsed_us_);

// Same as END_CALL but skips debug log (for high-frequency read/write ops).
#define END_CALL_NO_LOGS()                                \
  (void)_hdfs_start_;                                     \
  auto error_code = JindoSDK::getHandleCtxErrorCode(ctx); \
  FAULT_INJECTION(FI_OssError_Call_Failed,                \
                  [&] { error_code = JDO_IO_ERROR; });    \
  std::string error_msg;                                  \
  const char *msg = JindoSDK::getHandleCtxErrorMsg(ctx);  \
  if (msg != nullptr) {                                   \
    error_msg.assign(msg);                                \
  }                                                       \
  JindoSDK::freeHandleCtx(ctx);

#define GET_HDFS_PATH(path) estring().appends(uri_, path)

#define CHECK_SDK_INIT()                                       \
  if (!sdk_initialized_) {                                     \
    LOG_ERROR("Jindo SDK not initialized, operation aborted"); \
    return -EIO;                                               \
  }

static unsigned char hdfs_type_to_dirent_type(uint8_t type) {
  switch (type) {
    case JDO_FILE_TYPE_FILE:
      return DT_REG;
    case JDO_FILE_TYPE_DIRECTORY:
    case JDO_FILE_TYPE_MOUNT_POINT:
      return DT_DIR;
    case JDO_FILE_TYPE_SYMLINK:
      return DT_LNK;
    default:
      return DT_UNKNOWN;
  }
}

static mode_t hdfs_type_to_mode(uint8_t type) {
  switch (type) {
    case JDO_FILE_TYPE_FILE:
      return S_IFREG;
    case JDO_FILE_TYPE_DIRECTORY:
    case JDO_FILE_TYPE_MOUNT_POINT:
      return S_IFDIR;
    case JDO_FILE_TYPE_SYMLINK:
      return S_IFLNK;
    default:
      return 0;
  }
}

static std::string get_filename_from_hdfs_uri(std::string_view uri) {
  if (!uri.empty() && uri.back() == '/') {
    uri.remove_suffix(1);
  }

  auto pos = uri.find_last_of('/');
  if (pos == std::string::npos) {
    return std::string(uri);
  }

  return std::string(uri.substr(pos + 1));
}

static bool is_file_name_valid(std::string_view obj) {
  if (obj.size() == 0) return false;
  if (obj == "." || obj == "..") return false;
  // We must do this check as d_name is a 256 size array.
  if (obj.size() > kOssfsMaxFileNameLength) return false;
  return true;
}

// Visible for testing. Pure function, no side effects.
int jdo_error_code_to_posix(int jdo_error_code) {
  switch (jdo_error_code) {
    case 0:
    case JDO_EOF_ERROR:
      return 0;

    // Client errors.
    case JDO_CLIENT_ERROR:
    case JDO_CLIENT_ILLEGAL_CONF_ERROR:
    case JDO_CLIENT_ILLEGAL_REQUEST_ERROR:
    case JDO_CLIENT_CHECKSUM_ERROR:
    case JDO_JNI_OBJ_ALLOCATION_ERROR:
    case JDO_CONFIG_OPTION_ERROR:
    case JDO_CLIENT_NO_ENOUGHT_RESERVE_MEM_ERROR:
      return -EINVAL;

    case JDO_CLIENT_NO_SPACE_RESOURCE_ERROR:
      return -ENOSPC;

    case JDO_CLIENT_NO_MEM_RESOURCE_ERROR:
      return -ENOMEM;

    case JDO_CACHESET_NOT_AVAILABLE_ERROR:
      return -EIO;

    // Network errors (retriable).
    case JDO_SERVER_ERROR:
    case JDO_SERVER_UNAVAILABLE_ERROR:
    case JDO_SERVER_NO_IN_SERVICE_ERROR:
      return -ECONNREFUSED;

    case JDO_SERVER_TIMEOUT_ERROR:
      return -ETIMEDOUT;

    // Resource errors.
    case JDO_RESOURCE_ERROR:
    case JDO_IO_ERROR:
    case JDO_CORRUPT_DATA_ERROR:
      return -EIO;

    case JDO_FILE_NOT_FOUND_ERROR:
      return -ENOENT;

    case JDO_FILE_SIZE_EXCEEDED_ERROR:
      return -EFBIG;

    case JDO_FILE_NAME_EXCEEDED_ERROR:
      return -ENAMETOOLONG;

    case JDO_QUOTA_EXCEEDED_ERROR:
      return -EDQUOT;

    case JDO_SERVER_INTERNAL_ERROR:
      return -EIO;

    case JDO_NO_SERVER_ERROR:
      return -EHOSTUNREACH;

    case JDO_SERVER_RESPONSE_TOO_LARGE_ERROR:
      return -EFBIG;

    // System logic errors.
    case JDO_SYSTEM_LOGIC_ERROR:
    case JDO_NOT_SUPPORTED_ERROR:
      return -ENOTSUP;

    case JDO_RENAME_DST_UNDER_SRC_ERROR:
    case JDO_RENAME_DST_PARENT_NOT_FOUND_ERROR:
    case JDO_RENAME_DST_PARENT_FILE_ERROR:
    case JDO_RENAME_SRC_NOT_FOUND_ERROR:
    case JDO_RENAME_DST_EXIST_AS_FILE_ERROR:
    case JDO_RENAME_SRC_EQ_DST_AS_DIRECTORY_ERROR:
    case JDO_RENAME_SRC_EQ_DST_AS_FILE_ERROR:
    case JDO_RENAME_DST_EXIST_ERROR:
      return -EINVAL;

    case JDO_DELETE_NOT_ALLOW_ERROR:
    case JDO_NO_PERMISSION_ERROR:
      return -EACCES;

    case JDO_DELETE_DIRECTORY_NOT_EMPTY_ERROR:
      return -ENOTEMPTY;

    case JDO_PARENT_NOT_DIRECTORY_ERROR:
      return -ENOTDIR;

    case JDO_FILE_ALREADY_EXISTS_ERROR:
    case JDO_FILE_ALREADY_BEING_CREATED_ERROR:
      return -EEXIST;

    case JDO_INVALID_PATH_ERROR:
      return -EINVAL;

    case JDO_TIERED_OP_NOT_ALLOWED_ERROR:
    case JDO_LOCAL_WRITE_ERROR:
    case JDO_LOCAL_READ_ERROR:
      return -EIO;

    case JDO_READ_TASK_TIMEOUT_ERROR:
    case JDO_WRITE_TASK_TIMEOUT_ERROR:
      return -ETIMEDOUT;

    case JDO_FILE_TYPE_IS_DIRECTORY_ERROR:
      return -EISDIR;

    case JDO_INVALID_ARGUMENT_ERROR:
      return -EINVAL;

    case JDO_LEASE_EXPIRED_ERROR:
    case JDO_CONFLICT_LOCK_ERROR:
      return -EAGAIN;

    case JDO_SAFE_MODE_ERROR:
      return -EROFS;

    case JDO_PATH_NOT_EMPTY_DIRECTORY_ERROR:
      return -ENOTEMPTY;

    case JDO_SNAPSHOT_ERROR:
      return -EIO;

    case JDO_NOT_DLS_BUCKET_ERROR:
    case JDO_STANDBY_ERROR:
    case JDO_RENAME_ACROSS_STORES_ERROR:
    case JDO_CONCAT_ACROSS_STORES_ERROR:
    case JDO_UNRESOLVED_LINK_ERROR:
    case JDO_SYMLINK_LOOP_ERROR:
    case JDO_SYMLINK_ACROSS_STORES_ERROR:
      return -EINVAL;

    // Xattr errors.
    case JDO_XATTR_ERROR:
    case JDO_XATTR_NOT_EXIST_ERROR:
    case JDO_XATTR_SET_ERROR:
      return -ENODATA;

    // Atomic rename errors.
    case JDO_RENAME_LOCKED_BY_OTHERS_ERROR:
      return -EAGAIN;

    case JDO_RENAME_OTS_OP_ERROR:
    case JDO_RENAME_OTS_CONDITIONAL_UPDATE_ERROR:
    case JDO_RENAME_OTS_OBJECT_NOT_EXIST_ERROR:
    case JDO_RENAME_OTS_AUTH_FAILED_ERROR:
    case JDO_RENAME_OTS_SERVER_BUSY_ERROR:
      return -EIO;

    // Security errors.
    case JDO_SECURITY_ERROR:
    case JDO_AUTHENTICATION_FAILED_ERROR:
    case JDO_ACCESS_CONTROL_ERROR:
    case JDO_ACL_DENY_ERROR:
      return -EACCES;

    // SASL errors.
    case JDO_SASL_CLIENT_ERROR:
    case JDO_SASL_SERVER_ERROR:
    case JDO_KERBEROS_ERROR:
      return -EACCES;

    // Crypto errors.
    case JDO_CRYPTO_POLICY_ALREADY_EXIST_ERROR:
    case JDO_CRYPTO_POLICY_DIR_NOT_EMPTY_ERROR:
    case JDO_CRYPTO_KEY_NOT_EXIST_ERROR:
    case JDO_ENCRYPTED_KEY_ERROR:
    case JDO_CRYPTO_POLICY_DISABLED_ERROR:
      return -EACCES;

    // Mount errors.
    case JDO_CONFLICT_MOUNT_ERROR:
    case JDO_NO_SUCH_MOUNT_ERROR:
      return -EINVAL;

    // Access policy errors.
    case JDO_ROOT_POLICY_ALREADY_EXIST_ERROR:
    case JDO_ROOT_POLICY_NOT_EXIST_ERROR:
    case JDO_TOO_MANY_ACCESS_POLICIES_ERROR:
      return -EINVAL;

    // Object HTTP errors.
    case JDO_REST_HTTP_400_ERROR:
      return -EINVAL;

    case JDO_REST_HTTP_403_ERROR:
      return -EACCES;

    case JDO_REST_HTTP_404_ERROR:
      return -ENOENT;

    case JDO_REST_HTTP_405_ERROR:
      return -ENOTSUP;

    case JDO_REST_HTTP_409_ERROR:
      return -EEXIST;

    case JDO_REST_HTTP_411_ERROR:
    case JDO_REST_HTTP_412_ERROR:
      return -EINVAL;

    case JDO_REST_HTTP_416_ERROR:
      return -EINVAL;

    case JDO_REST_HTTP_424_ERROR:
    case JDO_REST_HTTP_500_ERROR:
    case JDO_REST_HTTP_502_ERROR:
    case JDO_REST_HTTP_503_ERROR:
      return -EIO;

    default:
      return -EIO;
  }
}

OssHdfsStore::OssHdfsStore(const char *key, const char *key_secret,
                           const ObjStoreOptions &options)
    : opts_(options) {
  if (!JindoSDK::load()) {
    LOG_ERROR("Failed to load JindoSDK");
    return;
  }
  jdo_opts_ = JindoSDK::createOptions();

  // TODO: fix endpoint start with http/https
  JindoSDK::setOption(jdo_opts_, kHdfsSdkOptEndpoint, options.endpoint.c_str());
  JindoSDK::setOption(jdo_opts_, kHdfsSdkOptAccessKeyId, key);
  JindoSDK::setOption(jdo_opts_, kHdfsSdkOptAccessKeySecret, key_secret);

  apply_sdk_options();

  uri_ = "oss://" + options.bucket + "." + options.endpoint;
  if (!options.prefix.empty()) {
    uri_ += "/" + options.prefix;
  }

  jdo_store_ = JindoSDK::createStore(jdo_opts_, uri_.c_str());

  int r = init_jindosdk();
  sdk_initialized_ = (r == 0);
  if (!sdk_initialized_) {
    LOG_ERROR("Jindo SDK init failed in constructor");
  }
}

OssHdfsStore::~OssHdfsStore() {
  if (jdo_store_) {
    JindoSDK::destroyStore(jdo_store_);
    JindoSDK::freeStore(jdo_store_);
  }
  if (jdo_opts_) JindoSDK::freeOptions(jdo_opts_);
}

int OssHdfsStore::stat(std::string_view path, struct stat *buf,
                       std::string *etag) {
  CHECK_SDK_INIT();
  bool fi_err = false;
  FAULT_INJECTION(FI_OssError_Failed_Without_Call, [&] { fi_err = true; });
  if (fi_err) return -EIO;
  START_CALL(jdo_store_, "stat", path);
  estring hdfs_path = GET_HDFS_PATH(path);
  auto file_status =
      opts_.enable_symlink
          ? JindoSDK::getFileLinkStatus(ctx, hdfs_path.c_str(), nullptr)
          : JindoSDK::getFileStatus(ctx, hdfs_path.c_str(), nullptr);
  END_CALL();

  if (error_code != 0) {
    if (error_code != JDO_FILE_NOT_FOUND_ERROR) {
      LOG_ERROR("Failed to stat HDFS file: `, ec: `, msg: `", path, error_code,
                error_msg);
    }
    if (file_status) JindoSDK::freeFileStatus(file_status);
    return jdo_error_code_to_posix(error_code);
  }

  if (!file_status) {
    return -EIO;
  }
  DEFER(JindoSDK::freeFileStatus(file_status));

  auto size = JindoSDK::getFileStatusSize(file_status);
  auto type = JindoSDK::getFileStatusType(file_status);
  auto mtime_ms = JindoSDK::getFileStatusMtime(file_status);
  auto atime_ms = JindoSDK::getFileStatusAtime(file_status);
  auto perm = JindoSDK::getFileStatusPerm(file_status);
  const char *user = JindoSDK::getFileStatusUser(file_status);
  const char *group = JindoSDK::getFileStatusGroup(file_status);

  memset(buf, 0, sizeof(struct stat));
  buf->st_size = size;
  buf->st_mtim.tv_sec = mtime_ms / 1000;
  buf->st_mtim.tv_nsec = (mtime_ms % 1000) * 1000000;
  buf->st_atim.tv_sec = atime_ms / 1000;
  buf->st_atim.tv_nsec = (atime_ms % 1000) * 1000000;
  buf->st_mode = hdfs_type_to_mode(type);
  buf->st_mode |= perm;
  buf->st_uid = username_to_uid(user ? user : "");
  buf->st_gid = groupname_to_gid(group ? group : "");

  return 0;
}

int OssHdfsStore::list_dir(std::string_view path, ObjectList &results,
                           std::string *context) {
  results.reserve(opts_.max_list_ret_cnt);
  estring hdfs_path = GET_HDFS_PATH(path);
  return do_list_dir(hdfs_path, results, false, opts_.max_list_ret_cnt,
                     context);
}

int OssHdfsStore::is_dir_empty(std::string_view path, bool &is_empty) {
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "is_dir_empty", path);
  estring hdfs_path = GET_HDFS_PATH(path);
  auto list_options = JindoSDK::createOptions();
  DEFER(JindoSDK::freeOptions(list_options));
  JindoSDK::setOption(list_options, JDO_LIST_OPTS_IS_ITERATIVE, "true");
  JindoSDK::setOption(list_options, JDO_LIST_OPTS_MAX_KEYS, "1");

  auto list_result =
      JindoSDK::listDir(ctx, hdfs_path.c_str(), false, list_options);
  END_CALL();

  if (!list_result) {
    if (error_code == JDO_FILE_NOT_FOUND_ERROR) {
      is_empty = true;
      return 0;
    }
    LOG_ERROR("Failed to check if HDFS directory is empty: `, ec: `, msg: `",
              path, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  is_empty = (JindoSDK::getListDirResultSize(list_result) == 0);
  JindoSDK::freeListDirResult(list_result);
  return 0;
}

int OssHdfsStore::check_bucket(bool allow_auto_create) {
  CHECK_SDK_INIT();
  ObjectList results;
  estring hdfs_path = GET_HDFS_PATH("");
  return do_list_dir(hdfs_path, results, false, 1);
}

int OssHdfsStore::init_jindosdk() {
  START_CALL(jdo_store_, "init", "");
  // The user identity passed to JindoSDK::init() determines the owner of
  // newly created files/directories on the HDFS server.
  char *login_name = getlogin();
  JindoSDK::init(ctx, login_name ? login_name : "root");
  END_CALL();
  return jdo_error_code_to_posix(error_code);
}

static const char kConfigFileKey[] = "sdk.config.file";

static const std::unordered_set<std::string> kReservedSdkKeys = {
    "fs.oss.endpoint",
    "fs.oss.accessKeyId",
    "fs.oss.accessKeySecret",
};

void OssHdfsStore::set_sdk_option(const std::string &key,
                                  const std::string &val) {
  if (kReservedSdkKeys.count(key)) {
    LOG_WARN("ignored reserved JindoSDK option ` (managed internally)", key);
    return;
  }
  JindoSDK::setOption(jdo_opts_, key.c_str(), val.c_str());
  LOG_INFO("applied JindoSDK option: ` = `", key, val);
}

void OssHdfsStore::load_sdk_config_file(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG_WARN("failed to open JindoSDK config file: `", path);
    return;
  }
  LOG_INFO("loading JindoSDK config from: `", path);

  // INI parser: only apply key=value from [jindosdk] section,
  // matching JcomOptions::getConfMap("jindosdk") behavior.
  bool in_sdk_section = true;  // true before any section header
  std::string line;
  while (std::getline(file, line)) {
    auto start = line.find_first_not_of(" \t");
    if (start == std::string::npos) continue;
    line = line.substr(start);
    if (line.empty() || line[0] == '#' || line[0] == '!') continue;

    if (line[0] == '[') {
      auto end = line.find(']');
      if (end == std::string::npos) continue;
      std::string section = line.substr(1, end - 1);
      while (!section.empty() &&
             (section.back() == ' ' || section.back() == '\t'))
        section.pop_back();
      auto sstart = section.find_first_not_of(" \t");
      if (sstart != std::string::npos) section = section.substr(sstart);
      in_sdk_section = (section == "jindosdk");
      continue;
    }

    if (!in_sdk_section) continue;

    auto eq = line.find('=');
    if (eq == std::string::npos || eq == 0) {
      LOG_WARN("invalid line in sdk config file: `", line);
      continue;
    }
    std::string key = line.substr(0, eq);
    std::string val = line.substr(eq + 1);
    while (!key.empty() && (key.back() == ' ' || key.back() == '\t'))
      key.pop_back();
    auto vstart = val.find_first_not_of(" \t");
    if (vstart != std::string::npos) val = val.substr(vstart);
    set_sdk_option(key, val);
  }
}

void OssHdfsStore::apply_sdk_options() {
  const std::string &opts_str = opts_.hdfs_client_options;
  if (opts_str.empty()) return;

  // First pass: find sdk.config.file and load it (lower priority).
  for (auto token : split_string(opts_str, ",")) {
    auto eq = token.find('=');
    if (eq == std::string_view::npos || eq == 0) continue;
    if (token.substr(0, eq) == kConfigFileKey) {
      load_sdk_config_file(std::string(token.substr(eq + 1)));
    }
  }

  for (auto token : split_string(opts_str, ",")) {
    auto eq = token.find('=');
    if (eq == std::string_view::npos || eq == 0) {
      LOG_WARN("invalid sdk option `: expected key=value format",
               std::string(token));
      continue;
    }
    std::string key(token.substr(0, eq));
    std::string val(token.substr(eq + 1));
    if (key == kConfigFileKey) continue;  // already handled
    set_sdk_option(key, val);
  }
}

void OssHdfsStore::rebuild_store(const char *key, const char *key_secret) {
  if (jdo_store_) {
    JindoSDK::destroyStore(jdo_store_);
    JindoSDK::freeStore(jdo_store_);
    jdo_store_ = nullptr;
  }
  if (jdo_opts_) {
    JindoSDK::freeOptions(jdo_opts_);
    jdo_opts_ = nullptr;
  }

  jdo_opts_ = JindoSDK::createOptions();
  JindoSDK::setOption(jdo_opts_, kHdfsSdkOptEndpoint, opts_.endpoint.c_str());
  JindoSDK::setOption(jdo_opts_, kHdfsSdkOptAccessKeyId, key);
  JindoSDK::setOption(jdo_opts_, kHdfsSdkOptAccessKeySecret, key_secret);
  apply_sdk_options();

  jdo_store_ = JindoSDK::createStore(jdo_opts_, uri_.c_str());
}

void OssHdfsStore::set_credentials(ObjCredentials &&creds) {
  if (sdk_initialized_) return;

  // TODO: support sts token.
  LOG_INFO("Retrying Jindo SDK init with new credentials");
  rebuild_store(creds.accessKeyId.c_str(), creds.accessKeySecret.c_str());

  int r = init_jindosdk();
  sdk_initialized_ = (r == 0);
  if (!sdk_initialized_) {
    LOG_ERROR("Jindo SDK init failed after set_credentials");
  }
}

int OssHdfsStore::do_list_dir(std::string_view path, ObjectList &results,
                              bool recursive, int max_keys,
                              std::string *marker) {
  CHECK_SDK_INIT();
  bool fi_err = false;
  FAULT_INJECTION(FI_OssError_Failed_Without_Call, [&] { fi_err = true; });
  if (fi_err) return -EIO;
  START_CALL(jdo_store_, "listDir", path);
  auto list_options = JindoSDK::createOptions();
  DEFER(JindoSDK::freeOptions(list_options));
  JindoSDK::setOption(list_options, JDO_LIST_OPTS_IS_ITERATIVE, "true");
  JindoSDK::setOption(list_options, JDO_LIST_OPTS_MAX_KEYS,
                      std::to_string(max_keys).c_str());
  JindoSDK::setOption(list_options, JDO_LIST_OPTS_MARKER,
                      marker ? marker->c_str() : "");

  auto list_result = JindoSDK::listDir(ctx, std::string(path).c_str(),
                                       recursive, list_options);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to list HDFS directory: `, ec: `, msg: `", path,
              error_code, error_msg);
    if (list_result) JindoSDK::freeListDirResult(list_result);
    return jdo_error_code_to_posix(error_code);
  }

  for (auto i = 0; i < JindoSDK::getListDirResultSize(list_result); i++) {
    JdoFileStatus_t file_info = JindoSDK::getListDirFileStatus(list_result, i);
    if (!file_info) continue;

    const char *hdfs_path = JindoSDK::getFileStatusPath(file_info);
    if (!hdfs_path) continue;

    auto name = get_filename_from_hdfs_uri(hdfs_path);
    if (!is_file_name_valid(name)) {
      LOG_WARN("skipped entry with invalid name ` in list results under `",
               name, path);
      continue;
    }

    auto size = JindoSDK::getFileStatusSize(file_info);
    auto type =
        hdfs_type_to_dirent_type(JindoSDK::getFileStatusType(file_info));
    auto mtime_ms = JindoSDK::getFileStatusMtime(file_info);
    auto atime_ms = JindoSDK::getFileStatusAtime(file_info);
    struct timespec mtime_ts = {mtime_ms / 1000, (mtime_ms % 1000) * 1000000};
    struct timespec atime_ts = {atime_ms / 1000, (atime_ms % 1000) * 1000000};

    auto perm = JindoSDK::getFileStatusPerm(file_info);
    const char *user = JindoSDK::getFileStatusUser(file_info);
    const char *group = JindoSDK::getFileStatusGroup(file_info);

    uid_t uid = username_to_uid(user ? user : "");
    gid_t gid = groupname_to_gid(group ? group : "");

    results.emplace_back(name, size, mtime_ts, type, "", perm, uid, gid,
                         atime_ts);

    FAULT_INJECTION(FI_Readdir_list_Delay,
                    []() { photon::thread_usleep(1000 * 300); });
  }

  if (marker) {
    if (!JindoSDK::isListDirResultTruncated(list_result)) {
      // Not truncated means no more entries, skip next list request.
      *marker = "";
    } else {
      const char *next_marker =
          JindoSDK::getListDirResultNextMarker(list_result);
      *marker = next_marker ? next_marker : "";
    }
  }
  JindoSDK::freeListDirResult(list_result);
  return 0;
}

int OssHdfsStore::do_mkdir(std::string_view path, mode_t mode) {
  estring hdfs_path = GET_HDFS_PATH(path);
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "mkdir", path);
  JindoSDK::mkdir(ctx, hdfs_path.c_str(), true, mode & kPermMask, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to create HDFS directory: `, ec: `, msg: `", hdfs_path,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

int OssHdfsStore::do_create_file(std::string_view path, mode_t mode) {
  RawObjHandle *handle = nullptr;
  int ret = open_object(path, O_WRONLY, mode, &handle);
  if (ret < 0) {
    LOG_ERROR("Failed to create empty HDFS file: `, ret: `", path, ret);
    return ret;
  }
  int r = handle->close();
  delete handle;
  if (r < 0) {
    LOG_ERROR("Failed to close after creating empty HDFS file: `, r: `", path,
              r);
    return r;
  }
  return 0;
}

ssize_t OssHdfsStore::put_object(std::string_view path, const struct iovec *iov,
                                 int iovcnt, uint64_t *expected_crc64,
                                 mode_t mode) {
  std::string path_str(path);

  if (!path_str.empty() && path_str.back() == '/') {
    path_str.pop_back();
    return do_mkdir(path_str, mode);
  }

  // Check if iov has actual data (not just empty iovec).
  if (iov != nullptr && iovcnt > 0) {
    for (int i = 0; i < iovcnt; i++) {
      if (iov[i].iov_base != nullptr && iov[i].iov_len > 0) {
        LOG_ERROR("put_object with data is not supported in HDFS mode");
        return -ENOTSUP;
      }
    }
  }

  return do_create_file(path, mode);
}

int OssHdfsStore::delete_object(std::string_view path) {
  CHECK_SDK_INIT();
  bool fi_err = false;
  FAULT_INJECTION(FI_OssError_Failed_Without_Call, [&] { fi_err = true; });
  if (fi_err) return -EIO;
  START_CALL(jdo_store_, "remove", path);
  estring hdfs_path = GET_HDFS_PATH(path);
  auto remove_options = JindoSDK::createOptions();
  DEFER(JindoSDK::freeOptions(remove_options));

  JindoSDK::remove(ctx, hdfs_path.c_str(), false, remove_options);
  END_CALL();

  if (error_code != 0) {
    if (error_code == JDO_FILE_NOT_FOUND_ERROR) return 0;

    LOG_ERROR("Failed to delete HDFS object: `, ec: `, msg: `", path,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

int OssHdfsStore::rename_object(std::string_view src_path,
                                std::string_view dst_path, bool set_mime,
                                bool dst_exists) {
  CHECK_SDK_INIT();
  estring hdfs_src = GET_HDFS_PATH(src_path);
  estring hdfs_dst = GET_HDFS_PATH(dst_path);

  // HDFS jdo_rename in overwrite mode leaves stale metadata entries,
  // causing subsequent is_dir_empty checks to fail with ENOTEMPTY.
  // Delete dst first, then rename.
  if (dst_exists) {
    bool fi_err = false;
    FAULT_INJECTION(FI_HdfsRename_PreDeleteFail, [&] { fi_err = true; });
    if (fi_err) return -EIO;
    START_CALL(jdo_store_, "pre_remove", dst_path);
    auto remove_options = JindoSDK::createOptions();
    DEFER(JindoSDK::freeOptions(remove_options));
    JindoSDK::remove(ctx, hdfs_dst.c_str(), false, remove_options);
    END_CALL();
    if (error_code != 0 && error_code != JDO_FILE_NOT_FOUND_ERROR) {
      LOG_ERROR("Failed to pre-delete dst for rename: `, ec: `, msg: `",
                dst_path, error_code, error_msg);
      return jdo_error_code_to_posix(error_code);
    }
  }

  START_CALL(jdo_store_, "rename", src_path);
  auto rename_options = JindoSDK::createOptions();
  DEFER(JindoSDK::freeOptions(rename_options));

  JindoSDK::rename(ctx, hdfs_src.c_str(), hdfs_dst.c_str(), rename_options);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to rename HDFS object: ` -> `, ec: `, msg: `", src_path,
              dst_path, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

int OssHdfsStore::rename_dir(std::string_view src_path,
                             std::string_view dst_path, bool dst_exists) {
  CHECK_SDK_INIT();
  estring hdfs_src = GET_HDFS_PATH(src_path);
  estring hdfs_dst = GET_HDFS_PATH(dst_path);

  // Same pre-delete logic as rename_object: avoid HDFS overwrite-mode
  // metadata residue.
  if (dst_exists) {
    START_CALL(jdo_store_, "pre_remove", dst_path);
    auto remove_options = JindoSDK::createOptions();
    DEFER(JindoSDK::freeOptions(remove_options));
    JindoSDK::remove(ctx, hdfs_dst.c_str(), false, remove_options);
    END_CALL();
    if (error_code != 0 && error_code != JDO_FILE_NOT_FOUND_ERROR) {
      LOG_ERROR("Failed to pre-delete dst for rename_dir: `, ec: `, msg: `",
                dst_path, error_code, error_msg);
      return jdo_error_code_to_posix(error_code);
    }
  }

  START_CALL(jdo_store_, "rename_dir", src_path);
  auto rename_options = JindoSDK::createOptions();
  DEFER(JindoSDK::freeOptions(rename_options));

  JindoSDK::rename(ctx, hdfs_src.c_str(), hdfs_dst.c_str(), rename_options);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to rename HDFS directory: ` -> `, ec: `, msg: `",
              src_path, dst_path, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

IObjStore *new_oss_hdfs_store(const char *key, const char *key_secret,
                              const ObjStoreOptions &options) {
  return new OssHdfsStore(key, key_secret, options);
}

OssHdfsStore::OssHdfsRawObjHandle::OssHdfsRawObjHandle(JdoStore_t store,

                                                       JdoIOContext_t io_ctx,
                                                       std::string hdfs_path)
    : store_(store), io_ctx_(io_ctx), path_(std::move(hdfs_path)) {}

OssHdfsStore::OssHdfsRawObjHandle::~OssHdfsRawObjHandle() {
  if (io_ctx_) {
    close();
  }
}

ssize_t OssHdfsStore::OssHdfsRawObjHandle::read(void *buffer, size_t length) {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "read", path_);
  int64_t ret = JindoSDK::read(ctx, (char *)buffer, length, nullptr);
  END_CALL_NO_LOGS();

  if (error_code != 0) {
    LOG_ERROR("Failed to read HDFS stream: `, ec: `, msg: `", path_, error_code,
              error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  if (ret < 0) return 0;
  return ret;
}

ssize_t OssHdfsStore::OssHdfsRawObjHandle::pread(void *buffer, size_t length,
                                                 off_t offset) {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "pread", path_);
  int64_t ret = JindoSDK::pread(ctx, (char *)buffer, length, offset, nullptr);
  END_CALL_NO_LOGS();

  if (error_code != 0) {
    LOG_ERROR("Failed to pread HDFS stream: `, offset: `, ec: `, msg: `", path_,
              offset, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  if (ret < 0) return 0;
  return ret;
}

ssize_t OssHdfsStore::OssHdfsRawObjHandle::write(const void *buffer,
                                                 size_t length) {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "write", path_);
  int64_t ret = JindoSDK::write(ctx, (const char *)buffer, length, nullptr);
  END_CALL_NO_LOGS();

  if (error_code != 0) {
    LOG_ERROR("Failed to write HDFS stream: `, ec: `, msg: `", path_,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return ret;
}

int OssHdfsStore::OssHdfsRawObjHandle::flush() {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "flush", path_);
  JindoSDK::flush(ctx, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to flush HDFS stream: `, ec: `, msg: `", path_,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

int OssHdfsStore::OssHdfsRawObjHandle::close() {
  if (closed_ || !io_ctx_) return 0;

  START_CALL2(store_, io_ctx_, "close", path_);
  JindoSDK::close(ctx, nullptr);
  END_CALL();

  closed_ = true;

  if (error_code != 0) {
    LOG_ERROR("Failed to close HDFS stream: `, ec: `, msg: `", path_,
              error_code, error_msg);
    int r = jdo_error_code_to_posix(error_code);
    JindoSDK::freeIOContext(io_ctx_);
    io_ctx_ = nullptr;
    return r;
  }

  JindoSDK::freeIOContext(io_ctx_);
  io_ctx_ = nullptr;
  return 0;
}

ssize_t OssHdfsStore::OssHdfsRawObjHandle::get_length() {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "getFileLength", path_);
  int64_t ret = JindoSDK::getFileLength(ctx, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to get HDFS file length: `, ec: `, msg: `", path_,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return ret;
}

ssize_t OssHdfsStore::OssHdfsRawObjHandle::tell() {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "tell", path_);
  int64_t ret = JindoSDK::tell(ctx, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to tell HDFS stream position: `, ec: `, msg: `", path_,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return ret;
}

ssize_t OssHdfsStore::OssHdfsRawObjHandle::seek(off_t offset) {
  if (!io_ctx_) return -EINVAL;

  START_CALL2(store_, io_ctx_, "seek", path_);
  JindoSDK::seek(ctx, offset, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to seek HDFS stream: `, offset: `, ec: `, msg: `", path_,
              offset, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

int OssHdfsStore::OssHdfsRawObjHandle::fallocate(off_t offset, off_t length) {
  if (!io_ctx_) return -EINVAL;

  START_CALL(store_, "fallocate", path_);
  JindoSDK::fallocate(ctx, path_.c_str(), static_cast<int64_t>(offset),
                      static_cast<int64_t>(length), 0, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR(
        "Failed to fallocate HDFS file: `, offset: `, length: `, ec: `, msg: `",
        path_, offset, length, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  return 0;
}

int OssHdfsStore::open_object(std::string_view path, int flags, mode_t mode,
                              RawObjHandle **out_handle) {
  *out_handle = nullptr;
  if (!sdk_initialized_) {
    LOG_ERROR("Jindo SDK not initialized, open_object aborted");
    return -EIO;
  }
  estring hdfs_path = GET_HDFS_PATH(path);

  // Filter out flags that Jindo SDK doesn't recognize.
  int posix_flags = flags & ~O_LARGEFILE;

  // Convert POSIX flags to JDO flags.
  // O_RDONLY is 0, so we need to check O_ACCMODE mask.
  int access_mode = posix_flags & O_ACCMODE;
  int32_t jdo_flags = 0;
  if (access_mode == O_RDONLY || access_mode == 0) {
    jdo_flags = JDO_OPEN_FLAG_READ_ONLY;
  } else if (access_mode == O_WRONLY) {
    if (posix_flags & O_APPEND) {
      jdo_flags = JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_APPEND;
    } else {
      // Always include CREATE for write streams.
      jdo_flags = JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_RANDOM_WRITE;
    }
  } else {
    // O_RDWR or any non-standard access mode (e.g., 3 from some FUSE
    // implementations). O_APPEND takes precedence over RANDOM_WRITE.
    if (posix_flags & O_APPEND) {
      jdo_flags = JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_APPEND;
    } else {
      jdo_flags = JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_RANDOM_WRITE;
    }
  }

  // POSIX: O_TRUNC truncates the file to zero length.
  // Delete the existing file first, then open with OVERWRITE.
  if (posix_flags & O_TRUNC) {
    if (!(posix_flags & O_CREAT)) {
      START_CALL(jdo_store_, "trunc_remove", path);
      JindoSDK::remove(ctx, hdfs_path.c_str(), false, nullptr);
      END_CALL();
      if (error_code != 0 && error_code != JDO_FILE_NOT_FOUND_ERROR) {
        LOG_ERROR("Failed to delete before trunc: `, ec: `", path, error_code);
        return jdo_error_code_to_posix(error_code);
      }
    }
    // Also add OVERWRITE for belt-and-suspenders.
    if (!(posix_flags & O_APPEND)) {
      jdo_flags |= JDO_OPEN_FLAG_OVERWRITE;
    }
  }

  START_CALL(jdo_store_, "open", path);
  int16_t perm = static_cast<int16_t>(mode & kPermMask);
  auto io_ctx =
      JindoSDK::open(ctx, hdfs_path.c_str(), jdo_flags, perm, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to open HDFS file: `, error_code: `", path, error_code);
    if (io_ctx) JindoSDK::freeIOContext(io_ctx);
    return jdo_error_code_to_posix(error_code);
  }

  *out_handle = new OssHdfsRawObjHandle(jdo_store_, io_ctx, hdfs_path);
  return 0;
}

int OssHdfsStore::truncate_object(std::string_view path, size_t to_size) {
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "truncate", path);
  estring hdfs_path = GET_HDFS_PATH(path);

  JindoSDK::truncate(ctx, hdfs_path.c_str(), static_cast<int64_t>(to_size),
                     nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("jdo_truncate failed, path: `, to_size: `, ec: `, msg: `", path,
              to_size, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  return 0;
}

int OssHdfsStore::set_permission(std::string_view path, mode_t mode) {
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "setPermission", path);
  estring hdfs_path = GET_HDFS_PATH(path);

  int16_t perm = static_cast<int16_t>(mode & kPermMask);
  JindoSDK::setPermission(ctx, hdfs_path.c_str(), perm, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("jdo_setPermission failed, path: `, mode: 0o`, ec: `, msg: `",
              path, OCT(perm), error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  return 0;
}

int OssHdfsStore::set_owner(std::string_view path, uid_t uid, gid_t gid,
                            int to_set) {
  CHECK_SDK_INIT();
  estring hdfs_path = GET_HDFS_PATH(path);

  std::string user, group;

  if ((to_set & kSetUid) && uid != (uid_t)-1) {
    user = uid_to_username(uid);
  }

  if ((to_set & kSetGid) && gid != (gid_t)-1) {
    group = gid_to_groupname(gid);
  }

  if (user.empty() && group.empty()) return -EINVAL;

  const char *user_ptr = user.empty() ? nullptr : user.c_str();
  const char *group_ptr = group.empty() ? nullptr : group.c_str();

  START_CALL(jdo_store_, "setOwner", path);
  JindoSDK::setOwner(ctx, hdfs_path.c_str(), user_ptr, group_ptr, nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("jdo_setOwner failed, path: `, uid: `, gid: `, ec: `, msg: `",
              path, uid, gid, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  return 0;
}

int OssHdfsStore::set_lock(std::string_view path, int64_t offset,
                           int64_t length, int16_t type, int64_t pid,
                           uint64_t owner) {
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "setLock", path);
  estring hdfs_path = GET_HDFS_PATH(path);

  JdoLockInfo_t lock_info = JindoSDK::createLockInfo();
  JindoSDK::setLockInfoOffset(lock_info, offset);
  JindoSDK::setLockInfoLength(lock_info, length);
  JindoSDK::setLockInfoType(lock_info, type);
  JindoSDK::setLockInfoPid(lock_info, pid);
  JindoSDK::setLockInfoOwner(lock_info, owner);

  JindoSDK::setLock(ctx, hdfs_path.c_str(), lock_info, nullptr);
  JindoSDK::freeLockInfo(lock_info);
  END_CALL();

  if (error_code != 0) {
    // clang-format off
    LOG_ERROR(
        "jdo_setLock failed, path: `, offset: `, len: `, type: `, ec: `, msg: `",
        path, offset, length, type, error_code, error_msg);
    // clang-format on
    return -EIO;
  }

  return 0;
}

int OssHdfsStore::get_lock(std::string_view path, int64_t &offset,
                           int64_t &length, int16_t &type, int64_t &pid,
                           uint64_t owner) {
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "getLock", path);
  estring hdfs_path = GET_HDFS_PATH(path);

  JdoLockInfo_t lock_info = JindoSDK::createLockInfo();
  JindoSDK::setLockInfoOffset(lock_info, offset);
  JindoSDK::setLockInfoLength(lock_info, length);
  JindoSDK::setLockInfoType(lock_info, type);
  JindoSDK::setLockInfoPid(lock_info, pid);
  JindoSDK::setLockInfoOwner(lock_info, owner);

  JdoLockInfo_t result =
      JindoSDK::getLock(ctx, hdfs_path.c_str(), lock_info, nullptr);
  JindoSDK::freeLockInfo(lock_info);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("jdo_getLock failed, path: `, ec: `, msg: `", path, error_code,
              error_msg);
    if (result) JindoSDK::freeLockInfo(result);
    return -EIO;
  }

  if (result) {
    offset = JindoSDK::getLockInfoOffset(result);
    length = JindoSDK::getLockInfoLength(result);
    type = JindoSDK::getLockInfoType(result);
    pid = JindoSDK::getLockInfoPid(result);
    JindoSDK::freeLockInfo(result);
  }

  return 0;
}

static constexpr size_t kMaxXattrNameSize = 255;
static constexpr size_t kMaxXattrValueSize = 65536;

static int get_all_xattrs(JdoStore_t store, std::string_view hdfs_path,
                          JdoXAttrList_t *out_list) {
  *out_list = nullptr;
  std::string path_str(hdfs_path);
  auto start = std::chrono::steady_clock::now();
  JdoHandleCtx_t ctx = JindoSDK::createHandleCtx1(store);
  JdoXAttrList_t list = JindoSDK::getXAttrs(ctx, path_str.c_str(), nullptr);
  int32_t ec = JindoSDK::getHandleCtxErrorCode(ctx);
  JindoSDK::freeHandleCtx(ctx);
  auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count();
  LOG_DEBUG("[HDFS] getXAttrs path: `, ec: `, elapsed: `us", hdfs_path, ec,
            elapsed_us);
  if (ec != 0) {
    JindoSDK::freeXAttrList(list);
    return jdo_error_code_to_posix(ec);
  }
  *out_list = list;
  return 0;
}

// RAII wrapper for strdup'd C strings returned by JindoSDK xattr APIs.
// Zero-copy: holds the original pointer, frees it on scope exit.
struct XAttrStrDeleter {
  void operator()(char *p) const noexcept {
    free(p);
  }
};
using XAttrStr = std::unique_ptr<char, XAttrStrDeleter>;

static bool xattr_exists(JdoXAttrList_t list, std::string_view name) {
  int64_t count = JindoSDK::getXAttrListSize(list);
  for (int64_t i = 0; i < count; i++) {
    JdoXAttr_t xa = JindoSDK::getXAttrsListIterator(list, i);
    if (xa == nullptr) continue;
    XAttrStr xa_name(JindoSDK::getXAttrName(xa));
    int xa_ns = JindoSDK::getXAttrNamespace(xa);
    if (xa_ns == JDO_XATTR_NAMESPACE_USER && xa_name && name == xa_name.get()) {
      return true;
    }
  }
  return false;
}

int OssHdfsStore::set_xattr(std::string_view path, const char *name,
                            const char *value, size_t size, int flags) {
  CHECK_SDK_INIT();
  // Valid flags: 0 (default), XATTR_CREATE(1), XATTR_REPLACE(2).
  // CREATE|REPLACE(3) is semantically contradictory — reject with EINVAL.
  // See https://man7.org/linux/man-pages/man2/setxattr.2.html
  if (flags < 0 || flags > 2) {
    return -EINVAL;
  }

  if (strlen(name) >= kMaxXattrNameSize || size >= kMaxXattrValueSize) {
    return -ERANGE;
  }

  estring hdfs_path = GET_HDFS_PATH(path);

  JdoXAttrList_t existing = nullptr;
  int ret = get_all_xattrs(jdo_store_, hdfs_path, &existing);
  if (ret < 0) {
    LOG_ERROR("set_xattr: getXAttrs failed, path: `, ret: `", path, ret);
    return ret;
  }

  bool exists = xattr_exists(existing, name);
  JindoSDK::freeXAttrList(existing);

  int32_t sdk_flag;
  if (flags == XATTR_CREATE) {
    if (exists) return -EEXIST;
    sdk_flag = JDO_XATTR_FLAG_CREATE;
  } else if (flags == XATTR_REPLACE) {
    if (!exists) return -ENODATA;
    sdk_flag = JDO_XATTR_FLAG_REPLACE;
  } else {
    sdk_flag = JDO_XATTR_FLAG_CREATE | JDO_XATTR_FLAG_REPLACE;
  }

  JdoXAttr_t xattr = JindoSDK::createXAttr();
  JindoSDK::setXAttrNamespace(xattr, JDO_XATTR_NAMESPACE_USER);
  JindoSDK::setXAttrName(xattr, name);
  std::string val_copy(value, size);
  JindoSDK::setXAttrValue(xattr, val_copy.c_str());

  START_CALL(jdo_store_, "setXAttr", path);
  JindoSDK::setXAttr(ctx, hdfs_path.c_str(), xattr, sdk_flag, nullptr);
  JindoSDK::freeXAttr(xattr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("set_xattr: setXAttr failed, path: `, name: `, ec: `, msg: `",
              path, name, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  return 0;
}

int OssHdfsStore::get_xattr(std::string_view path, const char *name,
                            char *value, size_t size) {
  CHECK_SDK_INIT();
  estring hdfs_path = GET_HDFS_PATH(path);

  JdoXAttrList_t list = nullptr;
  int ret = get_all_xattrs(jdo_store_, hdfs_path, &list);
  if (ret < 0) {
    LOG_ERROR("get_xattr: getXAttrs failed, path: `, ret: `", path, ret);
    return ret;
  }

  int64_t count = JindoSDK::getXAttrListSize(list);
  for (int64_t i = 0; i < count; i++) {
    JdoXAttr_t xa = JindoSDK::getXAttrsListIterator(list, i);
    if (xa == nullptr) continue;
    XAttrStr xa_name(JindoSDK::getXAttrName(xa));
    int xa_ns = JindoSDK::getXAttrNamespace(xa);
    if (xa_ns != JDO_XATTR_NAMESPACE_USER || !xa_name ||
        strcmp(xa_name.get(), name) != 0) {
      continue;
    }

    XAttrStr xa_value(JindoSDK::getXAttrValue(xa));
    size_t val_len = xa_value ? strlen(xa_value.get()) : 0;
    if (val_len >= kMaxXattrValueSize) {
      JindoSDK::freeXAttrList(list);
      return -E2BIG;
    }
    if (size == 0) {
      JindoSDK::freeXAttrList(list);
      return static_cast<int>(val_len);
    }
    // POSIX: ERANGE only when buffer is strictly smaller than value.
    // Using '>=' here (off-by-one in some implementations), causing ERANGE when
    // buffer exactly equals value size. See
    // https://man7.org/linux/man-pages/man2/getxattr.2.html.
    if (size < val_len) {
      JindoSDK::freeXAttrList(list);
      return -ERANGE;
    }
    memcpy(value, xa_value.get(), val_len);
    JindoSDK::freeXAttrList(list);
    return static_cast<int>(val_len);
  }

  JindoSDK::freeXAttrList(list);
  return -ENODATA;
}

int OssHdfsStore::list_xattr(std::string_view path, char *list, size_t size) {
  CHECK_SDK_INIT();
  estring hdfs_path = GET_HDFS_PATH(path);

  JdoXAttrList_t xattr_list = nullptr;
  int ret = get_all_xattrs(jdo_store_, hdfs_path, &xattr_list);
  if (ret < 0) {
    LOG_ERROR("list_xattr: getXAttrs failed, path: `, ret: `", path, ret);
    return ret;
  }

  size_t result_size = 0;
  int64_t count = JindoSDK::getXAttrListSize(xattr_list);

  if (size == 0) {
    for (int64_t i = 0; i < count; i++) {
      JdoXAttr_t xa = JindoSDK::getXAttrsListIterator(xattr_list, i);
      if (xa == nullptr) continue;
      XAttrStr xa_name(JindoSDK::getXAttrName(xa));
      if (!xa_name) continue;
      result_size += strlen(xa_name.get()) + 1;
    }
    JindoSDK::freeXAttrList(xattr_list);
    return static_cast<int>(result_size);
  }

  for (int64_t i = 0; i < count; i++) {
    JdoXAttr_t xa = JindoSDK::getXAttrsListIterator(xattr_list, i);
    if (xa == nullptr) continue;
    XAttrStr xa_name(JindoSDK::getXAttrName(xa));
    if (!xa_name) continue;
    size_t name_len = strlen(xa_name.get()) + 1;
    if (result_size + name_len > size) {
      JindoSDK::freeXAttrList(xattr_list);
      return -ERANGE;
    }
    memcpy(list + result_size, xa_name.get(), name_len);
    result_size += name_len;
  }

  JindoSDK::freeXAttrList(xattr_list);
  return static_cast<int>(result_size);
}

int OssHdfsStore::remove_xattr(std::string_view path, const char *name) {
  CHECK_SDK_INIT();
  estring hdfs_path = GET_HDFS_PATH(path);

  JdoXAttrList_t existing = nullptr;
  int ret = get_all_xattrs(jdo_store_, hdfs_path, &existing);
  if (ret < 0) {
    LOG_ERROR("remove_xattr: getXAttrs failed, path: `, ret: `", path, ret);
    return ret;
  }

  if (!xattr_exists(existing, name)) {
    JindoSDK::freeXAttrList(existing);
    return -ENODATA;
  }
  JindoSDK::freeXAttrList(existing);

  JdoXAttr_t xattr = JindoSDK::createXAttr();
  JindoSDK::setXAttrNamespace(xattr, JDO_XATTR_NAMESPACE_USER);
  JindoSDK::setXAttrName(xattr, name);

  START_CALL(jdo_store_, "removeXAttr", path);
  JindoSDK::removeXAttr(ctx, hdfs_path.c_str(), xattr, nullptr);
  JindoSDK::freeXAttr(xattr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR(
        "remove_xattr: removeXAttr failed, path: `, name: `, ec: `, msg: `",
        path, name, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  return 0;
}

int OssHdfsStore::check_permission(PermOp op, const struct stat *st,
                                   uid_t caller_uid, gid_t caller_gid) {
  switch (op) {
    // === Operations with real permission checks ===
    case PermOp::Chmod:
      if (caller_uid != 0 && caller_uid != st->st_uid) return -EPERM;
      return 0;

    case PermOp::Truncate:
    case PermOp::Ftruncate:
      if (caller_uid != st->st_uid) return -EACCES;
      return 0;

    case PermOp::Unlink:
    case PermOp::Utimensat:
      return check_hdfs_access(st, W_OK, caller_uid, caller_gid);

    // === Operations not checked at store layer (return 0) ===
    case PermOp::Open:
    case PermOp::Chown:
    case PermOp::Setxattr:
    case PermOp::Mkdir:
    case PermOp::Rmdir:
    case PermOp::Mknod:
    case PermOp::Create:
    case PermOp::Link:
    case PermOp::Symlink:
    case PermOp::Rename:
    default:
      return 0;
  }
}

int OssHdfsStore::set_times(std::string_view path, int64_t mtime_ms,
                            int64_t atime_ms) {
  CHECK_SDK_INIT();
  START_CALL(jdo_store_, "setTimes", path);
  estring hdfs_path = GET_HDFS_PATH(path);
  JindoSDK::setTimes(ctx, hdfs_path.c_str(), mtime_ms, atime_ms, nullptr);
  END_CALL();
  if (error_code != 0) {
    LOG_ERROR("Failed to set times: `, ec: `, msg: `", path, error_code,
              error_msg);
    return jdo_error_code_to_posix(error_code);
  }
  return 0;
}

ssize_t OssHdfsStore::put_symlink(std::string_view path,
                                  std::string_view target) {
  if (!opts_.enable_symlink) return -EOPNOTSUPP;
  CHECK_SDK_INIT();

  // Normalize target relative to symlink's parent directory.
  // e.g. path="/a/b/link", target="../other/file"
  //      -> base = "a/b", normalized = "a/other/file"
  std::filesystem::path target_path(target);
  std::filesystem::path base(path.substr(1));  // strip leading '/'
  auto normalized = (base.parent_path() / target_path).lexically_normal();
  auto hdfs_target = normalized.string();

  LOG_DEBUG("put hdfs symlink ` -> `(`)", path, hdfs_target, target);

  // Reject targets that escape the mount root.
  if ((hdfs_target.size() >= 3 && hdfs_target.substr(0, 3) == "../") ||
      hdfs_target == "..") {
    LOG_ERROR("invalid hdfs symlink target ` -> `(`)", path, hdfs_target,
              target);
    return -EINVAL;
  }

  START_CALL(jdo_store_, "createSymlink", path);
  estring link_uri = GET_HDFS_PATH(path);
  estring target_uri = GET_HDFS_PATH("/" + hdfs_target);
  JindoSDK::createSymlink(ctx, target_uri.c_str(), link_uri.c_str(), false,
                          nullptr);
  END_CALL();

  if (error_code != 0) {
    LOG_ERROR("Failed to create symlink: ` -> `, ec: `, msg: `", path,
              hdfs_target, error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  // Return the length of the symlink target (internal path without URI prefix).
  return static_cast<ssize_t>(hdfs_target.size());
}

int OssHdfsStore::get_symlink(std::string_view path, std::string &target) {
  if (!opts_.enable_symlink) return -EOPNOTSUPP;
  CHECK_SDK_INIT();

  START_CALL(jdo_store_, "getLinkTarget", path);
  estring hdfs_path = GET_HDFS_PATH(path);
  char *raw_target = JindoSDK::getLinkTarget(ctx, hdfs_path.c_str(), nullptr);
  END_CALL();

  if (error_code != 0) {
    if (raw_target) free(raw_target);
    LOG_ERROR("Failed to get symlink target: `, ec: `, msg: `", path,
              error_code, error_msg);
    return jdo_error_code_to_posix(error_code);
  }

  if (!raw_target) {
    return -EIO;
  }

  // raw_target is a full HDFS URI like "oss://bucket.endpoint/a/b/target".
  // Strip the uri_ prefix to get the mount-internal path.
  std::string target_uri(raw_target);
  free(raw_target);

  // Fault injection: simulate backend returning an unrecognizable target URI.
  FAULT_INJECTION(FI_HdfsSymlink_BadTarget, [&] {
    target_uri = "oss://wrong-bucket.invalid/bad/target";
  });
  // Fault injection: simulate backend returning a URI that escapes mount root.
  FAULT_INJECTION(FI_HdfsSymlink_EscapePath,
                  [&] { target_uri = uri_ + "/../../etc/passwd"; });

  if (target_uri.size() < uri_.size() ||
      target_uri.substr(0, uri_.size()) != uri_) {
    LOG_ERROR("symlink target URI ` does not start with expected prefix `",
              target_uri, uri_);
    return -EIO;
  }

  // "oss://bucket.endpoint/a/b/target" -> "/a/b/target" -> "a/b/target"
  std::string internal_path = target_uri.substr(uri_.size());
  if (!internal_path.empty() && internal_path.front() == '/') {
    internal_path.erase(0, 1);
  }

  // Both paths relative to mount root (strip leading '/').
  // path: "/a/c/link" -> "a/c/link", parent: "a/c"
  // internal_path: "a/c/target"
  // result: path("a/c/target").lexically_relative(path("a/c")) -> "target"
  auto parent_dir = std::filesystem::path(path.substr(1)).parent_path();
  auto result = std::filesystem::path(internal_path)
                    .lexically_relative(parent_dir)
                    .string();

  if (result.empty() || result.size() > PATH_MAX) {
    LOG_ERROR("symlink target conversion failed: ` -> `", path, target_uri);
    return -EIO;
  }

  target = std::move(result);
  return 0;
}

}  // namespace OssFileSystem
