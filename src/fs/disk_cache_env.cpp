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

#include "disk_cache_env.h"

#include <sys/file.h>

#include <cstring>

#include "common/utils.h"

namespace OssFileSystem {

int DiskCacheEnv::init() {
  static constexpr size_t kAlignment = 4096;
  io_alloc = new AlignedAlloc(kAlignment);
  std::error_code ec;
  // cache-data/ holds the cached data; the lock file sits at the root.
  data_dir = join_paths(options.cache_dir, kDiskCacheDataSubDir);
  // Create_directories creates parents recursively and returns false (not an
  // error) if the dir exists.
  std::filesystem::create_directories(data_dir, ec);
  if (ec) {
    LOG_ERROR("Failed to create cache data dir `, error `", data_dir,
              ec.message());
    return -1;
  }

  if (acquire_dir_exclusive_lock(options.cache_dir) != 0) {
    return -1;
  }

  auto local_fs =
      photon::fs::new_localfs_adaptor(data_dir.c_str(), options.io_engine_type);
  if (local_fs == nullptr) {
    LOG_ERRNO_RETURN(0, -1, "Failed to new localfs adaptor for dir `",
                     data_dir);
  }
  local_xattr_fs = dynamic_cast<photon::fs::IFileSystemXAttr *>(local_fs);
  RELEASE_ASSERT(local_xattr_fs != nullptr);
  if (!probe_xattr_support(local_fs, local_xattr_fs)) {
    delete local_xattr_fs;
    local_xattr_fs = nullptr;
    LOG_ERRNO_RETURN(0, -1, "Failed to probe xattr support, dir `", data_dir);
  }

  local_fs = photon::fs::new_aligned_fs_adaptor(local_fs, kAlignment, true,
                                                true, io_alloc);
  cache_fs = photon::fs::new_full_file_cached_fs(
      nullptr, local_fs, options.cache_refill_unit, options.cache_size_in_GB,
      30'000'000 /* recycle interval (30 s) */, options.disk_available_space,
      io_alloc, 0, nullptr);
  if (cache_fs == nullptr) {
    delete local_fs;
    local_xattr_fs = nullptr;
    LOG_ERRNO_RETURN(0, -1, "Failed to new full file cached fs, dir `",
                     data_dir);
  }
  return 0;
}

DiskCacheEnv::~DiskCacheEnv() {
  // Cache fs destructor: cache_fs → aligned_fs → local_fs(local_xattr_fs).
  delete cache_fs;
  delete io_alloc;
  // Closing the fd releases the flock.
  if (dir_lock_fd >= 0) {
    ::close(dir_lock_fd);
    dir_lock_fd = -1;
  }
}

int DiskCacheEnv::acquire_dir_exclusive_lock(const std::string &dir) {
  // The lock file sits at the cache dir root, outside cache-data, so the
  // caching mechanism never cleans it up.
  static constexpr const char *kDiskCacheLockFileName = ".ossfs2.lock";
  const std::string lock_path = join_paths(dir, kDiskCacheLockFileName);
  dir_lock_fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (dir_lock_fd < 0) {
    if (errno == EACCES) {
      LOG_ERRNO_RETURN(
          0, -1,
          "Failed to open disk cache lock file `, no write permission on "
          "cache dir `",
          lock_path, dir);
    }
    LOG_ERRNO_RETURN(0, -1, "Failed to open disk cache lock file `", lock_path);
  }
  if (::flock(dir_lock_fd, LOCK_EX | LOCK_NB) != 0) {
    // Best effort: read the holder pid recorded in the lock file. The kernel
    // releases flock automatically when the holder exits (even abnormally),
    // so the pid here is diagnostics only, never used for staleness checks.
    int err = errno;
    char buf[32] = {0};
    ::pread(dir_lock_fd, buf, sizeof(buf) - 1, 0);
    errno = err;
    // clang-format off
    LOG_ERRNO_RETURN(
        0, -1,
        "Failed to acquire exclusive lock on `, holder pid `, another instance may be using the same disk cache dir",
        dir, trim_string_view(std::string_view(buf)));
    // clang-format on
  }
  // Record our pid for diagnostics.
  char pid_buf[32];
  int len = snprintf(pid_buf, sizeof(pid_buf), "%d\n", getpid());
  ::ftruncate(dir_lock_fd, 0);
  if (::pwrite(dir_lock_fd, pid_buf, len, 0) != len) {
    LOG_WARN("Failed to write pid into disk cache lock file");
  }
  LOG_DEBUG("Acquired exclusive lock on `", lock_path);
  return 0;
}

bool DiskCacheEnv::probe_xattr_support(photon::fs::IFileSystem *fs,
                                       photon::fs::IFileSystemXAttr *xattr_fs) {
  static constexpr const char *kProbeFile = "/.ossfs2_xattr_probe";
  static constexpr const char *kProbeXattrKey = "trusted.ossfs2.xattr_probe";
  static constexpr const char *kProbeXattrValue = "test/123_456";

  // Check if the probe file already exists before creating it.
  std::string full_path = data_dir + kProbeFile;
  bool file_existed = (::access(full_path.c_str(), F_OK) == 0);

  auto file = fs->open(kProbeFile, O_CREAT | O_RDWR, 0644);
  if (file == nullptr) {
    LOG_ERRNO_RETURN(0, false, "Failed to create xattr probe file ` under `",
                     kProbeFile, data_dir);
  }
  // The file will be closed implicitly via `delete file`.
  delete file;
  // Only remove the probe file if it was newly created by us.
  DEFER(if (!file_existed) fs->unlink(kProbeFile));

  int ret = xattr_fs->setxattr(kProbeFile, kProbeXattrKey, kProbeXattrValue,
                               strlen(kProbeXattrValue), 0);
  if (ret != 0) {
    LOG_ERRNO_RETURN(0, false, "Failed to setxattr for ` under `", kProbeFile,
                     data_dir);
  }
  return true;
}

}  // namespace OssFileSystem
