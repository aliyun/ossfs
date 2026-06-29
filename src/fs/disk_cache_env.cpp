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

#include <cstring>

namespace OssFileSystem {

static constexpr size_t kAlignment = 4096;

int DiskCacheEnv::init() {
  io_alloc = new AlignedAlloc(kAlignment);
  std::error_code ec;
  const auto &dir = options.cache_dir;
  // Create_directories returns false (not an error) if the dir exists.
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    LOG_ERROR("Failed to create cache dir `, error `", dir, ec.message());
    return -1;
  }

  auto local_fs =
      photon::fs::new_localfs_adaptor(dir.c_str(), options.io_engine_type);
  if (local_fs == nullptr) {
    LOG_ERRNO_RETURN(0, -1, "Failed to new localfs adaptor for dir `", dir);
  }
  local_xattr_fs = dynamic_cast<photon::fs::IFileSystemXAttr *>(local_fs);
  RELEASE_ASSERT(local_xattr_fs != nullptr);
  if (!probe_xattr_support(local_fs, local_xattr_fs)) {
    delete local_xattr_fs;
    local_xattr_fs = nullptr;
    LOG_ERRNO_RETURN(0, -1, "Failed to probe xattr support, dir `", dir);
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
    LOG_ERRNO_RETURN(0, -1, "Failed to new full file cached fs, dir `", dir);
  }
  return 0;
}

DiskCacheEnv::~DiskCacheEnv() {
  // Cache fs destructor: cache_fs → aligned_fs → local_fs(local_xattr_fs).
  delete cache_fs;
  delete io_alloc;
}

bool DiskCacheEnv::probe_xattr_support(photon::fs::IFileSystem *fs,
                                       photon::fs::IFileSystemXAttr *xattr_fs) {
  static constexpr const char *kProbeFile = "/tmp/.ossfs2_xattr_probe";
  static constexpr const char *kProbeXattrKey = "trusted.ossfs2.xattr_probe";
  static constexpr const char *kProbeXattrValue = "test/123_456";

  // Check if the probe file already exists before creating it.
  std::string full_path = options.cache_dir + kProbeFile;
  bool file_existed = (::access(full_path.c_str(), F_OK) == 0);
  int ret = fs->mkdir("/tmp", 0755);
  if (ret != 0 && errno != EEXIST) {
    LOG_ERRNO_RETURN(0, false, "Failed to mkdir `/tmp", options.cache_dir);
  }

  auto file = fs->open(kProbeFile, O_CREAT | O_RDWR, 0644);
  if (file == nullptr) {
    LOG_ERRNO_RETURN(0, false, "Failed to create xattr probe file ` under `",
                     kProbeFile, options.cache_dir);
  }
  // The file will be closed implicitly via `delete file`.
  delete file;
  // Only remove the probe file if it was newly created by us.
  DEFER(if (!file_existed) fs->unlink(kProbeFile));

  ret = xattr_fs->setxattr(kProbeFile, kProbeXattrKey, kProbeXattrValue,
                           strlen(kProbeXattrValue), 0);
  if (ret != 0) {
    LOG_ERRNO_RETURN(0, false, "Failed to setxattr for ` under `", kProbeFile,
                     options.cache_dir);
  }
  return true;
}

}  // namespace OssFileSystem
