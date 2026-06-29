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
#include <photon/fs/aligned-file.h>
#include <photon/fs/cache/cache.h>
#include <photon/fs/localfs.h>
#include <photon/io/aio-wrapper.h>
#include <unistd.h>

#include <filesystem>

#include "common/logger.h"
#include "common/macros.h"

namespace OssFileSystem {

struct DiskCacheOptions {
  DiskCacheOptions(std::string_view cache_dir, uint64_t cache_size_in_GB,
                   uint64_t cache_refill_unit = 1024 * 1024,
                   int io_engine_type = photon::fs::ioengine_libaio,
                   uint64_t disk_available_space = 1 << 30)
      : cache_dir(cache_dir),
        cache_size_in_GB(cache_size_in_GB),
        cache_refill_unit(cache_refill_unit),
        io_engine_type(io_engine_type),
        disk_available_space(disk_available_space) {}

  std::string cache_dir;
  uint64_t cache_size_in_GB = 0;
  uint64_t cache_refill_unit = 1024 * 1024;
  int io_engine_type = photon::fs::ioengine_libaio;
  uint64_t disk_available_space = 1 << 30;
};

struct DiskCacheEnv {
  explicit DiskCacheEnv(const DiskCacheOptions &opts) : options(opts) {}

  int init();

  ~DiskCacheEnv();

  bool probe_xattr_support(photon::fs::IFileSystem *fs,
                           photon::fs::IFileSystemXAttr *xattr_fs);

  DiskCacheOptions options;

  photon::fs::ICachedFileSystem *cache_fs = nullptr;
  photon::fs::IFileSystemXAttr *local_xattr_fs = nullptr;
  IOAlloc *io_alloc = nullptr;
};

}  // namespace OssFileSystem
