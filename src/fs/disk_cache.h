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

#include <photon/fs/cache/cache.h>
#include <photon/fs/filesystem.h>

#include <string_view>

#include "bg_vcpu_env.h"
#include "cache.h"
#include "mem_pool.h"

namespace OssFileSystem {

class DiskCacheStore;

class DiskCache : public ICache {
 public:
  DiskCache(BGVCpuDiskCacheEnv *env,
            std::shared_ptr<FixedBlockMemoryPool> memory_pool)
      : env_(env),
        memory_pool_(std::move(memory_pool)),
        dispatch_to_bg_vcpu_(env->io_engine_type_ !=
                             photon::fs::ioengine_psync) {}

  ~DiskCache();

  size_t block_size() const override {
    return memory_pool_->block_size();
  }

  CacheHandle *get(std::string_view name, std::string_view etag,
                   size_t size = 0) override;
  void release(CacheHandle *h, uint64_t count) override;

  size_t capacity() override {
    return std::numeric_limits<size_t>::max();
  }

  size_t try_expand_blocks(uint64_t count, uint64_t max_capacity,
                           bool ignore_limit) override {
    return count;
  }

 private:
  std::mutex mtx_;

  BGVCpuDiskCacheEnv *env_ = nullptr;
  std::shared_ptr<FixedBlockMemoryPool> memory_pool_ = nullptr;

  // True when disk cache requests are dispatched to a background executor
  // (libaio); false when they run inline on the caller vCPU (psync).
  bool dispatch_to_bg_vcpu_ = false;

  int ref_cnt_ = 0;
  DiskCacheStore *cache_store_ = nullptr;
  RangeLock range_lock_;

  friend class DiskCacheStore;
};

}  // namespace OssFileSystem
