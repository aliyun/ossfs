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

#include "disk_cache.h"

#include <sys/xattr.h>
#include <unistd.h>

#include "common/fault_injector.h"
#include "common/macros.h"
#include "common/utils.h"
#include "error_codes.h"
#include "inode.h"

#define DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(__env, __func, ...)       \
  ({                                                                   \
    auto __exec = __env->get_executor();                               \
    auto __r = __exec->perform([&]() { return __func(__VA_ARGS__); }); \
    __r;                                                               \
  })

namespace OssFileSystem {

class DiskCacheStore : public ICacheStore {
 public:
  DiskCacheStore(DiskCache *cache) : cache_(cache) {}
  ~DiskCacheStore();

  int init(std::string_view key, std::string_view source_key);

  ssize_t pin(off_t offset, size_t count, void **buf) override {
    return -ENOTSUP;
  }

  void unpin(off_t offset) override {
    std::abort();
  }

  ssize_t pread(char *buf, off_t offset, size_t count) override;
  std::pair<off_t, size_t> query_refill_range(off_t offset,
                                              size_t count) override;
  void drop() override;

  int acquire_write_buffer(RangeBuffer &range_buffer) override;
  void release_write_buffer(const RangeBuffer &range_buffer,
                            bool evict = false) override;

  void set_actual_size(off_t size) {
    local_store_->set_actual_size(size);
  }

 private:
  BGVCpuDiskCacheEnv *get_env() const;
  int validate_source_key(std::string_view source_key) const;
  void release_store();

  DiskCache *cache_ = nullptr;

  photon::fs::ICacheStore *local_store_ = nullptr;
  int raw_fd_ = -1;
  bool psync_mode_ = false;
};

BGVCpuDiskCacheEnv *DiskCacheStore::get_env() const {
  return cache_->env_;
}

int DiskCacheStore::init(std::string_view key, std::string_view source_key) {
  int r = 0;
  FAULT_INJECTION(FI_DiskCache_Init_Failure, [&]() { r = -1; });
  if (r != 0) return r;

  auto cache_pool = get_env()->get_disk_cache_pool();
  local_store_ = DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(
      get_env(), cache_pool->open, key, O_CREAT | O_RDWR | O_CACHE_ONLY, 0644);
  if (local_store_ == nullptr) {
    LOG_ERRNO_RETURN(0, -EIO, "Failed to open cache key: `", key);
  }

  // Check psync mode and open raw fd for direct read bypass.
  psync_mode_ = (get_env()->io_engine_type_ == photon::fs::ioengine_psync);
  if (psync_mode_) {
    // TODO: get raw fd from cache store.
    auto store_key = local_store_->get_store_key();
    auto full_path = join_paths(get_env()->disk_cache_env->options.cache_dir,
                                std::string(store_key));
    raw_fd_ = ::open(full_path.c_str(), O_RDONLY);
    if (raw_fd_ < 0) {
      LOG_WARN("Failed to open raw fd for psync bypass: `, errno: `", full_path,
               errno);
    }
  }

  auto ret = validate_source_key(source_key);
  if (ret != 0) {
    release_store();
    return ret;
  }
  return 0;
}

int DiskCacheStore::validate_source_key(std::string_view source_key) const {
  static constexpr const char *kXattrSourceKey = "trusted.ossfs2.source_key";
  static constexpr int kXattrSourceKeyMaxLen = 2048;
  RELEASE_ASSERT(source_key.size() < kXattrSourceKeyMaxLen);
  auto xattr_fs = get_env()->get_disk_cache_xattr_fs();
  auto store_key = local_store_->get_store_key();
  char xattr_buf[kXattrSourceKeyMaxLen] = {0};
  auto xattr_len = xattr_fs->getxattr(store_key.data(), kXattrSourceKey,
                                      xattr_buf, kXattrSourceKeyMaxLen - 1);

  if (xattr_len > 0) {
    // Xattr exists, check for collision.
    if (std::string_view(xattr_buf, xattr_len) != source_key) {
      LOG_WARN("Cache key collision, key: `. Expected: `, cached: `", store_key,
               source_key, std::string_view(xattr_buf, xattr_len));
      return -E_DISK_CACHE_COLLISION;
    }
  } else {
    // Xattr not set, try to claim this cache file atomically.
    auto ret =
        xattr_fs->setxattr(store_key.data(), kXattrSourceKey, source_key.data(),
                           source_key.size(), XATTR_CREATE);
    if (ret != 0) {
      auto saved_errno = errno;
      if (saved_errno == EEXIST) {
        // Race: another inode claimed it between getxattr and setxattr.
        return -E_DISK_CACHE_COLLISION;
      }

      LOG_ERROR("Failed to claim cache key, errno: `", saved_errno);
      return -EIO;
    }
  }
  return 0;
}

DiskCacheStore::~DiskCacheStore() {
  release_store();
}

void DiskCacheStore::release_store() {
  if (raw_fd_ >= 0) {
    ::close(raw_fd_);
    raw_fd_ = -1;
  }
  if (local_store_ != nullptr) {
    auto release_func = [this]() {
      if (psync_mode_) {
        // 0-byte read to trigger LRU touch on close in psync mode.
        iovec touch_iov{nullptr, 0};
        local_store_->do_preadv2(&touch_iov, 0, 0, RW_V2_CACHE_ONLY);
      }
      local_store_->release();
      return 0;
    };
    DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(get_env(), release_func);
    local_store_ = nullptr;
  }
}

ssize_t DiskCacheStore::pread(char *buf, off_t offset, size_t count) {
  iovec iov{buf, count};
  if (psync_mode_ && raw_fd_ >= 0) {
    auto range = query_refill_range(offset, count);
    auto cache_hit = (range.second == 0);
    if (cache_hit) {
      ssize_t ret = ::preadv(raw_fd_, &iov, 1, offset);
      if (likely(ret == (ssize_t)count)) {
        return ret;
      } else {
        LOG_WARN("read cache file failed, offset `, count `, got `", offset,
                 count, ret);
        return -ENOENT;
      }
    }
  }

  auto ret = DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(
      get_env(), local_store_->try_preadv2, &iov, 1, offset, RW_V2_CACHE_ONLY);
  return ret.refill_size == 0 ? ret.size : -ENOENT;
}

std::pair<off_t, size_t> DiskCacheStore::query_refill_range(off_t offset,
                                                            size_t count) {
  return DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(
      get_env(), local_store_->queryRefillRange, offset, count);
}

void DiskCacheStore::drop() {
  auto cache_pool = get_env()->get_disk_cache_pool();
  DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(get_env(), cache_pool->evict,
                                       local_store_->get_store_key());
}

int DiskCacheStore::acquire_write_buffer(RangeBuffer &range_buffer) {
  auto pool = cache_->memory_pool_;
  auto block_size = cache_->block_size();
  auto block_num = (range_buffer.count + block_size - 1) / block_size;
  if (unlikely(block_num == 0)) return 0;
  auto alloc_ptrs = pool->allocate(block_num);
  if (unlikely(alloc_ptrs.size() != block_num)) {
    pool->deallocate(alloc_ptrs);
    return -ENOSPC;
  }

  range_buffer.buffer.resize(block_num);
  for (size_t i = 0; i < block_num; ++i) {
    range_buffer.buffer[i] = {alloc_ptrs[i], block_size};
  }
  size_t last_iov_len = range_buffer.count - (block_num - 1) * block_size;
  range_buffer.buffer.back().iov_len = last_iov_len;
  return 0;
}

void DiskCacheStore::release_write_buffer(const RangeBuffer &range_buffer,
                                          bool evict) {
  auto &buffer = range_buffer.buffer;
  DEFER({
    std::vector<char *> free_ptrs;
    free_ptrs.reserve(buffer.iovcnt());
    for (size_t i = 0; i < buffer.iovcnt(); ++i) {
      free_ptrs.push_back(static_cast<char *>(buffer[i].iov_base));
    }
    cache_->memory_pool_->deallocate(free_ptrs);
  });

  if (evict) return;

  auto ret = DO_SYNC_BACKGROUND_DISKCACHE_REQUEST(
      get_env(), local_store_->do_pwritev2, buffer.iovec(), buffer.iovcnt(),
      range_buffer.offset, 0);
  if (ret != static_cast<ssize_t>(buffer.sum())) {
    LOG_WARN("Failed to write buffer, offset: `, count: `, ret: `, errno: `",
             range_buffer.offset, range_buffer.count, ret, errno);
  }
}

DiskCache::~DiskCache() {
  std::lock_guard<std::mutex> l(mtx_);
  RELEASE_ASSERT(ref_cnt_ == 0);
  RELEASE_ASSERT(cache_store_ == nullptr);
}

CacheHandle *DiskCache::get(std::string_view name, std::string_view etag,
                            off_t actual_size) {
  auto source_key = std::string(name) + "/" + etag;
  auto cache_key = "/" + cityhash128_base64url(source_key);
  // Split the first 3 characters to construct the prefix directory,
  // to avoid flat directory structure.
  cache_key.insert(4, "/");

  FAULT_INJECTION(FI_DiskCache_Key_Collision, [&]() {
    auto hash = std::hash<std::string>{}(source_key);
    cache_key = "/" + std::to_string(hash % 5);
  });

  std::lock_guard<std::mutex> l(mtx_);
  if (cache_store_ == nullptr) {
    cache_store_ = new DiskCacheStore(this);
    int ret = 0;
    // Maximum retry attempts when cache key collision is detected.
    static constexpr int kMaxCollisionAttempts = 5;
    for (int attempt = 0; attempt < kMaxCollisionAttempts; ++attempt) {
      ret = cache_store_->init(cache_key, source_key);
      if (ret == -E_DISK_CACHE_COLLISION) {
        // '+' will not appear in the cache key (base64url).
        cache_key += "+";
      } else {
        break;
      }
    }

    if (unlikely(ret != 0)) {
      LOG_ERROR("Failed to init disk cache for `, key: `, ret: `", name,
                cache_key, ret);
      delete cache_store_;
      cache_store_ = nullptr;
      return nullptr;
    }
    cache_store_->set_actual_size(actual_size);
    LOG_DEBUG("Init disk cache for `, key: `", name, cache_key);
  }
  ++ref_cnt_;
  return new CacheHandle(cache_store_, &range_lock_);
}

void DiskCache::release(CacheHandle *h, uint64_t count) {
  delete h;
  DiskCacheStore *cache_ptr = nullptr;
  {
    std::lock_guard<std::mutex> l(mtx_);
    if (cache_store_ != nullptr) {
      RELEASE_ASSERT(ref_cnt_ > 0);
      if (--ref_cnt_ == 0) {
        // Transfer pointer ownership to local variable for destruction
        // outside the lock.
        cache_ptr = cache_store_;
        cache_store_ = nullptr;
      }
    }
  }

  if (cache_ptr != nullptr) {
    delete cache_ptr;
  }
}

}  // namespace OssFileSystem
