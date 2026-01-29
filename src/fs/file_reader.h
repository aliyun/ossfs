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

#include "common/filesystem.h"
#include "file_prefetching.h"
#include "mem_cache.h"
#include "test/class_declarations.h"

namespace OssFileSystem {

class FileInode;
class OssFs;

class IReader {
 public:
  virtual ~IReader() = default;

  // With inode rlock held outside.
  virtual ssize_t pread_rlocked(void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t pin_rlocked(off_t offset, size_t count, void **buf) = 0;

  // With inode wlock held outside.
  virtual int close() = 0;

  // No inode lock is required.
  virtual ssize_t pread(void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t pin(off_t offset, size_t count, void **buf) = 0;
  virtual void unpin(off_t offset) = 0;
  virtual std::string_view get_path() = 0;
};

class OssCachedReader final : public IReader,
                              public EnableFilePrefetching<OssCachedReader> {
 public:
  OssCachedReader(OssFs *fs, std::string_view path, FileInode *inode);

  ssize_t pread_rlocked(void *buf, size_t count, off_t offset) override;
  ssize_t pread(void *buf, size_t count, off_t offset) override;

  ssize_t pin_rlocked(off_t offset, size_t count, void **buf) override;
  ssize_t pin(off_t offset, size_t count, void **buf) override;

  void unpin(off_t offset) override;

  // With inode lock held outside.
  int close() override;

  virtual ~OssCachedReader();

  std::string_view get_path() override {
    return path_;
  }

 private:
  ssize_t bg_try_refill_range(OssAdapter *oss_client, off_t offset,
                              size_t count);

  void try_expand_prefetch_window(off_t remote_size);
  bool has_enough_space(size_t size);

  uint64_t get_prefetch_alignment();

  void set_latest_read_off(off_t offset);

  // Returns the number of allocated blocks.
  size_t try_realloc_cache_blocks(uint64_t new_total_blocks,
                                  bool from_bg_prefetch = false);

  ssize_t do_refill_range(OssAdapter *oss_client, uint64_t refill_off,
                          uint64_t refill_size, size_t count, char *input,
                          off_t offset, bool from_bg_prefetch);

  ssize_t do_pread(void *buf, size_t count, off_t offset, size_t refill_unit);

  // Needs inode's rlock.
  ssize_t read_from_dirty_inode(void *buf, size_t count, off_t offset);

  off_t get_remote_size();
  void set_remote_size(off_t size);

  bool refresh_attr_if_needed_and_invoke(std::function<void()> &&callback);

  std::shared_ptr<BlockCacheManager> cache_manager_;
  BlockCache *cache_ = nullptr;
  RangeLock *range_lock_ = nullptr;

  // Tracks the number of cache blocks allocated to this FileHandle.
  // These blocks are returned to the cache_manager_ when the file is closed.
  // And the prefetch window size is determined by this value.
  uint64_t total_blocks_ = 0;

  photon::spinlock attr_lock_;
  // remote_size is a cached copy of the inode file size, stored per file
  // handle. It is primarily used in prefetching and caching code paths to avoid
  // repeatedly acquiring the inode lock. The following value is only updated
  // during refresh_attr_if_needed_and_invoke.
  off_t remote_size_ = 0;
  timespec mtime_ = {0, 0};

  FileInode *inode_ = nullptr;
  const std::string path_;

  friend class EnableFilePrefetching<OssCachedReader>;

  DECLARE_TEST_FRIENDS_CLASSES;
};

class OssDirectReader final : public IReader {
 public:
  OssDirectReader(OssFs *fs, std::string_view path, FileInode *inode);

  ssize_t pread_rlocked(void *buf, size_t count, off_t offset) override;
  ssize_t pread(void *buf, size_t count, off_t offset) override;

  UNIMPLEMENTED(ssize_t pin_rlocked(off_t offset, size_t count, void **buf)
                    override);
  UNIMPLEMENTED(ssize_t pin(off_t offset, size_t count, void **buf) override);

  void unpin(off_t offset) override{};

  int close() override {
    return 0;
  }

  std::string_view get_path() override {
    return path_;
  }

 private:
  OssFs *fs_ = nullptr;
  FileInode *inode_ = nullptr;
  const std::string path_;
  std::atomic<off_t> file_size_ = {0};
};

std::unique_ptr<IReader> create_oss_reader(OssFs *fs, std::string_view path,
                                           FileInode *inode,
                                           bool cache_enabled = true);

}  // namespace OssFileSystem