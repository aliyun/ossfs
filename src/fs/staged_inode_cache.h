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

#include <malloc.h>
#include <photon/common/string_view.h>
#include <time.h>

#include <string>

#include "common/lru_map.h"
#include "inode.h"

namespace OssFileSystem {

// key: parent_nodeid + "/" + name

class StagedInodeCache {
 public:
  struct CacheEntry {
    uint64_t size;
    struct timespec mtime;
    std::string etag;
    // The original inode's nodeid should be cached. Consider this case:
    // There is a non-empty dir: dir/. All of them are forgotten, and inserted
    // to the lru cache. The next time we want to lookup them, the dir's inode
    // will be reconstructed, and if its nodeid is a new one, those subfiles
    // will not be found in the lru cache with the new parent nodeid (they are
    // cached with key: old parent nodeid + name).
    uint64_t nodeid;
    InodeType type;
    time_t attr_time;

    CacheEntry(uint64_t size, struct timespec mtime, std::string_view etag,
               uint64_t nodeid, InodeType type, time_t attr_time)
        : size(size),
          mtime(mtime),
          etag(etag),
          nodeid(nodeid),
          type(type),
          attr_time(attr_time) {}
  };

  StagedInodeCache(uint64_t timeout)
      : lru_map_(
            std::make_unique<LruMap<std::string, std::unique_ptr<CacheEntry>>>(
                [to = timeout](
                    const std::string &key,
                    const std::unique_ptr<CacheEntry> &node) -> bool {
                  return ::difftime(time(nullptr), node->attr_time) < to;
                })) {}

  bool find_and_erase(uint64_t parent_nodeid, std::string_view name,
                      std::unique_ptr<CacheEntry> *entry) {
    auto key = generate_key(parent_nodeid, name);
    return lru_map_->find_and_erase(key, entry);
  }

  bool exists(uint64_t parent_nodeid, std::string_view name) {
    auto key = generate_key(parent_nodeid, name);
    return lru_map_->probe(key);
  }

  void insert(uint64_t parent_nodeid, std::string_view name, uint64_t size,
              struct timespec mtime, std::string_view etag, uint64_t nodeid,
              InodeType type, time_t attr_time) {
    auto entry = std::make_unique<CacheEntry>(size, mtime, etag, nodeid, type,
                                              attr_time);
    lru_map_->insert(generate_key(parent_nodeid, name), std::move(entry));
  }

  void erase(uint64_t parent_nodeid, std::string_view name) {
    lru_map_->erase(generate_key(parent_nodeid, name));
  }

  uint64_t evict_keys(uint64_t left_cnt) {
    return lru_map_->prune(left_cnt);
  }

  size_t size() {
    return lru_map_->size();
  }

  void print_staged_cache_status() {
    LOG_INFO("[SystemInfo] staged cache size: `", size());
  }

 private:
  std::string generate_key(uint64_t parent_nodeid, std::string_view name) {
    return std::to_string(parent_nodeid)
        .append("/")
        .append(name.data(), name.size());
  }

  std::unique_ptr<LruMap<std::string, std::unique_ptr<CacheEntry>>> lru_map_;
};

}  // namespace OssFileSystem