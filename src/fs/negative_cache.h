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

#include <string>

#include "common/lru_map.h"

class Ossfs2NegativeCacheTest;

namespace OssFileSystem {

class NegativeCache {
 public:
  NegativeCache(uint64_t negative_cache_ttl, uint64_t negative_cache_size)
      : n_lru_map_(std::make_unique<LruMap<std::string, int64_t>>(
            negative_cache_size,
            [to = negative_cache_ttl](const std::string &key,
                                      int64_t cache_time) -> bool {
              auto now =
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now().time_since_epoch())
                      .count();
              return uint64_t(now - cache_time) < to * 1000;
            })) {
    // MapType key: KeyType (full path) at most 1023 bytes
    // MapType value: ListType::iterator costs 8 bytes
    // ListType: element type: pair<K, V>
    //           K: (full path) at most 1023 bytes
    //           V: costs 8 bytes
  }

  void insert(const std::string &path) {
    n_lru_map_->insert(path,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now().time_since_epoch())
                           .count());
  }

  void erase(const std::string &path) {
    n_lru_map_->erase(path);
  }

  bool exists(const std::string &path) {
    return n_lru_map_->probe(path);
  }

  void erase_by_prefix(std::string_view prefix) {
    size_t lru_size_before = n_lru_map_->size();
    auto key_starts_with_prefix = [&prefix](const std::string &key,
                                            int64_t not_used) -> bool {
      return prefix.length() <= key.length() &&
             std::string_view(key).substr(0, prefix.length()) == prefix;
    };

    n_lru_map_->prune(key_starts_with_prefix);

    LOG_INFO("entry num before: `, entry num now: `", lru_size_before,
             n_lru_map_->size());
  }

  static std::atomic<uint64_t> create_cache_hit_cnt_;
  static std::atomic<uint64_t> lookup_cache_hit_cnt_;

 private:
  std::unique_ptr<LruMap<std::string, int64_t>>
      n_lru_map_;  // value: cache time
  friend class ::Ossfs2NegativeCacheTest;
};

}  // namespace OssFileSystem
