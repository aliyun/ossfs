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

#include <functional>
#include <list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

using std::lock_guard;
using std::mutex;
using std::unordered_map;

template <typename K>
struct LruKeyView {
  using type = K;

  static const type &to_view(const K &key) {
    return key;
  }
};

template <>
struct LruKeyView<std::string> {
  using type = std::string_view;

  static type to_view(const std::string &key) {
    return std::string_view(key);
  }
};

// List element: pair<Key, Value>.
// Map key: the VIEW of Key in List element.
// Map value: the iterator of the element in List.
template <typename K, typename V>
class LruMap {
 public:
  using KeyType = K;
  using KeyViewType = typename LruKeyView<K>::type;
  using ValueType = V;
  using ItemType = std::pair<K, V>;
  using ListType = std::list<ItemType>;
  using MapType = unordered_map<KeyViewType, typename ListType::iterator>;
  using Filter = std::function<bool(const KeyType &, const ValueType &)>;

 public:
  LruMap() = default;
  LruMap(size_t size, Filter validator)
      : max_size_(size), validator_(validator) {}
  LruMap(size_t size) : LruMap(size, nullptr) {}
  LruMap(Filter validator)
      : LruMap(std::numeric_limits<size_t>::max(), validator) {}

  ~LruMap() {
    lock_guard<mutex> lock(mut_);
    map_.clear();
    list_.clear();
  }

  template <typename InsertValueType>
  void insert(const KeyType &key, InsertValueType &&value) {
    lock_guard<mutex> lock(mut_);

    auto it = map_.find(key);
    if (it != map_.end()) {
      auto list_it = it->second;
      list_.splice(list_.begin(), list_, list_it);
      list_it->second = std::forward<InsertValueType>(value);
    } else {
      list_.emplace_front(key, std::forward<InsertValueType>(value));
      map_.emplace(key_to_view(list_.begin()->first), list_.begin());
    }

    if (list_.size() > max_size_) {
      auto &pair = list_.back();
      map_.erase(key_to_view(pair.first));
      list_.pop_back();
    }
  }

  // Trim the lru map to expect_size.
  size_t prune(size_t expect_size) {
    return prune_internal(expect_size, nullptr);
  }

  // Erase keys when evict_cond_cb(k, v) returns true.
  size_t prune(Filter evict_cond_cb) {
    return prune_internal(0, evict_cond_cb);
  }

  void erase(const KeyType &key) {
    lock_guard<mutex> lock(mut_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      erase_node_safely(it);
    }
  }

  bool probe(const KeyType &key) {
    lock_guard<mutex> lock(mut_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      // Use it->second->first instead of it->first (could be string_view).
      if (validator_ && !validator_(it->second->first, it->second->second)) {
        erase_node_safely(it);
        return false;
      }

      list_.splice(list_.begin(), list_, it->second);
      assert(it->second == list_.begin());
      return true;
    }
    return false;
  }

  bool find_and_erase(const KeyType &key, ValueType *value) {
    lock_guard<mutex> lock(mut_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      if (validator_ && !validator_(it->second->first, it->second->second)) {
        erase_node_safely(it);
        return false;
      }

      if constexpr (std::is_move_constructible_v<ValueType>) {
        *value = std::move(it->second->second);
      } else {
        *value = it->second->second;
      }
      erase_node_safely(it);
      return true;
    }
    return false;
  }

  size_t size() {
    lock_guard<mutex> lock(mut_);
    return list_.size();
  }

 private:
  // If evict_cond_cb is not set, erase keys in LRU mode until expect_size is
  // reached; otherwise, erase keys only when evict_cond_cb returns true.
  size_t prune_internal(size_t expect_size, Filter evict_cond_cb) {
    lock_guard<mutex> l(mut_);

    size_t evict_cnt = 0;
    auto lit = list_.end();
    while (lit != list_.begin()) {
      if (list_.size() <= expect_size) break;

      lit--;
      KeyType &key = lit->first;
      ValueType &value = lit->second;

      if (evict_cond_cb && !evict_cond_cb(key, value)) continue;

      map_.erase(key_to_view(key));
      lit = list_.erase(lit);
      evict_cnt++;
    }

    return evict_cnt;
  }

  void erase_node_safely(typename MapType::iterator it) {
    auto list_it = it->second;
    map_.erase(it);
    list_.erase(list_it);
  }

  static decltype(auto) key_to_view(const KeyType &key) {
    return LruKeyView<K>::to_view(key);
  }

  size_t max_size_{std::numeric_limits<size_t>::max()};
  ListType list_;
  MapType map_;
  Filter validator_{nullptr};  // check if the kv is still valid
  mutex mut_;
};