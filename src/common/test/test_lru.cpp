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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <malloc.h>
#include <photon/common/alog.h>

#include <thread>

#include "common/lru_map.h"
#include "common/test/test_common.h"
#include "common/utils.h"

#define ASSERT_FIND_AND_ERASE(key, value, map) \
  {                                            \
    decltype(value) v;                         \
    ASSERT_TRUE(map.find_and_erase(key, &v));  \
    ASSERT_EQ(v, value);                       \
  }

DEFINE_uint32(test_lru_perf_concur, 4,
              "Test lru performance with this concurrency.");
DEFINE_uint64(test_lru_perf_filenum, 10'000'000,
              "Test lru performance with this many nodes.");

TEST(LRUMapTest, sanity_test) {
  LruMap<int, int> map;

  ASSERT_SIZE_EQ(0, map.size());
  ASSERT_FALSE(map.probe(1));

  map.insert(1, 1);
  ASSERT_SIZE_EQ(1, map.size());
  ASSERT_TRUE(map.probe(1));

  map.insert(1, 2);
  ASSERT_SIZE_EQ(1, map.size());
  ASSERT_TRUE(map.probe(1));
  ASSERT_FIND_AND_ERASE(1, 2, map);

  ASSERT_SIZE_EQ(0, map.size());
  ASSERT_FALSE(map.probe(1));

  map.insert(1, 1);
  ASSERT_SIZE_EQ(1, map.size());
  ASSERT_TRUE(map.probe(1));

  ASSERT_FALSE(map.probe(2));
  map.insert(2, 1);
  ASSERT_TRUE(map.probe(2));
  ASSERT_SIZE_EQ(2, map.size());
  ASSERT_FIND_AND_ERASE(2, 1, map);

  map.insert(2, 2);
  ASSERT_SIZE_EQ(2, map.size());
  ASSERT_TRUE(map.probe(2));
  ASSERT_FIND_AND_ERASE(2, 2, map);

  ASSERT_SIZE_EQ(1, map.size());
  ASSERT_FALSE(map.probe(2));
  map.erase(1);
  ASSERT_SIZE_EQ(0, map.size());
  ASSERT_FALSE(map.probe(1));
}

TEST(LRUMapTest, verify_probe_find_with_filter) {
  auto filter = [](const int &key, const int &value) -> bool {
    return key & 1;
  };

  LruMap<int, int> map(filter);
  ASSERT_SIZE_EQ(0, map.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
  }

  for (int i = 0; i < 100; i++) {
    if (i & 1)
      ASSERT_TRUE(map.probe(i));
    else
      ASSERT_FALSE(map.probe(i));
  }

  int v;
  for (int i = 0; i < 100; i++) {
    if (i & 1) {
      ASSERT_TRUE(map.find_and_erase(i, &v));
      ASSERT_EQ(v, i);
    } else {
      ASSERT_FALSE(map.find_and_erase(i, &v));
    }
  }

  ASSERT_SIZE_EQ(0, map.size());
}

TEST(LRUMapTest, verify_prune) {
  LruMap<int, int> map;

  ASSERT_SIZE_EQ(0, map.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
  }

  map.prune(0);
  ASSERT_SIZE_EQ(0, map.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
  }

  map.prune(1);
  ASSERT_SIZE_EQ(1, map.size());
  for (int i = 0; i < 99; i++) {
    ASSERT_FALSE(map.probe(i));
  }
  ASSERT_TRUE(map.probe(99));
  ASSERT_FIND_AND_ERASE(99, 99, map);

  map.prune(0);
  ASSERT_SIZE_EQ(0, map.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
    ASSERT_FIND_AND_ERASE(i, i, map);
    ASSERT_SIZE_EQ(i, map.size());
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
  }

  map.prune(10);
  ASSERT_SIZE_EQ(10, map.size());
  for (int i = 0; i < 90; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 90; i < 100; i++) {
    ASSERT_TRUE(map.probe(i));
    ASSERT_FIND_AND_ERASE(i, i, map);
  }

  ASSERT_SIZE_EQ(0, map.size());
}

TEST(LRUMapTest, verify_prune_with_cb) {
  auto filterCb_1 = [&](const int &k, const int &v) -> bool { return v & 1; };

  auto filterCb_2 = [&](const int &k, const int &v) -> bool {
    return v % 10 == 8;
  };

  LruMap<int, int> map;

  ASSERT_SIZE_EQ(0, map.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
    ASSERT_FIND_AND_ERASE(i, i, map);
    ASSERT_SIZE_EQ(i, map.size());
    map.insert(i, i);
    ASSERT_SIZE_EQ(i + 1, map.size());
  }

  map.prune(filterCb_1);
  // Only odd values are evicted.
  ASSERT_SIZE_EQ(50, map.size());
  for (int i = 0; i < 100; i++) {
    if (i & 1)
      ASSERT_FALSE(map.probe(i));
    else
      ASSERT_TRUE(map.probe(i));
  }

  map.prune(filterCb_2);
  // Only values whose last digit is 8 are evicted.
  ASSERT_SIZE_EQ(40, map.size());
  for (int i = 0; i < 100; i += 2) {
    if (i % 10 != 8) {
      ASSERT_TRUE(map.probe(i));
    } else {
      ASSERT_FALSE(map.probe(i));
    }
  }

  map.prune(filterCb_1);
  ASSERT_SIZE_EQ(40, map.size());  // nothing changed

  map.prune(0);
  ASSERT_SIZE_EQ(0, map.size());
}

TEST(LRUMapTest, verify_capacity) {
  const size_t cap = 50;
  LruMap<int, int> map(cap);

  for (int i = 0; i < 50; i++) {
    map.insert(i, i);
  }
  ASSERT_SIZE_EQ(cap, map.size());
  for (int i = 0; i < 50; i++) {
    ASSERT_TRUE(map.probe(i));
  }

  for (int i = 50; i < 77; i++) {
    map.insert(i, i);
    ASSERT_TRUE(map.probe(i));
  }

  ASSERT_SIZE_EQ(cap, map.size());
  for (int i = 0; i < 77; i++) {
    if (i < 27)
      ASSERT_FALSE(map.probe(i));
    else
      ASSERT_TRUE(map.probe(i));
  }

  map.prune(0);
  ASSERT_SIZE_EQ(0, map.size());
}

TEST(LRUMapTest, verify_destructor_invocation) {
  struct SumInt {
    SumInt(int val_, int *ref_) : val(val_), ref(ref_) {}
    ~SumInt() {
      *ref += val;
    }

    SumInt(SumInt const &) = delete;
    SumInt &operator=(SumInt const &) = delete;

    SumInt(SumInt &&other) : val(std::exchange(other.val, 0)), ref(other.ref) {}
    SumInt &operator=(SumInt &&other) {
      std::swap(val, other.val);
      std::swap(ref, other.ref);
      return *this;
    }

    int val;
    int *ref;
  };

  auto filterCb = [&](const int &k, const std::unique_ptr<SumInt> &v) -> bool {
    return v->val & 1;
  };

  int sum;
  LruMap<int, std::unique_ptr<SumInt>> map;

  ASSERT_SIZE_EQ(0, map.size());
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, std::make_unique<SumInt>(i, &sum));
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
  }

  sum = 0;
  map.prune(filterCb);
  ASSERT_SIZE_EQ(50, map.size());
  for (int i = 0; i < 100; i++) {
    if (i & 1)
      ASSERT_FALSE(map.probe(i));
    else
      ASSERT_TRUE(map.probe(i));
  }
  ASSERT_EQ((50 * 100) / 2, sum);  // 1 + 3 + ... + 99

  sum = 0;
  for (int i = 0; i < 100; i += 2) {
    map.erase(i);
    ASSERT_FALSE(map.probe(i));
  }
  ASSERT_SIZE_EQ(0, map.size());
  ASSERT_EQ((50 * 98) / 2, sum);  // 0 + 2 + ... + 98

  for (int i = 0; i < 100; i++) {
    map.insert(i, std::make_unique<SumInt>(i, &sum));
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
  }

  sum = 0;
  map.prune(1);
  ASSERT_SIZE_EQ(1, map.size());
  for (int i = 0; i < 99; i++) {
    ASSERT_FALSE(map.probe(i));
  }
  ASSERT_TRUE(map.probe(99));
  ASSERT_EQ((98 * 99) / 2, sum);

  sum = 0;
  {
    std::unique_ptr<SumInt> ptr;
    ASSERT_TRUE(map.find_and_erase(99, &ptr));
    ASSERT_EQ(99, ptr->val);
  }

  ASSERT_SIZE_EQ(0, map.size());
  ASSERT_EQ(99, sum);  // ptr is destructed
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(map.probe(i));
  }

  for (int i = 0; i < 100; i++) {
    map.insert(i, std::make_unique<SumInt>(i, &sum));
    ASSERT_SIZE_EQ(i + 1, map.size());
    ASSERT_TRUE(map.probe(i));
  }

  sum = 0;
  map.prune(10);
  ASSERT_SIZE_EQ(10, map.size());
  for (int i = 0; i < 90; i++) {
    ASSERT_FALSE(map.probe(i));
  }
  for (int i = 90; i < 100; i++) {
    ASSERT_TRUE(map.probe(i));
  }
  EXPECT_EQ((89 * 90) / 2, sum);

  sum = 0;
  for (int i = 0; i < 90; i++) {
    map.insert(i, std::make_unique<SumInt>(i + 1, &sum));
    ASSERT_TRUE(map.probe(i));
  }

  EXPECT_EQ(0, sum);
  for (int i = 90; i < 100; i++) {
    map.insert(i, std::make_unique<SumInt>(i + 1, &sum));
    ASSERT_TRUE(map.probe(i));
  }
  // the last 10 items are covered by a new value
  // originally, the last 10 items are: 90, 91, 92, ..., 99
  // now they are: 91, 92, 93, ..., 100
  ASSERT_EQ((10 * 189) / 2, sum);
  sum = 0;
  map.prune(0);
  EXPECT_EQ((90 * 91) / 2 + (10 * 191) / 2, sum);
}

TEST(LRUMapTest, lru_sanity_test) {
  const size_t cap = 1000;
  size_t value;
  auto lru_map_ptr = std::make_unique<LruMap<std::string, size_t>>(cap);
  LOG_INFO("Inserting ` items.", cap + 10);
  for (size_t i = 0; i < cap + 10; ++i) {
    lru_map_ptr->insert("prefix_" + std::to_string(i), i);
  }
  ASSERT_EQ(lru_map_ptr->size(), cap);

  // The first 10 items should be evicted.
  for (size_t i = 0; i < 10; ++i) {
    ASSERT_FALSE(lru_map_ptr->probe("prefix_" + std::to_string(i)));
  }

  // 2. verify insert with the same key.
  LOG_INFO("Inserting 10 items with the same key: prefix_10 ~ prefix_19");
  for (size_t i = 10; i < 20; ++i) {
    lru_map_ptr->insert("prefix_" + std::to_string(i), i + 10);
  }
  for (size_t i = 10; i < 20; ++i) {
    ASSERT_TRUE(
        lru_map_ptr->find_and_erase("prefix_" + std::to_string(i), &value));
    ASSERT_EQ(value, i + 10);
    lru_map_ptr->insert("prefix_" + std::to_string(i + cap), i + cap);
  }

  LOG_INFO("Probe prefix_20 ~ prefix_29");
  for (size_t i = 20; i < 30; ++i) {
    ASSERT_TRUE(lru_map_ptr->probe("prefix_" + std::to_string(i)));
  }
  // probe puts the item to the front of the list again. So the next time the
  // list is full and needs eviction, prefix_30 ~ prefix_39 will be evicted.
  for (size_t i = 0; i < 10; ++i) {
    lru_map_ptr->insert("prefix_" + std::to_string(i + cap + 20), i + cap + 20);
    ASSERT_FALSE(lru_map_ptr->probe("prefix_" + std::to_string(i + 30)));
  }

  // 3. verify find_and_erase.
  LOG_INFO("Randomly erase 50 items.");
  for (size_t i = 27; i < cap + 20; i += 20) {
    ASSERT_TRUE(
        lru_map_ptr->find_and_erase("prefix_" + std::to_string(i), &value));
    ASSERT_FALSE(lru_map_ptr->probe("prefix_" + std::to_string(i)));
  }
  ASSERT_EQ(lru_map_ptr->size(), cap - 50);

  // 4. verify prune with evict cb.
  LOG_INFO("Erase all odd values.");
  lru_map_ptr->prune([](const std::string &key, const uint64_t &value) -> bool {
    return value & 1;
  });  // erase all odd values.
  ASSERT_EQ(lru_map_ptr->size(), cap / 2);
  for (size_t i = 40; i < cap + 20; i += 2) {
    ASSERT_TRUE(lru_map_ptr->probe("prefix_" + std::to_string(i)));
  }

  lru_map_ptr->prune(0);
  ASSERT_EQ(lru_map_ptr->size(), size_t(0));
}

TEST(LRUMapTest, DISABLED_performance) {
  struct CacheNode {
    CacheNode(uint64_t v) : value(v) {}

    uint64_t value;
  };

  auto lru_map_ptr =
      std::make_unique<LruMap<std::string, std::unique_ptr<CacheNode>>>();
  const uint64_t round = 10'000'000;

  auto now = []() -> int64_t {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  };
  std::string prefix = "abcdefghijklmnopqrs_";  // prefix sized of 20

  LOG_INFO("[Before] Physical memory usage: ` KiB.", get_physical_memory_KiB());
  auto time_before = now();
  for (uint64_t i = 0; i < round; ++i) {
    lru_map_ptr->insert(prefix + std::to_string(i),
                        std::move(std::make_unique<CacheNode>(i)));
  }
  // clang-format off
  LOG_INFO(
      "[After Insertion] Inserting 1000w costs: ` ms. Physical memory usage: ` KiB.",
      now() - time_before, get_physical_memory_KiB());
  // clang-format on

  time_before = now();
  for (uint64_t i = 0; i < round; ++i) {
    lru_map_ptr->insert(prefix + std::to_string(i),
                        std::move(std::make_unique<CacheNode>(i + 10)));
  }

  // clang-format off
  LOG_INFO(
      "[After Insertion] Reinserting the same 1000w costs: ` ms. Physical memory usage: ` KiB.",
      now() - time_before, get_physical_memory_KiB());
  // clang-format on

  time_before = now();
  for (uint64_t i = 7; i < round; i += 10) {
    std::unique_ptr<CacheNode> value;
    ASSERT_TRUE(
        lru_map_ptr->find_and_erase(prefix + std::to_string(i), &value));
  }
  auto time_after = now();
  malloc_trim(0);
  LOG_INFO(
      "[After Erase] Erasing 100w costs: ` ms. Physical memory usage: ` KiB.",
      time_after - time_before, get_physical_memory_KiB());

  time_before = now();
  lru_map_ptr->prune(0);
  time_after = now();
  malloc_trim(0);
  LOG_INFO("[After Prune] Prune time: ` ms. Physical memory usage: ` KiB.",
           time_after - time_before, get_physical_memory_KiB());

  ASSERT_EQ(lru_map_ptr->size(), size_t(0));
}

TEST(LRUMapTest, DISABLED_concurrent_performance) {
  const uint32_t concurrency = FLAGS_test_lru_perf_concur;
  struct CacheNode {
    CacheNode(uint64_t v) : value(v) {}

    uint64_t value;
  };

  auto lru_map_ptr =
      std::make_unique<LruMap<std::string, std::unique_ptr<CacheNode>>>();
  const uint64_t total_num = FLAGS_test_lru_perf_filenum;
  const uint64_t num_per_thread = total_num / concurrency;

  auto now = []() -> int64_t {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  };
  std::string prefix = "_abcdefghijklmnopqr_";  // prefix sized of 20

  LOG_INFO("[Before] Physical memory usage: ` KiB.", get_physical_memory_KiB());
  std::vector<std::thread *> threads;
  auto time_before = now();
  for (uint32_t i = 0; i < concurrency; i++) {
    auto thread = new std::thread(
        [&lru_map_ptr, &prefix, &num_per_thread](int id) {
          for (uint64_t j = 0; j < num_per_thread; j++) {
            lru_map_ptr->insert(std::to_string(id) + prefix + std::to_string(j),
                                std::move(std::make_unique<CacheNode>(j)));
          }
        },
        i);
    threads.emplace_back(thread);
  }
  for (auto t : threads) {
    t->join();
    delete t;
  }
  threads.clear();
  // clang-format off
  LOG_INFO(
      "After inserting: [Concurrency=`][Nodes per Thread=`][Time Cost=` ms] Physical memory usage: ` KiB.",
      concurrency, num_per_thread, now() - time_before, get_physical_memory_KiB());
  // clang-format on
  ASSERT_SIZE_EQ(lru_map_ptr->size(), total_num);

  time_before = now();
  for (uint32_t i = 0; i < concurrency; i++) {
    auto thread = new std::thread(
        [&lru_map_ptr, &prefix, &num_per_thread](int id) {
          for (uint64_t j = 0; j < num_per_thread; j++) {
            std::unique_ptr<CacheNode> value;
            lru_map_ptr->find_and_erase(
                std::to_string(id) + prefix + std::to_string(j), &value);
          }
        },
        i);
    threads.emplace_back(thread);
  }
  for (auto t : threads) {
    t->join();
    delete t;
  }
  threads.clear();
  malloc_trim(0);
  // clang-format off
  LOG_INFO(
      "After erasing: [Concurrency=`][Nodes per Thread=`][Time Cost=` ms] Physical memory usage: ` KiB.",
      concurrency, num_per_thread, now() - time_before, get_physical_memory_KiB());
  // clang-format on
  ASSERT_SIZE_EQ(lru_map_ptr->size(), 0);
}

// ============================================================
// String key (map key = string_view) safety tests
// ============================================================

// 1. Verify temporary string insert: string_view points to its copy in the
// list, not the temp string.
TEST(LRUMapStringKeyTest, string_key_temporary_insert) {
  LruMap<std::string, int> map;

  for (int i = 0; i < 100; i++) {
    // The key is a temporary string, which will be destroyed after the function
    // returns.
    map.insert("tmp_key_" + std::to_string(i), i);
  }
  ASSERT_SIZE_EQ(100, map.size());

  // Verify that the map still works with newly constructed strings.
  for (int i = 0; i < 100; i++) {
    ASSERT_TRUE(map.probe("tmp_key_" + std::to_string(i)));
  }

  int v;
  for (int i = 0; i < 100; i++) {
    ASSERT_TRUE(map.find_and_erase("tmp_key_" + std::to_string(i), &v));
    ASSERT_EQ(v, i);
  }
  ASSERT_SIZE_EQ(0, map.size());
}

// Verify overwrite same key: old list node is deleted, old string_view in map
// is invalid.
TEST(LRUMapStringKeyTest, string_key_overwrite_same_key) {
  LruMap<std::string, int> map;

  for (int round = 0; round < 10; round++) {
    for (int i = 0; i < 50; i++) {
      map.insert("ow_" + std::to_string(i), round * 100 + i);
    }
    ASSERT_SIZE_EQ(50, map.size());

    // After overwrite, all keys are still accessible.
    for (int i = 0; i < 50; i++) {
      ASSERT_TRUE(map.probe("ow_" + std::to_string(i)));
    }
  }

  // The final values are the last round of writes.
  for (int i = 0; i < 50; i++) {
    int v;
    ASSERT_TRUE(map.find_and_erase("ow_" + std::to_string(i), &v));
    ASSERT_EQ(v, 9 * 100 + i);
  }
  ASSERT_SIZE_EQ(0, map.size());
}

// Capacity Eviction: verify list eviction and string_view validity.
TEST(LRUMapStringKeyTest, string_key_capacity_eviction) {
  const size_t cap = 50;
  LruMap<std::string, int> map(cap);

  for (int i = 0; i < 100; i++) {
    map.insert("evict_" + std::to_string(i), i);
  }
  ASSERT_SIZE_EQ(cap, map.size());

  for (int i = 0; i < 50; i++) {
    ASSERT_FALSE(map.probe("evict_" + std::to_string(i)));
  }

  for (int i = 50; i < 100; i++) {
    ASSERT_TRUE(map.probe("evict_" + std::to_string(i)));
  }

  for (int i = 100; i < 120; i++) {
    map.insert("evict_" + std::to_string(i), i);
  }
  ASSERT_SIZE_EQ(cap, map.size());

  for (int i = 50; i < 70; i++) {
    ASSERT_FALSE(map.probe("evict_" + std::to_string(i)));
  }
  for (int i = 70; i < 120; i++) {
    ASSERT_TRUE(map.probe("evict_" + std::to_string(i)));
  }
}

// probe triggered splice, the string_view still points to the same node after
// the node is moved.
TEST(LRUMapStringKeyTest, string_key_probe_splice_safety) {
  const size_t cap = 10;
  LruMap<std::string, int> map(cap);

  for (int i = 0; i < 10; i++) {
    map.insert("sp_" + std::to_string(i), i);
  }

  // Keep splicing: probe every element in reverse order.
  for (int round = 0; round < 5; round++) {
    for (int i = 9; i >= 0; i--) {
      ASSERT_TRUE(map.probe("sp_" + std::to_string(i)));
    }
  }

  // After splice and insert triggering eviction, verify the remained
  // string_view's validity.
  map.insert("sp_new", 99);
  ASSERT_SIZE_EQ(cap, map.size());
  ASSERT_FALSE(map.probe("sp_9"));
  ASSERT_TRUE(map.probe("sp_new"));

  for (int i = 0; i < 9; i++) {
    ASSERT_TRUE(map.probe("sp_" + std::to_string(i)));
  }
}

// Verify that validator accepts the real string instead of dangling
// string_view.
TEST(LRUMapStringKeyTest, string_key_validator_accesses_key_content) {
  auto validator = [](const std::string &key, const int &value) -> bool {
    return key.find("valid") != std::string::npos;
  };

  LruMap<std::string, int> map(validator);

  map.insert("valid_1", 1);
  map.insert("valid_2", 2);
  map.insert("inval_3", 3);
  map.insert("valid_4", 4);
  ASSERT_SIZE_EQ(4, map.size());

  ASSERT_TRUE(map.probe("valid_1"));
  ASSERT_TRUE(map.probe("valid_2"));
  ASSERT_FALSE(map.probe("inval_3"));
  ASSERT_TRUE(map.probe("valid_4"));
  ASSERT_SIZE_EQ(3, map.size());

  int v;
  ASSERT_TRUE(map.find_and_erase("valid_1", &v));
  ASSERT_EQ(v, 1);

  map.insert("inval_5", 5);
  ASSERT_FALSE(map.find_and_erase("inval_5", &v));
  ASSERT_SIZE_EQ(2, map.size());
}

TEST(LRUMapStringKeyTest, string_key_prune) {
  LruMap<std::string, int> map;

  for (int i = 0; i < 100; i++) {
    map.insert("pr_" + std::to_string(i), i);
  }

  // prune by size
  map.prune(50);
  ASSERT_SIZE_EQ(50, map.size());
  for (int i = 0; i < 50; i++) {
    ASSERT_FALSE(map.probe("pr_" + std::to_string(i)));
  }
  for (int i = 50; i < 100; i++) {
    ASSERT_TRUE(map.probe("pr_" + std::to_string(i)));
  }

  // prune with callback
  auto evict_even = [](const std::string &key, const int &value) -> bool {
    return value % 2 == 0;
  };
  map.prune(evict_even);
  ASSERT_SIZE_EQ(25, map.size());

  for (int i = 50; i < 100; i++) {
    if (i % 2 == 0)
      ASSERT_FALSE(map.probe("pr_" + std::to_string(i)));
    else
      ASSERT_TRUE(map.probe("pr_" + std::to_string(i)));
  }

  map.prune(0);
  ASSERT_SIZE_EQ(0, map.size());
}

// Mix insert/erase/probe/find_and_erase
TEST(LRUMapStringKeyTest, string_key_interleaved_ops) {
  const size_t cap = 200;
  LruMap<std::string, int> map(cap);

  for (int i = 0; i < 300; i++) {
    map.insert("il_" + std::to_string(i), i);
  }
  ASSERT_SIZE_EQ(cap, map.size());

  for (int i = 200; i < 250; i++) {
    ASSERT_TRUE(map.probe("il_" + std::to_string(i)));
  }

  for (int i = 250; i < 270; i++) {
    map.erase("il_" + std::to_string(i));
  }
  ASSERT_SIZE_EQ(cap - 20, map.size());

  for (int i = 250; i < 270; i++) {
    map.insert("il_" + std::to_string(i), i + 1000);
  }
  ASSERT_SIZE_EQ(cap, map.size());

  for (int i = 250; i < 270; i++) {
    int v;
    ASSERT_TRUE(map.find_and_erase("il_" + std::to_string(i), &v));
    ASSERT_EQ(v, i + 1000);
  }
  for (int i = 200; i < 250; i++) {
    ASSERT_TRUE(map.probe("il_" + std::to_string(i)));
  }

  map.prune(0);
  ASSERT_SIZE_EQ(0, map.size());
}

TEST(LRUMapStringKeyTest, string_key_with_unique_ptr_value) {
  int destruct_sum = 0;
  struct Tracked {
    Tracked(int v, int *sum) : val(v), sum_(sum) {}
    ~Tracked() {
      if (sum_) *sum_ += val;
    }
    Tracked(Tracked &&o) : val(o.val), sum_(o.sum_) {
      o.sum_ = nullptr;
    }
    Tracked &operator=(Tracked &&o) {
      val = o.val;
      std::swap(sum_, o.sum_);
      return *this;
    }
    int val;
    int *sum_;
  };

  LruMap<std::string, std::unique_ptr<Tracked>> map;

  for (int i = 0; i < 50; i++) {
    map.insert("tr_" + std::to_string(i),
               std::make_unique<Tracked>(i, &destruct_sum));
  }
  ASSERT_SIZE_EQ(50, map.size());

  destruct_sum = 0;
  for (int i = 0; i < 10; i++) {
    map.insert("tr_" + std::to_string(i),
               std::make_unique<Tracked>(i + 100, &destruct_sum));
  }
  ASSERT_EQ(destruct_sum, (9 * 10) / 2);  // 0+1+...+9 = 45

  // erase 10-19
  destruct_sum = 0;
  for (int i = 10; i < 20; i++) {
    map.erase("tr_" + std::to_string(i));
  }
  ASSERT_EQ(destruct_sum, (10 + 19) * 10 / 2);  // 10+11+...+19 = 145

  // find_and_erase 20-29
  destruct_sum = 0;
  for (int i = 20; i < 30; i++) {
    std::unique_ptr<Tracked> v;
    ASSERT_TRUE(map.find_and_erase("tr_" + std::to_string(i), &v));
    ASSERT_EQ(v->val, i);
  }
  ASSERT_EQ(destruct_sum, (20 + 29) * 10 / 2);  // 245

  ASSERT_SIZE_EQ(30, map.size());
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(map.probe("tr_" + std::to_string(i)));
  }
  for (int i = 30; i < 50; i++) {
    ASSERT_TRUE(map.probe("tr_" + std::to_string(i)));
  }

  destruct_sum = 0;
  map.prune(0);

  int expected = 0;
  for (int i = 0; i < 10; i++) expected += i + 100;
  for (int i = 30; i < 50; i++) expected += i;
  ASSERT_EQ(destruct_sum, expected);
  ASSERT_SIZE_EQ(0, map.size());
}
