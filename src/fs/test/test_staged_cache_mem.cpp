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

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "common/logger.h"
#include "common/test/test_common.h"
#include "common/utils.h"
#include "fs/staged_inode_cache.h"

// Shared with Ossfs2InodeTest.DISABLED_test_metadata_mem_usage so both
// benchmarks are driven by the same knob.
DECLARE_uint64(test_mem_usage_file_num);

using namespace OssFileSystem;

// Standalone (no OSS fixture, no bucket access) memory benchmark of
// StagedInodeCache entries. It mirrors the entry shape used by
// DISABLED_test_metadata_mem_usage: 50-char names and a quoted 32-hex etag
// (the production OSS form, stored allocation-free by CompactEtag), so the
// per-entry cost is comparable across that test and across the std::string /
// CompactEtag versions of CacheEntry::etag.
//
// DISABLED by default: it holds ~n entries (default 1,000,000) in memory.
// Run with:
//   ossfs2-test --gtest_also_run_disabled_tests
//     --gtest_filter=StagedCacheMemTest.* --test_mem_usage_file_num=1000000
TEST(StagedCacheMemTest, DISABLED_per_entry_memory) {
  const uint64_t n = FLAGS_test_mem_usage_file_num;
  const std::string len_of_50 =
      "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij";
  const CompactEtag etag("\"7D3EBDBD61A735B60D8F930C57D7BB73\"");

  LOG_INFO("sizeof(StagedInodeCache::CacheEntry) = `",
           sizeof(StagedInodeCache::CacheEntry));

  auto cache = std::make_unique<StagedInodeCache>(3600);
  const int64_t rss_before = get_physical_memory_KiB();

  for (uint64_t i = 0; i < n; ++i) {
    std::string index_str = std::to_string(i);
    std::string name = len_of_50.substr(0, 50 - index_str.size()) + index_str;
    cache->insert(1, name, 0, {0, 0}, etag, i + 2, InodeType::kFile, 0);
  }

  // Let the allocator settle before sampling RSS.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  const int64_t rss_after = get_physical_memory_KiB();

  ASSERT_SIZE_EQ(n, cache->size());

  const double per_entry = double(rss_after - rss_before) * 1024 / n;
  LOG_INFO(
      "StagedCache entries: `, RSS ` -> ` KiB (delta ` KiB), per-entry ` B", n,
      rss_before, rss_after, rss_after - rss_before, per_entry);

  // Loose sanity envelope only: the A/B signal is the logged per-entry
  // number, a tight bound would flake across allocators and name lengths.
  EXPECT_GT(per_entry, 0);
  EXPECT_LT(per_entry, 4096);
}
