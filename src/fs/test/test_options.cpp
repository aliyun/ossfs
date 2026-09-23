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

#include "test_suite.h"

TEST(OssFsOptionsTest, verify_memory_limit_adjustment) {
  OssFsOptions default_options;
  OssFsOptions options;

  // limit to 8 GiB with 60% of rw ratio
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 8ULL << 30, 0.6), 0);
  // 4.8 GiB for readwrite buffer.
  // (read : write) = (256 * 3 * 8 MiB : 64 * 8 MiB) = (12 : 1), so:
  //   prefetch_concurrency should be (4.8 GiB * 12 / 13) / 8 MiB / 3 = 189
  //   upload_concurrency should be (4.8 GiB * 1 / 13) / 8 MiB = 47
  EXPECT_EQ(options.prefetch_concurrency, 189U);
  EXPECT_EQ(options.prefetch_concurrency_per_file,
            default_options.prefetch_concurrency_per_file);
  EXPECT_EQ(options.upload_concurrency, 47U);
  EXPECT_EQ(options.prefetch_chunks, default_options.prefetch_chunks);

  // limit to 256 MB with 60% of rw ratio
  options = default_options;
  options.prefetch_chunk_size = 4ULL << 20;  // use 4 MiB
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 256ULL << 20, 0.6), 0);
  // 153 MiB for readwrite buffer.
  // (read : write) = (256 * 3 * 4 MiB : 64 * 8 MiB) = (6 : 1), so:
  //   prefetch_concurrency should be (153 MiB * 6 / 7) / 4 MiB / 3 = 10
  //   upload_concurrency should be (153 MiB * 1 / 7) / 8 MiB = 2
  EXPECT_EQ(options.prefetch_concurrency, 10U);
  EXPECT_EQ(options.prefetch_concurrency_per_file, 10U);
  EXPECT_EQ(options.upload_concurrency, 2U);
  EXPECT_EQ(options.prefetch_chunks, default_options.prefetch_chunks);

  // limit to 16 GiB, nothing changed
  options = default_options;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 16ULL << 30, 0.6), 0);
  EXPECT_EQ(options.prefetch_concurrency, default_options.prefetch_concurrency);
  EXPECT_EQ(options.prefetch_concurrency_per_file,
            default_options.prefetch_concurrency_per_file);
  EXPECT_EQ(options.upload_concurrency, default_options.upload_concurrency);
  EXPECT_EQ(options.prefetch_chunks, default_options.prefetch_chunks);

  // limit to 1 GiB with readonly mode
  options = default_options;
  options.readonly = true;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 1ULL << 30, 0.6), 0);
  // 600 MiB for read buffer.
  // prefetch_concurrency should be 600 MiB / 8 MiB / 3 = 25
  EXPECT_EQ(options.prefetch_concurrency, 25U);
  EXPECT_EQ(options.prefetch_concurrency_per_file, 25U);

  // limit to 4 GiB with invalid prefetch_chunks
  std::vector<int32_t> invalid_prefetch_chunks = {-1, 512};
  for (auto invalid_prefetch_chunk : invalid_prefetch_chunks) {
    options = default_options;
    options.prefetch_chunks = invalid_prefetch_chunk;
    EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 4ULL << 30, 0.6), 0);
    // 2.4 GiB for readwrite buffer.
    // (read : write) = (256 * 3 * 8 MiB : 64 * 8 MiB) = (12 : 1), so:
    //   prefetch_concurrency should be (2.4 GiB * 12 / 13) / 8 MiB / 3 = 94
    //   upload_concurrency should be (2.4 GiB * 1 / 12) / 8 MiB = 23
    EXPECT_EQ(options.prefetch_concurrency, 94U);
    EXPECT_EQ(options.prefetch_concurrency_per_file,
              default_options.prefetch_concurrency_per_file);
    EXPECT_EQ(options.upload_concurrency, 23U);

    // For invlaid prefetch_chunks, prefetch_chunks should be set to 0(auto
    // mode)
    EXPECT_EQ(options.prefetch_chunks, 0);
  }

  // limit to 1 GiB with direct read
  options = default_options;
  options.prefetch_concurrency = 0;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 1ULL << 30, 0.6), 0);
  EXPECT_EQ(options.prefetch_concurrency, 0U);
  EXPECT_EQ(options.prefetch_chunks, 0);

  // invalid parameter
  options = default_options;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 0, 0.6), -EINVAL);
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 1ULL << 20, -2.0), -EINVAL);
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 0, -1.0), -EINVAL);
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 1ULL << 30, 1.0), -EINVAL);
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 0, 10.1), -EINVAL);

  // invalid upload buffer size
  options = default_options;
  options.upload_buffer_size = 1ULL << 30;  // 1 GiB
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 1ULL << 30, 0.6), -EINVAL);
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 512ULL << 20, 0.6),
            -EINVAL);

  // cases with memory data cache
  // limit to 1 GiB with 512 MiB memory data cache
  //   upload_concurrency should be 512 MiB / 4 / 8 MiB = 16
  options = default_options;
  options.memory_data_cache_size = 512ULL << 20;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 1ULL << 30, 0.6), 0);
  EXPECT_EQ(options.upload_concurrency, 16U);
  EXPECT_EQ(options.prefetch_chunks, 64);

  // limit to 32 GiB with 28 GiB memory data cache
  options = default_options;
  options.memory_data_cache_size = 28ULL << 30;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 32ULL << 30, 0.6), 0);
  EXPECT_EQ(options.upload_concurrency, 64U);
  EXPECT_EQ(options.prefetch_chunks, 3584);

  // limit with read-only
  options = default_options;
  options.memory_data_cache_size = 28ULL << 30;
  options.readonly = true;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 32ULL << 30, 0.6), 0);
  EXPECT_EQ(options.upload_concurrency, 64U);
  EXPECT_EQ(options.prefetch_chunks, 3584);

  // invalid case: limit to 4 GiB with 3 GiB memory data cache
  options = default_options;
  options.memory_data_cache_size = 3ULL << 30;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 4ULL << 30, 0.6), -EINVAL);

  // invalid case: limit to 4 GiB with 2 GiB memory data cache and 2 GiB upload
  // buffer size
  options = default_options;
  options.memory_data_cache_size = 2ULL << 30;
  options.upload_buffer_size = 2ULL << 30;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 4ULL << 30, 0.6), -EINVAL);

  // invalid case: prefetch_concurrency set to 0 with memory data cache
  options = default_options;
  options.prefetch_concurrency = 0;
  options.memory_data_cache_size = 2ULL << 30;
  EXPECT_EQ(OssFsOptions::apply_mem_limit(&options, 4ULL << 30, 0.6), -EINVAL);
}

TEST(OssFsOptionsTest, verify_random_write_validation) {
  OssFsOptions options;

  // Random write disabled: nothing to validate.
  options.temp_dir = "";
  EXPECT_EQ(OssFsOptions::validate_random_write(options), 0);

  options.temp_dir = "/tmp/ossfs2_rw";

  // Default config (2 MiB chunk, 8 MiB part) is valid.
  EXPECT_EQ(OssFsOptions::validate_random_write(options), 0);

  // Mutually exclusive with appendable objects.
  options.enable_appendable_object = true;
  EXPECT_EQ(OssFsOptions::validate_random_write(options), -EINVAL);
  options.enable_appendable_object = false;

  // Unsupported on the HDFS backend (the FileInode write-state union slot
  // aliases hdfs_dirty_count and rw_ctx).
  options.storage_backend = IObjStore::StorageBackend::kHDFS;
  EXPECT_EQ(OssFsOptions::validate_random_write(options), -EINVAL);
  options.storage_backend = IObjStore::StorageBackend::kOSS;

  // chunk_size > upload_buffer_size is allowed: the constructor aligns
  // base_part_size up to chunk_size via align_up().
  options.upload_buffer_size = 1ULL << 20;
  options.random_write_chunk_size = 2ULL << 20;
  EXPECT_EQ(OssFsOptions::validate_random_write(options), 0);
}

TEST(DataBufferBudgetTest, verify_read_share) {
  OssFsOptions options;  // defaults: 1 MiB blocks, 8 MiB chunks, 256 prefetch
  auto budget = compute_data_buffer_budget(options);

  ASSERT_TRUE(budget.needed);
  ASSERT_EQ(budget.block_size, options.cache_block_size);

  // Legacy download pool capacity.
  const size_t blocks_per_chunk =
      options.prefetch_chunk_size / options.cache_block_size;
  const size_t legacy_read_capacity =
      blocks_per_chunk * options.prefetch_concurrency * 3;
  EXPECT_EQ(budget.read_quota_blocks, legacy_read_capacity);

  // Legacy upload pool capacity.
  const size_t blocks_per_buffer =
      options.upload_buffer_size / options.cache_block_size;
  const size_t legacy_write_capacity = blocks_per_buffer * (64 + 4);
  EXPECT_EQ(options.upload_concurrency, 64U);
  EXPECT_EQ(budget.pool_capacity, legacy_read_capacity + legacy_write_capacity);
  EXPECT_EQ(budget.max_cached_blocks, budget.pool_capacity);
}

TEST(DataBufferBudgetTest, verify_write_share_isolated) {
  OssFsOptions small;
  OssFsOptions large = small;
  large.upload_buffer_size = 32ULL << 20;

  auto small_budget = compute_data_buffer_budget(small);
  auto large_budget = compute_data_buffer_budget(large);

  EXPECT_EQ(small_budget.read_quota_blocks, large_budget.read_quota_blocks);
  EXPECT_GT(large_budget.pool_capacity, small_budget.pool_capacity);
}

TEST(DataBufferBudgetTest, verify_modes) {
  OssFsOptions base;
  const size_t read_capacity =
      compute_data_buffer_budget(base).read_quota_blocks;

  OssFsOptions ro = base;
  ro.readonly = true;
  auto ro_budget = compute_data_buffer_budget(ro);
  ASSERT_TRUE(ro_budget.needed);
  EXPECT_EQ(ro_budget.read_quota_blocks, read_capacity);
  EXPECT_EQ(ro_budget.pool_capacity, read_capacity);

  OssFsOptions none = ro;
  none.prefetch_concurrency = 0;
  EXPECT_FALSE(compute_data_buffer_budget(none).needed);

  OssFsOptions unlimited = base;
  unlimited.prefetch_chunks = -1;
  auto unlimited_budget = compute_data_buffer_budget(unlimited);
  EXPECT_EQ(unlimited_budget.pool_capacity, std::numeric_limits<size_t>::max());
  EXPECT_EQ(unlimited_budget.read_quota_blocks,
            std::numeric_limits<size_t>::max());
  EXPECT_LT(unlimited_budget.max_cached_blocks,
            std::numeric_limits<size_t>::max());

  OssFsOptions fixed = base;
  fixed.prefetch_chunks = 10;
  auto fixed_budget = compute_data_buffer_budget(fixed);
  EXPECT_EQ(fixed_budget.read_quota_blocks,
            (base.prefetch_chunk_size / base.cache_block_size) * 10);

  // main.cpp translates memory_data_cache_size into prefetch_chunks.
  const size_t cache_chunks = (1ULL << 30) / base.prefetch_chunk_size;
  OssFsOptions cached = base;
  cached.prefetch_chunks = static_cast<int32_t>(cache_chunks);
  auto cached_budget = compute_data_buffer_budget(cached);
  EXPECT_EQ(cached_budget.read_quota_blocks,
            (base.prefetch_chunk_size / base.cache_block_size) * cache_chunks);
}
