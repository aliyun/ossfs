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
