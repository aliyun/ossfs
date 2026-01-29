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

#include "common/logger.h"
#include "fs.h"

namespace OssFileSystem {

// In memory cache mode, we need to reserve at least
// std::min(4GiB, memory_data_cache_size * (1 - read_ratio) / read_ratio) memory
// for metadata and upload buffers, and adaptively adjust the upload buffer
// configuration to ensure sufficient metadata memory. If the memory limit
// is not enough to accommodate this reserved memory, an error will be returned.
static int apply_mem_limit_with_memory_cache(OssFsOptions *fs_options,
                                             uint64_t total_mem_limit,
                                             uint64_t write_buffer_mem,
                                             double read_ratio) {
  RELEASE_ASSERT(fs_options->memory_data_cache_size > 0);

  if (fs_options->prefetch_concurrency == 0) {
    LOG_ERROR("prefetch_concurrency must be greater than 0.");
    return -EINVAL;
  }

  int expeceted_prefetch_chunks =
      fs_options->memory_data_cache_size / fs_options->prefetch_chunk_size;
  if (expeceted_prefetch_chunks != fs_options->prefetch_chunks) {
    LOG_INFO("prefetch_chunks changed from ` to `", fs_options->prefetch_chunks,
             expeceted_prefetch_chunks);
    fs_options->prefetch_chunks = expeceted_prefetch_chunks;
  }

  const uint64_t GiBytes = 1ULL << 30;
  uint64_t remain_mem_limit =
      total_mem_limit > fs_options->memory_data_cache_size
          ? total_mem_limit - fs_options->memory_data_cache_size
          : 0;
  uint64_t reserved_mem =
      std::min(static_cast<uint64_t>(fs_options->memory_data_cache_size *
                                     (1 - read_ratio) / read_ratio),
               GiBytes * 4);
  if (remain_mem_limit < reserved_mem) {
    LOG_ERROR("The total_mem_limit ` Bytes is too small, at least ` MiBytes",
              total_mem_limit,
              (reserved_mem + fs_options->memory_data_cache_size) >> 20);
    return -EINVAL;
  }

  if (fs_options->readonly) return 0;

  LOG_INFO("Before adjusting read/write options.");
  LOG_INFO("Memory limit ` Bytes, write buffer ` Bytes, read buffer ` Bytes",
           total_mem_limit, write_buffer_mem,
           fs_options->memory_data_cache_size);

  // We set write buffer size to 1/4 of the remain memory.
  uint64_t new_write_buffer_mem =
      std::min(write_buffer_mem, remain_mem_limit / 4);

  LOG_INFO("After adjusting read/write options.");
  LOG_INFO("Memory limit ` Bytes, write buffer ` Bytes, read buffer ` Bytes",
           total_mem_limit, new_write_buffer_mem,
           fs_options->memory_data_cache_size);

  if (fs_options->upload_buffer_size > new_write_buffer_mem) {
    LOG_ERROR("The upload_buffer_size ` Bytes is too large, at most ` Bytes",
              fs_options->upload_buffer_size, new_write_buffer_mem);
    return -EINVAL;
  }

  uint32_t old_upload_concurrency = fs_options->upload_concurrency;
  fs_options->upload_concurrency =
      std::max(new_write_buffer_mem / fs_options->upload_buffer_size,
               static_cast<uint64_t>(1));
  LOG_INFO("upload_concurrency changed from ` to `", old_upload_concurrency,
           fs_options->upload_concurrency);

  return 0;
}

// Apply memory limit to adjust read/write buffer sizes and concurrency
// settings. In normal mode, we compress read/write memory proportionally based
// on the prefetch and upload buffer memory ratios. The compression is achieved
// by reducing the corresponding concurrency counts (equivalent to buffer
// counts). rw_ratio is the ratio of read/write buffer memory to total memory.
int OssFsOptions::apply_mem_limit(OssFsOptions *fs_options,
                                  uint64_t total_mem_limit, double rw_ratio) {
  if (total_mem_limit == 0 || rw_ratio <= 0 || rw_ratio >= 1) {
    LOG_ERROR("Invalid rw_ratio `", rw_ratio);
    return -EINVAL;
  }

  // Use total_rw_mem_ratio of total_mem_limit for read/write buffer, and the
  // rest part is for inode cache.
  uint64_t total_rw_mem_limit = total_mem_limit * rw_ratio;

  // write buffer
  uint64_t write_buffer_mem =
      fs_options->upload_buffer_size * fs_options->upload_concurrency;
  if (fs_options->readonly) {
    write_buffer_mem = 0;
  } else if (total_mem_limit <= fs_options->upload_buffer_size) {
    LOG_ERROR(
        "The total_mem_limit(`) should greater than upload_buffer_size(`)",
        total_mem_limit, fs_options->upload_buffer_size);
    return -EINVAL;
  }

  if (fs_options->memory_data_cache_size > 0) {
    // We use rw_ratio as read_ratio for memory cache mode
    return apply_mem_limit_with_memory_cache(fs_options, total_mem_limit,
                                             write_buffer_mem, rw_ratio);
  }

  // prefetch buffer
  uint64_t read_buffer_mem =
      fs_options->prefetch_chunk_size * fs_options->prefetch_concurrency * 3;

  if (write_buffer_mem + read_buffer_mem < total_rw_mem_limit) {
    // Nothing to do.
    return 0;
  }

  LOG_INFO("Before adjusting read/write options.");
  LOG_INFO("Memory limit ` Bytes, write buffer ` Bytes, read buffer ` Bytes",
           total_mem_limit, write_buffer_mem, read_buffer_mem);

  double read_mem_limit_ratio =
      read_buffer_mem * 1.0 / (read_buffer_mem + write_buffer_mem);

  uint64_t new_read_buffer_mem = total_rw_mem_limit * read_mem_limit_ratio;
  uint64_t new_write_buffer_mem = total_rw_mem_limit - new_read_buffer_mem;

  LOG_INFO("After adjusting read/write options.");
  LOG_INFO("Memory limit ` Bytes, write buffer ` Bytes, read buffer ` Bytes",
           total_mem_limit, new_write_buffer_mem, new_read_buffer_mem);

  uint32_t old_upload_concurrency = fs_options->upload_concurrency;
  fs_options->upload_concurrency =
      std::max(new_write_buffer_mem / fs_options->upload_buffer_size,
               static_cast<uint64_t>(1));
  LOG_INFO("upload_concurrency changed from ` to `", old_upload_concurrency,
           fs_options->upload_concurrency);

  if (new_read_buffer_mem > 0) {
    uint32_t old_prefetch_concurrency = fs_options->prefetch_concurrency;
    fs_options->prefetch_concurrency =
        std::max(new_read_buffer_mem / fs_options->prefetch_chunk_size / 3,
                 static_cast<uint64_t>(1));
    fs_options->prefetch_concurrency_per_file =
        std::min(fs_options->prefetch_concurrency,
                 fs_options->prefetch_concurrency_per_file);
    LOG_INFO("prefetch_concurrency changed from ` to `",
             old_prefetch_concurrency, fs_options->prefetch_concurrency);
  }

  if (fs_options->prefetch_chunks < 0 ||
      fs_options->prefetch_chunks * fs_options->prefetch_chunk_size >
          new_read_buffer_mem) {
    LOG_WARN("Set invalid prefetch_chunks ` to auto(0) when memory is limited.",
             fs_options->prefetch_chunks);
    fs_options->prefetch_chunks = 0;
  }

  return 0;
}

};  // namespace OssFileSystem
