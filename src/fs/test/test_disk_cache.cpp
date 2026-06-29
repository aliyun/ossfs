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

#include "fs/disk_cache.h"
#include "metric/metrics.h"
#include "test_suite.h"

class Ossfs2DiskCacheTest : public Ossfs2TestSuite {
 protected:
  void verify_init_disk_cache(int disk_cache_io_engine) {
    std::string cache_dir = "/root/tmp/ossfs2/cache";
    photon::Executor *executor = nullptr;
    OssFileSystem::BGVCpuDiskCacheEnv *bg_disk_cache_env = nullptr;

    uint64_t photon_io_init = photon::INIT_IO_NONE;
    int io_engine_type = random_disk_cache_io_engine(disk_cache_io_engine);
    if (io_engine_type == photon::fs::ioengine_libaio) {
      LOG_INFO("Using libaio IO engine");
      photon_io_init = photon::INIT_IO_LIBAIO;
    }

    auto test_init = [&](std::function<int()> &&init_func) -> int {
      std::filesystem::remove_all(cache_dir);
      bg_disk_cache_env = new OssFileSystem::BGVCpuDiskCacheEnv();
      executor =
          new photon::Executor(OSSFS_EVENT_ENGINE, photon_io_init,
                               LIBAIO_PHOTON_OPTION, EXECUTOR_QUEUE_OPTION);
      bg_disk_cache_env->set_executor(executor);

      DEFER(delete bg_disk_cache_env);
      return init_func();
    };

    OssFileSystem::DiskCacheOptions cache_opts(cache_dir, 1, 1024 * 1024,
                                               io_engine_type);
    int r = 0;
    r = test_init([&]() { return bg_disk_cache_env->init(cache_opts); });
    ASSERT_EQ(r, 0);

    // Test relative path.
    auto original_path = std::filesystem::current_path();
    std::filesystem::current_path("/root");
    r = test_init([&]() {
      auto opts = cache_opts;
      opts.cache_dir = "./tmp/ossfs2/cache";
      return bg_disk_cache_env->init(opts);
    });
    std::filesystem::current_path(original_path);
    ASSERT_EQ(r, 0);

    r = test_init([&]() {
      std::filesystem::remove_all("/root/tmp/ossfs2/");
      ::close(::open("/root/tmp/ossfs2", O_CREAT | O_RDWR, 0777));
      DEFER(::unlink("/root/tmp/ossfs2"));
      return bg_disk_cache_env->init(cache_opts);
    });
    ASSERT_NE(r, 0);

    // empty directory should init successfully
    r = test_init([&]() { return bg_disk_cache_env->init(cache_opts); });
    ASSERT_EQ(r, 0);
  }

  void verify_disk_cache_eviction_when_full() {
    Metric::set_enabled_metrics("all");
    DEFER(Metric::set_enabled_metrics(""));
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Named constants for test parameters.
    const uint64_t kFileSizeMB = 400;
    const uint64_t kHugeFileSizeMB = 2048;
    const int kConcurrency = 5;
    const size_t kPartialReadSize = 1024 * 1024;  // 1 MB
    // Metric collection requires a stabilization window: sleep before read
    // to separate from prior metrics, sleep after to let async metrics flush.
    const int kPreReadSleepSec = 2;
    const int kPostReadSleepSec = 1;

    enum ReadCacheResult { kError = -1, kCacheMiss = 0, kCacheHit = 1 };

    uint64_t nodeid = 0, crc = 0;
    std::vector<uint64_t> nodeids;
    std::vector<uint64_t> crcs;
    std::vector<std::string> filenames;
    DEFER({
      for (auto nodeid : nodeids) fs_->forget(nodeid, 1);
    });

    // Reads the file at |file_index|, verifies CRC, and returns whether the
    // read was served from disk cache (kCacheHit) or from OSS (kCacheMiss).
    // Returns kError on read failure or CRC mismatch.
    auto read_and_check_cache_hit = [&](int file_index) -> ReadCacheResult {
      std::this_thread::sleep_for(std::chrono::seconds(kPreReadSleepSec));
      auto start = std::chrono::steady_clock::now();
      uint64_t crc64 = 0;
      ssize_t r = read_file_in_folder(parent, filenames[file_index], &crc64);
      std::this_thread::sleep_for(std::chrono::seconds(kPostReadSleepSec));
      auto cost = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - start)
                      .count();
      auto cost_second = (cost + 1'000'000 - 1) / 1'000'000;
      auto metrics_map = Metric::get_metrics_map(cost_second);
      if (crc64 != crcs[file_index] || r < 0) {
        LOG_ERROR(
            "read_and_check_cache_hit failed: file_index=`, r=`, crc64=`, "
            "expected_crc64=`",
            file_index, r, crc64, crcs[file_index]);
        return kError;
      }
      return metrics_map["oss_read_cnt"] == 0 ? kCacheHit : kCacheMiss;
    };

    // Create file0, first read populates cache, second read hits.
    filenames.push_back("testfile0");
    crc = create_file_in_folder(parent, filenames[0], kFileSizeMB, nodeid);
    nodeids.push_back(nodeid);
    crcs.push_back(crc);
    LOG_INFO("create file `", filenames[0]);
    ASSERT_EQ(read_and_check_cache_hit(0), kCacheMiss);
    ASSERT_EQ(read_and_check_cache_hit(0), kCacheHit);

    // Create file1, same pattern.
    filenames.push_back("testfile1");
    crc = create_file_in_folder(parent, filenames[1], kFileSizeMB, nodeid);
    nodeids.push_back(nodeid);
    crcs.push_back(crc);
    ASSERT_EQ(read_and_check_cache_hit(1), kCacheMiss);
    ASSERT_EQ(read_and_check_cache_hit(1), kCacheHit);

    // Both file0 and file1 should still be cached.
    ASSERT_EQ(read_and_check_cache_hit(0), kCacheHit);
    ASSERT_EQ(read_and_check_cache_hit(1), kCacheHit);

    // Create file2: cache is now full, file0 and file1 get evicted.
    filenames.push_back("testfile2");
    crc = create_file_in_folder(parent, filenames[2], kFileSizeMB, nodeid);
    nodeids.push_back(nodeid);
    crcs.push_back(crc);
    ASSERT_EQ(read_and_check_cache_hit(2), kCacheMiss);
    ASSERT_EQ(read_and_check_cache_hit(2), kCacheHit);

    // file0 and file1 were evicted, so they require re-fetch from OSS.
    ASSERT_EQ(read_and_check_cache_hit(0), kCacheMiss);
    ASSERT_EQ(read_and_check_cache_hit(1), kCacheMiss);

    // Partial read (1MB) of file2 should not trigger eviction of file0/file1.
    void *handle = nullptr;
    bool unused;
    int r = fs_->open(nodeids[2], O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    char buf[kPartialReadSize];
    r = read_from_handle(handle, buf, kPartialReadSize, 0);
    ASSERT_EQ(r, (ssize_t)kPartialReadSize);
    r = fs_->release(nodeids[2], get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    ASSERT_EQ(read_and_check_cache_hit(0), kCacheHit);
    ASSERT_EQ(read_and_check_cache_hit(1), kCacheHit);

    // Concurrent reads of cached files should not crash or corrupt.
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < kConcurrency; i++) {
      auto task = std::async(std::launch::async, [&]() {
        INIT_PHOTON();
        ASSERT_NE(read_and_check_cache_hit(rand() % 3), kError);
      });
      tasks.push_back(std::move(task));
    }
    for (auto &task : tasks) task.wait();

    // Read a huge file (2GB) repeatedly -- verifies eviction under pressure.
    filenames.push_back("huge-file");
    crc = create_file_in_folder(parent, filenames[3], kHugeFileSizeMB, nodeid);
    nodeids.push_back(nodeid);
    crcs.push_back(crc);

    tasks.clear();
    for (int i = 0; i < kConcurrency; i++) {
      auto task = std::async(std::launch::async, [&]() {
        INIT_PHOTON();
        ASSERT_NE(read_and_check_cache_hit(3), kError);
      });
      tasks.push_back(std::move(task));
    }
    for (auto &task : tasks) task.wait();
  }

  void verify_disk_cache_mem_usage(uint64_t file_num) {
    // Test Mem Usage
    LOG_INFO("Begin to test mem usage for disk cache with ` files", file_num);
    auto before_mem_usage = get_physical_memory_KiB();
    LOG_INFO("Physical memory usage: ` KiB.", before_mem_usage);

    // Create disk cache files.
    for (uint64_t i = 0; i < file_num; i++) {
      auto name = "test_file" + std::to_string(i);
      auto cache = fs_->create_inode_cache();
      auto h = cache->get(name, "");
      cache->release(h, 0);
      if (i % 100'000 == 0) {
        LOG_INFO("Created ` files, physical mem usage: ` KiB.", i + 1,
                 get_physical_memory_KiB());
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
    auto after_mem_usage = get_physical_memory_KiB();
    LOG_INFO("After creating disk cache files, physical mem usage: ` KiB.",
             after_mem_usage);
    LOG_INFO("Physical memory usage increase: ` KiB.",
             after_mem_usage - before_mem_usage);
  }

  void verify_disk_cache_key_collision() {
    srand(time(nullptr));

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kFiles = 10;
    std::vector<uint64_t> nodeids(kFiles, 0);
    std::vector<uint64_t> crcs(kFiles, 0);
    std::vector<size_t> file_sizes(kFiles, 0);

    for (int i = 0; i < kFiles; i++) {
      std::string name = "collision_file_" + std::to_string(i);
      size_t file_sizes_MB = 10 + rand() % 500;
      file_sizes[i] = file_sizes_MB * 1024 * 1024;
      crcs[i] = create_file_in_folder(parent, name, file_sizes_MB, nodeids[i]);
      ASSERT_GT(crcs[i], 0ULL);
    }
    DEFER({
      for (int i = 0; i < kFiles; i++) fs_->forget(nodeids[i], 1);
    });

    // Append 1M to each file.
    const size_t MB = 1 << 20;
    std::vector<uint64_t> last_crcs(kFiles, 0);
    for (int i = 0; i < kFiles; i++) {
      void *handle = nullptr;
      bool unused;
      int r = fs_->open(nodeids[i], O_RDWR | O_APPEND, &handle, &unused);
      ASSERT_EQ(r, 0);

      auto buf = random_string(MB);
      r = fs_->write(nodeids[i], handle, buf.c_str(), MB, file_sizes[i]);
      ASSERT_EQ(r, (ssize_t)MB);
      file_sizes[i] += MB;

      r = fs_->release(nodeids[i], get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      last_crcs[i] = cal_crc64(0, (void *)buf.c_str(), MB);
      crcs[i] = cal_crc64(crcs[i], (void *)buf.c_str(), MB);
    }

    // Enable key-collision injection: multiple files are mapped to the same
    // small set of cache keys, triggering source_key-mismatch handling.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_DiskCache_Key_Collision);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_DiskCache_Key_Collision));

    // 20 concurrent readers for random files.
    const int kReaders = 20;
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < kReaders; i++) {
      tasks.push_back(std::async(std::launch::async, [&]() {
        INIT_PHOTON();
        int idx = rand() % kFiles;
        if (rand() % 2 == 0) {
          // read partial
          void *handle = nullptr;
          bool unused;
          int r = fs_->open(nodeids[idx], O_RDONLY, &handle, &unused);
          ASSERT_EQ(r, 0);

          if (rand() % 3 != 0) {
            char buf[MB];
            r = read_from_handle(handle, buf, MB, file_sizes[idx] - MB);
            ASSERT_EQ(r, (ssize_t)MB);
            ASSERT_EQ(cal_crc64(0, (void *)buf, MB), last_crcs[idx]);
          }

          std::this_thread::sleep_for(std::chrono::seconds(5));
          r = fs_->release(nodeids[idx], get_file_from_handle(handle));
          ASSERT_EQ(r, 0);
        } else {
          std::string name = "collision_file_" + std::to_string(idx);
          uint64_t out_crc = 0;
          ssize_t r = read_file_in_folder(parent, name, &out_crc);
          ASSERT_GT(r, 0);
          ASSERT_EQ(out_crc, crcs[idx]);
        }
      }));
    }

    // Check all files finally.
    for (int i = 0; i < kFiles; i++) {
      tasks.push_back(std::async(
          std::launch::async,
          [&](int idx) {
            INIT_PHOTON();
            std::string name = "collision_file_" + std::to_string(idx);
            uint64_t out_crc = 0;
            ssize_t r = read_file_in_folder(parent, name, &out_crc);
            ASSERT_GT(r, 0);
            ASSERT_EQ(out_crc, crcs[idx]);
          },
          i));
    }
    for (auto &t : tasks) t.wait();
  }

  void verify_disk_cache_with_network_error() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // --- Scenario 1: read recovery and cache hit ---
    // Upload files, verify read fails during network outage, succeeds after
    // recovery, and subsequent reads hit the cache even with permanent failure.
    const int file_count = 2;
    std::vector<std::string> filenames;
    std::vector<uint64_t> crcs(file_count, 0);
    for (int i = 0; i < file_count; i++) {
      filenames.push_back("test_net_error_" + std::to_string(i));
      std::string local_file = join_paths(test_path_, filenames[i]);
      create_random_file(local_file, 1, i + 1);
      ASSERT_EQ(upload_file(local_file, join_paths(parent_path, filenames[i]),
                            FLAGS_oss_bucket_prefix),
                0);
    }

    // Open first file, inject timeout, verify read fails.
    struct stat st;
    uint64_t nid0 = 0;
    ASSERT_EQ(fs_->lookup(parent, filenames[0].c_str(), &nid0, &st), 0);
    DEFER(fs_->forget(nid0, 1));

    void *handle = nullptr;
    bool unused = false;
    ASSERT_EQ(fs_->open(nid0, O_RDONLY, &handle, &unused), 0);
    DEFER(fs_->release(nid0, get_file_from_handle(handle)));

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout,
                                    FaultInjection(5, 0));
    char buf[4096];
    ASSERT_LT(read_from_handle(handle, buf, sizeof(buf), 0), (ssize_t)0);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    // After recovery, reads should succeed and populate cache.
    for (int i = 0; i < file_count; i++) {
      ssize_t sz = read_file_in_folder(parent, filenames[i], &crcs[i]);
      ASSERT_GT(sz, (ssize_t)0);
    }

    // Re-lookup and verify cached reads survive permanent failure.
    std::vector<uint64_t> cached_nids(file_count, 0);
    for (int i = 0; i < file_count; i++) {
      ASSERT_EQ(fs_->lookup(parent, filenames[i].c_str(), &cached_nids[i], &st),
                0);
    }
    DEFER({
      for (auto nid : cached_nids) {
        if (nid) fs_->forget(nid, 1);
      }
    });

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);
    for (int i = 0; i < file_count; i++) {
      uint64_t crc64 = 0;
      ASSERT_GT(read_file_in_folder(parent, filenames[i], &crc64), (ssize_t)0);
      ASSERT_EQ(crc64, crcs[i]);
    }
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    // --- Scenario 2: write retry on transient failure ---
    // Write data, inject failures fewer than retry_times, release should
    // succeed via retry, and read-back should match.
    std::string wr_file = "test_write_retry";
    uint64_t wr_nid = 0;
    void *wr_handle = nullptr;
    ASSERT_EQ(create_and_flush(parent, wr_file.c_str(), CREATE_BASE_FLAGS, 0777,
                               0, 0, 0, &wr_nid, &st, &wr_handle),
              0);
    DEFER(fs_->forget(wr_nid, 1));

    const size_t data_size = 1024 * 1024;
    std::string write_data = random_string(data_size);
    ASSERT_EQ(write_to_file_handle(wr_handle, write_data.c_str(), data_size, 0),
              (ssize_t)data_size);

    int fail_count =
        oss_options_.retry_times > 1 ? oss_options_.retry_times - 1 : 1;
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                    FaultInjection(fail_count, 0));
    ASSERT_EQ(fs_->release(wr_nid, get_file_from_handle(wr_handle)), 0);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    uint64_t wr_crc = 0;
    ASSERT_EQ(read_file_in_folder(parent, wr_file, &wr_crc),
              (ssize_t)data_size);
    ASSERT_EQ(wr_crc, cal_crc64(0, (void *)write_data.c_str(), data_size));
  }

  void verify_disk_cache_rehash_on_collision() {
    // Enable key-collision injection before any cache operations.
    // This forces all cache keys to be mapped to hash(name) % 5,
    // creating only 5 possible base keys.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_DiskCache_Key_Collision);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_DiskCache_Key_Collision));

    const int kFiles = 10;

    // Phase 1: Verify rehash produces valid cache handles.
    // Each file gets its own DiskCache. With collision injection,
    // multiple managers competing for the same base key will be rehashed
    // to alternative paths (base_key_1, base_key_2, base_key_3).
    {
      std::vector<std::shared_ptr<ICache>> managers;
      std::vector<CacheHandle *> handles;

      for (int i = 0; i < kFiles; i++) {
        std::string name = "rehash_test_" + std::to_string(i);
        auto mgr = fs_->create_inode_cache();
        ASSERT_NE(mgr, nullptr);
        auto h = mgr->get(name, "");
        // Core assertion: rehash must succeed for all files.
        // Without rehash, colliding files would get invalid handles
        // (CacheHandle(nullptr, nullptr)); with rehash, each collision is
        // resolved to an alternative path.
        ASSERT_TRUE(h);
        managers.push_back(mgr);
        handles.push_back(h);
      }

      // Release all handles
      for (int i = 0; i < kFiles; i++) {
        managers[i]->release(handles[i], 0);
      }
    }

    // Phase 2: Verify data correctness under collision + rehash.
    // Create actual files with distinct content and read them back through
    // the FS layer to confirm that rehash does not corrupt cached data.
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::vector<uint64_t> nodeids(kFiles, 0);
    std::vector<uint64_t> crcs(kFiles, 0);

    for (int i = 0; i < kFiles; i++) {
      std::string name = "rehash_data_" + std::to_string(i);
      size_t size_MB = 100 + rand() % 50;
      crcs[i] = create_file_in_folder(parent, name, size_MB, nodeids[i]);
      ASSERT_GT(crcs[i], 0ULL);
    }
    DEFER({
      for (int i = 0; i < kFiles; i++) fs_->forget(nodeids[i], 1);
    });

    // Read each file and verify CRC
    for (int i = 0; i < kFiles; i++) {
      std::string name = "rehash_data_" + std::to_string(i);
      uint64_t out_crc = 0;
      ssize_t r = read_file_in_folder(parent, name, &out_crc);
      ASSERT_GT(r, 0);
      ASSERT_EQ(out_crc, crcs[i]);
    }
  }
};

TEST_F(Ossfs2DiskCacheTest, verify_init_disk_cache) {
  INIT_PHOTON();
  LOG_INFO("verify_init_disk_cache with psync IO engine");
  verify_init_disk_cache(photon::fs::ioengine_psync);
  LOG_INFO("verify_init_disk_cache with libaio IO engine");
  verify_init_disk_cache(photon::fs::ioengine_libaio);
}

TEST_F(Ossfs2DiskCacheTest, DISABLED_verify_disk_cache_mem_usage) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;

  LOG_INFO("Test disk cache mem usage with 1000w files");
  init(opts);
  verify_disk_cache_mem_usage(10'000'000);
  destroy();

  LOG_INFO("Test disk cache mem usage with 2000w files");
  init(opts);
  verify_disk_cache_mem_usage(20'000'000);
  destroy();

  LOG_INFO("Test disk cache mem usage with 1e8 files");
  init(opts);
  verify_disk_cache_mem_usage(100'000'000);
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_eviction_when_full) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  init(opts);

  verify_disk_cache_eviction_when_full();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_key_collision) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  init(opts);

  verify_disk_cache_key_collision();
}

// Test: disk cache behaviour under various network error conditions.
TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_with_network_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  init(opts);
  verify_disk_cache_with_network_error();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_rehash_on_collision) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  init(opts);
  verify_disk_cache_rehash_on_collision();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_psync_eviction) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  init(opts, -1, "", false, photon::fs::ioengine_psync);

  verify_disk_cache_eviction_when_full();
}
