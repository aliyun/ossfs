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

#include <fcntl.h>
#include <photon/thread/thread.h>
#include <photon/thread/thread11.h>
#include <unistd.h>

#include <cstring>
#include <future>

#include "fs/disk_cache.h"
#include "fs/file.h"
#include "fs/file_reader.h"
#include "metric/metrics.h"
#include "test_suite.h"

namespace {
// Deterministic per-position byte (splitmix64): the written stream is
// replayable, so any read can be verified byte-exactly against the offset.
uint8_t det_byte(size_t pos) {
  uint64_t x = pos * 0x9E3779B97F4A7C15ULL + 0x123456789ABCDEFULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return static_cast<uint8_t>(x ^ (x >> 31));
}
}  // namespace

class Ossfs2DiskCacheTest : public Ossfs2TestSuite {
 protected:
  enum ReadCacheResult { kError = -1, kCacheMiss = 0, kCacheHit = 1 };

  ReadCacheResult read_file_and_check_cache_hit(uint64_t parent,
                                                const std::string &filename,
                                                uint64_t expected_crc) {
    Metric::set_enabled_metrics("all");
    DEFER(Metric::set_enabled_metrics(""));
    // Metric collection requires a stabilization window: sleep before read
    // to separate from prior metrics, sleep after to let async metrics flush.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto start = std::chrono::steady_clock::now();
    uint64_t crc64 = 0;
    ssize_t r = read_file_in_folder(parent, filename, &crc64);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto cost = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
    auto cost_second = (cost + 1'000'000 - 1) / 1'000'000;
    auto metrics_map = Metric::get_metrics_map(cost_second);
    if (r < 0 || crc64 != expected_crc) {
      LOG_ERROR(
          "read_file_and_check_cache_hit failed: file=`, r=`, crc64=`, "
          "expected_crc64=`",
          filename, r, crc64, expected_crc);
      return kError;
    }
    return metrics_map["oss_read_cnt"] == 0 ? kCacheHit : kCacheMiss;
  }

  void SetUp() override {
    SET_TEST_MODE(kTestOss);
    Ossfs2TestSuite::SetUp();
  }

  void verify_init_disk_cache(int disk_cache_io_engine) {
    auto case_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::string cache_dir = FLAGS_disk_cache_dir + "/" + std::string(case_name);
    OssFileSystem::BGVCpuDiskCacheEnv *bg_disk_cache_env = nullptr;

    uint64_t photon_io_init = photon::INIT_IO_NONE;
    int io_engine_type = random_disk_cache_io_engine(disk_cache_io_engine);
    if (io_engine_type == photon::fs::ioengine_libaio) {
      LOG_INFO("Using libaio IO engine");
      photon_io_init = photon::INIT_IO_LIBAIO;
    }

    auto make_env = [&]() {
      auto env = new OssFileSystem::BGVCpuDiskCacheEnv();
      auto executor =
          new photon::Executor(OSSFS_EVENT_ENGINE, photon_io_init,
                               LIBAIO_PHOTON_OPTION, EXECUTOR_QUEUE_OPTION);
      env->add_executor(executor);
      return env;
    };

    auto test_init = [&](std::function<int()> &&init_func) -> int {
      std::filesystem::remove_all(cache_dir);
      bg_disk_cache_env = make_env();

      DEFER(delete bg_disk_cache_env);
      return init_func();
    };

    OssFileSystem::DiskCacheOptions cache_opts(cache_dir, 1, 1024 * 1024,
                                               io_engine_type);
    int r = 0;
    r = test_init([&]() { return bg_disk_cache_env->init(cache_opts); });
    ASSERT_EQ(r, 0);

    r = test_init([&]() {
      std::filesystem::remove_all(cache_dir);
      ::close(::open(cache_dir.c_str(), O_CREAT | O_RDWR, 0777));
      DEFER(::unlink(cache_dir.c_str()));
      return bg_disk_cache_env->init(cache_opts);
    });
    ASSERT_NE(r, 0);

    // empty directory should init successfully
    r = test_init([&]() { return bg_disk_cache_env->init(cache_opts); });
    ASSERT_EQ(r, 0);

    // Exclusive dir lock: while one instance holds the cache dir, a second
    // instance on the same dir must fail; init succeeds again after the
    // first instance is destroyed.
    std::filesystem::remove_all(cache_dir);
    auto first_env = make_env();
    ASSERT_EQ(first_env->init(cache_opts), 0);

    // New layout: lock file at the cache dir root, cached data in the
    // cache-data sub directory.
    const std::string lock_path = cache_dir + "/.ossfs2.lock";
    const std::string data_dir = cache_dir + "/cache-data";
    ASSERT_TRUE(std::filesystem::exists(lock_path));
    ASSERT_TRUE(std::filesystem::is_directory(data_dir));
    // The legacy sibling lock file "<dir>.ossfs2.lock" must not be created.
    ASSERT_FALSE(std::filesystem::exists(cache_dir + ".ossfs2.lock"));

    auto second_env = make_env();
    ASSERT_NE(second_env->init(cache_opts), 0);
    delete second_env;

    delete first_env;
    auto third_env = make_env();
    ASSERT_EQ(third_env->init(cache_opts), 0);
    delete third_env;

    // xattr probe failures must reject init.
    const std::string probe_path = data_dir + "/.ossfs2_xattr_probe";

    // Probe path occupied by a directory: open(O_CREAT|O_RDWR) fails with
    // EISDIR, probe fails and init must be rejected.
    std::filesystem::remove_all(cache_dir);
    std::filesystem::create_directories(probe_path);
    {
      auto *probe_env = make_env();
      DEFER(delete probe_env);
      ASSERT_NE(probe_env->init(cache_opts), 0);
    }
    ASSERT_TRUE(std::filesystem::exists(probe_path));

    // Pre-existing regular probe file: init succeeds and the file is kept
    // (only a probe file created by us is removed).
    std::filesystem::remove_all(cache_dir);
    std::filesystem::create_directories(data_dir);
    ::close(::open(probe_path.c_str(), O_CREAT | O_RDWR, 0644));
    {
      auto *probe_env = make_env();
      DEFER(delete probe_env);
      ASSERT_EQ(probe_env->init(cache_opts), 0);
    }
    ASSERT_TRUE(std::filesystem::exists(probe_path));

    // Lock file path occupied by a directory: open fails and init must be
    // rejected before touching the cache data.
    std::filesystem::remove_all(cache_dir);
    std::filesystem::create_directories(lock_path);
    {
      auto *probe_env = make_env();
      DEFER(delete probe_env);
      ASSERT_NE(probe_env->init(cache_opts), 0);
    }
  }

  // With an empty etag the disk cache identity falls back to
  // (object_key, size, mtime): the same identity hits across cache instances,
  // a different mtime or size misses, and a non-empty etag ignores both.
  void verify_disk_cache_mtime_fallback_identity() {
    auto case_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::string cache_dir = FLAGS_disk_cache_dir + "/" + std::string(case_name);
    std::filesystem::remove_all(cache_dir);

    auto env = new OssFileSystem::BGVCpuDiskCacheEnv();
    DEFER(delete env);
    auto executor =
        new photon::Executor(OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE,
                             LIBAIO_PHOTON_OPTION, EXECUTOR_QUEUE_OPTION);
    env->add_executor(executor);
    OssFileSystem::DiskCacheOptions cache_opts(cache_dir, 1, 1024 * 1024,
                                               photon::fs::ioengine_psync);
    ASSERT_EQ(env->init(cache_opts), 0);

    const size_t kBlockSize = 1024 * 1024;
    auto pool = std::make_shared<OssFileSystem::FixedBlockMemoryPool>(
        kBlockSize, 16 << 20, 64, 60'000);

    const std::string path = "mtime-fallback-file";
    const struct timespec mt1 {
      1000, 500000000
    };
    const struct timespec mt2 {
      2000, 0
    };
    const char kData[] = "v1-data!";
    const size_t kDataLen = sizeof(kData) - 1;

    // Stages kData at offset 0 through a cache instance for 'key'.
    auto write_through = [&](const OssFileSystem::CacheKey &key) {
      OssFileSystem::DiskCache cache(env, pool);
      auto *h = cache.get(key);
      ASSERT_NE(h, nullptr);
      DEFER(cache.release(h, 0));
      OssFileSystem::RangeBuffer rb;
      rb.offset = 0;
      rb.count = kDataLen;
      ASSERT_EQ(h->acquire_write_buffer(rb), 0);
      memcpy(rb.buffer.iovec()[0].iov_base, kData, kDataLen);
      h->release_write_buffer(rb);
    };

    // Reads back through a fresh cache instance: read size on a hit,
    // -ENOENT on a miss, -EIO on mismatch or failure.
    auto read_back = [&](const OssFileSystem::CacheKey &key) -> ssize_t {
      OssFileSystem::DiskCache cache(env, pool);
      auto *h = cache.get(key);
      if (h == nullptr) return -EIO;
      DEFER(cache.release(h, 0));
      char buf[kDataLen];
      ssize_t r = h->pread(buf, 0, kDataLen);
      if (r < 0) return r;
      if (r != (ssize_t)kDataLen || memcmp(buf, kData, kDataLen) != 0) {
        return -EIO;
      }
      return r;
    };

    // 1. Empty etag: size and mtime carry the version.
    write_through({path, "", mt1, kDataLen});
    ASSERT_EQ(read_back({path, "", mt1, kDataLen}), (ssize_t)kDataLen);
    ASSERT_EQ(read_back({path, "", mt2, kDataLen}), (ssize_t)-ENOENT);
    ASSERT_EQ(read_back({path, "", mt1, kDataLen + 1}), (ssize_t)-ENOENT);

    // 2. Non-empty etag: size and mtime are not part of the identity.
    write_through({path, "etag-x", mt1, kDataLen});
    ASSERT_EQ(read_back({path, "etag-x", mt2, kDataLen}), (ssize_t)kDataLen);
    ASSERT_EQ(read_back({path, "etag-x", mt1, kDataLen + 1}),
              (ssize_t)kDataLen);
    ASSERT_EQ(read_back({path, "etag-y", mt1, kDataLen}), (ssize_t)-ENOENT);
  }

  void verify_disk_cache_eviction_when_full() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Named constants for test parameters.
    const uint64_t kFileSizeMB = 400;
    const uint64_t kHugeFileSizeMB = 2048;
    const int kConcurrency = 5;
    const size_t kPartialReadSize = 1024 * 1024;  // 1 MB

    uint64_t nodeid = 0, crc = 0;
    std::vector<uint64_t> nodeids;
    std::vector<uint64_t> crcs;
    std::vector<std::string> filenames;
    DEFER({
      for (auto nodeid : nodeids) fs_->forget(nodeid, 1);
    });

    auto read_and_check_cache_hit = [&](int file_index) -> ReadCacheResult {
      return read_file_and_check_cache_hit(parent, filenames[file_index],
                                           crcs[file_index]);
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
      auto cache = fs_->create_inode_cache(0);
      auto h = cache->get({name, ""});
      ASSERT_NE(h, nullptr);
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
        auto mgr = fs_->create_inode_cache(0);
        ASSERT_NE(mgr, nullptr);
        auto h = mgr->get({name, ""});
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

  void verify_disk_cache_drop_rejects_stale_refill(bool reopen_fail = false) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const std::string filename = "stale-refill";
    const size_t file_size = 1024 * 1024;
    const std::string old_data(file_size, 'a');
    const std::string new_data(file_size, 'b');

    uint64_t nodeid = 0;
    struct stat st;
    void *handle = nullptr;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    r = write_to_file_handle(handle, old_data.data(), old_data.size(), 0);
    ASSERT_EQ(r, static_cast<ssize_t>(old_data.size()));
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    void *reader_handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &reader_handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(reader_handle)));

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Do_Refill_Range_Delay_Before_Release);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Do_Refill_Range_Delay_Before_Release));

    std::string first_read(file_size, '\0');
    auto read_task = std::async(std::launch::async, [&]() {
      INIT_PHOTON();
      read_from_handle(reader_handle, first_read.data(), first_read.size(), 0);
    });
    DEFER(read_task.get());

    std::this_thread::sleep_for(std::chrono::seconds(1));

    void *writer_handle = nullptr;
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &writer_handle, &unused);
    ASSERT_EQ(r, 0);
    r = write_to_file_handle(writer_handle, new_data.data(), new_data.size(),
                             0);
    ASSERT_EQ(r, static_cast<ssize_t>(new_data.size()));
    r = fs_->release(nodeid, get_file_from_handle(writer_handle));
    ASSERT_EQ(r, 0);

    struct stat updated_st;
    r = fs_->getattr(nodeid, &updated_st);
    ASSERT_EQ(r, 0);

    if (reopen_fail) {
      g_fault_injector->set_injection(
          FaultInjectionId::FI_DiskCache_Init_Failure);
    }
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_DiskCache_Init_Failure));

    std::string new_read(file_size, '\0');
    r = read_from_handle(reader_handle, new_read.data(), new_read.size(), 0);
    ASSERT_EQ(r, static_cast<ssize_t>(file_size));
    auto read_crc64 = cal_crc64(0, (void *)new_read.c_str(), file_size);
    auto expected_crc64 = cal_crc64(0, (void *)new_data.c_str(), file_size);
    ASSERT_EQ(read_crc64, expected_crc64);
  }

  void verify_prefetch_eviction_for_large_file() {
    // Test: eviction detection limits cache misses when file > cache size.
    Metric::set_enabled_metrics("oss");
    DEFER(Metric::set_enabled_metrics(""));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto metric_start = std::chrono::steady_clock::now();

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // 2GB file, 1GB cache → eviction is guaranteed.
    const uint64_t kFileSizeMB = 2048;
    uint64_t nodeid = 0;
    uint64_t crc_expected =
        create_file_in_folder(parent, "evict_bounded", kFileSizeMB, nodeid);
    ASSERT_GT(crc_expected, 0ULL);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &handle, &unused), 0);

    auto oss_file = dynamic_cast<OssFileHandle *>(get_file_from_handle(handle));
    ASSERT_NE(oss_file, nullptr);
    auto reader = dynamic_cast<OssCachedReader *>(oss_file->reader_.get());
    ASSERT_NE(reader, nullptr);

    const size_t kReadSize = 1024 * 1024;  // 1MB per read
    std::vector<char> buf(kReadSize);
    uint64_t crc = 0;
    size_t cache_miss_cnt = 0;
    off_t off = 0;
    ssize_t total = static_cast<ssize_t>(kFileSizeMB) * 1024 * 1024;

    while (off < total) {
      size_t to_read = std::min(kReadSize, static_cast<size_t>(total - off));
      // Try cache read directly; failure means cache miss.
      ssize_t r = reader->cache_handle_->pread(buf.data(), off, to_read);
      if (r <= 0) cache_miss_cnt++;
      // Additional pread to trigger prefetching.
      r = reader->pread(buf.data(), to_read, off);
      ASSERT_EQ(r, static_cast<ssize_t>(to_read));
      crc = cal_crc64(crc, buf.data(), to_read);
      off += r;
    }

    ASSERT_EQ(crc, crc_expected);
    ASSERT_EQ(fs_->release(nodeid, oss_file), 0);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - metric_start)
                           .count() +
                       1;
    auto metrics = Metric::get_metrics_map(elapsed_sec);
    uint64_t oss_read_bytes = metrics["oss_read_bytes"];
    uint64_t oss_read_cnt = metrics["oss_get_object_range_cnt"];
    LOG_INFO("OSS total read traffic: ` MB, GET requests: `",
             oss_read_bytes / 1024 / 1024, oss_read_cnt);

    size_t total_reads = kFileSizeMB;
    LOG_INFO("cache miss cnt: `/`, miss rate: `%", cache_miss_cnt, total_reads,
             cache_miss_cnt * 100 / total_reads);
    // With eviction detection working, miss rate should be well under 10%.
    ASSERT_LT(cache_miss_cnt, total_reads / 10);
  }

  void verify_prefetch_eviction_for_multi_files() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create five 400MB files.
    const uint64_t kFileMB = 400;
    const int kFileCount = 5;

    std::vector<uint64_t> nodeids(kFileCount, 0);
    std::vector<uint64_t> crcs(kFileCount, 0);
    for (int i = 0; i < kFileCount; i++) {
      std::string name = "file_" + std::to_string(i);
      crcs[i] = create_file_in_folder(parent, name, kFileMB, nodeids[i]);
      ASSERT_GT(crcs[i], 0ULL);
    }

    DEFER({
      for (int i = 0; i < kFileCount; i++) fs_->forget(nodeids[i], 1);
    });

    for (int i = 0; i < kFileCount; i++) {
      uint64_t out_crc = 0;
      ssize_t r =
          read_file_in_folder(parent, "file_" + std::to_string(i), &out_crc);
      ASSERT_GT(r, 0);
      ASSERT_EQ(out_crc, crcs[i]);
    }

    std::vector<std::future<bool>> tasks;
    for (int i = 0; i < 10; i++) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      tasks.push_back(std::async(std::launch::async, [&]() -> bool {
        INIT_PHOTON();
        thread_local std::mt19937 rng(std::random_device{}());
        int index = rng() % kFileCount;
        uint64_t out_crc = 0;
        ssize_t r = read_file_in_folder(parent, "file_" + std::to_string(index),
                                        &out_crc);
        return r == kFileMB * 1024 * 1024 && out_crc == crcs[index];
      }));
    }
    for (auto &t : tasks) {
      ASSERT_EQ(t.get(), true);
    }
  }

  void verify_disk_cache_drop_reopens_on_same_key() {
    const std::string object_key = "drop-reopens/file";
    const std::string etag = "etag-v1";
    const size_t data_size = 1024 * 1024;
    std::string data = random_string(data_size);

    auto cache = fs_->create_inode_cache(0);
    ASSERT_NE(cache, nullptr);
    auto *h = cache->get({object_key, etag});
    ASSERT_NE(h, nullptr);
    DEFER(cache->release(h, 0));

    // Populate the cache file through the write buffer path.
    auto populate = [&]() {
      RangeBuffer rb;
      rb.offset = 0;
      rb.count = data_size;
      ASSERT_EQ(h->acquire_write_buffer(rb), 0);
      size_t copied = 0;
      for (size_t i = 0; i < static_cast<size_t>(rb.buffer.iovcnt()); i++) {
        memcpy(rb.buffer[i].iov_base, data.data() + copied,
               rb.buffer[i].iov_len);
        copied += rb.buffer[i].iov_len;
      }
      ASSERT_EQ(copied, data_size);
      h->release_write_buffer(rb);
    };

    std::vector<char> buf(data_size);
    populate();
    ASSERT_EQ(h->pread(buf.data(), 0, data_size),
              static_cast<ssize_t>(data_size));
    ASSERT_EQ(memcmp(buf.data(), data.data(), data_size), 0);

    // Drop with an unchanged identity still discards the cached data.
    h->drop({object_key, etag});
    ASSERT_EQ(h->pread(buf.data(), 0, data_size), -ENOENT);

    // Repopulate, then drop with a different identity discards too.
    populate();
    ASSERT_EQ(h->pread(buf.data(), 0, data_size),
              static_cast<ssize_t>(data_size));
    h->drop({object_key, "etag-v2"});
    ASSERT_EQ(h->pread(buf.data(), 0, data_size), -ENOENT);

    // force=true has no special handling on the disk cache.
    populate();
    ASSERT_EQ(h->pread(buf.data(), 0, data_size),
              static_cast<ssize_t>(data_size));
    h->drop({object_key, "etag-v2"}, /*force=*/true);
    ASSERT_EQ(h->pread(buf.data(), 0, data_size), -ENOENT);
  }

  void verify_disk_cache_max_file_size() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // File sizes are chosen relative to the 100MB limit configured by the
    // caller test.
    const uint64_t kSmallFileSizeMB = 50;
    const uint64_t kLargeFileSizeMB = 200;
    const size_t kPartialReadSize = 1 * 1024 * 1024;  // 1 MB

    // Opens |nodeid| once, reads the same byte range twice without closing
    // the file in between, and returns whether the second read was served
    // from cache (kCacheHit) or re-fetched from OSS (kCacheMiss).
    auto read_twice_within_open_and_check_hit =
        [&](uint64_t nodeid) -> ReadCacheResult {
      Metric::set_enabled_metrics("all");
      DEFER(Metric::set_enabled_metrics(""));
      void *handle = nullptr;
      bool unused = false;
      int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
      if (r < 0) return kError;
      DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

      std::vector<char> buf(kPartialReadSize);
      r = read_from_handle(handle, buf.data(), kPartialReadSize, 0);
      if (r != static_cast<ssize_t>(kPartialReadSize)) return kError;

      std::this_thread::sleep_for(std::chrono::seconds(2));
      auto start = std::chrono::steady_clock::now();
      r = read_from_handle(handle, buf.data(), kPartialReadSize, 0);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto cost = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - start)
                      .count();
      auto cost_second = (cost + 1'000'000 - 1) / 1'000'000;
      auto metrics_map = Metric::get_metrics_map(cost_second);
      if (r != static_cast<ssize_t>(kPartialReadSize)) return kError;
      return metrics_map["oss_read_cnt"] == 0 ? kCacheHit : kCacheMiss;
    };

    // Small file (below max_file_size): first read misses, second hits.
    uint64_t small_nodeid = 0;
    uint64_t small_crc = create_file_in_folder(parent, "small_file",
                                               kSmallFileSizeMB, small_nodeid);
    ASSERT_GT(small_crc, 0ULL);
    DEFER(fs_->forget(small_nodeid, 1));
    ASSERT_EQ(read_file_and_check_cache_hit(parent, "small_file", small_crc),
              kCacheMiss);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, "small_file", small_crc),
              kCacheHit);

    // Large file (above max_file_size): bypasses the persistent disk cache,
    // so separate opens never hit (nothing survives across opens).
    uint64_t large_nodeid = 0;
    uint64_t large_crc = create_file_in_folder(parent, "large_file",
                                               kLargeFileSizeMB, large_nodeid);
    ASSERT_GT(large_crc, 0ULL);
    DEFER(fs_->forget(large_nodeid, 1));
    ASSERT_EQ(read_file_and_check_cache_hit(parent, "large_file", large_crc),
              kCacheMiss);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, "large_file", large_crc),
              kCacheMiss);

    // But within a single open, re-reading the same range should still hit
    // an in-memory block cache.
    ASSERT_EQ(read_twice_within_open_and_check_hit(large_nodeid), kCacheHit);
  }

  void verify_disk_cache_switch_disk_to_mem_when_size_grows() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // File sizes are chosen relative to the 8MB limit configured by the
    // caller test: the file starts below the limit, then a local write
    // grows it above the limit.
    const uint64_t kSmallFileSizeMB = 4;
    const std::string filename = "grow_switch_disk_to_mem";

    uint64_t nodeid = 0;
    uint64_t small_crc =
        create_file_in_folder(parent, filename, kSmallFileSizeMB, nodeid);
    ASSERT_GT(small_crc, 0ULL);
    DEFER(fs_->forget(nodeid, 1));

    // Below the limit the file is served by the disk cache: miss then hit.
    ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, small_crc),
              kCacheMiss);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, small_crc),
              kCacheHit);

    // Grow the file past the limit and flush so mark_clean() sets
    // invalidate_data_cache.
    uint64_t grown_crc = 0;
    {
      void *fd = nullptr;
      bool unused = false;
      ASSERT_EQ(fs_->open(nodeid, O_RDWR, &fd, &unused), 0);
      DEFER(fs_->release(nodeid, get_file_from_handle(fd)));

      const size_t grow_size = 12 * 1024 * 1024;
      std::vector<char> grow_buf(grow_size, 'x');
      ASSERT_EQ(write_to_file_handle(fd, grow_buf.data(), grow_size,
                                     kSmallFileSizeMB * 1024 * 1024),
                static_cast<ssize_t>(grow_size));
      ASSERT_EQ(fs_->flush(nodeid, get_file_from_handle(fd)), 0);
      grown_crc = cal_crc64(small_crc, grow_buf.data(), grow_size);

      // While a handle is open the tier never switches: the flag-consuming
      // open drops the shared disk cache in place, so the first read misses
      // and refills the same instance, which persists across reopens.
      ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, grown_crc),
                kCacheMiss);
      ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, grown_crc),
                kCacheHit);
    }

    // The last handle is closed, so the next open rebuilds the inode cache
    // above the limit as a memory cache: it never persists across closes,
    // so every reopen misses (the disk tier hit on the second read above).
    ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, grown_crc),
              kCacheMiss);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, grown_crc),
              kCacheMiss);
  }

  void verify_disk_cache_switch_mem_to_disk_when_size_shrinks() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // File sizes are chosen relative to the 100MB limit configured by the
    // caller test: the file starts above it, then fd2's O_TRUNC rewrite
    // shrinks it below the limit.
    const uint64_t kLargeFileSizeMB = 200;
    const uint64_t kSmallFileSizeMB = 50;
    const size_t kPartialReadSize = 1 * 1024 * 1024;  // 1 MB
    const std::string filename = "shrink_mem_to_disk";

    uint64_t nodeid = 0;
    uint64_t large_crc =
        create_file_in_folder(parent, filename, kLargeFileSizeMB, nodeid);
    ASSERT_GT(large_crc, 0ULL);
    DEFER(fs_->forget(nodeid, 1));

    const size_t small_size = kSmallFileSizeMB * 1024 * 1024;
    std::vector<char> small_buf(small_size, 'x');
    uint64_t shrunk_crc = 0;
    {
      // fd1: open the oversized file; its reader is bound to the memory
      // tier. Read a partial range so the memory tier holds it.
      void *fd1 = nullptr;
      bool unused = false;
      ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &fd1, &unused), 0);
      DEFER(fs_->release(nodeid, get_file_from_handle(fd1)));
      std::vector<char> buf1(kPartialReadSize);
      ASSERT_EQ(read_from_handle(fd1, buf1.data(), kPartialReadSize, 0),
                static_cast<ssize_t>(kPartialReadSize));

      // fd2: truncate the file and rewrite it below the limit, then flush
      // so mark_clean() sets invalidate_data_cache.
      void *fd2 = nullptr;
      ASSERT_EQ(fs_->open(nodeid, O_RDWR | O_TRUNC, &fd2, &unused), 0);
      DEFER(fs_->release(nodeid, get_file_from_handle(fd2)));
      ASSERT_EQ(write_to_file_handle(fd2, small_buf.data(), small_size, 0),
                static_cast<ssize_t>(small_size));
      ASSERT_EQ(fs_->flush(nodeid, get_file_from_handle(fd2)), 0);
      shrunk_crc = cal_crc64(0, small_buf.data(), small_size);

      // While fd1 is open the tier never switches: the flag-consuming open
      // drops the shared memory cache in place and the refill fetches the
      // new content, so the first read misses.
      ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, shrunk_crc),
                kCacheMiss);

      // fd1 reads through the same refilled instance and observes the new
      // content, never the stale range cached before the rewrite.
      std::vector<char> buf2(kPartialReadSize);
      ASSERT_EQ(read_from_handle(fd1, buf2.data(), kPartialReadSize, 0),
                static_cast<ssize_t>(kPartialReadSize));
      ASSERT_EQ(memcmp(buf2.data(), small_buf.data(), kPartialReadSize), 0);
    }

    // All handles are closed, so the next open rebuilds the inode cache
    // below the limit as the disk tier: the first read misses and fills
    // the disk cache, the second hits the persistent disk cache (the
    // memory tier above the limit would have missed every time).
    ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, shrunk_crc),
              kCacheMiss);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, filename, shrunk_crc),
              kCacheHit);
  }

  void verify_disk_cache_ghost_pad_after_growth() {
    const size_t kInitialSize = 1024 * 1024 - 2048;
    const size_t kAppendSize = 4096 + 1024 * 1024;
    const size_t kFinalSize = kInitialSize + kAppendSize;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    std::string path = "testfile_ghost_pad_after_growth";
    int r = create_and_flush(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    std::string initial_data = random_string(kInitialSize);
    r = write_to_file_handle(handle, initial_data.data(), initial_data.size(),
                             0);
    ASSERT_EQ(r, static_cast<ssize_t>(initial_data.size()));
    auto write_file = get_file_from_handle(handle);
    r = fsync_file_handle(write_file);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, write_file);
    ASSERT_EQ(r, 0);

    bool unused = false;
    void *append_handle = nullptr;
    r = fs_->open(nodeid, O_RDWR, &append_handle, &unused);
    ASSERT_EQ(r, 0);

    void *read_handle = nullptr;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &unused);
    ASSERT_EQ(r, 0);

    // Seed the ghost pad [kInitialSize, 1MB).
    std::vector<char> read_buf(kFinalSize);
    r = read_from_handle(read_handle, read_buf.data(), kInitialSize, 0);
    ASSERT_EQ(r, static_cast<ssize_t>(kInitialSize));
    uint64_t expected_crc =
        cal_crc64(0, initial_data.data(), initial_data.size());
    ASSERT_EQ(cal_crc64(0, read_buf.data(), kInitialSize), expected_crc);

    // Grow by 4K+1MB (object: 1MB-2K -> 2MB+2K), fd1 stays open.
    std::string append_data = random_string(kAppendSize);
    r = write_to_file_handle(append_handle, append_data.data(),
                             append_data.size(), kInitialSize);
    ASSERT_EQ(r, static_cast<ssize_t>(append_data.size()));
    expected_crc =
        cal_crc64(expected_crc, append_data.data(), append_data.size());

    // Read a clean mid-range in [1MB, 2MB): its refill extends the cache
    // file's i_size past 1MB and un-gates the pad.
    {
      const off_t mid_off = 1024 * 1024 + 512 * 1024;
      const size_t mid_len = 64 * 1024;
      std::vector<char> mid_buf(mid_len);
      r = read_from_handle(read_handle, mid_buf.data(), mid_len, mid_off);
      ASSERT_EQ(r, static_cast<ssize_t>(mid_len));
      ASSERT_EQ(std::string(mid_buf.data(), mid_len),
                append_data.substr(mid_off - kInitialSize, mid_len));
    }

    // Full read: window 0 must not serve the ghost pad.
    std::fill(read_buf.begin(), read_buf.end(), 0);
    r = read_from_handle(read_handle, read_buf.data(), kFinalSize, 0);
    ASSERT_EQ(r, static_cast<ssize_t>(kFinalSize));
    // Forensics: the ghost pad surfaces as zeros at [kInitialSize, 1MB).
    {
      bool pad_zero = true;
      for (size_t i = kInitialSize; i < 1024 * 1024; i++) {
        if (read_buf[i] != 0) {
          pad_zero = false;
          break;
        }
      }
      // clang-format off
      LOG_ERROR(
          "GHOST-PAD-CHECK [`, `): all_zero = `", kInitialSize,
          1024 * 1024, pad_zero);
      // clang-format on
    }
    ASSERT_EQ(cal_crc64(0, read_buf.data(), kFinalSize), expected_crc);

    r = fs_->release(nodeid, get_file_from_handle(append_handle));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(read_handle));
    ASSERT_EQ(r, 0);
  }

  void verify_disk_cache_random_non_aligned_growth() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    std::string path = "testfile_random_non_aligned_growth";
    int r = create_and_flush(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    bool unused = false;
    // Reuse the create handle as the appending writer.
    auto write_file = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, write_file));

    const int rounds = 12 + rand() % 9;
    size_t total = 0;
    for (int round = 0; round < rounds; round++) {
      // Append a random non-4K-aligned chunk (16K..256K, odd length).
      size_t chunk = 16 * 1024 + rand() % (240 * 1024);
      chunk |= 1;
      std::vector<char> wbuf(chunk);
      for (size_t i = 0; i < chunk; i++) wbuf[i] = det_byte(total + i);
      r = write_file->pwrite(wbuf.data(), chunk, total);
      ASSERT_EQ(r, static_cast<ssize_t>(chunk));
      size_t round_end = total + chunk;

      // A fresh reader per round: re-opening re-anchors the identity and
      // drives the note_clean_size drop path on the appends that follow.
      void *read_handle = nullptr;
      r = fs_->open(nodeid, O_RDONLY, &read_handle, &unused);
      ASSERT_EQ(r, 0);
      auto read_file = get_file_from_handle(read_handle);

      // Random ranges are picked on this thread (rand() is not
      // thread-safe) and stay inside the written prefix.
      struct Range {
        off_t off;
        size_t len;
      };
      Range ranges[4];
      for (auto &rg : ranges) {
        rg.off = rand() % (round_end - 1);
        size_t cap = 1 + rand() % (128 * 1024);
        rg.len = std::min(cap, round_end - rg.off);
      }

      // About half the rounds flush while readers are in flight, mixing
      // dirty-serve and clean-cache read paths under ETag movement.
      bool flush_now = (rand() % 2) == 0;
      auto task = std::async(std::launch::async, [&]() {
        INIT_PHOTON();
        for (auto &rg : ranges) {
          if (rg.len == 0) continue;
          std::vector<char> rbuf(rg.len);
          auto rr = read_file->pread(rbuf.data(), rg.len, rg.off);
          if (rr != static_cast<ssize_t>(rg.len)) return -1;
          for (size_t z = 0; z < rg.len; z++) {
            if (static_cast<uint8_t>(rbuf[z]) != det_byte(rg.off + z)) {
              LOG_ERROR("RANDOM-GROWTH-CORRUPT round=` off=` len=` pos=`",
                        round, rg.off, rg.len, rg.off + z);
              return -2;
            }
          }
        }
        return 0;
      });
      if (flush_now) {
        r = fsync_file_handle(write_file);
        ASSERT_EQ(r, 0);
      }
      int rc = task.get();
      ASSERT_EQ(rc, 0) << "round " << round;

      r = fs_->release(nodeid, read_file);
      ASSERT_EQ(r, 0);
      total = round_end;
    }

    // Flush everything and verify the whole stream byte-exactly.
    r = fsync_file_handle(write_file);
    ASSERT_EQ(r, 0);
    void *final_handle = nullptr;
    r = fs_->open(nodeid, O_RDONLY, &final_handle, &unused);
    ASSERT_EQ(r, 0);
    auto final_file = get_file_from_handle(final_handle);
    DEFER(fs_->release(nodeid, final_file));
    std::vector<char> full(total);
    r = final_file->pread(full.data(), total, 0);
    ASSERT_EQ(r, static_cast<ssize_t>(total));
    for (size_t z = 0; z < total; z++) {
      ASSERT_EQ(static_cast<uint8_t>(full[z]), det_byte(z)) << "pos " << z;
    }
  }

  void verify_disk_cache_store_interfaces() {
    const std::string name = "store_iface_file";
    const std::string etag = "store_iface_etag";
    const size_t kBlockSize = fs_->create_inode_cache(0)->block_size();

    auto cache = fs_->create_inode_cache(0);
    ASSERT_NE(cache, nullptr);
    auto *h = cache->get({name, etag});
    ASSERT_TRUE(h);
    DEFER(cache->release(h, 0));

    // pin() is not supported by the disk cache store.
    void *pin_buf = nullptr;
    ASSERT_EQ(h->pin(0, 4096, &pin_buf), -ENOTSUP);

    // Fresh cache: the whole requested range needs refill.
    auto [off, cnt] = h->query_refill_range(0, kBlockSize);
    ASSERT_EQ(off, 0);
    ASSERT_EQ(cnt, kBlockSize);

    // count=0 hits the block_num==0 fast path and returns success.
    RangeBuffer empty_rb;
    empty_rb.offset = 0;
    empty_rb.count = 0;
    ASSERT_EQ(h->acquire_write_buffer(empty_rb), 0);

    // Stage data, then release with evict=true: the data must be discarded.
    RangeBuffer rb;
    rb.offset = 0;
    rb.count = kBlockSize;
    ASSERT_EQ(h->acquire_write_buffer(rb), 0);
    memset(rb.buffer.iovec()[0].iov_base, 0xCD, rb.buffer.sum());
    h->release_write_buffer(rb, /*evict=*/true);

    char buf[4096];
    ASSERT_LT(h->pread(buf, 0, sizeof(buf)), 0);

    // Stage data again and release without eviction: it lands in the cache.
    RangeBuffer rb2;
    rb2.offset = 0;
    rb2.count = kBlockSize;
    ASSERT_EQ(h->acquire_write_buffer(rb2), 0);
    memset(rb2.buffer.iovec()[0].iov_base, 0xAB, rb2.buffer.sum());
    h->release_write_buffer(rb2);

    ASSERT_EQ(h->pread(buf, 0, sizeof(buf)), (ssize_t)sizeof(buf));
    ASSERT_EQ((unsigned char)buf[0], 0xAB);

    // Fully cached: nothing left to refill.
    auto [off2, cnt2] = h->query_refill_range(0, kBlockSize);
    ASSERT_EQ(cnt2, 0U);

    // Stage a write buffer while the store is still alive; it must survive
    // the failed drop below and be released exactly once.
    RangeBuffer rb3;
    rb3.offset = 0;
    rb3.count = kBlockSize;
    ASSERT_EQ(h->acquire_write_buffer(rb3), 0);

    // A drop whose reopen fails leaves the store empty; a subsequent drop
    // must tolerate the empty store instead of dereferencing it.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_DiskCache_Init_Failure);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_DiskCache_Init_Failure));
    h->drop({name, etag});
    h->drop({name, etag});
    ASSERT_LT(h->pread(buf, 0, sizeof(buf)), 0);

    // Releasing the staged buffer now finds no store: the write must be
    // skipped silently and the buffer blocks still returned to the pool.
    h->release_write_buffer(rb3);

    // With the store gone, acquiring a write buffer must fail with -ENOSPC
    // instead of dereferencing the empty store.
    RangeBuffer enospc_rb;
    enospc_rb.offset = 0;
    enospc_rb.count = kBlockSize;
    ASSERT_EQ(h->acquire_write_buffer(enospc_rb), -ENOSPC);
  }

  void verify_disk_cache_serialized_concurrent_writes() {
    // Runs under the psync engine (see TEST_F), where per-store writes are
    // serialized through write_mutex_. Concurrent photon writers on the same
    // handle force mutex contention.
    const std::string name = "serialize_write_file";
    const std::string etag = "serialize_write_etag";
    const size_t kBlockSize = fs_->create_inode_cache(0)->block_size();
    const int kWriters = 8;
    const int kRounds = 16;

    auto cache = fs_->create_inode_cache(0);
    ASSERT_NE(cache, nullptr);
    auto *h = cache->get({name, etag});
    ASSERT_TRUE(h);
    DEFER(cache->release(h, 0));

    photon::semaphore sem(0);
    for (int i = 0; i < kWriters; i++) {
      photon::thread_create11([&]() {
        DEFER(sem.signal(1));
        for (int r = 0; r < kRounds; r++) {
          RangeBuffer rb;
          rb.offset = 0;
          rb.count = kBlockSize;
          ASSERT_EQ(h->acquire_write_buffer(rb), 0);
          memset(rb.buffer.iovec()[0].iov_base, 0x5A, rb.buffer.sum());
          h->release_write_buffer(rb);
        }
      });
    }
    sem.wait(kWriters);

    // All serialized writes carried the same pattern, so the cached block
    // must read back intact.
    std::vector<char> buf(4096);
    ASSERT_EQ(h->pread(buf.data(), 0, buf.size()), (ssize_t)buf.size());
    ASSERT_EQ((unsigned char)buf[0], 0x5A);
    ASSERT_EQ((unsigned char)buf.back(), 0x5A);
  }

  void verify_disk_cache_collision_exhaustion() {
    // With collision injection every source_key maps to hash%5, so there are
    // only 5 base keys. Claim all 5, then a victim sharing the same bucket
    // exhausts the "+" retry chain and get() must fail gracefully.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_DiskCache_Key_Collision);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_DiskCache_Key_Collision));

    // Mirrors make_source_key() for an empty etag (the get() calls below
    // leave size/mtime zeroed): "name/size@mtime_sec.mtime_nsec".
    auto bucket_of = [](const std::string &name) {
      std::string source_key = name + "/0@0.0";
      return std::hash<std::string_view>{}(source_key) % 5;
    };

    const std::string victim = "exhaust_victim";
    auto victim_bucket = bucket_of(victim);

    // Find 5 claimers mapping to the same bucket as the victim. Claimed
    // sequentially, they occupy every candidate key "/b", "/b+", ...,
    // "/b++++" via the collision retry chain.
    std::vector<std::string> claimers;
    for (int i = 0; claimers.size() < 5 && i < 100000; i++) {
      auto name = "exhaust_claimer_" + std::to_string(i);
      if (bucket_of(name) == victim_bucket) claimers.push_back(name);
    }
    ASSERT_EQ(claimers.size(), 5U);

    std::vector<std::shared_ptr<ICache>> mgrs;
    std::vector<CacheHandle *> handles;
    for (auto &name : claimers) {
      auto mgr = fs_->create_inode_cache(0);
      ASSERT_NE(mgr, nullptr);
      auto *h = mgr->get({name, ""});
      ASSERT_TRUE(h);
      mgrs.push_back(mgr);
      handles.push_back(h);
    }
    DEFER({
      for (size_t i = 0; i < handles.size(); i++) {
        mgrs[i]->release(handles[i], 0);
      }
    });

    // All candidate keys are claimed by other source keys: init retries are
    // exhausted and get() returns nullptr instead of a broken handle.
    auto victim_mgr = fs_->create_inode_cache(0);
    ASSERT_NE(victim_mgr, nullptr);
    auto *victim_h = victim_mgr->get({victim, ""});
    ASSERT_EQ(victim_h, nullptr);
  }

  // Rename a dirty random-written file with the disk cache pre-warmed: the
  // rename flush must drop the stale cache, every later read must return the
  // new bytes, and the re-keyed refill must land in the cache file keyed by
  // the new identity, where subsequent reads hit.
  void verify_disk_cache_interaction_with_rename_random_write() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);
    bool unused = false;

    const size_t kFileSize = 8 * 1024 * 1024;
    const std::string old_name = "dc_rw_rename_src";
    const std::string new_name = "dc_rw_rename_dst";

    std::string local_file = join_paths(test_path_, old_name + ".src");
    create_random_file(local_file, 8);
    ASSERT_EQ(upload_file(local_file, join_paths(parent_path, old_name),
                          FLAGS_oss_bucket_prefix),
              0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, old_name.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Anchored read handle that survives the rename, warming the disk cache
    // with the original content (miss + hit). The first read also captures
    // the baseline content.
    void *rh_old = nullptr;
    r = fs_->open(nodeid, O_RDONLY, &rh_old, &unused);
    ASSERT_EQ(r, 0);
    std::string data(kFileSize, '\0');
    for (int pass = 0; pass < 2; pass++) {
      std::string buf(kFileSize, '\0');
      ssize_t n = read_from_handle(rh_old, buf.data(), kFileSize, 0);
      ASSERT_EQ(n, (ssize_t)kFileSize);
      if (pass == 0) data = buf;
      ASSERT_EQ(buf, data) << "warm-up read pass " << pass << " mismatch";
    }

    // Random-overwrite through a write handle, mirroring into the baseline.
    void *wh = nullptr;
    r = fs_->open(nodeid, O_RDWR, &wh, &unused);
    ASSERT_EQ(r, 0);
    for (int p = 0; p < 3; p++) {
      off_t off = (off_t)(p * 2 * 1024 * 1024 + 123 * 1024);
      const size_t len = 256 * 1024;
      std::string patch = random_string(len);
      ASSERT_EQ(write_to_file_handle(wh, patch.data(), len, off), (ssize_t)len);
      data.replace((size_t)off, len, patch);
    }
    auto *inode =
        static_cast<FileInode *>(get_file_from_handle(wh)->get_inode());
    ASSERT_TRUE(inode->is_dirty);

    // Rename while dirty: the transient-writer flush must clean the inode
    // (mark_clean drops the cache), then copy+delete moves the object to the
    // new name.
    r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(inode->is_dirty) << "rename must flush the dirty inode";

    // The anchored reader still pins the pre-rename path; this read walks
    // the re-key chain (drop -> stale-path ENOENT -> set_path re-key ->
    // refill from the new name) and must return the new bytes.
    {
      std::string buf(kFileSize, '\0');
      ssize_t n = read_from_handle(rh_old, buf.data(), kFileSize, 0);
      ASSERT_EQ(n, (ssize_t)kFileSize)
          << "anchored-handle read failed after rename";
      ASSERT_EQ(buf, data) << "anchored handle served stale cached bytes";
    }

    // With every OSS request failing, the same handle must now serve the
    // read purely from the re-keyed cache: subsequent reads hit.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                    FaultInjection(1000, 0));
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed));
    {
      std::string buf(kFileSize, '\0');
      ASSERT_EQ(read_from_handle(rh_old, buf.data(), kFileSize, 0),
                (ssize_t)kFileSize)
          << "second anchored read must hit the re-keyed cache";
      ASSERT_EQ(buf, data) << "cache-hit read must return the new bytes";
    }
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    r = fs_->release(nodeid, get_file_from_handle(wh));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(rh_old));
    ASSERT_EQ(r, 0);

    // The re-key must also write to the cache file keyed by the new
    // identity. A fresh FUSE open cannot prove it (mark_clean zeroes
    // attr_time, so the next open evicts by design), so probe a separate
    // cache instance with the new key: a hit proves the file is correctly
    // keyed, data left under the old key misses here.
    {
      std::string new_path = nodeid_to_path(nodeid);
      auto probe = fs_->create_inode_cache(0);
      ASSERT_NE(probe, nullptr);
      CacheKey probe_key{new_path, inode->etag};
      auto *probe_h = probe->get(probe_key);
      ASSERT_NE(probe_h, nullptr);
      DEFER(probe->release(probe_h, 0));
      std::string probe_buf(kFileSize, '\0');
      ASSERT_EQ(probe_h->pread(probe_buf.data(), 0, kFileSize),
                (ssize_t)kFileSize)
          << "the re-keyed refill must live in the cache file keyed by the "
             "new name + flushed etag";
      ASSERT_EQ(probe_buf, data) << "re-keyed cache file holds wrong bytes";
    }

    // Fresh opens through the new name reuse the re-keyed cache right
    // away: the anchored-handle refill already persisted the flushed bytes
    // under the new identity (the copy keeps the etag, so opens do not
    // evict), in contrast to the clean-rename case where the entry stays
    // under the old key and the new name misses first.
    uint64_t expected_crc = cal_crc64(0, data.data(), kFileSize);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, new_name, expected_crc),
              kCacheHit);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, new_name, expected_crc),
              kCacheHit);

    // The rename flushed the staging file and moved the object: nothing may
    // remain at the old name, and the staging area must be empty.
    auto meta_old = get_file_meta(old_name, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", meta_old["Content-Length"])
        << "old name must not keep the object after rename";
    auto meta_new = get_file_meta(new_name, FLAGS_oss_bucket_prefix);
    EXPECT_EQ(std::to_string(kFileSize), meta_new["Content-Length"]);
    EXPECT_EQ(fs_->staging_disk_usage_.load(std::memory_order_relaxed), 0u);
  }

  // A clean rename must keep the cache of an already-open handle (the
  // entry is bound at open time and copy preserves the etag), while the new
  // name must not see the pre-rename entry (the cache key contains the path).
  void verify_disk_cache_rename_keeps_cache_when_clean() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);
    bool unused = false;

    const size_t kFileSize = 4 * 1024 * 1024;
    const std::string old_name = "dc_rw_rename_clean_src";
    const std::string new_name = "dc_rw_rename_clean_dst";

    std::string local_file = join_paths(test_path_, old_name + ".src");
    create_random_file(local_file, 4);
    ASSERT_EQ(upload_file(local_file, join_paths(parent_path, old_name),
                          FLAGS_oss_bucket_prefix),
              0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, old_name.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Warm the disk cache through the anchored handle; the first read also
    // captures the baseline content.
    void *handle = nullptr;
    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    std::string data(kFileSize, '\0');
    ASSERT_EQ(read_from_handle(handle, data.data(), kFileSize, 0),
              (ssize_t)kFileSize);

    r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
    ASSERT_EQ(r, 0);

    // With every OSS request failing, only a cache hit can satisfy the read;
    // a regression that evicts the disk cache on rename fails here.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                    FaultInjection(1000, 0));
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed));

    std::string buf(kFileSize, '\0');
    ASSERT_EQ(read_from_handle(handle, buf.data(), kFileSize, 0),
              (ssize_t)kFileSize);
    ASSERT_EQ(buf, data) << "clean rename must keep serving cached bytes";

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Key isolation in the other direction: a fresh open at the new name
    // must miss the pre-rename entry (the cache key contains the path) and
    // refill from the new name; the second read then hits.
    uint64_t expected_crc = cal_crc64(0, data.data(), kFileSize);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, new_name, expected_crc),
              kCacheMiss);
    ASSERT_EQ(read_file_and_check_cache_hit(parent, new_name, expected_crc),
              kCacheHit);
  }

  // G2 double rename: A -> B, then C -> A, no writes. The handle anchored
  // on A still pins the stale path, so its first read must catch the refill
  // etag mismatch, refresh the path to /B and re-key the store; a fresh
  // open of B must then hit the re-keyed cache and keep serving A's bytes.
  void verify_disk_cache_double_rename_rekeys_cache() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);
    bool unused = false;

    const size_t kRead = 256 * 1024;
    const std::string name_a = "dc_rw_drename_a";
    const std::string name_b = "dc_rw_drename_b";
    const std::string name_c = "dc_rw_drename_c";

    // Two distinct remote objects with different contents and etags.
    std::string local_a = join_paths(test_path_, name_a + ".src");
    std::string local_c = join_paths(test_path_, name_c + ".src");
    create_random_file(local_a, 8);
    create_random_file(local_c, 8);
    ASSERT_EQ(upload_file(local_a, join_paths(parent_path, name_a),
                          FLAGS_oss_bucket_prefix),
              0);
    ASSERT_EQ(upload_file(local_c, join_paths(parent_path, name_c),
                          FLAGS_oss_bucket_prefix),
              0);

    auto read_local_head = [&](const std::string &path, std::string &out) {
      int fd = ::open(path.c_str(), O_RDONLY);
      ASSERT_GE(fd, 0);
      DEFER(::close(fd));
      out.assign(kRead, '\0');
      ASSERT_EQ(::pread(fd, out.data(), kRead, 0), (ssize_t)kRead);
    };
    std::string head_a, head_c;
    read_local_head(local_a, head_a);
    read_local_head(local_c, head_c);
    ASSERT_NE(head_a, head_c);

    // Anchor a handle on A without reading, so the cache stays cold and
    // the first read goes through the refill etag check.
    uint64_t nodeid_a = 0;
    struct stat st;
    int r = fs_->lookup(parent, name_a.c_str(), &nodeid_a, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_a, 1));

    void *rh = nullptr;
    r = fs_->open(nodeid_a, O_RDONLY, &rh, &unused);
    ASSERT_EQ(r, 0);

    // C needs a local inode so rename can resolve the source path.
    uint64_t nodeid_c = 0;
    r = fs_->lookup(parent, name_c.c_str(), &nodeid_c, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_c, 1));

    // A -> B, then C -> A: the name /A now serves C's object.
    r = fs_->rename(parent, name_a.c_str(), parent, name_b.c_str(), 0);
    ASSERT_EQ(r, 0);
    r = fs_->rename(parent, name_c.c_str(), parent, name_a.c_str(), 0);
    ASSERT_EQ(r, 0);

    // First read: the refill at the stale /A sees C's etag, must re-key to
    // /B and return A's bytes (never C's).
    {
      std::string buf(kRead, '\0');
      ASSERT_EQ(read_from_handle(rh, buf.data(), kRead, 0), (ssize_t)kRead)
          << "anchored read after double rename failed";
      ASSERT_EQ(buf, head_a)
          << "anchored handle must serve A's content via /B, not C's bytes "
             "now sitting at /A";
    }

    // With every OSS request failing, the re-read must hit the re-keyed
    // cache and keep returning A's bytes.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                    FaultInjection(1000, 0));
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed));
    {
      std::string buf(kRead, '\0');
      ASSERT_EQ(read_from_handle(rh, buf.data(), kRead, 0), (ssize_t)kRead)
          << "second read must hit the re-keyed cache";
      ASSERT_EQ(buf, head_a);
    }
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    r = fs_->release(nodeid_a, get_file_from_handle(rh));
    ASSERT_EQ(r, 0);

    // Reopen B through a fresh handle: with OSS down the read can only be
    // served by the cache file the re-key wrote to, keyed by /B + A's etag.
    {
      uint64_t nodeid_b = 0;
      r = fs_->lookup(parent, name_b.c_str(), &nodeid_b, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid_b, 1));
      void *rh_b = nullptr;
      r = fs_->open(nodeid_b, O_RDONLY, &rh_b, &unused);
      ASSERT_EQ(r, 0);

      g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                      FaultInjection(1000, 0));
      DEFER(g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Failed));
      std::string buf(kRead, '\0');
      ASSERT_EQ(read_from_handle(rh_b, buf.data(), kRead, 0), (ssize_t)kRead)
          << "fresh open of B must hit the re-keyed cache";
      ASSERT_EQ(buf, head_a);
      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Failed);

      r = fs_->release(nodeid_b, get_file_from_handle(rh_b));
      ASSERT_EQ(r, 0);
    }

    // The new occupant of name A must serve C's content.
    {
      uint64_t nodeid_new_a = 0;
      r = fs_->lookup(parent, name_a.c_str(), &nodeid_new_a, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid_new_a, 1));
      void *rh_new = nullptr;
      r = fs_->open(nodeid_new_a, O_RDONLY, &rh_new, &unused);
      ASSERT_EQ(r, 0);
      std::string buf(kRead, '\0');
      ASSERT_EQ(read_from_handle(rh_new, buf.data(), kRead, 0), (ssize_t)kRead);
      ASSERT_EQ(buf, head_c) << "name A must serve C's content after C->A";
      r = fs_->release(nodeid_new_a, get_file_from_handle(rh_new));
      ASSERT_EQ(r, 0);
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

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_mtime_fallback_identity) {
  INIT_PHOTON();
  verify_disk_cache_mtime_fallback_identity();
}

TEST_F(Ossfs2DiskCacheTest, DISABLED_verify_disk_cache_mem_usage) {
  INIT_PHOTON();
  set_ossfs_log_to_stdout(ALOG_INFO);
  DEFER(set_ossfs_log_to_stdout(ALOG_DEBUG));
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;

  LOG_INFO("Test disk cache mem usage with 1000w files");
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_libaio);
  verify_disk_cache_mem_usage(10'000'000);
  destroy();

  LOG_INFO("Test disk cache mem usage with 2000w files");
  init(opts, -1, "", false, photon::fs::ioengine_libaio);
  verify_disk_cache_mem_usage(20'000'000);
  destroy();

  LOG_INFO("Test disk cache mem usage with 1e8 files");
  init(opts, -1, "", false, photon::fs::ioengine_libaio);
  verify_disk_cache_mem_usage(100'000'000);
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_eviction_when_full) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_disk_cache_eviction_when_full();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_max_file_size) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  opts.disk_data_cache_max_file_size = 100 * 1024 * 1024;  // 100 MiB
  init(opts);

  verify_disk_cache_max_file_size();
}

TEST_F(Ossfs2DiskCacheTest,
       verify_disk_cache_switch_disk_to_mem_when_size_grows) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  opts.disk_data_cache_max_file_size = 8 * 1024 * 1024;  // 8 MiB
  init(opts);

  verify_disk_cache_switch_disk_to_mem_when_size_grows();
}

TEST_F(Ossfs2DiskCacheTest,
       verify_disk_cache_switch_mem_to_disk_when_size_shrinks) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  opts.disk_data_cache_max_file_size = 100 * 1024 * 1024;  // 100 MiB
  init(opts);

  verify_disk_cache_switch_mem_to_disk_when_size_shrinks();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_key_collision) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_disk_cache_key_collision();
}

// Test: disk cache behaviour under various network error conditions.
TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_with_network_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_disk_cache_with_network_error();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_rehash_on_collision) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_disk_cache_rehash_on_collision();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_drop_rejects_stale_refill) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  opts.attr_timeout = 1;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_psync);
  verify_disk_cache_drop_rejects_stale_refill();
}

TEST_F(Ossfs2DiskCacheTest, verify_prefetch_eviction_for_large_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  opts.prefetch_chunk_size = 1048576 * 8;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_psync);
  verify_prefetch_eviction_for_large_file();
}

TEST_F(Ossfs2DiskCacheTest, verify_prefetch_eviction_for_multi_files) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_psync);
  verify_prefetch_eviction_for_multi_files();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_drop_with_reopen_failed) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  opts.attr_timeout = 1;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_psync);
  verify_disk_cache_drop_rejects_stale_refill(true);
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_drop_reopens_on_same_key) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_psync);
  verify_disk_cache_drop_reopens_on_same_key();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_ghost_pad_after_growth) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  opts.cache_type = CacheType::kDiskCache;
  init(opts, -1, "", false, photon::fs::ioengine_libaio);
  verify_disk_cache_ghost_pad_after_growth();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_random_non_aligned_growth) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  opts.cache_type = CacheType::kDiskCache;
  init(opts, -1, "", false, photon::fs::ioengine_libaio);
  verify_disk_cache_random_non_aligned_growth();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_store_interfaces) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  // Engine-agnostic interface semantics; let the suite randomize the engine.
  init(opts, -1, "", false, -1);
  verify_disk_cache_store_interfaces();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_serialized_concurrent_writes) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, -1, "", false, photon::fs::ioengine_psync);
  verify_disk_cache_serialized_concurrent_writes();
}

TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_collision_exhaustion) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  // Engine-agnostic key collision logic; let the suite randomize the
  // engine.
  init(opts, -1, "", false, -1);
  verify_disk_cache_collision_exhaustion();
}

// Rename a dirty random-written file with the disk cache enabled: the
// transient-writer flush must clean the inode (dropping the stale cache) and
// every read afterwards must return the new bytes.
TEST_F(Ossfs2DiskCacheTest,
       verify_disk_cache_interaction_with_rename_random_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // enable random write
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss);
  init(opts);
  verify_disk_cache_interaction_with_rename_random_write();
}

// A clean rename keeps the object content and its etag (copy preserves the
// etag), so the disk cache entry under the pre-rename key stays valid.
TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_rename_keeps_cache_when_clean) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // enable random write
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss);
  init(opts);
  verify_disk_cache_rename_keeps_cache_when_clean();
}

// G2 double rename (A -> B, C -> A) with an anchored handle on A: the
// refill etag check must re-key the store to the new name and later reads
// must hit the correctly-keyed cache. Random-write mode only: its cached
// reader verifies refill etags.
TEST_F(Ossfs2DiskCacheTest, verify_disk_cache_double_rename_rekeys_cache) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // enable random write
  opts.cache_type = CacheType::kDiskCache;
  SET_TEST_MODE(kTestOss);
  init(opts);
  verify_disk_cache_double_rename_rekeys_cache();
}
