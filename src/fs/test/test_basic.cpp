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
#include "test_util.h"

DEFINE_string(config_file, "/etc/ossfs2-gtest.conf",
              "default config file path");

class Ossfs2BasicTest : public Ossfs2TestSuite {
 protected:
  void verify_init() {
    struct stat st;
    uint64_t nodeid = 0;
    int r = fs_->getattr(root_nodeid_, &st);
    ASSERT_EQ(r, 0);

    r = fs_->mkdir(root_nodeid_, get_test_osspath("test_dir").c_str(), 0777, 0,
                   0, 0, &nodeid, &st);
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(nodeid, 1));

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);

    r = fs_->rmdir(root_nodeid_, get_test_osspath("test_dir").c_str());
    ASSERT_EQ(r, 0);

    auto ossutil = lookup_ossutil();
    ASSERT_NE(ossutil, "");
  }

  void verify_statfs() {
    struct statvfs stbuf;
    int r = fs_->statfs(&stbuf);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(stbuf.f_bsize, uint64_t(0x2000));
    ASSERT_EQ(stbuf.f_blocks, OssFs::kMaxFsSize / stbuf.f_bsize);
  }

  void verify_oss_prefix_types() {
    // 1. if oss prefix is "", also try with /.
    // 2. if oss prefix is not root, try with and without /.
    // 3. try prefix with two /, which is not same as 2.
    std::string orig_prefix = FLAGS_oss_bucket_prefix;
    DEFER(FLAGS_oss_bucket_prefix = orig_prefix);

    std::string filename = "testfile" + std::to_string(time(nullptr));
    struct stat st;
    {
      uint64_t nodeid = 0;
      uint64_t parent = get_test_dir_parent();
      DEFER(fs_->forget(parent, 1));

      auto crc64 = create_file_in_folder(parent, filename, 1, nodeid, 17);
      ASSERT_TRUE(crc64 > 0);
      DEFER(fs_->forget(nodeid, 1));
    }

    auto do_check = [&](int expected_r) {
      LOG_INFO("checking bucket prefix `", FLAGS_oss_bucket_prefix);
      remount();
      uint64_t parent = get_test_dir_parent();
      DEFER(fs_->forget(parent, 1));
      uint64_t nodeid = 0;
      int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
      ASSERT_EQ(r, expected_r);
      if (r == 0) {
        fs_->forget(nodeid, 1);
      }
    };

    if (FLAGS_oss_bucket_prefix.empty()) {
      FLAGS_oss_bucket_prefix = "/";
      do_check(0);
      return;
    }

    if (FLAGS_oss_bucket_prefix == "/") {
      FLAGS_oss_bucket_prefix = "";
      do_check(0);
      return;
    }

    // checking abc abc/ /abc /abc/. they are the same.
    std::string base_prefix = FLAGS_oss_bucket_prefix;
    if (base_prefix.back() == '/') base_prefix.pop_back();
    if (base_prefix.size() && base_prefix[0] == '/') base_prefix.erase(0, 1);

    std::vector<std::string> prefixes = {base_prefix, base_prefix + "/",
                                         "/" + base_prefix,
                                         "/" + base_prefix + "/"};

    for (size_t i = 0; i < prefixes.size(); ++i) {
      FLAGS_oss_bucket_prefix = prefixes[i];
      do_check(0);
    }

    std::string more_slashes = base_prefix + "//";
    FLAGS_oss_bucket_prefix = more_slashes;
    do_check(-ENOENT);

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    std::string new_name = filename + "slash";
    create_file_in_folder(parent, new_name, 1, nodeid, 11);
    DEFER(fs_->forget(nodeid, 1));
    int r = stat_file(nodeid_to_path(nodeid), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
  }

  void verify_oss_retrying() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filepath = "testfile";
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    const int retry_times = oss_options_.retry_times;
    uint64_t nodeid = 0;

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                    FaultInjection(retry_times + 1, 0));
    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_NE(r, 0);

    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid, 1);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_5xx,
                                    FaultInjection(retry_times + 1, 0));

    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_NE(r, 0);

    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid, 1);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_5xx);
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Qps_Limited);
    auto before = std::chrono::steady_clock::now();
    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_NE(r, 0);
    auto after = std::chrono::steady_clock::now();
    auto cost =
        std::chrono::duration_cast<std::chrono::microseconds>(after - before);
    LOG_DEBUG("cost  `us for qps limit", cost.count());
    g_fault_injector->clear_all_injections();
    ASSERT_GT(cost.count(), (int64_t)oss_options_.request_timeout_us);
    ASSERT_LT(cost.count(), (int64_t)oss_options_.request_timeout_us + 1000000);

    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Read_Partial,
                                    FaultInjection(retry_times + 1, 0));
    char *buf = new char[st.st_size];
    DEFER(delete[] buf);

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    ssize_t ret = read_from_handle(handle, buf, st.st_size, 0);
    ASSERT_EQ(ret, -EIO);

    ret = read_from_handle(handle, buf, st.st_size, 0);
    ASSERT_EQ(ret, st.st_size);
  }

  void verify_init_prefetch_options() {
    // Test case 1: Default values (prefetch_chunks = 0)
    {
      OssFsOptions opts;
      opts.prefetch_chunks = 0;
      opts.prefetch_concurrency = 256;
      opts.prefetch_concurrency_per_file = 64;
      opts.prefetch_chunk_size = 8 * 1024 * 1024;

      OssFs fs(opts, {});
      // Access private members through friend class capabilities
      ASSERT_EQ(fs.options_.prefetch_concurrency, 256U);
      ASSERT_EQ(fs.options_.prefetch_concurrency_per_file, 64U);
      ASSERT_EQ(fs.max_prefetch_size_per_handle_, 64ULL * 8 * 1024 * 1024);
      ASSERT_EQ(fs.max_prefetch_window_size_per_handle_,
                2ULL * 64 * 8 * 1024 * 1024);
    }

    // Test case 2: Positive prefetch_chunks value
    {
      OssFsOptions opts;
      opts.prefetch_chunks = 300;
      opts.prefetch_concurrency = 256;
      opts.prefetch_concurrency_per_file = 64;
      opts.prefetch_chunk_size = 8 * 1024 * 1024;

      OssFs fs(opts, {});
      // prefetch_concurrency should be adjusted to min(300/3, 256) = min(100,
      // 256) = 100
      ASSERT_EQ(fs.options_.prefetch_concurrency, 100U);
      ASSERT_EQ(fs.options_.prefetch_concurrency_per_file, 64U);
      ASSERT_EQ(fs.max_prefetch_size_per_handle_, 64ULL * 8 * 1024 * 1024);
      ASSERT_EQ(fs.max_prefetch_window_size_per_handle_,
                2ULL * 64 * 8 * 1024 * 1024);
    }

    // Test case 3: prefetch_chunks value smaller than prefetch_concurrency*3
    {
      OssFsOptions opts;
      opts.prefetch_chunks =
          60;  // This is smaller than prefetch_concurrency*3 (256*3=768)
      opts.prefetch_concurrency = 256;
      opts.prefetch_concurrency_per_file = 64;
      opts.prefetch_chunk_size = 8 * 1024 * 1024;

      OssFs fs(opts, {});
      // prefetch_concurrency should be adjusted to min(60/3, 256) = min(20,
      // 256) = 20
      ASSERT_EQ(fs.options_.prefetch_concurrency, 20U);
      ASSERT_EQ(fs.options_.prefetch_concurrency_per_file,
                20U);  // Should be min(64, 20) = 20
      ASSERT_EQ(fs.max_prefetch_size_per_handle_, 20ULL * 8 * 1024 * 1024);
      ASSERT_EQ(fs.max_prefetch_window_size_per_handle_,
                2ULL * 20 * 8 * 1024 * 1024);
    }

    // Test case 4: prefetch_chunks = -1 (unlimited)
    {
      OssFsOptions opts;
      opts.prefetch_chunks = -1;
      opts.prefetch_concurrency = 256;
      opts.prefetch_concurrency_per_file = 64;
      opts.prefetch_chunk_size = 8 * 1024 * 1024;

      OssFs fs(opts, {});
      ASSERT_EQ(fs.options_.prefetch_concurrency, 256U);
      ASSERT_EQ(fs.options_.prefetch_concurrency_per_file, 64U);
      ASSERT_EQ(fs.max_prefetch_size_per_handle_, 64ULL * 8 * 1024 * 1024);
      ASSERT_EQ(fs.max_prefetch_window_size_per_handle_,
                2ULL * 64 * 8 * 1024 * 1024);
    }

    // Test case 5: prefetch_concurrency_per_file larger than
    // prefetch_concurrency
    {
      OssFsOptions opts;
      opts.prefetch_chunks = 0;
      opts.prefetch_concurrency = 32;
      opts.prefetch_concurrency_per_file =
          64;  // Larger than prefetch_concurrency
      opts.prefetch_chunk_size = 8 * 1024 * 1024;

      OssFs fs(opts, {});
      ASSERT_EQ(fs.options_.prefetch_concurrency, 32U);
      ASSERT_EQ(fs.options_.prefetch_concurrency_per_file,
                32U);  // Should be min(64, 32) = 32
      ASSERT_EQ(fs.max_prefetch_size_per_handle_, 32ULL * 8 * 1024 * 1024);
      ASSERT_EQ(fs.max_prefetch_window_size_per_handle_,
                2ULL * 32 * 8 * 1024 * 1024);
    }
  }
};

TEST_F(Ossfs2BasicTest, verify_init) {
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_init();
}

TEST_F(Ossfs2BasicTest, verify_statfs) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_statfs();
}

TEST_F(Ossfs2BasicTest, verify_oss_prefix_types) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_oss_prefix_types();
}

TEST_F(Ossfs2BasicTest, verify_oss_retrying) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.prefetch_chunk_size = 0;
  init(opts);
  verify_oss_retrying();
}

TEST_F(Ossfs2BasicTest, verify_bind_ips) {
  INIT_PHOTON();
  auto ip = get_ip_by_interface("eth0");
  ASSERT_TRUE(!ip.empty());
  OssFsOptions opts;
  init(opts, -1, ip + ",ignored_ip");
}

TEST_F(Ossfs2BasicTest, verify_bind_ips_negative) {
  INIT_PHOTON();
  OssFsOptions opts;
  // will fallback to default mode
  init(opts, -1, "invalid_ip");
}

TEST_F(Ossfs2BasicTest, verify_init_prefetch_options) {
  INIT_PHOTON();
  verify_init_prefetch_options();
}

int main(int argc, char **arg) {
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = SA_SIGINFO;

  if (sigaction(SIGPIPE, &sa, NULL) == -1) {
    perror("sigaction");
    exit(EXIT_FAILURE);
  }

  ::testing::InitGoogleTest(&argc, arg);
  gflags::ParseCommandLineFlags(&argc, &arg, true);

  std::string config_file = FLAGS_config_file;
  gflags::SetCommandLineOption("flagfile", config_file.c_str());

  std::atomic<bool> stopped = {false};
  std::thread signal_handler_thread([&stopped]() {
    while (!stopped) {
      sleep(1);
    }
  });

  int r = RUN_ALL_TESTS();
  stopped = true;
  signal_handler_thread.join();
  return r;
}
