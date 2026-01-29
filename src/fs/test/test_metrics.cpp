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

#include "admin/uds_server.h"
#include "metric/metrics.h"
#include "test_suite.h"

class Ossfs2MetricsTest : public Ossfs2TestSuite {
 protected:
  void verify_basic_metrics(bool appendable = false) {
    Metric::set_enabled_metrics("all");
    DEFER(Metric::set_enabled_metrics(""));
    // wait for metrics clear
    std::this_thread::sleep_for(std::chrono::seconds(1));
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    size_t file_size_MB = 10;
    size_t file_size = file_size_MB * 1024 * 1024;
    create_file_in_folder(parent, "test", file_size_MB, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto metrics = Metric::get_metrics_map(10);
    std::string metrics_name =
        appendable ? "oss_append_object" : "oss_put_object";
    ASSERT_TRUE(metrics[metrics_name + "_latency"] > 0);
    ASSERT_GE(metrics["oss_write_bytes"], file_size);

    auto metrics_str = Metric::get_metrics_string(10);
    ASSERT_TRUE(metrics_str.find(metrics_name) != std::string::npos);
    ASSERT_TRUE(metrics_str.find("oss_write") != std::string::npos);

    void *handle = nullptr;
    bool unused = false;
    int r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    if (appendable) {
      size_t append_size = 1 * 1024 * 1024;
      auto data = random_string(append_size);
      ssize_t r =
          write_to_file_handle(handle, data.c_str(), append_size, file_size);
      ASSERT_EQ(r, static_cast<ssize_t>(append_size));
      r = get_file_from_handle(handle)->fsync();
      ASSERT_EQ(r, 0);
      file_size += append_size;
      std::this_thread::sleep_for(std::chrono::seconds(1));
      metrics = Metric::get_metrics_map(10);
      ASSERT_GE(metrics["oss_write_bytes"], file_size);
    }

    uint64_t offset = 0;
    uint64_t buf_size = IO_SIZE;
    char *buf = new char[IO_SIZE];
    DEFER(delete[] buf);
    uint64_t read_crc64 = 0;

    while (offset < file_size) {
      int64_t read_size = std::min(buf_size, file_size - offset);
      auto r = read_from_handle(handle, buf, read_size, offset);
      ASSERT_EQ(r, read_size);
      read_crc64 = cal_crc64(read_crc64, buf, r);
      offset += r;
    }

    auto file_meta = get_file_meta("test", FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(read_crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    metrics = Metric::get_metrics_map(10);
    ASSERT_TRUE(metrics["oss_get_object_range_latency"] > 0);
    ASSERT_GE(metrics["oss_read_bytes"], file_size);

    metrics_str = Metric::get_metrics_string(10);
    ASSERT_TRUE(metrics_str.find("oss_get_object_range") != std::string::npos);
    ASSERT_TRUE(metrics_str.find("oss_read") != std::string::npos);

    // check empty metrics
    std::this_thread::sleep_for(std::chrono::seconds(2));
    metrics = Metric::get_metrics_map(1);
    metrics_str = Metric::get_metrics_string(1);
    ASSERT_TRUE(metrics.empty());
    ASSERT_TRUE(metrics_str.empty());
  }

  void verify_metrics_filter() {
    auto write_file = [&](const std::string &file_name) {
      uint64_t parent = get_test_dir_parent();
      DEFER(fs_->forget(parent, 1));
      uint64_t nodeid = 0;
      create_file_in_folder(parent, file_name, 1, nodeid);
      DEFER(fs_->forget(nodeid, 1));
    };

    Metric::set_enabled_metrics("fs,oss");
    write_file("test1");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto metrics = Metric::get_metrics_map(5);
    ASSERT_GE(metrics["oss_write_bytes"], 1048576ul);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    Metric::set_enabled_metrics("");
    write_file("test2");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    metrics = Metric::get_metrics_map(2);
    ASSERT_TRUE(metrics.empty());
  }

  void verify_uds_server(bool enabled = true) {
    srand(time(nullptr));
    auto uds_path = Admin::generate_uds_path_from_pid(getpid());

    if (!enabled) {
      std::string output = "";
      Admin::send_uds_request(uds_path, "", "", output);
      ASSERT_TRUE(output.empty());
      return;
    }

    auto check_response = [&](const std::string &action,
                              const std::string &param) {
      std::string expect = "Not supported for action \"" + action +
                           "\" and param \"" + param + "\"";
      std::string output;
      Admin::send_uds_request(uds_path, action, param, output);
      return expect == output;
    };

    // empty action and params
    ASSERT_TRUE(check_response("", ""));
    ASSERT_TRUE(check_response("aaa", ""));
    ASSERT_TRUE(check_response("_aaa", "a,b,c"));

    // small request
    std::string action = random_string(20 + rand() % 5);
    std::string param = random_string(20 + rand() % 5);
    ASSERT_TRUE(check_response(action, param));

    // large request
    action = random_string(2000 + rand() % 100);
    param = random_string(2000 + rand() % 100);
    ASSERT_TRUE(check_response(action, param));

    action = random_string(1023);
    ASSERT_TRUE(check_response(action, ""));

    // stats request
    std::string output;
    Admin::send_uds_request(uds_path, "set-metrics", "all", output);
    DEFER(Admin::send_uds_request(uds_path, "set-metrics", "", output));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    create_file_in_folder(parent, "test_" + random_string(5), 1, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    std::this_thread::sleep_for(std::chrono::seconds(2));
    Admin::send_uds_request(uds_path, "stats", "5", output);
    ASSERT_TRUE(output.find("oss_put_object") != std::string::npos);
    Admin::send_uds_request(uds_path, "stats", "1", output);
    ASSERT_TRUE(output.empty());
  }
};

TEST_F(Ossfs2MetricsTest, verify_basic_metrics) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_basic_metrics();
}

TEST_F(Ossfs2MetricsTest, verify_basic_metrics_with_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  init(opts);
  verify_basic_metrics(true);
}

TEST_F(Ossfs2MetricsTest, verify_metrics_filter) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_metrics_filter();
}

TEST_F(Ossfs2MetricsTest, verify_uds_server) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_uds_server();

  destroy();
  auto uds_path = Admin::generate_uds_path_from_pid(getpid());
  int fd = ::open(uds_path.c_str(), O_RDWR | O_CREAT, 0644);
  ::close(fd);
  init(opts);
  verify_uds_server();

  destroy();
  opts.enable_admin_server = false;
  init(opts);
  verify_uds_server(false);
}
