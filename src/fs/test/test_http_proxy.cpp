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

#include <photon/net/http/client.h>
#include <photon/net/http/server.h>
#include <photon/net/socket.h>

#include <ctime>
#include <memory>
#include <mutex>
#include <vector>

#include "test_suite.h"

using namespace photon::net::http;
using photon::net::ISocketServer;

// Custom deleters for Photon objects
struct SocketServerDeleter {
  void operator()(ISocketServer *p) const {
    delete p;
  }
};

struct HTTPServerDeleter {
  void operator()(HTTPServer *p) const {
    delete p;
  }
};

struct HTTPHandlerDeleter {
  void operator()(HTTPHandler *p) const {
    delete p;
  }
};

struct HTTPClientDeleter {
  void operator()(photon::net::http::Client *p) const {
    delete p;
  }
};

// Smart pointer types for Photon objects
using SocketServerPtr = std::unique_ptr<ISocketServer, SocketServerDeleter>;
using HTTPServerPtr = std::unique_ptr<HTTPServer, HTTPServerDeleter>;
using HTTPHandlerPtr = std::unique_ptr<HTTPHandler, HTTPHandlerDeleter>;
using HTTPClientPtr =
    std::unique_ptr<photon::net::http::Client, HTTPClientDeleter>;

// Global variable for reverse proxy statistics
static std::atomic<int> g_proxy_request_count{0};

// Counts requests that reached the slow backend, so a timeout test can prove
// the backend was actually in the data path.
static std::atomic<int> g_slow_request_count{0};

static constexpr int kSlowResponseSec = 3;

// Reverse proxy director - forwards requests to backend server
static int proxy_director(void *arg, Request &src, Request &dst) {
  g_proxy_request_count.fetch_add(1);

  if (arg) {
    auto *headers =
        static_cast<const std::vector<std::pair<std::string, std::string>> *>(
            arg);
    for (const auto &kv : *headers) {
      auto it = src.headers.find(kv.first);
      EXPECT_NE(it, src.headers.end()) << kv.first;
      if (it != src.headers.end()) {
        EXPECT_EQ(it.second(), kv.second);
      }
    }
  }

  LOG_DEBUG("[ReverseProxy] Forwarding: `", src.target());

  // Reset destination to use the original target URL
  dst.reset(src.verb(), src.target());

  // Copy headers (except Host)
  for (auto kv = src.headers.begin(); kv != src.headers.end(); kv++) {
    if (kv.first() != "Host") {
      dst.headers.insert(kv.first(), kv.second(), 1);
    }
  }

  return 0;
}

// Reverse proxy modifier - processes backend response
static int proxy_modifier(void *arg, Response &src, Response &dst) {
  dst.set_result(src.status_code());
  for (auto kv : src.headers) {
    dst.headers.insert(kv.first, kv.second);
  }

  return 0;
}

// Slow server handler for timeout test. The leading void* is required by
// DelegateHTTPHandler's Func type; without it every argument shifts by one.
static int slow_server_handler(void *, Request &, Response &resp,
                               std::string_view) {
  g_slow_request_count.fetch_add(1);
  LOG_INFO("[SlowServer] Sleeping for ` seconds...", kSlowResponseSec);
  photon::thread_sleep(kSlowResponseSec);
  resp.set_result(200);
  resp.headers.content_length(2);
  resp.write("OK", 2);
  return 0;
}

// Helper function to get URL from server
static std::string get_server_url(photon::net::ISocketServer *server) {
  auto addr = server->getsockname();
  return "http://127.0.0.1:" + std::to_string(addr.port) + "/";
}

// Reverse proxy resources structure.
// Members destruct in reverse order, and HTTPServerImpl's destructor is what
// drains in-flight handlers, so the client they forward through must outlive
// the server. Freeing it first is a use-after-free.
struct ReverseProxyResources {
  HTTPClientPtr client;
  HTTPHandlerPtr handler;
  HTTPServerPtr http_server;
  SocketServerPtr server;
};

/**
 * Start a reverse proxy server
 *
 * @param backend_url The backend server URL to forward requests to
 * @return Pair of proxy URL and resources, or empty string on failure
 */
static std::pair<std::string, ReverseProxyResources> start_reverse_proxy(
    const std::string &backend_url,
    std::vector<std::pair<std::string, std::string>> *headers = nullptr) {
  ReverseProxyResources resources;

  // Create TCP server
  resources.server.reset(photon::net::new_tcp_socket_server());
  if (!resources.server) {
    LOG_ERROR("Failed to create TCP server for proxy");
    return {"", std::move(resources)};
  }

  resources.server->timeout(30000UL * 1000);
  if (resources.server->bind_v4localhost() < 0 ||
      resources.server->listen() < 0) {
    LOG_ERROR("Failed to bind/listen proxy server");
    resources.server.reset();
    return {"", std::move(resources)};
  }

  // Create HTTP server
  resources.http_server.reset(new_http_server());
  if (!resources.http_server) {
    resources.server.reset();
    return {"", std::move(resources)};
  }

  // Create HTTP client for forwarding
  resources.client.reset(new_http_client());
  if (!resources.client) {
    resources.http_server.reset();
    resources.server.reset();
    return {"", std::move(resources)};
  }
  resources.client->timeout(30000UL * 1000);

  // Create proxy handler
  resources.handler.reset(new_proxy_handler({headers, &proxy_director},
                                            {nullptr, &proxy_modifier},
                                            resources.client.get()));

  if (!resources.handler) {
    resources.client.reset();
    resources.http_server.reset();
    resources.server.reset();
    return {"", std::move(resources)};
  }

  // Setup and start
  resources.http_server->add_handler(resources.handler.get());
  resources.server->set_handler(
      resources.http_server->get_connection_handler());
  resources.server->start_loop();

  std::string proxy_url = get_server_url(resources.server.get());
  LOG_INFO("Reverse proxy started: ` -> `", proxy_url, backend_url);

  return {proxy_url, std::move(resources)};
}

class Ossfs2HttpProxyTest : public Ossfs2TestSuite {
 protected:
  void SetUp() override {
    Ossfs2TestSuite::SetUp();

    // Reset counter
    g_proxy_request_count.store(0);
    g_slow_request_count.store(0);
  }

  void TearDown() override {
    Ossfs2TestSuite::TearDown();
  }

  // Helper to create OSS store with proxy
  OssStore *create_oss_store_with_proxy(const std::string &proxy_url,
                                        const std::string &endpoint = "",
                                        const std::string &bucket = "") {
    ObjStoreOptions options;
    options.endpoint = endpoint.empty() ? FLAGS_oss_endpoint : endpoint;
    options.bucket = bucket.empty() ? FLAGS_oss_bucket : bucket;
    options.prefix = FLAGS_oss_bucket_prefix;
    options.proxy = proxy_url;
    options.request_timeout_us = FLAGS_oss_request_timeout_ms * 1000;
    options.user_agent = "Ossfs2HttpProxyTest/1.0";

    auto auth = new_basic_oss_authenticator(
        {FLAGS_oss_access_key_id, FLAGS_oss_access_key_secret});

    return new OssStore(options, auth);
  }

  // Helper to construct OSS URL with protocol
  std::string get_oss_url() {
    std::string url = FLAGS_oss_endpoint;
    if (url.find("http://") != 0 && url.find("https://") != 0) {
      url = "http://" + url;
    }
    return url;
  }

  void verify_invalid_proxy_connection_refused() {
    // Create OSS adapter with invalid proxy (nothing listening on this port)
    auto adapter = create_oss_store_with_proxy("http://127.0.0.1:59999");
    ASSERT_NE(adapter, nullptr);
    DEFER(delete adapter);

    // Try to check bucket - should fail due to proxy connection error
    int ret = adapter->check_bucket();

    // Expected to fail because proxy is not running
    EXPECT_NE(ret, 0) << "Should fail with invalid proxy";
    LOG_INFO(
        "Invalid proxy test passed: oss_check_bucket returned ` (expected "
        "failure)",
        ret);
  }

  void verify_invalid_proxy_malformed_url() {
    // Test various malformed proxy URLs
    std::vector<std::string> invalid_proxies = {
        "not_a_url",
        "ftp://localhost:8080",  // Wrong protocol
        "http://",
        "http://invalid:host:port",
    };

    for (const auto &invalid_proxy : invalid_proxies) {
      LOG_INFO("Testing invalid proxy URL: `", invalid_proxy);

      auto adapter = create_oss_store_with_proxy(invalid_proxy);
      ASSERT_NE(adapter, nullptr);
      DEFER(delete adapter);

      // Try an operation - should fail
      int ret = adapter->check_bucket();
      EXPECT_NE(ret, 0) << "Should fail with malformed proxy: "
                        << invalid_proxy;
    }
  }

  void verify_valid_reverse_proxy_success() {
    LOG_INFO("OSS endpoint: `", FLAGS_oss_endpoint);

    // Start reverse proxy
    auto [proxy_url, proxy_resources] = start_reverse_proxy(get_oss_url());
    ASSERT_FALSE(proxy_url.empty()) << "Failed to start reverse proxy";

    photon::thread_sleep(1);

    // Create OSS adapter with valid proxy
    auto adapter = create_oss_store_with_proxy(proxy_url);
    ASSERT_NE(adapter, nullptr);
    DEFER(delete adapter);

    LOG_INFO("Created OSS adapter with proxy: `", proxy_url);

    // Test 1: Check bucket
    int ret = adapter->check_bucket();
    EXPECT_EQ(ret, 0) << "Failed to check bucket through proxy, errno=" << -ret;

    if (ret == 0) {
      LOG_INFO("✓ Bucket check succeeded through proxy");
    }

    // Test 2: List directory
    ObjectList results;
    std::string test_path = "/proxy_test_" + std::to_string(time(nullptr));
    ret = adapter->list_dir(test_path, results);

    // List may fail if dir doesn't exist, but connection should work
    LOG_INFO("List dir result: ` (connection test)", ret);

    // Verify requests went through proxy
    LOG_INFO("Proxy request count: `", g_proxy_request_count.load());

    EXPECT_GT(g_proxy_request_count.load(), 0)
        << "No requests were forwarded through proxy";

    LOG_INFO("✓ Valid reverse proxy test passed");
  }

  void verify_proxy_file_operations() {
    // Start reverse proxy
    auto [proxy_url, proxy_resources] = start_reverse_proxy(get_oss_url());
    ASSERT_FALSE(proxy_url.empty());

    photon::thread_sleep(1);

    // Create OSS adapter with proxy
    auto adapter = create_oss_store_with_proxy(proxy_url);
    ASSERT_NE(adapter, nullptr);
    DEFER(delete adapter);

    // Test file operations through proxy

    // 1. Create a test file path
    std::string test_file =
        "/proxy_test_file_" + std::to_string(time(nullptr)) + ".txt";
    std::string test_content = "Hello through proxy!";

    LOG_INFO("Testing file upload through proxy: `", test_file);

    // 2. Upload file (put object)
    struct iovec iov;
    iov.iov_base = const_cast<char *>(test_content.data());
    iov.iov_len = test_content.size();

    ssize_t write_ret = adapter->put_object(test_file, &iov, 1);
    EXPECT_EQ(write_ret, (ssize_t)test_content.size())
        << "Failed to upload file through proxy";

    if (write_ret > 0) {
      LOG_INFO("✓ File uploaded through proxy: ` bytes", write_ret);
    }

    // 3. Download file (get object)
    char read_buf[1024] = {0};
    struct iovec read_iov;
    read_iov.iov_base = read_buf;
    read_iov.iov_len = test_content.size();

    ssize_t read_ret = adapter->get_object_range(test_file, &read_iov, 1, 0);
    EXPECT_EQ(read_ret, (ssize_t)test_content.size())
        << "Failed to download file through proxy";

    if (read_ret > 0) {
      EXPECT_STREQ(read_buf, test_content.c_str());
      LOG_INFO("✓ File downloaded through proxy: `",
               std::string(read_buf, read_ret));
    }

    // 4. Get file metadata
    ObjectHeaderMeta meta;
    int stat_ret = adapter->head_object(test_file, meta);
    EXPECT_EQ(stat_ret, 0) << "Failed to get file metadata through proxy";

    if (stat_ret == 0) {
      LOG_INFO("✓ File metadata retrieved, size: `", meta.size);
    }

    // 5. Delete file
    int delete_ret = adapter->delete_object(test_file);
    EXPECT_EQ(delete_ret, 0) << "Failed to delete file through proxy";

    if (delete_ret == 0) {
      LOG_INFO("✓ File deleted through proxy");
    }

    // Verify proxy was used
    LOG_INFO("Proxy request count: `", g_proxy_request_count.load());

    EXPECT_GT(g_proxy_request_count.load(), 0)
        << "File operations did not go through proxy";

    LOG_INFO("✓ Proxy file operations test passed");
  }

  void verify_proxy_timeout_behavior() {
    // Create a slow backend server that answers long after the client timeout
    SocketServerPtr slow_server(photon::net::new_tcp_socket_server());
    slow_server->timeout(10000UL * 1000);
    ASSERT_EQ(slow_server->bind_v4localhost(), 0);
    ASSERT_EQ(slow_server->listen(), 0);

    HTTPServerPtr slow_http_server(new_http_server());
    slow_http_server->add_handler({nullptr, &slow_server_handler});
    slow_server->set_handler(slow_http_server->get_connection_handler());
    slow_server->start_loop();

    std::string slow_url = get_server_url(slow_server.get());
    LOG_INFO("Slow server started at: `", slow_url);

    // Start reverse proxy pointing to slow server
    auto [proxy_url, proxy_resources] = start_reverse_proxy(slow_url);
    ASSERT_FALSE(proxy_url.empty());

    photon::thread_sleep(1);

    // The proxy forwards the request target verbatim, so the client has to name
    // the slow server itself: plain http plus path style, no bucket subdomain.
    // No trailing slash here, or the path-style URL gets a double slash.
    ObjStoreOptions options;
    options.endpoint =
        "http://127.0.0.1:" + std::to_string(slow_server->getsockname().port);
    options.bucket = "test";
    options.path_style = true;
    options.proxy = proxy_url;
    options.request_timeout_us = 1000000;  // 1 second timeout
    options.user_agent = "TimeoutTest/1.0";

    auto auth = new_basic_oss_authenticator({"test_ak", "test_sk"});
    auto adapter = new OssStore(options, auth);
    DEFER(delete adapter);

    // This should timeout
    int ret = adapter->check_bucket();
    EXPECT_NE(ret, 0) << "Should timeout with slow backend";
    EXPECT_GT(g_slow_request_count.load(), 0)
        << "The slow backend was never reached, so the timeout proves nothing";

    LOG_INFO("✓ Timeout behavior test passed (ret=`, expected failure)", ret);
  }

  void verify_compare_direct_vs_proxy() {
    // Test 1: Direct access (no proxy)
    LOG_INFO("Testing direct access (no proxy)...");
    ObjStoreOptions direct_options;
    direct_options.endpoint = FLAGS_oss_endpoint;
    direct_options.bucket = FLAGS_oss_bucket;
    direct_options.proxy = "";  // No proxy
    direct_options.request_timeout_us = FLAGS_oss_request_timeout_ms * 1000;

    auto direct_auth = new_basic_oss_authenticator(
        {FLAGS_oss_access_key_id, FLAGS_oss_access_key_secret});
    auto direct_adapter = new OssStore(direct_options, direct_auth);
    DEFER(delete direct_adapter);

    int direct_ret = direct_adapter->check_bucket();
    LOG_INFO("Direct access result: `", direct_ret);

    // Test 2: Proxy access
    LOG_INFO("Testing proxy access...");
    auto [proxy_url, proxy_resources] = start_reverse_proxy(get_oss_url());
    ASSERT_FALSE(proxy_url.empty());

    photon::thread_sleep(1);

    auto proxy_adapter = create_oss_store_with_proxy(proxy_url);
    ASSERT_NE(proxy_adapter, nullptr);
    DEFER(delete proxy_adapter);

    int proxy_ret = proxy_adapter->check_bucket();
    LOG_INFO("Proxy access result: `", proxy_ret);

    // Both should succeed (or both should fail with same error)
    if (direct_ret == 0) {
      EXPECT_EQ(proxy_ret, 0)
          << "Proxy access failed but direct access succeeded";
      LOG_INFO("✓ Both direct and proxy access succeeded");
    } else {
      LOG_INFO("Direct access failed with `, proxy also failed with `",
               direct_ret, proxy_ret);
    }

    LOG_INFO("Proxy request count: `", g_proxy_request_count.load());
  }

  void verify_filesystem_through_proxy() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    EXPECT_GT(g_proxy_request_count.load(), 0)
        << "No requests were forwarded through proxy";
  }
};

TEST_F(Ossfs2HttpProxyTest, verify_invalid_proxy_connection_refused) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_invalid_proxy_connection_refused();
}

TEST_F(Ossfs2HttpProxyTest, verify_invalid_proxy_malformed_url) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_invalid_proxy_malformed_url();
}

TEST_F(Ossfs2HttpProxyTest, verify_valid_reverse_proxy_success) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_valid_reverse_proxy_success();
}

TEST_F(Ossfs2HttpProxyTest, verify_proxy_file_operations) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_proxy_file_operations();
}

TEST_F(Ossfs2HttpProxyTest, verify_proxy_timeout_behavior) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_proxy_timeout_behavior();
}

TEST_F(Ossfs2HttpProxyTest, verify_compare_direct_vs_proxy) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_compare_direct_vs_proxy();
}

TEST_F(Ossfs2HttpProxyTest, verify_additional_headers) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  gflags::FlagSaver flag_saver;
  FLAGS_oss_additional_headers =
      "x-oss-user-hint-traffic-class=Y29wcGVy"
      ";x-myapp_source=dHJhaW5pbmc="
      ";x-oss-example=YTti"
      ";x-oss-path=QzpcdG1w"
      ";x-oss-query=ImEsYj1jJmQ9JTJGIg==";
  std::vector<std::pair<std::string, std::string>> headers = {
      {"x-oss-user-hint-traffic-class", "copper"},
      {"x-myapp_source", "training"},
      {"x-oss-example", "a;b"},
      {"x-oss-path", R"(C:\tmp)"},
      {"x-oss-query", R"("a,b=c&d=%2F")"}};
  auto url = get_oss_url();
  if (url.find("https://") == 0) url.replace(0, 8, "http://");
  FLAGS_oss_endpoint = url;
  auto [proxy_url, proxy_resources] = start_reverse_proxy(url, &headers);
  ASSERT_FALSE(proxy_url.empty());
  FLAGS_http_proxy = proxy_url;
  OssFsOptions opts;
  opts.enable_appendable_object = false;
  opts.upload_buffer_size = 5 * 1024 * 1024;
  ASSERT_NO_FATAL_FAILURE(init(opts));
  verify_filesystem_through_proxy();

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  auto crc64 = create_file_in_folder(parent, "data", 10, nodeid);
  ASSERT_NE(nodeid, 0ULL);
  DEFER(fs_->forget(nodeid, 1));
  uint64_t read_crc64 = 0;
  EXPECT_EQ(read_file_in_folder(parent, "data", &read_crc64), 10 * 1024 * 1024);
  EXPECT_EQ(read_crc64, crc64);
  EXPECT_EQ(fs_->unlink(parent, "data"), 0);
  EXPECT_GT(g_proxy_request_count.load(), 0);
}

TEST_F(Ossfs2HttpProxyTest, verify_additional_headers_failure) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  {
    gflags::FlagSaver flag_saver;
    FLAGS_oss_additional_headers = "x-custom source=dHJhaW5pbmc=";
    ASSERT_EQ(do_init(opts), -EINVAL);
    ASSERT_EQ(fs_, nullptr);
    ASSERT_EQ(bg_vcpu_env_.bg_obj_store_env, nullptr);
    ASSERT_EQ(bg_vcpu_env_.bg_disk_cache_env, nullptr);
  }
  ASSERT_NO_FATAL_FAILURE(init(opts));
  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  create_file_in_folder(parent, "data", 1, nodeid);
  ASSERT_NE(nodeid, 0ULL);
  DEFER(fs_->forget(nodeid, 1));
  DEFER(fs_->unlink(parent, "data"));

  auto url = get_oss_url();
  if (url.find("https://") == 0) url.replace(0, 8, "http://");
  std::vector<std::pair<std::string, std::string>> headers = {
      {"Range", "bytes=0-0"},
      {"x-oss-range-behavior", "standard"},
      {"x-myapp-id", "first"}};
  auto [proxy_url, proxy_resources] = start_reverse_proxy(url, &headers);
  ASSERT_FALSE(proxy_url.empty());
  ObjStoreOptions options = oss_options_;
  options.endpoint = url;
  options.proxy = proxy_url;
  options.custom_headers = {{"x-oversized", std::string(16 * 1024, 'z')}};
  ScopedBlockAllSignal block_signals;
  photon::Executor executor(OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE, {},
                            EXECUTOR_QUEUE_OPTION);
  std::string log_path = join_paths(test_path_, "headers.log");
  auto *output = new_log_output_file(log_path.c_str());
  ASSERT_NE(output, nullptr);
  DEFER(output->destruct());
  ASSERT_GE(output->get_log_file_fd(), 0);
  ALogLogger logger{output, ALOG_ERROR};
  auto *previous_logger = ossfs_logger_ptr;
  ossfs_logger_ptr = &logger;
  DEFER(ossfs_logger_ptr = previous_logger);

  auto adapter = executor.perform(
      [&]() { return new_oss_store("invalid-ak", "invalid-sk", options); });
  DEFER(executor.perform([&]() { delete adapter; }));
  char buf;
  iovec iov{&buf, 1};
  auto ret = executor.perform([&]() {
    adapter->set_credentials(
        {FLAGS_oss_access_key_id, FLAGS_oss_access_key_secret, ""}, {});
    return adapter->get_object_range("/" + gtest_base_dir + "/data", &iov, 1,
                                     0);
  });
  EXPECT_EQ(ret, -ENOBUFS);
  EXPECT_EQ(g_proxy_request_count.load(), 0);
  ret = executor.perform([&]() {
    return adapter->delete_objects_under_dir("/" + gtest_base_dir + "/",
                                             {"data"});
  });
  EXPECT_EQ(ret, -ENOBUFS);
  EXPECT_EQ(g_proxy_request_count.load(), 0);

  std::ifstream log(log_path);
  std::string text((std::istreambuf_iterator<char>(log)),
                   std::istreambuf_iterator<char>());
  EXPECT_NE(text.find("OSS header insert failed x-oversized"),
            std::string::npos);
  EXPECT_EQ(text.find(std::string(32, 'z')), std::string::npos);

  options.custom_headers = {{"Host", "unused.invalid"},
                            {"Range", "bytes=1-1"},
                            {"x-oss-range-behavior", "invalid"},
                            {"x-myapp-id", "first"},
                            {"X-Myapp-Id", "second"},
                            {"Authorization", "invalid"},
                            {"Date", "invalid"}};
  auto existing_adapter = executor.perform([&]() {
    return new_oss_store(FLAGS_oss_access_key_id.c_str(),
                         FLAGS_oss_access_key_secret.c_str(), options);
  });
  DEFER(executor.perform([&]() { delete existing_adapter; }));
  ret = executor.perform([&]() {
    return existing_adapter->get_object_range("/" + gtest_base_dir + "/data",
                                              &iov, 1, 0);
  });
  EXPECT_EQ(ret, 1);
  EXPECT_GT(g_proxy_request_count.load(), 0);
}

TEST_F(Ossfs2HttpProxyTest, verify_filesystem_through_proxy) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  // Start reverse proxy
  auto [proxy_url, proxy_resources] = start_reverse_proxy(get_oss_url());
  ASSERT_FALSE(proxy_url.empty());
  FLAGS_http_proxy = proxy_url;
  DEFER(FLAGS_http_proxy = "");

  OssFsOptions opts;
  init(opts);
  verify_filesystem_through_proxy();
}

// QPS-limit retry rebuilds the request headers to re-sign. Host must stay
// at physical offset 0, otherwise a later 307 redirect corrupts the whole
// header block via reset_host(). Probe: 503 QpsLimitExceeded -> 307 to the
// same URL -> 200 list response.
struct ResignProbeRecord {
  std::string host;
  int host_count = 0;
  std::string auth;
  std::string sign_date;
};

static std::mutex g_resign_probe_mtx;
static std::vector<ResignProbeRecord> g_resign_probe_records;
static std::atomic<int> g_resign_probe_count{0};
static std::string g_resign_probe_location;

static int resign_probe_handler(void *, Request &req, Response &resp,
                                std::string_view) {
  int n = g_resign_probe_count.fetch_add(1) + 1;
  ResignProbeRecord rec;
  for (auto kv : req.headers) {
    if (kv.first == "Host") {
      rec.host.assign(kv.second.data(), kv.second.size());
      rec.host_count++;
    } else if (kv.first == "Authorization") {
      rec.auth.assign(kv.second.data(), kv.second.size());
    } else if (kv.first == "x-oss-date" || kv.first == "Date") {
      rec.sign_date.assign(kv.second.data(), kv.second.size());
    }
  }
  LOG_INFO("[ResignProbe] request ` host=` auth_empty=`", n, rec.host.c_str(),
           rec.auth.empty());
  {
    std::lock_guard<std::mutex> lock(g_resign_probe_mtx);
    g_resign_probe_records.push_back(std::move(rec));
  }
  std::string body;
  if (n == 1) {
    resp.set_result(503);
    body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>TotalQpsLimitExceeded</Code>"
        "<Message>mock</Message><RequestId>mock</RequestId>"
        "<EC>0000-00000000</EC></Error>";
  } else if (n == 2) {
    resp.set_result(307);
    resp.headers.insert("Location", g_resign_probe_location);
  } else {
    resp.set_result(200);
    body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><Name>mockbucket</Name><Prefix></Prefix>"
        "<KeyCount>0</KeyCount><MaxKeys>1</MaxKeys>"
        "<IsTruncated>false</IsTruncated></ListBucketResult>";
  }
  resp.headers.content_length(body.size());
  if (!body.empty()) resp.write(body.data(), body.size());
  return 0;
}

TEST_F(Ossfs2HttpProxyTest, verify_qps_resign_keeps_host_layout) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  g_resign_probe_records.clear();
  g_resign_probe_count.store(0);

  SocketServerPtr server(photon::net::new_tcp_socket_server());
  ASSERT_NE(server, nullptr);
  server->timeout(10000UL * 1000);
  ASSERT_EQ(server->bind_v4localhost(), 0);
  ASSERT_EQ(server->listen(), 0);
  HTTPServerPtr http_server(new_http_server());
  http_server->add_handler({nullptr, &resign_probe_handler});
  server->set_handler(http_server->get_connection_handler());
  server->start_loop();
  std::string url = get_server_url(server.get());
  LOG_INFO("[ResignProbe] server at `", url);
  std::string hostport = url.substr(strlen("http://"));
  hostport.pop_back();
  g_resign_probe_location = url + "mockbucket/?list-type=2&max-keys=1&prefix=";

  ObjStoreOptions options;
  // Keep the http:// scheme: a bare host:port endpoint defaults to HTTPS.
  options.endpoint = url.substr(0, url.size() - 1);
  options.bucket = "mockbucket";
  options.path_style = true;
  options.region = "cn-hangzhou";
  options.request_timeout_us = 60ULL * 1000 * 1000;
  options.qps_limit_total_retry_time_us = 3ULL * 1000 * 1000;
  options.user_agent = "Ossfs2ResignProbe/1.0";
  auto client =
      new_oss_client(options, new_basic_oss_authenticator({"ak", "sk"}));
  ASSERT_NE(client, nullptr);
  DEFER(delete client);

  auto list_cb = [](const ListObjectsCBParameters &) { return 0; };
  int r = client->list_objects("", list_cb);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(g_resign_probe_count.load(), 3);

  std::lock_guard<std::mutex> lock(g_resign_probe_mtx);
  ASSERT_EQ(g_resign_probe_records.size(), 3u);
  for (const auto &rec : g_resign_probe_records) {
    EXPECT_EQ(rec.host_count, 1);
    EXPECT_EQ(rec.host, hostport);
    EXPECT_FALSE(rec.auth.empty());
    EXPECT_FALSE(rec.sign_date.empty());
  }
  // The QPS retry re-signed with a refreshed signature date.
  EXPECT_NE(g_resign_probe_records[0].sign_date,
            g_resign_probe_records[1].sign_date);
}

// Compressed-time mock of the OSS 15-minute signature validity window:
// the probe rejects any retried request whose x-oss-date is >= 2s old.
// With a 3s QPS budget (sleeps 1s + 2s, three attempts) the retry loop
// must re-sign to keep every attempt inside the window.
static std::atomic<int> g_skew_probe_count{0};
static std::atomic<int> g_skew_probe_rejects{0};

static int skew_probe_handler(void *, Request &req, Response &resp,
                              std::string_view) {
  int n = g_skew_probe_count.fetch_add(1) + 1;
  std::string sign_date;
  for (auto kv : req.headers) {
    if (kv.first == "x-oss-date")
      sign_date.assign(kv.second.data(), kv.second.size());
  }
  struct tm tm {};
  bool stale = true;
  if (strptime(sign_date.c_str(), "%Y%m%dT%H%M%SZ", &tm) != nullptr) {
    stale = difftime(time(nullptr), timegm(&tm)) >= 2;
  }
  std::string body;
  if (stale && n > 1) {
    g_skew_probe_rejects.fetch_add(1);
    resp.set_result(403);
    body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>RequestTimeTooSkewed</Code>"
        "<Message>mock skew</Message><RequestId>mock</RequestId>"
        "<EC>0000-00000000</EC></Error>";
  } else if (n < 3) {
    resp.set_result(503);
    body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>TotalQpsLimitExceeded</Code>"
        "<Message>mock</Message><RequestId>mock</RequestId>"
        "<EC>0000-00000000</EC></Error>";
  } else {
    resp.set_result(200);
    body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><Name>mockbucket</Name><Prefix></Prefix>"
        "<KeyCount>0</KeyCount><MaxKeys>1</MaxKeys>"
        "<IsTruncated>false</IsTruncated></ListBucketResult>";
  }
  resp.headers.content_length(body.size());
  if (!body.empty()) resp.write(body.data(), body.size());
  return 0;
}

TEST_F(Ossfs2HttpProxyTest, verify_qps_resign_beats_skew_window) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  g_skew_probe_count.store(0);
  g_skew_probe_rejects.store(0);

  SocketServerPtr server(photon::net::new_tcp_socket_server());
  ASSERT_NE(server, nullptr);
  server->timeout(10000UL * 1000);
  ASSERT_EQ(server->bind_v4localhost(), 0);
  ASSERT_EQ(server->listen(), 0);
  HTTPServerPtr http_server(new_http_server());
  http_server->add_handler({nullptr, &skew_probe_handler});
  server->set_handler(http_server->get_connection_handler());
  server->start_loop();
  std::string url = get_server_url(server.get());

  ObjStoreOptions options;
  // Keep the http:// scheme: a bare host:port endpoint defaults to HTTPS.
  options.endpoint = url.substr(0, url.size() - 1);
  options.bucket = "mockbucket";
  options.path_style = true;
  options.region = "cn-hangzhou";
  options.request_timeout_us = 60ULL * 1000 * 1000;
  options.qps_limit_total_retry_time_us = 3ULL * 1000 * 1000;
  options.user_agent = "Ossfs2SkewProbe/1.0";
  auto client =
      new_oss_client(options, new_basic_oss_authenticator({"ak", "sk"}));
  ASSERT_NE(client, nullptr);
  DEFER(delete client);

  auto list_cb = [](const ListObjectsCBParameters &) { return 0; };
  int r = client->list_objects("", list_cb);
  ASSERT_EQ(r, 0);
  EXPECT_EQ(g_skew_probe_count.load(), 3);
  EXPECT_EQ(g_skew_probe_rejects.load(), 0);
}
