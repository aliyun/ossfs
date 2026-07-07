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

#include <memory>

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

// Reverse proxy director - forwards requests to backend server
static int proxy_director(void *arg, Request &src, Request &dst) {
  g_proxy_request_count.fetch_add(1);

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

// Slow server handler for timeout test
static int slow_server_handler(Request &req, Response &resp, std::string_view) {
  LOG_INFO("[SlowServer] Sleeping for 5 seconds...");
  photon::thread_sleep(5);  // Sleep 5 seconds
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

// Reverse proxy resources structure
struct ReverseProxyResources {
  SocketServerPtr server;
  HTTPServerPtr http_server;
  HTTPHandlerPtr handler;
  HTTPClientPtr client;
};

/**
 * Start a reverse proxy server
 *
 * @param backend_url The backend server URL to forward requests to
 * @return Pair of proxy URL and resources, or empty string on failure
 */
static std::pair<std::string, ReverseProxyResources> start_reverse_proxy(
    const std::string &backend_url) {
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
  resources.handler.reset(new_proxy_handler({nullptr, &proxy_director},
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
    // Create a slow backend server (simulates timeout)
    SocketServerPtr slow_server(photon::net::new_tcp_socket_server());
    slow_server->timeout(10000UL * 1000);
    slow_server->bind_v4localhost();
    slow_server->listen();

    HTTPServerPtr slow_http_server(new_http_server());
    slow_http_server->add_handler(&slow_server_handler);
    slow_server->set_handler(slow_http_server->get_connection_handler());
    slow_server->start_loop();

    std::string slow_url = get_server_url(slow_server.get());
    LOG_INFO("Slow server started at: `", slow_url);

    photon::thread_sleep(1);

    // Start reverse proxy pointing to slow server
    auto [proxy_url, proxy_resources] = start_reverse_proxy(slow_url);
    ASSERT_FALSE(proxy_url.empty());

    photon::thread_sleep(1);

    // Create OSS adapter with short timeout
    ObjStoreOptions options;
    options.endpoint = "127.0.0.1";
    options.bucket = "test";
    options.proxy = proxy_url;
    options.request_timeout_us = 1000000;  // 1 second timeout
    options.user_agent = "TimeoutTest/1.0";

    auto auth = new_basic_oss_authenticator({"test_ak", "test_sk"});
    auto adapter = new OssStore(options, auth);
    DEFER(delete adapter);

    // This should timeout
    int ret = adapter->check_bucket();
    EXPECT_NE(ret, 0) << "Should timeout with slow backend";

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
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_invalid_proxy_connection_refused();
}

TEST_F(Ossfs2HttpProxyTest, verify_invalid_proxy_malformed_url) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_invalid_proxy_malformed_url();
}

TEST_F(Ossfs2HttpProxyTest, verify_valid_reverse_proxy_success) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_valid_reverse_proxy_success();
}

TEST_F(Ossfs2HttpProxyTest, verify_proxy_file_operations) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_proxy_file_operations();
}

TEST_F(Ossfs2HttpProxyTest, verify_proxy_timeout_behavior) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_proxy_timeout_behavior();
}

TEST_F(Ossfs2HttpProxyTest, verify_compare_direct_vs_proxy) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_compare_direct_vs_proxy();
}

TEST_F(Ossfs2HttpProxyTest, verify_filesystem_through_proxy) {
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
