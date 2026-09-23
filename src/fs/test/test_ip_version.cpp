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

#include <netdb.h>
#include <photon/net/http/server.h>
#include <photon/net/socket.h>
#include <sys/socket.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "test_suite.h"

using photon::net::EndPoint;
using photon::net::IPAddr;
using photon::net::ISocketClient;
using photon::net::ISocketServer;
using photon::net::http::HTTPServer;
using photon::net::http::Request;
using photon::net::http::Response;
using photon::net::http::Verb;

namespace {

// Stand-in for OSS listening on the loopback. "localhost" resolves to both
// families, so when both listeners share a port, the counter that moves tells
// which address ossfs2 dialed.
std::atomic<int> g_hits_v4{0};
std::atomic<int> g_hits_v6{0};

// An empty bucket listing: it satisfies the ListObjects that check_bucket
// issues while mounting, and reports every object as missing.
constexpr char kEmptyBucket[] =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<ListBucketResult><Name>ossfs2-ip-version-test</Name><Prefix/>"
    "<MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>";

int respond_like_oss(Request &req, Response &resp) {
  if (req.verb() == Verb::GET) {
    resp.set_result(200);
    resp.headers.content_length(sizeof(kEmptyBucket) - 1);
    resp.write(kEmptyBucket, sizeof(kEmptyBucket) - 1);
  } else {
    resp.set_result(404);
    resp.headers.content_length(0);
  }
  return 0;
}

int serve_v4(void *, Request &req, Response &resp, std::string_view) {
  g_hits_v4.fetch_add(1);
  return respond_like_oss(req, resp);
}

int serve_v6(void *, Request &req, Response &resp, std::string_view) {
  g_hits_v6.fetch_add(1);
  return respond_like_oss(req, resp);
}

// Environment probe, deliberately independent of the code under test. Both
// addresses come from the hosts file, so this performs no network I/O.
bool localhost_is_dual_stack() {
  struct addrinfo hints {
  }, *res = nullptr;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo("localhost", nullptr, &hints, &res) != 0) return false;
  bool has_v4 = false, has_v6 = false;
  for (auto *ai = res; ai != nullptr; ai = ai->ai_next) {
    if (ai->ai_family == AF_INET) has_v4 = true;
    if (ai->ai_family == AF_INET6) has_v6 = true;
  }
  freeaddrinfo(res);
  return has_v4 && has_v6;
}

bool ipv4_loopback_busy(uint16_t port) {
  std::unique_ptr<ISocketClient> client(photon::net::new_tcp_socket_client());
  if (!client) return true;
  client->timeout(200UL * 1000);
  auto stream = client->connect(EndPoint(IPAddr::V4Loopback(), port));
  bool busy = stream != nullptr;
  delete stream;
  return busy;
}

struct LoopbackStub {
  std::unique_ptr<ISocketServer> sock;
  std::unique_ptr<HTTPServer> http;
};

// Binds one loopback family, picking a free port when given 0. Returns the
// bound port, or 0 on failure. Ownership stays with `stubs`, which the caller
// keeps on the stack so the listeners die before photon is finalized.
uint16_t start_stub(std::vector<LoopbackStub> &stubs, bool ipv6,
                    uint16_t port) {
  LoopbackStub stub;
  stub.sock.reset(photon::net::new_tcp_socket_server());
  stub.http.reset(photon::net::http::new_http_server());
  stub.sock->timeout(10UL * 1000 * 1000);
  int r = ipv6 ? stub.sock->bind_v6localhost(port)
               : stub.sock->bind_v4localhost(port);
  if (r < 0 || stub.sock->listen() < 0) {
    LOG_WARN("bind ` loopback port ` failed, errno `", ipv6 ? "IPv6" : "IPv4",
             port, errno);
    return 0;
  }
  stub.http->add_handler({nullptr, ipv6 ? &serve_v6 : &serve_v4});
  stub.sock->set_handler(stub.http->get_connection_handler());
  stub.sock->start_loop();
  uint16_t bound = stub.sock->getsockname().port;
  stubs.push_back(std::move(stub));
  return bound;
}

// One hostname, two candidate addresses. Retries because the ephemeral IPv6
// port may already be taken on IPv4; a listener from a failed attempt is never
// dialed and simply stays in `stubs`.
uint16_t start_dual_stack_stubs(std::vector<LoopbackStub> &stubs,
                                int attempts = 8) {
  for (int i = 0; i < attempts; i++) {
    uint16_t port = start_stub(stubs, true, 0);
    if (port != 0 && start_stub(stubs, false, port) == port) return port;
  }
  return 0;
}

// IPv6 only, with the matching IPv4 port verified closed: an ossfs2 that merely
// preferred IPv4 would still find a working address here.
uint16_t start_ipv6_only_stub(std::vector<LoopbackStub> &stubs,
                              int attempts = 8) {
  for (int i = 0; i < attempts; i++) {
    uint16_t port = start_stub(stubs, true, 0);
    if (port != 0 && !ipv4_loopback_busy(port)) return port;
  }
  return 0;
}

}  // namespace

class Ossfs2IPVersionTest : public Ossfs2TestSuite {
 protected:
  void SetUp() override {
    Ossfs2TestSuite::SetUp();
    g_hits_v4.store(0);
    g_hits_v6.store(0);
  }

  void TearDown() override {
    FLAGS_oss_endpoint = saved_endpoint_;
    FLAGS_path_style = saved_path_style_;
    FLAGS_enable_ipv6 = saved_enable_ipv6_;
    Ossfs2TestSuite::TearDown();
  }

  // Points the mount at a loopback stub by setting mount options only. The
  // option -> object store translation stays the one ossfs2 performs itself.
  void aim_at_stub(uint16_t port, bool enable_ipv6) {
    FLAGS_oss_endpoint = "http://localhost:" + std::to_string(port);
    // The stub answers on its own name; virtual-hosted style would ask for
    // "<bucket>.localhost" instead.
    FLAGS_path_style = true;
    FLAGS_enable_ipv6 = enable_ipv6;
  }

  const std::string saved_endpoint_ = FLAGS_oss_endpoint;
  const bool saved_path_style_ = FLAGS_path_style;
  const bool saved_enable_ipv6_ = FLAGS_enable_ipv6;
};

// Every case below needs "localhost" to have both an A and an AAAA record; on a
// single-stack host there is no IPv6 address to filter out.
#define SKIP_UNLESS_DUAL_STACK_LOOPBACK()                                 \
  do {                                                                    \
    if (!localhost_is_dual_stack())                                       \
      GTEST_SKIP() << "localhost does not resolve to both IPv4 and IPv6"; \
  } while (0)

// --enable_ipv6=false must mount over IPv4 even though the very same port is
// also listening on the IPv6 loopback.
TEST_F(Ossfs2IPVersionTest, verify_enable_ipv6_false_mounts_over_ipv4) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  SKIP_UNLESS_DUAL_STACK_LOOPBACK();

  std::vector<LoopbackStub> stubs;
  uint16_t port = start_dual_stack_stubs(stubs);
  ASSERT_NE(port, 0) << "failed to bind both loopback families on one port";
  aim_at_stub(port, /*enable_ipv6=*/false);

  ASSERT_EQ(do_init(OssFsOptions()), 0);

  // Serve a filesystem request as well, so the whole mounted client and not
  // only its mount-time bucket check is shown to stay on IPv4.
  uint64_t nodeid = 0;
  struct stat st {};
  EXPECT_EQ(fs_->lookup(root_nodeid_, "no_such_file", &nodeid, &st), -ENOENT);

  EXPECT_GT(g_hits_v4.load(), 0);
  EXPECT_EQ(g_hits_v6.load(), 0)
      << "the IPv6 address was dialed despite enable_ipv6=false";
}

// Control for the case above: with the default enable_ipv6=true an IPv6-only
// endpoint mounts fine, so the absence of IPv6 traffic there is a decision
// rather than an unreachable address.
TEST_F(Ossfs2IPVersionTest, verify_enable_ipv6_true_mounts_over_ipv6) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  SKIP_UNLESS_DUAL_STACK_LOOPBACK();

  std::vector<LoopbackStub> stubs;
  uint16_t port = start_ipv6_only_stub(stubs);
  ASSERT_NE(port, 0);
  aim_at_stub(port, /*enable_ipv6=*/true);

  ASSERT_EQ(do_init(OssFsOptions()), 0);
  EXPECT_GT(g_hits_v6.load(), 0);
}

// Nothing listens on the IPv4 loopback, so falling back to IPv6 would succeed.
// --enable_ipv6=false must discard the IPv6 address instead and fail the mount,
// proving it narrows resolution rather than reordering it.
TEST_F(Ossfs2IPVersionTest, verify_enable_ipv6_false_never_dials_ipv6) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  SKIP_UNLESS_DUAL_STACK_LOOPBACK();

  std::vector<LoopbackStub> stubs;
  uint16_t port = start_ipv6_only_stub(stubs);
  ASSERT_NE(port, 0);
  aim_at_stub(port, /*enable_ipv6=*/false);

  int r = do_init(OssFsOptions());
  LOG_INFO("mount against an IPv6-only endpoint with enable_ipv6=false: `", r);
  EXPECT_LT(r, 0);
  EXPECT_EQ(g_hits_v6.load(), 0)
      << "the IPv6 address was dialed despite enable_ipv6=false";
}
