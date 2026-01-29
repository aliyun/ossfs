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

#include "http_server.h"

#include <photon/net/http/server.h>
#include <photon/thread/thread.h>

#include "common/filesystem.h"
#include "common/logger.h"
#include "metric/metrics_server.h"

namespace OssFileSystem {
namespace Admin {

int start_http_server(const bool &is_stopping, std::string &exposed_ip,
                      uint16_t port) {
  auto tcpserver = photon::net::new_tcp_socket_server();
  if (!tcpserver) {
    LOG_ERRNO_RETURN(1, -1, "fail to new_tcp_socket_server");
  }
  DEFER(delete tcpserver);

  tcpserver->timeout(1000UL * 1000 * 10);  // 10s
  // Exposed to any ip.
  tcpserver->bind(port, photon::net::IPAddr(exposed_ip.c_str()));
  tcpserver->listen();

  auto server = photon::net::http::new_http_server();
  if (!server) {
    LOG_ERRNO_RETURN(1, -1, "fail to new_http_server");
  }
  DEFER(delete server);

  Metric::MetricHandler *handler = new Metric::MetricHandler();
  if (!handler) {
    LOG_ERRNO_RETURN(1, -1, "fail to new MetricHandler");
  }
  DEFER(delete handler);

  server->add_handler(handler);
  tcpserver->set_handler(server->get_connection_handler());
  tcpserver->start_loop();

  while (!is_stopping) {
    photon::thread_usleep(1000 * 1000 * 1);
  }

  LOG_INFO("http server stoppped.");
  return 0;
}

}  // namespace Admin
}  // namespace OssFileSystem
