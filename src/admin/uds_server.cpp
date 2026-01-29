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

#include "uds_server.h"

#include <photon/net/http/client.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

#include "common/logger.h"
#include "common/utils.h"

namespace OssFileSystem {
namespace Admin {

static const uint32_t kUdsRecvSize = 1024;
static const uint32_t kUdsMessageLengthSize = 10;
static const std::string kUdsDir = "/run/ossfs2";

std::string generate_uds_path_from_pid(pid_t pid) {
  if (pid == 0) return "";
  return join_paths(kUdsDir, "ossfs2_uds_" + std::to_string(pid) + ".sock");
}

static void send_message(photon::net::ISocketStream *sock,
                         const std::string &data) {
  std::string length = std::to_string(data.size());
  length += std::string(kUdsMessageLengthSize - length.size(), '\0');
  sock->send(length.c_str(), length.size());

  sock->send(data.c_str(), data.size());
}

static void recv_message(photon::net::ISocketStream *sock,
                         std::string &output) {
  char length_buf[kUdsMessageLengthSize + 1] = {0};
  sock->recv(length_buf, kUdsMessageLengthSize);
  int length = std::atoi(length_buf);

  char buf[kUdsRecvSize] = {0};
  output.clear();
  while (length > 0) {
    auto recv_size = sock->recv(buf, kUdsRecvSize);
    output.append(buf, recv_size);
    length -= recv_size;
  }
}

void send_uds_request(const std::string &uds_path, const std::string &action,
                      const std::string &additional_param,
                      std::string &output) {
  auto client = photon::net::new_uds_client();
  if (client == nullptr) {
    std::cerr << "Failed to new_uds_client" << std::endl;
    return;
  }
  DEFER(delete client);
  client->timeout(5 * 1000 * 1000);  // 5s
  auto stream = client->connect(uds_path.c_str(), uds_path.size());
  if (stream == nullptr) {
    std::cerr << "Failed to connect to " << uds_path << std::endl;
    return;
  }
  DEFER(delete stream);

  std::string sent_content = action + "," + additional_param;
  send_message(stream, sent_content);

  recv_message(stream, output);
}

photon::net::ISocketServer *create_uds_server(
    const std::string &uds_path,
    std::function<std::string(std::string_view, std::string_view)> process) {
  photon::net::ISocketServer *uds_server = nullptr;
  int r = 0;
  DEFER({
    if (r != 0 && uds_server) delete uds_server;
  });

  auto handler = [process](photon::net::ISocketStream *sock) -> int {
    std::string input;
    recv_message(sock, input);

    std::string_view input_view = input;
    std::string_view action, param;
    auto pos = input.find(',');
    if (pos == std::string::npos) {
      action = input_view;
    } else {
      action = input_view.substr(0, pos);
      param = input_view.substr(pos + 1, input.size() - pos - 1);
    }

    std::string output = process(action, param);
    send_message(sock, output);
    return 0;
  };

  uds_server = photon::net::new_uds_server();
  if (uds_server == nullptr) {
    LOG_ERROR("Failed to new_uds_server");
    return nullptr;
  }

  ::mkdir(kUdsDir.c_str(), 0755);
  r = uds_server->bind(uds_path.c_str(), uds_path.size());
  if (r != 0) {
    if (errno == EADDRINUSE) {
      LOG_WARN("Remove UDS file: `", uds_path);
      ::unlink(uds_path.c_str());
      r = uds_server->bind(uds_path.c_str(), uds_path.size());
    }

    if (r != 0) {
      LOG_ERROR("Failed to bind Unix Domain Socket `", uds_path);
      return nullptr;
    }
  }

  r = uds_server->set_handler(handler)->listen();
  if (r != 0) {
    LOG_ERROR("Failed to listen to Unix Domain Socket `", uds_path);
    return nullptr;
  }

  return uds_server;
}

void destroy_uds_server(photon::net::ISocketServer *server) {
  if (server) {
    server->terminate();
    delete server;
  }
}

}  // namespace Admin
}  // namespace OssFileSystem
