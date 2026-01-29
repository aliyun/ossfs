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

#pragma once

#include <photon/net/socket.h>

#include <functional>

namespace OssFileSystem {
namespace Admin {

std::string generate_uds_path_from_pid(pid_t pid);

void send_uds_request(const std::string &uds_path, const std::string &action,
                      const std::string &additional_param, std::string &output);

photon::net::ISocketServer *create_uds_server(
    const std::string &uds_path,
    std::function<std::string(std::string_view, std::string_view)> process);

void destroy_uds_server(photon::net::ISocketServer *server);

}  // namespace Admin
}  // namespace OssFileSystem
