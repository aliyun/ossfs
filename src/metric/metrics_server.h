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

#include <photon/net/http/server.h>

#include <string>

namespace OssFileSystem {
namespace Metric {

class MetricHandler : public photon::net::http::HTTPHandler {
 public:
  void failed_resp(photon::net::http::Response &resp, int result = 404);
  int handle_request(photon::net::http::Request &req,
                     photon::net::http::Response &resp,
                     std::string_view not_used);
};

}  // namespace Metric
}  // namespace OssFileSystem
