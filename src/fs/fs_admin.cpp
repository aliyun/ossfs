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

#include <sys/types.h>
#include <unistd.h>

#include "admin/uds_server.h"
#include "common/utils.h"
#include "fs.h"
#include "metric/metrics.h"

namespace OssFileSystem {

std::string OssFs::process_uds_request(std::string_view action,
                                       std::string_view param) {
  if (action == "stats") {
    return Metric::get_metrics_string(std::atoll(param.data()));
  } else if (action == "set-metrics") {
    return Metric::set_enabled_metrics(param);
  }

  std::string output = "Not supported for action \"" + std::string(action) +
                       "\" and param \"" + std::string(param) + "\"";
  return output;
}

void OssFs::start_uds_server(std::promise<bool> &uds_server_running) {
  INIT_PHOTON();

  auto uds_path = Admin::generate_uds_path_from_pid(getpid());

  photon::net::ISocketServer *uds_server = nullptr;
  int r = 0;
  DEFER({
    if (r != 0 || !uds_server) uds_server_running.set_value(false);
    if (uds_server) {
      LOG_INFO("Destroy uds server");
      Admin::destroy_uds_server(uds_server);
      ::unlink(uds_path.c_str());
    }
  });

  auto process_func = [this](std::string_view action, std::string_view param) {
    return this->process_uds_request(action, param);
  };
  uds_server = Admin::create_uds_server(uds_path, process_func);
  if (!uds_server) {
    int error_number = errno;
    LOG_ERROR("Failed to create uds server, errno: `", error_number);
    return;
  }

  r = uds_server->start_loop();
  if (r != 0) {
    int error_number = errno;
    LOG_ERROR("Failed to start Unix Domain Socket loop `, errno: `", uds_path,
              error_number);
    return;
  }

  LOG_INFO("Start uds server: `", uds_path);
  uds_server_running.set_value(true);

  while (!is_stopping_) photon::thread_usleep(100 * 1000);
}

}  // namespace OssFileSystem
