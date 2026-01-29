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

#include "cli_stats.h"

#include <photon/photon.h>
#include <photon/thread/thread.h>
#include <unistd.h>

#include <iostream>

#include "common/logger.h"
#include "common/macros.h"
#include "uds_server.h"

namespace OssFileSystem {
namespace Admin {

static void set_enabled_metrics_to_uds_server(const std::string &uds_path,
                                              const std::string &filter) {
  std::string output = "";
  send_uds_request(uds_path, "set-metrics", filter, output);
  std::cout << output << std::endl;
}

static void print_stats_from_uds_server(const std::string &uds_path,
                                        size_t interval) {
  std::string output = "";
  send_uds_request(uds_path, "stats", std::to_string(interval), output);
  std::cout << output << std::endl;
}

static void print_time() {
  time_t rawtime;
  struct tm *timeinfo;
  char buffer[128];
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buffer, sizeof(buffer), "%Y/%m/%d %H:%M:%S", timeinfo);
  std::cout << buffer << " ";
}

int stats_show_command_handler(bool stats_continue, pid_t pid,
                               size_t interval) {
  if (pid == 0) {
    std::cerr << "Error: pid is not specified" << std::endl;
    return 1;
  }
  auto uds_path = generate_uds_path_from_pid(pid);
  if (stats_continue) {
    while (true) {
      print_time();
      print_stats_from_uds_server(uds_path, interval);
      photon::thread_usleep(interval * 1000000);
    }
  } else {
    print_stats_from_uds_server(uds_path, interval);
  }
  return 0;
}

int stats_set_command_handler(pid_t pid, const std::string &filter) {
  if (pid == 0) {
    std::cerr << "Error: pid is not specified" << std::endl;
    return 1;
  }
  auto uds_path = generate_uds_path_from_pid(pid);
  set_enabled_metrics_to_uds_server(uds_path, filter);
  return 0;
}

}  // namespace Admin
}  // namespace OssFileSystem
