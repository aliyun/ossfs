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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <string>
#include <thread>

#include "common/logger.h"
#include "common/ssl_probe.h"

DEFINE_string(config_file, "/etc/ossfs2-gtest.conf",
              "default config file path");

namespace {

// Read --config_file from argv before flag parsing. Options defined in
// options.cpp (e.g. oss_endpoint/oss_bucket) carry non-empty validators that
// run during ParseCommandLineFlags against current values; their configured
// values live in the flagfile, so the flagfile must be loaded first or parse
// fails on the empty defaults.
std::string extract_config_file(int argc, char **argv) {
  std::string config_file = FLAGS_config_file;
  const std::string kPrefix = "--config_file=";
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    if (arg.rfind(kPrefix, 0) == 0) {
      config_file = arg.substr(kPrefix.size());
    } else if (arg == "--config_file" && i + 1 < argc) {
      config_file = argv[++i];
    }
  }
  return config_file;
}

}  // namespace

int main(int argc, char **arg) {
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = SA_SIGINFO;

  if (sigaction(SIGPIPE, &sa, NULL) == -1) {
    perror("sigaction");
    exit(EXIT_FAILURE);
  }

  if (!SSLProbe::setup_ssl_env()) {
    LOG_WARN("Failed to get SSL certificate file");
  }

  ::testing::InitGoogleTest(&argc, arg);

  std::string config_file = extract_config_file(argc, arg);
  gflags::SetCommandLineOption("flagfile", config_file.c_str());
  gflags::ParseCommandLineFlags(&argc, &arg, true);

  std::atomic<bool> stopped = {false};
  std::thread signal_handler_thread([&stopped]() {
    while (!stopped) {
      sleep(1);
    }
  });

  int r = RUN_ALL_TESTS();
  stopped = true;
  signal_handler_thread.join();
  return r;
}
