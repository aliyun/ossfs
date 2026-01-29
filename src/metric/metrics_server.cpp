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

#include "metrics_server.h"

#include <gflags/gflags.h>
#include <string.h>

#include <array>

#include "common/logger.h"
#include "metric/metrics.h"

DEFINE_uint64(metrics_interval, 60, "metrics collection interval in seconds");

namespace OssFileSystem {
namespace Metric {

enum PrometheusMetrics {
  kReadSize = 0,
  kReadQps = 1,
  kReadLatency = 2,
  kWriteSize = 3,
  kWriteQps = 4,
  kWriteLatency = 5,
  kMetricsCount = 6
};

static std::array<std::string, PrometheusMetrics::kMetricsCount> kMetricsNames =
    {"read_bytes",  "read_qps",  "read_latency",
     "write_bytes", "write_qps", "write_latency"};

// Output last `FLAGS_metrics_interval` seconds metrics in prometheus format.
static int collect_metrics(std::string &output) {
  auto cur_metrics = get_metrics_map(FLAGS_metrics_interval);
  auto get_metric_val = [&](const std::string &key) -> uint64_t {
    auto iter = cur_metrics.find(key);
    if (iter == cur_metrics.end()) {
      return 0;
    }
    return iter->second;
  };

  for (int i = 0; i < int(PrometheusMetrics::kMetricsCount); i++) {
    std::string type = "gauge";
    std::string metric_str = "ossfs2_" + kMetricsNames[i];
    output += ("# HELP " + metric_str + "\n");
    output += ("# TYPE " + metric_str + " " + type + "\n");

    uint64_t val = get_metric_val(kMetricsNames[i]);
    output += (metric_str + " " + std::to_string(val)) + "\n";
  }

  return 0;
}

void MetricHandler::failed_resp(photon::net::http::Response &resp, int result) {
  resp.set_result(result);
  resp.headers.content_length(0);
  resp.keep_alive(true);
}

int MetricHandler::handle_request(photon::net::http::Request &req,
                                  photon::net::http::Response &resp,
                                  std::string_view not_used) {
  auto target = req.target();
  if (target == "/metrics") {
    std::string metric_str;
    collect_metrics(metric_str);
    resp.set_result(200);
    resp.headers.insert("Content-Type", "text/plain");
    resp.headers.insert("Content-Length", std::to_string(metric_str.size()));
    return resp.write(metric_str.c_str(), metric_str.size());
  } else {
    failed_resp(resp);
    LOG_ERROR_RETURN(0, 0, "` not supported", target);
  }

  return 0;
}

}  // namespace Metric
}  // namespace OssFileSystem
