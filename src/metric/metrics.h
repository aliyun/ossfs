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

#include <photon/thread/thread.h>

#include <map>
#include <memory>
#include <string_view>
#include <thread>

#include "common/utils.h"

namespace OssFileSystem {
namespace Metric {

#define DECLARE_METRIC_LATENCY(name, type)                                    \
  static thread_local OssFileSystem::Metric::MetricsNode name##_metric(#name, \
                                                                       type); \
  OssFileSystem::Metric::MetricsLatencyHelper helper##_metric(&name##_metric);

#define DECLARE_METRIC_VALUE(name, type, latency, size, cnt)                  \
  static thread_local OssFileSystem::Metric::MetricsNode name##_metric(#name, \
                                                                       type); \
  name##_metric.report(latency, size, cnt);

#define REPORT_ALL_METRIC_SUCCESSFUL(name, type, time_before, size)    \
  auto time_now = std::chrono::steady_clock::now();                    \
  auto period = std::chrono::duration_cast<std::chrono::microseconds>( \
                    time_now - time_before)                            \
                    .count();                                          \
  DECLARE_METRIC_VALUE(name, type, period, size, 1)

#define REPORT_ALL_METRIC_FAILED(name, type, time_before)              \
  auto time_now = std::chrono::steady_clock::now();                    \
  auto period = std::chrono::duration_cast<std::chrono::microseconds>( \
                    time_now - time_before)                            \
                    .count();                                          \
  DECLARE_METRIC_VALUE(name, type, period, 0, 1)

struct MetricsVal;
class MetricsCollector;

enum MetricsType {
  kIoMetrics = 0,
  kFsMetrics = 1,
  kOssMetrics = 2,
  kInternalMetrics = 3,
  kMetricsTypeCount,
};

static const std::string_view kMetricsTypeName[] = {"io", "fs", "oss",
                                                    "internal"};
static_assert(sizeof(kMetricsTypeName) / sizeof(std::string_view) ==
                  kMetricsTypeCount,
              "metrics type name size is wrong");

// MetricsNode is used to record metrics name and value in thread local.
class MetricsNode {
  std::string_view name_;
  MetricsType type_;
  MetricsVal *val_ptr_ = nullptr;
  std::shared_ptr<MetricsCollector> collector_;

  friend class MetricsCollector;

 public:
  MetricsNode(const MetricsNode &) = delete;
  MetricsNode &operator=(const MetricsNode &) = delete;
  MetricsNode(MetricsNode &&) = delete;
  MetricsNode &operator=(MetricsNode &&) = delete;

  MetricsNode(std::string_view name, MetricsType type);
  void report(uint64_t latency, uint64_t size = 0, uint64_t cnt = 1);
  ~MetricsNode();
};

class MetricsLatencyHelper {
  MetricsNode *node_ = nullptr;
  std::chrono::steady_clock::time_point start_time_;

 public:
  MetricsLatencyHelper(const MetricsLatencyHelper &) = delete;
  MetricsLatencyHelper &operator=(const MetricsLatencyHelper &) = delete;
  MetricsLatencyHelper(MetricsLatencyHelper &&) = delete;
  MetricsLatencyHelper &operator=(MetricsLatencyHelper &&) = delete;

  explicit MetricsLatencyHelper(MetricsNode *node);
  ~MetricsLatencyHelper();
};

std::string set_enabled_metrics(std::string_view filter);

std::string get_metrics_string(size_t interval_sec = 60);
std::map<std::string, uint64_t> get_metrics_map(size_t interval_sec = 60);

}  // namespace Metric
}  // namespace OssFileSystem
