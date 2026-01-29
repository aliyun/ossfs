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

#include "metrics.h"

#include <photon/common/estring.h>
#include <photon/photon.h>
#include <photon/thread/timer.h>

#include <algorithm>
#include <bitset>
#include <iomanip>
#include <mutex>
#include <numeric>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"
#include "common/utils.h"

namespace OssFileSystem {
namespace Metric {

const size_t kIntervalUsec = 500 * 1000;  // 500ms
const size_t kIntervalCountPerSec = 1'000'000 / kIntervalUsec;
const size_t kMaxHistorySec = 600;
const size_t kMaxHistorySize = kMaxHistorySec * kIntervalCountPerSec;

std::bitset<MetricsType::kMetricsTypeCount> g_enabled_metrics_mask(1);

std::string set_enabled_metrics(std::string_view filter) {
  if (filter.find("all") != std::string::npos) {
    g_enabled_metrics_mask.set();
    return "Enabled all metrics.";
  }
  g_enabled_metrics_mask = 1;
  std::string ret = "Enabled metrics: io metrics";
  auto parts = estring_view(filter).split(',');
  for (const auto &part : parts) {
    for (int i = 0; i < MetricsType::kMetricsTypeCount; i++) {
      if (kMetricsTypeName[i] == part) {
        g_enabled_metrics_mask.set(i);
        ret += ", " + std::string(kMetricsTypeName[i]) + " metrics";
        break;
      }
    }
  }
  return ret;
}

struct MetricsVal {
  uint64_t latency = 0;
  uint64_t size = 0;
  uint64_t cnt = 0;

  MetricsVal &operator+=(const MetricsVal &rhs) {
    latency += rhs.latency;
    size += rhs.size;
    cnt += rhs.cnt;
    return *this;
  }

  MetricsVal &operator-=(const MetricsVal &rhs) {
    latency -= rhs.latency;
    size -= rhs.size;
    cnt -= rhs.cnt;
    return *this;
  }
};

using MetricsValMap = std::unordered_map<std::string_view, MetricsVal>;
MetricsValMap operator+(const MetricsValMap &lhs, const MetricsValMap &rhs) {
  MetricsValMap ret = lhs;
  for (const auto &[k, v] : rhs) {
    auto iter = ret.find(k);
    if (iter != ret.end()) {
      iter->second += v;
    } else {
      ret.insert({k, v});
    }
  }
  return ret;
};

// MetricsCollector collects and aggregates metrics from nodes,
// records metrics history and exposes history if needed.
class MetricsCollector {
  std::unordered_map<std::string_view, std::vector<MetricsVal *>> metrics_map_;
  std::mutex mtx_;

  std::vector<MetricsValMap> metrics_history_;
  size_t metrics_cur_index_ = 0;  // the index for next aggregation

  // For metrics aggregation.
  MetricsValMap old_metrics_;
  bool is_stopping_ = false;
  std::thread *aggregate_th_ = nullptr;

  MetricsCollector();
  ~MetricsCollector();

  void aggregate();

 public:
  static std::shared_ptr<MetricsCollector> instance();

  MetricsCollector(const MetricsCollector &) = delete;
  MetricsCollector &operator=(const MetricsCollector &) = delete;
  MetricsCollector(MetricsCollector &&) = delete;
  MetricsCollector &operator=(MetricsCollector &&) = delete;

  void add(MetricsNode *p);
  void remove(MetricsNode *p);
  double get_history_and_elapsed_sec(size_t interval_sec, MetricsValMap &res);
};

MetricsCollector::MetricsCollector() {
  metrics_history_.reserve(kMaxHistorySize);
  aggregate_th_ = new std::thread([this]() { this->aggregate(); });
  LOG_INFO("Create metrics collector");
}

MetricsCollector::~MetricsCollector() {
  is_stopping_ = true;
  if (aggregate_th_) {
    aggregate_th_->join();
    delete aggregate_th_;
    aggregate_th_ = nullptr;
  }
  LOG_INFO("Destroy metrics collector");
}

void MetricsCollector::aggregate() {
  INIT_PHOTON();

  auto agg_func = [&]() -> uint64_t {
    std::lock_guard<std::mutex> lock(mtx_);
    MetricsValMap sum_metrics;
    // 1. Add metrics from all threads.
    for (auto &[k, v] : metrics_map_) {
      MetricsVal val;
      for (auto &p : v) val += *p;
      sum_metrics[k] = val;
    }
    // 2. Calculate diff metrics.
    MetricsValMap diff_metrics;
    for (const auto &[k, v] : sum_metrics) {
      MetricsVal val = v;
      auto iter = old_metrics_.find(k);
      if (iter != old_metrics_.end() && val.cnt >= iter->second.cnt) {
        val -= iter->second;
      }
      if (val.cnt > 0) diff_metrics[k] = val;
    }
    // 3. Record metrics to history and update old metrics.
    if (metrics_history_.size() < kMaxHistorySize) {
      metrics_history_.emplace_back(std::move(diff_metrics));
    } else {
      metrics_history_[metrics_cur_index_].swap(diff_metrics);
    }
    metrics_cur_index_ = (metrics_cur_index_ + 1) % kMaxHistorySize;
    old_metrics_.swap(sum_metrics);

    // This part takes an average of 20 to 50 us,
    // so it's unnecessary to consider it in the 500 ms timer.
    return kIntervalUsec;
  };

  photon::Timer timer(kIntervalUsec, agg_func, true);
  while (!is_stopping_) photon::thread_usleep(100000);
}

std::shared_ptr<MetricsCollector> MetricsCollector::instance() {
  static std::shared_ptr<MetricsCollector> instance(
      new MetricsCollector(), [](MetricsCollector *p) { delete p; });
  return instance;
}

void MetricsCollector::add(MetricsNode *p) {
  std::lock_guard<std::mutex> lock(mtx_);
  metrics_map_[p->name_].emplace_back(p->val_ptr_);
}

void MetricsCollector::remove(MetricsNode *p) {
  std::lock_guard<std::mutex> lock(mtx_);
  auto &vec = metrics_map_[p->name_];
  vec.erase(std::remove(vec.begin(), vec.end(), p->val_ptr_), vec.end());
}

double MetricsCollector::get_history_and_elapsed_sec(size_t interval_sec,
                                                     MetricsValMap &history) {
  history = MetricsValMap();
  std::lock_guard<std::mutex> lock(mtx_);

  size_t interval_cnt = interval_sec * kIntervalCountPerSec;
  size_t cur_size = metrics_history_.size();
  if (interval_cnt > cur_size) interval_cnt = cur_size;

  size_t idx = metrics_cur_index_;
  auto begin = metrics_history_.begin();
  auto end = metrics_history_.end();
  if (idx >= interval_cnt) {
    history = std::accumulate(begin + idx - interval_cnt, begin + idx, history);
  } else {
    RELEASE_ASSERT(cur_size == kMaxHistorySize);
    // The metrics history is wrapped around, and we need to concat the history
    // `[history_size - interval_cnt + idx, history_size)` and `[0, idx)`.
    history = std::accumulate(end - interval_cnt + idx, end, history);
    history = std::accumulate(begin, begin + idx, history);
  }
  return 1.0 * interval_cnt / kIntervalCountPerSec;
}

MetricsNode::MetricsNode(std::string_view name, MetricsType type)
    : name_(name), type_(type) {
  val_ptr_ = new MetricsVal();

  collector_ = MetricsCollector::instance();
  collector_->add(this);
}

void MetricsNode::report(uint64_t latency, uint64_t size, uint64_t cnt) {
  if (g_enabled_metrics_mask.test(type_))
    (*val_ptr_) += MetricsVal{latency, size, cnt};
}

MetricsNode::~MetricsNode() {
  collector_->remove(this);
  if (val_ptr_) {
    delete val_ptr_;
    val_ptr_ = nullptr;
  }
}

MetricsLatencyHelper::MetricsLatencyHelper(MetricsNode *node)
    : node_(node), start_time_(std::chrono::steady_clock::now()) {}

MetricsLatencyHelper::~MetricsLatencyHelper() {
  auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - start_time_)
                     .count();
  node_->report(latency);
}

struct MetricsResult {
  uint64_t avglat = 0;
  uint64_t cnt = 0;
  uint64_t qps = 0;
  uint64_t size = 0;
  uint64_t bandwidth = 0;
};

using MetricsResultMap = std::map<std::string_view, MetricsResult>;

static MetricsResultMap get_metrics(size_t interval_sec) {
  auto collector = MetricsCollector::instance();
  if (collector == nullptr) return MetricsResultMap();
  MetricsValMap sum_metrics;
  double elapsed_sec =
      collector->get_history_and_elapsed_sec(interval_sec, sum_metrics);

  MetricsResultMap metrics;
  for (const auto &[k, v] : sum_metrics) {
    if (v.cnt == 0) continue;
    MetricsResult res;
    res.avglat = v.latency / v.cnt;
    res.cnt = v.cnt;
    res.qps = v.cnt / elapsed_sec;
    res.size = v.size;
    res.bandwidth = v.size / elapsed_sec;
    metrics[k] = res;
  }
  return metrics;
}

std::string get_metrics_string(size_t interval_sec) {
  auto metrics = get_metrics(interval_sec);
  if (metrics.empty()) return "";
  std::stringstream frontend_ss, backend_ss;
  auto print_kv = [](std::stringstream &ss, std::string_view s, uint64_t v) {
    std::string unit = "";
    if (s == "bw: ") {
      static const std::vector<std::string> kUnits = {"B/s", "KB/s", "MB/s",
                                                      "GB/s"};
      size_t unit_idx = 0;
      while (v >= 10000 && unit_idx < kUnits.size() - 1) {
        v /= 1000;
        ++unit_idx;
      }
      unit = kUnits[unit_idx];
    } else if (s == "lat: ") {
      unit = "us";
    }
    ss << s << std::setw(15) << std::left << (std::to_string(v) + " " + unit);
  };
  for (const auto &[k, v] : metrics) {
    auto &ss = k.substr(0, 3) == "oss" ? backend_ss : frontend_ss;
    ss << std::setw(35) << std::left << k;
    print_kv(ss, "lat: ", v.avglat);
    print_kv(ss, "qps: ", v.qps);
    if (v.bandwidth) print_kv(ss, "bw: ", v.bandwidth);
    ss << "\n";
  }
  return "Metrics: \n" + frontend_ss.str() +
         (backend_ss.str().empty() ? "" : "\n" + backend_ss.str());
}

std::map<std::string, uint64_t> get_metrics_map(size_t interval_sec) {
  auto metrics = get_metrics(interval_sec);
  std::map<std::string, uint64_t> res;
  for (const auto &[k, v] : metrics) {
    std::string name(k);
    res.emplace(name + "_latency", v.avglat);
    res.emplace(name + "_cnt", v.cnt);
    res.emplace(name + "_qps", v.qps);
    res.emplace(name + "_bandwidth", v.bandwidth);
    res.emplace(name + "_bytes", v.size);
  }
  return res;
}

}  // namespace Metric
}  // namespace OssFileSystem
