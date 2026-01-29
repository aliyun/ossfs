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

#include <functional>
#include <mutex>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "common/logger.h"

class FaultInjector;
extern std::unique_ptr<FaultInjector> g_fault_injector;

typedef enum {
  FI_Invalid,
  FI_Modify_Write_Buffer,
  FI_Lookup_Delay_After_Getting_Remote_attr,
  FI_OssError_Call_Failed,
  FI_OssError_Read_Partial,
  FI_OssError_5xx,
  FI_OssError_Qps_Limited,
  FI_Download_Failed_During_Merge_Remote_Data,
  FI_OssError_Call_Timeout,
  FI_Forget_Delay,
  FI_Readdir_list_Delay,
  FI_Data_Sync_Failed,
  FI_OssError_Failed_Without_Call,
  FI_First_Prefetch_Delay,
  FI_Readdir_Delay_After_Construct_Inodes,
  FI_OssError_No_Crc64,
  FI_Readdir_Delay_Noplus,
  FI_Oss_Partial_Deletion,
  FI_Reverse_Invalidate_Inode_Forcefully,
  FI_Force_Flush_Dirty_Handle_Delay,
  FI_Lookup_Delay_Before_Getting_OSS_Response,
  FI_Lookup_Oss_Failure,
  FI_Count,
} FaultInjectionId;

#define ENABLE_TESTS() (unlikely(g_fault_injector != nullptr))

#define IS_FAULT_INJECTION_ENABLED(id)     \
  (unlikely(g_fault_injector != nullptr && \
            g_fault_injector->is_injection_enabled(id)))

#define FAULT_INJECTION(id, executor)        \
  if (unlikely(g_fault_injector != nullptr)) \
    g_fault_injector->fault_injection(id, executor);

static const std::string_view FaultInjectionName[] = {
    "FI_Invalid",
    "FI_Modify_Write_Buffer",
    "FI_Lookup_Delay_After_Getting_Remote_attr",
    "FI_OssError_Call_Failed",
    "FI_OssError_Read_Partial",
    "FI_OssError_5xx",
    "FI_OssError_Qps_Limited",
    "FI_Download_Failed_During_Merge_Remote_Data",
    "FI_OssError_Call_Timeout",
    "FI_Forget_Delay",
    "FI_Readdir_list_Delay",
    "FI_Data_Sync_Failed",
    "FI_OssError_Failed_Without_Call",
    "FI_First_Prefetch_Delay",
    "FI_Readdir_Delay_After_Construct_Inodes",
    "FI_OssError_No_Crc64",
    "FI_Readdir_Delay_Noplus",
    "FI_Oss_Partial_Deletion",
    "FI_Reverse_Invalidate_Inode_Forcefully",
    "FI_Force_Flush_Dirty_Handle_Delay",
    "FI_Lookup_Delay_Before_Getting_OSS_Response",
    "FI_Lookup_Oss_Failure",
};

static_assert(sizeof(FaultInjectionName) / sizeof(std::string_view) == FI_Count,
              "fault injection name size is wrong");

struct FaultInjection {
  FaultInjection(uint32_t rcount = std::numeric_limits<uint32_t>::max(),
                 uint32_t scount = 0)
      : run_count(rcount), skip_count(scount) {}

  uint32_t run_count;
  uint32_t skip_count;
  bool inject_hit = false;

  std::string to_string() const {
    return "run count " + std::to_string(run_count) + " skip count " +
           std::to_string(skip_count);
  }
};

class FaultInjector {
 public:
  bool is_injection_enabled(FaultInjectionId id) {
    std::lock_guard<std::mutex> l(lock_);
    return injection_map_.count(id) && injection_map_[id].run_count != 0;
  }

  void set_injection(FaultInjectionId id,
                     const FaultInjection &injection = FaultInjection{}) {
    LOG_INFO("inject fault ` with params `", FaultInjectionName[id],
             injection.to_string());
    std::lock_guard<std::mutex> l(lock_);
    injection_map_[id] = injection;
  }

  void clear_injection(FaultInjectionId id) {
    LOG_INFO("clear fault `", FaultInjectionName[id]);
    std::lock_guard<std::mutex> l(lock_);
    injection_map_.erase(id);
  }

  void clear_all_injections() {
    LOG_INFO("clear all faults");
    std::lock_guard<std::mutex> l(lock_);
    injection_map_.clear();
  }

  void fault_injection(FaultInjectionId id, std::function<void()> executor) {
    if (!is_injection_enabled(id)) return;
    LOG_INFO("inject fault hit ` with params `", FaultInjectionName[id],
             injection_map_[id].to_string());
    {
      std::lock_guard<std::mutex> l(lock_);
      auto iter = injection_map_.find(id);
      if (iter == injection_map_.end() || injection_map_[id].run_count == 0)
        return;

      if (injection_map_[id].skip_count > 0) {
        injection_map_[id].skip_count--;
        return;
      }

      if (0 == --injection_map_[id].run_count) injection_map_.erase(id);
    }

    executor();
  }

 private:
  std::mutex lock_;
  std::unordered_map<int, FaultInjection> injection_map_;
};
