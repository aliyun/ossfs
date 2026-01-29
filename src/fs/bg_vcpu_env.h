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

#include <photon/common/executor/executor.h>

#include "oss/oss_adapter.h"

namespace OssFileSystem {

//
// A structure that binds each OSS client to a specific vCPU.
//
// This structure manages a collection of OSS clients where each client is
// associated with a particular vCPU.
//
struct VCpuOssClientEnv {
  std::vector<OssAdapter *> oss_clients;
  std::unordered_map<photon::vcpu_base *, int> vcpu_map;

  OssAdapter *get_oss_client() {
    return oss_clients[vcpu_map[photon::get_vcpu()]];
  }
};

//
// A background vCPU environment for OSS clients management.
//
// This structure extends VCpuOssClientEnv to provide a background execution
// environment where each OSS client is bound to a specific vCPU. It uses
// photon::Executor to manage vCPU-specific execution contexts and provides
// round-robin access to these contexts.
//
struct BGVCpuOssClientEnv : public VCpuOssClientEnv {
  BGVCpuOssClientEnv() : vcpu_id(0) {}

  ~BGVCpuOssClientEnv() {
    for (int i = 0; i < vcpu_num; i++) {
      auto e = executors[i];
      if (!e) continue;

      if (destory_executor) {
        e->perform([&]() { delete oss_clients[i]; });
        delete e;
      }
    }
  }

  struct EnvContext {
    photon::Executor *executor;
    OssAdapter *oss_client;
    photon::vcpu_base *vcpu;
  };

  bool destory_executor = true;
  std::vector<photon::Executor *> executors;
  std::vector<photon::vcpu_base *> vcpu_list;
  std::atomic<uint64_t> vcpu_id;
  int vcpu_num = 0;

  EnvContext get_oss_client_env_next() {
    auto next_id = vcpu_id.fetch_add(1) % vcpu_num;
    return get_oss_client_env(next_id);
  }

  EnvContext get_oss_client_env(int vcpu_id) {
    EnvContext env;
    env.executor = executors[vcpu_id];
    env.oss_client = oss_clients[vcpu_id];
    env.vcpu = vcpu_list[vcpu_id];
    return env;
  }

  void add_oss_client_env(photon::Executor *executor, OssAdapter *oss_client) {
    executors.push_back(executor);
    oss_clients.push_back(oss_client);
    executor->perform([&]() {
      auto vcpu = photon::get_vcpu();
      vcpu_map[vcpu] = executors.size() - 1;
      vcpu_list.push_back(vcpu);
    });

    vcpu_num = executors.size();
  }

  photon::vcpu_base *get_vcpu_next() {
    auto next_id = vcpu_id.fetch_add(1) % vcpu_num;
    return vcpu_list[next_id];
  }

  std::vector<OssAdapter *> get_all_oss_clients() {
    return oss_clients;
  }

  std::vector<EnvContext> get_all_env_cxts() {
    std::vector<EnvContext> ctxs;
    for (int i = 0; i < vcpu_num; i++) {
      ctxs.emplace_back(get_oss_client_env(i));
    }
    return ctxs;
  }
};

struct BackgroundVCpuEnv {
  BGVCpuOssClientEnv *bg_oss_client_env = nullptr;
};

};  // namespace OssFileSystem
