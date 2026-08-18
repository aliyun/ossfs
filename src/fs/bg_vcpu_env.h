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
#include <photon/io/aio-wrapper.h>

#include "fs/disk_cache_env.h"
#include "oss/obj_store.h"

namespace OssFileSystem {

//
// A structure that binds each object store backend to a specific vCPU.
//
// This structure manages a collection of object store backends where each
// backend is associated with a particular vCPU.
//
struct VCpuObjStoreEnv {
  std::vector<IObjStore *> obj_stores;
  std::unordered_map<photon::vcpu_base *, int> vcpu_map;

  IObjStore *get_obj_store() {
    return obj_stores[vcpu_map[photon::get_vcpu()]];
  }
};

//
// A background vCPU environment for object store backends management.
//
// This structure extends VCpuObjStoreEnv to provide a background execution
// environment where each object store backend is bound to a specific vCPU.
// It uses photon::Executor to manage vCPU-specific execution contexts and
// provides round-robin access to these contexts.
//
struct BGVCpuObjStoreEnv : public VCpuObjStoreEnv {
  BGVCpuObjStoreEnv() : vcpu_id(0) {}

  ~BGVCpuObjStoreEnv() {
    for (int i = 0; i < vcpu_num; i++) {
      auto e = executors[i];
      if (!e) continue;

      if (destroy_executor) {
        e->perform([&]() { delete obj_stores[i]; });
        delete e;
      }
    }
  }

  struct EnvContext {
    photon::Executor *executor;
    IObjStore *obj_store;
    photon::vcpu_base *vcpu;
  };

  bool destroy_executor = true;
  std::vector<photon::Executor *> executors;
  std::vector<photon::vcpu_base *> vcpu_list;
  std::atomic<uint64_t> vcpu_id;
  int vcpu_num = 0;

  EnvContext get_obj_store_env_next() {
    auto next_id = vcpu_id.fetch_add(1) % vcpu_num;
    return get_obj_store_env(next_id);
  }

  EnvContext get_obj_store_env(int vcpu_id) {
    EnvContext env;
    env.executor = executors[vcpu_id];
    env.obj_store = obj_stores[vcpu_id];
    env.vcpu = vcpu_list[vcpu_id];
    return env;
  }

  void add_obj_store_env(photon::Executor *executor, IObjStore *obj_store) {
    executors.push_back(executor);
    obj_stores.push_back(obj_store);
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

  std::vector<IObjStore *> get_all_obj_stores() {
    return obj_stores;
  }

  std::vector<EnvContext> get_all_env_cxts() {
    std::vector<EnvContext> ctxs;
    for (int i = 0; i < vcpu_num; i++) {
      ctxs.emplace_back(get_obj_store_env(i));
    }
    return ctxs;
  }
};

//
// A background vCPU environment for disk cache operations. Local store
// requests are dispatched round-robin to the registered executors (libaio
// only; psync runs them inline on the caller vCPU). The primary executor
// (index 0) owns the disk cache env and its recycle timer.
//
struct BGVCpuDiskCacheEnv {
  int init(const OssFileSystem::DiskCacheOptions &opts) {
    if (executors_.empty()) {
      return -1;
    }
    io_engine_type_ = opts.io_engine_type;
    if (io_engine_type_ == photon::fs::ioengine_libaio) {
      // libaio wrapper must be initialized on every executor vCPU.
      for (auto e : executors_) {
        e->perform([]() { photon::libaio_wrapper_init(); });
      }
    }
    // The env is created on the primary executor which owns its lifecycle.
    return executors_[0]->perform([&]() {
      disk_cache_env = new OssFileSystem::DiskCacheEnv(opts);
      int r = disk_cache_env->init();
      if (r != 0) {
        delete disk_cache_env;
        disk_cache_env = nullptr;
      }
      return r;
    });
  }

  ~BGVCpuDiskCacheEnv() {
    if (!executors_.empty()) {
      // Env dies on the primary executor; libaio fini pairs with init per
      // executor vCPU.
      executors_[0]->perform([&]() { delete disk_cache_env; });
      if (io_engine_type_ == photon::fs::ioengine_libaio) {
        for (auto e : executors_) {
          e->perform([]() { photon::libaio_wrapper_fini(); });
        }
      }
      for (auto e : executors_) delete e;
    }
  }

  int io_engine_type_ = photon::fs::ioengine_libaio;
  OssFileSystem::DiskCacheEnv *disk_cache_env = nullptr;
  std::vector<photon::Executor *> executors_;
  std::atomic<uint64_t> executor_id_{0};

  void add_executor(photon::Executor *executor) {
    executors_.push_back(executor);
  }

  photon::Executor *get_executor_next() {
    auto next_id = executor_id_.fetch_add(1) % executors_.size();
    return executors_[next_id];
  }

  photon::fs::ICachePool *get_disk_cache_pool() const {
    return disk_cache_env->cache_fs->get_pool();
  }

  photon::fs::IFileSystemXAttr *get_disk_cache_xattr_fs() const {
    return disk_cache_env->local_xattr_fs;
  }
};

struct BackgroundVCpuEnv {
  BGVCpuObjStoreEnv *bg_obj_store_env = nullptr;
  BGVCpuDiskCacheEnv *bg_disk_cache_env = nullptr;
};

};  // namespace OssFileSystem
