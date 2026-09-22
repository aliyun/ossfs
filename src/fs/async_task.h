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

#include <photon/common/string-keyed.h>
#include <photon/thread/thread.h>
#include <photon/thread/workerpool.h>

#include <memory>
#include <string_view>

namespace OssFileSystem {

class OssFs;

class AsyncTask {
 public:
  virtual ~AsyncTask() = default;
  virtual void process() = 0;
  virtual std::string task_id() = 0;

  void set_finished(bool is_finished) {
    finished_.store(is_finished);
  }

  bool is_finished() {
    return finished_.load();
  }

 private:
  std::atomic<bool> finished_ = ATOMIC_VAR_INIT(false);
};

class WarmupTask final : public AsyncTask {
 public:
  WarmupTask(std::string_view warmup_path, OssFs *fs)
      : warmup_path_(warmup_path), fs_(fs) {}

  void process() override;
  std::string task_id() override {
    return "warmup_" + warmup_path_;
  }

 private:
  std::string warmup_path_;
  OssFs *fs_ = nullptr;
};

class AsyncTaskManager {
 public:
  explicit AsyncTaskManager(uint32_t task_limit);
  ~AsyncTaskManager() = default;

  std::string add_task(std::shared_ptr<AsyncTask> task);
  std::string dump_task_status();

 private:
  uint32_t task_limit_ = 0;
  unordered_map_string_key<std::shared_ptr<AsyncTask>> tasks_;
  std::unique_ptr<photon::WorkPool> task_pool_;
  photon::mutex task_lock_;
};

}  // namespace OssFileSystem
