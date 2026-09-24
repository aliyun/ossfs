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

#include "async_task.h"

#include <photon/common/string-keyed.h>
#include <photon/photon.h>
#include <photon/thread/thread-pool.h>
#include <photon/thread/thread.h>

#include <sstream>

#include "common/utils.h"
#include "fs.h"

namespace OssFileSystem {

void WarmupTask::process() {
  fs_->warmup_internal(warmup_path_);
}

AsyncTaskManager::AsyncTaskManager(uint32_t task_limit)
    : task_limit_(task_limit) {
  size_t vcpu_num = task_limit_ < 4 ? task_limit_ : 4;
  ScopedBlockAllSignal block_signals;
  task_pool_ = std::make_unique<photon::WorkPool>(
      vcpu_num, OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE, task_limit_);
}

std::string AsyncTaskManager::add_task(std::shared_ptr<AsyncTask> task) {
  SCOPED_LOCK(task_lock_);

  for (auto it = tasks_.begin(); it != tasks_.end();) {
    if (it->second->is_finished()) {
      it = tasks_.erase(it);
    } else {
      ++it;
    }
  }

  if (tasks_.size() >= task_limit_) {
    return "Task number limit exceeded.";
  }

  std::string task_id = task->task_id();
  if (tasks_.find(task_id) != tasks_.end()) {
    return "Task already exists: " + task_id;
  }

  tasks_[task_id] = task;
  task_pool_->async_call(new std::function<void()>([task]() {
    task->set_finished(false);
    DEFER(task->set_finished(true));
    task->process();
  }));

  // clang-format off
  return "Task generated. Dump the status by \"ossfs2 run_task -p <pid> dump\"";
  // clang-format on
}

std::string AsyncTaskManager::dump_task_status() {
  SCOPED_LOCK(task_lock_);

  std::ostringstream oss;
  oss << "Running Tasks List:";

  int index = 0;
  for (const auto &pair : tasks_) {
    if (pair.second->is_finished()) {
      continue;
    }
    oss << "\n  " << (++index) << ": " << pair.first;
  }

  if (index == 0) {
    return "No tasks are running.";
  }

  return oss.str();
}

}  // namespace OssFileSystem
