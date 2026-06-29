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

#include "fs/id_manager.h"

#include <atomic>

namespace OssFileSystem {

class HeapIdManager : public IIdManager {
 public:
  explicit HeapIdManager(uint64_t initial_id = 2)
      : next_id_(initial_id), start_id_(initial_id) {}

  uint64_t next_id() override {
    return next_id_.fetch_add(1);
  }
  uint64_t get_start_id() const override {
    return start_id_;
  }

 private:
  std::atomic<uint64_t> next_id_{0};
  const uint64_t start_id_;
};

constexpr uint64_t kDefaultInitialId = 2;
std::unique_ptr<IIdManager> create_heap_id_manager() {
  return std::make_unique<HeapIdManager>(kDefaultInitialId);
}

}  // namespace OssFileSystem
