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

#include <memory>

namespace OssFileSystem {

class IIdManager {
 public:
  virtual ~IIdManager() = default;
  virtual uint64_t next_id() = 0;
  virtual uint64_t get_start_id() const = 0;
};

std::unique_ptr<IIdManager> create_heap_id_manager();

}  // namespace OssFileSystem
