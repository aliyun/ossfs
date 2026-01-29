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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <string_view>

#include "common/logger.h"
#include "common/macros.h"

namespace common {

class ShmHandle {
 public:
  // RAII interface.
  static int open(const std::string &name, size_t size, int flags,
                  std::unique_ptr<ShmHandle> &ptr);

  static int unlink(const std::string &name);

  static std::string escape_shm_name(const std::string &name);

  ShmHandle(const ShmHandle &) = delete;
  ShmHandle(ShmHandle &&) = delete;
  ShmHandle &operator=(const ShmHandle &) = delete;
  ShmHandle &operator=(ShmHandle &&) = delete;

  ~ShmHandle() {
    this->close();
  }

  void close() {
    if (ptr_ == nullptr) return;

    if (munmap(ptr_, size_) == -1) {
      LOG_ERROR("munmap failed, `", strerror(errno));
    }
    ptr_ = nullptr;
  }

  void *ptr() const {
    return ptr_;
  }

  size_t size() const {
    return size_;
  }

 private:
  static constexpr mode_t kDefaultShmMode = 0644;
  ShmHandle(const std::string &name, size_t size, void *ptr)
      : name_(name), size_(size), ptr_(ptr) {}

  const std::string name_;
  const size_t size_;
  void *ptr_ = nullptr;
};

}  // namespace common