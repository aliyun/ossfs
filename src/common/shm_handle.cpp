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

#include "shm_handle.h"

#include <sys/stat.h>

#include <cstddef>

namespace common {

int ShmHandle::open(const std::string &name, int flags,
                    std::unique_ptr<ShmHandle> &shm_handle) {
  flags &= ~O_CREAT;
  return ShmHandle::open_internal(name, 0, flags, shm_handle);
}

int ShmHandle::create(const std::string &name, size_t trunc_size, int flags,
                      std::unique_ptr<ShmHandle> &shm_handle) {
  flags |= O_CREAT;
  return ShmHandle::open_internal(name, trunc_size, flags, shm_handle);
}

int ShmHandle::open_internal(const std::string &shm_name, size_t trunc_size,
                             int flags,
                             std::unique_ptr<ShmHandle> &shm_handle) {
  size_t shm_size = trunc_size;
  LOG_DEBUG("open shmem `", shm_name);
  int fd = shm_open(shm_name.c_str(), flags, kDefaultShmMode);
  if (fd == -1) {
    RETURN_IF_TRUE(errno == ENOENT, -ENOENT,
                   LOG_WARN("open shmem failed, shmem ` not found", shm_name));
    LOG_ERROR_RETURN(0, -1, "open shmem failed, name: `, err: `", shm_name,
                     strerror(errno));
  }
  DEFER(::close(fd));

  if (flags & O_CREAT) {
    if (ftruncate(fd, trunc_size) == -1) {
      LOG_ERROR("ftruncate shmem failed, `", strerror(errno));
      return -1;
    }
  } else {
    struct stat st;
    if (fstat(fd, &st) == -1) {
      LOG_ERROR("stat failed, shm_name: `, err: `", shm_name, strerror(errno));
      return -1;
    }
    shm_size = st.st_size;
  }

  void *mmap_ptr =
      mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (mmap_ptr == MAP_FAILED) {
    LOG_ERROR("mmap failed, `", strerror(errno));
    return -1;
  }

  shm_handle.reset(new ShmHandle(shm_name, shm_size, mmap_ptr));
  return 0;
}

int ShmHandle::unlink(const std::string &shm_name) {
  LOG_DEBUG("unlink shmem `", shm_name);
  if (shm_unlink(shm_name.c_str()) == 0 || errno == ENOENT) return 0;

  LOG_ERROR("shm_unlink failed, shm_name: `, err: `", shm_name,
            strerror(errno));
  return -1;
}

// valid name: [a-zA-Z0-9._-]
std::string ShmHandle::escape_shm_name(const std::string &name) {
  std::string result;
  result.reserve(name.size() + 1);
  result += '/';

  for (char c : name) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ||
        c == '.') {
      result += c;
    } else {
      result += '.';
    }
  }
  return result;
}

}  // namespace common