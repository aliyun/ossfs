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

#include "common/filesystem.h"

namespace OssFileSystem {

class FileInode;
class OssFs;

class IWriter {
 public:
  virtual ~IWriter() = default;

  // With inode wlock held outside.
  virtual int open() = 0;
  virtual ssize_t pwrite(size_t count, off_t offset, const void *buf,
                         struct fuse_bufvec *bufv, std::string *wpath) = 0;
  virtual int flush() = 0;

  // With inode rlock held outside.
  virtual ssize_t pread_from_upload_buffer(void *buf, size_t count,
                                           off_t offset) = 0;
  virtual size_t calc_remote_size() = 0;

  // With either inode rlock or wlock held outside.
  virtual bool get_is_dirty() = 0;
  virtual bool get_is_immutable() = 0;
};

std::unique_ptr<IWriter> create_oss_writer(OssFs *fs, std::string_view path,
                                           FileInode *inode, int flags,
                                           bool is_dirty = false);

}  // namespace OssFileSystem