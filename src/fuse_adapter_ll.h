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

#include "common/filesystem.h"
#include "common/fuse.h"

// Readdirplus mode for FUSE_CAP_READDIRPLUS negotiation.
enum class ReaddirplusMode {
  kOff,   // Disabled: kernel uses plain READDIR.
  kOn,    // Always use READDIRPLUS (includes entry attributes).
  kAuto,  // Adaptive: kernel chooses READDIR or READDIRPLUS based on usage.
};

struct FuseLLOptions {
  uint64_t attr_timeout = 60;
  uint64_t entry_timeout = 60;
  uint64_t negative_timeout = 0;
  ReaddirplusMode readdirplus = ReaddirplusMode::kOn;
  bool ignore_fsync = true;
  bool ignore_flush = false;
  bool replace_unresolved_uid_gid = false;
  bool enable_flock = false;
};

void set_fuse_ll_fs(IFileSystemFuseLL *fs);

void set_fuse_ll_options(const FuseLLOptions &opts);

struct fuse_lowlevel_ops *get_fuse_ll_oper();

// Hooks for xattr and lock operations are null by default, so the kernel
// receives ENOSYS and stops sending these requests. Enable them only for
// backends that support xattr/locks (HDFS).
void enable_fuse_ll_xattr_and_lock();

int fuse_session_loop_mt_with_photon(struct fuse_session *se, int threads,
                                     void *pre_init(void *) = nullptr,
                                     void *cleanup(void *) = nullptr,
                                     void *user_data = nullptr);
