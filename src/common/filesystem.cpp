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

#include "common/filesystem.h"

#include <grp.h>
#include <pwd.h>

#include <cstring>
#include <vector>

#include "common/fault_injector.h"

UserGroupMapping g_test_user_mapping;

std::string uid_to_username(uid_t uid) {
  std::string mapped;
  FAULT_INJECTION(FI_Hdfs_UserGroup_Mapping, [&] {
    auto it = g_test_user_mapping.uid_to_name.find(uid);
    if (it != g_test_user_mapping.uid_to_name.end()) mapped = it->second;
  });
  if (!mapped.empty()) return mapped;

  struct passwd pwd;
  struct passwd *result = nullptr;
  char buf[4096];
  if (getpwuid_r(uid, &pwd, buf, sizeof(buf), &result) == 0 && result) {
    return std::string(pwd.pw_name);
  }
  return "";
}

std::string gid_to_groupname(gid_t gid) {
  std::string mapped;
  FAULT_INJECTION(FI_Hdfs_UserGroup_Mapping, [&] {
    auto it = g_test_user_mapping.gid_to_name.find(gid);
    if (it != g_test_user_mapping.gid_to_name.end()) mapped = it->second;
  });
  if (!mapped.empty()) return mapped;

  struct group grp;
  struct group *result = nullptr;
  char buf[4096];
  if (getgrgid_r(gid, &grp, buf, sizeof(buf), &result) == 0 && result) {
    return std::string(grp.gr_name);
  }
  return "";
}

// Check if caller's username is listed in file_gid's group member list.
static bool is_uid_include_group(uid_t caller_uid, gid_t file_gid) {
  // 1. Resolve caller uid to username (reuses uid_to_username with FI mock).
  std::string username = uid_to_username(caller_uid);
  if (username.empty()) return false;

  // 2. Resolve file gid to group struct (with ERANGE retry).
  long init_size = sysconf(_SC_GETGR_R_SIZE_MAX);
  if (init_size < 0) init_size = 1024;
  std::vector<char> gr_buf(init_size);
  struct group grp;
  struct group *gr_result = nullptr;
  while (getgrgid_r(file_gid, &grp, gr_buf.data(), gr_buf.size(), &gr_result) ==
         ERANGE) {
    gr_buf.resize(gr_buf.size() * 2);
  }
  if (gr_result == nullptr) return false;

  // 3. Check gr_mem for username.
  for (char **mem = gr_result->gr_mem; mem && *mem; mem++) {
    if (username == *mem) return true;
  }
  return false;
}

int check_hdfs_access(const struct stat *stbuf, int mask, uid_t current_uid,
                      gid_t current_gid) {
  if (mask == F_OK) return 0;

  // Root exemption (HDFS has no real execute bit concept).
  if (current_uid == 0) return 0;

  mode_t file_mode = stbuf->st_mode & kPermMask;
  mode_t base_mask = S_IRWXO;

  // Primary gid comparison (fast path).
  bool in_group = (current_gid == stbuf->st_gid);

  // FI: mock supplementary group check for tests.
  FAULT_INJECTION(FI_Hdfs_UserGroup_Mapping, [&] {
    if (!in_group) {
      auto name_it = g_test_user_mapping.uid_to_name.find(current_uid);
      auto mem_it = g_test_user_mapping.group_members.find(stbuf->st_gid);
      if (name_it != g_test_user_mapping.uid_to_name.end() &&
          mem_it != g_test_user_mapping.group_members.end()) {
        for (const auto &member : mem_it->second) {
          if (member == name_it->second) {
            in_group = true;
            break;
          }
        }
      }
    }
  });

  // Production: getgrgid_r + gr_mem.
  if (!in_group) {
    in_group = is_uid_include_group(current_uid, stbuf->st_gid);
  }

  if (in_group) base_mask |= S_IRWXG;

  mode_t mode = file_mode & base_mask;

  if ((mask & W_OK) && !(mode & (S_IWUSR | S_IWGRP | S_IWOTH))) {
    return -EACCES;
  }
  if ((mask & R_OK) && !(mode & (S_IRUSR | S_IRGRP | S_IROTH))) {
    return -EACCES;
  }
  if ((mask & X_OK) && !(mode & (S_IXUSR | S_IXGRP | S_IXOTH))) {
    return -EACCES;
  }
  return 0;
}
