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

#include <photon/common/estring.h>

#include <string>
#include <unordered_set>
#include <vector>

// fstab args: ossfs2 [bucket name] [mountpoint] -o [options]
inline bool is_fstab_args(int argc, char *argv[]) {
  // If the args count is 5 and the 4th arg is "-o", we're in fstab mode.
  return argc == 5 && strcmp(argv[3], "-o") == 0;
}

std::vector<std::string> parse_fstab(int argc, char *argv[]) {
  auto is_ignored = [](estring_view key) {
    // These hard-coded options listed aren't relevant to Ossfs.
    // "auto" - "nosuid" may come in from fstab, but we can't handle them.
    // "rw" is our default behavior, so we ignore it.
    static const std::unordered_set<estring> kIgnoredOptions = {
        "auto",   "noauto", "user",  "nouser", "users",  "_netdev",
        "nofail", "dev",    "nodev", "suid",   "nosuid", "rw",
    };
    if (kIgnoredOptions.count(key)) return true;
    // Options prefixed with `x-` are 'comments' in fstab, and we ignore them.
    return key.starts_with("x-");
  };

  // The first argument (program name) can be omitted.
  std::vector<std::string> args = {"mount", argv[2], "--oss_bucket", argv[1]};

  auto tokens = estring_view(argv[4]).split(',');
  for (const auto &token : tokens) {
    if (token.empty()) continue;

    auto eq_pos = token.find('=');
    if (eq_pos != std::string::npos) {
      auto key = token.substr(0, eq_pos);
      if (!key.empty() && !is_ignored(key)) {
        args.emplace_back("--" + std::string(token));
      }
    } else if (!is_ignored(token)) {
      args.emplace_back("--" + std::string(token));
    }
  }
  return args;
}