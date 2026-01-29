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

#include <string>

namespace OssFileSystem {
namespace Admin {

int stats_show_command_handler(bool stats_continue, pid_t pid, size_t interval);

int stats_set_command_handler(pid_t pid, const std::string &filter);

}  // namespace Admin
}  // namespace OssFileSystem
