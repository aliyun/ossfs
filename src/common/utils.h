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

#include <photon/common/string_view.h>
#include <stdint.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

std::string join_paths(const std::string &first, const std::string &second);
std::string remove_prepend_backslash(const std::string &path);
std::string add_backslash(const std::string &path);
int daemonize(int foreground, int pipefd, void (*log_exit_error)(char));

// Check if child is a subdirectory of parent.
// The inputs should be normalized paths.
bool is_subdir(const std::string &parent, const std::string &child);

std::string_view trim_string_view(
    std::string_view s,
    std::function<bool(char)> pred = [](char c) { return std::isspace(c); });

int64_t get_physical_memory_KiB();

[[noreturn]] __attribute__((noinline, cold)) void release_assert_fail(
    const char *file, int line, const char *expr);

std::optional<uint64_t> parse_bytes_string(std::string_view s);

int split_command_tokens(const std::string &command,
                         std::vector<std::string> &tokens);

int run_process_safe(const std::vector<std::string> &args,
                     std::string &stdout_output, std::string &stderr_output,
                     size_t max_output_size, int timeout_sec = -1);

time_t parse_iso8601_time(std::string_view s);

bool is_valid_fd(int fd);

std::vector<std::string_view> split_string(std::string_view str,
                                           std::string_view delimiter);
