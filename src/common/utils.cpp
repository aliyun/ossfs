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

#include "common/utils.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/dir.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wordexp.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <sstream>

#include "common/logger.h"

std::string join_paths(const std::string &first, const std::string &second) {
  if (first.empty()) {
    return second;
  }
  if (second.empty()) {
    return first;
  }
  if ((first[first.size() - 1] == '/') || (second[0] == '/')) {
    return first + second;
  } else {
    return first + "/" + second;
  }
}

std::string remove_prepend_backslash(const std::string &path) {
  if (path.empty() || path[0] != '/') {
    return path;
  } else if (path == "/") {
    return "";
  } else {
    return path.substr(1, path.size() - 1);
  }
}

std::string add_backslash(const std::string &path) {
  if (path.empty()) {
    return path;
  } else if (path[path.size() - 1] == '/') {
    return path;
  } else {
    return path + "/";
  }
}

bool is_subdir(const std::string &parent, const std::string &child) {
  if (parent.empty() || child.empty()) return false;

  std::string parent_path = add_backslash(parent);
  std::string child_path = add_backslash(child);

  if (parent_path.size() >= child_path.size()) {
    return false;
  } else {
    return child_path.substr(0, parent_path.size()) == parent_path;
  }
}

int daemonize(int foreground, int pipefd, void (*log_exit_error)(char)) {
  int fd;
  char c;

  if (foreground) {
    chdir("/");
    return 0;
  }

  switch (fork()) {
    case -1:
      return -1;
    case 0:
      break;
    default:
      // wait for a completion signal
      read(pipefd, &c, sizeof(c));
      log_exit_error(c);
      _exit(c);
  }

  if (setsid() == -1) {
    return -1;
  }

  chdir("/");
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  return 0;
}

std::string_view trim_string_view(std::string_view s,
                                  std::function<bool(char)> pred) {
  auto ltrim = [pred](std::string_view s) {
    auto it = std::find_if(s.begin(), s.end(),
                           [pred](unsigned char ch) { return !pred(ch); });
    return s.substr(it - s.begin());
  };

  auto rtrim = [pred](std::string_view s) {
    auto it = std::find_if(s.rbegin(), s.rend(), [pred](unsigned char ch) {
                return !pred(ch);
              }).base();
    return s.substr(0, it - s.begin());
  };

  return rtrim(ltrim(s));
}

int64_t get_physical_memory_KiB() {
  FILE *file = fopen("/proc/self/status", "r");
  if (file == NULL) {
    return -1;
  }

  int64_t result = -1;
  char line[128];

  while (fgets(line, 128, file) != nullptr) {
    if (strncmp(line, "VmRSS:", 6) == 0) {
      int len = strlen(line);

      const char *p = line;
      for (; *p && !std::isdigit(*p); ++p) {
      }

      line[len - 3] = 0;
      result = atoll(p);
      break;
    }
  }

  fclose(file);
  return result;
}

void release_assert_fail(const char *file, int line, const char *expr) {
  LOG_ERROR("RELEASE_ASSERT failed at `:`: `", file, line, expr);
  __builtin_trap();
}

std::optional<uint64_t> parse_bytes_string(std::string_view s) {
  auto trimed = trim_string_view(s);
  if (trimed.empty()) return std::nullopt;

  size_t num_end = 0;
  while (num_end < trimed.size() && std::isdigit(trimed[num_end])) {
    ++num_end;
  }

  if (num_end == 0) return std::nullopt;

  std::string_view num_str = trimed.substr(0, num_end);
  std::string_view unit_str = trimed.substr(num_end);

  uint64_t value = 0;
  for (char c : num_str) {
    int digit = c - '0';
    if (value > UINT64_MAX / 10) return std::nullopt;
    value *= 10;
    if (value > UINT64_MAX - digit) return std::nullopt;
    value += digit;
  }

  auto to_upper = [](char c) -> char {
    return (c >= 'a' && c <= 'z') ? static_cast<char>(c - ('a' - 'A')) : c;
  };

  auto is_equal = [to_upper](std::string_view lhs, std::string_view rhs) {
    if (lhs.size() != rhs.size()) return false;
    for (size_t i = 0; i < lhs.size(); ++i) {
      if (to_upper(lhs[i]) != rhs[i]) return false;
    }
    return true;
  };

  if (!unit_str.empty() && (unit_str.back() == 'B' || unit_str.back() == 'b')) {
    unit_str.remove_suffix(1);
  }

  uint64_t multiplier = 1;
  if (unit_str.empty()) {
    // empty body
  } else if (is_equal(unit_str, "K") || is_equal(unit_str, "KI")) {
    multiplier = 1ULL << 10;
  } else if (is_equal(unit_str, "M") || is_equal(unit_str, "MI")) {
    multiplier = 1ULL << 20;
  } else if (is_equal(unit_str, "G") || is_equal(unit_str, "GI")) {
    multiplier = 1ULL << 30;
  } else if (is_equal(unit_str, "T") || is_equal(unit_str, "TI")) {
    multiplier = 1ULL << 40;
  } else if (is_equal(unit_str, "P") || is_equal(unit_str, "PI")) {
    multiplier = 1ULL << 50;
  } else {
    return std::nullopt;
  }

  if (value != 0 && multiplier > UINT64_MAX / value) {
    return std::nullopt;
  }

  return value * multiplier;
}

static void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags != -1) {
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  }
}

int split_command_tokens(const std::string &command,
                         std::vector<std::string> &tokens) {
  tokens.clear();
  wordexp_t wexp;

  int r = wordexp(command.c_str(), &wexp, WRDE_NOCMD | WRDE_UNDEF);
  if (r != 0) {
    return -r;
  }

  for (size_t i = 0; i < wexp.we_wordc; i++) {
    tokens.push_back(wexp.we_wordv[i]);
  }

  wordfree(&wexp);
  return 0;
}

int run_process_safe(const std::vector<std::string> &args,
                     std::string &stdout_output, std::string &stderr_output,
                     size_t max_output_size, int timeout_sec) {
  if (args.empty()) return -EINVAL;

  stdout_output.clear();
  stderr_output.clear();

  int stdout_pipe[2] = {-1, -1};
  int stderr_pipe[2] = {-1, -1};

  if (pipe(stdout_pipe) == -1 || pipe(stderr_pipe) == -1) {
    if (stdout_pipe[0] != -1) {
      close(stdout_pipe[0]);
      close(stdout_pipe[1]);
    }
    if (stderr_pipe[0] != -1) {
      close(stderr_pipe[0]);
      close(stderr_pipe[1]);
    }
    return -1;
  }

  pid_t pid = fork();
  if (pid == -1) {
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    close(stderr_pipe[0]);
    close(stderr_pipe[1]);
    return -1;
  }

  if (pid == 0) {
    // child
    close(stdout_pipe[0]);
    close(stderr_pipe[0]);

    dup2(stdout_pipe[1], STDOUT_FILENO);
    dup2(stderr_pipe[1], STDERR_FILENO);

    close(stdout_pipe[1]);
    close(stderr_pipe[1]);

    std::vector<char *> cargs;
    cargs.reserve(args.size() + 1);

    for (const auto &arg : args) {
      cargs.push_back(const_cast<char *>(arg.c_str()));
    }
    cargs.push_back(nullptr);

    execv(cargs[0], cargs.data());
    _exit(errno);
  }

  // parent
  close(stdout_pipe[1]);
  close(stderr_pipe[1]);

  set_nonblocking(stdout_pipe[0]);
  set_nonblocking(stderr_pipe[0]);

  char buf[4096];
  auto start_time = std::chrono::steady_clock::now();

  // 0: stdout, 1: stderr
  bool eof[2] = {false, false};
  int read_fd[2] = {stdout_pipe[0], stderr_pipe[0]};
  std::string *output[2] = {&stdout_output, &stderr_output};
  bool timed_out = false;
  bool truncated = false;

  while (!eof[0] || !eof[1]) {
    if (timeout_sec > 0) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed =
          std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
              .count();
      if (elapsed >= timeout_sec) {
        timed_out = true;
        goto exit;
      }
    }

    for (int i = 0; i < 2; i++) {
      if (!eof[i]) {
        ssize_t n = read(read_fd[i], buf, sizeof(buf));
        if (n > 0) {
          output[i]->append(buf, n);
          if (output[i]->size() > max_output_size) {
            truncated = true;
            goto exit;
          }
        } else if (n == -1) {
          if (errno != EAGAIN && errno != EWOULDBLOCK) {
            eof[i] = true;  // Error reading, treat as EOF
          }
        } else {
          // n == 0: EOF
          eof[i] = true;
        }
      }
    }

    // Small delay to prevent busy waiting
    usleep(10000);  // 10ms
  }

exit:
  if (timed_out || truncated) {
    kill(pid, SIGTERM);
    usleep(10000);  // 10ms
    kill(pid, SIGKILL);
  }

  close(stdout_pipe[0]);
  close(stderr_pipe[0]);

  int status;
  if (waitpid(pid, &status, 0) == -1) {
    return -1;
  }

  if (timed_out) {
    return -ETIMEDOUT;
  }

  if (truncated) {
    return -ENOMEM;
  }

  if (!WIFEXITED(status)) {
    return -1;
  }

  return WEXITSTATUS(status);
}

time_t parse_iso8601_time(std::string_view s) {
  if (s.size() < 20) return 0;

  struct tm tm;
  memset(&tm, 0, sizeof(tm));
  char *r = strptime(s.data(), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return r ? timegm(&tm) : 0;
}

bool is_valid_fd(int fd) {
  if (fd < 0) {
    return false;
  }

  return fcntl(fd, F_GETFD) != -1;
}

std::vector<std::string_view> split_string(std::string_view str,
                                           std::string_view delimiter) {
  std::vector<std::string_view> parts;

  if (delimiter.empty()) {
    if (!str.empty()) parts.push_back(str);
    return parts;
  }

  size_t pos = 0;
  while (true) {
    size_t next = str.find(delimiter, pos);
    if (next == std::string_view::npos) {
      parts.push_back(str.substr(pos));
      break;
    }
    parts.push_back(str.substr(pos, next - pos));
    pos = next + delimiter.size();
  }

  return parts;
}
