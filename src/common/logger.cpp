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

#include "logger.h"

#include <fcntl.h>
#include <limits.h>
#include <photon/thread/thread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace std;

ALogLogger *ossfs_logger_ptr = &default_logger;

// copied from photon alog.cpp
class BaseLogOutput : public ILogOutput {
 public:
  uint64_t throttle = -1UL;
  uint64_t count = 0;
  time_t ts = 0;
  int log_file_fd;

  constexpr BaseLogOutput(int fd = 0) : log_file_fd(fd) {}

  void write(int, const char *begin, const char *end) override {
    std::ignore = ::write(log_file_fd, begin, end - begin);
    throttle_block();
  }
  void throttle_block() {
    if (throttle == -1UL) return;
    time_t t = time(0);
    if (t > ts) {
      ts = t;
      count = 0;
    }
    if (++count > throttle) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  virtual int get_log_file_fd() override {
    return log_file_fd;
  }
  virtual uint64_t set_throttle(uint64_t t = -1) override {
    return throttle = t;
  }
  virtual uint64_t get_throttle() override {
    return throttle;
  }
  virtual void destruct() override {}
};

// copied from photon alog.cpp
class LogOutputFile final : public BaseLogOutput {
 public:
  uint64_t log_file_size_limit = 0;
  char *log_file_name = nullptr;
  atomic<uint64_t> log_file_size{0};
  unsigned int log_file_max_cnt = 10;

  void destruct() override {
    log_output_file_close();
    delete this;
  }

  int fopen(const char *fn) {
    auto mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    return open(fn, O_CREAT | O_WRONLY | O_APPEND, mode);
  }

  void write(int, const char *begin, const char *end) override {
    if (log_file_fd < 0) return;
    uint64_t length = end - begin;
    iovec iov{(void *)begin, length};
    std::ignore = ::writev(log_file_fd, &iov,
                           1);  // writev() is atomic, whereas write() is not
    throttle_block();
    if (log_file_name && log_file_size_limit) {
      log_file_size += length;
      if (log_file_size > log_file_size_limit) {
        static mutex log_file_lock;
        lock_guard<mutex> guard(log_file_lock);
        if (log_file_size > log_file_size_limit) {
          log_file_rotate();
          reopen_log_output_file();
        }
      }
    }
  }

  static inline void add_generation(char *buf, int size,
                                    unsigned int generation) {
    if (generation == 0) {
      buf[0] = '\0';
    } else {
      snprintf(buf, size, ".%u", generation);
    }
  }

  void log_output_file_setting(int fd) {
    if (fd < 0) return;
    if (log_file_fd > 2 && log_file_fd != fd) close(log_file_fd);

    log_file_fd = fd;
    log_file_size.store(lseek(fd, 0, SEEK_END));
    free(log_file_name);
    log_file_name = nullptr;
    log_file_size_limit = 0;
  }

  int log_output_file_setting(const char *fn, uint64_t rotate_limit,
                              int max_log_files) {
    int fd = fopen(fn);
    if (fd < 0) return -1;

    log_output_file_setting(fd);
    free(log_file_name);
    log_file_name = strdup(fn);
    log_file_size_limit = max(rotate_limit, (uint64_t)(1024 * 1024));
    return 0;
  }

  void reopen_log_output_file() {
    int fd = fopen(log_file_name);
    if (fd < 0) {
      static char msg[] = "failed to open log output file: ";
      std::ignore = ::write(log_file_fd, msg, sizeof(msg) - 1);
      if (log_file_name)
        std::ignore =
            ::write(log_file_fd, log_file_name, strlen(log_file_name));
      std::ignore = ::write(log_file_fd, "\n", 1);
      return;
    }

    log_file_size = 0;
    dup2(fd, log_file_fd);  // to make sure log_file_fd
    close(fd);              // doesn't change
  }

  void log_file_rotate() {
    if (!log_file_name || access(log_file_name, F_OK) != 0) return;

    int fn_length = (int)strlen(log_file_name);
    char fn0[PATH_MAX], fn1[PATH_MAX];
    strcpy(fn0, log_file_name);
    strcpy(fn1, log_file_name);

    unsigned int last_generation = 1;  // not include
    while (true) {
      add_generation(fn0 + fn_length, sizeof(fn0) - fn_length, last_generation);
      if (0 != access(fn0, F_OK)) break;
      last_generation++;
    }

    while (last_generation >= 1) {
      add_generation(fn0 + fn_length, sizeof(fn0) - fn_length,
                     last_generation - 1);
      add_generation(fn1 + fn_length, sizeof(fn1) - fn_length, last_generation);

      if (last_generation >= log_file_max_cnt) {
        unlink(fn0);
      } else {
        rename(fn0, fn1);
      }
      last_generation--;
    }
  }

  int log_output_file_close() {
    if (log_file_fd < 0) {
      errno = EALREADY;
      return -1;
    }
    close(log_file_fd);
    log_file_fd = -1;
    free(log_file_name);
    log_file_name = nullptr;
    return 0;
  }
};

class StdLogOutput final : public BaseLogOutput {
 public:
  void destruct() override {
    delete this;
  }

  void write(int level, const char *begin, const char *end) override {
    uint64_t length = end - begin;
    iovec iov{(void *)begin, length};
    if (level == ALOG_ERROR || level == ALOG_FATAL) {
      ::writev(2, &iov, 1);
    } else {
      ::writev(1, &iov, 1);
    }
  }
};

static StdLogOutput _ossfs_log_output_stdout;
ILogOutput *const ossfs_log_output_stdout = &_ossfs_log_output_stdout;

ALogLogger ossfs_logger{ossfs_log_output_stdout, ALOG_DEBUG};

static LogOutputFile ossfs_log_output_file;

int set_ossfs_log_setting(const char *fn, uint64_t rotate_limit,
                          int max_log_files, uint64_t throttle, int log_level) {
  int ret = ossfs_log_output_file.log_output_file_setting(fn, rotate_limit,
                                                          max_log_files);
  if (ret < 0) LOG_ERROR_RETURN(0, -1, "Fail to open log file ", fn);
  ossfs_log_output_file.set_throttle(throttle);
  ossfs_logger.log_output = &ossfs_log_output_file;
  ossfs_logger.log_level = log_level;

  ossfs_logger_ptr = &ossfs_logger;
  return 0;
}

void set_ossfs_log_to_stdout(int log_level) {
  ossfs_logger.log_level = log_level;
  ossfs_logger_ptr = &ossfs_logger;
}

void set_default_logger_output_to_ossfs_log_file(int log_level) {
  default_logger.log_output = &ossfs_log_output_file;
  default_logger.log_level = log_level;
}

__attribute__((constructor)) static void __initial_timezone() {
  tzset();
}
static time_t dayid = 0, minuteid = -1, tsdelta = 0;
static struct tm olog_time = {0};
static struct tm *olog_update_time(time_t now0) {
  auto now = now0 + tsdelta;
  int sec = now % 60;
  now /= 60;
  if (unlikely(now != minuteid)) {  // calibrate wall time every minute
    now = time(0) - timezone;
    tsdelta = now - now0;
    sec = now % 60;
    now /= 60;
    minuteid = now;
  }
  int min = now % 60;
  now /= 60;
  int hor = now % 24;
  now /= 24;
  if (now != dayid) {
    dayid = now;
    auto now_ = now0 + tsdelta;
    gmtime_r(&now_, &olog_time);
    olog_time.tm_year += 1900;
    olog_time.tm_mon++;
  } else {
    olog_time.tm_sec = sec;
    olog_time.tm_min = min;
    olog_time.tm_hour = hor;
  }
  return &olog_time;
}

LogBuffer &operator<<(LogBuffer &log, const OssfsPrologue &pro) {
  auto ts = photon::__update_now();
  auto t = olog_update_time(ts.sec());

#define DEC_W2P0(x) DEC(x).width(2).padding('0')
  log.printf(t->tm_year, '/');
  log.printf(DEC_W2P0(t->tm_mon), '/');
  log.printf(DEC_W2P0(t->tm_mday), ' ');
  log.printf(DEC_W2P0(t->tm_hour), ':');
  log.printf(DEC_W2P0(t->tm_min), ':');
  log.printf(DEC_W2P0(t->tm_sec), '.');
  log.printf(DEC(ts.usec()).width(6).padding('0'));

  static const char levels[] =
      "|DEBUG|th=|INFO |th=|WARN |th=|ERROR|th=|FATAL|th=|TEMP |th=|AUDIT|th=";
  log.level = pro.level;
  log.printf(ALogString(&levels[pro.level * 10], 10));
  log.printf(photon::CURRENT, '|');
  if (pro.level != ALOG_AUDIT) {
    log.printf(ALogString(pro.addr_file, pro.len_file), ':');
    log.printf(pro.line, '|');
  }
  return log;
}
