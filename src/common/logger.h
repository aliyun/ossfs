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

#include <photon/common/alog-stdstring.h>
#include <photon/common/alog.h>

#undef LOG_DEBUG
#undef LOG_INFO
#undef LOG_WARN
#undef LOG_ERROR
#undef LOG_FATAL
#undef LOG_TEMP
#undef LOG_ERROR_RETURN
#undef LOG_ERRNO_RETURN
#undef LOG_EVERY_T
#undef LOG_EVERY_N
#undef LOG_FIRST_N

extern ALogLogger *ossfs_logger_ptr;

struct OssfsPrologue {
  const char *addr_file;
  int len_file;
  int line, level;

  template <typename FILEN>
  constexpr OssfsPrologue(FILEN addr_file_, int line_, int level_)
      : addr_file(addr_file_.chars),
        len_file(addr_file_.len),
        line(line_),
        level(level_) {}
};

LogBuffer &operator<<(LogBuffer &log, const OssfsPrologue &pro);

struct OssfsSTFMTLogBuffer : public LogBuffer {
  using LogBuffer::LogBuffer;

  __INLINE__ ~OssfsSTFMTLogBuffer() = default;

  template <typename ST, typename T, typename... Ts>
  auto print_fmt(ST st, T &&t, Ts &&...ts) ->
      typename std::enable_if<ST::template cut<'`'>().tail.chars[0] !=
                              '`'>::type {
    printf<const ALogString &,
           typename CopyOrRef<decltype(alog_forwarding(t))>::type>(
        ALogString(st.template cut<'`'>().head.chars,
                   st.template cut<'`'>().head.len),
        alog_forwarding(std::forward<T>(t)));
    print_fmt(st.template cut<'`'>().tail, std::forward<Ts>(ts)...);
  }

  template <typename ST, typename T, typename... Ts>
  auto print_fmt(ST st, T &&t, Ts &&...ts) ->
      typename std::enable_if<ST::template cut<'`'>().tail.chars[0] ==
                              '`'>::type {
    printf<const ALogString &, const char>(
        ALogString(st.template cut<'`'>().head.chars,
                   st.template cut<'`'>().head.len),
        '`');
    print_fmt(st.template cut<'`'>().tail.template cut<'`'>().tail,
              std::forward<T>(t), std::forward<Ts>(ts)...);
  }

  template <typename ST>
  void print_fmt(ST) {
    printf<const ALogString &>(ALogString(ST::chars, ST::len));
  }

  template <typename T, typename... Ts>
  void print_fmt(ConstString::TString<> st, T &&t, Ts &&...ts) {
    printf<typename CopyOrRef<decltype(alog_forwarding(t))>::type,
           typename CopyOrRef<decltype(alog_forwarding(ts))>::type...>(
        alog_forwarding(std::forward<T>(t)),
        alog_forwarding(std::forward<Ts>(ts))...);
  }
};

template <typename FMT, typename... Ts>
inline OssfsSTFMTLogBuffer __ossfs_log__(int level, ILogOutput *output,
                                         const OssfsPrologue &prolog, FMT fmt,
                                         Ts &&...xs) {
  OssfsSTFMTLogBuffer log(output);
  log << prolog;
  log.print_fmt(fmt, std::forward<Ts>(xs)..., '\n');
  log.level = level;
  return log;
}

#define DEFINE_OSSFS_PROLOGUE(level)                                         \
  auto _prologue_file_r = TSTRING(__FILE__).reverse();                       \
  constexpr auto _partial_file =                                             \
      ConstString::TSpliter<'/', ' ',                                        \
                            decltype(_prologue_file_r)>::Current::reverse(); \
  constexpr static OssfsPrologue prolog(_partial_file, __LINE__, level);

#define __DO_OSSFS_LOG__(attr, logger, level, first, ...)                  \
  ({                                                                       \
    DEFINE_OSSFS_PROLOGUE(level);                                          \
    auto L = [&](ILogOutput * out) __attribute__(attr) {                   \
      if (_IS_LITERAL_STRING(first)) {                                     \
        return __ossfs_log__(level, out, prolog,                           \
                             TSTRING(#first).template strip<'\"'>(),       \
                             ##__VA_ARGS__);                               \
      } else {                                                             \
        return __ossfs_log__(level, out, prolog, ConstString::TString<>(), \
                             first, ##__VA_ARGS__);                        \
      }                                                                    \
    };                                                                     \
    LogBuilder<decltype(L)>(level, ::std::move(L), &logger);               \
  })

#define __OSSFS_LOG__(...)                   \
  ({                                         \
    if (ossfs_logger_ptr == &default_logger) \
      __LOG__(__VA_ARGS__);                  \
    else                                     \
      __DO_OSSFS_LOG__(__VA_ARGS__);         \
  })

#define LOG_DEBUG(...) \
  (__OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, ALOG_DEBUG, __VA_ARGS__))
#define LOG_INFO(...) \
  (__OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, ALOG_INFO, __VA_ARGS__))
#define LOG_WARN(...) \
  (__OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, ALOG_WARN, __VA_ARGS__))
#define LOG_ERROR(...) \
  (__OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, ALOG_ERROR, __VA_ARGS__))
#define LOG_FATAL(...) \
  (__OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, ALOG_FATAL, __VA_ARGS__))
#define LOG_TEMP(...)                                             \
  {                                                               \
    auto _err_bak = errno;                                        \
    __OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, ALOG_TEMP, \
                  __VA_ARGS__);                                   \
    errno = _err_bak;                                             \
  }

// output a log message, set errno, then return a value
// keep errno unchaged if new_errno == 0
#define LOG_ERROR_RETURN(new_errno, retv, ...) \
  {                                            \
    int xcode = (int)(new_errno);              \
    if (xcode == 0) xcode = errno;             \
    LOG_ERROR(__VA_ARGS__);                    \
    errno = xcode;                             \
    return retv;                               \
  }

// output a log message with errno info, set errno, then return a value
// keep errno unchaged if new_errno == 0
#define LOG_ERRNO_RETURN(new_errno, retv, ...) \
  {                                            \
    ERRNO eno;                                 \
    LOG_ERROR(__VA_ARGS__, ' ', eno);          \
    if (new_errno) eno.set(new_errno);         \
    return retv;                               \
  }

#define __OSSFS_LOG_WITH_LIMIT_NO_TAIL(type, ...) \
  [&] {                                           \
    static type limiter;                          \
    auto __logstat__ = __VA_ARGS__;               \
    __logstat__.done = limiter();                 \
  }()

#define __LOG_WITH_LEVEL(level, ...) \
  (__LOG__((noinline, cold), *ossfs_logger_ptr, level, __VA_ARGS__))

#define __OSSFS_LOG_WITH_LEVEL(level, ...) \
  (__DO_OSSFS_LOG__((noinline, cold), *ossfs_logger_ptr, level, __VA_ARGS__))

// Output log once every N rounds. N should larger than 1.
// Example: LOG_EVERY_N(10, LOG_INFO(....))
// This log line will only print 1 time every 10 rounds.
#define LOG_EVERY_N(N, level, ...)                                          \
  ({                                                                        \
    if (ossfs_logger_ptr == &default_logger)                                \
      __OSSFS_LOG_WITH_LIMIT_NO_TAIL(__limit_every_n<N>,                    \
                                     __LOG_WITH_LEVEL(level, __VA_ARGS__)); \
    else                                                                    \
      __OSSFS_LOG_WITH_LIMIT_NO_TAIL(                                       \
          __limit_every_n<N>, __OSSFS_LOG_WITH_LEVEL(level, __VA_ARGS__));  \
  })

int set_ossfs_log_setting(const char *fn, uint64_t rotate_limit = UINT64_MAX,
                          int max_log_files = 10, uint64_t throttle = -1UL,
                          int log_level = ALOG_INFO);

void set_ossfs_log_to_stdout(int log_level);
void set_default_logger_output_to_ossfs_log_file(int log_level);
