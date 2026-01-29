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

#include <photon/io/signal.h>
#include <photon/photon.h>

#include "common/logger.h"
#include "common/utils.h"

constexpr const char *__basename(const char *path) {
  const char *last_slash = path;
  for (const char *p = path; *p != '\0'; ++p) {
    if (*p == '/' || *p == '\\') {
      last_slash = p + 1;
    }
  }
  return last_slash;
}

#define __FILENAME__ __basename(__FILE__)

#define RELEASE_ASSERT(expr)         \
  (static_cast<bool>(unlikely(expr)) \
       ? void(0)                     \
       : release_assert_fail(__FILENAME__, __LINE__, #expr))

#define RELEASE_ASSERT_WITH_MSG(expr, msg...)             \
  do {                                                    \
    if (!unlikely(expr)) {                                \
      LOG_ERROR(msg);                                     \
      release_assert_fail(__FILENAME__, __LINE__, #expr); \
    }                                                     \
  } while (0)

#define RETURN_IF_TRUE(cond, ret_val, ...) \
  do {                                     \
    if (cond) {                            \
      __VA_ARGS__;                         \
      return (ret_val);                    \
    }                                      \
  } while (0)

#define MACRO_STR_HELPER(x) #x
#define MACRO_STR(x) MACRO_STR_HELPER(x)

#define OSSFS_EVENT_ENGINE photon::INIT_EVENT_EPOLL | photon::INIT_EVENT_SELECT

#define INIT_PHOTON()                                     \
  photon::block_all_signal();                             \
  photon::init(OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE); \
  DEFER(photon::fini())
