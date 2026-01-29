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

#include <map>

#include "oss/oss_adapter.h"

namespace OssFileSystem {

class CredentialsParser {
 public:
  using Result = std::map<std::string, std::string>;

  static inline constexpr char kAccessKeyId[] = "AccessKeyId";
  static inline constexpr char kAccessKeySecret[] = "AccessKeySecret";
  static inline constexpr char kSecurityToken[] = "SecurityToken";
  static inline constexpr char kExpiration[] = "Expiration";

  static int from_json(const std::string &body, Result &out);

  static time_t expiration_to_time(std::string_view expiration);
};

class CredentialsProvider : public Object {
 public:
  static constexpr int64_t kRetryIntervalInUsec = 15ULL * 1000000;

  struct CredentialsInfo {
    std::shared_ptr<OssCredentials> creds;
    int64_t next_refresh_interval_us = 0;
  };

  using CredentialsValidator = std::function<bool(const OssCredentials &)>;

  virtual CredentialsInfo refresh_credentials(CredentialsValidator validator);

 protected:
  virtual int get_credentials(OssCredentials &out_creds, time_t &expiration) {
    return -ENOSYS;
  }
};

CredentialsProvider *new_ram_role_creds_provider(std::string_view ram_role);
CredentialsProvider *new_process_creds_provider(std::string_view process_cmd);

};  // namespace OssFileSystem
