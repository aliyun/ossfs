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

#include <photon/ecosystem/simple_dom.h>

#include "common/logger.h"
#include "common/utils.h"
#include "process_creds.h"
#include "ram_role_creds.h"

namespace OssFileSystem {

int CredentialsParser::from_json(const std::string &body, Result &out) {
  if (body.empty()) return -EINVAL;
  auto root = photon::SimpleDOM::parse_copy(body.c_str(), body.size(),
                                            photon::SimpleDOM::DOC_JSON);
  if (!root[kAccessKeyId] || !root[kAccessKeySecret]) {
    return -EINVAL;
  }

  out.clear();
  out.emplace(kAccessKeyId, root[kAccessKeyId].to_string_view());
  out.emplace(kAccessKeySecret, root[kAccessKeySecret].to_string_view());
  out.emplace(kSecurityToken, root[kSecurityToken].to_string_view());
  out.emplace(kExpiration, root[kExpiration].to_string_view());
  return 0;
}

time_t CredentialsParser::expiration_to_time(std::string_view expiration) {
  return parse_iso8601_time(expiration);
}

CredentialsProvider::CredentialsInfo CredentialsProvider::refresh_credentials(
    CredentialsValidator validator) {
  const auto interval = std::chrono::microseconds(15ULL * 1000 * 1000);
  const int expire_margin_in_sec = 60 * 20;

  // Should not be refreshed parallel.
  static time_t expiration = 0;

  if (expiration >= time(nullptr) + expire_margin_in_sec) {
    return {nullptr, interval.count()};
  }

  OssCredentials creds;
  time_t new_expiration = 0;
  const int max_retry = 3;
  int r = 0;
  for (int i = 0; i < max_retry; i++) {
    r = get_credentials(creds, new_expiration);
    if (r == 0) break;
    photon::thread_usleep(100000);
  }

  if (r != 0) {
    // Retry after 15s.
    return {nullptr, kRetryIntervalInUsec};
  }

  if (!validator(creds)) {
    // Force refresh next time.
    expiration = 0;
    return {nullptr, kRetryIntervalInUsec};
  }

  if (new_expiration == -1) {
    return {std::make_shared<OssCredentials>(creds), -1};
  }

  expiration = new_expiration;
  return {std::make_shared<OssCredentials>(creds), interval.count()};
}

CredentialsProvider *new_ram_role_creds_provider(std::string_view ram_role) {
  return new RamRoleCredentialsProvider(ram_role);
}

CredentialsProvider *new_process_creds_provider(std::string_view process_cmd) {
  return new ProcessCredentialsProvider(process_cmd);
}

};  // namespace OssFileSystem
