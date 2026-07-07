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
#include <photon/thread/thread.h>

#include <chrono>

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
  if (refresh_interval_sec_ > 0) {
    return refresh_with_fixed_interval(validator);
  }

  // Expiration-based refresh mode.
  const auto poll_interval = std::chrono::microseconds(15ULL * 1000 * 1000);
  const int expire_margin_in_sec = 60 * 20;

  if (current_expiration_ >= time(nullptr) + expire_margin_in_sec) {
    return {nullptr, poll_interval.count()};
  }

  ObjCredentials new_creds;
  time_t new_expiration = 0;
  const int max_retry = 3;
  int r = 0;
  for (int i = 0; i < max_retry; i++) {
    auto t0 = std::chrono::steady_clock::now();
    r = get_credentials(new_creds, new_expiration);
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    LOG_INFO("get_credentials attempt ` completed in ` us, r: `", i + 1,
             elapsed, r);
    if (r == 0) break;
    photon::thread_usleep(100000);
  }

  if (r != 0) {
    return {nullptr, kRetryIntervalInUsec};
  }

  if (is_credentials_changed(new_creds, new_expiration) &&
      !validator(new_creds)) {
    current_expiration_ = 0;
    return {nullptr, kRetryIntervalInUsec};
  }

  current_creds_ = new_creds;
  current_expiration_ = new_expiration;

  if (new_expiration == -1) {
    // Never expires, no auto refresh.
    return {std::make_shared<ObjCredentials>(current_creds_), -1};
  }
  return {std::make_shared<ObjCredentials>(current_creds_),
          poll_interval.count()};
}

CredentialsProvider::CredentialsInfo
CredentialsProvider::refresh_with_fixed_interval(
    CredentialsValidator validator) {
  time_t now = time(nullptr);

  time_t next_refresh_time = last_refresh_time_ + refresh_interval_sec_;
  if (last_refresh_time_ > 0 && now < next_refresh_time) {
    int64_t wait_sec = next_refresh_time - now;
    return {nullptr, std::chrono::seconds(wait_sec).count() * 1000000};
  }

  ObjCredentials new_creds;
  time_t new_expiration = 0;
  const int max_retry = 3;
  int r = 0;
  for (int i = 0; i < max_retry; i++) {
    auto t0 = std::chrono::steady_clock::now();
    r = get_credentials(new_creds, new_expiration);
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    LOG_INFO("get_credentials attempt ` completed in ` us, r: `", i + 1,
             elapsed, r);
    if (r == 0) break;
    photon::thread_usleep(100000);
  }

  if (r != 0) {
    return {nullptr, kRetryIntervalInUsec};
  }

  // Skip validation if credentials haven't changed.
  // In fixed interval mode, force expiration to 0 so it won't trigger changed.
  if (is_credentials_changed(new_creds, 0) && !validator(new_creds)) {
    last_refresh_time_ = 0;
    return {nullptr, kRetryIntervalInUsec};
  }

  current_creds_ = new_creds;
  last_refresh_time_ = time(nullptr);

  return {std::make_shared<ObjCredentials>(current_creds_),
          std::chrono::seconds(refresh_interval_sec_).count() * 1000000};
}

bool CredentialsProvider::is_credentials_changed(
    const ObjCredentials &new_creds, time_t new_expiration) const {
  // Always return true when current_creds_.accessKeyId is empty.
  if (current_creds_.accessKeyId.empty()) {
    return true;
  }

  return current_creds_.accessKeyId != new_creds.accessKeyId ||
         current_creds_.accessKeySecret != new_creds.accessKeySecret ||
         current_creds_.securityToken != new_creds.securityToken ||
         current_expiration_ != new_expiration;
}

CredentialsProvider *new_ram_role_creds_provider(std::string_view ram_role) {
  return new RamRoleCredentialsProvider(ram_role);
}

CredentialsProvider *new_process_creds_provider(std::string_view process_cmd,
                                                uint64_t refresh_interval_sec) {
  return new ProcessCredentialsProvider(process_cmd, refresh_interval_sec);
}

};  // namespace OssFileSystem
