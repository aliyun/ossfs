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

#include <algorithm>
#include <chrono>
#include <random>

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

namespace {

int64_t monotonic_now_usec() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

bool creds_differ(const ObjCredentials &a, const ObjCredentials &b) {
  if (a.accessKeyId.empty() || b.accessKeyId.empty()) return true;
  return a.accessKeyId != b.accessKeyId ||
         a.accessKeySecret != b.accessKeySecret ||
         a.securityToken != b.securityToken;
}

}  // namespace

int64_t FixedIntervalRetryStrategy::next_failure_interval_usec() {
  LOG_WARN("credential refresh failed, next attempt in ` us",
           kRetryIntervalInUsec);
  return kRetryIntervalInUsec;
}

int64_t BackoffRetryStrategy::failure_backoff_usec(uint32_t n) {
  if (n == 0) n = 1;
  int64_t backoff = kRetryIntervalInUsec;
  for (uint32_t i = 1; i < n; ++i) {
    backoff = std::min(backoff * 2, kMaxBackoffInUsec);
    if (backoff == kMaxBackoffInUsec) break;
  }
  return backoff;
}

void BackoffRetryStrategy::set_retry_after(uint64_t seconds) {
  if (seconds == 0) return;
  int64_t capped = std::min<int64_t>(seconds, kMaxRetryAfterInUsec / 1000000);
  retry_after_deadline_us_ = monotonic_now_usec() + capped * 1000000;
  LOG_WARN("credential refresh deferred for ` second(s) per Retry-After",
           capped);
}

void BackoffRetryStrategy::on_success() {
  consecutive_failures_ = 0;
  retry_after_deadline_us_ = 0;
}

int64_t BackoffRetryStrategy::next_failure_interval_usec() {
  consecutive_failures_++;
  int64_t backoff = failure_backoff_usec(consecutive_failures_);

  // Jitter spreads mounts that fail in phase (e.g. batch-started sandboxes):
  // pick uniformly from [backoff/2, backoff].
  thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<int64_t> dist(backoff / 2, backoff);
  int64_t interval = dist(rng);

  int64_t retry_after_wait = retry_after_deadline_us_ - monotonic_now_usec();
  if (retry_after_wait > interval) {
    interval = std::min(retry_after_wait, kMaxRetryAfterInUsec);
  }

  LOG_WARN(
      "credential refresh failed ` consecutive time(s), next attempt in ` us",
      consecutive_failures_, interval);
  return interval;
}

std::unique_ptr<RefreshRetryStrategy> make_refresh_retry_strategy(
    bool backoff_enabled) {
  if (backoff_enabled) {
    return std::make_unique<BackoffRetryStrategy>();
  }
  return std::make_unique<FixedIntervalRetryStrategy>();
}

void CredentialsProvider::set_retry_after(uint64_t seconds) {
  retry_strategy_->set_retry_after(seconds);
}

bool CredentialsProvider::fetch_creds_with_retry(ObjCredentials &out_creds,
                                                 time_t &out_expiration) {
  for (int i = 0; i < 3; i++) {
    auto t0 = std::chrono::steady_clock::now();
    int r = get_credentials(out_creds, out_expiration);
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    LOG_INFO("get_credentials attempt ` completed in ` us, r: `", i + 1,
             elapsed, r);
    if (r == 0) return true;
    photon::thread_usleep(100000);
  }
  return false;
}

CredentialsProvider::CredentialsInfo CredentialsProvider::refresh_credentials(
    CredentialsValidator validator, bool force) {
  if (refresh_interval_sec_ > 0) {
    return refresh_with_fixed_interval(validator, force);
  }

  // Expiration-based refresh mode.
  const auto poll_interval = std::chrono::microseconds(15ULL * 1000 * 1000);
  const int expire_margin_in_sec = 60 * 20;

  if (!force && current_expiration_ >= time(nullptr) + expire_margin_in_sec) {
    return {nullptr, poll_interval.count()};
  }

  ObjCredentials new_creds;
  time_t new_expiration = 0;
  if (!fetch_creds_with_retry(new_creds, new_expiration)) {
    return {nullptr, retry_strategy_->next_failure_interval_usec()};
  }

  const bool changed = creds_differ(current_creds_, new_creds);

  if ((force || changed || current_expiration_ != new_expiration) &&
      !validator(new_creds)) {
    current_expiration_ = 0;
    return {nullptr, retry_strategy_->next_failure_interval_usec()};
  }

  current_creds_ = new_creds;
  current_expiration_ = new_expiration;
  retry_strategy_->on_success();
  if (changed) generation_++;

  // A credential that never expires schedules no auto refresh.
  const int64_t next_us = new_expiration == -1 ? -1 : poll_interval.count();
  return {std::make_shared<ObjCredentials>(current_creds_),
          next_us,
          {new_expiration, generation_}};
}

CredentialsProvider::CredentialsInfo
CredentialsProvider::refresh_with_fixed_interval(CredentialsValidator validator,
                                                 bool force) {
  time_t now = time(nullptr);

  time_t next_refresh_time = last_refresh_time_ + refresh_interval_sec_;
  if (!force && last_refresh_time_ > 0 && now < next_refresh_time) {
    int64_t wait_sec = next_refresh_time - now;
    return {nullptr, std::chrono::seconds(wait_sec).count() * 1000000};
  }

  ObjCredentials new_creds;
  time_t new_expiration = 0;
  if (!fetch_creds_with_retry(new_creds, new_expiration)) {
    return {nullptr, retry_strategy_->next_failure_interval_usec()};
  }

  const bool changed = creds_differ(current_creds_, new_creds);

  if ((force || changed) && !validator(new_creds)) {
    last_refresh_time_ = 0;
    return {nullptr, retry_strategy_->next_failure_interval_usec()};
  }

  current_creds_ = new_creds;
  last_refresh_time_ = time(nullptr);
  retry_strategy_->on_success();
  if (changed) generation_++;

  return {std::make_shared<ObjCredentials>(current_creds_),
          std::chrono::seconds(refresh_interval_sec_).count() * 1000000,
          {new_expiration, generation_}};
}

int CredentialsProvider::force_refresh(uint64_t observed_gen,
                                       CredentialsValidator validator,
                                       CredentialsInfo *info) {
  if (generation_ > observed_gen) {
    return 0;
  }
  if (monotonic_now_usec() < force_refresh_not_before_us_) {
    LOG_WARN("skip forced credential refresh, cooling down, gen `",
             observed_gen);
    return -1;
  }

  *info = refresh_credentials(validator, true);
  // Set on failure too, so an unreachable STS is not hammered per request.
  force_refresh_not_before_us_ =
      monotonic_now_usec() + kForceRefreshCooldownUSec;

  if (info->creds == nullptr) {
    LOG_ERROR("forced credential refresh got no credentials, gen `",
              observed_gen);
    return -1;
  }

  LOG_INFO("forced credential refresh done, gen ` -> `", observed_gen,
           info->meta.generation);
  return 0;
}

CredentialsProvider *new_ram_role_creds_provider(std::string_view ram_role,
                                                 bool backoff_enabled) {
  return new RamRoleCredentialsProvider(ram_role, backoff_enabled);
}

CredentialsProvider *new_process_creds_provider(std::string_view process_cmd,
                                                uint64_t refresh_interval_sec,
                                                bool backoff_enabled) {
  return new ProcessCredentialsProvider(process_cmd, refresh_interval_sec,
                                        backoff_enabled);
}

};  // namespace OssFileSystem
