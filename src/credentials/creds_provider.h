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

#include <cstdint>
#include <map>
#include <memory>

#include "oss/obj_store.h"

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

// Decides when the refresh loop should retry after a failed credential
// refresh. New STS retry policies plug in by implementing this interface and
// supplying it via make_refresh_retry_strategy() or the strategy-injection
// constructor.
class RefreshRetryStrategy {
 public:
  static constexpr int64_t kRetryIntervalInUsec = 15ULL * 1000000;

  virtual ~RefreshRetryStrategy() = default;

  // Interval (usec) to wait before the next attempt following a failure.
  virtual int64_t next_failure_interval_usec() = 0;

  // Reset any accumulated state after a successful refresh.
  virtual void on_success() = 0;

  // HTTP providers report a 429/503 Retry-After hint (delta-seconds) here so
  // the strategy can defer the next attempt accordingly.
  virtual void set_retry_after(uint64_t seconds) = 0;
};

// Historical behavior: retry after a fixed interval, ignore Retry-After.
class FixedIntervalRetryStrategy : public RefreshRetryStrategy {
 public:
  int64_t next_failure_interval_usec() override;
  void on_success() override {}
  void set_retry_after(uint64_t) override {}
};

// Exponential backoff with jitter, honoring Retry-After deadlines.
class BackoffRetryStrategy : public RefreshRetryStrategy {
 public:
  static constexpr int64_t kMaxBackoffInUsec = 300ULL * 1000000;
  static constexpr int64_t kMaxRetryAfterInUsec = 3600ULL * 1000000;

  int64_t next_failure_interval_usec() override;
  void on_success() override;
  void set_retry_after(uint64_t seconds) override;

  // Deterministic exponential backoff for the n-th consecutive failure
  // (1-based): 15s, 30s, 60s, 120s, 240s, capped at kMaxBackoffInUsec.
  static int64_t failure_backoff_usec(uint32_t n);

 private:
  uint32_t consecutive_failures_ = 0;
  // Monotonic deadline (usec) from Retry-After, 0 means none.
  int64_t retry_after_deadline_us_ = 0;
};

// Selects fixed-interval or backoff retry based on the backoff flag.
std::unique_ptr<RefreshRetryStrategy> make_refresh_retry_strategy(
    bool backoff_enabled);

class CredentialsProvider : public Object {
 public:
  struct CredentialsInfo {
    std::shared_ptr<ObjCredentials> creds;
    int64_t next_refresh_interval_us = 0;
    CredsMeta meta;
  };

  using CredentialsValidator = std::function<bool(const ObjCredentials &)>;

  CredentialsProvider() : CredentialsProvider(false) {}
  // Enables exponential backoff with jitter (and Retry-After handling) on
  // refresh failures. Disabled by default: failures retry after a fixed
  // interval, matching the historical behavior.
  explicit CredentialsProvider(bool backoff_enabled)
      : CredentialsProvider(0, make_refresh_retry_strategy(backoff_enabled)) {}
  CredentialsProvider(uint64_t refresh_interval_sec,
                      bool backoff_enabled = false)
      : CredentialsProvider(refresh_interval_sec,
                            make_refresh_retry_strategy(backoff_enabled)) {}
  // Direct strategy injection for future custom STS retry policies.
  CredentialsProvider(uint64_t refresh_interval_sec,
                      std::unique_ptr<RefreshRetryStrategy> strategy)
      : refresh_interval_sec_(refresh_interval_sec),
        retry_strategy_(std::move(strategy)) {}
  virtual ~CredentialsProvider() = default;

  virtual CredentialsInfo refresh_credentials(CredentialsValidator validator,
                                              bool force = false);

  uint64_t generation() const {
    return generation_;
  }

  int force_refresh(uint64_t observed_gen, CredentialsValidator validator,
                    CredentialsInfo *info);

 protected:
  virtual int get_credentials(ObjCredentials &out_creds, time_t &expiration) {
    return -ENOSYS;
  }

  CredentialsInfo refresh_with_fixed_interval(CredentialsValidator validator,
                                              bool force);

  bool fetch_creds_with_retry(ObjCredentials &out_creds,
                              time_t &out_expiration);

  // HTTP providers call this when the server answers 429/503 with a
  // Retry-After header (delta-seconds form); forwards to the strategy.
  void set_retry_after(uint64_t seconds);

  // 0 means use default expiration-based strategy.
  const uint64_t refresh_interval_sec_ = 0;

  // Unsynchronized: callers must serialize refreshes.
  ObjCredentials current_creds_;
  time_t current_expiration_ = 0;
  time_t last_refresh_time_ = 0;
  uint64_t generation_ = 0;

  static constexpr int64_t kForceRefreshCooldownUSec = 5LL * 1000 * 1000;
  int64_t force_refresh_not_before_us_ = 0;

  std::unique_ptr<RefreshRetryStrategy> retry_strategy_;
};

CredentialsProvider *new_ram_role_creds_provider(std::string_view ram_role,
                                                 bool backoff_enabled = false);
CredentialsProvider *new_process_creds_provider(
    std::string_view process_cmd, uint64_t refresh_interval_sec = 0,
    bool backoff_enabled = false);

};  // namespace OssFileSystem
