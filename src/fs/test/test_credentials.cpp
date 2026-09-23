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

#include <photon/thread/thread.h>
#include <photon/thread/thread11.h>

#include <algorithm>
#include <thread>
#include <vector>

#include "credentials/creds_provider.h"
#include "credentials/ram_role_creds.h"
#include "oss/oss_store.h"
#include "test_suite.h"

class Ossfs2CredentialsTest : public Ossfs2TestSuite {
 public:
  static std::string format_creds(const std::string &ak, const std::string &sk,
                                  const std::string &token,
                                  const std::string &expire) {
    return "{\"AccessKeyId\":\"" + ak + "\",\"AccessKeySecret\":\"" + sk +
           "\",\"SecurityToken\":\"" + token + "\",\"Expiration\":\"" + expire +
           "\"}\n";
  }

  static std::string to_iso8601(time_t t) {
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&t), "%Y-%m-%dT%H:%M:%SZ");
    return ss.str();
  }

 protected:
  void verify_credentials_parser() {
    std::string credentials_response =
        "{\n\t\"AccessKeyId\":\"test_access_key_id\",\n\t\"AccessKeySecret\":"
        "\"test_"
        "access_key_secret\",\n\t\"SecurityToken\":\"test_security_tokenxsq/"
        "3c+/f\",\n"
        "\t\"Expiration\":\"2020-01-01T00:00:00Z\"\n}";
    LOG_INFO("Test `", credentials_response);
    CredentialsParser::Result result;
    int r = CredentialsParser::from_json(credentials_response, result);
    EXPECT_EQ(r, 0);
    EXPECT_EQ(result[CredentialsParser::kAccessKeyId], "test_access_key_id");
    EXPECT_EQ(result[CredentialsParser::kAccessKeySecret],
              "test_access_key_secret");
    EXPECT_EQ(result[CredentialsParser::kSecurityToken],
              "test_security_tokenxsq/3c+/f");
    EXPECT_EQ(result[CredentialsParser::kExpiration], "2020-01-01T00:00:00Z");
    EXPECT_EQ(CredentialsParser::expiration_to_time(
                  result[CredentialsParser::kExpiration]),
              1577836800);

    credentials_response =
        "{\n\t\"AccessKeyId\":\"test_access_key_id\",\n\t\"AccessKeySecret\":"
        "\"test_"
        "access_key_secret\"\n}";
    LOG_INFO("Test `", credentials_response);
    r = CredentialsParser::from_json(credentials_response, result);
    EXPECT_EQ(r, 0);
    EXPECT_EQ(result[CredentialsParser::kAccessKeyId], "test_access_key_id");
    EXPECT_EQ(result[CredentialsParser::kAccessKeySecret],
              "test_access_key_secret");
    EXPECT_EQ(result[CredentialsParser::kSecurityToken], "");
    EXPECT_EQ(result[CredentialsParser::kExpiration], "");

    credentials_response = "";
    LOG_INFO("Test `", credentials_response);
    r = CredentialsParser::from_json(credentials_response, result);
    EXPECT_EQ(r, -EINVAL);
    std::string credentials_response2 = "asdc";
    LOG_INFO("Test `", credentials_response2);
    r = CredentialsParser::from_json(credentials_response2, result);
    EXPECT_EQ(r, -EINVAL);
  }

  static void write_creds_file(const std::string &path,
                               const std::string &expire) {
    write_file(path, format_creds(FLAGS_oss_access_key_id,
                                  FLAGS_oss_access_key_secret, "", expire));
  }

  static int count_lines(const std::string &path) {
    auto content = read_file(path);
    return std::count(content.begin(), content.end(), '\n');
  }

  // Serves valid creds from a file and appends one line per invocation, so a
  // test can count fetches. Returns the counter file path.
  std::string use_counting_credential_process(OssFsOptions &opts) {
    const std::string cred_path = join_paths(test_path_, "creds_process_file");
    const std::string count_path = join_paths(test_path_, "fetch_count");
    write_creds_file(cred_path, to_iso8601(time(nullptr) + 3600));
    write_file(count_path, "");
    opts.credential_process = "/bin/bash -c '/bin/echo x >> " + count_path +
                              " && /bin/cat " + cred_path + "'";
    return count_path;
  }

  int succeeding_force_refreshes(int n) {
    const uint64_t gen = fs_->creds_provider_->generation();
    std::vector<int> results(n, -1);
    std::vector<std::thread> threads;
    threads.reserve(n);
    for (int i = 0; i < n; i++) {
      threads.emplace_back(
          [&, i, gen]() { results[i] = fs_->force_refresh_creds(gen); });
    }
    for (auto &t : threads) t.join();
    return std::count(results.begin(), results.end(), 0);
  }

  void plant_expired_creds() {
    fs_->update_creds(ObjCredentials{"bad_ak", "bad_sk", ""},
                      {time(nullptr) - 10, fs_->creds_provider_->generation()});
  }
};

TEST_F(Ossfs2CredentialsTest, verify_credentials_parser) {
  verify_credentials_parser();
}

TEST_F(Ossfs2CredentialsTest, verify_failure_backoff) {
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(0),
            INT64_C(15) * 1000000);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(1),
            INT64_C(15) * 1000000);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(2),
            INT64_C(30) * 1000000);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(3),
            INT64_C(60) * 1000000);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(4),
            INT64_C(120) * 1000000);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(5),
            INT64_C(240) * 1000000);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(6),
            BackoffRetryStrategy::kMaxBackoffInUsec);
  EXPECT_EQ(BackoffRetryStrategy::failure_backoff_usec(100),
            BackoffRetryStrategy::kMaxBackoffInUsec);
}

TEST_F(Ossfs2CredentialsTest, verify_fixed_interval_retry) {
  FixedIntervalRetryStrategy strategy;
  // Fixed retry interval, no growth, Retry-After ignored.
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(strategy.next_failure_interval_usec(),
              RefreshRetryStrategy::kRetryIntervalInUsec);
  }
  strategy.set_retry_after(60);
  EXPECT_EQ(strategy.next_failure_interval_usec(),
            RefreshRetryStrategy::kRetryIntervalInUsec);
}

TEST_F(Ossfs2CredentialsTest, verify_backoff_retry) {
  BackoffRetryStrategy strategy;

  // Intervals grow with consecutive failures inside [backoff/2, backoff].
  for (uint32_t n = 1; n <= 6; n++) {
    int64_t backoff = BackoffRetryStrategy::failure_backoff_usec(n);
    int64_t interval = strategy.next_failure_interval_usec();
    EXPECT_GE(interval, backoff / 2);
    EXPECT_LE(interval, backoff);
  }

  // After the cap, intervals stay inside [kMaxBackoff/2, kMaxBackoff].
  for (int i = 0; i < 5; i++) {
    int64_t interval = strategy.next_failure_interval_usec();
    EXPECT_GE(interval, BackoffRetryStrategy::kMaxBackoffInUsec / 2);
    EXPECT_LE(interval, BackoffRetryStrategy::kMaxBackoffInUsec);
  }

  // Retry-After defers the next attempt at least that long.
  strategy.set_retry_after(600);
  int64_t interval = strategy.next_failure_interval_usec();
  EXPECT_GE(interval, INT64_C(599) * 1000000);
  EXPECT_LE(interval, INT64_C(600) * 1000000);
}

TEST_F(Ossfs2CredentialsTest, verify_make_refresh_retry_strategy) {
  EXPECT_NE(dynamic_cast<FixedIntervalRetryStrategy *>(
                make_refresh_retry_strategy(false).get()),
            nullptr);
  EXPECT_NE(dynamic_cast<BackoffRetryStrategy *>(
                make_refresh_retry_strategy(true).get()),
            nullptr);
}

namespace {

// Counts on_success callbacks so the provider's delegation can be verified.
class CountingRetryStrategy : public RefreshRetryStrategy {
 public:
  int success_count_ = 0;
  int64_t next_failure_interval_usec() override {
    return RefreshRetryStrategy::kRetryIntervalInUsec;
  }
  void on_success() override {
    success_count_++;
  }
  void set_retry_after(uint64_t) override {}
};

// Always succeeds so refresh_credentials takes the success path without any
// network access or retry sleeps.
class AlwaysSucceedProvider : public CredentialsProvider {
 public:
  explicit AlwaysSucceedProvider(std::unique_ptr<RefreshRetryStrategy> strategy)
      : CredentialsProvider(0, std::move(strategy)) {}

 private:
  int get_credentials(ObjCredentials &out_creds, time_t &expiration) override {
    out_creds = {"test_ak", "test_sk", "test_token"};
    expiration = -1;  // Never expires.
    return 0;
  }
};

class ScriptedProvider : public CredentialsProvider {
 public:
  explicit ScriptedProvider(uint64_t refresh_interval_sec = 0)
      : CredentialsProvider(refresh_interval_sec) {}

  ObjCredentials next_creds_{"ak1", "sk1", "token1"};
  time_t next_expiration_ = -1;
  int fetch_count_ = 0;
  bool fail_fetch_ = false;

  time_t expiration() const {
    return current_expiration_;
  }
  void clear_cooldown() {
    force_refresh_not_before_us_ = 0;
  }

 private:
  int get_credentials(ObjCredentials &out_creds, time_t &expiration) override {
    fetch_count_++;
    if (fail_fetch_) return -EIO;
    out_creds = next_creds_;
    expiration = next_expiration_;
    return 0;
  }
};

}  // namespace

TEST_F(Ossfs2CredentialsTest, verify_provider_delegates_to_strategy) {
  auto strategy = std::make_unique<CountingRetryStrategy>();
  auto *strategy_ptr = strategy.get();
  AlwaysSucceedProvider provider(std::move(strategy));

  auto info =
      provider.refresh_credentials([](const ObjCredentials &) { return true; });

  EXPECT_EQ(strategy_ptr->success_count_, 1);
  EXPECT_NE(info.creds, nullptr);
  EXPECT_EQ(info.next_refresh_interval_us, -1);
}

TEST_F(Ossfs2CredentialsTest, verify_mount_with_invalid_ramrole) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.ram_role = "gtest-ramrole-invalid";

  // init should failed
  SET_TEST_MODE(kTestOss | kTestHdfs);
  EXPECT_NE(do_init(opts), 0);
}

TEST_F(Ossfs2CredentialsTest, verify_credential_process) {
  INIT_PHOTON();
  OssFsOptions opts;

  std::string cred_path = join_paths(test_path_, "creds_process_file");
  std::ofstream cred_file(cred_path);
  DEFER(unlink(cred_path.c_str()));

  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "");
  cred_file.close();

  opts.credential_process = "/bin/cat " + cred_path;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  EXPECT_EQ(do_init(opts), 0);
}

TEST_F(Ossfs2CredentialsTest, verify_invalid_credential_process) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.credential_process = "echo \"{invalid\"";
  EXPECT_NE(do_init(opts), 0);
  destroy();

  opts.credential_process = "echo `ls`";
  EXPECT_NE(do_init(opts), 0);
}

TEST_F(Ossfs2CredentialsTest, verify_credential_refresh) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;

  std::string cred_path = join_paths(test_path_, "creds_process_file");
  std::string ts_path = join_paths(test_path_, "timestamp.txt");
  std::ofstream cred_file(cred_path);
  DEFER(unlink(cred_path.c_str()));
  DEFER(unlink(ts_path.c_str()));

  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "",
                            to_iso8601(time(nullptr) + 5));
  cred_file.close();

  opts.credential_process =
      "/bin/bash -c '/bin/echo $(date -u +\"%Y-%m-%dT%H:%M:%SZ\") > " +
      ts_path + " && /bin/cat " + cred_path + "'";
  EXPECT_EQ(do_init(opts), 0);
  time_t first_refresh = parse_iso8601_time(read_file(ts_path));

  // set invalid expiration time
  cred_file.open(cred_path, std::ios::out | std::ios::trunc);
  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "invalid");
  cred_file.close();

  LOG_INFO("Wait for 20 seconds for credential refresh");
  sleep(20);
  LOG_INFO("After 20 seconds, check result");

  // check timestamp
  time_t refresh_time_1 = parse_iso8601_time(read_file(ts_path));
  EXPECT_TRUE(refresh_time_1 > first_refresh);

  // remove expiration and will not refresh credential anymore
  cred_file.open(cred_path, std::ios::out | std::ios::trunc);
  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "");
  cred_file.close();

  LOG_INFO("Wait for 20 seconds for credential refresh");
  sleep(20);
  LOG_INFO("After 20 seconds, check result");

  // check timestamp
  time_t refresh_time_2 = parse_iso8601_time(read_file(ts_path));
  EXPECT_TRUE(refresh_time_2 > refresh_time_1);

  LOG_INFO("Wait for 20 seconds for credential refresh again");
  sleep(20);
  LOG_INFO("After 20 seconds, check result");

  // refresh time should not change
  EXPECT_TRUE(parse_iso8601_time(read_file(ts_path)) == refresh_time_2);
}

TEST_F(Ossfs2CredentialsTest, verify_credential_fixed_interval_refresh) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.credential_refresh_interval = 3;  // 3 seconds interval.

  std::string cred_path = join_paths(test_path_, "creds_process_file");
  std::string ts_path = join_paths(test_path_, "timestamp.txt");
  std::ofstream cred_file(cred_path);
  DEFER(unlink(cred_path.c_str()));
  DEFER(unlink(ts_path.c_str()));

  // Write initial credentials with real AK/SK
  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "");
  cred_file.close();

  opts.credential_process =
      "/bin/bash -c '/bin/echo $(date -u +\"%Y-%m-%dT%H:%M:%SZ\") > " +
      ts_path + " && /bin/cat " + cred_path + "'";
  EXPECT_EQ(do_init(opts), 0);
  time_t first_refresh = parse_iso8601_time(read_file(ts_path));

  // Wait for fixed interval (3 seconds) + some buffer
  sleep(5);

  // check timestamp - should have refreshed
  time_t refresh_time_1 = parse_iso8601_time(read_file(ts_path));
  EXPECT_TRUE(refresh_time_1 > first_refresh);

  // Wait another interval
  sleep(5);

  // check timestamp - should have refreshed again
  time_t refresh_time_2 = parse_iso8601_time(read_file(ts_path));
  EXPECT_TRUE(refresh_time_2 > refresh_time_1);
}

TEST_F(Ossfs2CredentialsTest, verify_refresh_gates_and_force_bypass) {
  auto valid = [](const ObjCredentials &) { return true; };
  const auto rotated = ObjCredentials{"ak2", "sk2", "token2"};

  ScriptedProvider by_expiry;
  by_expiry.next_expiration_ = time(nullptr) + 7200;
  EXPECT_NE(by_expiry.refresh_credentials(valid).creds, nullptr);
  EXPECT_EQ(by_expiry.fetch_count_, 1);
  EXPECT_EQ(by_expiry.refresh_credentials(valid).creds, nullptr);
  EXPECT_EQ(by_expiry.fetch_count_, 1);

  by_expiry.next_creds_ = rotated;
  CredentialsProvider::CredentialsInfo forced;
  EXPECT_EQ(by_expiry.force_refresh(by_expiry.generation(), valid, &forced), 0);
  EXPECT_EQ(by_expiry.fetch_count_, 2);
  ASSERT_NE(forced.creds, nullptr);
  EXPECT_EQ(forced.creds->accessKeyId, "ak2");
  EXPECT_EQ(by_expiry.generation(), 2U);

  ScriptedProvider by_interval(60);
  EXPECT_NE(by_interval.refresh_credentials(valid).creds, nullptr);
  EXPECT_EQ(by_interval.fetch_count_, 1);
  EXPECT_EQ(by_interval.refresh_credentials(valid).creds, nullptr);
  EXPECT_EQ(by_interval.fetch_count_, 1);

  by_interval.next_creds_ = rotated;
  CredentialsProvider::CredentialsInfo forced_interval;
  EXPECT_EQ(by_interval.force_refresh(by_interval.generation(), valid,
                                      &forced_interval),
            0);
  EXPECT_EQ(by_interval.fetch_count_, 2);
  EXPECT_EQ(by_interval.generation(), 2U);
}

TEST_F(Ossfs2CredentialsTest, verify_credentials_info_expiration) {
  auto valid = [](const ObjCredentials &) { return true; };
  const time_t future = time(nullptr) + 7200;

  ScriptedProvider by_expiry;
  by_expiry.next_expiration_ = future;
  auto info = by_expiry.refresh_credentials(valid);
  EXPECT_EQ(info.meta.expiration, future);
  EXPECT_EQ(info.next_refresh_interval_us, INT64_C(15) * 1000000);
  EXPECT_EQ(info.meta.generation, 1U);

  ScriptedProvider never_expires;
  info = never_expires.refresh_credentials(valid);
  EXPECT_EQ(info.meta.expiration, -1);
  EXPECT_EQ(info.next_refresh_interval_us, -1);
  EXPECT_EQ(info.meta.generation, 1U);

  info = never_expires.refresh_credentials(valid);
  EXPECT_NE(info.creds, nullptr);
  EXPECT_EQ(info.meta.generation, 1U);
  EXPECT_EQ(never_expires.fetch_count_, 2);

  ScriptedProvider by_interval(60);
  by_interval.next_expiration_ = future;
  info = by_interval.refresh_credentials(valid);
  EXPECT_EQ(info.meta.expiration, future);
  EXPECT_EQ(info.next_refresh_interval_us, INT64_C(60) * 1000000);
  EXPECT_EQ(info.meta.generation, 1U);
}

TEST_F(Ossfs2CredentialsTest, verify_force_refresh_unchanged_and_generation) {
  auto valid = [](const ObjCredentials &) { return true; };
  ScriptedProvider provider;
  provider.next_expiration_ = time(nullptr) + 7200;
  ASSERT_NE(provider.refresh_credentials(valid).creds, nullptr);
  ASSERT_EQ(provider.fetch_count_, 1);
  ASSERT_EQ(provider.generation(), 1U);

  CredentialsProvider::CredentialsInfo info;
  EXPECT_EQ(provider.force_refresh(1, valid, &info), 0);
  EXPECT_NE(info.creds, nullptr);
  EXPECT_EQ(info.meta.generation, 1U);
  EXPECT_EQ(provider.generation(), 1U);
  EXPECT_EQ(provider.fetch_count_, 2);

  provider.clear_cooldown();
  provider.next_creds_ = ObjCredentials{"ak2", "sk2", "token2"};
  CredentialsProvider::CredentialsInfo rotated;
  EXPECT_EQ(provider.force_refresh(1, valid, &rotated), 0);
  EXPECT_EQ(rotated.meta.generation, 2U);
  EXPECT_EQ(provider.generation(), 2U);
  EXPECT_EQ(provider.fetch_count_, 3);

  CredentialsProvider::CredentialsInfo stale;
  EXPECT_EQ(provider.force_refresh(1, valid, &stale), 0);
  EXPECT_EQ(stale.creds, nullptr);
  EXPECT_EQ(provider.fetch_count_, 3);
}

TEST_F(Ossfs2CredentialsTest, verify_force_refresh_throttle) {
  auto valid = [](const ObjCredentials &) { return true; };
  ScriptedProvider provider;
  provider.next_expiration_ = time(nullptr) + 7200;
  ASSERT_NE(provider.refresh_credentials(valid).creds, nullptr);

  provider.next_creds_ = ObjCredentials{"ak2", "sk2", "token2"};
  CredentialsProvider::CredentialsInfo first;
  EXPECT_EQ(provider.force_refresh(provider.generation(), valid, &first), 0);
  EXPECT_EQ(provider.fetch_count_, 2);

  provider.next_creds_ = ObjCredentials{"ak3", "sk3", "token3"};
  CredentialsProvider::CredentialsInfo blocked;
  EXPECT_EQ(provider.force_refresh(provider.generation(), valid, &blocked), -1);
  EXPECT_EQ(blocked.creds, nullptr);
  EXPECT_EQ(provider.fetch_count_, 2);

  provider.clear_cooldown();
  CredentialsProvider::CredentialsInfo third;
  EXPECT_EQ(provider.force_refresh(provider.generation(), valid, &third), 0);
  EXPECT_EQ(provider.fetch_count_, 3);
  ASSERT_NE(third.creds, nullptr);
  EXPECT_EQ(third.creds->accessKeyId, "ak3");
}

TEST_F(Ossfs2CredentialsTest, verify_force_refresh_gate_handling_on_failure) {
  INIT_PHOTON();
  auto valid = [](const ObjCredentials &) { return true; };
  auto reject = [](const ObjCredentials &) { return false; };
  const time_t future = time(nullptr) + 7200;

  ScriptedProvider unreachable;
  unreachable.next_expiration_ = future;
  ASSERT_NE(unreachable.refresh_credentials(valid).creds, nullptr);
  unreachable.fail_fetch_ = true;
  CredentialsProvider::CredentialsInfo info;
  EXPECT_EQ(unreachable.force_refresh(unreachable.generation(), valid, &info),
            -1);
  EXPECT_EQ(info.creds, nullptr);
  EXPECT_EQ(unreachable.expiration(), future);

  ScriptedProvider rejected;
  rejected.next_expiration_ = future;
  ASSERT_NE(rejected.refresh_credentials(valid).creds, nullptr);
  info = CredentialsProvider::CredentialsInfo();
  EXPECT_EQ(rejected.force_refresh(rejected.generation(), reject, &info), -1);
  EXPECT_EQ(info.creds, nullptr);
  EXPECT_EQ(rejected.expiration(), 0);

  ScriptedProvider interval_unreachable(60);
  ASSERT_NE(interval_unreachable.refresh_credentials(valid).creds, nullptr);
  ASSERT_EQ(interval_unreachable.fetch_count_, 1);
  interval_unreachable.fail_fetch_ = true;
  info = CredentialsProvider::CredentialsInfo();
  EXPECT_EQ(interval_unreachable.force_refresh(
                interval_unreachable.generation(), valid, &info),
            -1);
  // All three retries burned, and the failed refresh left the interval gate
  // alone: the next refresh is still deferred, not attempted immediately.
  EXPECT_EQ(interval_unreachable.fetch_count_, 4);
  EXPECT_EQ(interval_unreachable.refresh_credentials(valid).creds, nullptr);
  EXPECT_EQ(interval_unreachable.fetch_count_, 4);
}

TEST_F(Ossfs2CredentialsTest, verify_refresh_creds_if_expired_before_send) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  std::unique_ptr<IObjStore> store(new_oss_store("", "", oss_options_));
  int handler_calls = 0;
  uint64_t handler_gen = 0;
  store->set_creds_refresh_handler([&](uint64_t gen) {
    handler_calls++;
    handler_gen = gen;
    return -1;
  });

  auto set_creds = [&](const CredsMeta &creds_meta) {
    store->set_credentials(ObjCredentials{FLAGS_oss_access_key_id,
                                          FLAGS_oss_access_key_secret, ""},
                           creds_meta);
  };
  const std::string missing =
      "/" + gtest_base_dir + "/expired_" + random_string(8);
  ObjHeaderMeta meta;

  for (auto expiration : {time_t(0), time_t(-1), time(nullptr) + 3600}) {
    set_creds({expiration, 7});
    EXPECT_EQ(store->head_object(missing, meta), -ENOENT);
    EXPECT_EQ(handler_calls, 0);
  }

  int expected_calls = 0;
  for (auto expiration : {time(nullptr) - 10, time(nullptr) + 30}) {
    set_creds({expiration, 7});
    EXPECT_EQ(store->head_object(missing, meta), -ENOENT);
    EXPECT_EQ(handler_calls, ++expected_calls);
    EXPECT_EQ(handler_gen, 7U);
  }
}

TEST_F(Ossfs2CredentialsTest, verify_expired_creds_refreshed_before_send) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  const std::string count_path = use_counting_credential_process(opts);
  init(opts);
  ASSERT_EQ(count_lines(count_path), 1);

  plant_expired_creds();

  const std::string missing =
      "/" + gtest_base_dir + "/expired_" + random_string(8);
  ObjHeaderMeta meta;
  EXPECT_EQ(PERFORM_BACKGROUND_OBJ_REQUEST(fs_, head_object, missing, meta),
            -ENOENT);
  EXPECT_EQ(count_lines(count_path), 2);
}

TEST_F(Ossfs2CredentialsTest, verify_auth_error_does_not_trigger_refresh) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  std::unique_ptr<IObjStore> store(new_oss_store("", "", oss_options_));
  int handler_calls = 0;
  store->set_creds_refresh_handler([&](uint64_t) {
    handler_calls++;
    return -1;
  });
  store->set_credentials(ObjCredentials{"bad_ak", "bad_sk", ""},
                         {time(nullptr) + 3600, 1});

  const std::string missing =
      "/" + gtest_base_dir + "/eacces_" + random_string(8);
  ObjHeaderMeta meta;

  EXPECT_EQ(store->head_object(missing, meta), -EACCES);
  EXPECT_EQ(handler_calls, 0);
}

TEST_F(Ossfs2CredentialsTest, verify_concurrent_force_refresh_single_flight) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  const std::string count_path = use_counting_credential_process(opts);
  init(opts);
  ASSERT_EQ(count_lines(count_path), 1);

  // Single-flight is proven by the fetch counter, not by the return codes:
  // had the credential rotated, every caller would return 0 without fetching.
  EXPECT_GE(succeeding_force_refreshes(8), 1);
  EXPECT_EQ(count_lines(count_path), 2);
}

TEST_F(Ossfs2CredentialsTest, verify_single_bg_vcpu_force_refresh_no_deadlock) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  const std::string count_path = use_counting_credential_process(opts);
  bg_vcpu_num_ = 1;
  init(opts);
  ASSERT_EQ(count_lines(count_path), 1);
  ASSERT_EQ(fs_->bg_vcpu_env().bg_obj_store_env->vcpu_num, 1);

  plant_expired_creds();

  // One bg vcpu closes the cycle: the request parks there waiting for the creds
  // executor, which performs back onto it. perform() parks only the coroutine.
  const std::string missing =
      "/" + gtest_base_dir + "/expired_" + random_string(8);
  const int kRequests = 8;
  std::vector<int> results(kRequests, 0);
  photon::semaphore done(0);
  for (int i = 0; i < kRequests; i++) {
    photon::thread_create11([&, i]() {
      DEFER(done.signal(1));
      ObjHeaderMeta meta;
      results[i] =
          PERFORM_BACKGROUND_OBJ_REQUEST(fs_, head_object, missing, meta);
    });
  }

  // A deadlock would hang past teardown, so bound the wait and abort.
  constexpr uint64_t kTimeoutUSec = 120ULL * 1000 * 1000;
  RELEASE_ASSERT_WITH_MSG(done.wait(kRequests, kTimeoutUSec) == 0,
                          "forced credential refresh deadlocked on a single "
                          "background vcpu");

  for (int i = 0; i < kRequests; i++) EXPECT_EQ(results[i], -ENOENT);
  // Only the first waiter fetched; the rest were turned away.
  EXPECT_EQ(count_lines(count_path), 2);
}

TEST_F(Ossfs2CredentialsTest, verify_force_refresh_reaches_every_bg_vcpu) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  const std::string count_path = use_counting_credential_process(opts);
  bg_vcpu_num_ = 4;
  init(opts);
  ASSERT_EQ(count_lines(count_path), 1);
  ASSERT_EQ(fs_->bg_vcpu_env().bg_obj_store_env->vcpu_num, 4);

  plant_expired_creds();

  // Eight sequential requests round-robin over the four stores. A store the
  // refresh never reached would still sign with the planted bad credentials
  // and answer -EACCES instead of -ENOENT.
  const std::string missing =
      "/" + gtest_base_dir + "/expired_" + random_string(8);
  for (int i = 0; i < 8; i++) {
    ObjHeaderMeta meta;
    EXPECT_EQ(PERFORM_BACKGROUND_OBJ_REQUEST(fs_, head_object, missing, meta),
              -ENOENT);
  }
  EXPECT_EQ(count_lines(count_path), 2);
}
