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

#include "credentials/creds_provider.h"
#include "credentials/ram_role_creds.h"
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
};

TEST_F(Ossfs2CredentialsTest, verify_credentials_parser) {
  verify_credentials_parser();
}

TEST_F(Ossfs2CredentialsTest, verify_mount_with_invalid_ramrole) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.ram_role = "gtest-ramrole-invalid";

  // init should failed
  EXPECT_NE(do_init(opts), 0);
}

TEST_F(Ossfs2CredentialsTest, verify_credential_process) {
  INIT_PHOTON();
  OssFsOptions opts;

  std::ofstream cred_file("ossfs2_test_creds_process_file");
  DEFER(unlink("ossfs2_test_creds_process_file"));

  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "");
  cred_file.close();

  opts.credential_process = "/bin/cat ossfs2_test_creds_process_file";
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

  std::ofstream cred_file("ossfs2_test_creds_process_file");
  DEFER(unlink("ossfs2_test_creds_process_file"));

  DEFER(unlink("timestamp.txt"));

  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "",
                            to_iso8601(time(nullptr) + 5));
  cred_file.close();

  opts.credential_process =
      "/bin/bash -c '/bin/echo $(date -u +\"%Y-%m-%dT%H:%M:%SZ\") > "
      "timestamp.txt && /bin/cat "
      "ossfs2_test_creds_process_file'";
  EXPECT_EQ(do_init(opts), 0);

  time_t first_refresh = parse_iso8601_time(read_file("timestamp.txt"));

  // set invalid expiration time
  cred_file.open("ossfs2_test_creds_process_file",
                 std::ios::out | std::ios::trunc);
  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "invalid");
  cred_file.close();

  LOG_INFO("Wait for 20 seconds for credential refresh");
  sleep(20);
  LOG_INFO("After 20 seconds, check result");

  // check timestamp
  time_t refresh_time_1 = parse_iso8601_time(read_file("timestamp.txt"));
  EXPECT_TRUE(refresh_time_1 > first_refresh);

  // remove expiration and will not refresh credential anymore
  cred_file.open("ossfs2_test_creds_process_file",
                 std::ios::out | std::ios::trunc);
  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret, "", "");
  cred_file.close();

  LOG_INFO("Wait for 20 seconds for credential refresh");
  sleep(20);
  LOG_INFO("After 20 seconds, check result");

  // check timestamp
  time_t refresh_time_2 = parse_iso8601_time(read_file("timestamp.txt"));
  EXPECT_TRUE(refresh_time_2 > refresh_time_1);

  LOG_INFO("Wait for 20 seconds for credential refresh again");
  sleep(20);
  LOG_INFO("After 20 seconds, check result");

  // refresh time should not change
  EXPECT_TRUE(parse_iso8601_time(read_file("timestamp.txt")) == refresh_time_2);
}
