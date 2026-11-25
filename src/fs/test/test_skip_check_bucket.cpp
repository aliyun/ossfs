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

#include "common/fault_injector.h"
#include "test_suite.h"

// These tests exercise the skip_check_bucket mount option end to end.
//
// The option only bypasses the mount-time bucket validation; periodic
// credential revalidation still probes the backend (OssFs::validate_creds is
// called with skip_validation=false on every refresh after mount). The
// mount-time check runs inside OssFs::init(), so each case mounts a real OssFs
// and uses FI_Check_Bucket_List_Not_Found to force check_bucket's ListObjects
// to report the bucket as missing. That injection lives only in
// OssStore::check_bucket, hence these cases are OSS-mode only.
class Ossfs2SkipCheckBucketTest : public Ossfs2TestSuite {
 protected:
  // Minimal credential_process payload carrying the real AK/SK, mirroring the
  // helper in test_credentials.cpp.
  static std::string format_creds(const std::string &ak,
                                  const std::string &sk) {
    return "{\"AccessKeyId\":\"" + ak + "\",\"AccessKeySecret\":\"" + sk +
           "\",\"SecurityToken\":\"\",\"Expiration\":\"\"}\n";
  }
};

// Baseline: without skip, a check_bucket error surfaces during mount and fails
// it, proving the mount path really consults check_bucket.
TEST_F(Ossfs2SkipCheckBucketTest, mount_fails_without_skip_on_check_error) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();

  g_fault_injector->set_injection(FI_Check_Bucket_List_Not_Found);
  OssFsOptions opts;
  opts.skip_check_bucket = false;
  EXPECT_NE(do_init(opts), 0);
}

// With skip enabled the static AK/SK mount path never calls check_bucket, so
// the same injected error is not reached and the mount succeeds.
TEST_F(Ossfs2SkipCheckBucketTest, mount_succeeds_with_skip_on_check_error) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();

  g_fault_injector->set_injection(FI_Check_Bucket_List_Not_Found);
  OssFsOptions opts;
  opts.skip_check_bucket = true;
  ASSERT_EQ(do_init(opts), 0);
}

// The creds_provider mount path must skip validation while still fetching
// credentials: do_refresh_creds(true, /*skip_validation=*/true) accepts the
// refreshed creds without probing the backend, so the injected check_bucket
// error never fires and the mount succeeds.
TEST_F(Ossfs2SkipCheckBucketTest,
       mount_succeeds_with_skip_via_credential_process) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();

  std::string cred_path = join_paths(test_path_, "creds_process_file");
  std::ofstream cred_file(cred_path);
  DEFER(unlink(cred_path.c_str()));
  cred_file << format_creds(FLAGS_oss_access_key_id,
                            FLAGS_oss_access_key_secret);
  cred_file.close();

  g_fault_injector->set_injection(FI_Check_Bucket_List_Not_Found);
  OssFsOptions opts;
  opts.credential_process = "/bin/cat " + cred_path;
  opts.skip_check_bucket = true;
  ASSERT_EQ(do_init(opts), 0);
}
