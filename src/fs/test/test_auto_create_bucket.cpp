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

// These tests exercise the auto_create_bucket mount option end to end against
// a real OSS endpoint. They create a fresh bucket and delete it afterwards, so
// the credentials must be granted PutBucket / DeleteBucket permissions.
class Ossfs2AutoCreateBucketTest : public Ossfs2TestSuite {
 protected:
  // Generate a globally-unique, valid OSS bucket name (3-63 chars, lowercase
  // letters/digits/hyphen, starts and ends with an alphanumeric char).
  // Use a nanosecond-precision timestamp so concurrently running tests do not
  // collide on the same bucket name (a second-granularity suffix can be shared
  // by tests started within the same second).
  static std::string gen_bucket_name() {
    static const char cs[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    static std::mt19937 rng(std::random_device{}());
    std::string suffix;
    for (int i = 0; i < 20; i++) suffix += cs[rng() % (sizeof(cs) - 1)];
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t now_ns =
        static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
    return "ossfs2-ac-" + suffix + "-" + std::to_string(now_ns);
  }

  OssStore *make_store(const std::string &bucket, bool auto_create,
                       const std::string &agentic = "") {
    ObjStoreOptions options;
    options.endpoint = FLAGS_oss_endpoint;
    options.bucket = bucket;
    options.request_timeout_us = FLAGS_oss_request_timeout_ms * 1000;
    options.user_agent = "Ossfs2AutoCreateBucketTest/1.0";
    options.auto_create_bucket = auto_create;
    options.agentic_bucket = agentic;
    auto auth = new_basic_oss_authenticator(
        {FLAGS_oss_access_key_id, FLAGS_oss_access_key_secret});
    return new OssStore(options, auth);
  }
};

TEST_F(Ossfs2AutoCreateBucketTest, verify_auto_create_and_delete) {
  INIT_PHOTON();

  const std::string bucket = gen_bucket_name();
  LOG_INFO("auto create bucket test uses bucket `", bucket);

  // Best-effort cleanup: delete the bucket even if an assertion fails midway.
  bool created = false;
  DEFER({
    if (created) {
      auto cleanup = make_store(bucket, false);
      int dr = cleanup->delete_bucket();
      LOG_INFO("cleanup delete_bucket returned `", dr);
      delete cleanup;
    }
  });

  // 1. Bucket does not exist and auto_create is disabled: check_bucket reports
  //    the bucket as missing.
  {
    auto store = make_store(bucket, false);
    DEFER(delete store);
    EXPECT_EQ(store->check_bucket(), -ENOENT);
  }

  // 2. auto_create enabled: check_bucket creates the bucket and succeeds.
  {
    auto store = make_store(bucket, true, "agentic-test-bucket");
    DEFER(delete store);
    int r = store->check_bucket();
    ASSERT_EQ(r, 0) << "auto create bucket failed, errno=" << -r;
    created = true;
  }

  // 3. The bucket now exists: check_bucket succeeds even without auto_create.
  {
    auto store = make_store(bucket, false);
    DEFER(delete store);
    EXPECT_EQ(store->check_bucket(), 0);
  }

  // 4. delete_bucket removes it and check_bucket reports it missing again.
  {
    auto store = make_store(bucket, false);
    DEFER(delete store);
    ASSERT_EQ(store->delete_bucket(), 0);
    created = false;
    EXPECT_EQ(store->check_bucket(), -ENOENT);
  }
}

TEST_F(Ossfs2AutoCreateBucketTest, verify_auto_create_idempotent_on_existing) {
  INIT_PHOTON();

  const std::string bucket = gen_bucket_name();
  LOG_INFO("auto create idempotent test uses bucket `", bucket);

  bool created = false;
  DEFER({
    if (created) {
      auto cleanup = make_store(bucket, false);
      int dr = cleanup->delete_bucket();
      LOG_INFO("cleanup delete_bucket returned `", dr);
      delete cleanup;
    }
  });

  // First auto_create call creates the bucket.
  {
    auto store = make_store(bucket, true);
    DEFER(delete store);
    ASSERT_EQ(store->check_bucket(), 0);
    created = true;
  }

  // Second auto_create call against the now-existing bucket is a no-op: the
  // initial list succeeds, so no PutBucket is issued and it still returns 0.
  {
    auto store = make_store(bucket, true);
    DEFER(delete store);
    EXPECT_EQ(store->check_bucket(), 0);
  }
}

// For an already-existing bucket, fault-inject the initial ListObjects to
// report the bucket as missing. check_bucket then issues PutBucket against the
// existing bucket, which the server rejects with HTTP 409 -> -ENOTSUP. That
// case is treated as success, so check_bucket still returns 0. This exercises
// the ENOTSUP handling branch without relying on a real missing bucket.
TEST_F(Ossfs2AutoCreateBucketTest, verify_existing_bucket_put_conflict_is_ok) {
  INIT_PHOTON();

  auto store = make_store(FLAGS_oss_bucket, true, "agentic-test-bucket");
  DEFER(delete store);

  // Fire once: only the initial list gets a synthetic -ENOENT, driving
  // check_bucket into the auto-create branch against the existing bucket.
  g_fault_injector->set_injection(FI_Check_Bucket_List_Not_Found,
                                  FaultInjection{1, 0});
  EXPECT_EQ(store->check_bucket(), 0);
}
