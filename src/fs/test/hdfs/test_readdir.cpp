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
#include "fs/test/test_suite.h"

class Ossfs2HdfsReaddirTest : public OssHdfsTestSuite {
 protected:
  // readdir error path.
  void verify_readdir_call_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create a subdirectory.
    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0755, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    g_fault_injector->set_injection(FI_OssError_Call_Failed);
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    std::vector<TestInode> children;
    r = read_dir(dir_nodeid, children);
    ASSERT_EQ(r, -EIO);
  }

  // readdir error path (without call).
  void verify_readdir_failed_without_call() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create a subdirectory.
    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir2", 0755, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    g_fault_injector->set_injection(FI_OssError_Failed_Without_Call);
    DEFER(g_fault_injector->clear_injection(FI_OssError_Failed_Without_Call));

    std::vector<TestInode> children;
    r = read_dir(dir_nodeid, children);
    ASSERT_EQ(r, -EIO);
  }
};

TEST_F(Ossfs2HdfsReaddirTest, verify_readdir_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdir_call_fail();
}

TEST_F(Ossfs2HdfsReaddirTest, verify_readdir_failed_without_call) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdir_failed_without_call();
}
