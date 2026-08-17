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

#include <sys/xattr.h>

#include "fs/test/test_suite.h"

class Ossfs2HdfsXattrTest : public OssHdfsTestSuite {
 protected:
  // Helper to create a file.
  void create_test_file(uint64_t parent, const char *name, uint64_t &nodeid,
                        void *&handle) {
    struct stat st;
    int r = create_and_flush(parent, name, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
  }

  // Basic setxattr + getxattr.
  void verify_setxattr_getxattr_basic() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_basic", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.test_key";
    const char *value = "test_value";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    // Read back.
    char buf[64] = {};
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_EQ(r, (int)strlen(value));
    ASSERT_EQ(std::string(buf, r), "test_value");

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // XATTR_CREATE: fail if already exists.
  void verify_setxattr_create_flag() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_create", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.create_key";
    const char *value = "first";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), XATTR_CREATE);
    ASSERT_EQ(r, 0);

    // Second XATTR_CREATE should fail.
    r = fs_->setxattr(nodeid, name, value, strlen(value), XATTR_CREATE);
    ASSERT_NE(r, 0);  // -EEXIST or similar

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // XATTR_REPLACE: fail if doesn't exist.
  void verify_setxattr_replace_flag() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_replace", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.replace_key";
    const char *value = "original";

    // REPLACE on non-existent -> should fail.
    int r = fs_->setxattr(nodeid, name, value, strlen(value), XATTR_REPLACE);
    ASSERT_NE(r, 0);  // -ENOATTR or similar

    // Create it first.
    r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    // REPLACE on existing -> should succeed.
    const char *new_value = "replaced";
    r = fs_->setxattr(nodeid, name, new_value, strlen(new_value),
                      XATTR_REPLACE);
    ASSERT_EQ(r, 0);

    // Verify new value.
    char buf[64] = {};
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_EQ(r, (int)strlen(new_value));
    ASSERT_EQ(std::string(buf, r), "replaced");

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // listxattr: list all xattr names.
  void verify_listxattr() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_list", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set multiple xattrs.
    fs_->setxattr(nodeid, "user.key1", "val1", 4, 0);
    fs_->setxattr(nodeid, "user.key2", "val2", 4, 0);
    fs_->setxattr(nodeid, "user.key3", "val3", 4, 0);

    // List all xattrs.
    char list[256] = {};
    int r = fs_->listxattr(nodeid, list, sizeof(list));
    ASSERT_GE(r, 0);

    // Verify all keys appear in the list.
    std::string list_str(list, r);
    ASSERT_NE(list_str.find("user.key1"), std::string::npos);
    ASSERT_NE(list_str.find("user.key2"), std::string::npos);
    ASSERT_NE(list_str.find("user.key3"), std::string::npos);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // removexattr: remove an xattr.
  void verify_removexattr() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_remove", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.remove_key";
    const char *value = "to_remove";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    // Verify it exists.
    char buf[32] = {};
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_EQ(r, (int)strlen(value));

    // Remove it.
    r = fs_->removexattr(nodeid, name);
    ASSERT_EQ(r, 0);

    // Verify it's gone.
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_LT(r, 0);  // -ENOATTR or similar

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // xattr persists after close/reopen.
  void verify_xattr_persistence() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_persist", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.persist_key";
    const char *value = "persistent_value";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    // Close and reopen.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    void *handle2 = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &handle2, &keep_cache);
    ASSERT_EQ(r, 0);

    // Verify xattr persists.
    char buf[64] = {};
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_EQ(r, (int)strlen(value));
    ASSERT_EQ(std::string(buf, r), "persistent_value");

    r = fs_->release(nodeid, get_file_from_handle(handle2));
    ASSERT_EQ(r, 0);
  }

  // xattr survives rename.
  void verify_xattr_rename_survival() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_rename_src", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.rename_key";
    const char *value = "survive_rename";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Rename the file.
    r = fs_->rename(parent, "xattr_rename_src", parent, "xattr_rename_dst", 0);
    ASSERT_EQ(r, 0);

    // Verify xattr still exists.
    char buf[64] = {};
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_EQ(r, (int)strlen(value));
    ASSERT_EQ(std::string(buf, r), "survive_rename");
  }

  // XATTR_CREATE on existing key -> -EEXIST.
  void verify_setxattr_create_flag_on_existing() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_create_exist", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.create_exist";
    const char *value = "original";
    // Create the xattr first.
    int r = fs_->setxattr(nodeid, name, value, strlen(value), XATTR_CREATE);
    ASSERT_EQ(r, 0);

    // XATTR_CREATE again on existing key -> -EEXIST.
    r = fs_->setxattr(nodeid, name, value, strlen(value), XATTR_CREATE);
    ASSERT_EQ(r, -EEXIST);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // XATTR_REPLACE on missing key -> -ENODATA.
  void verify_setxattr_replace_flag_on_missing() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_replace_miss", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.replace_missing";
    const char *value = "should_fail";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), XATTR_REPLACE);
    ASSERT_EQ(r, -ENODATA);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // Invalid flags (CREATE|REPLACE = 3) -> -EINVAL.
  void verify_setxattr_invalid_flags() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_invalid_flags", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.invalid_flags";
    const char *value = "test";
    // flags=3 (XATTR_CREATE|XATTR_REPLACE) is semantically contradictory.
    int r = fs_->setxattr(nodeid, name, value, strlen(value),
                          XATTR_CREATE | XATTR_REPLACE);
    ASSERT_EQ(r, -EINVAL);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // Name too long (>= 255 chars) -> -ERANGE.
  void verify_setxattr_name_too_long() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_long_name", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Build a name that is 255 chars long (>= kMaxXattrNameSize).
    std::string long_name(255, 'a');
    const char *value = "test";
    int r = fs_->setxattr(nodeid, long_name.c_str(), value, strlen(value), 0);
    ASSERT_EQ(r, -ERANGE);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // Value too large (>= 65536 bytes) -> -ERANGE.
  void verify_setxattr_value_too_large() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_large_val", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.large_val";
    // Build a value that is 65536 bytes (>= kMaxXattrValueSize).
    std::string large_value(65536, 'x');
    int r =
        fs_->setxattr(nodeid, name, large_value.c_str(), large_value.size(), 0);
    ASSERT_EQ(r, -ERANGE);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // getxattr with size=0 returns value length.
  void verify_getxattr_size_query() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_size_query", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.size_query";
    const char *value = "hello_world";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    // size=0 should return the value length without copying.
    r = fs_->getxattr(nodeid, name, nullptr, 0);
    ASSERT_EQ(r, (int)strlen(value));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // getxattr with buffer smaller than value -> -ERANGE.
  void verify_getxattr_buffer_too_small() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_small_buf", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    const char *name = "user.small_buf";
    const char *value = "a_long_value_string";
    int r = fs_->setxattr(nodeid, name, value, strlen(value), 0);
    ASSERT_EQ(r, 0);

    // Buffer smaller than value -> -ERANGE.
    char buf[4] = {};
    r = fs_->getxattr(nodeid, name, buf, sizeof(buf));
    ASSERT_EQ(r, -ERANGE);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // getxattr on non-existent key -> -ENODATA.
  void verify_getxattr_not_found() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_not_found", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    char buf[64] = {};
    int r = fs_->getxattr(nodeid, "user.nonexistent", buf, sizeof(buf));
    ASSERT_EQ(r, -ENODATA);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // listxattr with size=0 returns total name length.
  void verify_listxattr_size_query() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_list_query", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set two xattrs.
    fs_->setxattr(nodeid, "user.k1", "v1", 2, 0);
    fs_->setxattr(nodeid, "user.k2", "v2", 2, 0);

    // size=0 should return total bytes needed.
    int r = fs_->listxattr(nodeid, nullptr, 0);
    ASSERT_GT(r, 0);
    // Expected: "user.k1\0" (8) + "user.k2\0" (8) = 16 bytes.
    ASSERT_EQ(r, 16);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // listxattr with buffer too small -> -ERANGE.
  void verify_listxattr_buffer_too_small() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_list_small", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    fs_->setxattr(nodeid, "user.k1", "v1", 2, 0);
    fs_->setxattr(nodeid, "user.k2", "v2", 2, 0);

    // Buffer too small -> -ERANGE.
    char buf[4] = {};
    int r = fs_->listxattr(nodeid, buf, sizeof(buf));
    ASSERT_EQ(r, -ERANGE);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // removexattr on non-existent key -> -ENODATA.
  void verify_removexattr_not_found() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "xattr_rm_missing", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    int r = fs_->removexattr(nodeid, "user.does_not_exist");
    ASSERT_EQ(r, -ENODATA);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }
};

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_getxattr_basic) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_getxattr_basic();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_create_flag) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_create_flag();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_replace_flag) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_replace_flag();
}

TEST_F(Ossfs2HdfsXattrTest, verify_listxattr) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_listxattr();
}

TEST_F(Ossfs2HdfsXattrTest, verify_removexattr) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_removexattr();
}

TEST_F(Ossfs2HdfsXattrTest, verify_xattr_persistence) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_xattr_persistence();
}

TEST_F(Ossfs2HdfsXattrTest, verify_xattr_rename_survival) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_xattr_rename_survival();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_create_flag_on_existing) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_create_flag_on_existing();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_replace_flag_on_missing) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_replace_flag_on_missing();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_invalid_flags) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_invalid_flags();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_name_too_long) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_name_too_long();
}

TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_value_too_large) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setxattr_value_too_large();
}

TEST_F(Ossfs2HdfsXattrTest, verify_getxattr_size_query) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_getxattr_size_query();
}

TEST_F(Ossfs2HdfsXattrTest, verify_getxattr_buffer_too_small) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_getxattr_buffer_too_small();
}

TEST_F(Ossfs2HdfsXattrTest, verify_getxattr_not_found) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_getxattr_not_found();
}

TEST_F(Ossfs2HdfsXattrTest, verify_listxattr_size_query) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_listxattr_size_query();
}

TEST_F(Ossfs2HdfsXattrTest, verify_listxattr_buffer_too_small) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_listxattr_buffer_too_small();
}

TEST_F(Ossfs2HdfsXattrTest, verify_removexattr_not_found) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_removexattr_not_found();
}

// setxattr error path.
TEST_F(Ossfs2HdfsXattrTest, verify_setxattr_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  void *handle = nullptr;
  create_test_file(parent, "setxattr_fail", nodeid, handle);
  DEFER(fs_->forget(nodeid, 1));
  if (handle) fs_->release(nodeid, get_file_from_handle(handle));

  g_fault_injector->set_injection(FI_OssError_Call_Failed);
  DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));
  const char *value = "test_value";
  int r = fs_->setxattr(nodeid, "user.test", value, strlen(value), 0);
  ASSERT_EQ(r, -EIO);
}

// removexattr error path.
TEST_F(Ossfs2HdfsXattrTest, verify_removexattr_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  void *handle = nullptr;
  create_test_file(parent, "removexattr_fail", nodeid, handle);
  DEFER(fs_->forget(nodeid, 1));
  if (handle) fs_->release(nodeid, get_file_from_handle(handle));

  // First set an xattr.
  const char *value = "test_value";
  int r = fs_->setxattr(nodeid, "user.test", value, strlen(value), 0);
  ASSERT_EQ(r, 0);

  g_fault_injector->set_injection(FI_OssError_Call_Failed);
  DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));
  r = fs_->removexattr(nodeid, "user.test");
  ASSERT_EQ(r, -EIO);
}
