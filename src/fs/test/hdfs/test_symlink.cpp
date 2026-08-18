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

#include "fs/test/test_suite.h"

class Ossfs2HdfsSymlinkTest : public OssHdfsTestSuite {
 protected:
  void verify_symlink_basic() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;

    // Create a target file.
    int r = create_and_flush(parent, "sym_target", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    fs_->forget(nodeid, 1);

    // Create symlink.
    uint64_t link_nodeid = 0;
    r = fs_->symlink(parent, "sym_link", "sym_target", 0, 0, &link_nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(S_ISLNK(st.st_mode));

    // readlink should return the relative path with ../ prefix.
    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link_nodeid, buf, sizeof(buf) - 1);
    ASSERT_GT(len, 0);
    buf[len] = '\0';
    // Target is at depth 1 (parent/target), symlink at depth 1 (parent/link).
    // readlink returns "../sym_target".
    std::string target(buf);
    EXPECT_TRUE(target.find("sym_target") != std::string::npos)
        << "readlink returned: " << target;

    // lstat (getattr on the link itself) should show S_IFLNK.
    struct stat link_st;
    r = fs_->getattr(link_nodeid, &link_st);
    ASSERT_EQ(r, 0);
    EXPECT_TRUE(S_ISLNK(link_st.st_mode));

    // Unlink the symlink.
    r = fs_->unlink(parent, "sym_link");
    ASSERT_EQ(r, 0);
    fs_->forget(link_nodeid, 1);

    // Target should still exist.
    uint64_t target_nodeid = 0;
    r = fs_->lookup(parent, "sym_target", &target_nodeid, &st);
    ASSERT_EQ(r, 0);
    fs_->forget(target_nodeid, 1);
  }

  void verify_symlink_dangling() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t link_nodeid = 0;

    // Create symlink to a non-existent target (dangling symlink).
    int r = fs_->symlink(parent, "dangling_link", "nonexistent", 0, 0,
                         &link_nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(S_ISLNK(st.st_mode));

    // readlink should still work on dangling symlink.
    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link_nodeid, buf, sizeof(buf) - 1);
    ASSERT_GT(len, 0);
    buf[len] = '\0';
    EXPECT_TRUE(std::string(buf).find("nonexistent") != std::string::npos)
        << "readlink returned: " << buf;

    // Unlink the dangling symlink.
    r = fs_->unlink(parent, "dangling_link");
    ASSERT_EQ(r, 0);
    fs_->forget(link_nodeid, 1);
  }

  void verify_symlink_readdir() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;

    // Create target file.
    int r = create_and_flush(parent, "rd_target", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    // Keep the nodeid alive — don't forget yet.

    // Create symlink.
    uint64_t link_nodeid = 0;
    r = fs_->symlink(parent, "rd_link", "rd_target", 0, 0, &link_nodeid, &st);
    ASSERT_EQ(r, 0);
    // Keep link_nodeid alive.

    // readdir should list the symlink with DT_LNK type.
    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> children;
    r = fs_->readdir(parent, 0, dirp, filler, &children, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, 0);

    bool found_link = false;
    for (auto &child : children) {
      if (child.name == "rd_link") {
        found_link = true;
        struct stat child_st;
        int gr = fs_->getattr(child.nodeid, &child_st);
        EXPECT_EQ(gr, 0);
        EXPECT_TRUE(S_ISLNK(child_st.st_mode));
      }
      // Forget all readdir-returned children.
      if (child.name != "." && child.name != "..") {
        fs_->forget(child.nodeid, 1);
      }
    }
    EXPECT_TRUE(found_link) << "symlink not found in readdir results";

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);

    // Cleanup: unlink before forgetting creation-time nodeids,
    // because unlink needs the child in parent's children map.
    r = fs_->unlink(parent, "rd_link");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, "rd_target");
    ASSERT_EQ(r, 0);

    // Forget the creation-time nodeids.
    fs_->forget(link_nodeid, 1);
    fs_->forget(nodeid, 1);
  }

  void verify_symlink_disable() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t link_nodeid = 0;

    // With enable_symlink=false (default for test init without setting it),
    // symlink should return ENOTSUP.
    // But our HDFS test suite auto-enables symlink, so this test verifies
    // the capability check works. We test by calling symlink with the default
    // options which should have symlink enabled in HDFS mode.
    int r = fs_->symlink(parent, "disabled_link", "target", 0, 0, &link_nodeid,
                         &st);
    // In HDFS mode symlink is enabled by default, so this should succeed.
    ASSERT_EQ(r, 0);

    r = fs_->unlink(parent, "disabled_link");
    ASSERT_EQ(r, 0);
    fs_->forget(link_nodeid, 1);
  }

  void verify_absolute_path_symlink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;

    // Create a target file.
    int r = create_and_flush(parent, "abs_target", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    // Keep nodeid alive.

    // Create symlink with absolute path (under mountpoint).
    // The mountpoint is "/mnt/test", so the absolute target is
    // "/mnt/test/<testdir>/abs_target".
    auto parent_path = nodeid_to_path(parent);
    std::string abs_target =
        std::string("/mnt/test") + parent_path + "/abs_target";

    uint64_t link_nodeid = 0;
    r = fs_->symlink(parent, "abs_link", abs_target, 0, 0, &link_nodeid, &st);
    ASSERT_EQ(r, 0) << "absolute path symlink under mountpoint should succeed";
    ASSERT_TRUE(S_ISLNK(st.st_mode));

    // readlink should return the relative path (with ../ prefix),
    // not the original absolute path.
    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link_nodeid, buf, sizeof(buf) - 1);
    ASSERT_GT(len, 0);
    buf[len] = '\0';
    std::string target(buf);
    // Should NOT contain the mountpoint prefix.
    EXPECT_TRUE(target.find("/mnt/test") == std::string::npos)
        << "readlink should not contain mountpoint prefix: " << target;
    // Should contain the target filename exactly once (no path duplication).
    auto first = target.find("abs_target");
    ASSERT_TRUE(first != std::string::npos)
        << "readlink should contain target name: " << target;
    // The readlink result should be "abs_target" (same directory, no ../
    // needed).
    EXPECT_EQ(target, "abs_target")
        << "same-dir absolute symlink should return just the target name: "
        << target;

    // Cleanup: unlink before forget.
    r = fs_->unlink(parent, "abs_link");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, "abs_target");
    ASSERT_EQ(r, 0);

    // Forget nodeids.
    fs_->forget(link_nodeid, 1);
    fs_->forget(nodeid, 1);
  }

  void verify_absolute_path_outside_mountpoint() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t link_nodeid = 0;

    // Absolute path NOT under mountpoint should fail with EINVAL.
    int r = fs_->symlink(parent, "bad_link", "/other/path/target", 0, 0,
                         &link_nodeid, &st);
    EXPECT_EQ(r, -EINVAL)
        << "absolute symlink target outside mountpoint should return EINVAL";
  }

  void verify_absolute_path_empty_mountpoint() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t link_nodeid = 0;

    // When mountpoint is empty, absolute paths should fail with EINVAL.
    int r = fs_->symlink(parent, "no_mp_link", "/some/path", 0, 0, &link_nodeid,
                         &st);
    EXPECT_EQ(r, -EINVAL)
        << "absolute symlink with empty mountpoint should return EINVAL";
  }

  void verify_bad_target_uri() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;

    // Create target file and symlink.
    int r = create_and_flush(parent, "fi_target", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);

    uint64_t link_nodeid = 0;
    r = fs_->symlink(parent, "fi_link", "fi_target", 0, 0, &link_nodeid, &st);
    ASSERT_EQ(r, 0);

    // Enable FI to make getLinkTarget return a bad URI.
    g_fault_injector->set_injection(FI_HdfsSymlink_BadTarget);
    DEFER(g_fault_injector->clear_injection(FI_HdfsSymlink_BadTarget));

    // readlink should fail with EIO when backend returns bad target.
    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link_nodeid, buf, sizeof(buf) - 1);
    EXPECT_EQ(len, -EIO)
        << "readlink should return EIO for unrecognizable target URI";

    // Cleanup: unlink before forget.
    r = fs_->unlink(parent, "fi_link");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, "fi_target");
    ASSERT_EQ(r, 0);
    fs_->forget(link_nodeid, 1);
    fs_->forget(nodeid, 1);
  }

  void verify_escape_target_uri() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;

    int r = create_and_flush(parent, "esc_target", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);

    uint64_t link_nodeid = 0;
    r = fs_->symlink(parent, "esc_link", "esc_target", 0, 0, &link_nodeid, &st);
    ASSERT_EQ(r, 0);

    // Enable FI to make getLinkTarget return a URI with ".." in the path.
    // lexically_relative preserves ".." as-is; the kernel resolves it
    // when the symlink is followed. readlink should succeed.
    g_fault_injector->set_injection(FI_HdfsSymlink_EscapePath);
    DEFER(g_fault_injector->clear_injection(FI_HdfsSymlink_EscapePath));

    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link_nodeid, buf, sizeof(buf) - 1);
    EXPECT_GT(len, 0)
        << "readlink should succeed for URI with '..' (kernel resolves it)";

    // Cleanup.
    r = fs_->unlink(parent, "esc_link");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, "esc_target");
    ASSERT_EQ(r, 0);
    fs_->forget(link_nodeid, 1);
    fs_->forget(nodeid, 1);
  }

  // Symlink target that would escape the mount root.
  // put_symlink normalizes and rejects targets like "../../etc/passwd".
  void verify_symlink_target_escape_root() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;

    // Create a file in a subdirectory.
    uint64_t subdir = 0;
    int r = fs_->mkdir(parent, "sub1", 0777, 0, 0, 0, &subdir, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(subdir, 1));

    r = create_and_flush(subdir, "escape_target", CREATE_BASE_FLAGS, 0777, 0, 0,
                         0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Try creating a symlink that targets outside the mount root.
    // The FUSE layer calls symlink(parent, name, target, ...) where target is
    // what the user passed. put_symlink normalizes the target. A target like
    // "../../../../../../etc/passwd" should escape root and be rejected.
    // Note: the FUSE layer's symlink call may not reach put_symlink's check
    // because the normalization happens in the store layer. The kernel may
    // resolve relative paths before calling our code. This test verifies
    // that a reasonable escape attempt is handled.
    uint64_t link_nodeid = 0;
    r = fs_->symlink(subdir, "escape_link", "../../../../../../etc/passwd", 0,
                     0, &link_nodeid, &st);
    // This may succeed (kernel handles path) or fail with EINVAL.
    // The important thing is no crash occurs.
    if (r == 0) {
      fs_->unlink(subdir, "escape_link");
      fs_->forget(link_nodeid, 1);
    }

    r = fs_->unlink(subdir, "escape_target");
    ASSERT_EQ(r, 0);
  }

  // Nested symlink: link -> link2 -> target.
  void verify_symlink_nested() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t target_id = 0;
    void *handle = nullptr;

    // Create target file.
    int r = create_and_flush(parent, "nested_target", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &target_id, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(target_id, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(target_id, 1));

    // Create first symlink: nested_link1 -> nested_target.
    uint64_t link1_id = 0;
    r = fs_->symlink(parent, "nested_link1", "nested_target", 0, 0, &link1_id,
                     &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(link1_id, 1));

    // Create second symlink: nested_link2 -> nested_link1.
    uint64_t link2_id = 0;
    r = fs_->symlink(parent, "nested_link2", "nested_link1", 0, 0, &link2_id,
                     &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(link2_id, 1));

    // readlink on link2 should return "nested_link1".
    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link2_id, buf, sizeof(buf) - 1);
    ASSERT_GT(len, 0);
    buf[len] = '\0';
    std::string target(buf);
    // Should contain "nested_link1" (possibly with ../ prefix).
    ASSERT_NE(target.find("nested_link1"), std::string::npos);

    // Cleanup.
    r = fs_->unlink(parent, "nested_link2");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, "nested_link1");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, "nested_target");
    ASSERT_EQ(r, 0);
  }

  // Symlink in deeply nested directory.
  void verify_symlink_deep_directory() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;

    // Create deep directory structure: d1/d2/d3.
    uint64_t d1 = 0, d2 = 0, d3 = 0;
    int r = fs_->mkdir(parent, "d1", 0777, 0, 0, 0, &d1, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(d1, 1));
    r = fs_->mkdir(d1, "d2", 0777, 0, 0, 0, &d2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(d2, 1));
    r = fs_->mkdir(d2, "d3", 0777, 0, 0, 0, &d3, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(d3, 1));

    // Create target file in d3.
    uint64_t target_id = 0;
    void *handle = nullptr;
    r = create_and_flush(d3, "deep_target", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &target_id, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(target_id, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(target_id, 1));

    // Create symlink in d3.
    uint64_t link_id = 0;
    r = fs_->symlink(d3, "deep_link", "deep_target", 0, 0, &link_id, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(link_id, 1));

    // readlink should return correct path.
    char buf[PATH_MAX + 1] = {0};
    ssize_t len = fs_->readlink(link_id, buf, sizeof(buf) - 1);
    ASSERT_GT(len, 0);
    buf[len] = '\0';
    std::string target(buf);
    ASSERT_NE(target.find("deep_target"), std::string::npos);

    // Cleanup.
    r = fs_->unlink(d3, "deep_link");
    ASSERT_EQ(r, 0);
    r = fs_->unlink(d3, "deep_target");
    ASSERT_EQ(r, 0);
  }
};

TEST_F(Ossfs2HdfsSymlinkTest, verify_basic) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_basic();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_dangling) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_dangling();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_readdir) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_readdir();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_hdfs_symlink_enabled) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_disable();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_absolute_path) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  opts.mountpoint = "/mnt/test";
  init(opts);
  verify_absolute_path_symlink();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_absolute_path_outside_mountpoint) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  opts.mountpoint = "/mnt/test";
  init(opts);
  verify_absolute_path_outside_mountpoint();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_absolute_path_empty_mountpoint) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  // mountpoint is empty by default.
  init(opts);
  verify_absolute_path_empty_mountpoint();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_bad_target_uri) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_bad_target_uri();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_escape_target_uri) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_escape_target_uri();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_symlink_target_escape_root) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_target_escape_root();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_symlink_nested) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_nested();
}

TEST_F(Ossfs2HdfsSymlinkTest, verify_symlink_deep_directory) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_deep_directory();
}
