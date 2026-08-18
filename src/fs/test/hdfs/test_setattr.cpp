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
#include "oss/oss_hdfs_store.h"

class Ossfs2HdfsSetattrTest : public OssHdfsTestSuite {
 protected:
  void SetUp() override {
    OssHdfsTestSuite::SetUp();
    // Inject uid/gid <-> username/groupname mapping for tests that use
    // arbitrary uid/gid values not present on the system.
    using namespace OssFileSystem;
    g_test_user_mapping.uid_to_name[500] = "test_user_500";
    g_test_user_mapping.name_to_uid["test_user_500"] = 500;
    g_test_user_mapping.uid_to_name[1000] = "test_user_1000";
    g_test_user_mapping.name_to_uid["test_user_1000"] = 1000;
    g_test_user_mapping.uid_to_name[2000] = "test_user_2000";
    g_test_user_mapping.name_to_uid["test_user_2000"] = 2000;
    g_test_user_mapping.gid_to_name[600] = "test_group_600";
    g_test_user_mapping.name_to_gid["test_group_600"] = 600;
    g_test_user_mapping.gid_to_name[1000] = "test_group_1000";
    g_test_user_mapping.name_to_gid["test_group_1000"] = 1000;
    g_test_user_mapping.gid_to_name[2000] = "test_group_2000";
    g_test_user_mapping.name_to_gid["test_group_2000"] = 2000;
    // Simulate supplementary group membership:
    // test_user_1000 (uid=1000) is a member of test_group_2000 (gid=2000).
    g_test_user_mapping.group_members[2000] = {"test_user_1000"};
    if (g_fault_injector)
      g_fault_injector->set_injection(FI_Hdfs_UserGroup_Mapping, {});
  }

  void TearDown() override {
    if (g_fault_injector)
      g_fault_injector->clear_injection(FI_Hdfs_UserGroup_Mapping);
    g_test_user_mapping = {};
    OssHdfsTestSuite::TearDown();
  }
  // Helper to create a file and return nodeid + handle.
  void create_test_file(uint64_t parent, const char *name, uint64_t &nodeid,
                        void *&handle) {
    struct stat st;
    int r = create_and_flush(parent, name, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
  }

  // setattr: change mode (chmod).
  void verify_setattr_mode() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "mode_file", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set mode to 0644.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0644;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);
    ASSERT_EQ(r, 0);

    // Verify via getattr.
    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_mode & 07777, (mode_t)0644);

    // Set mode to 0755.
    st.st_mode = 0755;
    r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);
    ASSERT_EQ(r, 0);

    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_mode & 07777, (mode_t)0755);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // setattr: change uid/gid (chown).
  void verify_setattr_uid_gid() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "chown_file", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set uid=1000, gid=2000.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    st.st_gid = 2000;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    ASSERT_EQ(r, 0);

    // Verify.
    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_uid, (uid_t)1000);
    ASSERT_EQ(st2.st_gid, (gid_t)2000);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // chown: unresolvable uid/gid validation.
  // - Both unresolvable → EINVAL (user explicitly requested both, neither can
  //   be resolved to local username/groupname)
  // - One unresolvable, one resolvable → OK (HDFS treats "" as "don't change")
  // - -1 ("don't change") → always OK (no resolution needed)
  void verify_chown_unresolvable_uid_gid() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "chown_unresolvable", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Both unresolvable -> EINVAL.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 9999;
    st.st_gid = 9999;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    ASSERT_EQ(r, -EINVAL);

    // uid resolvable but gid not -> OK (HDFS keeps old gid).
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;  // mapped via FI
    st.st_gid = 9999;  // NOT mapped -> "" -> HDFS no-op for gid
    r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    ASSERT_EQ(r, 0);

    // gid resolvable but uid not -> OK (HDFS keeps old uid).
    memset(&st, 0, sizeof(st));
    st.st_uid = 9999;  // NOT mapped -> "" -> HDFS no-op for uid
    st.st_gid = 1000;  // mapped via FI
    r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // setattr: change atime (utimensat).
  void verify_setattr_atime() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "atime_file", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_atim.tv_sec = 1700000000;
    st.st_atim.tv_nsec = 0;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_ATIME);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_atim.tv_sec, (time_t)1700000000);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // setattr: change mtime.
  void verify_setattr_mtime() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "mtime_file", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mtim.tv_sec = 1600000000;
    st.st_mtim.tv_nsec = 0;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MTIME);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_mtim.tv_sec, (time_t)1600000000);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // setattr: change size (non-zero).
  void verify_setattr_size_nonzero() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "size_file", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Write 8KB.
    const size_t buf_size = 8192;
    char *buf = new char[buf_size];
    DEFER(delete[] buf);
    memset(buf, 'Z', buf_size);
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(buf, buf_size, 0);
    ASSERT_EQ(w, (ssize_t)buf_size);

    // Truncate to 1KB.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_size = 1024;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_size, (off_t)1024);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // setattr: combined mode + uid + gid.
  void verify_setattr_combined() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "combined_file", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0600;
    st.st_uid = 500;
    st.st_gid = 600;
    int r = fs_->setattr(
        nodeid, &st,
        FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_mode & 07777, (mode_t)0600);
    ASSERT_EQ(st2.st_uid, (uid_t)500);
    ASSERT_EQ(st2.st_gid, (gid_t)600);

    // Also set mtime + atime in a combined call.
    struct stat st3;
    memset(&st3, 0, sizeof(st3));
    st3.st_atim.tv_sec = 1500000000;
    st3.st_mtim.tv_sec = 1550000000;
    r = fs_->setattr(nodeid, &st3, FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME);
    ASSERT_EQ(r, 0);

    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_atim.tv_sec, (time_t)1500000000);
    ASSERT_EQ(st2.st_mtim.tv_sec, (time_t)1550000000);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // Verify HDFS-specific stat attributes: blksize=512, dir size=4096.
  void verify_stat_attributes() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Verify parent directory stat.
    struct stat dir_st;
    int r = fs_->getattr(parent, &dir_st);
    ASSERT_EQ(r, 0);
    EXPECT_TRUE(S_ISDIR(dir_st.st_mode));
    EXPECT_EQ(dir_st.st_size, (off_t)4096)
        << "directory st_size should be 4096 in HDFS mode";
    EXPECT_EQ(dir_st.st_blksize, (blksize_t)512)
        << "directory st_blksize should be 512 in HDFS mode";

    // Create a file and verify its stat.
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "stat_test_file", nodeid, handle);
    DEFER({
      fs_->release(nodeid, get_file_from_handle(handle));
      fs_->forget(nodeid, 1);
    });

    struct stat file_st;
    r = fs_->getattr(nodeid, &file_st);
    ASSERT_EQ(r, 0);
    EXPECT_TRUE(S_ISREG(file_st.st_mode));
    EXPECT_EQ(file_st.st_blksize, (blksize_t)512)
        << "file st_blksize should be 512 in HDFS mode";
    EXPECT_EQ(file_st.st_size, (off_t)0) << "empty file st_size should be 0";
  }

  // access with F_OK mask -> always returns 0.
  void verify_access_f_ok() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_f_ok", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // F_OK should always succeed regardless of uid/gid.
    int r = fs_->access(nodeid, F_OK, 1000, 1000);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: root (uid=0) can read/write anything.
  void verify_access_root_user() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_root", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set restrictive mode 0000.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0000;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);

    // Root can still read and write.
    int r = fs_->access(nodeid, R_OK, 0, 0);
    ASSERT_EQ(r, 0);
    r = fs_->access(nodeid, W_OK, 0, 0);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: HDFS has no execute bit concept; root is unconditionally exempt.
  void verify_access_root_execute_check() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_root_x", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set mode without any execute bits.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0644;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);

    // Root X_OK succeeds in HDFS mode (no execute bit concept).
    int r = fs_->access(nodeid, X_OK, 0, 0);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: owner permissions check.
  void verify_access_owner_permissions() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_owner", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set mode 0700 (owner rwx, no group/other).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0700;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);

    // Owner uid=0 (default in HDFS tests) should have full access.
    int r = fs_->access(nodeid, R_OK | W_OK | X_OK, 0, 0);
    ASSERT_EQ(r, 0);

    // Non-owner uid=1000 should be denied (falls to other: 000).
    r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, -EACCES);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: group permissions check.
  void verify_access_group_permissions() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_group", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set mode 0070 (group rwx only).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0070;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);

    // Group member (uid=1000, gid=0 matching file's group) should have access.
    // Note: in HDFS tests, the file's gid defaults to kReservedUnresolvedGid.
    // We need to set the gid first to test group matching.
    st.st_gid = 1000;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_GID);

    // uid=1000 is not owner (uid=0/kReservedUnresolvedUid), but gid=1000
    // matches -> group permissions apply.
    int r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: other permissions check.
  void verify_access_other_permissions() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_other", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set mode 0007 (other rwx only).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0007;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);

    // Non-owner, non-group: uid=1000 gid=2000 -> other permissions.
    int r = fs_->access(nodeid, R_OK, 1000, 2000);
    ASSERT_EQ(r, 0);

    // Set mode 0000 -> other denied.
    st.st_mode = 0000;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);
    r = fs_->access(nodeid, R_OK, 1000, 2000);
    ASSERT_EQ(r, -EACCES);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: denied when permission bits don't match.
  void verify_access_denied() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_denied", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Mode 0400: owner read only.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0400;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);

    // Owner can read but not write.
    int r = fs_->access(nodeid, R_OK, 0, 0);
    ASSERT_EQ(r, 0);
    // Root can still write (root bypasses owner check).
    r = fs_->access(nodeid, W_OK, 0, 0);
    ASSERT_EQ(r, 0);

    // Non-owner uid=1000 cannot read.
    r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, -EACCES);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: supplementary group membership grants group permissions.
  void verify_access_supplementary_group_allowed() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "supp_group_allow", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set file gid=2000 (test_group_2000), mode=0070 (group rwx only).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_gid = 2000;
    st.st_mode = 0070;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_GID | FUSE_SET_ATTR_MODE);

    // Caller: uid=1000, gid=1000.
    // Primary gid (1000) != file gid (2000) -> primary gid check fails.
    // But uid=1000 (test_user_1000) is in group_members[2000] -> supplementary
    // group match -> group permissions apply -> R_OK allowed.
    int r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, 0);

    // W_OK should also pass via group permission.
    r = fs_->access(nodeid, W_OK, 1000, 1000);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // access: supplementary group match but group lacks permission -> denied.
  void verify_access_supplementary_group_denied() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "supp_group_deny", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set file gid=2000, mode=0007 (other rwx only, group has no permission).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_gid = 2000;
    st.st_mode = 0007;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_GID | FUSE_SET_ATTR_MODE);

    // Caller: uid=1000, gid=1000.
    // Supplementary group match (gid 2000), but group bits are 000.
    // Only other bits have rwx. R_OK should pass via other.
    int r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, 0);

    // Set mode=0000 -> no permissions at all -> denied.
    st.st_mode = 0000;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);
    r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, -EACCES);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // check_permission: chmod by non-owner non-root -> -EPERM.
  void verify_check_permission_chmod_non_owner() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "chmod_non_owner", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Set owner uid=1000.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID);

    // Try chmod as uid=2000 (not owner, not root) -> -EPERM.
    st.st_mode = 0644;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE, nullptr, 2000, 2000);
    ASSERT_EQ(r, -EPERM);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // check_permission: truncate by non-owner -> -EACCES.
  void verify_check_permission_truncate_non_owner() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "trunc_non_owner", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    // Write some data first.
    auto file = get_file_from_handle(handle);
    const char *data = "truncate_test";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // Set owner uid=1000.
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID);

    // Try truncate as uid=2000 (not owner) -> -EACCES.
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    st.st_size = 0;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE, &fi, 2000, 2000);
    ASSERT_EQ(r, -EACCES);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // set_permission backend failure via FI.
  void verify_set_permission_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "chmod_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0644;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);
    ASSERT_LT(r, 0);
  }

  // set_times backend failure via FI.
  void verify_set_times_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "utimes_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mtim.tv_sec = 1600000000;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MTIME);
    ASSERT_LT(r, 0);
  }

  // set_owner backend failure via FI.
  void verify_set_owner_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "chown_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID);
    ASSERT_LT(r, 0);
  }

  // Non-root non-owner setting explicit times → EPERM.
  void verify_utimensat_non_owner_eperm() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "utimensat_eperm", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID);
    ASSERT_EQ(r, 0);

    memset(&st, 0, sizeof(st));
    st.st_mtim.tv_sec = 1600000000;
    r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MTIME, nullptr, 2000, 2000);
    ASSERT_EQ(r, -EPERM);
  }

  // Owner setting ATIME_NOW + MTIME_NOW.
  void verify_utimensat_owner_now() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "utimensat_now", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID);
    ASSERT_EQ(r, 0);

    memset(&st, 0, sizeof(st));
    st.st_atim.tv_nsec = UTIME_NOW;
    st.st_mtim.tv_nsec = UTIME_NOW;
    r = fs_->setattr(nodeid, &st,
                     FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_ATIME_NOW |
                         FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_MTIME_NOW,
                     nullptr, 1000, 1000);
    ASSERT_EQ(r, 0);
  }

  // check_access: aligned with jindo-fuse which only checks group+other bits
  // (owner bits are skipped due to unreliable uid mapping).
  void verify_access_owner_branch() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "access_owner_br", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    struct stat st;
    memset(&st, 0, sizeof(st));
    // Use mode with other-read bit so check_hdfs_access can verify via
    // the other branch (owner bits are not checked, per jindo-fuse design).
    st.st_uid = 1000;
    st.st_mode = 0704;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_MODE);
    ASSERT_EQ(r, 0);

    // R_OK succeeds because other has read bit (004).
    r = fs_->access(nodeid, R_OK, 1000, 1000);
    ASSERT_EQ(r, 0);
    // W_OK fails because neither group nor other has write bit.
    r = fs_->access(nodeid, W_OK, 1000, 1000);
    ASSERT_EQ(r, -EACCES);
    // Non-owner, non-group also gets R_OK via other bit.
    r = fs_->access(nodeid, R_OK, 2000, 2000);
    ASSERT_EQ(r, 0);
  }

  // check_permission(Unlink): owner with W_OK should succeed.
  void verify_unlink_permission_owner() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "unlink_perm_owner", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    // Set file owner to uid=1000, mode=0700 (owner rwx).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    st.st_mode = 0700;
    fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_MODE);

    // Unlink as owner (uid=1000) with W_OK → should succeed.
    int r = fs_->unlink(parent, "unlink_perm_owner");
    // Note: unlink is called on parent dir entry, not file.
    // The check_permission(Unlink) is called with the file's stat.
    // Here we just verify the unlink call itself works.
    ASSERT_EQ(r, 0);
  }

  // check_permission(Utimensat): non-root, non-owner, group match → W_OK check.
  void verify_utimensat_group_branch() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "utimensat_grp", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    // Set file uid=500, gid=1000, mode=0070 (group rwx).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 500;
    st.st_gid = 1000;
    st.st_mode = 0070;
    fs_->setattr(nodeid, &st,
                 FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID | FUSE_SET_ATTR_MODE);

    // Caller: uid=2000, gid=1000.
    // uid!=0, uid!=file_uid(500) → enter permission check.
    // atime_now && mtime_now → check_permission(Utimensat)
    // → check_posix_access(W_OK, uid=2000, gid=1000)
    // → gid==file_gid(1000) → check W_OK against S_IWGRP (0070 has S_IWGRP) →
    // OK.
    memset(&st, 0, sizeof(st));
    int r = fs_->setattr(nodeid, &st,
                         FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_ATIME_NOW |
                             FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_MTIME_NOW,
                         nullptr, 2000, 1000);
    ASSERT_EQ(r, 0);  // group has W_OK → allowed
  }

  // check_permission(Utimensat): non-root, non-owner, no group match, no W_OK.
  void verify_utimensat_other_denied() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "utimensat_deny", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    // Set file uid=500, gid=500, mode=0070 (group rwx, other no perm).
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 500;
    st.st_gid = 500;
    st.st_mode = 0070;
    fs_->setattr(nodeid, &st,
                 FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID | FUSE_SET_ATTR_MODE);

    // Caller: uid=2000, gid=2000.
    // uid!=0, uid!=file_uid(500), gid!=file_gid(500) → other branch.
    // atime_now && mtime_now → check_permission(Utimensat)
    // → check_posix_access(W_OK, uid=2000, gid=2000)
    // → other has no W_OK (mode 0070) → denied.
    memset(&st, 0, sizeof(st));
    int r = fs_->setattr(nodeid, &st,
                         FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_ATIME_NOW |
                             FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_MTIME_NOW,
                         nullptr, 2000, 2000);
    ASSERT_EQ(r, -EACCES);  // other has no W_OK → denied
  }

  // === Backend persistence verification ===
  // All use attr_timeout=0 to force getattr to hit backend directly.

  // 2a: create persists mode to backend.
  void verify_create_mode_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "persist_mode_0644", CREATE_BASE_FLAGS,
                             0644, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_mode & 07777, (mode_t)0644)
        << "create did not persist mode to backend";
  }

  // 2b: create with hdfs_set_owner_on_create persists uid/gid.
  void verify_create_uid_gid_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "persist_uid_gid", CREATE_BASE_FLAGS, 0644,
                             1000, 1000, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_uid, (uid_t)1000)
        << "create did not persist uid to backend";
    EXPECT_EQ(backend_st.st_gid, (gid_t)1000)
        << "create did not persist gid to backend";
  }

  // 2c: mknod persists mode to backend.
  void verify_mknod_mode_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->mknod(parent, "persist_mknod_0600", S_IFREG | 0600, 0, 0,
                       &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_mode & 07777, (mode_t)0600)
        << "mknod did not persist mode to backend";
  }

  // 2d: symlink target mode persisted to backend.
  void verify_symlink_mode_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t target_id = 0;
    struct stat target_st;
    int r = fs_->mknod(parent, "sym_target_file", S_IFREG | 0755, 0, 0,
                       &target_id, &target_st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(target_id, 1));

    uint64_t link_id = 0;
    struct stat link_st;
    r = fs_->symlink(parent, "sym_link_persist", "sym_target_file", 0, 0,
                     &link_id, &link_st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(link_id, 1));

    struct stat backend_st;
    r = fs_->getattr(target_id, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_mode & 07777, (mode_t)0755)
        << "symlink target mode not persisted to backend";
  }

  // 3a: setattr chmod persists mode to backend.
  void verify_setattr_mode_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "persist_chmod", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = 0600;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MODE);
    ASSERT_EQ(r, 0);

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_mode & 07777, (mode_t)0600)
        << "setattr chmod did not persist to backend";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // 3b: setattr chown persists uid/gid to backend.
  void verify_setattr_uid_gid_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "persist_chown", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_uid = 1000;
    st.st_gid = 1000;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    ASSERT_EQ(r, 0);

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_uid, (uid_t)1000)
        << "setattr chown uid did not persist to backend";
    EXPECT_EQ(backend_st.st_gid, (gid_t)1000)
        << "setattr chown gid did not persist to backend";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // 3c: setattr truncate persists size to backend.
  void verify_setattr_size_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "persist_trunc", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto *file = get_file_from_handle(handle);
    char buf[100];
    memset(buf, 'A', sizeof(buf));
    ssize_t w = file->pwrite(buf, sizeof(buf), 0);
    ASSERT_EQ(w, (ssize_t)sizeof(buf));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_size = 50;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_size, (off_t)50)
        << "setattr truncate did not persist size to backend";
  }

  // 3d: setattr utimens persists mtime to backend.
  void verify_setattr_mtime_persisted() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_test_file(parent, "persist_utimens", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mtim.tv_sec = 1700000000;
    st.st_mtim.tv_nsec = 0;
    int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_MTIME);
    ASSERT_EQ(r, 0);

    struct stat backend_st;
    r = fs_->getattr(nodeid, &backend_st);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(backend_st.st_mtim.tv_sec, (time_t)1700000000)
        << "setattr utimens did not persist mtime to backend";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }
};

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_mode) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setattr_mode();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_uid_gid) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setattr_uid_gid();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_atime) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setattr_atime();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_mtime) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setattr_mtime();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_size_nonzero) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setattr_size_nonzero();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_combined) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_setattr_combined();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_stat_attributes) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_stat_attributes();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_f_ok) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_f_ok();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_root_user) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_root_user();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_root_execute_check) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_root_execute_check();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_owner_permissions) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_owner_permissions();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_group_permissions) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_group_permissions();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_other_permissions) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_other_permissions();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_denied) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_denied();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_check_permission_chmod_non_owner) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_check_permission_chmod_non_owner();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_check_permission_truncate_non_owner) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_check_permission_truncate_non_owner();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_set_permission_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_set_permission_fail();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_set_times_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_set_times_fail();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_set_owner_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_set_owner_fail();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_utimensat_non_owner_eperm) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_utimensat_non_owner_eperm();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_utimensat_owner_now) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_utimensat_owner_now();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_owner_branch) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_owner_branch();
}

// === TEST_F wrappers ===

TEST_F(Ossfs2HdfsSetattrTest, verify_create_mode_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_create_mode_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_create_uid_gid_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  opts.hdfs_set_owner_on_create = true;
  init(opts);
  verify_create_uid_gid_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_mknod_mode_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_mknod_mode_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_symlink_mode_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_mode_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_mode_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_setattr_mode_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_uid_gid_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_setattr_uid_gid_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_size_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_setattr_size_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_setattr_mtime_persisted) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_setattr_mtime_persisted();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_supplementary_group_allowed) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_supplementary_group_allowed();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_access_supplementary_group_denied) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_access_supplementary_group_denied();
}

TEST_F(Ossfs2HdfsSetattrTest, verify_chown_unresolvable_uid_gid) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_chown_unresolvable_uid_gid();
}
