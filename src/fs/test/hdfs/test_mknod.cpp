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

class Ossfs2HdfsMknodTest : public OssHdfsTestSuite {
 protected:
  // mknod S_IFREG: create a regular file.
  void verify_mknod_regular_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;

    int r =
        fs_->mknod(parent, "regular_file", S_IFREG | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Verify it's a regular file.
    ASSERT_TRUE(S_ISREG(st.st_mode));

    // Open and write to it.
    void *handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto file = get_file_from_handle(handle);

    const char *data = "hello mknod";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen and read back.
    r = fs_->open(nodeid, O_RDONLY, &handle, &keep_cache);
    ASSERT_EQ(r, 0);
    file = get_file_from_handle(handle);
    char buf[32] = {};
    ssize_t n = file->pread(buf, sizeof(buf), 0);
    ASSERT_EQ(n, (ssize_t)strlen(data));
    ASSERT_EQ(std::string(buf, n), "hello mknod");
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // mknod S_IFIFO -> -ENOTSUP.
  void verify_mknod_fifo_unsupported() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;

    int r = fs_->mknod(parent, "fifo_file", S_IFIFO | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, -ENOTSUP);
  }

  // mknod S_IFSOCK -> -ENOTSUP.
  void verify_mknod_socket_unsupported() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;

    int r =
        fs_->mknod(parent, "sock_file", S_IFSOCK | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, -ENOTSUP);
  }

  // mknod S_IFBLK -> -ENOTSUP.
  void verify_mknod_block_device_unsupported() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;

    int r = fs_->mknod(parent, "blk_file", S_IFBLK | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, -ENOTSUP);
  }

  // mknod S_IFCHR -> -ENOTSUP.
  void verify_mknod_char_device_unsupported() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;

    int r = fs_->mknod(parent, "chr_file", S_IFCHR | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, -ENOTSUP);
  }

  // mknod on existing file -> -EEXIST.
  void verify_mknod_existing_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    // Create a file first.
    uint64_t nodeid = 0;
    void *fh = nullptr;
    int r = create_and_flush(parent, "existing_file", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &fh);
    ASSERT_EQ(r, 0);
    if (fh) {
      fs_->release(nodeid, get_file_from_handle(fh));
    }
    DEFER(fs_->forget(nodeid, 1));

    // mknod on existing file -> -EEXIST.
    uint64_t nodeid2 = 0;
    r = fs_->mknod(parent, "existing_file", S_IFREG | 0644, 0, 0, &nodeid2,
                   &st);
    ASSERT_EQ(r, -EEXIST);
  }

  // mknod on existing directory -> -EEXIST.
  void verify_mknod_existing_dir() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t dir_nodeid = 0;
    int r = fs_->mkdir(parent, "existing_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    uint64_t nodeid2 = 0;
    r = fs_->mknod(parent, "existing_dir", S_IFREG | 0644, 0, 0, &nodeid2, &st);
    ASSERT_EQ(r, -EEXIST);
  }

  // mknod: mode permissions are preserved.
  void verify_mknod_mode_preserved() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;

    mode_t requested_mode = S_IFREG | 0755;
    int r = fs_->mknod(parent, "mode_file", requested_mode, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Verify mode via getattr.
    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_mode & 0777, (mode_t)0755);
  }

  // hdfs_set_owner_on_create: mknod with owner persistence.
  void verify_set_owner_on_create() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    int r = fs_->mknod(parent, "owner_create", S_IFREG | 0777, 1000, 1000,
                       &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_uid, (uid_t)1000);
    ASSERT_EQ(st2.st_gid, (gid_t)1000);
  }

  // hdfs_set_owner_on_create: mkdir with owner persistence.
  void verify_set_owner_on_mkdir() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t dir_id = 0;
    int r =
        fs_->mkdir(parent, "owner_mkdir", 0777, 1000, 1000, 0, &dir_id, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_id, 1));

    struct stat st2;
    r = fs_->getattr(dir_id, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_uid, (uid_t)1000);
    ASSERT_EQ(st2.st_gid, (gid_t)1000);
  }

  // lookup/stat backend failure via FI.
  void verify_lookup_call_failed() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    int r =
        fs_->mknod(parent, "lookup_fail", S_IFREG | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Forget inode to force backend stat on next lookup.
    fs_->forget(nodeid, 1);

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    uint64_t new_nodeid = 0;
    r = fs_->lookup(parent, "lookup_fail", &new_nodeid, &st);
    ASSERT_LT(r, 0);
  }

  // unlink backend failure via FI.
  void verify_unlink_call_failed() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    int r =
        fs_->mknod(parent, "unlink_fail", S_IFREG | 0644, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    r = fs_->unlink(parent, "unlink_fail");
    ASSERT_LT(r, 0);
  }

  // mkdir backend failure via FI.
  void verify_mkdir_call_failed() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    struct stat st;
    uint64_t dir_id = 0;
    int r = fs_->mkdir(parent, "mkdir_fail", 0777, 0, 0, 0, &dir_id, &st);
    ASSERT_LT(r, 0);
  }

  // rmdir (is_dir_empty) backend failure via FI.
  void verify_rmdir_call_failed() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t dir_id = 0;
    int r = fs_->mkdir(parent, "rmdir_fail", 0777, 0, 0, 0, &dir_id, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_id, 1));

    g_fault_injector->set_injection(FI_OssError_Call_Failed,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    r = fs_->rmdir(parent, "rmdir_fail");
    ASSERT_LT(r, 0);
  }
};

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_regular_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_regular_file();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_fifo_unsupported) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_fifo_unsupported();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_socket_unsupported) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_socket_unsupported();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_block_device_unsupported) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_block_device_unsupported();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_char_device_unsupported) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_char_device_unsupported();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_existing_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_existing_file();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_existing_dir) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_existing_dir();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mknod_mode_preserved) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mknod_mode_preserved();
}

TEST_F(Ossfs2HdfsMknodTest, verify_set_owner_on_create) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.hdfs_set_owner_on_create = true;
  init(opts);
  verify_set_owner_on_create();
}

TEST_F(Ossfs2HdfsMknodTest, verify_set_owner_on_mkdir) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.hdfs_set_owner_on_create = true;
  init(opts);
  verify_set_owner_on_mkdir();
}

TEST_F(Ossfs2HdfsMknodTest, verify_lookup_call_failed) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_lookup_call_failed();
}

TEST_F(Ossfs2HdfsMknodTest, verify_unlink_call_failed) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_unlink_call_failed();
}

TEST_F(Ossfs2HdfsMknodTest, verify_mkdir_call_failed) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_mkdir_call_failed();
}

TEST_F(Ossfs2HdfsMknodTest, verify_rmdir_call_failed) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rmdir_call_failed();
}
