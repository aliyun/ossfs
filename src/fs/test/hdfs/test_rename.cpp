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

class Ossfs2HdfsRenameTest : public OssHdfsTestSuite {
 protected:
  // Rename a directory that was created remotely (via HdfsTestHelper).
  void verify_rename_remote_dir() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;

    auto parent_path = hdfs_helper_->full_uri(nodeid_to_path(parent));

    // Create a directory and a file in it using the HDFS helper (remote path).
    std::string remote_dir = join_paths(parent_path, "remote_dir");
    int r = hdfs_helper_->create_dir(remote_dir);
    ASSERT_EQ(r, 0);

    // Upload a file under remote_dir via helper.
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    std::string filepath = join_paths(remote_dir, "test_file");
    r = hdfs_helper_->upload_file(local_file, filepath);
    ASSERT_EQ(r, 0);

    // Lookup the remote dir via fuse.
    r = fs_->lookup(parent, "remote_dir", &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Plain rename (flags=0): remote_dir -> remote_dir_renamed
    r = fs_->rename(parent, "remote_dir", parent, "remote_dir_renamed", 0);
    ASSERT_EQ(r, 0);

    // Verify old path no longer exists.
    r = hdfs_helper_->stat_file(remote_dir);
    ASSERT_EQ(r, -ENOENT);

    // Verify new path exists and contains the file.
    std::string new_dir = join_paths(parent_path, "remote_dir_renamed");
    r = hdfs_helper_->stat_file(new_dir);
    ASSERT_EQ(r, 0);
    std::vector<std::string> children;
    r = hdfs_helper_->list_dir(new_dir, children);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(children.size(), size_t(1));
    ASSERT_EQ(children[0], "test_file");

    // Create another existing dir and try to overwrite it -> -ENOTEMPTY.
    std::string exist_dir = join_paths(parent_path, "remote_dir_exist");
    r = hdfs_helper_->create_dir(exist_dir);
    ASSERT_EQ(r, 0);
    std::string exist_file = join_paths(exist_dir, "test_file");
    r = hdfs_helper_->upload_file(local_file, exist_file);
    ASSERT_EQ(r, 0);

    r = fs_->rename(parent, "remote_dir_renamed", parent, "remote_dir_exist",
                    0);
    ASSERT_EQ(r, -ENOTEMPTY);

    // RENAME_NOREPLACE: target exists -> -EEXIST.
    uint64_t existing_dir_nodeid = 0;
    r = fs_->lookup(parent, "remote_dir_exist", &existing_dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(existing_dir_nodeid, 1));

    uint64_t file_nodeid = 0;
    r = fs_->lookup(existing_dir_nodeid, "test_file", &file_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(file_nodeid, 1));

    // Upload a second file to existing_dir.
    std::string exist_file2 = join_paths(exist_dir, "test_file2");
    r = hdfs_helper_->upload_file(local_file, exist_file2);
    ASSERT_EQ(r, 0);

    // RENAME_NOREPLACE with target existing -> -EEXIST.
    r = fs_->rename(existing_dir_nodeid, "test_file", existing_dir_nodeid,
                    "test_file2", RENAME_NOREPLACE);
    ASSERT_EQ(r, -EEXIST);

    // Plain rename over existing file -> success.
    r = fs_->rename(existing_dir_nodeid, "test_file", existing_dir_nodeid,
                    "test_file2", 0);
    ASSERT_EQ(r, 0);
  }

  // Rename a directory multiple times and verify descendants count stays
  // consistent, using the list_all_descendants helper.
  void verify_rename_dir_continuously() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    auto parent_path = hdfs_helper_->full_uri(nodeid_to_path(parent));

    uint64_t dir_nodeid = 0;
    std::string dir_name = "test_dir";
    int r =
        fs_->mkdir(parent, dir_name.c_str(), 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    uint64_t subdir_nodeid = 0;
    std::string subdir_name = "subdir";
    r = fs_->mkdir(dir_nodeid, subdir_name.c_str(), 0777, 0, 0, 0,
                   &subdir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(subdir_nodeid, 1));

    // Create 50 files under the subdir (smaller than OSS test for speed).
    const int file_cnt = 50;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    for (int i = 0; i < file_cnt; i++) {
      auto file_name = "testfile-" + std::to_string(i);
      void *fh = nullptr;
      int r =
          create_and_flush(subdir_nodeid, file_name.c_str(), CREATE_BASE_FLAGS,
                           0777, 0, 0, 0, &nodeids[i], &st, &fh);
      ASSERT_EQ(r, 0);
      if (fh) {
        fs_->release(nodeids[i], get_file_from_handle(fh));
      }
    }

    std::string old_dir_name = dir_name;

    // Do 4 rounds of rename.
    for (int i = 0; i < 4; i++) {
      // Verify file count under old dir using list_all_descendants.
      auto old_files_base = join_paths(parent_path, old_dir_name);
      old_files_base = join_paths(old_files_base, subdir_name);
      std::vector<std::string> list_results;
      r = hdfs_helper_->list_all_descendants(old_files_base, list_results);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(list_results.size(), size_t(file_cnt));

      std::string new_dir_name = "test_dir_new_" + std::to_string(i);
      r = fs_->rename(parent, old_dir_name.c_str(), parent,
                      new_dir_name.c_str(), 0);
      ASSERT_EQ(r, 0);

      // Old path should be empty/gone.
      list_results.clear();
      r = hdfs_helper_->list_all_descendants(old_files_base, list_results);
      // list_all_descendants returns -ENOENT when dir doesn't exist, or 0
      // with empty results. Both are acceptable.
      if (r == 0) {
        ASSERT_EQ(list_results.size(), size_t(0));
      }

      // New path should have all files.
      auto new_files_base = join_paths(parent_path, new_dir_name);
      new_files_base = join_paths(new_files_base, subdir_name);
      list_results.clear();
      r = hdfs_helper_->list_all_descendants(new_files_base, list_results);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(list_results.size(), size_t(file_cnt));

      old_dir_name = new_dir_name;
    }

    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  // One thread continuously writes to a file while another thread renames it.
  void verify_rename_while_writing() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat stbuf;
    int r = create_and_flush(parent, "write_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    std::string random_file = join_paths(test_path_, "random_rename_write.dat");
    uint64_t file_size_in_mb = 2;
    create_random_file(random_file, file_size_in_mb);

    int run_time_seconds = 3;
    auto future = std::async(std::launch::async, [=]() -> uint64_t {
      INIT_PHOTON();
      return write_file_intervally(nodeid, random_file,
                                   file_size_in_mb * 1024 * 1024,
                                   run_time_seconds, 3);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    {
      DEFER(future.wait());
      std::string old_name = "write_file";
      for (int i = 0; i < run_time_seconds * 5; i++) {
        auto new_name = "write_file_renamed_" + std::to_string(i);
        r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
        ASSERT_EQ(r, 0);
        old_name = new_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
    }

    // File should still be readable after all renames.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    struct stat st;
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_GT(st.st_size, 0);
  }

  // Rename a parent directory while children files are being written.
  void verify_rename_dir_while_writing() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t dir_nodeid = 0;
    int r = fs_->mkdir(parent, "src_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    // Create two files in the directory.
    uint64_t nodeid1 = 0, nodeid2 = 0;
    void *handle1 = nullptr;
    void *handle2 = nullptr;
    r = create_and_flush(dir_nodeid, "file1", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid1, &st, &handle1);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid1, 1));

    r = create_and_flush(dir_nodeid, "file2", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid2, &st, &handle2);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));
    if (handle2) {
      fs_->release(nodeid2, get_file_from_handle(handle2));
    }

    std::string random_file = join_paths(test_path_, "random_dir_rename.dat");
    create_random_file(random_file, 1);

    int run_time_seconds = 3;
    auto future = std::async(std::launch::async, [=]() -> uint64_t {
      INIT_PHOTON();
      return write_file_intervally(nodeid1, random_file, 1024 * 1024,
                                   run_time_seconds, 2);
    });
    DEFER(future.wait());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Rename the parent directory.
    r = fs_->rename(parent, "src_dir", parent, "dst_dir", 0);
    ASSERT_EQ(r, 0);

    // The file should still be accessible via its nodeid.
    r = fs_->getattr(nodeid1, &st);
    ASSERT_EQ(r, 0);

    // Rename back.
    r = fs_->rename(parent, "dst_dir", parent, "src_dir", 0);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid1, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
  }

  // RENAME_NOREPLACE: target exists -> -EEXIST.
  void verify_rename_noreplace() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid1 = 0, nodeid2 = 0;
    void *fh_src = nullptr, *fh_dst = nullptr;
    int r = create_and_flush(parent, "file_src", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid1, &st, &fh_src);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid1, 1));
    if (fh_src) fs_->release(nodeid1, get_file_from_handle(fh_src));

    r = create_and_flush(parent, "file_dst", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid2, &st, &fh_dst);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));
    if (fh_dst) fs_->release(nodeid2, get_file_from_handle(fh_dst));

    // Target exists -> -EEXIST.
    r = fs_->rename(parent, "file_src", parent, "file_dst", RENAME_NOREPLACE);
    ASSERT_EQ(r, -EEXIST);

    // Target does not exist -> success.
    r = fs_->rename(parent, "file_src", parent, "file_new", RENAME_NOREPLACE);
    ASSERT_EQ(r, 0);

    // Same for directories.
    uint64_t dir1 = 0, dir2 = 0;
    r = fs_->mkdir(parent, "dir_src", 0777, 0, 0, 0, &dir1, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir1, 1));
    r = fs_->mkdir(parent, "dir_dst", 0777, 0, 0, 0, &dir2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir2, 1));

    r = fs_->rename(parent, "dir_src", parent, "dir_dst", RENAME_NOREPLACE);
    ASSERT_EQ(r, -EEXIST);

    r = fs_->rename(parent, "dir_src", parent, "dir_new", RENAME_NOREPLACE);
    ASSERT_EQ(r, 0);
  }

  // RENAME_EXCHANGE is not supported by HDFS -> -ENOTSUP.
  void verify_rename_exchange_unsupported() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid1 = 0, nodeid2 = 0;
    void *fh_a = nullptr, *fh_b = nullptr;
    int r = create_and_flush(parent, "file_a", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &nodeid1, &st, &fh_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid1, 1));
    if (fh_a) fs_->release(nodeid1, get_file_from_handle(fh_a));

    r = create_and_flush(parent, "file_b", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid2, &st, &fh_b);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));
    if (fh_b) fs_->release(nodeid2, get_file_from_handle(fh_b));

    r = fs_->rename(parent, "file_a", parent, "file_b", RENAME_EXCHANGE);
    ASSERT_EQ(r, -ENOTSUP);

    // Same for directories.
    uint64_t dir1 = 0, dir2 = 0;
    r = fs_->mkdir(parent, "dir_a", 0777, 0, 0, 0, &dir1, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir1, 1));
    r = fs_->mkdir(parent, "dir_b", 0777, 0, 0, 0, &dir2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir2, 1));

    r = fs_->rename(parent, "dir_a", parent, "dir_b", RENAME_EXCHANGE);
    ASSERT_EQ(r, -ENOTSUP);
  }

  // rename with pre-delete failure via FI.
  void verify_rename_predelete_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t src_id = 0, dst_id = 0;
    void *src_handle = nullptr, *dst_handle = nullptr;

    int r = create_and_flush(parent, "rename_src", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &src_id, &st, &src_handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    if (src_handle) fs_->release(src_id, get_file_from_handle(src_handle));

    r = create_and_flush(parent, "rename_dst", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &dst_id, &st, &dst_handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_id, 1));
    if (dst_handle) fs_->release(dst_id, get_file_from_handle(dst_handle));

    g_fault_injector->set_injection(FI_HdfsRename_PreDeleteFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsRename_PreDeleteFail));

    r = fs_->rename(parent, "rename_src", parent, "rename_dst", 0);
    ASSERT_EQ(r, -EIO);
  }
};

TEST_F(Ossfs2HdfsRenameTest, verify_rename_remote_dir) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_remote_dir();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_dir_continuously) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_dir_continuously();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_while_writing) {
  INIT_PHOTON();
  OssFsOptions opts;
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_rename_while_writing();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_dir_while_writing) {
  INIT_PHOTON();
  OssFsOptions opts;
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_rename_dir_while_writing();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_noreplace) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_noreplace();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_exchange_unsupported) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_exchange_unsupported();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_predelete_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_predelete_fail();
}

// rename_dir error path.
TEST_F(Ossfs2HdfsRenameTest, verify_rename_dir_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));

  // Create a directory.
  uint64_t dir_nodeid = 0;
  struct stat st;
  int r = fs_->mkdir(parent, "src_dir", 0755, 0, 0, 0, &dir_nodeid, &st);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(dir_nodeid, 1));

  g_fault_injector->set_injection(FI_OssError_Call_Failed);
  DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

  r = fs_->rename(parent, "src_dir", parent, "dst_dir", 0);
  ASSERT_EQ(r, -EIO);
}
