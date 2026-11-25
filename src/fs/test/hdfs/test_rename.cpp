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

    // write_file_intervally returns the CRC64 of everything it wrote and always
    // writes the full total_size (the timing only paces the writes), so the
    // final content is fully determined no matter how the renames interleaved.
    // Capture that CRC64 and prove the data survived all concurrent renames.
    const uint64_t total_size = file_size_in_mb * 1024 * 1024;
    uint64_t expected_crc = future.get();
    ASSERT_NE(expected_crc, 0ULL);

    // Release the create handle so the file reaches a clean, fully-flushed
    // state.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    struct stat st;
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)total_size);

    // The original name must be gone after the renames.
    uint64_t gone_id = 0;
    ASSERT_EQ(fs_->lookup(parent, "write_file", &gone_id, &st), -ENOENT);

    // Read the whole file back through the fs_ connection (reliable for HDFS;
    // an independent SDK stat is not) and compare its CRC64 with what was
    // written. A matching CRC64 plus the exact size proves no chunk was lost,
    // duplicated, or misplaced across the concurrent renames.
    void *rhandle = nullptr;
    bool keep_cache = false;
    ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &rhandle, &keep_cache), 0);
    auto *rdr = get_file_from_handle(rhandle);
    DEFER(fs_->release(nodeid, rdr));

    const size_t kBuf = 1 << 20;
    std::vector<char> buf(kBuf);
    uint64_t got_crc = 0;
    for (off_t off = 0; off < (off_t)total_size;) {
      size_t n = std::min(kBuf, (size_t)(total_size - off));
      ssize_t rd = rdr->pread(buf.data(), n, off);
      ASSERT_EQ(rd, (ssize_t)n) << "offset " << off;
      got_crc = cal_crc64(got_crc, buf.data(), n);
      off += rd;
    }
    ASSERT_EQ(got_crc, expected_crc);
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

  // RENAME_NOREPLACE must return -EEXIST even when the local dst inode is
  // stale: the destination was recreated remotely after the local inode was
  // marked stale, so only a remote probe can detect it.
  void verify_rename_noreplace_stale_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t src_id = 0;
    int r = create_and_flush(parent, "stale_src", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &src_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    if (handle) fs_->release(src_id, get_file_from_handle(handle));

    uint64_t dst_id = 0;
    handle = nullptr;
    r = create_and_flush(parent, "stale_dst", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &dst_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_id, 1));
    if (handle) fs_->release(dst_id, get_file_from_handle(handle));

    // Remove dst via unlink (marks the local inode stale), then recreate
    // dst remotely behind the local cache's back.
    ASSERT_EQ(fs_->unlink(parent, "stale_dst", 0, 0), 0);
    std::string dst_uri = hdfs_helper_->full_uri(nodeid_to_path(dst_id));
    std::string local_file = join_paths(test_path_, "stale_dst_content");
    create_random_file(local_file, 1);
    ASSERT_EQ(hdfs_helper_->upload_file(local_file, dst_uri), 0);

    r = fs_->rename(parent, "stale_src", parent, "stale_dst", RENAME_NOREPLACE);
    ASSERT_EQ(r, -EEXIST);
  }

  // Read back a file via the fs and compare its content.
  void verify_file_content(uint64_t parent, const char *name,
                           const char *expected) {
    struct stat st;
    uint64_t id = 0;
    ASSERT_EQ(fs_->lookup(parent, name, &id, &st), 0);
    DEFER(fs_->forget(id, 1));
    bool keep_cache = false;
    void *rd = nullptr;
    ASSERT_EQ(fs_->open(id, O_RDONLY, &rd, &keep_cache), 0);
    char buf[64] = {};
    ssize_t n = get_file_from_handle(rd)->pread(buf, sizeof(buf), 0);
    ASSERT_EQ(n, (ssize_t)strlen(expected));
    ASSERT_EQ(std::string(buf, n), expected);
    ASSERT_EQ(fs_->release(id, get_file_from_handle(rd)), 0);
  }

  // Plain rename must replace a destination that exists only remotely (no
  // local inode), per POSIX overwrite semantics.
  void verify_rename_overwrite_remote_only_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t src_id = 0;
    int r = create_and_flush(parent, "ow_src", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &src_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    auto file = get_file_from_handle(handle);
    const char *data = "overwrite-content";
    ASSERT_EQ(file->pwrite(data, strlen(data), 0), (ssize_t)strlen(data));
    ASSERT_EQ(fs_->release(src_id, file), 0);

    // dst exists only remotely and was never looked up locally.
    auto parent_path = hdfs_helper_->full_uri(nodeid_to_path(parent));
    std::string dst_uri = join_paths(parent_path, "ow_dst");
    std::string local_file = join_paths(test_path_, "ow_dst_old");
    create_random_file(local_file, 1);
    ASSERT_EQ(hdfs_helper_->upload_file(local_file, dst_uri), 0);

    r = fs_->rename(parent, "ow_src", parent, "ow_dst", 0);
    ASSERT_EQ(r, 0);

    // The destination must now hold the src content.
    verify_file_content(parent, "ow_dst", data);
  }

  // Plain rename must overwrite a stale dst that was recreated remotely.
  void verify_rename_overwrite_stale_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t src_id = 0;
    int r = create_and_flush(parent, "os_src", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &src_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    auto file = get_file_from_handle(handle);
    const char *data = "stale-overwrite-content";
    ASSERT_EQ(file->pwrite(data, strlen(data), 0), (ssize_t)strlen(data));
    ASSERT_EQ(fs_->release(src_id, file), 0);

    uint64_t dst_id = 0;
    handle = nullptr;
    r = create_and_flush(parent, "os_dst", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &dst_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_id, 1));
    if (handle) fs_->release(dst_id, get_file_from_handle(handle));

    // unlink marks the local dst inode stale, then another client recreates
    // the dst remotely with different content.
    ASSERT_EQ(fs_->unlink(parent, "os_dst", 0, 0), 0);
    std::string dst_uri = hdfs_helper_->full_uri(nodeid_to_path(dst_id));
    std::string local_file = join_paths(test_path_, "os_dst_old");
    create_random_file(local_file, 1);
    ASSERT_EQ(hdfs_helper_->upload_file(local_file, dst_uri), 0);

    r = fs_->rename(parent, "os_src", parent, "os_dst", 0);
    ASSERT_EQ(r, 0);

    // The destination must now hold the src content.
    verify_file_content(parent, "os_dst", data);
  }

  // Renaming onto a remote-only dst of a different type must be rejected.
  void verify_rename_type_mismatch_remote_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;
    auto parent_uri = hdfs_helper_->full_uri(nodeid_to_path(parent));

    // file -> remote-only dir dst
    uint64_t src_id = 0;
    int r = create_and_flush(parent, "tm_src", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &src_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    if (handle) fs_->release(src_id, get_file_from_handle(handle));

    std::string dst_uri = join_paths(parent_uri, "tm_dir");
    ASSERT_EQ(hdfs_helper_->create_dir(dst_uri), 0);
    r = fs_->rename(parent, "tm_src", parent, "tm_dir", 0);
    ASSERT_EQ(r, -EISDIR);

    // dir -> remote-only file dst
    uint64_t dir_id = 0;
    r = fs_->mkdir(parent, "tm_src_dir", 0755, 0, 0, 0, &dir_id, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_id, 1));

    std::string file_uri = join_paths(parent_uri, "tm_file");
    std::string local_file = join_paths(test_path_, "tm_file_content");
    create_random_file(local_file, 1);
    ASSERT_EQ(hdfs_helper_->upload_file(local_file, file_uri), 0);
    r = fs_->rename(parent, "tm_src_dir", parent, "tm_file", 0);
    ASSERT_EQ(r, -ENOTDIR);
  }

  // Stale dst replaced remotely with a different type must be rejected.
  void verify_rename_type_mismatch_stale_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t src_id = 0;
    int r = create_and_flush(parent, "ts_src", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &src_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    if (handle) fs_->release(src_id, get_file_from_handle(handle));

    uint64_t dst_id = 0;
    handle = nullptr;
    r = create_and_flush(parent, "ts_dst", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &dst_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_id, 1));
    if (handle) fs_->release(dst_id, get_file_from_handle(handle));

    // unlink marks the local dst inode stale, then another client recreates
    // it as a dir.
    ASSERT_EQ(fs_->unlink(parent, "ts_dst", 0, 0), 0);
    std::string dst_uri = hdfs_helper_->full_uri(nodeid_to_path(dst_id));
    ASSERT_EQ(hdfs_helper_->create_dir(dst_uri), 0);

    r = fs_->rename(parent, "ts_src", parent, "ts_dst", 0);
    ASSERT_EQ(r, -EISDIR);
  }

  // The probe runs even with a fresh local dst inode: if another client
  // changed the remote dst's type, the rename must be rejected.
  void verify_rename_probe_fresh_local_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t src_id = 0;
    int r = create_and_flush(parent, "fresh_src", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &src_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_id, 1));
    if (handle) fs_->release(src_id, get_file_from_handle(handle));

    uint64_t dst_id = 0;
    handle = nullptr;
    r = create_and_flush(parent, "fresh_dst", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &dst_id, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_id, 1));
    if (handle) fs_->release(dst_id, get_file_from_handle(handle));

    // Another client replaces the remote dst file with a dir.
    std::string dst_uri = hdfs_helper_->full_uri(nodeid_to_path(dst_id));
    ASSERT_EQ(hdfs_helper_->delete_file(dst_uri), 0);
    ASSERT_EQ(hdfs_helper_->create_dir(dst_uri), 0);

    r = fs_->rename(parent, "fresh_src", parent, "fresh_dst", 0);
    ASSERT_EQ(r, -EISDIR);
  }

  // A reader opened before rename must keep serving data afterwards: the
  // handle detects the path change and rebuilds its backend stream.
  void verify_read_open_file_after_rename() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    create_file_in_folder(parent, "rename_rd_src", 16, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    bool keep_cache = false;
    void *handle = nullptr;
    ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &handle, &keep_cache), 0);
    auto *reader = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, reader));

    ASSERT_EQ(fs_->rename(parent, "rename_rd_src", parent, "rename_rd_dst", 0),
              0);

    // create_file_in_folder writes a local random file with the same name.
    std::ifstream lf(join_paths(test_path_, "rename_rd_src"), std::ios::binary);
    ASSERT_TRUE(lf.good());

    const size_t kChunk = 1 << 20;
    std::vector<char> buf(kChunk);
    std::vector<char> expected(kChunk);
    for (uint64_t off = 0; off < 16; off++) {
      ssize_t n = reader->pread(buf.data(), kChunk, off * kChunk);
      ASSERT_EQ(n, (ssize_t)kChunk) << "chunk " << off;
      lf.read(expected.data(), kChunk);
      ASSERT_EQ(memcmp(buf.data(), expected.data(), kChunk), 0)
          << "chunk " << off;
    }
    ASSERT_EQ(reader->pread(buf.data(), kChunk, 16 * kChunk), 0);

    // The new path must serve the same data to a freshly opened reader.
    struct stat st;
    uint64_t dst_id = 0;
    ASSERT_EQ(fs_->lookup(parent, "rename_rd_dst", &dst_id, &st), 0);
    DEFER(fs_->forget(dst_id, 1));
    void *dst_handle = nullptr;
    ASSERT_EQ(fs_->open(dst_id, O_RDONLY, &dst_handle, &keep_cache), 0);
    auto *dst_reader = get_file_from_handle(dst_handle);
    ASSERT_EQ(dst_reader->pread(buf.data(), kChunk, 0), (ssize_t)kChunk);
    lf.clear();
    lf.seekg(0);
    lf.read(expected.data(), kChunk);
    ASSERT_EQ(memcmp(buf.data(), expected.data(), kChunk), 0);
    ASSERT_EQ(fs_->release(dst_id, dst_reader), 0);
  }

  void verify_read_after_rename_path_refresh(bool use_fallocate) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const size_t kChunk = 1 << 20;
    const off_t file_size = 16 * kChunk;
    uint64_t nodeid = 0;
    create_file_in_folder(parent, "reader_refresh_src", 16, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    bool keep_cache = false;
    void *handle = nullptr;
    ASSERT_EQ(fs_->open(nodeid, O_RDWR, &handle, &keep_cache), 0);
    auto *file = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, file));

    ASSERT_EQ(fs_->rename(parent, "reader_refresh_src", parent,
                          "reader_refresh_dst", 0),
              0);

    auto parent_uri = hdfs_helper_->full_uri(nodeid_to_path(parent));
    auto source_uri = join_paths(parent_uri, "reader_refresh_src");
    auto target_uri = join_paths(parent_uri, "reader_refresh_dst");
    ASSERT_EQ(hdfs_helper_->stat_file(source_uri), -ENOENT);
    int64_t backend_size = 0;
    ASSERT_EQ(hdfs_helper_->stat_file_size(target_uri, backend_size), 0);
    ASSERT_EQ(backend_size, file_size);

    g_fault_injector->set_injection(FI_HdfsReaderRebuild_ReopenFail);
    DEFER(g_fault_injector->clear_injection(FI_HdfsReaderRebuild_ReopenFail));

    if (use_fallocate) {
      ASSERT_EQ(fs_->fallocate(nodeid, kChunk, kChunk, handle), 0);
    } else {
      struct stat attr = {};
      attr.st_size = file_size;
      struct fuse_file_info fi = {};
      fi.fh = reinterpret_cast<uint64_t>(handle);
      ASSERT_EQ(fs_->setattr(nodeid, &attr, FUSE_SET_ATTR_SIZE, &fi, 0, 0), 0);
      ASSERT_EQ(attr.st_size, file_size);
    }

    std::ifstream lf(join_paths(test_path_, "reader_refresh_src"),
                     std::ios::binary);
    ASSERT_TRUE(lf.good());
    std::vector<char> buf(kChunk);
    std::vector<char> expected(kChunk);
    ASSERT_EQ(file->pread(buf.data(), kChunk, 0), -EIO);
    g_fault_injector->clear_injection(FI_HdfsReaderRebuild_ReopenFail);
    for (off_t offset = 0; offset < file_size; offset += kChunk) {
      ASSERT_EQ(file->pread(buf.data(), kChunk, offset), (ssize_t)kChunk)
          << "offset " << offset;
      lf.read(expected.data(), kChunk);
      ASSERT_EQ(lf.gcount(), (std::streamsize)kChunk);
      ASSERT_EQ(memcmp(buf.data(), expected.data(), kChunk), 0)
          << "offset " << offset;
      if (offset == 0) {
        g_fault_injector->set_injection(FI_HdfsReaderRebuild_ReopenFail);
      }
    }
    ASSERT_EQ(file->pread(buf.data(), kChunk, file_size), 0);

    ASSERT_EQ(hdfs_helper_->stat_file_size(target_uri, backend_size), 0);
    ASSERT_EQ(backend_size, file_size);
  }

  // A failed rebuild must be retried on the next pread.
  void verify_read_rebuild_reopen_fail_retries() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    create_file_in_folder(parent, "rebuild_fail_src", 1, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    bool keep_cache = false;
    void *handle = nullptr;
    ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &handle, &keep_cache), 0);
    auto *reader = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, reader));

    ASSERT_EQ(
        fs_->rename(parent, "rebuild_fail_src", parent, "rebuild_fail_dst", 0),
        0);

    const size_t kChunk = 4096;
    std::vector<char> buf(kChunk);

    g_fault_injector->set_injection(FI_HdfsReaderRebuild_ReopenFail);
    DEFER(g_fault_injector->clear_injection(FI_HdfsReaderRebuild_ReopenFail));
    ASSERT_EQ(reader->pread(buf.data(), kChunk, 0), -EIO);

    g_fault_injector->clear_injection(FI_HdfsReaderRebuild_ReopenFail);
    ASSERT_EQ(reader->pread(buf.data(), kChunk, 0), (ssize_t)kChunk);
  }

  // The writer stream must stay inode-bound across rename, for both HDFS open
  // flows: RANDOM_WRITE (O_RDWR, positional writes) and APPEND (O_APPEND, every
  // write lands at the stream end). The file is renamed repeatedly WHILE it is
  // being written, so every chunk must land at its correct offset in the FINAL
  // target and all intermediate names must disappear. Each chunk carries
  // distinct content: identical chunks would mask a write landing at the wrong
  // offset or a duplicated/lost chunk after a rename.
  void verify_write_open_file_after_rename(bool append_mode) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t nodeid = 0;
    int r = create_and_flush(parent, "rename_wr_src", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    handle = nullptr;
    bool keep_cache = false;
    int open_flags = append_mode ? (O_RDWR | O_APPEND) : O_RDWR;
    ASSERT_EQ(fs_->open(nodeid, open_flags, &handle, &keep_cache), 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    const size_t kChunk = 1 << 20;
    const uint64_t kNumChunks = 4;
    std::vector<std::vector<char>> chunks(kNumChunks);
    for (uint64_t c = 0; c < kNumChunks; c++) {
      chunks[c].resize(kChunk);
      for (size_t i = 0; i < kChunk; i++) {
        chunks[c][i] = (char)(c * 31 + i * 7 + 13);
      }
    }

    // Interleave writes and renames on the SAME open handle: rename the file
    // again after every chunk except the last, so the writer must follow the
    // target across multiple renames. Under O_APPEND the offset is ignored and
    // each write appends at the end; the sequential offsets below coincide with
    // those append positions, so both modes produce the same layout.
    std::string cur_name = "rename_wr_src";
    std::vector<std::string> stale_names;
    for (uint64_t c = 0; c < kNumChunks; c++) {
      ASSERT_EQ(
          write_to_file_handle(handle, chunks[c].data(), kChunk, c * kChunk),
          (ssize_t)kChunk)
          << "chunk " << c;
      if (c + 1 < kNumChunks) {
        std::string next_name = "rename_wr_step" + std::to_string(c);
        ASSERT_EQ(
            fs_->rename(parent, cur_name.c_str(), parent, next_name.c_str(), 0),
            0)
            << "rename " << c;
        stale_names.push_back(cur_name);
        cur_name = next_name;
      }
    }
    const std::string final_name = cur_name;

    ASSERT_EQ(fsync_file_handle(handle, true), 0);

    // Every name the file had before the final one must be gone.
    for (const auto &name : stale_names) {
      uint64_t gone_id = 0;
      ASSERT_EQ(fs_->lookup(parent, name.c_str(), &gone_id, &st), -ENOENT)
          << name;
    }

    // The final target must hold all chunks, each at its correct offset.
    uint64_t dst_id = 0;
    ASSERT_EQ(fs_->lookup(parent, final_name.c_str(), &dst_id, &st), 0);
    DEFER(fs_->forget(dst_id, 1));
    ASSERT_EQ(st.st_size, (off_t)(kNumChunks * kChunk));

    void *dst_handle = nullptr;
    ASSERT_EQ(fs_->open(dst_id, O_RDONLY, &dst_handle, &keep_cache), 0);
    auto *dst_reader = get_file_from_handle(dst_handle);
    DEFER(fs_->release(dst_id, dst_reader));

    std::vector<char> buf(kChunk);
    for (uint64_t c = 0; c < kNumChunks; c++) {
      ASSERT_EQ(dst_reader->pread(buf.data(), kChunk, c * kChunk),
                (ssize_t)kChunk)
          << "chunk " << c;
      ASSERT_EQ(memcmp(buf.data(), chunks[c].data(), kChunk), 0)
          << "chunk " << c;
    }
    ASSERT_EQ(dst_reader->pread(buf.data(), kChunk, kNumChunks * kChunk), 0);
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

TEST_F(Ossfs2HdfsRenameTest, verify_rename_noreplace_stale_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_noreplace_stale_dst();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_overwrite_remote_only_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_overwrite_remote_only_dst();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_overwrite_stale_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_overwrite_stale_dst();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_type_mismatch_remote_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_type_mismatch_remote_dst();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_type_mismatch_stale_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_type_mismatch_stale_dst();
}

TEST_F(Ossfs2HdfsRenameTest, verify_rename_probe_fresh_local_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_probe_fresh_local_dst();
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

TEST_F(Ossfs2HdfsRenameTest, verify_read_open_file_after_rename) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_read_open_file_after_rename();
}

TEST_F(Ossfs2HdfsRenameTest, verify_read_after_rename_ftruncate_rebuild_retry) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_read_after_rename_path_refresh(false);
}

TEST_F(Ossfs2HdfsRenameTest, verify_read_after_rename_fallocate_rebuild_retry) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_read_after_rename_path_refresh(true);
}

TEST_F(Ossfs2HdfsRenameTest, verify_read_rebuild_reopen_fail_retries) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_read_rebuild_reopen_fail_retries();
}

TEST_F(Ossfs2HdfsRenameTest, verify_write_open_file_after_rename_random_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_write_open_file_after_rename(false);
}

TEST_F(Ossfs2HdfsRenameTest, verify_write_open_file_after_rename_append) {
  INIT_PHOTON();
  OssFsOptions opts;
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_write_open_file_after_rename(true);
}
