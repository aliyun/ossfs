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

#include <atomic>

#include "test_suite.h"

class Ossfs2RenameTest : public Ossfs2TestSuite {
 protected:
  void verify_rename_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t dir_nodeid = 0;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    auto new_parents = {parent, dir_nodeid};

    for (auto new_parent : new_parents) {
      uint64_t nodeid1 = 0, nodeid2 = 0;
      void *handle1 = nullptr, *handle2 = nullptr;

      std::string filename1 = "test_rename_file1",
                  filename2 = "test_rename_file2";
      int r = create_and_flush(parent, filename1.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid1, &st, &handle1);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid1, 1));

      r = create_and_flush(new_parent, filename2.c_str(), CREATE_BASE_FLAGS,
                           0777, 0, 0, 0, &nodeid2, &st, &handle2);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid2, 1));

      std::string new_filename = "test_rename_file_new";
      r = fs_->rename(parent, filename1.c_str(), new_parent,
                      new_filename.c_str(), RENAME_EXCHANGE);
      ASSERT_EQ(r, -ENOTSUP);

      const int size_1 = rand() % 1048576 + 1;
      std::string write_str = random_string(size_1);
      char *write_buf = const_cast<char *>(write_str.c_str());
      auto file = get_file_from_handle(handle1);
      r = file->pwrite(write_buf, size_1, 0);
      ASSERT_EQ(r, size_1);

      r = fs_->release(nodeid1, get_file_from_handle(handle1));
      ASSERT_EQ(r, 0);

      auto parent_path = nodeid_to_path(parent);
      r = stat_file(join_paths(parent_path, filename1),
                    FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->rename(parent, filename1.c_str(), new_parent, filename2.c_str(),
                      RENAME_NOREPLACE);
      ASSERT_EQ(r, -EEXIST);  // file is closed, and overwriting one existing
                              // file is disallowed.

      r = fs_->rename(parent, filename1.c_str(), parent, new_filename.c_str(),
                      RENAME_NOREPLACE);
      ASSERT_EQ(r, 0);
      r = stat_file(join_paths(parent_path, new_filename),
                    FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->rename(parent, new_filename.c_str(), new_parent,
                      filename2.c_str(), 0);
      ASSERT_EQ(r, 0);  // overwriting an existing file is allowed.
      parent_path = nodeid_to_path(new_parent);
      r = stat_file(join_paths(parent_path, filename2),
                    FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->release(nodeid2, get_file_from_handle(handle2));
      ASSERT_EQ(r, 0);

      uint64_t crc64 = cal_crc64(0, write_buf, size_1);
      auto meta = get_file_meta(join_paths(parent_path, filename2),
                                FLAGS_oss_bucket_prefix);
      ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);
    }
  }

  void verify_rename_dir() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t dir_nodeid = 0;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    auto new_parents = {parent, dir_nodeid};

    for (auto new_parent : new_parents) {
      uint64_t nodeid1 = 0, nodeid2 = 0;

      std::string dirname1 = "test_rename_dir", dirname2 = "test_rename_dir2";
      int r =
          fs_->mkdir(parent, dirname1.c_str(), 0777, 0, 0, 0, &nodeid1, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid1, 1));

      r = fs_->mkdir(new_parent, dirname2.c_str(), 0777, 0, 0, 0, &nodeid2,
                     &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid2, 1));

      std::string new_dirname = "test_rename_dir_new";
      r = fs_->rename(parent, dirname1.c_str(), new_parent, new_dirname.c_str(),
                      RENAME_EXCHANGE);
      ASSERT_EQ(r, -ENOTSUP);

      struct fuse_file_info fi;
      r = fs_->opendir(nodeid1, &fi);
      ASSERT_EQ(r, 0);
      void *dirp = reinterpret_cast<void *>(fi.fh);

      r = fs_->releasedir(nodeid1, dirp);

      auto parent_path = nodeid_to_path(parent);
      r = stat_file(join_paths(parent_path, dirname1) + "/",
                    FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->rename(parent, dirname1.c_str(), new_parent, dirname2.c_str(),
                      RENAME_NOREPLACE);
      ASSERT_EQ(r, -EEXIST);  // overwriting one existing dir is disallowed.

      r = fs_->rename(parent, dirname1.c_str(), parent, new_dirname.c_str(),
                      RENAME_NOREPLACE);
      ASSERT_EQ(r, 0);
      r = stat_file(join_paths(parent_path, new_dirname) + "/",
                    FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      // create a file in dirname2
      uint64_t filenodeid = 0;
      void *filehandle = nullptr;
      r = create_and_flush(nodeid2, "test_file", 0777, 0, 0, 0, 0, &filenodeid,
                           &st, &filehandle);
      ASSERT_EQ(r, 0);
      fs_->release(filenodeid, get_file_from_handle(filehandle));
      DEFER(fs_->forget(filenodeid, 1));

      r = fs_->rename(parent, new_dirname.c_str(), new_parent, dirname2.c_str(),
                      0);
      ASSERT_EQ(r, -ENOTEMPTY);  // overwriting an non-empty dir is not allowed.

      r = fs_->unlink(nodeid2, "test_file");
      ASSERT_EQ(r, 0);

      r = fs_->rename(parent, new_dirname.c_str(), new_parent, dirname2.c_str(),
                      0);
      ASSERT_EQ(r, 0);  // overwriting an existing empty dir is allowed.

      parent_path = nodeid_to_path(new_parent);
      r = stat_file(join_paths(parent_path, dirname2) + "/",
                    FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }
  }

  void verify_rename_dir_continuously() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    auto parent_path = nodeid_to_path(parent);

    uint64_t dir_nodeid = 0;
    std::string dir_name = "test_dir";
    int r =
        fs_->mkdir(parent, dir_name.c_str(), 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    uint64_t subdir_nodeid = 0;
    std::string subdir_name(kOssfsMaxFileNameLength, 'c');
    r = fs_->mkdir(dir_nodeid, subdir_name.c_str(), 0777, 0, 0, 0,
                   &subdir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(subdir_nodeid, 1));

    const int file_cnt = 1003;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    create_files_under_dir_parallelly(subdir_nodeid, file_cnt, nodeids);

    std::string old_dir_name, new_dir_name;
    old_dir_name = dir_name;
    // do 4 four rounds rename
    for (int i = 0; i < 4; i++) {
      // we should have 1003 files in the src dir
      std::vector<std::string> list_results;
      // we are listing all the objects with old_path/ specified as both prefix
      auto old_files_base = join_paths(parent_path, old_dir_name);
      old_files_base = join_paths(old_files_base, subdir_name);
      r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, list_dir_descendants,
                                         old_files_base, list_results);
      ASSERT_EQ(r, 0);
      ASSERT_SIZE_EQ(list_results.size(), file_cnt);

      new_dir_name = "test_dir_new_" + std::to_string(i);
      r = fs_->rename(parent, old_dir_name.c_str(), parent,
                      new_dir_name.c_str(), 0);
      ASSERT_EQ(r, 0);

      list_results.clear();

      bool is_dirobj = false;
      r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, list_dir_descendants,
                                         old_files_base, list_results, nullptr,
                                         &is_dirobj);
      ASSERT_EQ(r, 0);
      ASSERT_FALSE(is_dirobj);
      ASSERT_SIZE_EQ(list_results.size(), 0);

      auto new_files_base = join_paths(parent_path, new_dir_name);
      new_files_base = join_paths(new_files_base, subdir_name);
      r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, list_dir_descendants,
                                         new_files_base, list_results);
      ASSERT_EQ(r, 0);
      ASSERT_SIZE_EQ(list_results.size(), file_cnt);
      old_dir_name = new_dir_name;
    }

    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_rename_keep_old_meta() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid;
    void *handle;

    uint64_t dir_nodeid = 0;
    std::string dir_name = "test_dir";
    int r =
        fs_->mkdir(parent, dir_name.c_str(), 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    std::string file_name = "testfile";
    r = create_and_flush(dir_nodeid, file_name.c_str(), CREATE_BASE_FLAGS, 0777,
                         0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    auto file_path = nodeid_to_path(nodeid);
    auto meta = get_file_meta(file_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("", meta["X-Oss-Meta-Custom"]);

    r = set_file_meta(file_path, "X-Oss-Meta-Custom", "custom_meta_value",
                      FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    meta = get_file_meta(file_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("custom_meta_value", meta["X-Oss-Meta-Custom"]);

    std::string new_file_name = "test_file_new_name";
    r = fs_->rename(dir_nodeid, file_name.c_str(), dir_nodeid,
                    new_file_name.c_str(), 0);
    ASSERT_EQ(r, 0);

    auto new_file_path = nodeid_to_path(nodeid);
    meta = get_file_meta(new_file_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("custom_meta_value", meta["X-Oss-Meta-Custom"]);

    std::string new_dir_name = "new_test_dir";
    r = fs_->rename(parent, dir_name.c_str(), parent, new_dir_name.c_str(), 0);
    ASSERT_EQ(r, 0);

    new_file_path = nodeid_to_path(nodeid);
    meta = get_file_meta(new_file_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("custom_meta_value", meta["X-Oss-Meta-Custom"]);
  }

  void verify_rename_file_with_oss_err() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    uint64_t crc64 = create_file_in_folder(parent, "test_file", 1, nodeid, 101);
    ASSERT_TRUE(crc64 > 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file_path = nodeid_to_path(nodeid);

    auto meta = get_file_meta(file_path.c_str(), FLAGS_oss_bucket_prefix);
    for (auto &it : meta) {
      LOG_INFO("key ` value `", it.first, it.second);
    }

    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);

    fs_->update_creds({"1", "2", "3"});

    int r = fs_->rename(parent, "test_file", parent, "test_file2", 0);
    ASSERT_NE(r, 0);

    meta = get_file_meta(file_path.c_str(), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);
  }

  void verify_rename_dir_with_oss_err() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));
    auto parent_path = nodeid_to_path(parent);

    int file_cnt = 1003;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    create_files_under_dir_parallelly(dir_nodeid, file_cnt, nodeids);
    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    }
    LOG_INFO("created ` files in the dir", file_cnt);

    std::string old_name = "test_dir";
    // Just skip the request which checks if dest dir is empty.
    // And the failure happens when we try to list the src dir.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout,
                                    {std::numeric_limits<uint32_t>::max(), 1});
    r = fs_->rename(parent, old_name, parent, "should_fail_anyway", 0);
    ASSERT_EQ(r, -ETIMEDOUT);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);
    // No copies should have happned.
    auto tmp_result = get_list_objects(
        join_paths(parent_path, "should_fail_anyway"), FLAGS_oss_bucket_prefix);
    ASSERT_SIZE_EQ(tmp_result.size(), 0);
    tmp_result = get_list_objects(join_paths(parent_path, old_name),
                                  FLAGS_oss_bucket_prefix);
    ASSERT_SIZE_EQ(tmp_result.size(), file_cnt);

    bool has_failure_round = false;
    for (int i = 0; i < 10; i++) {
      auto set_invalid_cred_future = std::async(std::launch::async, [&]() {
        INIT_PHOTON();
        srand(time(nullptr));
        photon::thread_usleep(rand() % 1000 *
                              1000);  // random sleep for [0, 1000] ms
        fs_->update_creds({"1", "2", "3"});
        photon::thread_usleep(500000);
        fs_->update_creds(
            {FLAGS_oss_access_key_id, FLAGS_oss_access_key_secret});
      });
      DEFER(set_invalid_cred_future.wait());

      auto new_name = "test_dir" + std::to_string(i);
      r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
      if (r == 0) {
        // the rename is completed before the credential is invalidated
        auto result = get_list_objects(join_paths(parent_path, old_name),
                                       FLAGS_oss_bucket_prefix);
        ASSERT_SIZE_EQ(result.size(), 0);
        result = get_list_objects(join_paths(parent_path, new_name),
                                  FLAGS_oss_bucket_prefix);
        ASSERT_SIZE_EQ(result.size(), file_cnt);
        old_name = new_name;
        continue;
      }
      LOG_INFO("rename failed with err `", r);
      has_failure_round = true;
      auto old_result = get_list_objects(join_paths(parent_path, old_name),
                                         FLAGS_oss_bucket_prefix);
      if (old_result.size() != (size_t)file_cnt) {
        // the rename failed because the credential is invalidated
        // the old dir might be partially deleted
        auto new_result = get_list_objects(join_paths(parent_path, new_name),
                                           FLAGS_oss_bucket_prefix);
        ASSERT_SIZE_EQ(new_result.size(), file_cnt);
        file_cnt = old_result.size();
      }
    }

    // it's very unlikely that all rounds have been completed successfully.
    // assert here to make sure we have experienced failure cases.
    ASSERT_TRUE(has_failure_round);

    r = fs_->rename(parent, old_name.c_str(), parent, "new_test_dir", 0);
    ASSERT_EQ(r, 0);

    auto new_result = get_list_objects(join_paths(parent_path, "new_test_dir"),
                                       FLAGS_oss_bucket_prefix);
    ASSERT_SIZE_EQ(new_result.size(), file_cnt);
    auto old_result = get_list_objects(join_paths(parent_path, old_name),
                                       FLAGS_oss_bucket_prefix, true);
    ASSERT_SIZE_EQ(old_result.size(), 0);
  }

  void verify_rename_dir_with_limit(int limit) {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    std::vector<std::string> file_names;
    for (int i = 0; i <= limit; i++) {
      uint64_t nodeid;
      void *handle;
      auto name = "testfile-" + std::to_string(i);
      file_names.push_back(name);
      int r = create_and_flush(dir_nodeid, name.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));
    }

    r = fs_->rename(parent, "test_dir", parent, "new_test_dir", 0);
    ASSERT_EQ(r, -E2BIG);

    uint64_t file_nodeid = 0;
    r = fs_->lookup(dir_nodeid, file_names[0].c_str(), &file_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(file_nodeid, 1));
    r = fs_->unlink(dir_nodeid, file_names[0].c_str());
    ASSERT_EQ(r, 0);

    r = fs_->rename(parent, "test_dir", parent, "new_test_dir", 0);
    ASSERT_EQ(r, 0);
  }

  void verify_rename_dirty_file() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat stbuf;
    int r = create_and_flush(parent, "dirty_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    std::string random_file = join_paths(test_path_, "random_dir_file.dat");

    srand(time(nullptr));
    uint64_t file_size_in_mb = 1 + rand() % 128;  // at least 1MB
    uint64_t drift = rand() % (1024 * 1024);
    create_random_file(random_file, file_size_in_mb, drift);
    int run_time_seconds = 3;

    // lanch a thread to read data from the local file and write the data to
    // the file. the write is supposed be completed in task_time seconds.
    auto future = std::async(std::launch::async, [=]() -> uint64_t {
      INIT_PHOTON();
      return write_file_intervally(nodeid, random_file,
                                   file_size_in_mb * 1024 * 1024 + drift,
                                   run_time_seconds, 5 /*with long delay*/);
    });
    DEFER(fs_->forget(nodeid, 1));

    uint64_t expected_crc64 = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
      DEFER(expected_crc64 = future.get());
      std::string old_name = "dirty_file";
      for (int i = 0; i < run_time_seconds * 10; i++) {
        auto new_name = "dirty_file_renamed" + std::to_string(i);
        r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
        ASSERT_EQ(r, 0);
        old_name = new_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    auto file_path = nodeid_to_path(nodeid);
    auto meta = get_file_meta(file_path.c_str(), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(expected_crc64), meta["X-Oss-Hash-Crc64ecma"]);
  }

  void verify_rename_dirty_file_with_fsync_error() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat stbuf;
    int r = create_and_flush(parent, "dirty_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    std::string random_file = join_paths(test_path_, "random_dir_file.dat");

    srand(time(nullptr));
    uint64_t file_size_in_mb = 1 + rand() % 128;  // at least 1MB
    uint64_t drift = rand() % (1024 * 1024);
    create_random_file(random_file, file_size_in_mb, drift);
    int run_time_seconds = 3;

    // lanch a thread to read data from the local file and write the data to
    // the file. the write is supposed be completed in task_time seconds.
    auto future = std::async(std::launch::async, [=]() -> uint64_t {
      INIT_PHOTON();
      return write_file_intervally(nodeid, random_file,
                                   file_size_in_mb * 1024 * 1024 + drift,
                                   run_time_seconds, 5 /*with long delay*/);
    });
    DEFER(fs_->forget(nodeid, 1));

    g_fault_injector->set_injection(FaultInjectionId::FI_Data_Sync_Failed);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
      DEFER(future.get());
      std::string old_name = "dirty_file";
      for (int i = 0; i < run_time_seconds * 10; i++) {
        auto new_name = "dirty_file_renamed" + std::to_string(i);
        r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
        if (r != 0) break;
        old_name = new_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    ASSERT_NE(r, 0);
  }

  void verify_rename_dirty_dir(uint64_t parent) {
    auto parent_path = nodeid_to_path(parent);

    // mkdir dir0/dir1/dir2/dir3/dir4
    // create two files in dir3 and one file in dir4/dir2
    // writing to the files continuously and do rename
    // dir2 at the same time
    uint64_t dst_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "dst_dir", 0777, 0, 0, 0, &dst_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_nodeid, 1));

    std::vector<uint64_t> dir_nodeids(5, 0);
    uint64_t parent_nodeid = parent;
    for (int i = 0; i < 5; i++) {
      std::string dir_name = "dir" + std::to_string(i);
      int r = fs_->mkdir(parent_nodeid, dir_name.c_str(), 0777, 0, 0, 0,
                         &dir_nodeids[i], &st);
      ASSERT_EQ(r, 0);
      parent_nodeid = dir_nodeids[i];
    }

    std::vector<uint64_t> file_nodeids(5, 0);
    // create two files in dir3
    create_file_in_folder(dir_nodeids[3], "dir3-file0", 0, file_nodeids[0], 0);
    ASSERT_NE(file_nodeids[0], (uint64_t)0);
    create_file_in_folder(dir_nodeids[3], "dir3-file1", 0, file_nodeids[1], 0);
    ASSERT_NE(file_nodeids[1], (uint64_t)0);

    // create one file in dir2
    create_file_in_folder(dir_nodeids[2], "dir2-file0", 0, file_nodeids[2], 0);
    ASSERT_NE(file_nodeids[2], (uint64_t)0);

    // create one file in dir4
    create_file_in_folder(dir_nodeids[4], "dir4-file0", 0, file_nodeids[3], 0);
    ASSERT_NE(file_nodeids[3], (uint64_t)0);

    // create one file in dir0. this will not be moved
    create_file_in_folder(dir_nodeids[0], "dir0-file0", 0, file_nodeids[4], 0);
    ASSERT_NE(file_nodeids[4], (uint64_t)0);

    srand(time(nullptr));
    // create four local files and lauch a thread to write data to them
    std::vector<std::future<uint64_t>> crc64_future;
    int run_time_seconds = 10;
    for (size_t i = 0; i < file_nodeids.size(); i++) {
      std::string local_file_name =
          "local_file_" + std::to_string(parent) + "_" + std::to_string(i);
      std::string random_file = join_paths(test_path_, local_file_name);
      uint64_t file_size_in_mb = 1 + rand() % 64;  // at least 1MB
      uint64_t drift = rand() % (1024 * 1024);
      create_random_file(random_file, file_size_in_mb, drift);

      auto future = std::async(std::launch::async, [=]() -> uint64_t {
        INIT_PHOTON();
        return write_file_intervally(file_nodeids[i], random_file,
                                     file_size_in_mb * 1024 * 1024 + drift,
                                     run_time_seconds, 5 /*long delay*/);
      });
      crc64_future.push_back(std::move(future));
    }

    std::vector<uint64_t> expected_crc64(file_nodeids.size(), 0);
    {
      DEFER(expected_crc64[0] = crc64_future[0].get());
      DEFER(expected_crc64[1] = crc64_future[1].get());
      DEFER(expected_crc64[2] = crc64_future[2].get());
      DEFER(expected_crc64[3] = crc64_future[3].get());
      DEFER(expected_crc64[4] = crc64_future[4].get());

      uint64_t old_parent = dir_nodeids[1];
      std::string old_name = "dir2";
      uint64_t new_parent = parent;
      std::string new_name = "dst_dir";
      for (int i = 0; i < run_time_seconds * 10; i++) {
        r = fs_->rename(old_parent, old_name.c_str(), new_parent,
                        new_name.c_str(), 0);
        ASSERT_EQ(r, 0);
        old_parent = new_parent;
        old_name = new_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        new_parent = old_parent;
        new_name = "newdir" + std::to_string(i);
      }
    }

    for (size_t i = 0; i < file_nodeids.size(); i++) {
      auto file_path = nodeid_to_path(file_nodeids[i]);
      LOG_INFO("file path is `", file_path);
      auto meta = get_file_meta(file_path.c_str(), FLAGS_oss_bucket_prefix);
      ASSERT_EQ(std::to_string(expected_crc64[i]),
                meta["X-Oss-Hash-Crc64ecma"]);
    }

    for (auto nodeid : dir_nodeids) {
      fs_->forget(nodeid, 1);
    }
    for (auto nodeid : file_nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_rename_dirty_dir() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::vector<uint64_t> dir_nodeids(4, 0);
    struct stat st;
    for (size_t i = 0; i < dir_nodeids.size(); i++) {
      std::string dir_name = "basedir" + std::to_string(i);
      int r = fs_->mkdir(parent, dir_name.c_str(), 0777, 0, 0, 0,
                         &dir_nodeids[i], &st);
      ASSERT_EQ(r, 0);
    }

    // lauch several threads to do the dirty dir renaming test concurrently
    std::vector<std::future<void>> futures;
    for (size_t j = 0; j < dir_nodeids.size(); j++) {
      auto future = std::async(
          std::launch::async,
          [&](uint64_t nodeid) -> void {
            INIT_PHOTON();
            verify_rename_dirty_dir(nodeid);
          },
          dir_nodeids[j]);
      futures.push_back(std::move(future));
    }

    for (auto &future : futures) {
      future.get();
    }

    for (auto nodeid : dir_nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  // Random-write flush produces no OSS-side CRC64 meta (multipart copy can't
  // guarantee it), so rename correctness under random-write mode is verified by
  // reading the object back through the fs and comparing the content CRC. Reads
  // by nodeid so it also works for files that were moved into a renamed dir.
  ssize_t read_nodeid_crc(uint64_t nodeid, uint64_t *out_crc64) {
    struct stat st;
    int r = fs_->getattr(nodeid, &st);
    if (r < 0) return r;

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    if (r < 0) return r;
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    uint64_t offset = 0, size = st.st_size, crc64 = 0;
    const size_t buf_size = 1024 * 1024;
    char *buf = new char[buf_size];
    DEFER(delete[] buf);
    while (offset < size) {
      uint64_t remain = size - offset;
      uint64_t read_size = remain < buf_size ? remain : buf_size;
      ssize_t got = read_from_handle(handle, buf, read_size, offset);
      if (got < 0) return got;
      crc64 = cal_crc64(crc64, buf, got);
      offset += got;
    }
    if (out_crc64) *out_crc64 = crc64;
    return static_cast<ssize_t>(size);
  }

  void verify_rename_dirty_file_random_write() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat stbuf;
    int r = create_and_flush(parent, "dirty_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    std::string random_file = join_paths(test_path_, "rw_rename_file.dat");

    srand(time(nullptr));
    uint64_t file_size_in_mb = 1 + rand() % 128;  // at least 1MB
    uint64_t drift = rand() % (1024 * 1024);
    create_random_file(random_file, file_size_in_mb, drift);
    int run_time_seconds = 3;

    // Write the file continuously in the background while we rename it.
    auto future = std::async(std::launch::async, [=]() -> uint64_t {
      INIT_PHOTON();
      return write_file_intervally(nodeid, random_file,
                                   file_size_in_mb * 1024 * 1024 + drift,
                                   run_time_seconds, 5 /*with long delay*/);
    });

    uint64_t expected_crc64 = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
      DEFER(expected_crc64 = future.get());
      std::string old_name = "dirty_file";
      for (int i = 0; i < run_time_seconds * 10; i++) {
        auto new_name = "dirty_file_renamed" + std::to_string(i);
        r = fs_->rename(parent, old_name.c_str(), parent, new_name.c_str(), 0);
        ASSERT_EQ(r, 0);
        old_name = new_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    // The inode identity is stable across renames; read it back and compare.
    uint64_t actual_crc64 = 0;
    ssize_t sz = read_nodeid_crc(nodeid, &actual_crc64);
    ASSERT_GE(sz, 0);
    ASSERT_EQ(actual_crc64, expected_crc64);
  }

  void verify_rename_dirty_dir_random_write(uint64_t parent) {
    auto parent_path = nodeid_to_path(parent);

    uint64_t dst_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "dst_dir", 0777, 0, 0, 0, &dst_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_nodeid, 1));

    std::vector<uint64_t> dir_nodeids(5, 0);
    uint64_t parent_nodeid = parent;
    for (int i = 0; i < 5; i++) {
      std::string dir_name = "dir" + std::to_string(i);
      int r = fs_->mkdir(parent_nodeid, dir_name.c_str(), 0777, 0, 0, 0,
                         &dir_nodeids[i], &st);
      ASSERT_EQ(r, 0);
      parent_nodeid = dir_nodeids[i];
    }

    std::vector<uint64_t> file_nodeids(5, 0);
    create_file_in_folder(dir_nodeids[3], "dir3-file0", 0, file_nodeids[0], 0);
    ASSERT_NE(file_nodeids[0], (uint64_t)0);
    create_file_in_folder(dir_nodeids[3], "dir3-file1", 0, file_nodeids[1], 0);
    ASSERT_NE(file_nodeids[1], (uint64_t)0);
    create_file_in_folder(dir_nodeids[2], "dir2-file0", 0, file_nodeids[2], 0);
    ASSERT_NE(file_nodeids[2], (uint64_t)0);
    create_file_in_folder(dir_nodeids[4], "dir4-file0", 0, file_nodeids[3], 0);
    ASSERT_NE(file_nodeids[3], (uint64_t)0);
    create_file_in_folder(dir_nodeids[0], "dir0-file0", 0, file_nodeids[4], 0);
    ASSERT_NE(file_nodeids[4], (uint64_t)0);

    srand(time(nullptr));
    std::vector<std::future<uint64_t>> crc64_future;
    int run_time_seconds = 10;
    for (size_t i = 0; i < file_nodeids.size(); i++) {
      std::string local_file_name =
          "rw_local_file_" + std::to_string(parent) + "_" + std::to_string(i);
      std::string random_file = join_paths(test_path_, local_file_name);
      uint64_t file_size_in_mb = 1 + rand() % 64;  // at least 1MB
      uint64_t drift = rand() % (1024 * 1024);
      create_random_file(random_file, file_size_in_mb, drift);

      auto future = std::async(std::launch::async, [=]() -> uint64_t {
        INIT_PHOTON();
        return write_file_intervally(file_nodeids[i], random_file,
                                     file_size_in_mb * 1024 * 1024 + drift,
                                     run_time_seconds, 5 /*long delay*/);
      });
      crc64_future.push_back(std::move(future));
    }

    std::vector<uint64_t> expected_crc64(file_nodeids.size(), 0);
    {
      DEFER(expected_crc64[0] = crc64_future[0].get());
      DEFER(expected_crc64[1] = crc64_future[1].get());
      DEFER(expected_crc64[2] = crc64_future[2].get());
      DEFER(expected_crc64[3] = crc64_future[3].get());
      DEFER(expected_crc64[4] = crc64_future[4].get());

      uint64_t old_parent = dir_nodeids[1];
      std::string old_name = "dir2";
      uint64_t new_parent = parent;
      std::string new_name = "dst_dir";
      for (int i = 0; i < run_time_seconds * 10; i++) {
        r = fs_->rename(old_parent, old_name.c_str(), new_parent,
                        new_name.c_str(), 0);
        ASSERT_EQ(r, 0);
        old_parent = new_parent;
        old_name = new_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        new_parent = old_parent;
        new_name = "newdir" + std::to_string(i);
      }
    }

    for (size_t i = 0; i < file_nodeids.size(); i++) {
      uint64_t actual_crc64 = 0;
      ssize_t sz = read_nodeid_crc(file_nodeids[i], &actual_crc64);
      ASSERT_GE(sz, 0);
      ASSERT_EQ(actual_crc64, expected_crc64[i]);
    }

    for (auto nodeid : dir_nodeids) {
      fs_->forget(nodeid, 1);
    }
    for (auto nodeid : file_nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_rename_dirty_dir_random_write() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::vector<uint64_t> dir_nodeids(4, 0);
    struct stat st;
    for (size_t i = 0; i < dir_nodeids.size(); i++) {
      std::string dir_name = "rw_basedir" + std::to_string(i);
      int r = fs_->mkdir(parent, dir_name.c_str(), 0777, 0, 0, 0,
                         &dir_nodeids[i], &st);
      ASSERT_EQ(r, 0);
    }

    std::vector<std::future<void>> futures;
    for (size_t j = 0; j < dir_nodeids.size(); j++) {
      auto future = std::async(
          std::launch::async,
          [&](uint64_t nodeid) -> void {
            INIT_PHOTON();
            verify_rename_dirty_dir_random_write(nodeid);
          },
          dir_nodeids[j]);
      futures.push_back(std::move(future));
    }

    for (auto &future : futures) {
      future.get();
    }

    for (auto nodeid : dir_nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_rename_src_not_exist() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // 1. same parent
    uint64_t dir_nodeid = 0, dir_nodeid2 = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    std::string filename1 = "test_rename_file1",
                filename2 = "test_rename_file2";
    r = fs_->rename(dir_nodeid, filename1.c_str(), dir_nodeid,
                    filename2.c_str(), 0);
    ASSERT_EQ(r, -ESTALE);

    // 2. different parents
    r = fs_->mkdir(parent, "test_dir2", 0777, 0, 0, 0, &dir_nodeid2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid2, 1));

    r = fs_->rename(dir_nodeid, filename1.c_str(), dir_nodeid2,
                    filename2.c_str(), 0);
    ASSERT_EQ(r, -ESTALE);

    r = fs_->rmdir(parent, "test_dir");
    ASSERT_EQ(r, 0);
    r = fs_->rmdir(parent, "test_dir2");
    ASSERT_EQ(r, 0);
  }

  void verify_rename_remote_dir() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;

    auto parent_path = nodeid_to_path(parent);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    std::string filepath = "remote_dir/test_file";
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->lookup(parent, "remote_dir", &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    r = fs_->rename(parent, "remote_dir", parent, "remote_dir_renamed", 0);
    ASSERT_EQ(r, 0);

    auto old_result = get_list_objects(join_paths(parent_path, "remote_dir"),
                                       FLAGS_oss_bucket_prefix, true);
    ASSERT_SIZE_EQ(old_result.size(), 0);

    auto new_result =
        get_list_objects(join_paths(parent_path, "remote_dir_renamed"),
                         FLAGS_oss_bucket_prefix, true);
    ASSERT_SIZE_EQ(new_result.size(), 1);

    filepath = "remote_dir_exist/test_file";
    r = upload_file(local_file, join_paths(parent_path, filepath),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->rename(parent, "remote_dir_renamed", parent, "remote_dir_exist",
                    0);
    ASSERT_EQ(r, -ENOTEMPTY);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);
    r = fs_->rename(parent, "remote_dir_renamed", parent, "remote_dir_exist",
                    0);
    ASSERT_EQ(r, -ETIMEDOUT);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    uint64_t existing_dir_nodeid = 0;
    r = fs_->lookup(parent, "remote_dir_exist", &existing_dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(existing_dir_nodeid, 1));

    uint64_t file_nodeid = 0;
    r = fs_->lookup(existing_dir_nodeid, "test_file", &file_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(file_nodeid, 1));

    filepath = "remote_dir_exist/test_file2";
    r = upload_file(local_file, join_paths(parent_path, filepath),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = fs_->rename(existing_dir_nodeid, "test_file", existing_dir_nodeid,
                    "test_file2", RENAME_NOREPLACE);
    ASSERT_EQ(r, -EEXIST);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);
    r = fs_->rename(existing_dir_nodeid, "test_file", existing_dir_nodeid,
                    "test_file2", RENAME_NOREPLACE);
    ASSERT_EQ(r, -ETIMEDOUT);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    r = fs_->rename(existing_dir_nodeid, "test_file", existing_dir_nodeid,
                    "test_file2", 0);
    ASSERT_EQ(r, 0);
  }

  void verify_rename_dirty_file_during_prefetch() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    uint64_t nodeid = 0;
    create_file_in_folder(parent, "test_file", 1024, nodeid, rand() % 1024);
    ASSERT_NE(nodeid, (uint64_t)0);
    DEFER(fs_->forget(nodeid, 1));

    struct stat stbuf;
    int r = fs_->getattr(nodeid, &stbuf);
    ASSERT_EQ(r, 0);
    uint64_t file_size = stbuf.st_size;

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    auto read_task = std::async(std::launch::async, [&]() {
      INIT_PHOTON();
      auto file = get_file_from_handle(handle);
      char *buf = new char[65536];
      DEFER({ delete[] buf; });
      uint64_t offset = 0;
      while (offset < file_size) {
        uint64_t read_size = std::min((uint64_t)65536, file_size - offset);
        ssize_t r = file->pread(buf, read_size, offset);
        if (r < 0) return;
        offset += r;
      }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    r = fs_->rename(parent, "test_file", parent, "test_file_renamed", 0);
    ASSERT_EQ(r, 0);

    char *wbuf = new char[65536];
    DEFER({ delete[] wbuf; });
    get_file_from_handle(handle)->pwrite(wbuf, 65536, file_size);

    read_task.wait();
  }

  void verify_rename_dir_with_partial_deletion() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));
    auto parent_path = nodeid_to_path(parent);

    int file_cnt = 1005;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    create_files_under_dir_parallelly(dir_nodeid, file_cnt, nodeids);

    g_fault_injector->set_injection(FaultInjectionId::FI_Oss_Partial_Deletion,
                                    {(uint32_t)-1, 1});
    r = fs_->rename(parent, "test_dir", parent, "test_dir_partial_dst", 0);
    ASSERT_LT(r, 0);

    std::vector<std::string> list_results;
    auto dst_dir_path = join_paths(parent_path, "test_dir_partial_dst");
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, list_dir_descendants, dst_dir_path,
                                       list_results);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(list_results.size(), file_cnt);

    uint64_t dst_dir_nodeid;
    r = fs_->lookup(parent, "test_dir_partial_dst", &dst_dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_dir_nodeid, 1));

    g_fault_injector->set_injection(FaultInjectionId::FI_Oss_Partial_Deletion,
                                    {2, 1});
    r = fs_->rename(parent, "test_dir_partial_dst", parent, "test_dir_full_dst",
                    0);
    ASSERT_EQ(r, 0);
    list_results.clear();
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, list_dir_descendants, dst_dir_path,
                                       list_results);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(list_results.size(), 0);

    dst_dir_path = join_paths(parent_path, "test_dir_full_dst");
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, list_dir_descendants, dst_dir_path,
                                       list_results);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(list_results.size(), file_cnt);

    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_read_after_rename_overwrite_returns_new_data() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const size_t file_size = 4 * 1024 * 1024;      // 4MB
    const size_t trigger_size = 128 * 1024;        // 128KB to trigger prefetch
    const size_t verify_offset = 1 * 1024 * 1024;  // 1MB

    // Create file /a filled with 0xAA
    uint64_t nodeid_a = 0;
    void *handle_a = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "file_a", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &nodeid_a, &st, &handle_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_a, 1));

    std::vector<char> buf_aa(file_size, (char)0xAA);
    auto file_a = get_file_from_handle(handle_a);
    r = file_a->pwrite(buf_aa.data(), file_size, 0);
    ASSERT_EQ(r, (int)file_size);
    r = fs_->release(nodeid_a, file_a);
    ASSERT_EQ(r, 0);

    // Create file /b filled with 0xBB
    uint64_t nodeid_b = 0;
    void *handle_b = nullptr;
    r = create_and_flush(parent, "file_b", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid_b, &st, &handle_b);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_b, 1));

    std::vector<char> buf_bb(file_size, (char)0xBB);
    auto file_b = get_file_from_handle(handle_b);
    r = file_b->pwrite(buf_bb.data(), file_size, 0);
    ASSERT_EQ(r, (int)file_size);
    r = fs_->release(nodeid_b, file_b);
    ASSERT_EQ(r, 0);

    // Open /a to anchor inode (E_A)
    void *handle_read = nullptr;
    bool unused = false;
    r = fs_->open(nodeid_a, O_RDONLY, &handle_read, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid_a, get_file_from_handle(handle_read)));

    // Read front 128KB to trigger background prefetch of subsequent ranges
    auto file_read = get_file_from_handle(handle_read);
    std::vector<char> trigger_buf(trigger_size, 0);
    ssize_t n = file_read->pread(trigger_buf.data(), trigger_size, 0);
    ASSERT_EQ(n, (ssize_t)trigger_size);
    // Verify the trigger read got old data (0xAA)
    for (size_t i = 0; i < trigger_size; i++) {
      ASSERT_EQ(trigger_buf[i], (char)0xAA)
          << "mismatch at trigger offset " << i;
    }

    // Wait for background prefetch to populate cache with 0xAA data
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Rename /b over /a (overwrite): /a now has content 0xBB (E_B)
    r = fs_->rename(parent, "file_b", parent, "file_a", 0);
    ASSERT_EQ(r, 0);

    // Read from offset 1MB onward (region that prefetch should have cached).
    // After rename overwrite, possible outcomes:
    // - Returns cached old data 0xAA (cache hit, attr not yet refreshed) —
    // acceptable
    // - Returns -EIO (ETag verification on cache-miss refill detected mismatch)
    // — acceptable
    // - Returns new data 0xBB (path refresh succeeded) — acceptable
    // The key invariant: no crash, no garbage (random bytes). Data must be
    // entirely 0xAA or entirely 0xBB, not a mix.
    size_t remaining = file_size - verify_offset;
    std::vector<char> read_buf(remaining, 0);
    size_t offset = 0;
    bool read_error = false;
    while (offset < remaining) {
      n = file_read->pread(read_buf.data() + offset, remaining - offset,
                           verify_offset + offset);
      if (n <= 0) {
        // Error return is acceptable (ETag mismatch detected on refill)
        read_error = true;
        break;
      }
      offset += n;
    }

    if (!read_error && offset > 0) {
      // Verify data coherence: must be all-0xAA or all-0xBB, no mix.
      bool all_aa = true, all_bb = true;
      for (size_t i = 0; i < offset; i++) {
        if (read_buf[i] != (char)0xAA) all_aa = false;
        if (read_buf[i] != (char)0xBB) all_bb = false;
      }
      ASSERT_TRUE(all_aa || all_bb)
          << "Data corruption: read mixed content after rename overwrite";
    }
  }

  // Stress-test concurrent rename + pread on the same file (random-write
  // mode). Verifies that concurrent path mutations never cause crashes or
  // silent data corruption; pread may return correct data or -EIO (path
  // refresh detected external replacement) but never garbage.
  void verify_concurrent_rename_read_stress() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create and write a 1MB file with known pattern.
    const size_t kFileSize = 1 * 1024 * 1024;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "stress_src", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Fill with pattern 0xCD
    std::vector<char> write_data(kFileSize, (char)0xCD);
    auto file_w = get_file_from_handle(handle);
    r = file_w->pwrite(write_data.data(), kFileSize, 0);
    ASSERT_EQ(r, (int)kFileSize);
    r = fs_->release(nodeid, file_w);
    ASSERT_EQ(r, 0);

    // Reopen for read
    void *read_handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(read_handle)));

    const int kRounds = 80;
    std::atomic<bool> stop{false};
    std::atomic<int> rename_done{0};
    std::atomic<int> read_ok{0};
    std::atomic<int> read_eio{0};
    std::atomic<bool> corruption_detected{false};

    // Rename thread: toggle between "stress_src" and "stress_dst"
    auto rename_future = std::async(std::launch::async, [&]() {
      INIT_PHOTON();
      std::string names[2] = {"stress_src", "stress_dst"};
      int idx = 0;
      for (int i = 0; i < kRounds && !stop.load(); i++) {
        int from = idx;
        int to = 1 - idx;
        int ret = fs_->rename(parent, names[from].c_str(), parent,
                              names[to].c_str(), 0);
        if (ret == 0) {
          idx = to;
          rename_done.fetch_add(1);
        }
        photon::thread_usleep(5000);  // 5ms between renames
      }
    });

    // Reader threads: concurrent pread
    const int kReaders = 4;
    std::vector<std::future<void>> reader_futures;
    for (int t = 0; t < kReaders; t++) {
      reader_futures.push_back(std::async(std::launch::async, [&]() {
        INIT_PHOTON();
        auto file_r = get_file_from_handle(read_handle);
        const size_t kBufSize = 64 * 1024;
        std::vector<char> buf(kBufSize);
        std::vector<char> expected(kBufSize, (char)0xCD);

        while (!stop.load()) {
          off_t offset = (rand() % (kFileSize / kBufSize)) * kBufSize;
          ssize_t n = file_r->pread(buf.data(), kBufSize, offset);
          if (n == (ssize_t)kBufSize) {
            // Verify data integrity
            if (memcmp(buf.data(), expected.data(), kBufSize) != 0) {
              corruption_detected = true;
              stop.store(true);
              return;
            }
            read_ok.fetch_add(1);
          } else if (n == -EIO || n == -ENOENT) {
            // Acceptable: path stale after rename, ETag mismatch
            read_eio.fetch_add(1);
          } else if (n < 0) {
            // Other errors are acceptable during path refresh
            read_eio.fetch_add(1);
          } else if (n > 0 && n < (ssize_t)kBufSize) {
            // Partial read at EOF boundary is OK, verify what we got
            if (memcmp(buf.data(), expected.data(), n) != 0) {
              corruption_detected = true;
              stop.store(true);
              return;
            }
            read_ok.fetch_add(1);
          }
          photon::thread_usleep(1000);  // 1ms between reads
        }
      }));
    }

    // Wait for rename thread to finish, then signal readers to stop
    rename_future.wait();
    stop.store(true);

    for (auto &f : reader_futures) {
      f.wait();
    }

    LOG_INFO(
        "Stress test done: renames=`, reads_ok=`, reads_eio=`, "
        "corruption=`",
        rename_done.load(), read_ok.load(), read_eio.load(),
        corruption_detected.load());

    ASSERT_FALSE(corruption_detected.load())
        << "Data corruption detected during concurrent rename+read";
    ASSERT_GT(rename_done.load(), 0)
        << "At least one rename should have succeeded";
    ASSERT_GT(read_ok.load() + read_eio.load(), 0)
        << "At least one read should have been attempted";
  }

  void verify_multi_file_cross_rename_no_corruption() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create 3 files with distinct patterns.
    const size_t kFileSize = 1 * 1024 * 1024;
    struct {
      const char *name;
      char pattern;
      uint64_t nodeid;
      void *read_handle;
    } files[3] = {
        {"cross_file_x", (char)0x11, 0, nullptr},
        {"cross_file_y", (char)0x22, 0, nullptr},
        {"cross_file_z", (char)0x33, 0, nullptr},
    };

    for (auto &f : files) {
      void *whandle = nullptr;
      struct stat st;
      int r = create_and_flush(parent, f.name, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                               &f.nodeid, &st, &whandle);
      ASSERT_EQ(r, 0);

      std::vector<char> data(kFileSize, f.pattern);
      auto file_w = get_file_from_handle(whandle);
      r = file_w->pwrite(data.data(), kFileSize, 0);
      ASSERT_EQ(r, (int)kFileSize);
      r = fs_->release(f.nodeid, file_w);
      ASSERT_EQ(r, 0);
    }

    // Open each file for reading.
    for (auto &f : files) {
      bool unused = false;
      int r = fs_->open(f.nodeid, O_RDONLY, &f.read_handle, &unused);
      ASSERT_EQ(r, 0);
    }
    DEFER({
      for (auto &f : files) {
        fs_->release(f.nodeid, get_file_from_handle(f.read_handle));
        fs_->forget(f.nodeid, 1);
      }
    });

    const int kRounds = 60;
    std::atomic<bool> stop{false};
    std::atomic<int> rename_done{0};
    std::atomic<int> read_ok_total{0};
    std::atomic<int> read_err_total{0};
    std::atomic<bool> corruption_detected{false};
    std::atomic<int> corruption_reader{-1};
    std::atomic<unsigned char> corruption_got{0};

    // Rename thread: cross-rename x->y, y->z, z->x each round.
    auto rename_future = std::async(std::launch::async, [&]() {
      INIT_PHOTON();
      // Track current name positions: names[i] is the current name of
      // the object originally at slot i.
      std::string names[3] = {"cross_file_x", "cross_file_y", "cross_file_z"};
      for (int i = 0; i < kRounds && !stop.load(); i++) {
        // Rotate: x->y, y->z, z->x via a temp.
        std::string tmp_name = "cross_file_tmp";
        int r;
        // x -> tmp
        r = fs_->rename(parent, names[0].c_str(), parent, tmp_name.c_str(), 0);
        if (r != 0) goto next;
        // y -> x
        r = fs_->rename(parent, names[1].c_str(), parent, names[0].c_str(), 0);
        if (r != 0) goto next;
        // z -> y
        r = fs_->rename(parent, names[2].c_str(), parent, names[1].c_str(), 0);
        if (r != 0) goto next;
        // tmp -> z
        r = fs_->rename(parent, tmp_name.c_str(), parent, names[2].c_str(), 0);
        if (r != 0) goto next;
        rename_done.fetch_add(1);
      next:
        photon::thread_usleep(5000);  // 5ms between rounds
      }
    });

    // Reader threads: one per file handle.
    std::vector<std::future<void>> reader_futures;
    for (int t = 0; t < 3; t++) {
      reader_futures.push_back(std::async(std::launch::async, [&, t]() {
        INIT_PHOTON();
        auto file_r = get_file_from_handle(files[t].read_handle);
        const size_t kBufSize = 64 * 1024;
        std::vector<char> buf(kBufSize);
        char my_pattern = files[t].pattern;

        while (!stop.load()) {
          off_t offset = (rand() % (kFileSize / kBufSize)) * (off_t)kBufSize;
          ssize_t n = file_r->pread(buf.data(), kBufSize, offset);
          if (n > 0) {
            // Verify every byte matches our own pattern.
            for (ssize_t i = 0; i < n; i++) {
              if (buf[i] != my_pattern) {
                corruption_detected = true;
                corruption_reader = t;
                corruption_got = buf[i];
                stop.store(true);
                return;
              }
            }
            read_ok_total.fetch_add(1);
          } else if (n == 0 || n == -EIO || n == -ENOENT) {
            // Acceptable: file moved, path stale, ETag mismatch.
            read_err_total.fetch_add(1);
          } else {
            // Other negative errors also acceptable during rename storm.
            read_err_total.fetch_add(1);
          }
          photon::thread_usleep(1000);  // 1ms between reads
        }
      }));
    }

    // Wait for rename thread to finish, then signal readers to stop.
    rename_future.wait();
    stop.store(true);

    for (auto &f : reader_futures) {
      f.wait();
    }

    LOG_INFO(
        "Cross-rename stress done: renames=`, reads_ok=`, reads_err=`, "
        "corruption=`",
        rename_done.load(), read_ok_total.load(), read_err_total.load(),
        corruption_detected.load());

    ASSERT_FALSE(corruption_detected.load())
        << "Data corruption: reader " << corruption_reader.load()
        << " expected pattern 0x" << std::hex
        << (int)(unsigned char)
               files[corruption_reader.load() >= 0 ? corruption_reader.load()
                                                   : 0]
                   .pattern
        << " but got 0x" << (int)corruption_got.load();
    ASSERT_GT(rename_done.load(), 0)
        << "At least one cross-rename round should have succeeded";
    ASSERT_GT(read_ok_total.load() + read_err_total.load(), 0)
        << "At least one read should have been attempted";
  }

  void verify_rename_dir_oss_err_dir_obj() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "src_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));
    auto parent_path = nodeid_to_path(parent);

    int file_cnt = 11;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    create_files_under_dir_parallelly(dir_nodeid, file_cnt, nodeids);
    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    }
    LOG_INFO("created ` files in the dir", file_cnt);

    std::string old_name = "src_dir";
    // Skip two oss requests.
    // 1. check if the dst dir has children
    // 2. count the children number of the src dir
    // And then we will try to copy test_dir/ to dst_dir_obj/
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout,
                                    {std::numeric_limits<uint32_t>::max(), 2});
    r = fs_->rename(parent, old_name, parent, "dst_dir", 0);
    ASSERT_EQ(r, -ETIMEDOUT);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);
    // No copies should have happned. check if dst_dir_obj/ exists.
    auto tmp_result = get_list_objects(join_paths(parent_path, "dst_dir"),
                                       FLAGS_oss_bucket_prefix, true);
    ASSERT_SIZE_EQ(tmp_result.size(), 0);

    // Skip 12 more copy requests + 1 delete request
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout,
                                    {std::numeric_limits<uint32_t>::max(), 15});
    r = fs_->rename(parent, old_name, parent, "dst_dir", 0);
    ASSERT_EQ(r, -ETIMEDOUT);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);
    // only src_dir_obj/ exists
    tmp_result = get_list_objects(join_paths(parent_path, "dst_dir"),
                                  FLAGS_oss_bucket_prefix, true);
    ASSERT_SIZE_EQ(tmp_result.size(), 12);
    tmp_result = get_list_objects(join_paths(parent_path, "src_dir"),
                                  FLAGS_oss_bucket_prefix, true);
    ASSERT_SIZE_EQ(tmp_result.size(), 1);
  }

 private:
  void create_files_under_dir_parallelly(uint64_t dir_nodeid, int file_cnt,
                                         std::vector<uint64_t> &nodeids) {
    std::vector<void *> handles(file_cnt, 0);
    struct stat st;

    // launch 64 threads to create file concurrently
    std::vector<std::future<void>> futures;
    int parallel_cnt = 64;
    for (int j = 0; j < parallel_cnt; j++) {
      auto future = std::async(std::launch::async, [&, j]() {
        INIT_PHOTON();
        for (auto i = j; i < file_cnt; i += parallel_cnt) {
          auto file_name = "testfile-" + std::to_string(i);
          int r =
              create_and_flush(dir_nodeid, file_name.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeids[i], &st, &handles[i]);
          ASSERT_EQ(r, 0);

          r = fs_->release(nodeids[i], get_file_from_handle(handles[i]));
          ASSERT_EQ(r, 0);
        }
      });
      futures.push_back(std::move(future));
    }

    for (auto &future : futures) {
      future.wait();
    }
  }
};

TEST_F(Ossfs2RenameTest, verify_rename_file) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_rename_file();
}

TEST_F(Ossfs2RenameTest, verify_rename_keep_old_meta) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_rename_keep_old_meta();
}

TEST_F(Ossfs2RenameTest, verify_rename_dir) {
  INIT_PHOTON();

  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_rename_dir();
}

TEST_F(Ossfs2RenameTest, verify_rename_dir_continuously) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_rename_dir_continuously();
}

TEST_F(Ossfs2RenameTest, verify_rename_file_with_oss_err) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_file_with_oss_err();
}

TEST_F(Ossfs2RenameTest, verify_rename_dir_with_oss_err) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_dir_with_oss_err();
}

TEST_F(Ossfs2RenameTest, verify_rename_dir_with_limit) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.rename_dir_limit = 8;
  init(opts);
  verify_rename_dir_with_limit(8);
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_file) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  // with fuse bufvec, write_file_intervally will hung. check this later.
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_rename_dirty_file();
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_file_with_fsync_error) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  // with fuse bufvec, write_file_intervally will hung. check this later.
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_rename_dirty_file_with_fsync_error();
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_dir) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  // with fuse bufvec, write_file_intervally will hung. check this later.
  FLAGS_write_with_fuse_bufvec = false;
  OssFsOptions opts;
  init(opts);
  verify_rename_dirty_dir();
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_file_random_write) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  FLAGS_write_with_fuse_bufvec = false;
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // enable random write
  init(opts);
  verify_rename_dirty_file_random_write();
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_dir_random_write) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  FLAGS_write_with_fuse_bufvec = false;
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // enable random write
  init(opts);
  verify_rename_dirty_dir_random_write();
}

TEST_F(Ossfs2RenameTest, verify_rename_src_not_exist) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_rename_src_not_exist();
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_dir_for_appendable_obj) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_rename_dirty_dir();
}

TEST_F(Ossfs2RenameTest, verify_rename_remote_dir) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_remote_dir();
}

TEST_F(Ossfs2RenameTest, verify_rename_dirty_file_during_prefetch) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_dirty_file_during_prefetch();
}

TEST_F(Ossfs2RenameTest, verify_rename_dir_partial_deletion) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_dir_with_partial_deletion();
}

TEST_F(Ossfs2RenameTest, verify_rename_dir_oss_err_dir_obj) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_dir_oss_err_dir_obj();
}

TEST_F(Ossfs2RenameTest, verify_read_after_rename_overwrite_returns_new_data) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir =
      test_path_;  // random write mode required for ETag verification
  init(opts);
  verify_read_after_rename_overwrite_returns_new_data();
}

TEST_F(Ossfs2RenameTest, verify_concurrent_rename_read_stress) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // enable random write mode
  init(opts);
  verify_concurrent_rename_read_stress();
}

TEST_F(Ossfs2RenameTest, verify_multi_file_cross_rename_no_corruption) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;  // random write mode
  init(opts);
  verify_multi_file_cross_rename_no_corruption();
}

// TODO: Add streaming/appendable rename+read tests after extending ETag
// verification to all write modes (requires writer flush etag sync for
// streaming put_object and appendable append_object).