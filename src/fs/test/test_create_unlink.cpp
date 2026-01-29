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

#include "test_suite.h"

class Ossfs2CreateUnlinkTest : public Ossfs2TestSuite {
 protected:
  void verify_basic_create_unlink_files() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    void *handle = nullptr;
    std::vector<uint64_t> nodeids;
    for (int i = 1; i <= 10; i++) {
      std::string filepath = "testfile_" + std::to_string(i);
      uint64_t nodeid = 0;
      int r = create_and_flush(parent, filepath.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      nodeids.push_back(nodeid);

      r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
      ASSERT_EQ(r, 0);

      r = stat_file(filepath, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    // create when the target exists
    uint64_t noid = 0;
    int rr = create_and_flush(parent, "testfile_3", CREATE_BASE_FLAGS, 0777, 0,
                              0, 0, &noid, &st, &handle);
    ASSERT_EQ(rr, -EEXIST);

    rr = fs_->mkdir(parent, "testfile_3", 0777, 0, 0, 0, &noid, &st);
    ASSERT_EQ(rr, -EEXIST);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    rr = create_and_flush(parent, "testfile_new", CREATE_BASE_FLAGS, 0777, 0, 0,
                          0, &noid, &st, &handle);
    ASSERT_EQ(rr, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    for (int i = 1; i <= 10; i++) {
      std::string filepath = "testfile_" + std::to_string(i);
      int r = fs_->unlink(parent, filepath.c_str());
      ASSERT_EQ(r, 0);
    }

    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    };
  }

  void verify_unlink_dirty_file() {
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
    int run_time_seconds = 5;

    // lanch a thread to read data from the local file and write the data to
    // the file. the write is supposed be completed in task_time seconds.
    auto future = std::async(std::launch::async, [=]() -> uint64_t {
      INIT_PHOTON();
      return write_file_intervally(nodeid, random_file,
                                   file_size_in_mb * 1024 * 1024 + drift,
                                   run_time_seconds);
    });
    DEFER(fs_->forget(nodeid, 1));

    photon::thread_usleep(1000000);

    std::string full_path = nodeid_to_path(nodeid);

    r = fs_->unlink(parent, "dirty_file");
    ASSERT_EQ(r, 0);

    future.wait();

    r = stat_file(full_path, FLAGS_oss_bucket_prefix);
    ASSERT_NE(r, 0);
    struct stat st;
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, -ESTALE);
  }

  void verify_unlink_rmdir() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // 1. unlink a file that doesn't exist
    int r = fs_->unlink(parent, "testfile");
    ASSERT_EQ(r, -ENOENT);

    // 2. unlink a stale file
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::string filepath = "testfile";
    r = upload_file(local_file, join_paths(parent_path, filepath),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid, dir_nodeid;
    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(nodeid, 1));

    // unlink req returns a non ENOENT error
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->unlink(parent, filepath.c_str());
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    r = fs_->unlink(parent, filepath.c_str());
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, filepath.c_str());
    ASSERT_EQ(r, -ESTALE);

    // 3. rmdir a dir that doesn't exist
    r = fs_->rmdir(parent, "testdir");
    ASSERT_EQ(r, -ENOENT);

    // 4. rmdir a stale dir
    r = create_dir(join_paths(parent_path, "testdir"), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->lookup(parent, "testdir", &dir_nodeid, &st);
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(dir_nodeid, 1));

    uint64_t file_nodeid = 0;
    void *file_handle = nullptr;
    std::string filename = "testfile";
    r = create_and_flush(dir_nodeid, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                         0, 0, 0, &file_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(file_nodeid, get_file_from_handle(file_handle));
    ASSERT_EQ(r, 0);

    r = fs_->rmdir(parent, "testdir");
    ASSERT_EQ(r, -ENOTEMPTY);

    r = fs_->unlink(dir_nodeid, "testfile");
    ASSERT_EQ(r, 0);
    fs_->forget(file_nodeid, 1);

    // unlink req returns a non ENOENT error
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->rmdir(parent, "testdir");
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    // mark dir as stale
    r = fs_->rmdir(parent, "testdir");
    ASSERT_EQ(r, 0);
    r = fs_->rmdir(parent, "testdir");
    ASSERT_EQ(r, -ESTALE);

    r = fs_->unlink(dir_nodeid, "whatever_file");
    ASSERT_EQ(r, -ESTALE);

    r = fs_->rmdir(dir_nodeid, "whatever_dir");
    ASSERT_EQ(r, -ESTALE);
  }
};

TEST_F(Ossfs2CreateUnlinkTest, verify_basic_create_unlink_files) {
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_basic_create_unlink_files();
}

TEST_F(Ossfs2CreateUnlinkTest, verify_unlink_dirty_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_unlink_dirty_file();
}

TEST_F(Ossfs2CreateUnlinkTest, verify_unlink_rmdir) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_unlink_rmdir();
}
