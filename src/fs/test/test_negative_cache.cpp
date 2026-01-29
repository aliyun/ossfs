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

class Ossfs2NegativeCacheTest : public Ossfs2TestSuite {
 protected:
  void verify_negative_cache_create() {
    // generally negative cache ttl should be smaller than attr_timeout
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    void *handle = nullptr;
    struct stat st;

    const int oss_call_fault_cnt = oss_options_.retry_times + 1;

    // case 1. lookup a not-existing file, add it to the negative cache,
    //         the following create does need to send an OSS req
    std::string name1("file1");
    uint64_t nodeid1 = 0;

    int r = fs_->lookup(parent, name1.c_str(), &nodeid1, &st);
    ASSERT_EQ(r, -ENOENT);

    srand(time(nullptr));
    std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 300 + 500));
    // oss_stat contains a HeadObj and a ListObj req
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(2 * oss_call_fault_cnt));
    r = fs_->creat(parent, name1.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid1, &st, &handle);
    ASSERT_EQ(r, 0);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    r = fs_->release(nodeid1, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);

    // mark_clean() resets inode's attr_time
    std::this_thread::sleep_for(std::chrono::seconds(1));
    r = fs_->lookup(parent, name1.c_str(), &nodeid1, &st);
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, name1.c_str());
    ASSERT_EQ(r, 0);
    fs_->forget(nodeid1, 2);

    // case 2. add stale inodes to the negative cache
    std::string name2("file2");
    uint64_t nodeid2, nodeid3;

    // add to the negative cache
    r = fs_->lookup(parent, name2.c_str(), &nodeid2, &st);
    ASSERT_EQ(r, -ENOENT);

    r = upload_file(local_file, join_paths(parent_path, name2),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // verify negative cache timeout
    std::this_thread::sleep_for(std::chrono::seconds(1));
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(oss_call_fault_cnt));
    r = fs_->creat(parent, name2.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid2, &st, &handle);
    ASSERT_EQ(r, -EIO);

    r = fs_->lookup(parent, name2.c_str(), &nodeid2, &st);
    ASSERT_EQ(r, 0);

    r = delete_file(join_paths(parent_path, name2), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // mark stale and add to the negative cache again
    std::this_thread::sleep_for(std::chrono::seconds(2));
    r = fs_->lookup(parent, name2.c_str(), &nodeid3, &st);
    ASSERT_EQ(r, -ENOENT);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(2 * oss_call_fault_cnt));
    r = fs_->creat(parent, name2.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid3, &st, &handle);
    ASSERT_EQ(r, 0);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    r = fs_->release(nodeid1, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid2, 1);
    fs_->forget(nodeid3, 1);
  }

  void verify_negative_cache_lookup() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    void *handle = nullptr;
    struct stat st;
    const int oss_call_fault_cnt = oss_options_.retry_times + 1;
    int r = 0;

    uint64_t nodeid;
    for (int i = 0; i < 4; i++) {
      std::string name = "file_" + std::to_string(i);
      r = fs_->lookup(parent, name.c_str(), &nodeid, &st);
      ASSERT_EQ(r, -ENOENT);
    }

    // Now file_0 should not be in the negative cache
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    r = fs_->lookup(parent, "file_0", &nodeid, &st);
    ASSERT_EQ(r, -EIO);  // Not in the negative cache.
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // case 1:
    // 1. Lookup a non-existing file
    // 2. Disable the OSS req, and lookup this file again
    // 3. Wait for the negative cache timeout, and re-lookup
    std::string name1("file1"), name2("file2"), name3("file3"), name4("file4");
    uint64_t nodeid1 = 0, nodeid2 = 0, nodeid3 = 0, nodeid4 = 0;

    // inserted to the negative cache
    r = fs_->lookup(parent, name1.c_str(), &nodeid1, &st);
    ASSERT_EQ(r, -ENOENT);

    srand(time(nullptr));
    std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 300 + 500));
    // oss_stat contains a HeadObj and a ListObj req
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(4 * oss_call_fault_cnt));
    r = fs_->lookup(parent, name1.c_str(), &nodeid1, &st);
    ASSERT_EQ(r, -ENOENT);

    // the negative cache timeout
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    r = fs_->lookup(parent, name1.c_str(), &nodeid1, &st);
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // case 2:
    // 1. Lookup non-existing 3 files: file2, file3, file4
    // 2. Create file2. Looking up file2 is supposed to succeed.
    // 3. Rename file2 to file3. Looking up file3 is supposed to succeed.
    // 4. Upload file4. Readdirplus parent dir. Looking up file4 is supposed to
    // succeed.

    fs_->options_.attr_timeout = 2;
    r = fs_->lookup(parent, name2.c_str(), &nodeid2, &st);
    ASSERT_EQ(r, -ENOENT);

    r = create_and_flush(parent, name2.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0,
                         0, &nodeid2, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid2, reinterpret_cast<IFileHandleFuseLL *>(handle));
    ASSERT_EQ(r, 0);
    r = fs_->lookup(parent, name2.c_str(), &nodeid2, &st);
    ASSERT_EQ(r, 0);

    // wait for the attr to timeout
    std::this_thread::sleep_for(std::chrono::seconds(2));
    r = fs_->lookup(parent, name3.c_str(), &nodeid3, &st);
    ASSERT_EQ(r, -ENOENT);

    r = fs_->rename(parent, name2.c_str(), parent, name3.c_str(), 0);
    ASSERT_EQ(r, 0);
    r = fs_->lookup(parent, name3.c_str(), &nodeid3, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(nodeid2, nodeid3);

    r = fs_->lookup(parent, name4.c_str(), &nodeid4, &st);
    ASSERT_EQ(r, -ENOENT);
    r = upload_file(local_file, join_paths(parent_path, name4),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    void *dirp = nullptr;
    r = fs_->opendir(parent, &dirp);
    ASSERT_EQ(r, 0);
    std::vector<TestInode> childs;
    r = fs_->readdir(parent, 0, dirp, filler, &childs, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);
    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    r = fs_->lookup(parent, name4.c_str(), &nodeid4, &st);
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid2, 4);
    fs_->forget(nodeid4, 2);
  }

  void verify_negative_cache_renamedir() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto iter = fs_->global_inodes_map_.find(parent);
    ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
    std::string parent_name = iter->second->name;

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::string new_parent_name = parent_name + "_renamed";
    uint64_t newpid;
    struct stat st;
    int r = fs_->mkdir(root_nodeid_, new_parent_name.c_str(), 0777, 0, 0, 0,
                       &newpid, &st);
    ASSERT_EQ(r, 0);

    // add new_parent_path/file_x to the neg cache
    for (int i = 0; i < 5; ++i) {
      std::string name = "file_" + std::to_string(i);
      uint64_t nodeid_tmp;
      r = fs_->lookup(newpid, name.c_str(), &nodeid_tmp, &st);
      ASSERT_EQ(r, -ENOENT);
    }

    r = fs_->rmdir(root_nodeid_, new_parent_name.c_str());
    ASSERT_EQ(r, 0);
    fs_->forget(newpid, 1);

    r = fs_->lookup(root_nodeid_, new_parent_name.c_str(), &newpid, &st);
    ASSERT_EQ(r, -ENOENT);

    for (int i = 0; i < 5; i++) {
      std::string name = "file_" + std::to_string(i);
      uint64_t nodeid_tmp;
      // add more files (src_dir/filex) to the negative cache
      r = fs_->lookup(parent, name.c_str(), &nodeid_tmp, &st);
      ASSERT_EQ(r, -ENOENT);

      r = upload_file(local_file, join_paths(parent_path, name),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    ASSERT_EQ(int(fs_->negative_cache_->n_lru_map_->size()), 11);

    // rename dir
    r = fs_->rename(root_nodeid_, parent_name.c_str(), root_nodeid_,
                    new_parent_name.c_str(), 0);
    ASSERT_EQ(r, 0);

    ASSERT_EQ(int(fs_->negative_cache_->n_lru_map_->size()), 5);

    // verify these items have been removed from the negative cache
    for (int i = 0; i < 5; i++) {
      std::string name = "file_" + std::to_string(i);
      uint64_t nodeid_tmp;
      r = fs_->lookup(parent, name.c_str(), &nodeid_tmp, &st);
      ASSERT_EQ(r, 0);

      fs_->forget(nodeid_tmp, 1);
    }

    r = fs_->lookup(root_nodeid_, new_parent_name.c_str(), &newpid, &st);
    ASSERT_EQ(r, 0);
    fs_->forget(newpid, 1);
  }
};

TEST_F(Ossfs2NegativeCacheTest, verify_negative_cache_create) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 2;
  opts.oss_negative_cache_size = 1000;
  opts.oss_negative_cache_timeout = 1;
  init(opts);
  verify_negative_cache_create();
}

TEST_F(Ossfs2NegativeCacheTest, verify_negative_cache_lookup) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 2;
  opts.oss_negative_cache_size = 3;
  opts.oss_negative_cache_timeout = 1;
  init(opts);
  verify_negative_cache_lookup();
}

TEST_F(Ossfs2NegativeCacheTest, verify_negative_cache_renamedir) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 2;
  opts.oss_negative_cache_size = 1000;
  opts.oss_negative_cache_timeout = 1;
  init(opts);
  verify_negative_cache_renamedir();
}
