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

class Ossfs2StagedInodeCacheTest : public Ossfs2TestSuite {
 protected:
  void verify_lru() {
    int r = 0;
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    // Should exclude the root inode and the parent inode.
    int lru_size = fs_->get_inode_eviction_threshold() - 2;
    std::vector<uint64_t> nodeids;
    struct stat st;
    uint64_t nodeid = 0;
    std::string file_name;
    for (int i = 0; i < lru_size; i++) {
      file_name = "test_lru_file_" + std::to_string(i);
      r = upload_file(local_file, join_paths(parent_path, file_name),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->lookup(parent, file_name.c_str(), &nodeid, &st);

      nodeids.push_back(nodeid);
      fs_->forget(nodeid, 1);
    }

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    // all of the inodes should be in the lru list and not evicted
    for (int i = 0; i < lru_size; i++) {
      file_name = "test_lru_file_" + std::to_string(i);
      r = fs_->lookup(parent, file_name.c_str(), &nodeid, &st);
      ASSERT_EQ(nodeid, nodeids[i]);
      fs_->forget(nodeid, 1);
    }

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    // create another 3 files
    for (int i = lru_size; i < lru_size + 3; i++) {
      file_name = "test_lru_file_" + std::to_string(i);
      r = upload_file(local_file, join_paths(parent_path, file_name),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->lookup(parent, file_name.c_str(), &nodeid, &st);

      nodeids.push_back(nodeid);
    }

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // the first 3 files should be evicted
    for (int i = 0; i < 3; i++) {
      std::string file_name = "test_lru_file_" + std::to_string(i);
      r = fs_->lookup(parent, file_name.c_str(), &nodeid, &st);
      ASSERT_EQ(r, -EIO);
    }

    for (int i = 3; i < lru_size + 3; i++) {
      std::string file_name = "test_lru_file_" + std::to_string(i);
      r = fs_->lookup(parent, file_name.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(nodeid, nodeids[i]);

      if (i >= lru_size) {
        fs_->forget(nodeid, 2);
      } else {
        fs_->forget(nodeid, 1);
      }
    }

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
  }

  void verify_lru_forget_with_parent(int file_num) {
    // Should exclude the root inode.
    int lru_size = fs_->get_inode_eviction_threshold() - 1;
    bool with_evict = (lru_size <= file_num);

    int r = 0;
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::vector<uint64_t> nodeids;
    struct stat st;
    uint64_t nodeid = 0;
    std::string file_name;

    // Should exclude the root inode and the parent inode.
    for (int i = 0; i < file_num; i++) {
      file_name = "test_lru_file_" + std::to_string(i);
      r = upload_file(local_file, join_paths(parent_path, file_name),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->lookup(parent, file_name.c_str(), &nodeid, &st);

      nodeids.push_back(nodeid);
    }

    fs_->forget(parent, 1);  // forget out of order
    for (int i = 0; i < file_num; i++) {
      fs_->forget(nodeids[i], 1);
    }

    // Wait for the background thread to clear cache nodes.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Case 1: staged size is large enough.
    uint64_t new_parent;
    r = fs_->lookup(root_nodeid_, gtest_base_dir.c_str(), &new_parent, &st);
    ASSERT_EQ(r, 0);

    if (with_evict) {
      // The parent was evicted from the staged cache, and now created with a
      // new nodeid.
      ASSERT_NE(new_parent, parent);
    } else {
      ASSERT_EQ(new_parent, parent);
    }

    for (int i = 0; i < file_num; i++) {
      r = fs_->lookup(new_parent,
                      ("test_lru_file_" + std::to_string(i)).c_str(), &nodeid,
                      &st);
      ASSERT_EQ(r, 0);

      if (with_evict) {
        ASSERT_NE(nodeid, nodeids[i]);
      } else {
        ASSERT_EQ(nodeid, nodeids[i]);
      }

      fs_->forget(nodeid, 1);
    }

    fs_->forget(new_parent, 1);

    // Wait for the bg thread to clear extra inodes.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  void verify_forget_stale_inode_lru() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "testfile";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);

    r = fs_->forget(nodeid, 1);
    ASSERT_EQ(r, 0);

    auto &global_mp = fs_->global_inodes_map_;
    ASSERT_TRUE(global_mp.find(nodeid) == global_mp.end());
  }

  void verify_lru_cache_removed_in_lookup() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Lookup_Delay_Before_Getting_OSS_Response,
        FaultInjection(1, 0));

    uint64_t nodeid, file_nodeid;
    void *handle;
    struct stat st;

    std::thread create_and_forget_th([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      INIT_PHOTON();

      int rr = fs_->creat(parent, "local_file", CREATE_BASE_FLAGS, 0777, 0, 0,
                          0, &file_nodeid, &st, &handle);
      ASSERT_EQ(rr, 0);

      rr = fs_->release(file_nodeid, get_file_from_handle(handle));
      ASSERT_EQ(rr, 0);

      rr = fs_->lookup(parent, "local_file", &file_nodeid, &st);
      ASSERT_EQ(rr, 0);

      fs_->forget(file_nodeid, 2);
      ASSERT_EQ(fs_->staged_inodes_cache_->size(), size_t(1));
      rr = delete_file(join_paths(parent_path, "local_file"),
                       FLAGS_oss_bucket_prefix);
      ASSERT_EQ(rr, 0);
    });

    int r = fs_->lookup(parent, "local_file", &nodeid, &st);
    if (create_and_forget_th.joinable()) {
      create_and_forget_th.join();
    }

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Delay_Before_Getting_OSS_Response);
    ASSERT_EQ(r, -ENOENT);
    ASSERT_EQ(fs_->staged_inodes_cache_->size(), size_t(0));

    r = fs_->lookup(parent, "local_file", &nodeid, &st);
    ASSERT_EQ(r, -ENOENT);
  }

  void verify_readdir_while_forget(int file_num) {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "testfile";
    void *handle = nullptr;
    struct stat st;
    int r = 0;

    for (int i = 0; i < file_num; ++i) {
      uint64_t nodeid = 0;
      r = create_and_flush(parent, (filename + std::to_string(i)).c_str(),
                           CREATE_BASE_FLAGS, 0777, 0, 0, 0, &nodeid, &st,
                           &handle);
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      // release resets the attr_time, so we need to lookup it so forget can
      // insert this file into the lru cache.
      r = fs_->lookup(parent, (filename + std::to_string(i)).c_str(), &nodeid,
                      &st);
      ASSERT_EQ(r, 0);
      fs_->forget(nodeid, 2);
    }

    void *dh = nullptr;
    r = fs_->opendir(parent, &dh);
    ASSERT_EQ(r, 0);

    std::vector<TestInode> children;
    r = fs_->readdir(parent, 0, dh, filler, &children, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(children.size(), file_num + 2);

    r = fs_->releasedir(parent, dh);
    ASSERT_EQ(r, 0);
    // the lookup_cnts of these 3 files are 1

    std::thread t([&]() {
      g_fault_injector->set_injection(
          FaultInjectionId::FI_Readdir_Delay_After_Construct_Inodes);
      void *dirp = nullptr;
      int r = fs_->opendir(parent, &dirp);
      ASSERT_EQ(r, 0);

      std::vector<TestInode> childs;
      r = fs_->readdir(parent, 0, dirp, filler, &childs, nullptr, true,
                       nullptr);
      ASSERT_SIZE_EQ(childs.size(), file_num + 2);
      ASSERT_EQ(r, 0);

      r = fs_->releasedir(parent, dirp);
      ASSERT_EQ(r, 0);

      g_fault_injector->clear_injection(
          FaultInjectionId::FI_Readdir_Delay_After_Construct_Inodes);
    });
    DEFER(if (t.joinable()) t.join(););

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    for (int i = 0; i < file_num + 2; ++i) {
      if (children[i].name == "." || children[i].name == "..") continue;
      fs_->forget(children[i].nodeid, 1);

      auto it = fs_->global_inodes_map_.find(children[i].nodeid);
      ASSERT_TRUE(it != fs_->global_inodes_map_.end());
    }

    if (t.joinable()) t.join();

    if (fs_->enable_staged_cache()) {
      // remember inode in readdirplus removes inodes from the lru map
      ASSERT_EQ(int(fs_->staged_inodes_cache_->size()), 0);
    }

    for (int i = 0; i < file_num + 2; ++i) {
      if (children[i].name == "." || children[i].name == "..") continue;
      fs_->forget(children[i].nodeid, 1);
    }

    if (fs_->enable_staged_cache()) {
      // Wait for the background thread to evict extra inodes.
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  void verify_readdir_type_changed() {
    std::string name = "testname";
    int r = 0;

    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    struct NodeWithST {
      uint64_t nodeid = 0;
      std::string name;
      struct stat st;
      NodeWithST(uint64_t nodeid, const std::string &name, struct stat stbuf)
          : nodeid(nodeid), name(name), st(stbuf) {}
    };

    auto filler_withst = [](void *ctx, const uint64_t nodeid, const char *name,
                            const struct stat *stbuf, off_t off) -> int {
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;

      std::vector<NodeWithST> *nodes =
          reinterpret_cast<std::vector<NodeWithST> *>(ctx);
      nodes->emplace_back(nodeid, name, *stbuf);
      return 0;
    };

    auto readdir = [&](uint64_t parent,
                       std::vector<NodeWithST> &childs) -> int {
      void *dirp = nullptr;
      int rr = fs_->opendir(parent, &dirp);
      if (rr < 0) return rr;

      rr = fs_->readdir(parent, 0, dirp, filler_withst, &childs, nullptr, true,
                        nullptr);
      if (rr < 0) return rr;

      return fs_->releasedir(parent, dirp);
    };

    // 1. insert testname/, testname/subfile into staged cache
    r = create_dir(join_paths(parent_path, name), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = upload_file(local_file, join_paths(parent_path, name + "/subfile"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    std::vector<NodeWithST> childs1;
    r = readdir(parent, childs1);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs1.size(), 1);
    ASSERT_TRUE(S_ISDIR(childs1[0].st.st_mode));

    uint64_t nodeid_1;
    struct stat st;
    r = fs_->lookup(childs1[0].nodeid, "subfile", &nodeid_1, &st);
    ASSERT_EQ(r, 0);

    fs_->forget(childs1[0].nodeid, 1);
    fs_->forget(nodeid_1, 1);

    // 2. lookup them, dir's nodeid should be unchanged
    std::vector<NodeWithST> childs2;
    r = readdir(parent, childs2);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs2.size(), 1);
    ASSERT_TRUE(S_ISDIR(childs2[0].st.st_mode));
    ASSERT_EQ(childs2[0].nodeid, childs1[0].nodeid);

    uint64_t nodeid_2;
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->lookup(childs1[0].nodeid, "subfile", &nodeid_2, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(nodeid_2, nodeid_1);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    fs_->forget(childs1[0].nodeid, 1);
    fs_->forget(nodeid_2, 1);

    // 3. change the type of testname/ to file on cloud
    r = delete_file(join_paths(parent_path, name + "/subfile"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = delete_dir(join_paths(parent_path, name), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = upload_file(local_file, join_paths(parent_path, name),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    std::vector<NodeWithST> childs3;
    r = readdir(parent, childs3);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs3.size(), 1);
    ASSERT_FALSE(S_ISDIR(childs3[0].st.st_mode));
    fs_->forget(childs3[0].nodeid, 1);

    // 4. change the type of testname/ to dir on cloud
    r = delete_file(join_paths(parent_path, name), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = upload_file(local_file, join_paths(parent_path, name + "/subfile"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    std::vector<NodeWithST> childs4;
    r = readdir(parent, childs4);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs4.size(), 1);
    ASSERT_TRUE(S_ISDIR(childs4[0].st.st_mode));
    ASSERT_NE(childs4[0].nodeid, childs1[0].nodeid);
    ASSERT_NE(childs4[0].nodeid, childs3[0].nodeid);

    uint64_t nodeid_3;
    r = fs_->lookup(childs4[0].nodeid, "subfile", &nodeid_3, &st);
    ASSERT_EQ(r, 0);
    ASSERT_NE(nodeid_2, nodeid_3);

    fs_->forget(nodeid_3, 1);
    fs_->forget(childs4[0].nodeid, 1);
  }

  void verify_staged_cache_lookup_update() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    void *handle = nullptr;
    struct stat st, st1;
    uint64_t nodeid = 0, nodeid1 = 0, nodeid2 = 0;
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = upload_file(local_file, join_paths(parent_path, "testfile"),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->lookup(parent, "testfile", &nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 3 << 20);
    // insert it to the staged cache
    fs_->forget(nodeid, 1);

    r = delete_file(join_paths(nodeid_to_path(parent), "testfile"),
                    FLAGS_oss_bucket_prefix);

    int r1 = 0;
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
    g_fault_injector->set_injection(FaultInjectionId::FI_Lookup_Oss_Failure);

    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Oss_Failure));
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr));
    std::thread th([&]() {
      INIT_PHOTON();
      // Hit staged cache, and then sleep.
      r1 = fs_->lookup(parent, "testfile", &nodeid2, &st);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // During lock-free period, upload a new file with a different size but the
    // same name.
    r = create_and_flush(parent, "testfile", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid1, &st1, &handle);
    ASSERT_EQ(r, 0);
    char *buf = new char[4096];
    DEFER(delete[] buf);
    auto w_size = write_to_file_handle(handle, buf, 4096, 0);
    ASSERT_EQ(w_size, 4096);
    r = fs_->release(nodeid1, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    th.join();
    ASSERT_EQ(r1, 0);
    ASSERT_EQ(nodeid2, nodeid1);
    ASSERT_NE(nodeid2, nodeid);
    ASSERT_EQ(st.st_size, 4096);

    ASSERT_EQ(fs_->staged_inodes_cache_->size(), size_t(0));
    fs_->forget(nodeid2, 2);
  }

  void verify_stagedcache_expiry() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = upload_file(local_file, join_paths(parent_path, "testfile"),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid = 0, nodeid1 = 0, nodeid2 = 0, nodeid3 = 0, nodeid4 = 0;
    void *handle = nullptr;
    struct stat st, st1;
    r = fs_->lookup(parent, "testfile", &nodeid, &st);
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid, 1);
    photon::thread_usleep(1000 * 1000);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    // staged cache miss
    r = fs_->lookup(parent, "testfile", &nodeid1, &st1);
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    r = fs_->lookup(parent, "testfile", &nodeid1, &st1);
    ASSERT_EQ(r, 0);
    ASSERT_NE(nodeid1, nodeid);
    fs_->forget(nodeid1, 1);

    photon::thread_usleep(1000 * 1000);
    r = delete_file(join_paths(parent_path, "testfile"),
                    FLAGS_oss_bucket_prefix);
    // Staged cache miss, so create should succeed.
    r = fs_->creat(parent, "testfile", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid2, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid2, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    r = fs_->lookup(parent, "testfile", &nodeid3, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(nodeid3, nodeid2);
    ASSERT_NE(nodeid2, nodeid1);
    fs_->forget(nodeid2, 2);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->lookup(parent, "testfile", &nodeid4, &st);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    // Staged cache hits.
    ASSERT_EQ(r, 0);
    ASSERT_EQ(nodeid4, nodeid3);
    fs_->forget(nodeid4, 1);
  }

  void verify_staged_cache_memory() {
    int64_t mem_before = get_physical_memory_KiB();
    LOG_DEBUG("memory at first: `", mem_before);

    const uint64_t test_num = 2000000;
    uint64_t parent_nodeid = 1;
    for (int j = 0; j < 10; j++) {
      for (uint64_t i = 1; i <= test_num; i++) {
        fs_->staged_inodes_cache_->insert(parent_nodeid,
                                          "test_file_" + std::to_string(i), 0,
                                          {0, 0}, "", i, InodeType::kFile, 0);
        if (i % 200000 == 0) {
          LOG_DEBUG("inserted ` staged cache nodes", i);
        }
      }
      LOG_DEBUG("memory after round `: `", j, get_physical_memory_KiB());
    }

    photon::thread_usleep(30 * 1000 * 1000);
    int64_t mem_after = get_physical_memory_KiB();
    LOG_DEBUG("memory at last: `", mem_after);

    // The memory of 1w (max_inode_cache_count) staged cache should not exceed
    // 100 MiB.
#if defined(__SANITIZE_ADDRESS__)
    ASSERT_GT(mem_before + 2 * 1024 * 1024, mem_after);
#else
    ASSERT_GT(mem_before + 100 * 1024, mem_after);
#endif
  }
};

TEST_F(Ossfs2StagedInodeCacheTest, verify_lru) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););
  OssFsOptions opts;
  opts.max_inode_cache_count = 7;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts);
  verify_lru();
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_lru_forget_with_parent) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););

  OssFsOptions opts;
  opts.max_inode_cache_count = 6;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts);
  verify_lru_forget_with_parent(2);
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_lru_with_evict_forget_with_parent) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););

  OssFsOptions opts;
  opts.max_inode_cache_count = 3;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts);
  verify_lru_forget_with_parent(4);
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_forget_stale_inode_lru) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););

  OssFsOptions opts;
  opts.max_inode_cache_count = 3;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts);
  verify_forget_stale_inode_lru();
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_lru_cache_removed_in_lookup) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););

  OssFsOptions opts;
  opts.max_inode_cache_count = 10;
  opts.attr_timeout = 60;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts);
  verify_lru_cache_removed_in_lookup();
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_lru_readdir_while_forget) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););

  OssFsOptions opts;
  opts.max_inode_cache_count = 5;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts, get_random_max_keys());
  verify_readdir_while_forget(opts.max_inode_cache_count - 2);
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_lru_with_evict_readdir_while_forget) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););

  OssFsOptions opts;
  opts.max_inode_cache_count = 5;
  opts.inode_cache_eviction_interval_ms = 10;
  init(opts, get_random_max_keys());
  verify_readdir_while_forget(opts.max_inode_cache_count);
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_readdir_while_forget) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, get_random_max_keys());
  verify_readdir_while_forget(3);
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_readdir_type_changed) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.max_inode_cache_count = 10;
  init(opts, get_random_max_keys());
  verify_readdir_type_changed();
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_staged_cache_lookup_update) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.max_inode_cache_count = 10;
  init(opts, get_random_max_keys());
  verify_staged_cache_lookup_update();
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_stagedcache_expiry) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.max_inode_cache_count = 10;
  init(opts, get_random_max_keys());
  verify_stagedcache_expiry();
}

TEST_F(Ossfs2StagedInodeCacheTest, verify_staged_cache_memory) {
  INIT_PHOTON();
  g_fault_injector->set_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully);
  DEFER(g_fault_injector->clear_injection(
      FaultInjectionId::FI_Reverse_Invalidate_Inode_Forcefully););
  OssFsOptions opts;
  opts.max_inode_cache_count = 10000;
  opts.inode_cache_eviction_interval_ms = 30000;
  init(opts, get_random_max_keys());
  verify_staged_cache_memory();
}
