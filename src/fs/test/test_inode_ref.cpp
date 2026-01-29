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

class Ossfs2InodeRefTest : public Ossfs2TestSuite {
 protected:
  void verify_concurrent_inode_refs_holding_shared_path(uint64_t parent) {
    //
    // case: one is holding file/dir shared path
    // file/dir shared path can be acquired
    // ancestors/descendants shared path can be acquired
    // file/dir unique path could not be acquired
    // ancestors/descendants unique path could not be acquired
    // each path in path2 case follows the same rule
    //
    // ./a/b/c
    // ./a/d
    // holding b's shared lock
    uint64_t dira_nodeid = 0, dirb_nodeid = 0, dird_nodeid = 0,
             filec_nodeid = 0;
    void *file_handle = nullptr;
    struct stat st;
    int r = fs_->mkdir(parent, "a", 0777, 0, 0, 0, &dira_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dira_nodeid, 1));

    r = fs_->mkdir(dira_nodeid, "b", 0777, 0, 0, 0, &dirb_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dirb_nodeid, 1));
    r = fs_->mkdir(dira_nodeid, "d", 0777, 0, 0, 0, &dird_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dird_nodeid, 1));

    r = create_and_flush(dirb_nodeid, "c", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &filec_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(filec_nodeid, get_file_from_handle(file_handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(filec_nodeid, 1));

    std::atomic<bool> base_locked{false};
    auto hold_shared_path_and_wait_for_1s = [=, &base_locked](uint64_t nodeid) {
      auto ref =
          fs_->get_inode_ref(nodeid, OssFs::InodeRefPathType::kPathTypeRead);
      DEFER(fs_->return_inode_ref(ref));
      base_locked.store(true);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    };

    auto time_acquired_lock_with_nodeid = [=](uint64_t nodeid,
                                              bool write_lock) -> int {
      auto before = std::chrono::steady_clock::now();
      auto path_ref = write_lock ? OssFs::InodeRefPathType::kPathTypeWrite
                                 : OssFs::InodeRefPathType::kPathTypeRead;
      auto ref = fs_->get_inode_ref(nodeid, path_ref);
      DEFER(fs_->return_inode_ref(ref));
      auto after = std::chrono::steady_clock::now();
      auto cost =
          std::chrono::duration_cast<std::chrono::microseconds>(after - before);
      return cost.count() / 1000;
    };

    auto time_acquired_lock_with_parent_and_name =
        [=](uint64_t parent, const char *name) -> int {
      auto before = std::chrono::steady_clock::now();
      auto ref = fs_->get_inode_ref(parent, name);
      DEFER(fs_->return_inode_ref(ref));
      auto after = std::chrono::steady_clock::now();
      auto cost =
          std::chrono::duration_cast<std::chrono::microseconds>(after - before);
      return cost.count() / 1000;
    };

    auto time_acquired_lock_with_path2 =
        [=](uint64_t p1, const char *n1, uint64_t p2, const char *n2) -> int {
      auto before = std::chrono::steady_clock::now();
      auto ref = fs_->get_inode_ref(p1, n1, p2, n2);
      DEFER(fs_->return_inode_ref(ref));
      auto after = std::chrono::steady_clock::now();
      auto cost =
          std::chrono::duration_cast<std::chrono::microseconds>(after - before);
      return cost.count() / 1000;
    };

    auto start_async_hold_shared_path = [&]() -> std::future<void> {
      auto future = std::async(std::launch::async, [&]() {
        hold_shared_path_and_wait_for_1s(dirb_nodeid);
      });
      // let the async function start
      while (!base_locked.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      base_locked.store(false);

      return future;
    };

    {
      // shared paths
      auto future = start_async_hold_shared_path();
      DEFER(future.wait());
      auto time = time_acquired_lock_with_nodeid(dirb_nodeid, false);
      ASSERT_LT(time, 150);
      time = time_acquired_lock_with_parent_and_name(dirb_nodeid, "c");
      ASSERT_LT(time, 150);
      time = time_acquired_lock_with_nodeid(dira_nodeid, false);
      ASSERT_LT(time, 150);
      time = time_acquired_lock_with_path2(dirb_nodeid, "c", dira_nodeid, "d");
      ASSERT_LT(time, 150);
    }

    {
      // unique paths
      auto future = start_async_hold_shared_path();
      DEFER(future.wait());
      auto time = time_acquired_lock_with_nodeid(dirb_nodeid, true);
      ASSERT_GT(time, 900);
    }

    {
      // parent unique path
      auto future = start_async_hold_shared_path();
      DEFER(future.wait());
      auto time = time_acquired_lock_with_nodeid(dira_nodeid, true);
      ASSERT_GT(time, 900);
    }

    {
      // path2. src needs unique path
      auto future = start_async_hold_shared_path();
      DEFER(future.wait());
      auto time =
          time_acquired_lock_with_path2(dira_nodeid, "b", dira_nodeid, "d");
      ASSERT_GT(time, 900);
    }

    {
      // path2. dst needs unique path
      auto future = start_async_hold_shared_path();
      DEFER(future.wait());
      auto time =
          time_acquired_lock_with_path2(dira_nodeid, "d", dira_nodeid, "b");
      ASSERT_GT(time, 900);
    }
  }

  void verify_concurrent_inode_refs_holding_unique_path(uint64_t parent) {
    //
    // case : one is holding file/dir unique path
    // file/dir shared path could not be acquired
    // ancestors/descendants shared path could not be acuqred
    // file/dir unique path could not be acquired
    // ancestors/descendants unique path could not be acquired
    // each path in path2 case follows the same rule
    //
    // ./a/b/c
    // ./a/d
    // holding b's unique lock
    uint64_t dira_nodeid = 0, dirb_nodeid = 0, dird_nodeid = 0,
             filec_nodeid = 0;
    void *file_handle = nullptr;
    struct stat st;
    int r = fs_->mkdir(parent, "a", 0777, 0, 0, 0, &dira_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dira_nodeid, 1));

    r = fs_->mkdir(dira_nodeid, "b", 0777, 0, 0, 0, &dirb_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dirb_nodeid, 1));
    r = fs_->mkdir(dira_nodeid, "d", 0777, 0, 0, 0, &dird_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dird_nodeid, 1));

    r = create_and_flush(dirb_nodeid, "c", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &filec_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(filec_nodeid, get_file_from_handle(file_handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(filec_nodeid, 1));

    std::atomic<bool> base_locked{false};
    auto hold_unique_path_with_nodeid_and_wait_for_1s = [=, &base_locked](
                                                            uint64_t nodeid) {
      auto ref =
          fs_->get_inode_ref(nodeid, OssFs::InodeRefPathType::kPathTypeWrite);
      DEFER(fs_->return_inode_ref(ref));
      base_locked.store(true);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    };

    auto hold_unique_path_with_parent_and_name_and_wait_for_1s =
        [=, &base_locked](uint64_t parent, const char *name) {
          auto ref = fs_->get_inode_ref(parent, name);
          DEFER(fs_->return_inode_ref(ref));
          base_locked.store(true);
          std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        };

    auto time_acquired_lock_with_nodeid = [=](uint64_t nodeid,
                                              bool write_lock) -> int {
      auto before = std::chrono::steady_clock::now();
      auto path_ref = write_lock ? OssFs::InodeRefPathType::kPathTypeWrite
                                 : OssFs::InodeRefPathType::kPathTypeRead;
      auto ref = fs_->get_inode_ref(nodeid, path_ref);
      DEFER(fs_->return_inode_ref(ref));
      auto after = std::chrono::steady_clock::now();
      auto cost =
          std::chrono::duration_cast<std::chrono::microseconds>(after - before);
      return cost.count() / 1000;
    };

    auto time_acquired_lock_with_parent_and_name =
        [=](uint64_t parent, const char *name) -> int {
      auto before = std::chrono::steady_clock::now();
      auto ref = fs_->get_inode_ref(parent, name);
      DEFER(fs_->return_inode_ref(ref));
      auto after = std::chrono::steady_clock::now();
      auto cost =
          std::chrono::duration_cast<std::chrono::microseconds>(after - before);
      return cost.count() / 1000;
    };

    auto time_acquired_lock_with_path2 =
        [=](uint64_t p1, const char *n1, uint64_t p2, const char *n2) -> int {
      auto before = std::chrono::steady_clock::now();
      auto ref = fs_->get_inode_ref(p1, n1, p2, n2);
      DEFER(fs_->return_inode_ref(ref));
      auto after = std::chrono::steady_clock::now();
      auto cost =
          std::chrono::duration_cast<std::chrono::microseconds>(after - before);
      return cost.count() / 1000;
    };

    srand(time(nullptr));
    auto start_async_hold_unique_path = [&]() -> std::future<void> {
      auto future = std::async(std::launch::async, [&]() {
        if (rand() % 2) {
          hold_unique_path_with_nodeid_and_wait_for_1s(dirb_nodeid);
        } else {
          hold_unique_path_with_parent_and_name_and_wait_for_1s(dira_nodeid,
                                                                "b");
        }
      });

      // let the async function start
      while (!base_locked.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      base_locked.store(false);

      return future;
    };

    {
      // shared path
      auto future = start_async_hold_unique_path();
      DEFER(future.wait());
      auto time = time_acquired_lock_with_nodeid(dirb_nodeid, false);
      ASSERT_GT(time, 900);
    }

    {
      // shared path as a parent
      auto future = start_async_hold_unique_path();
      DEFER(future.wait());
      auto time = time_acquired_lock_with_parent_and_name(dirb_nodeid, "c");
      ASSERT_GT(time, 900);
    }

    {
      // parent unique path
      auto future = start_async_hold_unique_path();
      DEFER(future.wait());
      auto time = time_acquired_lock_with_nodeid(dira_nodeid, false);
      ASSERT_LT(time, 150);  // ok to get parent shared lock
      time = time_acquired_lock_with_nodeid(dira_nodeid, true);
      ASSERT_GT(time, 900);  // blocked to get parent unique lock
    }

    {
      // path2. src needs shared path
      auto future = start_async_hold_unique_path();
      DEFER(future.wait());
      auto time =
          time_acquired_lock_with_path2(dirb_nodeid, "c", dira_nodeid, "d");
      ASSERT_GT(time, 900);
    }

    {
      // path2. src needs unique path
      auto future = start_async_hold_unique_path();
      DEFER(future.wait());
      auto time =
          time_acquired_lock_with_path2(dira_nodeid, "b", dira_nodeid, "d");
      ASSERT_GT(time, 900);
    }

    {
      // path2. dst needs unique path
      auto future = start_async_hold_unique_path();
      DEFER(future.wait());
      auto time =
          time_acquired_lock_with_path2(dira_nodeid, "d", dira_nodeid, "b");
      ASSERT_GT(time, 900);
    }
  }

  void verify_concurrent_inode_refs() {
    auto orig_val = FLAGS_enable_locking_debug_logs;
    FLAGS_enable_locking_debug_logs = true;
    DEFER(FLAGS_enable_locking_debug_logs = orig_val);
    // mainly test inode refs
    // firstly create some dirs and files
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int dir_cnt = 64;
    std::vector<uint64_t> nodeids(dir_cnt, 0);
    for (int i = 0; i < dir_cnt; i++) {
      struct stat st;
      auto name = "parent_dir" + std::to_string(i);
      int r = fs_->mkdir(parent, name.c_str(), 0777, 0, 0, 0, &nodeids[i], &st);
      ASSERT_EQ(r, 0);
    }

    srand(time(nullptr));
    std::vector<std::future<void>> futures;
    for (int i = 0; i < dir_cnt; i++) {
      auto nodeid = nodeids[i];
      auto future = std::async(std::launch::async, [&, nodeid]() {
        if (rand() % 2) {
          verify_concurrent_inode_refs_holding_shared_path(nodeid);
        } else {
          verify_concurrent_inode_refs_holding_unique_path(nodeid);
        }
      });
      futures.push_back(std::move(future));
    }

    for (auto &future : futures) {
      future.wait();
    }

    for (auto nodeid : nodeids) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_queued_path2_inode_refs() {
    // /a/b/c /a/d/e both have read path lock
    // a-r2 b-r1 c-r1 d-r1 e-r1
    //
    // /a/b/c --try renaming to -> /a/d/e.
    // getpath2 request will be queued and waiting
    //
    // release path lock for c
    // a-r1 b-r0 c-r0 d-r1 e-r1
    //
    // queued getpath2 request will wake and retry.
    // but retry failed. nothing changed.
    //
    // remove a. one more request queued and waiting
    //
    // release path lock for e
    // a-r0 b-r0 c-r0 d-r0 e-r0
    //
    // queued items wakeup and grabs lock acorrding to
    // their requesting time.
    //
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t dir_nodeid_a = 0;
    int r = fs_->mkdir(parent, "a", 0777, 0, 0, 0, &dir_nodeid_a, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid_a, 1));

    uint64_t dir_nodeid_b = 0, dir_nodeid_d = 0;
    r = fs_->mkdir(dir_nodeid_a, "b", 0777, 0, 0, 0, &dir_nodeid_b, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid_b, 1));
    r = fs_->mkdir(dir_nodeid_a, "d", 0777, 0, 0, 0, &dir_nodeid_d, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid_d, 1));

    uint64_t file_nodeid_c = 0, file_nodeid_e = 0;
    create_file_in_folder(dir_nodeid_b, "c", 0, file_nodeid_c);
    DEFER(fs_->forget(file_nodeid_c, 1));
    create_file_in_folder(dir_nodeid_d, "e", 0, file_nodeid_e);
    DEFER(fs_->forget(file_nodeid_e, 1));

    auto dump_all_refs = [&] {
      std::map<uint64_t, std::string> nodes = {{dir_nodeid_a, "a"},
                                               {dir_nodeid_b, "b"},
                                               {dir_nodeid_d, "d"},
                                               {file_nodeid_c, "c"},
                                               {file_nodeid_e, "e"}};
      for (auto it : nodes) {
        auto inode = fs_->global_inodes_map_[it.first];
        LOG_INFO("node ` path lock ` ref ctr `", it.second, inode->pathlock,
                 inode->ref_ctr);
      };
    };

    auto file_c_ref = fs_->get_inode_ref(
        file_nodeid_c, OssFs::InodeRefPathType::kPathTypeRead);
    auto file_e_ref = fs_->get_inode_ref(
        file_nodeid_e, OssFs::InodeRefPathType::kPathTypeRead);

    auto start = std::chrono::steady_clock::now();

    // we don't do rename/remove op actually and just grab the path lock.
    auto rename_future = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(dir_nodeid_b, "c", dir_nodeid_d, "e");
      DEFER(fs_->return_inode_ref(ref));

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto now = std::chrono::steady_clock::now();
      auto period =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - start)
              .count();
      return period;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    fs_->return_inode_ref(file_c_ref);

    auto remove_future = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(parent, "a");
      DEFER(fs_->return_inode_ref(ref));

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto now = std::chrono::steady_clock::now();
      auto period =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - start)
              .count();
      return period;
    });

    dump_all_refs();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    fs_->return_inode_ref(file_e_ref);

    dump_all_refs();

    auto rename_done_time = rename_future.get();
    auto remove_done_time = remove_future.get();
    ASSERT_LT(rename_done_time, remove_done_time);
  }

  void verify_get_inode_ref_err() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // create one file
    // thread 1 get the inode ref and sleep there for 2s
    // thread 2 wait for the inode ref
    // remove the inode remotely which will make it stale
    // forget the inode
    // thread 2 waiting done and get one stale error code
    struct stat st;
    uint64_t dir_nodeid = 0, dir_nodeid2 = 0;
    int r = fs_->mkdir(parent, "test_dir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    r = fs_->mkdir(parent, "test_dir2", 0777, 0, 0, 0, &dir_nodeid2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid2, 1));

    auto dir_path = nodeid_to_path(dir_nodeid);

    auto future1 = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(parent, "test_dir");
      DEFER(fs_->return_inode_ref(ref));
      std::this_thread::sleep_for(std::chrono::milliseconds(4000));
    });
    DEFER(future1.wait());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto future2 = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(dir_nodeid,
                                    OssFs::InodeRefPathType::kPathTypeRead);
      DEFER(fs_->return_inode_ref(ref));
    });
    DEFER(future2.wait());

    auto future3 = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(dir_nodeid,
                                    OssFs::InodeRefPathType::kPathTypeWrite);
      DEFER(fs_->return_inode_ref(ref));
    });
    DEFER(future3.wait());

    auto future4 = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(dir_nodeid, "test_file", dir_nodeid2,
                                    "test_file2");
      DEFER(fs_->return_inode_ref(ref));
    });
    DEFER(future4.wait());

    auto future5 = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(dir_nodeid2, "test_file2", dir_nodeid,
                                    "test_file");
      DEFER(fs_->return_inode_ref(ref));
    });
    DEFER(future5.wait());

    auto future6 = std::async(std::launch::async, [&]() {
      auto ref = fs_->get_inode_ref(dir_nodeid, "test_file2");
      DEFER(fs_->return_inode_ref(ref));
    });
    DEFER(future6.wait());

    uint64_t unused_nodeid = 0;
    r = fs_->lookup(parent, "test_dir", &unused_nodeid, &st);
    ASSERT_EQ(r, 0);
    r = fs_->forget(dir_nodeid, 1);
    ASSERT_EQ(r, 0);

    // remove the dir on the cloud
    r = delete_dir(dir_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // make sure the attr times out
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    r = fs_->lookup(parent, "test_dir", &unused_nodeid, &st);
    ASSERT_EQ(r, -ENOENT);  // wakes other futures up

    r = fs_->forget(dir_nodeid, 1);
    ASSERT_EQ(r, 0);  // for create

    {
      std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
      ASSERT_TRUE(fs_->global_inodes_map_.find(dir_nodeid) !=
                  fs_->global_inodes_map_.end());
      auto inode = fs_->global_inodes_map_[dir_nodeid];
      ASSERT_TRUE(inode->is_stale);  // future1 still hold refs
      LOG_INFO("file inode ref is `", inode->ref_ctr);
    }
  }

  // level0...leveln-2 are all dirs. leveln-1 are files.
  void mock_level_inodes_only(const std::vector<int> &levels,
                              std::vector<std::vector<Inode *>> &nodes,
                              int name_base_size = 50) {
    if (levels.empty()) return;

    Inode *parent_inode = fs_->mp_inode_;

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    auto create_child_inode = [&](int cur_level, int cur_index, bool is_dir) {
      std::string namestr(name_base_size, cur_level + '0');
      namestr.append("-").append(std::to_string(cur_index));
      InodeType type = is_dir ? InodeType::kDir : InodeType::kFile;
      Inode *child_inode =
          fs_->create_new_inode(InodeManager::next(), namestr, 0, now, type,
                                false, parent_inode->nodeid, nullptr, "");
      fs_->add_new_inode_to_global_map(child_inode);
      static_cast<DirInode *>(parent_inode)->add_child_node(child_inode);
      return child_inode;
    };

    nodes.resize(levels.size());
    for (int i = 0; i < levels[0]; i++) {
      nodes[0].push_back(create_child_inode(0, i, levels.size() > 1));
    }

    for (size_t i = 1; i < levels.size(); i++) {
      for (size_t j = 0; j < nodes[i - 1].size(); j++) {
        parent_inode = nodes[i - 1][j];
        for (int k = 0; k < levels[i]; k++) {
          nodes[i].push_back(create_child_inode(i, k, i != levels.size() - 1));
        }
      }
    }
  }

  void verify_get_path_performance() {
    std::vector<std::vector<Inode *>> nodes;
    {
      std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
      mock_level_inodes_only({10, 10, 10, 10, 10, 10}, nodes);
    }

    auto &f_nodes = nodes.back();
    std::random_device rd;
    std::mt19937 rng(rd());

    std::shuffle(f_nodes.begin(), f_nodes.end(), rng);

    auto before = std::chrono::steady_clock::now();
    for (auto inode : f_nodes) {
      auto path = nodeid_to_path(inode->nodeid);
    }
    auto after = std::chrono::steady_clock::now();
    auto cost =
        std::chrono::duration_cast<std::chrono::microseconds>(after - before);
    LOG_INFO("took time `us to get all path, map size `", cost.count(),
             fs_->global_inodes_map_.size());

    {
      std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
      fs_->global_inodes_map_.clear();
      for (auto &vc : nodes) {
        for (auto inode : vc) {
          delete inode;
        }
      }
      fs_->add_new_inode_to_global_map(
          fs_->mp_inode_);  // make the teardown check pass
    }
  }
};

TEST_F(Ossfs2InodeRefTest, verify_concurrent_inode_refs) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_concurrent_inode_refs();
}

TEST_F(Ossfs2InodeRefTest, verify_queued_path2_inode_refs) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_queued_path2_inode_refs();
}

TEST_F(Ossfs2InodeRefTest, verify_get_inode_ref_err) {
  FLAGS_enable_locking_debug_logs = true;
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  init(opts);
  verify_get_inode_ref_err();
}

TEST_F(Ossfs2InodeRefTest, DISABLED_verify_get_path_performance) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_get_path_performance();
}
