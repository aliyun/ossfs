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

class Ossfs2ReaddirTest : public Ossfs2TestSuite {
 protected:
  void verify_readdir_while_create_and_unlink() {
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent, 1));

    // 1. readdir while creating
    std::map<std::string, uint64_t> created_files;
    std::vector<TestInode> childs;
    auto th = std::thread([&]() {
      for (int i = 0; i < 10; ++i) {
        uint64_t nodeid;
        void *handle;
        std::string name = "file_internal_" + std::to_string(i);
        r = create_and_flush(parent, name.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
        EXPECT_EQ(r, 0);
        if (r != 0) break;
        r = fs_->release(nodeid, get_file_from_handle(handle));
        EXPECT_EQ(r, 0);
        if (r != 0) break;
        created_files.insert(std::make_pair(name, nodeid));
      }
    });

    DEFER(if (th.joinable()) th.join(););

    int r2 = read_dir_without_dots(parent, childs);
    ASSERT_EQ(r2, 0);
    if (th.joinable()) th.join();

    // The file could be in the list result or not. If it is, it
    // should have the same nodeid as the inode created.
    for (auto &child : childs) {
      auto iter = created_files.find(child.name);
      if (iter != created_files.end()) {
        ASSERT_EQ(child.nodeid, iter->second);
        break;
      }
    }

    // 2. readdir while unlinking
    childs.clear();

    th = std::thread([&]() {
      for (int i = 0; i < 10; ++i) {
        std::string name = "file_internal_" + std::to_string(i);
        int r2 = fs_->unlink(parent, name.c_str());
        EXPECT_EQ(r2, 0);
        if (r2 != 0) break;
      }
    });
    DEFER(if (th.joinable()) th.join(););

    r = read_dir_without_dots(parent, childs);
    ASSERT_EQ(r, 0);

    if (th.joinable()) th.join();

    // no matter whether the list results are gotten before unlinking
    // or not, this inode should exist and be marked as STALE
    auto piter = fs_->global_inodes_map_.find(parent);
    ASSERT_TRUE(piter != fs_->global_inodes_map_.end());
    for (int i = 0; i < 10; ++i) {
      std::string name = "file_internal_" + std::to_string(i);
      Inode *cinode =
          static_cast<DirInode *>(piter->second)->find_child_node(name);
      ASSERT_NE(cinode, nullptr);
      ASSERT_TRUE(cinode->is_stale);
      fs_->forget(cinode->nodeid, cinode->lookup_cnt);
    }
  }

  void verify_partial_readdir() {
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent, 1));

    std::set<uint64_t> created_files;
    for (int i = 0; i < 30; ++i) {
      uint64_t nodeid;
      void *handle;
      std::string filename = "file_" + std::to_string(i);
      r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      created_files.insert(nodeid);
    }
    // these 30 inodes' lookupcnt == 1

    auto filler_25_files = [](void *ctx, const uint64_t nodeid,
                              const char *name, const struct stat *stbuf,
                              off_t off) -> int {
      std::vector<TestInode> *nodes =
          reinterpret_cast<std::vector<TestInode> *>(ctx);
      if (nodes->size() >= 25) return -1;
      nodes->emplace_back(nodeid, name);
      // we get all the dir entries one time
      return 0;
    };

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    // fill 25 nodes one time
    std::vector<TestInode> childs;
    r = fs_->readdir(parent, 0, dirp, filler_25_files, &childs, nullptr, true,
                     nullptr);
    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs.size(), 25);

    std::vector<TestInode> all_childs;
    r = read_dir_without_dots(parent, all_childs);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(all_childs.size(), 30);
    // only the first 25 inodes' lookupcnt == 3

    for (auto &child : childs) {
      if (child.name == "." || child.name == "..") continue;

      auto iter = fs_->global_inodes_map_.find(child.nodeid);
      ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
      ASSERT_EQ(iter->second->lookup_cnt, uint64_t(3));
      created_files.erase(child.nodeid);

      fs_->forget(child.nodeid, 3);
      iter = fs_->global_inodes_map_.find(child.nodeid);
      ASSERT_TRUE(iter == fs_->global_inodes_map_.end());
    }

    for (auto &file : created_files) {
      auto iter = fs_->global_inodes_map_.find(file);
      ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
      ASSERT_EQ(iter->second->lookup_cnt, uint64_t(2));

      fs_->forget(file, 2);
      iter = fs_->global_inodes_map_.find(file);
      ASSERT_TRUE(iter == fs_->global_inodes_map_.end());
    }

    // fill 25 nodes one more time
    // to check there are no inodes for the remained entries
    childs.clear();
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    dirp = reinterpret_cast<void *>(fi.fh);
    r = fs_->readdir(parent, 0, dirp, filler_25_files, &childs, nullptr, true,
                     nullptr);
    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);

    ASSERT_SIZE_EQ(childs.size(), 25);
    for (auto &child : childs) {
      if (child.name == "." || child.name == "..") continue;

      auto iter = fs_->global_inodes_map_.find(child.nodeid);
      ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
      ASSERT_EQ(iter->second->lookup_cnt, uint64_t(1));

      fs_->forget(child.nodeid, 1);
    }

    // let the teardown to check there are no remained inodes
  }

  void verify_readdir_offset(bool readdirplus) {
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent, 1));

    for (int i = 0; i < 10; ++i) {
      uint64_t nodeid;
      void *handle;
      std::string filename = "file_" + std::to_string(i);
      r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      fs_->forget(nodeid, 1);
    }

    auto filler_5_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      std::unordered_map<std::string, uint64_t> *nodes =
          reinterpret_cast<std::unordered_map<std::string, uint64_t> *>(ctx);
      if (nodes->size() && nodes->size() % 5 == 0) return -ENOSPC;
      (*nodes)[std::string(name)] = nodeid;
      return 0;
    };

    auto forget_children =
        [&](std::unordered_map<std::string, uint64_t> &children) {
          if (!readdirplus) return;

          for (auto &ent : children) {
            if (ent.first != ".." && ent.first != ".")
              fs_->forget(ent.second, 1);
          }
        };

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    // Fill 5 nodes once.
    std::unordered_map<std::string, uint64_t> childs;
    r = fs_->readdir(parent, 0, dirp, filler_5_files, &childs, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -ENOSPC);  // succeed to fill 5 nodes
    ASSERT_SIZE_EQ(childs.size(), 5);
    for (int i = 0; i < 10; ++i) {
      std::string filename = "file_" + std::to_string(i);
      if (i < 3)
        ASSERT_TRUE(childs.count(filename));
      else
        ASSERT_FALSE(childs.count(filename));
    }

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    forget_children(childs);

    // offset: 1
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    dirp = reinterpret_cast<void *>(fi.fh);

    std::unordered_map<std::string, uint64_t> childs_2;
    r = fs_->readdir(parent, 1, dirp, filler_5_files, &childs_2, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -ENOSPC);  // succeed to fill 5 nodes
    ASSERT_SIZE_EQ(childs_2.size(), 5);
    for (int i = 0; i < 10; ++i) {
      std::string filename = "file_" + std::to_string(i);
      if (i < 4)
        ASSERT_TRUE(childs_2.count(filename));
      else
        ASSERT_FALSE(childs_2.count(filename));
    }

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    forget_children(childs_2);

    // offset: 2
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    dirp = reinterpret_cast<void *>(fi.fh);

    std::unordered_map<std::string, uint64_t> childs_3, childs_4, childs_5,
        childs_6;
    r = fs_->readdir(parent, 2, dirp, filler_5_files, &childs_3, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -ENOSPC);  // succeed to fill 5 nodes
    ASSERT_SIZE_EQ(childs_3.size(), 5);
    for (int i = 0; i < 10; ++i) {
      std::string filename = "file_" + std::to_string(i);
      if (i < 5)
        ASSERT_TRUE(childs_3.count(filename));
      else
        ASSERT_FALSE(childs_3.count(filename));
    }

    // Out-of-order readdir.
    r = fs_->readdir(parent, 8, dirp, filler_5_files, &childs_4, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, 0);
    for (int i = 0; i < 10; ++i) {
      std::string filename = "file_" + std::to_string(i);
      if (i >= 6)
        ASSERT_TRUE(childs_4.count(filename));
      else
        ASSERT_FALSE(childs_4.count(filename));
    }

    off_t off = 4;
    do {
      childs_5.clear();
      r = fs_->readdir(parent, off, dirp, filler_5_files, &childs_5, nullptr,
                       readdirplus, nullptr);
      if (r != 0) {
        ASSERT_EQ(r, -ENOSPC);
        ASSERT_EQ(int(childs_5.size()), 5);
      }

      for (int i = 0; i < 10; ++i) {
        std::string filename = "file_" + std::to_string(i);
        if (i >= int(off) - 2 && i <= int(off - 3 + childs_5.size()))
          ASSERT_TRUE(childs_5.count(filename));
        else
          ASSERT_FALSE(childs_5.count(filename));
      }

      off += childs_5.size();
      forget_children(childs_5);

      if (r == 0) {
        ASSERT_SIZE_EQ(childs_5.size(), 3);
        break;
      }
    } while (r == -ENOSPC);

    r = fs_->readdir(parent, 16, dirp, filler_5_files, &childs_6, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -EINVAL);

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);

    forget_children(childs_3);
    forget_children(childs_4);
  }

  void verify_readdir_out_of_order(bool readdirplus) {
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 1);

    for (int i = 0; i < 14; ++i) {
      std::string filename("file_");
      filename.push_back(char('A' + i));
      r = upload_file(local_file, join_paths(parent_path, filename),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    auto filler_2_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      std::unordered_map<std::string, uint64_t> *nodes =
          reinterpret_cast<std::unordered_map<std::string, uint64_t> *>(ctx);
      if (nodes->size() && nodes->size() % 2 == 0) return -ENOSPC;
      (*nodes)[std::string(name)] = nodeid;
      return 0;
    };

    auto forget_children =
        [&](std::unordered_map<std::string, uint64_t> &children) {
          if (!readdirplus) return;

          for (auto &ent : children) {
            if (ent.first != ".." && ent.first != ".")
              fs_->forget(ent.second, 1);
          }
        };

    auto readdir_check = [&](std::unordered_map<std::string, uint64_t> &childs,
                             void *dirp, off_t off, bool &check_pass) {
      check_pass = false;
      int rr = fs_->readdir(parent, off, dirp, filler_2_files, &childs, nullptr,
                            readdirplus, nullptr);
      ASSERT_EQ(rr, -ENOSPC);
      ASSERT_SIZE_EQ(childs.size(), 2);
      for (auto &dd : childs) {
        LOG_INFO("name: `", dd.first);
      }
      for (int i = 0; i < 14; ++i) {
        std::string filename("file_");
        filename.push_back(char('A' + i));
        if (i <= off - 1 && i >= off - 2)
          ASSERT_TRUE(childs.count(filename));
        else
          ASSERT_FALSE(childs.count(filename));
      }

      forget_children(childs);
      check_pass = true;
    };

    bool check_pass = false;
    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    // 1. Readdir from offset 0, and we will get 5 results from oss.
    std::unordered_map<std::string, uint64_t> childs, childs_1, childs_2,
        childs_3, childs_4, childs_5, childs_6;
    r = fs_->readdir(parent, 0, dirp, filler_2_files, &childs, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -ENOSPC);  // succeed to fill 2 nodes
    ASSERT_SIZE_EQ(childs.size(), 2);
    for (int i = 0; i < 8; ++i) {
      std::string filename = "file_" + std::to_string(i);
      ASSERT_FALSE(childs.count(filename));
    }

    // 2. Expect offset is 2 now, jump forwards to 4.
    OssDirHandle *odh = static_cast<OssDirHandle *>(dirp);
    ASSERT_TRUE(odh->telldir() != 4);
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->readdir(parent, 4, dirp, filler_2_files, &childs_1, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    readdir_check(childs_1, dirp, 4, check_pass);
    ASSERT_TRUE(check_pass);

    // 3. Expect offset is 6 now, jump backwards to 3, fill 2 files.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    ASSERT_TRUE(odh->telldir() != 2);
    readdir_check(childs_2, dirp, 3, check_pass);
    ASSERT_TRUE(check_pass);

    // 4. Expect offset is 5 now, jump forwards to 7, need to trigger a new req.
    ASSERT_TRUE(odh->telldir() != 7);
    r = fs_->readdir(parent, 7, dirp, filler_2_files, &childs_3, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -EIO);
    ASSERT_SIZE_EQ(childs_3.size(), 0);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    readdir_check(childs_3, dirp, 5, check_pass);
    ASSERT_TRUE(check_pass);

    // 5. Expect offset is 9 now, jump backwards to 4.
    // Hit last response.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    readdir_check(childs_4, dirp, 4, check_pass);
    ASSERT_TRUE(check_pass);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    // 6. Jump forwards to 12, need to trigger a new req, update last response.
    readdir_check(childs_5, dirp, 12, check_pass);
    ASSERT_TRUE(check_pass);

    // 7. Jump backwards to 4, cannot hit last response.
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->readdir(parent, 4, dirp, filler_2_files, &childs_6, nullptr,
                     readdirplus, nullptr);
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    readdir_check(childs_6, dirp, 4, check_pass);
    ASSERT_TRUE(check_pass);

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
  }

  void verify_readdir_outoforder_with_dirty_children() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    auto filler_3_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      std::unordered_map<std::string, uint64_t> *nodes =
          reinterpret_cast<std::unordered_map<std::string, uint64_t> *>(ctx);
      if (nodes->size() && nodes->size() % 3 == 0) return -ENOSPC;
      (*nodes)[std::string(name)] = nodeid;
      return 0;
    };

    auto forget_children =
        [&](std::unordered_map<std::string, uint64_t> &children) {
          for (auto &ent : children) {
            if (ent.first != ".." && ent.first != ".")
              fs_->forget(ent.second, 1);
          }
        };

    bool check_pass = false;
    auto readdir_check = [&](void *dirp, off_t off,
                             const std::vector<int> &expected_files) {
      check_pass = false;
      std::unordered_map<std::string, uint64_t> childs;
      int rr = fs_->readdir(parent, off, dirp, filler_3_files, &childs, nullptr,
                            true, nullptr);
      ASSERT_EQ(rr, -ENOSPC);
      ASSERT_SIZE_EQ(childs.size(), 3);

      ASSERT_SIZE_EQ(3, expected_files.size());
      for (int i = 0; i < 3; ++i) {
        std::string filename = "testfile_" + std::to_string(expected_files[i]);
        ASSERT_TRUE(childs.count(filename));
      }
      check_pass = true;

      forget_children(childs);
    };

    struct stat stbuf;
    int r = 0;
    std::vector<void *> handles;
    std::vector<uint64_t> dirty_nodeids;
    // Ensure all opened handles are released even if ASSERT fails early.
    DEFER({
      for (size_t i = 0; i < handles.size(); ++i) {
        fs_->release(dirty_nodeids[i], get_file_from_handle(handles[i]));
        fs_->forget(dirty_nodeids[i], 1);
      }
    });
    for (int i = 1; i < 10; i += 2) {
      std::string filename = "testfile_" + std::to_string(i);
      uint64_t nodeid;
      void *handle = nullptr;
      r = fs_->creat(parent, filename, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                     &nodeid, &stbuf, &handle);
      ASSERT_EQ(r, 0);
      handles.push_back(handle);
      dirty_nodeids.push_back(nodeid);
    }

    for (int i = 0; i < 10; ++i) {
      std::string filename = "testfile_" + std::to_string(i);
      r = upload_file(local_file, join_paths(parent_path, filename),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }
    // The ascii of 'a' is larger than '9'. Upload testfile_a to avoid
    // list ending right at testfile_9.
    r = upload_file(local_file, join_paths(parent_path, "testfile_a"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);
    DEFER(if (dirp) fs_->releasedir(parent, dirp));
    // Ensure fault injection is cleared on any exit path.
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed));

    std::unordered_map<std::string, uint64_t> childs;
    // 1. readdir from offset 0. cur_list_res: empty
    // dirty_children: testfile_ 1 3 5 7 9
    r = fs_->readdir(parent, 0, dirp, filler_3_files, &childs, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, -ENOSPC);
    ASSERT_SIZE_EQ(childs.size(), 3);
    OssDirHandle *odh = static_cast<OssDirHandle *>(dirp);
    ASSERT_TRUE(childs.count("testfile_1"));
    ASSERT_TRUE(childs.count("."));
    ASSERT_TRUE(childs.count(".."));
    forget_children(childs);
    childs.clear();

    readdir_check(dirp, 3, {3, 5, 7});
    ASSERT_TRUE(check_pass);

    // Jump backwards inside dirty children.
    ASSERT_EQ(odh->telldir(), 4);
    readdir_check(dirp, 3, {3, 5, 7});
    ASSERT_TRUE(check_pass);

    // Comsumed all the dirty children.
    ASSERT_EQ(odh->telldir(), 4);
    readdir_check(dirp, 4, {5, 7, 9});
    ASSERT_TRUE(check_pass);

    // Verify the case pos_ == 0
    ASSERT_EQ(odh->get_list_pos(), size_t(0));
    ASSERT_EQ(odh->telldir(), 5);
    readdir_check(dirp, 3, {3, 5, 7});
    ASSERT_TRUE(check_pass);

    ASSERT_EQ(odh->telldir(), 4);
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->readdir(parent, 6, dirp, filler_3_files, &childs, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, -EIO);
    forget_children(childs);  // testfile_9 is dirty, so it is also filled
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    // 3. readdir in cur_list_res: 0 1 2 3 4 5 6
    readdir_check(dirp, 6, {9, 0, 2});
    ASSERT_TRUE(check_pass);
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);

    // Move backwards to 7, skip dirty children in cur_list_res
    ASSERT_EQ(odh->telldir(), 7);
    readdir_check(dirp, 7, {0, 2, 4});
    ASSERT_TRUE(check_pass);

    // 4. trigger a new list req, readdir in last response.
    // cur_list_res: testfile_ 7 8 9 a, last_response: testfile_ 0 1 2 3 4 5 6
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    ASSERT_EQ(odh->telldir(), 8);
    readdir_check(dirp, 9, {4, 6, 8});
    ASSERT_TRUE(check_pass);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    ASSERT_EQ(odh->telldir(), 10);
    readdir_check(dirp, 7, {0, 2, 4});
    ASSERT_TRUE(check_pass);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    dirp = nullptr;  // DEFER no longer needs to release

    // DEFER handles cleanup of handles/dirty_nodeids.
  }

  void verify_readdir_no_plus() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    size_t total_files = rand() % 100 + 1;

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 1);
    for (size_t i = 0; i < total_files; i++) {
      std::string filepath = "testfile" + std::to_string(i);
      int r = upload_file(local_file, join_paths(parent_path, filepath),
                          FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    struct fuse_file_info fi;
    int r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs;
    r = fs_->readdir(parent, 0, dirp, filler, &childs, nullptr, false, nullptr);
    ASSERT_EQ(r, 0);

    DEFER(fs_->releasedir(parent, dirp));

    // include "." and ".."
    ASSERT_EQ(childs.size(), total_files + 2);

    std::set<std::string> child_names;
    for (auto &child : childs) {
      child_names.insert(child.name);
    }

    for (size_t i = 0; i < total_files; i++) {
      std::string filepath = "testfile" + std::to_string(i);
      ASSERT_TRUE(child_names.find(filepath) != child_names.end());
    }
  }

  void verify_readdir_interruption(bool readdirplus) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int r = 0;
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    auto filler_2_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      std::unordered_map<std::string, uint64_t> *nodes =
          reinterpret_cast<std::unordered_map<std::string, uint64_t> *>(ctx);
      if (nodes->size() && nodes->size() % 2 == 0) return -ENOSPC;
      (*nodes)[std::string(name)] = nodeid;
      return 0;
    };

    auto forget_children =
        [&](std::unordered_map<std::string, uint64_t> &children) {
          if (!readdirplus) return;

          for (auto &ent : children) {
            if (ent.first != ".." && ent.first != ".")
              fs_->forget(ent.second, 1);
          }
        };

    for (size_t i = 0; i < 6; i++) {
      std::string file_name = "file_" + std::to_string(i);
      r = upload_file(local_file, join_paths(parent_path, file_name),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    auto is_interrupted = [](void *ctx) -> int {
      auto *tp =
          static_cast<std::chrono::time_point<std::chrono::steady_clock> *>(
              ctx);
      auto now = std::chrono::steady_clock::now();
      auto period =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - *tp)
              .count();
      if (period > 400) {
        return 1;
      }

      return 0;
    };

    std::unordered_map<std::string, uint64_t> childs;
    auto time_before_req = std::chrono::steady_clock::now();
    r = fs_->readdir(parent, 0, dirp, filler_2_files, &childs, is_interrupted,
                     readdirplus, &time_before_req);
    ASSERT_EQ(r, -ENOSPC);
    forget_children(childs);
    childs.clear();

    // Jump forwards to trigger seekdir, verify it can be interrupted.
    g_fault_injector->set_injection(FaultInjectionId::FI_Readdir_list_Delay);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Readdir_list_Delay));
    r = fs_->readdir(parent, 4, dirp, filler_2_files, &childs, is_interrupted,
                     readdirplus, &time_before_req);
    ASSERT_EQ(r, -EINTR);

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);

    auto now = std::chrono::steady_clock::now();
    ASSERT_LT(childs.size(), size_t(6));
    auto period = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now - time_before_req)
                      .count();
    ASSERT_LT(period, 1200);  // less than 300ms * 3
  }

  void verify_readdir_with_dirty_files() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // 1. dir contains dirty files that doesn't exist on the cloud
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::string filepath = "testfile1";
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t file_nodeid = 0;
    void *file_handle = nullptr;
    std::string filename = "testfile2";
    r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &file_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    // Ensure handle cleanup on any exit path.
    DEFER({
      if (file_handle) {
        fs_->release(file_nodeid,
                     reinterpret_cast<IFileHandleFuseLL *>(file_handle));
      }
      if (file_nodeid) fs_->forget(file_nodeid, 1);
    });

    // In HDFS mode, file_handle is HdfsFileHandle*, not OssFileHandle*.
    // "dirty" concept only applies to OSS mode (local staging).
    if (!is_hdfs_test_mode()) {
      auto file = (OssFileHandle *)(file_handle);
      ASSERT_TRUE(file->get_is_dirty());
    }

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);
    DEFER(if (dirp) fs_->releasedir(parent, dirp));

    std::vector<TestInode> childs;
    r = fs_->readdir(parent, 0, dirp, filler, &childs, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);

    bool found_1 = false, found_2 = false;
    for (auto &child : childs) {
      if (child.name == "testfile2")
        found_2 = true;
      else if (child.name == "testfile1")
        found_1 = true;

      if (child.name != "." && child.name != "..") fs_->forget(child.nodeid, 1);
    }
    ASSERT_TRUE(found_1);
    ASSERT_TRUE(found_2);

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    dirp = nullptr;  // DEFER no longer needs to release

    r = fs_->release(file_nodeid,
                     reinterpret_cast<IFileHandleFuseLL *>(file_handle));
    ASSERT_EQ(r, 0);
    file_handle = nullptr;  // DEFER no longer needs to release
    // forget is handled by DEFER

    // 2. dir contains dirty files that exist on the cloud
    bool unused = 0;
    r = fs_->open(file_nodeid, O_RDWR | O_TRUNC, &file_handle, &unused);
    ASSERT_EQ(r, 0);

    auto file_ptr = get_file_from_handle(file_handle);
    auto random_data = random_string(1 << 10);
    r = file_ptr->pwrite(random_data.c_str(), 1 << 10, 0);
    ASSERT_EQ(r, 1 << 10);

    struct Entry {
      uint64_t nodeid = 0;
      std::string name;
      off_t size = 0;

      Entry(uint64_t nodeid, const std::string &name, off_t size)
          : nodeid(nodeid), name(name), size(size) {}
    };

    auto filler_with_stat = [](void *ctx, const uint64_t nodeid,
                               const char *name, const struct stat *stbuf,
                               off_t off) -> int {
      std::vector<Entry> *nodes = reinterpret_cast<std::vector<Entry> *>(ctx);
      nodes->emplace_back(nodeid, name, stbuf->st_size);
      // we get all the dir entries one time
      return 0;
    };

    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<Entry> childs_2;
    r = fs_->readdir(parent, 0, dirp, filler_with_stat, &childs_2, nullptr,
                     true, nullptr);
    ASSERT_EQ(r, 0);

    found_1 = false;
    found_2 = false;
    for (auto &child : childs_2) {
      if (child.name == "testfile2") {
        found_2 = true;
        ASSERT_EQ(child.size, 1 << 10);
      } else if (child.name == "testfile1") {
        found_1 = true;
      }

      if (child.name != "." && child.name != "..") fs_->forget(child.nodeid, 1);
    }

    ASSERT_TRUE(found_2);
    ASSERT_TRUE(found_1);
    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
    dirp = nullptr;

    r = fs_->release(file_nodeid,
                     reinterpret_cast<IFileHandleFuseLL *>(file_handle));
    ASSERT_EQ(r, 0);
    file_handle = nullptr;
  }

  void verify_readdir_type_change() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // create dir same_name, and file same_name/file
    uint64_t dir_nodeid, subfile_nodeid;
    void *file_handle;
    int r = fs_->mkdir(parent, "same_name", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    r = fs_->creat(dir_nodeid, "file", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &subfile_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(subfile_nodeid,
                     reinterpret_cast<IFileHandleFuseLL *>(file_handle));
    DEFER(fs_->forget(subfile_nodeid, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    // upload file samename.
    r = upload_file(local_file, join_paths(parent_path, "same_name"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // verify readdir
    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs;
    r = fs_->readdir(parent, 0, dirp, filler, &childs, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);

    ASSERT_SIZE_EQ(childs.size(), 4);
    int same_name_cnt = 0;
    for (auto &node : childs) {
      if (node.name == "same_name") {
        same_name_cnt++;
        fs_->forget(node.nodeid, 1);
      } else if (node.name != "." && node.name != "..") {
        fs_->forget(node.nodeid, 1);
      }
    }

    ASSERT_EQ(same_name_cnt, 2);
  }

  void verify_readdir_while_renamedir() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t dir_id;
    struct stat st;
    int r = fs_->mkdir(parent, "testdir-suffix", 0777, 0, 0, 0, &dir_id, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_id, 1));

    auto parent_path = nodeid_to_path(dir_id);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    for (int i = 0; i < 5; ++i) {
      r = upload_file(local_file,
                      join_paths(parent_path, "testfile_" + std::to_string(i)),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    auto filler_2_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;
      std::unordered_map<std::string, uint64_t> *nodes =
          reinterpret_cast<std::unordered_map<std::string, uint64_t> *>(ctx);
      if (nodes->size() && nodes->size() % 2 == 0) return -ENOSPC;
      (*nodes)[std::string(name)] = nodeid;
      return 0;
    };

    // 1. opendir
    struct fuse_file_info fi;
    r = fs_->opendir(dir_id, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    // 2. readdir for the 1st time
    std::unordered_map<std::string, uint64_t> childs_1, childs_2;
    r = fs_->readdir(dir_id, 0, dirp, filler_2_files, &childs_1, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, -ENOSPC);
    ASSERT_SIZE_EQ(childs_1.size(), 2);

    // 3. renamedir
    r = fs_->rename(parent, "testdir-suffix", parent, "testdir", 0);
    ASSERT_EQ(r, 0);

    // 4. readdir for the 2nd time
    r = fs_->readdir(dir_id, 4, dirp, filler_2_files, &childs_2, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, -ESTALE);
    // Otherwise, if we keep readdir to the end, the total children's number
    // will be greater than 5.

    r = fs_->releasedir(dir_id, dirp);
    ASSERT_EQ(r, 0);
    for (auto &entry : childs_1) {
      fs_->forget(entry.second, 1);
    }
  }

  void verify_readdir_concurrently_noplus() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    int r = 0;
    int file_num = 9;
    for (int i = 0; i < file_num; ++i) {
      r = upload_file(
          local_file,
          join_paths(parent_path, ("file_" + std::to_string(i)).c_str()),
          FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    auto filler_5_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      std::vector<TestInode> *nodes =
          reinterpret_cast<std::vector<TestInode> *>(ctx);
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;

      if (nodes->size() && nodes->size() % 5 == 0) return -ENOSPC;
      nodes->emplace_back(nodeid, name);
      return 0;
    };

    struct fuse_file_info fi, fi2;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    g_fault_injector->set_injection(FaultInjectionId::FI_Readdir_Delay_Noplus);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Readdir_Delay_Noplus));
    auto readdircb = [&]() -> int {
      std::vector<TestInode> childs, child2;
      int rr = fs_->readdir(parent, 0, dirp, filler_5_files, &childs, nullptr,
                            false, nullptr);
      if (rr == -ENOSPC) {
        // Fill the rest 5 entries. The first 7 entries includes . and ..
        rr = fs_->readdir(parent, 7, dirp, filler_5_files, &child2, nullptr,
                          false, nullptr);
      }

      return rr;
    };

    int r1 = 0;
    auto thread1 = std::thread([&]() {
      INIT_PHOTON();
      r1 = readdircb();
    });

    r = readdircb();
    if (thread1.joinable()) thread1.join();

    // Out-of-order readdir is supported, so the 2 threads
    // will both get the whole result.
    ASSERT_EQ(r1, 0);
    ASSERT_EQ(r, 0);

    fs_->releasedir(parent, dirp);

    // refresh_dir is also protected by dir_lock,
    // so the readdir in the 2 threads will both refresh and get the whole
    // result .
    std::vector<TestInode> childs1, childs2;
    r = fs_->opendir(parent, &fi2);
    ASSERT_EQ(r, 0);
    void *dirp2 = reinterpret_cast<void *>(fi2.fh);
    auto readdircb2 = [&](std::vector<TestInode> &childs) {
      int rr = fs_->readdir(parent, 0, dirp2, filler, &childs, nullptr, false,
                            nullptr);
      EXPECT_EQ(rr, 0);
    };

    auto thread2 = std::thread([&]() {
      INIT_PHOTON();
      readdircb2(childs1);
    });
    DEFER(if (thread2.joinable()) thread2.join());

    readdircb2(childs2);
    thread2.join();

    ASSERT_EQ(r, 0);
    ASSERT_EQ(int(childs1.size() + childs2.size()), 2 * (2 + file_num));

    fs_->releasedir(parent, dirp2);
  }

  void verify_readdir_cover_stale_child() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = 0;

    auto parent_path = nodeid_to_path(parent);
    for (int i = 0; i < 5; ++i) {
      r = upload_file(local_file,
                      join_paths(parent_path, "testfile_" + std::to_string(i)),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs1, childs2;
    r = fs_->readdir(parent, 0, dirp, filler, &childs1, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs1.size(), 7);

    r = fs_->unlink(parent, "testfile_2");  // stale
    ASSERT_EQ(r, 0);

    uint64_t testfile2_nodeid1;
    for (auto &entry : childs1) {
      if (entry.name == "testfile_2") {
        testfile2_nodeid1 = entry.nodeid;
        break;
      }
    }

    r = upload_file(local_file, join_paths(parent_path, "testfile_2"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->readdir(parent, 0, dirp, filler, &childs2, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs2.size(), 7);
    for (size_t i = 0; i < childs2.size(); ++i) {
      if (childs2[i].name == "testfile_2") {
        ASSERT_NE(childs2[i].nodeid, testfile2_nodeid1);
        fs_->forget(childs2[i].nodeid, 1);
        fs_->forget(testfile2_nodeid1, 1);
      } else if (childs2[i].name != "." && childs2[i].name != "..") {
        ASSERT_EQ(childs2[i].nodeid, childs1[i].nodeid);
        fs_->forget(childs2[i].nodeid, 1);
        fs_->forget(childs1[i].nodeid, 1);
      }
    }

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
  }

  void verify_remember_null_stale_child() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = 0;

    auto parent_path = nodeid_to_path(parent);
    for (int i = 0; i < 10; ++i) {
      r = upload_file(local_file,
                      join_paths(parent_path, "testfile_" + std::to_string(i)),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
    }

    auto filler_6_files = [](void *ctx, const uint64_t nodeid, const char *name,
                             const struct stat *stbuf, off_t off) -> int {
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;

      auto nodes = reinterpret_cast<std::vector<TestInode> *>(ctx);
      if (nodes->size() && nodes->size() % 6 == 0) return -ENOSPC;
      nodes->emplace_back(nodeid, name);

      return 0;
    };

    struct fuse_file_info fi;
    r = fs_->opendir(parent, &fi);
    ASSERT_EQ(r, 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> children, childs1, childs2;
    r = fs_->readdir(parent, 0, dirp, filler_6_files, &childs1, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, -ENOSPC);

    // testfile_2 is in the readdir result but is stale with the right
    // lookup_cnt.
    r = fs_->unlink(parent, "testfile_2");
    ASSERT_EQ(r, 0);

    r = fs_->unlink(parent, "testfile_7");
    ASSERT_EQ(r, 0);

    r = fs_->readdir(parent, 2 + childs1.size(), dirp, filler_6_files,
                     &children, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);

    childs1.insert(childs1.end(), children.begin(), children.end());
    ASSERT_SIZE_EQ(childs1.size(), 9);  // skip testfile_7
    for (size_t i = 0; i < childs1.size(); ++i) {
      ASSERT_NE(childs1[i].name, "testfile_7");
    }

    uint64_t nodeid;
    struct stat st;
    r = fs_->lookup(parent, "testfile_2", &nodeid, &st);
    ASSERT_EQ(r, -ENOENT);

    r = fs_->lookup(parent, "testfile_8", &nodeid, &st);
    ASSERT_EQ(r, 0);

    r = fs_->readdir(parent, 0, dirp, filler_6_files, &childs2, nullptr, true,
                     nullptr);
    ASSERT_EQ(r, -ENOSPC);
    r = fs_->unlink(parent, "testfile_8");
    ASSERT_EQ(r, 0);
    fs_->forget(nodeid, 2);  // this inode should be destroyed

    children.clear();
    r = fs_->readdir(parent, 2 + childs2.size(), dirp, filler_6_files,
                     &children, nullptr, true, nullptr);
    ASSERT_EQ(r, 0);
    childs2.insert(childs2.end(), children.begin(), children.end());
    ASSERT_SIZE_EQ(childs2.size(), 7);

    for (size_t i = 0; i < childs1.size(); ++i) {
      if (childs1[i].name != "." && childs1[i].name != "..") {
        fs_->forget(childs1[i].nodeid, 1);
      }
    }
    for (size_t i = 0; i < childs2.size(); ++i) {
      if (childs2[i].name != "." && childs2[i].name != "..") {
        fs_->forget(childs2[i].nodeid, 1);
      }
    }

    r = fs_->releasedir(parent, dirp);
    ASSERT_EQ(r, 0);
  }

  void verify_readdir_missing_object_type() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create regular files
    std::vector<uint64_t> file_nodeids;
    for (int i = 0; i < 3; i++) {
      uint64_t nodeid;
      void *handle;
      struct stat st;
      std::string name = "file_" + std::to_string(i);
      int r = create_and_flush(parent, name.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                               0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      file_nodeids.push_back(nodeid);
    }
    DEFER(for (auto nid : file_nodeids) fs_->forget(nid, 2););

    // Enable injection: clear type in walk_list_results
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Oss_Missing_Object_Type);

    // readdir should succeed, all files should be regular (not symlinks)
    std::vector<TestInode> childs;
    int r = read_dir_without_dots(parent, childs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(childs.size(), 3u);

    // Verify each file's inode type is regular file
    for (auto &child : childs) {
      struct stat st;
      r = fs_->getattr(child.nodeid, &st);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(S_ISREG(st.st_mode))
          << child.name << " should be regular file, got mode=" << st.st_mode;
    }

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Oss_Missing_Object_Type);
  }

  // Drop the whole lookup_cnt of every remaining child of dir_nodeid so
  // their inodes are destroyed.
  void forget_all_children(uint64_t dir_nodeid) {
    auto iter = fs_->global_inodes_map_.find(dir_nodeid);
    if (iter == fs_->global_inodes_map_.end()) return;
    auto *dir = static_cast<DirInode *>(iter->second);
    std::vector<std::pair<uint64_t, uint64_t>> child_ids;
    for (auto &kv : dir->children) {
      child_ids.emplace_back(kv.second->nodeid, kv.second->lookup_cnt);
    }
    for (auto &p : child_ids) {
      if (p.second > 0) fs_->forget(p.first, p.second);
    }
  }

  // Drop the whole lookup_cnt of a known nodeid. Deleted children keep
  // their create/fill references but may be unreachable via the parent's
  // children map after unlink/rmdir/release, so clean them up explicitly.
  void forget_inode_full(uint64_t nodeid) {
    auto iter = fs_->global_inodes_map_.find(nodeid);
    if (iter == fs_->global_inodes_map_.end()) return;
    uint64_t cnt = iter->second->lookup_cnt;
    if (cnt > 0) fs_->forget(nodeid, cnt);
  }

  int create_file_and_release(uint64_t parent, const std::string &name,
                              uint64_t *nodeid) {
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, name.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, nodeid, &st, &handle);
    if (r != 0) return r;
    return fs_->release(*nodeid, get_file_from_handle(handle));
  }

  static int filler_all(void *ctx, const uint64_t nodeid, const char *name,
                        const struct stat *stbuf, off_t off) {
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;
    auto *nodes = reinterpret_cast<std::vector<TestInode> *>(ctx);
    nodes->emplace_back(nodeid, name);
    return 0;
  }

  // Release-hidden is serialized with readdirplus by the parent lock: even
  // when release is racing to land inside the readdirplus call (the window is
  // opened by fault injection), it must wait until the call finishes. The
  // listing therefore sees the hidden object still present and fills it with
  // the original live inode; no ghost is left behind.
  void verify_readdirplus_release_hidden_serialized() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    ASSERT_EQ(create_and_flush(parent, "victim", CREATE_BASE_FLAGS, 0777, 0, 0,
                               0, &nodeid, &st, &handle),
              0);
    auto *inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

    // Unlink while open: the file is hidden instead of deleted.
    ASSERT_EQ(fs_->unlink(parent, "victim"), 0);
    ASSERT_TRUE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Readdir_Delay_Before_Construct, FaultInjection{1});

    std::vector<TestInode> childs;
    std::thread th([&]() {
      struct fuse_file_info fi;
      ASSERT_EQ(fs_->opendir(parent, &fi), 0);
      void *dirp = reinterpret_cast<void *>(fi.fh);
      ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_all, &childs, nullptr,
                             true, nullptr),
                0);
      ASSERT_EQ(fs_->releasedir(parent, dirp), 0);
    });

    // Release tries to land inside the readdirplus call; the parent lock must
    // make it wait until the call finishes.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    ASSERT_EQ(fs_->release(nodeid, get_file_from_handle(handle)), 0);
    ASSERT_TRUE(inode->is_stale);

    th.join();
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Readdir_Delay_Before_Construct);

    // The hidden entry was listed before the delete and filled with the
    // original inode.
    ASSERT_SIZE_EQ(childs.size(), 1);
    ASSERT_TRUE(childs[0].name.rfind(".fuse_hidden", 0) == 0);
    ASSERT_EQ(childs[0].nodeid, nodeid);

    forget_inode_full(nodeid);

    // No ghost may survive: the directory must be removable.
    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  // The objects of local children are deleted remotely; a from-start readdir
  // gets an empty listing and must mark those ghost children stale so they
  // are no longer served from the local attr cache.
  void verify_readdirplus_empty_listing_reaps_ghost_files() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    constexpr int kFileCnt = 3;
    for (int i = 0; i < kFileCnt; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      uint64_t nodeid = 0;
      ASSERT_EQ(create_file_and_release(parent, name, &nodeid), 0);
    }

    // Materialize each child with a lookup so a live local inode with a
    // valid attr cache exists: this is the ghost the sweep must reap.
    for (int i = 0; i < kFileCnt; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      uint64_t nid = 0;
      ASSERT_EQ(fs_->lookup(parent, name, &nid, &st), 0);
    }

    // Delete the objects remotely; the local children know nothing about it.
    auto parent_path = nodeid_to_path(parent);
    auto background_env =
        bg_vcpu_env_.bg_obj_store_env->get_obj_store_env_next();
    background_env.executor->perform([&]() {
      auto obj_store = background_env.obj_store;
      for (int i = 0; i < kFileCnt; ++i) {
        std::string name = "file_";
        name.push_back(char('a' + i));
        ASSERT_EQ(obj_store->delete_object(join_paths(parent_path, name)), 0);
      }
    });

    struct fuse_file_info fi;
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs;
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_all, &childs, nullptr, true,
                           nullptr),
              0);
    ASSERT_EQ(fs_->releasedir(parent, dirp), 0);
    ASSERT_SIZE_EQ(childs.size(), 0);

    // The ghost children must have been marked stale by the sweep.
    auto piter = fs_->global_inodes_map_.find(parent);
    ASSERT_TRUE(piter != fs_->global_inodes_map_.end());
    for (int i = 0; i < kFileCnt; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      Inode *cinode =
          static_cast<DirInode *>(piter->second)->find_child_node(name);
      ASSERT_NE(cinode, nullptr);
      ASSERT_TRUE(cinode->is_stale);
    }

    // The ghosts must no longer be served from the local attr cache.
    for (int i = 0; i < kFileCnt; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      uint64_t nid = 0;
      ASSERT_EQ(fs_->lookup(parent, name, &nid, &st), -ENOENT);
    }

    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  struct CappedCtx {
    std::vector<TestInode> *nodes;
    size_t cap;
  };

  // A filler accepting at most `cap` entries; returns -1 when full, so
  // readdir stops with the rest of the listing page left unfilled.
  static int filler_capped(void *ctx, const uint64_t nodeid, const char *name,
                           const struct stat *stbuf, off_t off) {
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;
    auto *c = reinterpret_cast<CappedCtx *>(ctx);
    if (c->nodes->size() >= c->cap) return -1;
    c->nodes->emplace_back(nodeid, name);
    return 0;
  }

  // Sequential continuation over a cached page after deleting an entry that
  // was listed but not yet filled: the stale child is skipped, never
  // resurrected as a new inode, and the rest of the page is filled normally.
  void verify_readdirplus_seq_delete_rest_of_page() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    uint64_t na = 0, nb = 0, nc = 0;
    ASSERT_EQ(create_file_and_release(parent, "file_a", &na), 0);
    ASSERT_EQ(create_file_and_release(parent, "file_b", &nb), 0);
    ASSERT_EQ(create_file_and_release(parent, "file_c", &nc), 0);

    struct fuse_file_info fi;
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    // Fill only file_a; file_b/file_c stay constructed but unfilled.
    std::vector<TestInode> childs1;
    CappedCtx ctx1{&childs1, 1};
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_capped, &ctx1, nullptr, true,
                           nullptr),
              -1);
    ASSERT_SIZE_EQ(childs1.size(), 1);

    ASSERT_EQ(fs_->unlink(parent, "file_b"), 0);

    // Sequential continuation: file_b is stale and skipped, no new inode.
    std::vector<TestInode> childs2;
    ASSERT_EQ(fs_->readdir(parent, 3, dirp, filler_all, &childs2, nullptr, true,
                           nullptr),
              0);
    ASSERT_SIZE_EQ(childs2.size(), 1);
    ASSERT_EQ(childs2[0].name, "file_c");
    ASSERT_EQ(childs2[0].nodeid, nc);

    Inode *b = static_cast<DirInode *>(fs_->global_inodes_map_[parent])
                   ->find_child_node("file_b");
    ASSERT_NE(b, nullptr);
    ASSERT_EQ(b->nodeid, nb);
    ASSERT_TRUE(b->is_stale);

    ASSERT_EQ(fs_->releasedir(parent, dirp), 0);

    ASSERT_EQ(fs_->unlink(parent, "file_a"), 0);
    ASSERT_EQ(fs_->unlink(parent, "file_c"), 0);
    forget_all_children(parent);
    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  // A deletion of an entry in a not-yet-fetched page lands before that page
  // is LISTed: the fresh listing under the parent lock never contains it and
  // no inode is constructed for it.
  void verify_readdirplus_seq_delete_next_page() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    uint64_t nids[5];
    for (int i = 0; i < 5; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      ASSERT_EQ(create_file_and_release(parent, name, &nids[i]), 0);
    }

    struct fuse_file_info fi;
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    // max_keys is 2: fill only file_a of the first page.
    std::vector<TestInode> childs1;
    CappedCtx ctx1{&childs1, 1};
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_capped, &ctx1, nullptr, true,
                           nullptr),
              -1);
    ASSERT_SIZE_EQ(childs1.size(), 1);

    ASSERT_EQ(fs_->unlink(parent, "file_c"), 0);

    // Continuation pages through the rest: file_c is absent from the fresh
    // listings and never constructed.
    std::vector<TestInode> childs2;
    ASSERT_EQ(fs_->readdir(parent, 3, dirp, filler_all, &childs2, nullptr, true,
                           nullptr),
              0);
    ASSERT_SIZE_EQ(childs2.size(), 3);
    ASSERT_EQ(childs2[0].name, "file_b");
    ASSERT_EQ(childs2[0].nodeid, nids[1]);
    ASSERT_EQ(childs2[1].name, "file_d");
    ASSERT_EQ(childs2[1].nodeid, nids[3]);
    ASSERT_EQ(childs2[2].name, "file_e");
    ASSERT_EQ(childs2[2].nodeid, nids[4]);

    Inode *c = static_cast<DirInode *>(fs_->global_inodes_map_[parent])
                   ->find_child_node("file_c");
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(c->nodeid, nids[2]);
    ASSERT_TRUE(c->is_stale);

    ASSERT_EQ(fs_->releasedir(parent, dirp), 0);

    for (auto name : {"file_a", "file_b", "file_d", "file_e"}) {
      ASSERT_EQ(fs_->unlink(parent, name), 0);
    }
    forget_all_children(parent);
    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  // An entry is deleted and recreated before the sequential continuation
  // fills it: the fill must return the brand-new inode, not the stale one.
  void verify_readdirplus_seq_recreate_after_delete() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    uint64_t na = 0, nb = 0, nc = 0;
    ASSERT_EQ(create_file_and_release(parent, "file_a", &na), 0);
    ASSERT_EQ(create_file_and_release(parent, "file_b", &nb), 0);
    ASSERT_EQ(create_file_and_release(parent, "file_c", &nc), 0);
    DEFER(forget_inode_full(nb));

    struct fuse_file_info fi;
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs1;
    CappedCtx ctx1{&childs1, 1};
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_capped, &ctx1, nullptr, true,
                           nullptr),
              -1);
    ASSERT_SIZE_EQ(childs1.size(), 1);

    ASSERT_EQ(fs_->unlink(parent, "file_b"), 0);
    uint64_t nb2 = 0;
    ASSERT_EQ(create_file_and_release(parent, "file_b", &nb2), 0);
    ASSERT_NE(nb2, nb);

    std::vector<TestInode> childs2;
    ASSERT_EQ(fs_->readdir(parent, 3, dirp, filler_all, &childs2, nullptr, true,
                           nullptr),
              0);
    ASSERT_SIZE_EQ(childs2.size(), 2);
    ASSERT_EQ(childs2[0].name, "file_b");
    ASSERT_EQ(childs2[0].nodeid, nb2);
    ASSERT_EQ(childs2[1].name, "file_c");
    ASSERT_EQ(childs2[1].nodeid, nc);

    ASSERT_EQ(fs_->releasedir(parent, dirp), 0);

    for (auto name : {"file_a", "file_b", "file_c"}) {
      ASSERT_EQ(fs_->unlink(parent, name), 0);
    }
    forget_all_children(parent);
    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  // Rename over an existing target while a cached page is being consumed:
  // the overwritten name fills with the renamed (source) inode and the old
  // source name is gone; no inode is constructed for the vanished name.
  void verify_readdirplus_seq_rename_overwrite_while_filling() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    uint64_t na = 0, nb = 0, nc = 0;
    ASSERT_EQ(create_file_and_release(parent, "file_a", &na), 0);
    ASSERT_EQ(create_file_and_release(parent, "file_b", &nb), 0);
    ASSERT_EQ(create_file_and_release(parent, "file_c", &nc), 0);
    DEFER(forget_inode_full(nb));

    struct fuse_file_info fi;
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs1;
    CappedCtx ctx1{&childs1, 1};
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_capped, &ctx1, nullptr, true,
                           nullptr),
              -1);
    ASSERT_SIZE_EQ(childs1.size(), 1);

    // file_c overwrites file_b.
    ASSERT_EQ(fs_->rename(parent, "file_c", parent, "file_b", 0), 0);

    std::vector<TestInode> childs2;
    ASSERT_EQ(fs_->readdir(parent, 3, dirp, filler_all, &childs2, nullptr, true,
                           nullptr),
              0);
    ASSERT_SIZE_EQ(childs2.size(), 1);
    ASSERT_EQ(childs2[0].name, "file_b");
    ASSERT_EQ(childs2[0].nodeid, nc);

    Inode *c = static_cast<DirInode *>(fs_->global_inodes_map_[parent])
                   ->find_child_node("file_c");
    ASSERT_EQ(c, nullptr);

    ASSERT_EQ(fs_->releasedir(parent, dirp), 0);

    ASSERT_EQ(fs_->unlink(parent, "file_a"), 0);
    ASSERT_EQ(fs_->unlink(parent, "file_b"), 0);
    forget_all_children(parent);
    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  // All children are forgotten after a full listing; a rewind from the start
  // must construct them again from the fresh listing without dropping any.
  void verify_readdirplus_seq_forget_then_refresh() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    ASSERT_EQ(
        fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st), 0);
    DEFER(fs_->forget(parent, 1));
    DEFER(forget_all_children(parent));

    uint64_t nids[3];
    for (int i = 0; i < 3; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      ASSERT_EQ(create_file_and_release(parent, name, &nids[i]), 0);
    }

    struct fuse_file_info fi;
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);

    std::vector<TestInode> childs1;
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_all, &childs1, nullptr, true,
                           nullptr),
              0);
    ASSERT_SIZE_EQ(childs1.size(), 3);

    // The kernel forgets every child; the inodes are reaped.
    forget_all_children(parent);

    // Rewind: the fresh listing reconstructs all entries; nothing is lost.
    std::vector<TestInode> childs2;
    ASSERT_EQ(fs_->readdir(parent, 0, dirp, filler_all, &childs2, nullptr, true,
                           nullptr),
              0);
    ASSERT_SIZE_EQ(childs2.size(), 3);
    // The staged cache is disabled for this case, so reconstructed inodes
    // must carry brand-new nodeids.
    for (int i = 0; i < 3; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      ASSERT_EQ(childs2[i].name, name);
      ASSERT_NE(childs2[i].nodeid, nids[i]);
    }

    ASSERT_EQ(fs_->releasedir(parent, dirp), 0);

    for (int i = 0; i < 3; ++i) {
      std::string name = "file_";
      name.push_back(char('a' + i));
      ASSERT_EQ(fs_->unlink(parent, name), 0);
    }
    forget_all_children(parent);
    ASSERT_EQ(fs_->rmdir(test_dir_nodeid, "workdir"), 0);
  }

  // TODO: cover out-of-order (backward seekdir) readdirplus scenarios: the
  // cached-snapshot replay path may still resurrect locally deleted children
  // as ghost inodes; add those cases together with the freshness fix.
};

TEST_F(Ossfs2ReaddirTest, verify_readdir_while_create_and_unlink) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_while_create_and_unlink();
}

TEST_F(Ossfs2ReaddirTest, verify_partial_readdir) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_partial_readdir();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_offset) {
  INIT_PHOTON();
  srand(time(nullptr));
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_offset(true);
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_offset_noplus) {
  INIT_PHOTON();
  srand(time(nullptr));
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_offset(false);
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_out_of_order) {
  INIT_PHOTON();
  srand(time(nullptr));
  OssFsOptions opts;
  opts.readdir_remember_count = 5;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, 6);
  verify_readdir_out_of_order(true);
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_outoforder_with_dirty_children) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  srand(time(nullptr));
  OssFsOptions opts;
  opts.readdir_remember_count = 8;
  init(opts, 8);
  verify_readdir_outoforder_with_dirty_children();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_out_of_order_noplus) {
  INIT_PHOTON();
  srand(time(nullptr));
  OssFsOptions opts;
  opts.readdir_remember_count = 5;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, 6);
  verify_readdir_out_of_order(false);
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_no_plus) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_no_plus();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_interruption) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, 2);
  verify_readdir_interruption(true);
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_interruption_noplus) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, 2);
  verify_readdir_interruption(false);
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_with_dirty_files) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, get_random_max_keys() + 1);
  verify_readdir_with_dirty_files();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_with_dirty_files_maxkeys_1) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, 1);
  verify_readdir_with_dirty_files();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_type_change) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, get_random_max_keys());
  verify_readdir_type_change();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_while_renamedir) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_while_renamedir();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_concurrently_noplus) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_concurrently_noplus();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_cover_stale_child) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts, get_random_max_keys());
  verify_readdir_cover_stale_child();
}

TEST_F(Ossfs2ReaddirTest, verify_remember_null_stale_child) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_remember_null_stale_child();
}

TEST_F(Ossfs2ReaddirTest, verify_readdir_missing_object_type) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, get_random_max_keys());
  verify_readdir_missing_object_type();
}

TEST_F(Ossfs2ReaddirTest, verify_readdirplus_release_hidden_serialized) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_readdirplus_release_hidden_serialized();
}

TEST_F(Ossfs2ReaddirTest, verify_readdirplus_empty_listing_reaps_ghost_files) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, 3);
  verify_readdirplus_empty_listing_reaps_ghost_files();
}

TEST_F(Ossfs2ReaddirTest, verify_readdirplus_seq_delete_rest_of_page) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdirplus_seq_delete_rest_of_page();
}

TEST_F(Ossfs2ReaddirTest, verify_readdirplus_seq_delete_next_page) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts, 2);
  verify_readdirplus_seq_delete_next_page();
}

TEST_F(Ossfs2ReaddirTest, verify_readdirplus_seq_recreate_after_delete) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdirplus_seq_recreate_after_delete();
}

TEST_F(Ossfs2ReaddirTest,
       verify_readdirplus_seq_rename_overwrite_while_filling) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdirplus_seq_rename_overwrite_while_filling();
}

TEST_F(Ossfs2ReaddirTest, verify_readdirplus_seq_forget_then_refresh) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.max_inode_cache_count = 0;
  init(opts);
  verify_readdirplus_seq_forget_then_refresh();
}
