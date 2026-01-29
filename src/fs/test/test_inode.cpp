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

DEFINE_uint64(test_mem_usage_file_num, 1000000,
              "Create this many sub-FileInodes under a DirInode.");
DEFINE_bool(test_mem_usage_lru, false,
            "Enable metadata usage test with LRU cache.");
DEFINE_bool(test_mem_usage_dir, false, "Create subdir instead of subfile");

// Ossfs2InodeTest contains basic inode tests for lookup, forget,
// getattr, setattr and evict.
class Ossfs2InodeTest : public Ossfs2TestSuite {
 protected:
  void verify_lookup() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Case 1: lookup file with long name
    uint64_t nodeid5 = 0;
    int r = fs_->lookup(parent, random_string(256).c_str(), &nodeid5, &st);
    ASSERT_EQ(r, -ENAMETOOLONG);
    DEFER(fs_->forget(nodeid5, 1));

    // Case 2: lookup file at the same time
    uint64_t file_nodeid = 0;
    void *file_handle = nullptr;
    std::string filename = "testfile";
    r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &file_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(file_nodeid, get_file_from_handle(file_handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(file_nodeid, 1));

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
    uint64_t file_nodeid1 = 0, file_nodeid2 = 0;
    std::thread task([&]() {
      int local_r = fs_->lookup(parent, filename.c_str(), &file_nodeid1, &st);
      ASSERT_EQ(local_r, 0);
      DEFER(fs_->forget(file_nodeid1, 1));
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);

    r = fs_->lookup(parent, filename.c_str(), &file_nodeid2, &st);
    ASSERT_EQ(r, 0);

    if (task.joinable()) {
      task.join();
    }

    ASSERT_EQ(file_nodeid1, file_nodeid2);
    DEFER(fs_->forget(file_nodeid2, 1));

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);

    uint64_t file_nodeid3 = 0;
    r = fs_->lookup(parent, filename.c_str(), &file_nodeid3, &st);
    ASSERT_EQ(r, -ENOENT);
    DEFER(fs_->forget(file_nodeid3, 1));
  }

  void verify_lookup_stale_dirs_recursively() {
    fs_->options_.attr_timeout = 1;
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));
    int r = 0;

    uint64_t parent_nodeid;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent_nodeid,
                   &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent_nodeid, 1));

    // 1. lookup multi-level dirs and files existing on oss
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    uint64_t parent = parent_nodeid;
    auto parent_path = nodeid_to_path(parent);
    const int dir_depth = 5;
    std::set<uint64_t> dir_nodeids, file_nodeids;
    uint64_t last_dir_nodeid, last_file_nodeid;
    for (int i = 0; i < dir_depth; i++) {
      r = upload_file(local_file, join_paths(parent_path, "dir-internal/"),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
      r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->lookup(parent, "dir-internal", &last_dir_nodeid, &st);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(S_ISDIR(st.st_mode));
      dir_nodeids.insert(last_dir_nodeid);

      r = fs_->lookup(parent, "file-internal", &last_file_nodeid, &st);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(S_ISREG(st.st_mode));
      file_nodeids.insert(last_file_nodeid);

      parent_path = join_paths(parent_path, "dir-internal");
      parent = last_dir_nodeid;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Open the last file, to avoid the dir inodes from being marked stale.
    void *handle = nullptr;
    bool unused;
    r = fs_->open(last_file_nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    // 2. delete dirs and files just created
    parent = parent_nodeid;
    parent_path = nodeid_to_path(parent);
    r = delete_dir(parent_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t tmp;
    // mark stale recursively, with opened subfile
    r = fs_->lookup(test_dir_nodeid, "workdir", &tmp, &st);
    ASSERT_EQ(r, -ENOENT);

    fs_->options_.attr_timeout = 30;
    for (auto nodeid : file_nodeids) {
      r = fs_->getattr(nodeid, &st);
      if (nodeid != last_file_nodeid) {
        ASSERT_EQ(r, -ESTALE);
        fs_->forget(nodeid, 1);
      } else {
        ASSERT_EQ(r, 0);
      }
    }

    for (auto nodeid : dir_nodeids) {
      r = fs_->getattr(nodeid, &st);
      if (nodeid != last_dir_nodeid)
        ASSERT_EQ(r, 0);
      else
        ASSERT_EQ(r, -ESTALE);
    }

    r = fs_->release(last_file_nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    fs_->forget(last_file_nodeid, 1);

    // mark stale recursively again
    fs_->options_.attr_timeout = 0;
    r = fs_->lookup(test_dir_nodeid, "workdir", &tmp, &st);
    ASSERT_EQ(r, -ENOENT);

    fs_->options_.attr_timeout = 30;
    r = fs_->getattr(last_file_nodeid, &st);
    ASSERT_EQ(r, -ESTALE);

    for (auto nodeid : dir_nodeids) {
      r = fs_->getattr(nodeid, &st);
      ASSERT_EQ(r, -ESTALE);
      fs_->forget(nodeid, 1);
    }
  }

  void verify_lookup_simultaneously() {
    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));
    int r = 0;

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    auto parent_path = nodeid_to_path(test_dir_nodeid);
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
    uint64_t nodeid1, nodeid2;
    struct stat st1, st2;
    // 1. thread 1, lookup and get
    std::thread t1([&]() {
      int r1 = fs_->lookup(test_dir_nodeid, "file-internal", &nodeid1, &st1);
      ASSERT_EQ(r1, 0);
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
    create_random_file(local_file, 4);
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    r = fs_->lookup(test_dir_nodeid, "file-internal", &nodeid2, &st2);
    ASSERT_EQ(r, 0);

    t1.join();
    ASSERT_EQ(nodeid1, nodeid2);
    ASSERT_EQ(st1.st_size, st2.st_size);
    ASSERT_EQ(st1.st_size, 4 << 20);

    fs_->forget(nodeid1, 2);
  }

  void verify_lookup_stale_inodes_while_req() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    fs_->options_.attr_timeout = 0;

    std::string file_name = "file";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat stbuf;

    // case 1: thread 1, lookup, sleep after OSS req finishes
    //         thread 2, destroy the inode
    int r = create_and_flush(parent, file_name.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
    std::thread t1([&]() {
      uint64_t new_nodeid;
      r = fs_->lookup(parent, file_name.c_str(), &new_nodeid, &stbuf);
      ASSERT_EQ(r, 0);

      ASSERT_NE(nodeid, new_nodeid);
      fs_->forget(new_nodeid, 1);
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    r = fs_->unlink(parent, file_name.c_str());
    ASSERT_EQ(r, 0);
    fs_->forget(nodeid, 1);

    t1.join();
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);

    // case 2: OSS req fails with error code not being ENOENT
    std::string file_name_1 = "file1";
    r = create_and_flush(parent, file_name_1.c_str(), CREATE_BASE_FLAGS, 0777,
                         0, 0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    uint64_t new_nodeid;
    r = fs_->lookup(parent, file_name_1.c_str(), &new_nodeid, &stbuf);
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    fs_->forget(nodeid, 1);

    // case 3: thread 1, lookup, sleep after OSS req finishes
    //         thread 2, unlink the inode and the parent to mark them stale
    std::string file_name_2 = "file2";
    r = create_and_flush(parent, file_name_2.c_str(), CREATE_BASE_FLAGS, 0777,
                         0, 0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
    std::thread t3([&]() {
      uint64_t new_nodeid;
      r = fs_->lookup(parent, file_name_2.c_str(), &new_nodeid, &stbuf);
      ASSERT_EQ(r, -ESTALE);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    r = fs_->unlink(parent, file_name_2.c_str());
    ASSERT_EQ(r, 0);
    fs_->forget(nodeid, 1);

    std::string parent_path = nodeid_to_path(parent);
    r = delete_dir(parent_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // mark parent inode as stale
    r = fs_->getattr(parent, &stbuf);
    ASSERT_EQ(r, -ENOENT);
    ASSERT_TRUE(fs_->global_inodes_map_.count(parent) > 0);
    ASSERT_TRUE(fs_->global_inodes_map_[parent]->is_stale);
    fs_->forget(parent, 1);
    t3.join();

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Lookup_Delay_After_Getting_Remote_attr);
  }

  void verify_lookup_getattr_update(bool test_lookup = true) {
    fs_->options_.attr_timeout = 1;

    auto test_dir_nodeid = get_test_dir_parent();
    DEFER(fs_->forget(test_dir_nodeid, 1));
    int r = 0;

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    auto parent_path = nodeid_to_path(test_dir_nodeid);
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid1, nodeid2, nodeid3;
    struct stat st;
    // 1. lookup to create a new inode, lookup_cnt == 1
    r = fs_->lookup(test_dir_nodeid, "file-internal", &nodeid1, &st);
    ASSERT_EQ(r, 0);

    // read the file
    void *handle = nullptr;
    bool unused;
    r = fs_->open(nodeid1, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto file = get_file_from_handle(handle);
    char *buf_1MB = new char[0x100000];
    DEFER(delete[] buf_1MB);

    std::ifstream rf(local_file);
    uint64_t crc64 = 0, local_crc64 = 0;
    uint64_t total_size = 3 * 1024 * 1024, offset = 0;
    size_t buf_size = IO_SIZE;  // read/write IO
    while (offset < total_size) {
      buf_size = std::min(buf_size, total_size - offset);
      rf.read(buf_1MB, buf_size);
      local_crc64 = cal_crc64(local_crc64, buf_1MB, buf_size);

      ssize_t read_size = file->pread(buf_1MB, buf_size, offset);
      ASSERT_EQ(read_size, static_cast<ssize_t>(buf_size));
      crc64 = cal_crc64(crc64, buf_1MB, buf_size);
      offset += buf_size;
    }

    ASSERT_EQ(crc64, local_crc64);
    r = fs_->release(nodeid1, file);
    ASSERT_EQ(r, 0);

    // 2. delete file on cloud and mark stale
    r = delete_file(join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (test_lookup) {
      // mark stale
      r = fs_->lookup(test_dir_nodeid, "file-internal", &nodeid1, &st);
      ASSERT_EQ(r, -ENOENT);
    } else {
      // mark stale
      r = fs_->getattr(nodeid1, &st);
      ASSERT_EQ(r, -ENOENT);
    }

    r = fs_->open(nodeid1, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, -ESTALE);

    // 3. upload again, to create a new inode nodeid2
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = fs_->lookup(test_dir_nodeid, "file-internal", &nodeid2, &st);
    ASSERT_EQ(r, 0);
    ASSERT_NE(nodeid1, nodeid2);

    // 4. upload a new file with different size, update attr
    std::this_thread::sleep_for(std::chrono::seconds(1));
    create_random_file(local_file, 4);
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    if (test_lookup) {
      r = fs_->lookup(test_dir_nodeid, "file-internal", &nodeid3, &st);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(nodeid3, nodeid2);
      ASSERT_EQ(st.st_size, 4 << 20);
    } else {
      r = fs_->getattr(nodeid2, &st);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(st.st_size, 4 << 20);
    }

    std::ifstream rf2(local_file);
    crc64 = 0;
    local_crc64 = 0;
    buf_size = IO_SIZE;

    r = fs_->open(nodeid2, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    file = get_file_from_handle(handle);
    while (offset < total_size) {
      buf_size = std::min(buf_size, total_size - offset);
      rf2.read(buf_1MB, buf_size);
      local_crc64 = cal_crc64(local_crc64, buf_1MB, buf_size);

      ssize_t read_size = file->pread(buf_1MB, buf_size, offset);
      ASSERT_EQ(read_size, static_cast<ssize_t>(buf_size));
      crc64 = cal_crc64(crc64, buf_1MB, buf_size);
      offset += buf_size;
    }
    ASSERT_EQ(crc64, local_crc64);
    r = fs_->release(nodeid2, file);

    if (test_lookup) {
      fs_->forget(nodeid2, 2);
    } else {
      fs_->forget(nodeid2, 1);
    }

    auto iter = fs_->global_inodes_map_.find(test_dir_nodeid);
    ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
    ASSERT_SIZE_EQ(static_cast<DirInode *>(iter->second)->children.size(), 0);

    // forget stale nodeid1
    fs_->forget(nodeid1, 1);
  }

  void verify_forget_no_parent() {
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    auto parent_path = nodeid_to_path(parent);
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // part 1: invalidate file inode with no parent
    uint64_t nodeid1, nodeid2;
    void *handle;
    // create inode1 for testdir/workdir/file-internal
    r = fs_->lookup(parent, "file-internal", &nodeid1, &st);
    ASSERT_EQ(r, 0);
    // mark inode1 as stale, parent->children["file-internal"] = inode1
    r = fs_->unlink(parent, "file-internal");
    ASSERT_EQ(r, 0);
    // create inode2 for testdir/workdir/file-internal again
    // parent->children["file-internal"] = inode2
    r = create_and_flush(parent, "file-internal", CREATE_BASE_FLAGS, 0777, 0, 0,
                         0, &nodeid2, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid2, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    // mark inode2 as stale
    r = fs_->unlink(parent, "file-internal");
    ASSERT_EQ(r, 0);
    // mark parent as stale
    r = fs_->rmdir(test_dir_nodeid, "workdir");
    ASSERT_EQ(r, 0);

    // destroy inode2 first
    fs_->forget(nodeid2, 1);
    ASSERT_FALSE(fs_->global_inodes_map_.count(nodeid2));
    // parent->children is empty, now destroy parent
    fs_->forget(parent, 1);
    ASSERT_FALSE(fs_->global_inodes_map_.count(parent));
    // destroy inode1 with parent being null
    fs_->forget(nodeid1, 1);
    ASSERT_FALSE(fs_->global_inodes_map_.count(nodeid1));

    // part 2: invalidate dir inode without parent
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);

    uint64_t dir_nodeid;
    r = fs_->mkdir(parent, "tempname", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);

    // create dir inode1 for testdir/workdir/tempname/file
    parent_path = nodeid_to_path(dir_nodeid);
    r = upload_file(local_file, join_paths(parent_path, "file"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->lookup(dir_nodeid, "file", &nodeid1, &st);
    ASSERT_EQ(r, 0);

    // unlink testdir/workdir/tempname/file
    r = fs_->unlink(dir_nodeid, "file");
    ASSERT_EQ(r, 0);

    // rmdir testdir/workdir/tempname
    r = fs_->rmdir(parent, "tempname");
    ASSERT_EQ(r, 0);

    // create file inode2 for testdir/workdir/tempname again
    // parent->children["tempname"] = inode2
    r = create_and_flush(parent, "tempname", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid2, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid2, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // mark inode2 as stale
    r = fs_->unlink(parent, "tempname");
    ASSERT_EQ(r, 0);
    // mark parent as stale
    r = fs_->rmdir(test_dir_nodeid, "workdir");
    ASSERT_EQ(r, 0);

    // destroy inode2 first
    fs_->forget(nodeid2, 1);
    ASSERT_FALSE(fs_->global_inodes_map_.count(nodeid2));
    // parent->children is empty, now destroy parent
    fs_->forget(parent, 1);
    ASSERT_FALSE(fs_->global_inodes_map_.count(parent));

    fs_->forget(dir_nodeid, 1);
    // children are not empty, invalidation should fail
    ASSERT_TRUE(fs_->global_inodes_map_.count(dir_nodeid));
    fs_->forget(nodeid1, 1);
    ASSERT_FALSE(fs_->global_inodes_map_.count(nodeid1));
    // recursive invalidation
    ASSERT_FALSE(fs_->global_inodes_map_.count(dir_nodeid));
  }

  void verify_forget_out_of_order() {
    const int rounds = 3;
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent, 1));

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    // create 5x5 files
    for (int i = 0; i < 5; ++i) {
      std::string dir_name = "file-" + std::to_string(i);
      uint64_t dir_nodeid;
      r = fs_->mkdir(parent, dir_name.c_str(), 0777, 0, 0, 0, &dir_nodeid, &st);
      ASSERT_EQ(r, 0);

      for (int j = 0; j < 5; ++j) {
        std::string file_name = "file-" + std::to_string(j);
        uint64_t file_nodeid;
        void *handle = nullptr;
        r = create_and_flush(dir_nodeid, file_name.c_str(), CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &file_nodeid, &st, &handle);
        ASSERT_EQ(r, 0);
        r = fs_->release(file_nodeid, get_file_from_handle(handle));
        ASSERT_EQ(r, 0);

        fs_->forget(file_nodeid, 1);
        ASSERT_TRUE(fs_->global_inodes_map_.find(file_nodeid) ==
                    fs_->global_inodes_map_.end());
      }
      fs_->forget(dir_nodeid, 1);
      ASSERT_TRUE(fs_->global_inodes_map_.find(dir_nodeid) ==
                  fs_->global_inodes_map_.end());
    }

    // readdir and forget active inodes
    for (int i = 0; i < rounds; ++i) {
      LOG_INFO("round ` **************************", i + 1);
      std::vector<TestInode> dir_childs;
      r = read_dir_without_dots(parent, dir_childs);
      ASSERT_EQ(r, 0);
      ASSERT_SIZE_EQ(dir_childs.size(), 5);

      std::vector<TestInode> all_childs;
      for (int j = 0; j < 5; ++j) {
        std::vector<TestInode> file_childs;
        r = read_dir_without_dots(dir_childs[j].nodeid, file_childs);
        ASSERT_EQ(r, 0);
        ASSERT_SIZE_EQ(file_childs.size(), 5);
        all_childs.insert(all_childs.end(), file_childs.begin(),
                          file_childs.end());
      }

      // forget non-empty dirs first
      LOG_INFO("start forgetting dirs");
      for (auto &dir : dir_childs) {
        fs_->forget(dir.nodeid, 1);
        ASSERT_TRUE(fs_->global_inodes_map_.find(dir.nodeid) !=
                    fs_->global_inodes_map_.end());
      }
      // forget files then.
      // recursively forget ancestors
      LOG_INFO("start forgetting files");
      for (auto &file : all_childs) {
        fs_->forget(file.nodeid, 1);
        ASSERT_TRUE(fs_->global_inodes_map_.find(file.nodeid) ==
                    fs_->global_inodes_map_.end());
      }

      for (auto &dir : dir_childs) {
        ASSERT_TRUE(fs_->global_inodes_map_.find(dir.nodeid) ==
                    fs_->global_inodes_map_.end());
      }
    }

    // readdir and forget stale inodes
    std::vector<TestInode> dir_childs;
    r = read_dir_without_dots(parent, dir_childs);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(dir_childs.size(), 5);

    std::vector<TestInode> all_childs;
    for (int j = 0; j < 5; ++j) {
      std::vector<TestInode> file_childs;
      r = read_dir_without_dots(dir_childs[j].nodeid, file_childs);
      ASSERT_EQ(r, 0);
      ASSERT_SIZE_EQ(file_childs.size(), 5);
      all_childs.insert(all_childs.end(), file_childs.begin(),
                        file_childs.end());

      for (auto &file : file_childs) {
        r = fs_->unlink(dir_childs[j].nodeid, file.name.c_str());
        ASSERT_EQ(r, 0);
      }

      r = fs_->rmdir(parent, dir_childs[j].name.c_str());
      ASSERT_EQ(r, 0);
    }

    // forget non-empty dirs first
    LOG_INFO("start forgetting dirs");
    for (auto &dir : dir_childs) {
      fs_->forget(dir.nodeid, 1);
      ASSERT_TRUE(fs_->global_inodes_map_.find(dir.nodeid) !=
                  fs_->global_inodes_map_.end());
    }
    // forget files then.
    // recursively forget ancestors
    LOG_INFO("start forgetting files");
    for (auto &file : all_childs) {
      fs_->forget(file.nodeid, 1);
      ASSERT_TRUE(fs_->global_inodes_map_.find(file.nodeid) ==
                  fs_->global_inodes_map_.end());
    }

    for (auto &dir : dir_childs) {
      ASSERT_TRUE(fs_->global_inodes_map_.find(dir.nodeid) ==
                  fs_->global_inodes_map_.end());
    }
  }

  void verify_forget_while_rename() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int r = 0;
    uint64_t nodeid1 = 0;
    void *handle1 = nullptr;
    struct stat st;
    r = create_and_flush(parent, "testfile1", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid1, &st, &handle1);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid1, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);

    uint64_t nodeid2 = 0;
    void *handle2 = nullptr;
    r = create_and_flush(parent, "testfile2", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid2, &st, &handle2);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid2, get_file_from_handle(handle2));
    ASSERT_EQ(r, 0);

    r = fs_->unlink(parent, "testfile1");
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(FaultInjectionId::FI_Forget_Delay);
    DEFER(g_fault_injector->clear_injection(FaultInjectionId::FI_Forget_Delay));

    std::thread forget_thread([&](uint64_t id) { fs_->forget(id, 1); },
                              nodeid1);

    r = fs_->rename(parent, "testfile2", parent, "testfile1", 0);
    ASSERT_EQ(r, 0);

    forget_thread.join();

    ASSERT_TRUE(fs_->global_inodes_map_.find(nodeid1) ==
                fs_->global_inodes_map_.end());
    ASSERT_TRUE(fs_->global_inodes_map_.find(nodeid2) !=
                fs_->global_inodes_map_.end());

    auto iter = fs_->global_inodes_map_.find(parent);
    ASSERT_TRUE(iter != fs_->global_inodes_map_.end());

    DirInode *dir_inode = static_cast<DirInode *>(iter->second);
    auto child_inode = dir_inode->find_child_node("testfile1");
    ASSERT_TRUE(child_inode);

    ASSERT_EQ(child_inode->nodeid, nodeid2);
    ASSERT_EQ(dir_inode->find_child_node("testfile2"), nullptr);

    fs_->forget(nodeid2, 1);
  }

  void verify_etag() {
    auto add_dquo = [](const std::string &s) { return "\"" + s + "\""; };

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    // 1. upload file and check inode etag
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    std::string filepath = "testdir/testfile";
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    {
      uint64_t nodeid = 0;
      uint64_t dir_nodeid = 0;
      r = fs_->lookup(parent, "testdir", &dir_nodeid, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(dir_nodeid, 1));

      auto meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
      r = fs_->lookup(dir_nodeid, "testfile", &nodeid, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      {
        std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
        ASSERT_TRUE(fs_->global_inodes_map_.find(nodeid) !=
                    fs_->global_inodes_map_.end());
        auto inode = static_cast<FileInode *>(fs_->global_inodes_map_[nodeid]);
        ASSERT_EQ(inode->etag, add_dquo(meta["Etag"]));
      }
    }

    // 2. write new file and check inode etag after upload
    for (int i = 0; i < 2; i++) {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      std::string filename = "newfile" + std::to_string(i);
      r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));
      FileInode *inode = nullptr;
      {
        std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
        ASSERT_TRUE(fs_->global_inodes_map_.find(nodeid) !=
                    fs_->global_inodes_map_.end());
        inode = static_cast<FileInode *>(fs_->global_inodes_map_[nodeid]);
        ASSERT_EQ(inode->etag, "");
      }

      if (i == 0) {
        // PutObject
        auto data = random_string(11);
        r = write_to_file_handle(handle, data.c_str(), data.size(), 0);
        ASSERT_EQ(r, static_cast<int64_t>(data.size()));
      } else {
        // MultipartUpload
        auto data = random_string(1048576);
        off_t offset = 0;
        for (int j = 0; j < 32; j++) {
          r = write_to_file_handle(handle, data.c_str(), data.size(), offset);
          ASSERT_EQ(r, static_cast<int64_t>(data.size()));
          offset += data.size();
        }
      }

      ASSERT_EQ(inode->etag, "");
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      // etag should be refresh
      r = fs_->getattr(nodeid, &st);
      ASSERT_EQ(r, 0);

      auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(inode->etag, add_dquo(meta["Etag"]));
    }

    // 3. test remote update
    for (int i = 0; i < 2; i++) {
      uint64_t nodeid = 0;
      std::string local_file2 = join_paths(test_path_, "local_file2");
      create_random_file(local_file, 3);
      create_random_file(local_file2, 3);
      uint64_t crc64_1 = 0, crc64_2 = 0;
      const ssize_t io_size = 1048576;
      {
        std::ifstream rf(local_file);
        for (int i = 0; i < 3; i++) {
          char *buf = new char[io_size];
          DEFER(delete[] buf);
          rf.read(buf, io_size);
          crc64_1 = cal_crc64(crc64_1, buf, io_size);
        }
      }
      {
        std::ifstream rf(local_file2);
        for (int i = 0; i < 3; i++) {
          char *buf = new char[io_size];
          DEFER(delete[] buf);
          rf.read(buf, io_size);
          crc64_2 = cal_crc64(crc64_2, buf, io_size);
        }
      }

      std::string filepath = "test_remote_update";
      int r = upload_file(local_file, join_paths(parent_path, filepath),
                          FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
      r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      // open it and mark invalidate_data_cache to false
      void *handle = nullptr;
      bool unused;
      r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
      ASSERT_EQ(r, 0);
      fs_->release(nodeid, get_file_from_handle(handle));

      FileInode *inode = nullptr;
      std::string old_etag;
      {
        std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
        ASSERT_TRUE(fs_->global_inodes_map_.find(nodeid) !=
                    fs_->global_inodes_map_.end());
        inode = static_cast<FileInode *>(fs_->global_inodes_map_[nodeid]);
        ASSERT_FALSE(inode->etag.empty());
        old_etag = inode->etag;
      }

      r = upload_file(local_file2, join_paths(parent_path, filepath),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
      if (i == 0) {
        // refresh by lookup after attrcache timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(3050));
        r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
        ASSERT_EQ(r, 0);
        fs_->forget(nodeid, 1);
      } else {
        // refresh by readdirplus
        std::vector<TestInode> dir_children;
        r = read_dir_without_dots(parent, dir_children);
        ASSERT_EQ(r, 0);
        for (auto child : dir_children) {
          fs_->forget(child.nodeid, 1);
        }
      }

      auto meta = get_file_meta("test_remote_update", FLAGS_oss_bucket_prefix);
      ASSERT_EQ(inode->etag, add_dquo(meta["Etag"]));
      ASSERT_NE(old_etag, inode->etag);
      ASSERT_TRUE(inode->invalidate_data_cache);
    }
  }

  void verify_remote_inode_type_change() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::string should_be_file = "should_be_file",
                should_be_dir = "should_be_dir";
    int r = upload_file(local_file, join_paths(parent_path, should_be_file),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = upload_file(local_file,
                    join_paths(parent_path, should_be_file + "/unused"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = upload_file(local_file, join_paths(parent_path, should_be_dir),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = upload_file(local_file,
                    join_paths(parent_path, should_be_dir + "/unused"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    std::vector<TestInode> old_childs;
    r = read_dir(parent, old_childs);
    ASSERT_SIZE_EQ(old_childs.size(), 6);  // duplicate nodes for should_be_file
                                           // and should_be_dir + ./..
    for (int i = 2; i < 6; i++) {
      LOG_INFO("nodeid: `, name: `", old_childs[i].nodeid, old_childs[i].name);
    }
    // we return duplicate nodes. so there will be duplicate forgets accordingly
    DEFER(fs_->forget(old_childs[3].nodeid, 1));
    DEFER(fs_->forget(old_childs[2].nodeid, 1));
    DEFER(fs_->forget(old_childs[4].nodeid, 1));
    DEFER(fs_->forget(old_childs[5].nodeid, 1));

    struct stat st;
    uint64_t file_nodeid, dir_nodeid;
    r = fs_->lookup(parent, should_be_file.c_str(), &file_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(file_nodeid, 1));
    ASSERT_TRUE(S_ISDIR(st.st_mode));
    std::vector<TestInode> file_childs;
    r = read_dir(file_nodeid, file_childs);
    ASSERT_SIZE_EQ(file_childs.size(), 3);  // one valid + ./..
    // file_childs[2] is forget later

    r = fs_->lookup(parent, should_be_dir.c_str(), &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));
    ASSERT_TRUE(S_ISDIR(st.st_mode));
    std::vector<TestInode> dir_childs;
    r = read_dir(dir_nodeid, dir_childs);
    ASSERT_SIZE_EQ(dir_childs.size(), 3);         // one valid + ./..
    DEFER(fs_->forget(dir_childs[2].nodeid, 1));  // the last inode is valid

    r = delete_file(join_paths(parent_path, should_be_file + "/unused"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = delete_file(join_paths(parent_path, should_be_dir),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    std::vector<TestInode> new_childs;
    r = read_dir(parent, new_childs);
    ASSERT_EQ(
        new_childs.size(),
        (size_t)4);  // unique nodes for should_be_file and should_be_dir + ./..
    DEFER(fs_->forget(new_childs[2].nodeid, 1));
    DEFER(fs_->forget(new_childs[3].nodeid, 1));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // attrtime

    bool file_is_as_expected = false, dir_is_as_expected = false;
    for (auto &it : new_childs) {
      LOG_INFO("nodeid: `, name: `", it.nodeid, it.name);
      struct stat st;
      if (it.name == should_be_file) {
        uint64_t new_nodeid = 0;

        // file_nodeid is not empty, so it could not be marked as stale
        r = fs_->getattr(it.nodeid, &st);
        if (r == -ENOENT) {
          r = fs_->lookup(parent, should_be_file.c_str(), &new_nodeid, &st);
          ASSERT_EQ(r, -ENOENT);
        }

        // now try to invalidate the inode under file_nodeid 'dir'
        r = fs_->forget(file_childs[2].nodeid,
                        1);  // decrease the lookup from readdir
        // r = fs_->lookup(parent, should_be_file.c_str(), &new_nodeid, &st);
        r = fs_->getattr(it.nodeid, &st);
        ASSERT_EQ(r,
                  -ENOENT);  // should_be_file dir is empty and be marked stale
        r = fs_->lookup(parent, should_be_file.c_str(), &new_nodeid, &st);
        ASSERT_EQ(r, 0);
        DEFER(fs_->forget(new_nodeid, 1));
        ASSERT_TRUE(S_ISREG(st.st_mode));
        file_is_as_expected = true;
      }

      if (it.name == should_be_dir) {
        r = fs_->getattr(it.nodeid, &st);
        ASSERT_EQ(r, 0);
        ASSERT_TRUE(S_ISDIR(st.st_mode));
        dir_is_as_expected = true;
      }
    }

    ASSERT_TRUE(file_is_as_expected && dir_is_as_expected);
  }

  void verify_getattr_stale_dir() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // mkdir, empty dir
    uint64_t dir_nodeid;
    struct stat st;
    int r = fs_->mkdir(parent, "workdir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    // rmdir on the cloud
    std::string dir_path = nodeid_to_path(dir_nodeid);
    r = delete_dir(dir_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // getattr marks the inode as stale
    r = fs_->getattr(dir_nodeid, &st);
    ASSERT_EQ(r, -ENOENT);

    // mkdir on the cloud again, lookup, and verify that we'll get a new nodeid
    r = create_dir(join_paths(dir_path, "testdir"), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t dir_nodeid_2, file_nodeid;
    void *handle;
    r = fs_->lookup(parent, "workdir", &dir_nodeid_2, &st);
    ASSERT_EQ(r, 0);
    ASSERT_NE(dir_nodeid, dir_nodeid_2);
    DEFER(fs_->forget(dir_nodeid_2, 1));

    // create file under dir, not empty dir
    r = fs_->creat(dir_nodeid_2, "file", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &file_nodeid, &st, &handle);
    ASSERT_EQ(r, 0);

    r = fs_->release(file_nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(file_nodeid, 1));

    // rmdir, deletefile on the cloud
    dir_path = nodeid_to_path(dir_nodeid_2);
    r = delete_dir(dir_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = fs_->getattr(file_nodeid, &st);
    ASSERT_EQ(r, -ENOENT);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    r = upload_file(local_file, join_paths(dir_path, "file"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t file_nodeid_2;
    r = fs_->lookup(dir_nodeid_2, "file", &file_nodeid_2, &st);
    ASSERT_EQ(r, 0);
    ASSERT_NE(file_nodeid, file_nodeid_2);

    r = delete_dir(dir_path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // Mark the descendants stale.
    r = fs_->getattr(dir_nodeid_2, &st);
    ASSERT_EQ(r, -ENOENT);
    r = fs_->getattr(file_nodeid_2, &st);
    ASSERT_EQ(r, -ESTALE);

    DEFER(fs_->forget(file_nodeid_2, 1));
  }

  void verify_evictable_inodes_collection() {
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 1);

    int depth = rand() % 3 + 1;
    int width = rand() % 4 + 1;
    int files = rand() % 5 + 1;
    ASSERT_EQ(
        0, upload_file_tree(depth, width, files, local_file, "testdir-", ""));

    // fetch all inodes
    uint64_t parent = get_test_dir_parent();
    std::vector<uint64_t> forget_list;

    std::vector<uint64_t> leaf_node_list;
    auto bfs = [&](uint64_t root, bool shuffle = false) {
      int r = 0;
      std::queue<uint64_t> q;
      q.push(root);
      while (!q.empty()) {
        uint64_t nodeid = q.front();
        q.pop();
        struct stat st;
        r = fs_->getattr(nodeid, &st);
        if (r < 0) return r;
        forget_list.push_back(nodeid);
        if (S_ISDIR(st.st_mode)) {
          std::vector<TestInode> dirents;
          r = read_dir_without_dots(nodeid, dirents);
          if (r < 0) return r;

          if (shuffle) {
            std::shuffle(dirents.begin(), dirents.end(),
                         std::mt19937(std::random_device()()));
          }

          for (auto &dirent : dirents) {
            q.push(dirent.nodeid);
          }

          if (dirents.size() == 0) {
            leaf_node_list.push_back(nodeid);
          }
        } else {
          leaf_node_list.push_back(nodeid);
        }
      }

      return r;
    };

    ASSERT_EQ(bfs(parent, true), 0);

    std::vector<uint64_t> evictable_inodes;
    uint64_t threshold = rand() % fs_->global_inodes_map_.size() + 1;

    LOG_INFO("inode count: `, threshold: `", fs_->global_inodes_map_.size(),
             threshold);
    int r = fs_->for_each_evictable_inodes(
        threshold, [&](const DentryView &dentry) {
          evictable_inodes.push_back(dentry.nodeid);
          return 0;
        });
    ASSERT_EQ(r, 0);

    // every evictable inode should be leaf node
    uint64_t empty_dir_or_file_cnt = 0;
    for (auto &nodeid : evictable_inodes) {
      LOG_INFO("file ` inode ` is evictable", nodeid_to_path(nodeid), nodeid);
      struct stat st;
      int r = fs_->getattr(nodeid, &st);
      ASSERT_EQ(r, 0);
      if (S_ISDIR(st.st_mode)) {
        DirInode *dir_node =
            static_cast<DirInode *>(fs_->global_inodes_map_[nodeid]);
        if (dir_node->children.size() == 0) {
          empty_dir_or_file_cnt++;
        }
      } else {
        empty_dir_or_file_cnt++;
      }
    }

    ASSERT_TRUE(empty_dir_or_file_cnt >=
                std::min(leaf_node_list.size(),
                         fs_->global_inodes_map_.size() - threshold));

    for (auto &nodeid : forget_list) {
      fs_->forget(nodeid, 1);
    }
  }

  void verify_oss_dir_check() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    uint64_t nodeid1 = 0, nodeid2 = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "local_empty_dir", 0777, 0, 0, 0, &nodeid1, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid1, 1));
    r = fs_->mkdir(parent, "local_non_empty_dir", 0777, 0, 0, 0, &nodeid2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));

    uint64_t file_nodeid = 0;
    create_file_in_folder(nodeid2, "file", 0, file_nodeid, 0);
    DEFER(fs_->forget(file_nodeid, 1));

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    std::string filepath = "remote_non_empty_dir/new_test_file";
    r = upload_file(local_file, join_paths(parent_path, filepath),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    uint64_t nodeid3 = 0;
    r = fs_->lookup(parent, "remote_non_empty_dir", &nodeid3, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid3, 1));

    bool is_dir_empty = false;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(
        fs_, oss_is_dir_empty, join_paths(parent_path, "non_exist_path"),
        is_dir_empty);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(is_dir_empty);

    is_dir_empty = false;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_is_dir_empty,
                                       nodeid_to_path(nodeid1), is_dir_empty);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(is_dir_empty);

    is_dir_empty = false;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_is_dir_empty,
                                       nodeid_to_path(nodeid2), is_dir_empty);
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(is_dir_empty);

    is_dir_empty = false;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_is_dir_empty,
                                       nodeid_to_path(nodeid3), is_dir_empty);
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(is_dir_empty);

    std::vector<uint64_t> nodeids = {nodeid1, nodeid2, nodeid3};
    for (size_t i = 0; i < nodeids.size(); i++) {
      struct stat st;
      std::string unused_etag;
      r = DO_SYNC_BACKGROUND_OSS_REQUEST(
          fs_, oss_stat, nodeid_to_path(nodeid1).c_str(), &st, &unused_etag);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(S_ISDIR(st.st_mode));
    }

    // add one rename case to a local empty but remote not empty dir.
    uint64_t nodeid4 = 0;
    r = fs_->mkdir(parent, "local_tmp_dir", 0777, 0, 0, 0, &nodeid4, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid4, 1));
    r = fs_->rename(parent, "local_tmp_dir", parent, "remote_non_empty_dir", 0);
    ASSERT_EQ(r, -ENOTEMPTY);
  }

  void verify_test_setattr() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    std::string filename = "testfile";
    uint64_t crc = create_file_in_folder(parent, filename, 10, nodeid, 17);
    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(crc), meta["X-Oss-Hash-Crc64ecma"]);
    DEFER(fs_->forget(nodeid, 1));

    struct timespec old_mtime;
    int r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);

    old_mtime = st.st_mtim;

    // make sure the mtime is changed
    photon::thread_usleep(1000000);

    struct timespec new_mtime;
    clock_gettime(CLOCK_REALTIME, &new_mtime);
    st.st_mtim = new_mtime;
    r = fs_->setattr(nodeid, &st, 1 << 5);  // set mtime
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    struct stat new_st;
    r = fs_->getattr(nodeid, &new_st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(new_st.st_mtim.tv_sec, new_mtime.tv_sec);
    ASSERT_EQ(new_st.st_mtim.tv_nsec, new_mtime.tv_nsec);

    // test set filesize
    new_st.st_size = 5;
    r = fs_->setattr(nodeid, &new_st, 1 << 3);  // set filesize
    ASSERT_EQ(r, -ENOTSUP);  // not allowed set filesize not to 0
    DEFER(fs_->forget(nodeid, 1));

    uint64_t nodeid1 = 0;
    void *handle1 = nullptr;
    struct stat st1;
    r = create_and_flush(parent, "testfile1", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid1, &st1, &handle1);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid1, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid1, 1));

    void *new_handle1 = nullptr;
    write_dirty_file(parent, "testfile1", 5, nodeid1, new_handle1,
                     29);  // make this file dirty
    r = fs_->setattr(nodeid1, &st1, 1 << 3);
    ASSERT_EQ(r, -EBUSY);
    r = fs_->release(nodeid1, get_file_from_handle(new_handle1));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid1, 1));

    // test set file size to 0
    uint64_t nodeid2 = 0;
    struct stat st2;
    crc = create_file_in_folder(parent, "testfile2", 13, nodeid2, 5);
    ASSERT_TRUE(crc > 0);

    r = fs_->getattr(nodeid2, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(st2.st_size > 0);

    st2.st_size = 0;
    r = fs_->setattr(nodeid2, &st2, 1 << 3);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));

    struct stat new_st2;
    r = fs_->getattr(nodeid2, &new_st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(new_st2.st_size, 0);

    // truncate dir
    uint64_t dir_nodeid = 0;
    r = fs_->mkdir(parent, "testdir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    st.st_size = 0;
    r = fs_->setattr(dir_nodeid, &st, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, -EISDIR);
  }

  void verify_truncate_with_oss_error(bool appendable = false) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    {
      // 1. truncate non-empty file
      std::string filename = "testfile";
      uint64_t crc = create_file_in_folder(parent, filename, 2, nodeid, 1);
      auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(meta["X-Oss-Hash-Crc64ecma"], std::to_string(crc));
      ASSERT_NE(meta["Content-Length"], "0");
      DEFER(fs_->forget(nodeid, 1));

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Failed_Without_Call);
      DEFER(g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Failed_Without_Call));

      st.st_size = 0;
      int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE);
      ASSERT_EQ(r, -EIO);

      bool unused;
      if (!appendable) {
        r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle, &unused);
        ASSERT_EQ(r, 0);
        r = fs_->release(nodeid, get_file_from_handle(handle));
        ASSERT_EQ(r, -EIO);
      } else {
        r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle, &unused);
        ASSERT_EQ(r, -EIO);
      }

      meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_NE(meta["Content-Length"], "0");
    }

    {
      // 2. truncate empty file
      std::string filename = "testfile2";
      create_file_in_folder(parent, filename, 0, nodeid, 0);
      auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(meta["Content-Length"], "0");
      DEFER(fs_->forget(nodeid, 1));

      st.st_size = 0;
      int r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE);
      ASSERT_EQ(r, 0);
      meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(meta["Content-Length"], "0");

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Failed_Without_Call);
      DEFER(g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Failed_Without_Call));

      r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE);
      ASSERT_EQ(r, 0);
      meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(meta["Content-Length"], "0");
    }
  }

  void test_metadata_mem_usage() {
    uint64_t file_num = FLAGS_test_mem_usage_file_num;
    LOG_INFO("sizeof(DirInode) = `, sizeof(FileInode) = `", sizeof(DirInode),
             sizeof(FileInode));

    LOG_INFO("Create 1 DirInode and ` sub- ` whose names are sized of 50.",
             file_num, FLAGS_test_mem_usage_dir ? "dirs" : "files");
    bool enable_lru = FLAGS_test_mem_usage_lru;
    LOG_INFO("Test LRU mem usage: `", enable_lru);

    DirInode *root = new DirInode(1, "/", {0, 0}, 0, nullptr);
    std::map<uint64_t, Inode *> glb_map;
    std::string len_of_50 =
        "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij";
    std::string etag = "7D3EBDBD61A735B60D8F930C57D7BB73";

    auto staged_cache =
        enable_lru ? std::make_unique<StagedInodeCache>(3600) : nullptr;

    for (uint64_t i = 0; i < file_num; ++i) {
      std::string index_str = std::to_string(i);
      std::string name = len_of_50.substr(0, 50 - index_str.size()) + index_str;

      Inode *inode = nullptr;

      if (FLAGS_test_mem_usage_dir) {
        inode = new DirInode(i + 2, name, {0, 0}, 1, root);
      } else {
        inode = new FileInode(i + 2, name, 0, {0, 0}, InodeType::kFile, false,
                              1, root, etag, false, 0);
      }

      root->add_child_node(inode);
      glb_map[i + 2] = inode;

      if (enable_lru) {
        staged_cache->insert(root->nodeid, name, 0, {0, 0}, etag, i + 2,
                             InodeType::kFile, 0);
        root->erase_child_node(name, i + 2);
        glb_map.erase(i + 2);
        delete inode;
      }
    }

    LOG_INFO("Sleeping for 5s ...");
    std::this_thread::sleep_for(std::chrono::seconds(5));
    LOG_INFO("Physical memory usage: ` KiB.", get_physical_memory_KiB());

    if (enable_lru)
      ASSERT_SIZE_EQ(glb_map.size(), 0);
    else
      ASSERT_SIZE_EQ(glb_map.size(), file_num);

    delete root;
    for (auto &it : glb_map) {
      delete it.second;
    }
  }

  void dir_inode_sanity_check() {
    DirInode *root = new DirInode(1, "/", {0, 0}, 0, nullptr);
    DEFER(delete root);

    DirInode *dir_inode = new DirInode(2, "testdir", {0, 0}, 0, root);
    ASSERT_TRUE(dir_inode->is_dir());
    ASSERT_TRUE(dir_inode->is_dir_empty());
    DEFER(delete dir_inode);

    std::vector<Inode *> children;
    for (int i = 0; i < 3; ++i) {
      FileInode *node = new FileInode(i + 3, "testfile_" + std::to_string(i), 0,
                                      {0, 0}, InodeType::kFile, false, 2,
                                      dir_inode, "ETAG", false, 0);
      ASSERT_TRUE(node->is_file());
      ASSERT_FALSE(node->is_dirty_file());
      children.push_back(node);
      ASSERT_FALSE(dir_inode->find_child_node(node->name));
      dir_inode->add_child_node(node);
      ASSERT_TRUE(dir_inode->find_child_node(node->name));
    }
    ASSERT_SIZE_EQ(children.size(), 3);

    for (auto node : children) {
      ASSERT_FALSE(dir_inode->is_dir_empty());
      node->is_stale = true;
    }

    ASSERT_TRUE(dir_inode->is_dir_empty());
    ASSERT_FALSE(dir_inode->is_children_empty());

    FileInode *node1 =
        new FileInode(6, "testfile_1", 0, {0, 0}, InodeType::kFile, false, 2,
                      dir_inode, "ETAG", false, 0);
    ASSERT_TRUE(node1->is_file());
    ASSERT_FALSE(node1->is_dirty_file());
    dir_inode->add_child_node(node1);  // cover a stale child node
    ASSERT_FALSE(dir_inode->is_dir_empty());
    ASSERT_FALSE(dir_inode->is_children_empty());

    for (auto node : children) {
      dir_inode->erase_child_node(node->name, node->nodeid);
      delete node;
    }
    ASSERT_SIZE_EQ(dir_inode->children.size(), 1);
    ASSERT_TRUE(dir_inode->find_child_node("testfile_1"));

    FileInode *node2 =
        new FileInode(7, "testfile_2", 0, {0, 0}, InodeType::kFile, false, 2,
                      dir_inode, "ETAG", false, 0);
    ASSERT_TRUE(node2->is_file());
    ASSERT_FALSE(node2->is_dirty_file());
    dir_inode->add_child_node_directly(node2);
    ASSERT_SIZE_EQ(dir_inode->children.size(), 2);
    ASSERT_TRUE(dir_inode->find_child_node("testfile_2"));

    dir_inode->erase_child_node("testfile_1", node1->nodeid);
    delete node1;
    dir_inode->erase_child_node("testfile_2", node2->nodeid);
    delete node2;
    ASSERT_TRUE(dir_inode->is_dir_empty());
    ASSERT_TRUE(dir_inode->is_children_empty());
  }
};

TEST_F(Ossfs2InodeTest, verify_lookup) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  init(opts);
  verify_lookup();
}

TEST_F(Ossfs2InodeTest, verify_lookup_stale_dirs_recursively) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.allow_mark_dir_stale_recursively = true;
  opts.allow_rename_dir = true;
  init(opts);
  verify_lookup_stale_dirs_recursively();
}

TEST_F(Ossfs2InodeTest, verify_lookup_simultaneously) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_lookup_simultaneously();
}

TEST_F(Ossfs2InodeTest, verify_lookup_stale_inodes_while_req) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_lookup_stale_inodes_while_req();
}

TEST_F(Ossfs2InodeTest, verify_stale_lookup) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_lookup_getattr_update();
}

TEST_F(Ossfs2InodeTest, verify_getattr_stale_dir) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  opts.allow_mark_dir_stale_recursively = true;
  init(opts);
  verify_getattr_stale_dir();
}

TEST_F(Ossfs2InodeTest, verify_stale_getattr) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_lookup_getattr_update(false);
}

TEST_F(Ossfs2InodeTest, verify_forget_no_parent) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_forget_no_parent();
}

TEST_F(Ossfs2InodeTest, verify_forget_out_of_order) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_forget_out_of_order();
}

TEST_F(Ossfs2InodeTest, verify_forget_while_rename) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_forget_while_rename();
}

TEST_F(Ossfs2InodeTest, verify_etag) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 3;
  init(opts);
  verify_etag();
}

TEST_F(Ossfs2InodeTest, verify_remote_inode_type_change) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  init(opts);
  verify_remote_inode_type_change();
}

TEST_F(Ossfs2InodeTest, verify_evictable_inodes_collection) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.inode_cache_eviction_interval_ms = 1000;
  opts.inode_cache_eviction_threshold = 10;
  init(opts);
  verify_evictable_inodes_collection();
}

TEST_F(Ossfs2InodeTest, verify_oss_dir_check) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_oss_dir_check();
}

TEST_F(Ossfs2InodeTest, verify_test_setattr) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_test_setattr();
}

TEST_F(Ossfs2InodeTest, verify_truncate_with_oss_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_truncate_with_oss_error();
}

TEST_F(Ossfs2InodeTest, verify_truncate_with_oss_error_for_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  init(opts);
  verify_truncate_with_oss_error(true);
}

TEST_F(Ossfs2InodeTest, DISABLED_test_metadata_mem_usage) {
  test_metadata_mem_usage();
}

TEST_F(Ossfs2InodeTest, dir_inode_sanity_check) {
  dir_inode_sanity_check();
}
