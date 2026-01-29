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

class Ossfs2FilenameTest : public Ossfs2TestSuite {
 protected:
  void verify_test_special_characters_in_url() {
    std::vector<std::string> filenames = {
        "x?让我们说中文&e<7>8中国語を話してください"};
    auto add_name = [&filenames](const std::string &name) {
      // make sure the name is not in filenames
      if (std::find(filenames.begin(), filenames.end(), name) !=
          filenames.end())
        return;

      filenames.emplace_back(name);
    };

    // Generate 10 random names with a length of 1-255 consisting of special
    // characters These characters should be at the beginning, end, and middle
    // of the string
    for (int i = 0; i < 10; i++) {
      int len = rand() % 253 + 1;
      std::string name = "";
      for (int j = 1; j <= len; j++) {
        int random_number = rand() % 127 + 1;
        char random_char = (char)random_number;

        // skip "/"
        if (random_char == '/') continue;
        name += random_char;
      }

      if (name.empty()) continue;
      add_name(name);
      add_name(name + "ab");
      add_name("ab" + name);
      add_name("a" + name + "b");
    }

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    // Check create files and directories and rename with these special
    // characters.
    for (auto &name : filenames) {
      uint64_t nodeid1 = 0, nodeid2 = 0, nodeid3 = 0, nodeid4 = 0;

      // test create file
      create_file_in_folder(parent, name, 0, nodeid1, 0);

      int r = fs_->unlink(parent, name.c_str());
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid1, 1));

      // test create directory
      r = fs_->mkdir(parent, name.c_str(), 0777, 0, 0, 0, &nodeid2, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid2, 1));

      r = fs_->rmdir(parent, name.c_str());
      ASSERT_EQ(r, 0);

      // test rename one file to another file with special characters
      std::string new_name = filenames[rand() % filenames.size()];
      if (new_name == name) continue;

      create_file_in_folder(parent, name, 0, nodeid3, 0);
      DEFER(fs_->forget(nodeid3, 1));

      r = fs_->rename(parent, name.c_str(), parent, new_name.c_str(), 0);
      ASSERT_EQ(r, 0);

      r = fs_->unlink(parent, new_name.c_str());
      ASSERT_EQ(r, 0);

      // test rename one directory to another directory with special characters
      r = fs_->mkdir(parent, name.c_str(), 0777, 0, 0, 0, &nodeid4, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid4, 1));

      r = fs_->rename(parent, name.c_str(), parent, new_name.c_str(), 0);
      ASSERT_EQ(r, 0);

      r = fs_->rmdir(parent, new_name.c_str());
      ASSERT_EQ(r, 0);
    }

    // have another try to move dir containing files with special characters
    // it will call delete multiple objects interface
    uint64_t dir_nodeid = 0;
    int r = fs_->mkdir(parent, "testdir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    std::map<std::string, uint64_t> nodeids;
    for (auto &name : filenames) {
      uint64_t nodeid = 0;
      create_file_in_folder(dir_nodeid, name, 0, nodeid, 0);
      nodeids[name] = nodeid;
    }

    auto parent_path = nodeid_to_path(parent);
    std::vector<std::string> list_results;
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_list_dir_descendants,
                                       join_paths(parent_path, "testdir"),
                                       list_results);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(list_results.size(), filenames.size());

    r = fs_->rename(parent, "testdir", parent, "testdir_new", 0);
    ASSERT_EQ(r, 0);

    list_results.clear();
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_list_dir_descendants,
                                       join_paths(parent_path, "testdir"),
                                       list_results);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(list_results.size(), 0);

    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_list_dir_descendants,
                                       join_paths(parent_path, "testdir_new"),
                                       list_results);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(list_results.size(), filenames.size());

    // list results are in lexicographical order
    int i = 0;
    for (auto &it : nodeids) {
      int r = fs_->unlink(dir_nodeid, it.first.c_str());
      ASSERT_EQ(r, 0);
      fs_->forget(it.second, 1);

      ASSERT_EQ(it.first, list_results[i++]);
    }
  }

  void verify_long_file_name() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::string invalid_name(kOssfsMaxFileNameLength + 1, 'a');
    int r = upload_file(local_file, join_paths(parent_path, invalid_name),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    std::string max_valid_name(kOssfsMaxFileNameLength, 'a');
    r = upload_file(local_file, join_paths(parent_path, max_valid_name),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    std::vector<TestInode> childs;
    r = read_dir(parent, childs);
    ASSERT_EQ(r, 0);

    // the invalid name should be filtered out
    ASSERT_SIZE_EQ(childs.size(), 3);  // max_valid_name/./..

    uint64_t nodeid = 0;
    void *handle = 0;
    struct stat st;
    r = fs_->lookup(parent, invalid_name.c_str(), &nodeid, &st);
    ASSERT_EQ(r, -ENAMETOOLONG);

    r = create_and_flush(parent, invalid_name.c_str(), CREATE_BASE_FLAGS, 0777,
                         0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, -ENAMETOOLONG);

    r = fs_->mkdir(parent, invalid_name.c_str(), 0777, 0, 0, 0, &nodeid, &st);
    ASSERT_EQ(r, -ENAMETOOLONG);

    r = fs_->rename(parent, max_valid_name.c_str(), parent,
                    invalid_name.c_str(), 0);
    ASSERT_EQ(r, -ENAMETOOLONG);

    uint64_t file_nodeid = 0, dir_nodeid = 0;
    void *file_handle = nullptr;
    std::string max_valid_name2(kOssfsMaxFileNameLength, 'b');
    std::string max_valid_name3(kOssfsMaxFileNameLength, 'c');
    std::string max_valid_name4(kOssfsMaxFileNameLength, 'd');
    r = fs_->lookup(parent, max_valid_name2.c_str(), &file_nodeid, &st);
    ASSERT_EQ(r, -ENOENT);

    r = create_and_flush(parent, max_valid_name2.c_str(), CREATE_BASE_FLAGS,
                         0777, 0, 0, 0, &file_nodeid, &st, &file_handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(file_nodeid, get_file_from_handle(file_handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(file_nodeid, 1));

    r = fs_->mkdir(parent, max_valid_name3.c_str(), 0777, 0, 0, 0, &dir_nodeid,
                   &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    r = fs_->rename(parent, max_valid_name.c_str(), parent,
                    max_valid_name4.c_str(), 0);
    ASSERT_EQ(r, 0);

    for (auto &it : childs) {
      LOG_INFO("nodeid: `, name: `", it.nodeid, it.name);
      if (it.name != "." && it.name != "..") fs_->forget(it.nodeid, 1);
    }

    childs.clear();
    r = read_dir(parent, childs);
    ASSERT_EQ(r, 0);

    ASSERT_SIZE_EQ(childs.size(), 5);
    for (auto &it : childs) {
      LOG_INFO("nodeid: `, name: `", it.nodeid, it.name);
      if (it.name != "." && it.name != "..") fs_->forget(it.nodeid, 1);
    }

    auto result = get_list_objects(parent_path, FLAGS_oss_bucket_prefix);
    ASSERT_SIZE_EQ(result.size(), 4);  // 5 + 1(invalid）- 2（./..）
  }

  void verify_exceeds_oss_length_limit() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // generate 256 length filename
    auto name = random_string(256);
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;

    int r = create_and_flush(parent, name.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, -ENAMETOOLONG);

    // generate a four-level directory, each level is 255 long
    // the total length should exceeds the oss limit of 1024
    std::vector<uint64_t> nodeids(5, 0);
    nodeids[0] = parent;

    // create 3 subdirs, all should be successful
    for (int i = 0; i < 3; i++) {
      auto subdir = random_string(255);

      r = fs_->mkdir(nodeids[i], subdir.c_str(), 0777, 0, 0, 0, &nodeids[i + 1],
                     &st);
      ASSERT_EQ(r, 0);
    }

    // the 4th subdir should fail
    auto subdir = random_string(255);
    r = fs_->mkdir(nodeids[3], subdir.c_str(), 0777, 0, 0, 0, &nodeids[4], &st);
    ASSERT_EQ(r, -EOPNOTSUPP);

    // create a file with 256 length filename in the 4th level dir
    uint64_t nodeid1 = 0;
    void *handle1 = nullptr;
    auto invalid_name = random_string(256);
    r = create_and_flush(nodeids[3], invalid_name.c_str(), CREATE_BASE_FLAGS,
                         0777, 0, 0, 0, &nodeid1, &st, &handle1);
    ASSERT_EQ(r, -ENAMETOOLONG);
    DEFER(fs_->forget(nodeid1, 1));

    // create a file with 255 length filename in the 4th level dir
    uint64_t nodeid2 = 0;
    void *handle2 = 0;
    auto valid_name = random_string(255);
    r = create_and_flush(nodeids[3], valid_name.c_str(), CREATE_BASE_FLAGS,
                         0777, 0, 0, 0, &nodeid2, &st, &handle2);
    ASSERT_EQ(r, -EOPNOTSUPP);
    DEFER(fs_->forget(nodeid2, 1));

    // create a file with short name in the 4th level dir
    uint64_t nodeid3 = 0;
    void *handle3 = nullptr;
    auto short_file = random_string(10);
    r = create_and_flush(nodeids[3], short_file.c_str(), CREATE_BASE_FLAGS,
                         0777, 0, 0, 0, &nodeid3, &st, &handle3);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid3, get_file_from_handle(handle3));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid3, 1));

    r = fs_->rename(nodeids[3], short_file.c_str(), nodeids[3],
                    valid_name.c_str(), 0);
    ASSERT_EQ(r, -EOPNOTSUPP);

    r = fs_->rename(nodeids[3], short_file.c_str(), nodeids[3],
                    invalid_name.c_str(), 0);
    ASSERT_EQ(r, -ENAMETOOLONG);

    // create a dir with short name in the 4th level dir
    uint64_t dir_nodeid1 = 0;
    auto short_dir1 = random_string(11);
    r = fs_->mkdir(nodeids[3], short_dir1.c_str(), 0777, 0, 0, 0, &dir_nodeid1,
                   &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid1, 1));

    auto short_dir2 = random_string(12);
    r = fs_->rename(nodeids[3], short_dir1.c_str(), nodeids[3],
                    short_dir2.c_str(), 0);
    ASSERT_EQ(r, 0);
    auto result = get_list_objects(short_dir1, FLAGS_oss_bucket_prefix,
                                   true);  //  check old dir exists
    ASSERT_SIZE_EQ(result.size(), 0);

    auto valid_dir = random_string(255);
    r = fs_->rename(nodeids[3], short_dir2.c_str(), nodeids[3],
                    valid_dir.c_str(), 0);
    ASSERT_EQ(r, -EOPNOTSUPP);

    auto invalid_dir = random_string(256);
    r = fs_->rename(nodeids[3], valid_dir.c_str(), nodeids[3],
                    invalid_dir.c_str(), 0);
    ASSERT_EQ(r, -ENAMETOOLONG);

    for (int i = 1; i < 5; i++) {
      DEFER(fs_->forget(nodeids[i], 1));
    }
  }

  void verify_remote_obj_with_multiple_slashes(std::string &filepath) {
    LOG_INFO("verify_remote_obj_with_multiple_slashes, filepath `", filepath);
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t dir_nodeid;
    r = fs_->mkdir(parent, "tempdir", 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    uint64_t nodeid = 0;
    r = fs_->lookup(parent, "a", &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);

    std::vector<TestInode> childs;
    r = read_dir(parent, childs);
    ASSERT_EQ(r, 0);
    bool found = false;
    for (auto &child : childs) {
      if (child.name == "a") {
        found = true;
      }

      fs_->forget(child.nodeid, 1);
    }
    ASSERT_TRUE(found);

    childs.clear();
    r = read_dir_without_dots(nodeid, childs);
    ASSERT_EQ(r, 0);
    ASSERT_SIZE_EQ(childs.size(), 0);

    r = fs_->rename(parent, "tempdir", parent, "a", 0);
    ASSERT_EQ(r, -ENOTEMPTY);

    r = delete_file(join_paths(parent_path, filepath), FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
  }
};

TEST_F(Ossfs2FilenameTest, verify_test_special_characters_in_url) {
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_test_special_characters_in_url();
}

TEST_F(Ossfs2FilenameTest, verify_long_file_name) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_long_file_name();
}

TEST_F(Ossfs2FilenameTest, verify_exceeds_oss_length_limit) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_exceeds_oss_length_limit();
}

TEST_F(Ossfs2FilenameTest, verify_remote_obj_with_multiple_slashes) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  std::string filepath = "a///b";
  if (time(0) & 1) {
    filepath = "a//b";
  }
  verify_remote_obj_with_multiple_slashes(filepath);
}
