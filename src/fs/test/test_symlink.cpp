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

#include <filesystem>

#include "test_suite.h"

DEFINE_bool(get_object_meta_has_type_field, false,
            "will be removed after GetObjectMeta is fixed");

#define CHECK_GET_OBJECT_META()                                              \
  if (!FLAGS_get_object_meta_has_type_field) {                               \
    LOG_INFO("Skip test because get object meta does not have type field."); \
    return;                                                                  \
  }

class Ossfs2SymlinkTest : public Ossfs2TestSuite {
 protected:
  void verify_oss_symlink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);

    auto background_env =
        bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next();
    auto run_test = [&]() {
      auto oss_client = background_env.oss_client;

      auto do_verify = [&](const std::string &src, const std::string &target) {
        std::filesystem::path src_path(src.substr(1));
        std::filesystem::path target_path(target);
        auto normalized_target =
            (src_path.parent_path() / target_path).lexically_normal();

        LOG_INFO("` -> ` expected_target: `", src, target,
                 normalized_target.string());

        ssize_t r = oss_client->oss_put_symlink(src, target);
        ASSERT_TRUE(r > 0);

        std::string oss_target;
        r = oss_client->oss_get_symlink(src, oss_target);
        ASSERT_EQ(r, 0);

        std::filesystem::path oss_target_path(oss_target);
        auto normalized_oss_target =
            (src_path.parent_path() / oss_target_path).lexically_normal();

        ASSERT_EQ(normalized_target, normalized_oss_target);

        std::string ossutil_oss_target;
        r = read_oss_symlink(src, ossutil_oss_target, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);

        if (FLAGS_oss_bucket_prefix.empty()) {
          ASSERT_EQ(normalized_oss_target.string(), ossutil_oss_target);
        } else {
          ASSERT_EQ(
              FLAGS_oss_bucket_prefix + "/" + normalized_oss_target.string(),
              ossutil_oss_target);
        }
      };

      std::vector<std::string> targets = {
          "test_dir/test_file",      "../test_file",
          "..//test_file",           "test_dir/../test_file",
          "test_dir/test_file/..",   "./test_file",
          "./test_dir/../test_file", "test_dir/./test_file",
          "test_dir//test_file",     "test_dir/./../test_file",
          "../test_dir/./test_file", "../test_dir/../test_file",
          "./../test_file",          ".././test_file",
          ".././test_dir/"};

      for (auto &target : targets) {
        do_verify(join_paths(parent_path, "test_symlink"), target);
      }

      do_verify(join_paths(parent_path, "deep/nested/test_symlink"),
                "../../../test_file");
      do_verify(join_paths(parent_path, "deep/nested/test_symlink"),
                "../../test_dir/test_file");
      do_verify(join_paths(parent_path, "nested/test_symlink"),
                "../test_dir/subdir/file");

      // invalid cases
      ASSERT_EQ(oss_client->oss_put_symlink(
                    join_paths(parent_path, "test_symlink"), "/a/b/c"),
                -EINVAL);
      ASSERT_EQ(
          oss_client->oss_put_symlink(join_paths(parent_path, "test_symlink"),
                                      "../../../../../a"),
          -EINVAL);
    };

    background_env.executor->perform(run_test);
  }

  void verify_symlink_and_readlink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string link_prefix = relative_path_to_root(parent_path);

    std::vector<std::string> targets = {
        "test_file",    "test_dir/test_file",    "test_dir/",
        "../test_file", "../test_dir/test_file", "../test_dir/"};

    int r = 0;
    uint64_t nodeid = 0;
    struct stat stbuf;

    for (size_t i = 0; i < targets.size(); i++) {
      std::string symlink_path = "symlink_to_" + std::to_string(i);
      std::string target = targets[i];

      r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));

      DEFER(fs_->forget(nodeid, 1));

      char link_target[4097];
      memset(link_target, 0, sizeof(link_target));
      r = fs_->readlink(nodeid, link_target, 4096);
      ASSERT_TRUE(r > 0);

      std::filesystem::path target_path(
          join_paths(parent_path.substr(1), target));
      ASSERT_EQ(std::string_view(link_target),
                link_prefix + target_path.lexically_normal().string());
    }

    // invalid cases
    // case 1: The symlink target escapes the current mount point.
    {
      std::string symlink_path = "invalid_symlink";
      std::string target = "../../a";
      r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
      ASSERT_EQ(r, -EINVAL);

      target = "../../../../a";
      r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
      ASSERT_EQ(r, -EINVAL);
    }

    // case 2: The symlink target is absolute path.
    {
      std::string symlink_path = "invalid_symlink";
      std::string target = "/a";
      r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
      ASSERT_EQ(r, -EINVAL);
    }

    // case 3: The symlink target is too lang.
    {
      std::string symlink_path = "invalid_symlink";
      std::string target = std::string(1023, 'a');
      r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
      ASSERT_EQ(r, -EINVAL);

      target = std::string(1024 - FLAGS_oss_bucket_prefix.size() - 1, 'a');
      r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
      ASSERT_EQ(r, -EINVAL);
    }

    // case 4: The oss symlink target is invalid.
    std::vector<std::string> invalid_targets = {"../test_file"};
    if (!FLAGS_oss_bucket_prefix.empty()) {
      invalid_targets.push_back(FLAGS_oss_bucket_prefix + "/../test_file");
    }

    for (auto &target : invalid_targets) {
      std::string symlink_path = "invalid_symlink";
      r = create_oss_symlink(symlink_path, target, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->lookup(parent, symlink_path, &nodeid, &stbuf);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));
      ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));

      char link_target[4097];
      memset(link_target, 0, sizeof(link_target));
      r = fs_->readlink(nodeid, link_target, 4096);
      ASSERT_EQ(r, -EINVAL);
    }

    // case 5: The oss symlink target escapes the current bucket prefix.
    if (!FLAGS_oss_bucket_prefix.empty()) {
      std::string symlink_path = "invalid_symlink";
      r = create_oss_symlink(symlink_path, "test_file",
                             FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      r = fs_->lookup(parent, symlink_path, &nodeid, &stbuf);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));
      ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));

      char link_target[4097];
      memset(link_target, 0, sizeof(link_target));
      r = fs_->readlink(nodeid, link_target, 4096);
      ASSERT_EQ(r, -EINVAL);
    }

    // case 6: The oss symlink end with '/', we always treat it as a directory.
    {
      std::string symlink_path = "symlink_dir/";
      r = create_oss_symlink(symlink_path, "test_file",
                             FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
      r = fs_->lookup(parent, symlink_path, &nodeid, &stbuf);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));
      ASSERT_TRUE(S_ISDIR(stbuf.st_mode));
    }
  }

  void verify_readdirplus() {
    verify_symlink_meta(true);
  }

  void verify_lookup_and_getattr() {
    verify_symlink_meta();
  }

  void verify_disable_symlink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string symlink_path = "unsupport_symlink";
    std::string target = "test_file";

    int r = 0;
    uint64_t nodeid = 0;
    struct stat stbuf;

    r = fs_->symlink(parent, symlink_path, target, 0, 0, &nodeid, &stbuf);
    ASSERT_EQ(r, -ENOTSUP);

    char link_target[4097];
    memset(link_target, 0, sizeof(link_target));
    r = fs_->readlink(nodeid, link_target, 4096);
    ASSERT_EQ(r, -ENOTSUP);

    verify_readdirplus();
    verify_lookup_and_getattr();
  }

  void verify_inode_type_change() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int r = 0;
    auto parent_path = nodeid_to_path(parent);

    std::vector<InodeType> target_types = {InodeType::kFile, InodeType::kDir};

    // round 0: lookup
    // round 1: readdirplus
    // round 2: getattr
    for (int round = 0; round < 3; round++) {
      for (auto &target_type : target_types) {
        // create symlink and lookup it
        std::string file_name = "test_symlink";
        r = create_oss_symlink(join_paths(parent_path, file_name), "test_file",
                               FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);

        uint64_t nodeid = 0;
        struct stat stbuf;

        auto do_lookup = [&](bool fist_lookup = false) {
          switch (round) {
            case 0: {
              if (!fist_lookup) {
                ASSERT_NE(nodeid, 0ULL);
                r = fs_->lookup(parent, file_name, &nodeid, &stbuf);
                ASSERT_NE(r, 0);
                fs_->forget(nodeid, 1);
              }
              r = fs_->lookup(parent, file_name, &nodeid, &stbuf);
              break;
            }
            case 1: {
              uint64_t old_nodeid = nodeid;
              r = lookup_by_readdirplus(parent, file_name, nodeid);
              fs_->forget(old_nodeid, 1);
              break;
            }
            case 2: {
              if (!fist_lookup) {
                ASSERT_NE(nodeid, 0ULL);
                r = fs_->getattr(nodeid, &stbuf);
                ASSERT_NE(r, 0);
                fs_->forget(nodeid, 1);
              }
              r = fs_->lookup(parent, file_name, &nodeid, &stbuf);
            }
          }
          ASSERT_EQ(r, 0);
        };

        do_lookup(true);
        ASSERT_NE(nodeid, 0ULL);

        r = fs_->getattr(nodeid, &stbuf);
        ASSERT_EQ(r, 0);
        ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));

        // remote delete symlink and upload target_type object
        r = delete_file(join_paths(parent_path, file_name),
                        FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);

        if (target_type == InodeType::kFile) {
          std::ofstream tmpfile("tmpfile", std::ios::out | std::ios::trunc);
          DEFER(unlink("tmpfile"));
          r = upload_file("tmpfile", join_paths(parent_path, file_name),
                          FLAGS_oss_bucket_prefix);
          ASSERT_EQ(r, 0);
        } else {
          r = create_dir(join_paths(parent_path, file_name),
                         FLAGS_oss_bucket_prefix);
          ASSERT_EQ(r, 0);
        }

        std::this_thread::sleep_for(
            std::chrono::seconds(fs_->options_.attr_timeout));

        do_lookup();
        ASSERT_NE(nodeid, 0ULL);

        r = fs_->getattr(nodeid, &stbuf);
        ASSERT_EQ(r, 0);

        if (target_type == InodeType::kFile) {
          ASSERT_TRUE(S_ISREG(stbuf.st_mode));
        } else {
          ASSERT_TRUE(S_ISDIR(stbuf.st_mode));
        }

        // remote delete it and create symlink again
        if (target_type == InodeType::kFile) {
          r = delete_file(join_paths(parent_path, file_name),
                          FLAGS_oss_bucket_prefix);
        } else {
          r = delete_dir(join_paths(parent_path, file_name),
                         FLAGS_oss_bucket_prefix);
        }
        ASSERT_EQ(r, 0);

        r = create_oss_symlink(join_paths(parent_path, file_name), "test_file",
                               FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);

        std::this_thread::sleep_for(
            std::chrono::seconds(fs_->options_.attr_timeout));

        do_lookup();
        ASSERT_NE(nodeid, 0ULL);

        r = fs_->getattr(nodeid, &stbuf);
        ASSERT_EQ(r, 0);
        ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));

        fs_->forget(nodeid, 1);
      }
    }
  }

 private:
  void verify_symlink_meta(bool with_readdirplus = false) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);

    std::vector<std::string> targets = {"test_file", "test_dir/test_file",
                                        "test_dir/"};

    targets.push_back(parent_path + "/test_file");
    targets.push_back(parent_path + "/test_dir/test_file");
    targets.push_back(parent_path + "/test_dir/");

    for (std::string target : targets) {
      if (!FLAGS_oss_bucket_prefix.empty()) {
        target = join_paths(FLAGS_oss_bucket_prefix, target);
      }

      const std::string file_name = "test_symlink";
      int r = create_oss_symlink(file_name, target, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      uint64_t nodeid = 0;
      struct stat stbuf;
      DEFER({
        if (nodeid != 0) fs_->forget(nodeid, 1);
      });

      if (with_readdirplus) {
        r = lookup_by_readdirplus(parent, file_name, nodeid);
      } else {
        r = fs_->lookup(parent, file_name, &nodeid, &stbuf);
      }
      ASSERT_EQ(r, 0);

      ASSERT_NE(nodeid, 0ULL);
      r = fs_->getattr(nodeid, &stbuf);
      ASSERT_EQ(r, 0);

      if (fs_->options_.enable_symlink) {
        ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));

        char link_target[4097];
        memset(link_target, 0, sizeof(link_target));
        r = fs_->readlink(nodeid, link_target, 4096);
        ASSERT_EQ(r, stbuf.st_size);
      } else {
        ASSERT_TRUE(S_ISREG(stbuf.st_mode));
        ASSERT_EQ(size_t(stbuf.st_size), target.size());
      }

      std::this_thread::sleep_for(
          std::chrono::seconds(fs_->options_.attr_timeout));
      r = fs_->getattr(nodeid, &stbuf);
      ASSERT_EQ(r, 0);

      if (fs_->options_.enable_symlink) {
        ASSERT_EQ(stbuf.st_mode, mode_t(0777 | S_IFLNK));
      } else {
        ASSERT_TRUE(S_ISREG(stbuf.st_mode));
        ASSERT_EQ(size_t(stbuf.st_size), target.size());
      }
    }
  }

  int lookup_by_readdirplus(uint64_t parent, std::string_view name,
                            uint64_t &nodeid) {
    nodeid = 0;
    std::vector<TestInode> children;
    void *dh = nullptr;
    int r = fs_->opendir(parent, &dh);
    if (r != 0) return r;

    r = fs_->readdir(parent, 0, dh, filler, &children, nullptr, true, nullptr);
    if (r != 0) return r;
    if (children.size() != 3) return -EIO;

    for (size_t i = 0; i < children.size(); i++) {
      if (children[i].name == name) {
        nodeid = children[i].nodeid;
        break;
      }
    }

    r = fs_->releasedir(parent, dh);
    if (r != 0) return r;

    if (nodeid == 0) return -ENOENT;
    return 0;
  }

  static std::string relative_path_to_root(std::string_view abs_parent) {
    if (abs_parent.empty()) return "";
    int depth = std::count(abs_parent.begin(), abs_parent.end(), '/');
    std::string res;
    for (int i = 0; i < depth; i++) {
      res += "../";
    }

    return res;
  }
};

TEST_F(Ossfs2SymlinkTest, verify_oss_symlink) {
  INIT_PHOTON();

  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_oss_symlink();
}

TEST_F(Ossfs2SymlinkTest, verify_symlink_and_readlink) {
  CHECK_GET_OBJECT_META();

  INIT_PHOTON();

  OssFsOptions opts;
  opts.enable_symlink = true;
  init(opts);
  verify_symlink_and_readlink();
}

TEST_F(Ossfs2SymlinkTest, verify_readdirplus) {
  CHECK_GET_OBJECT_META();

  INIT_PHOTON();

  OssFsOptions opts;
  opts.attr_timeout = 2;
  opts.enable_symlink = true;
  init(opts);
  verify_readdirplus();
}

TEST_F(Ossfs2SymlinkTest, verify_lookup_and_getattr) {
  CHECK_GET_OBJECT_META();

  INIT_PHOTON();

  OssFsOptions opts;
  opts.attr_timeout = 2;
  opts.enable_symlink = true;
  init(opts);
  verify_lookup_and_getattr();
}

TEST_F(Ossfs2SymlinkTest, DISABLED_verify_etag) {
  // TODO(hongren.lhr): fix this test
}

TEST_F(Ossfs2SymlinkTest, verify_disable_symlink) {
  INIT_PHOTON();

  OssFsOptions opts;
  opts.attr_timeout = 2;
  init(opts);
  verify_disable_symlink();
}

TEST_F(Ossfs2SymlinkTest, verify_inode_type_change) {
  CHECK_GET_OBJECT_META();

  OssFsOptions opts;
  opts.enable_symlink = true;
  opts.attr_timeout = 1;
  init(opts);
  verify_inode_type_change();
}
