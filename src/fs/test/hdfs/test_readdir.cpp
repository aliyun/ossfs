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

class Ossfs2HdfsReaddirTest : public OssHdfsTestSuite {
 protected:
  void verify_readdirplus_atime(bool keep_existing_inode) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    struct stat st = {};
    ASSERT_EQ(
        fs_->mknod(parent, "atime_file", S_IFREG | 0644, 0, 0, &nodeid, &st),
        0);
    bool creation_ref_held = true;
    DEFER(if (creation_ref_held) fs_->forget(nodeid, 1));

    const time_t initial_atime = 1500000000;
    const time_t mtime = 1550000000;
    st = {};
    st.st_atim.tv_sec = initial_atime;
    st.st_mtim.tv_sec = mtime;
    ASSERT_EQ(
        fs_->setattr(nodeid, &st, FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME),
        0);
    ASSERT_EQ(st.st_atim.tv_sec, initial_atime);
    ASSERT_EQ(st.st_mtim.tv_sec, mtime);

    auto path = join_paths(nodeid_to_path(parent), "atime_file");
    const time_t expected_atime =
        keep_existing_inode ? 1600000000 : initial_atime;
    if (keep_existing_inode) {
      // Change only the backend so listing must refresh the cached atime.
      ASSERT_EQ(PERFORM_BACKGROUND_OBJ_REQUEST(
                    fs_, set_times, path, static_cast<int64_t>(mtime) * 1000,
                    static_cast<int64_t>(expected_atime) * 1000),
                0);
      ASSERT_EQ(fs_->getattr(nodeid, &st), 0);
      ASSERT_EQ(st.st_atim.tv_sec, initial_atime);
    } else {
      ASSERT_EQ(fs_->forget(nodeid, 1), 0);
      creation_ref_held = false;
      ASSERT_EQ(fs_->getattr(nodeid, &st), -ESTALE);
    }

    struct stat backend_st = {};
    std::string etag;
    ASSERT_EQ(
        PERFORM_BACKGROUND_OBJ_REQUEST(fs_, stat, path, &backend_st, &etag), 0);
    ASSERT_EQ(backend_st.st_atim.tv_sec, expected_atime);
    ASSERT_EQ(backend_st.st_mtim.tv_sec, mtime);

    struct fuse_file_info fi = {};
    ASSERT_EQ(fs_->opendir(parent, &fi), 0);
    void *dirp = reinterpret_cast<void *>(fi.fh);
    DEFER(fs_->releasedir(parent, dirp));

    std::vector<struct stat> entries;
    DEFER(for (const auto &entry : entries) fs_->forget(entry.st_ino, 1));
    auto filler = [](void *ctx, uint64_t, const char *name,
                     const struct stat *stbuf, off_t) -> int {
      if (std::string_view(name) == "." || std::string_view(name) == "..") {
        return 0;
      }
      static_cast<std::vector<struct stat> *>(ctx)->push_back(*stbuf);
      return 0;
    };
    ASSERT_EQ(
        fs_->readdir(parent, 0, dirp, filler, &entries, nullptr, true, nullptr),
        0);
    ASSERT_EQ(entries.size(), size_t{1});
    const auto &entry = entries.front();
    if (keep_existing_inode) {
      ASSERT_EQ(entry.st_ino, nodeid);
    } else {
      ASSERT_NE(entry.st_ino, nodeid);
    }
    ASSERT_EQ(entry.st_atim.tv_sec, expected_atime);
    ASSERT_EQ(entry.st_atim.tv_nsec, 0);
    ASSERT_EQ(entry.st_mtim.tv_sec, mtime);
    ASSERT_EQ(entry.st_mtim.tv_nsec, 0);

    g_fault_injector->set_injection(FI_OssError_Failed_Without_Call);
    DEFER(g_fault_injector->clear_injection(FI_OssError_Failed_Without_Call));
    ASSERT_EQ(fs_->getattr(entry.st_ino, &st), 0);
    ASSERT_EQ(st.st_atim.tv_sec, expected_atime);
    ASSERT_EQ(st.st_atim.tv_nsec, 0);
    ASSERT_EQ(st.st_mtim.tv_sec, mtime);

    uint64_t lookup_nodeid = 0;
    ASSERT_EQ(fs_->lookup(parent, "atime_file", &lookup_nodeid, &st), 0);
    DEFER(fs_->forget(lookup_nodeid, 1));
    ASSERT_EQ(lookup_nodeid, entry.st_ino);
    ASSERT_EQ(st.st_atim.tv_sec, expected_atime);
    ASSERT_EQ(st.st_atim.tv_nsec, 0);
    ASSERT_EQ(st.st_mtim.tv_sec, mtime);
  }

  // readdir error path.
  void verify_readdir_call_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create a subdirectory.
    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir", 0755, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    g_fault_injector->set_injection(FI_OssError_Call_Failed);
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));

    std::vector<TestInode> children;
    r = read_dir(dir_nodeid, children);
    ASSERT_EQ(r, -EIO);
  }

  // readdir error path (without call).
  void verify_readdir_failed_without_call() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create a subdirectory.
    uint64_t dir_nodeid = 0;
    struct stat st;
    int r = fs_->mkdir(parent, "test_dir2", 0755, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    g_fault_injector->set_injection(FI_OssError_Failed_Without_Call);
    DEFER(g_fault_injector->clear_injection(FI_OssError_Failed_Without_Call));

    std::vector<TestInode> children;
    r = read_dir(dir_nodeid, children);
    ASSERT_EQ(r, -EIO);
  }
};

TEST_F(Ossfs2HdfsReaddirTest, verify_readdirplus_new_inode_atime) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 3600;
  opts.max_inode_cache_count = 0;
  init(opts, 100, "", true);
  verify_readdirplus_atime(false);
}

TEST_F(Ossfs2HdfsReaddirTest, verify_readdirplus_existing_inode_atime) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 3600;
  opts.max_inode_cache_count = 0;
  init(opts, 100, "", true);
  verify_readdirplus_atime(true);
}

TEST_F(Ossfs2HdfsReaddirTest, verify_readdir_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdir_call_fail();
}

TEST_F(Ossfs2HdfsReaddirTest, verify_readdir_failed_without_call) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readdir_failed_without_call();
}
