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

#include "fs/test/test_suite.h"

// Unlink/rename-over of an opened file hides it (rename to ".fuse_hiddenXXX")
// instead of deleting it, so open handles keep working until the last release
// removes the hidden object.
class Ossfs2HdfsUnlinkHiddenTest : public OssHdfsTestSuite {
 protected:
  std::vector<std::string> hidden_children(uint64_t parent) {
    auto parent_path = hdfs_helper_->full_uri(nodeid_to_path(parent));
    std::vector<std::string> children;
    EXPECT_EQ(hdfs_helper_->list_dir(parent_path, children), 0);
    std::vector<std::string> hidden;
    for (auto &c : children) {
      if (c.rfind(".fuse_hidden", 0) == 0) hidden.push_back(c);
    }
    return hidden;
  }

  void verify_unlink_open_file_readable() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    create_file_in_folder(parent, "uh_rd_src", 4, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    bool keep_cache = false;
    void *handle = nullptr;
    ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &handle, &keep_cache), 0);
    auto *reader = get_file_from_handle(handle);

    // Unlink while open: the file is hidden, not deleted.
    ASSERT_EQ(fs_->unlink(parent, "uh_rd_src"), 0);
    auto *inode = static_cast<FileInode *>(reader->get_inode());
    ASSERT_TRUE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);

    uint64_t gone_id = 0;
    struct stat st;
    ASSERT_EQ(fs_->lookup(parent, "uh_rd_src", &gone_id, &st), -ENOENT);
    auto hidden = hidden_children(parent);
    ASSERT_EQ(hidden.size(), size_t(1));

    // The open reader must serve the whole content from the hidden object.
    std::ifstream lf(join_paths(test_path_, "uh_rd_src"), std::ios::binary);
    ASSERT_TRUE(lf.good());
    const size_t kChunk = 1 << 20;
    std::vector<char> buf(kChunk);
    std::vector<char> expected(kChunk);
    for (uint64_t off = 0; off < 4; off++) {
      ASSERT_EQ(reader->pread(buf.data(), kChunk, off * kChunk),
                (ssize_t)kChunk)
          << "chunk " << off;
      lf.read(expected.data(), kChunk);
      ASSERT_EQ(memcmp(buf.data(), expected.data(), kChunk), 0)
          << "chunk " << off;
    }
    ASSERT_EQ(reader->pread(buf.data(), kChunk, 4 * kChunk), 0);

    // The last release deletes the hidden object.
    ASSERT_EQ(fs_->release(nodeid, reader), 0);
    ASSERT_TRUE(inode->is_stale);
    ASSERT_TRUE(hidden_children(parent).empty());
  }

  void verify_unlink_open_file_writable() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    void *handle = nullptr;

    uint64_t nodeid = 0;
    int r = create_and_flush(parent, "uh_wr_src", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    handle = nullptr;
    bool keep_cache = false;
    ASSERT_EQ(fs_->open(nodeid, O_RDWR, &handle, &keep_cache), 0);
    auto *file = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, file));

    const size_t kChunk = 1 << 20;
    std::vector<char> data(kChunk);
    for (size_t i = 0; i < kChunk; i++) data[i] = (char)(i * 7 + 13);
    ASSERT_EQ(write_to_file_handle(handle, data.data(), kChunk, 0),
              (ssize_t)kChunk);

    // Unlink while open: the file is hidden, writes keep landing.
    ASSERT_EQ(fs_->unlink(parent, "uh_wr_src"), 0);
    auto *inode = static_cast<FileInode *>(file->get_inode());
    ASSERT_TRUE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);

    ASSERT_EQ(write_to_file_handle(handle, data.data(), kChunk, kChunk),
              (ssize_t)kChunk);
    ASSERT_EQ(fsync_file_handle(handle, true), 0);

    // The hidden object must hold all written data.
    auto hidden = hidden_children(parent);
    ASSERT_EQ(hidden.size(), size_t(1));
    auto parent_path = hdfs_helper_->full_uri(nodeid_to_path(parent));
    int64_t size = 0;
    ASSERT_EQ(
        hdfs_helper_->stat_file_size(join_paths(parent_path, hidden[0]), size),
        0);
    ASSERT_EQ(size, (int64_t)(2 * kChunk));
  }

  // rename(src, open dst) must hide the dst instead of dropping it: the open
  // dst handle keeps serving the old content until released.
  void verify_rename_over_open_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t dst_id = 0;
    create_file_in_folder(parent, "rov_dst", 1, dst_id);
    DEFER(fs_->forget(dst_id, 1));

    uint64_t src_id = 0;
    create_file_in_folder(parent, "rov_src", 2, src_id);
    DEFER(fs_->forget(src_id, 1));

    bool keep_cache = false;
    void *dst_handle = nullptr;
    ASSERT_EQ(fs_->open(dst_id, O_RDONLY, &dst_handle, &keep_cache), 0);
    auto *dst_reader = get_file_from_handle(dst_handle);

    ASSERT_EQ(fs_->rename(parent, "rov_src", parent, "rov_dst", 0), 0);
    auto *dst_inode = static_cast<FileInode *>(dst_reader->get_inode());
    ASSERT_TRUE(dst_inode->is_hidden);
    ASSERT_FALSE(dst_inode->is_stale);
    auto hidden = hidden_children(parent);
    ASSERT_EQ(hidden.size(), size_t(1));

    const size_t kChunk = 1 << 20;
    std::vector<char> buf(kChunk);
    std::vector<char> expected(kChunk);

    // The open dst handle reads the old dst content (1MB) then hits EOF.
    std::ifstream lf_dst(join_paths(test_path_, "rov_dst"), std::ios::binary);
    ASSERT_TRUE(lf_dst.good());
    ASSERT_EQ(dst_reader->pread(buf.data(), kChunk, 0), (ssize_t)kChunk);
    lf_dst.read(expected.data(), kChunk);
    ASSERT_EQ(memcmp(buf.data(), expected.data(), kChunk), 0);
    ASSERT_EQ(dst_reader->pread(buf.data(), kChunk, kChunk), 0);

    // The dst name now serves the src content (2MB).
    struct stat st;
    uint64_t new_dst_id = 0;
    ASSERT_EQ(fs_->lookup(parent, "rov_dst", &new_dst_id, &st), 0);
    DEFER(fs_->forget(new_dst_id, 1));
    ASSERT_NE(new_dst_id, dst_id);
    void *new_handle = nullptr;
    ASSERT_EQ(fs_->open(new_dst_id, O_RDONLY, &new_handle, &keep_cache), 0);
    auto *new_reader = get_file_from_handle(new_handle);

    std::ifstream lf_src(join_paths(test_path_, "rov_src"), std::ios::binary);
    ASSERT_TRUE(lf_src.good());
    for (uint64_t off = 0; off < 2; off++) {
      ASSERT_EQ(new_reader->pread(buf.data(), kChunk, off * kChunk),
                (ssize_t)kChunk);
      lf_src.read(expected.data(), kChunk);
      ASSERT_EQ(memcmp(buf.data(), expected.data(), kChunk), 0);
    }
    ASSERT_EQ(fs_->release(new_dst_id, new_reader), 0);

    // Releasing the last handle of the hidden dst deletes the hidden object.
    ASSERT_EQ(fs_->release(dst_id, dst_reader), 0);
    ASSERT_TRUE(dst_inode->is_stale);
    ASSERT_TRUE(hidden_children(parent).empty());

    ASSERT_EQ(fs_->unlink(parent, "rov_dst"), 0);
  }
};

TEST_F(Ossfs2HdfsUnlinkHiddenTest, verify_unlink_open_file_readable) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_unlink_open_file_readable();
}

TEST_F(Ossfs2HdfsUnlinkHiddenTest, verify_unlink_open_file_writable) {
  INIT_PHOTON();
  OssFsOptions opts;
  FLAGS_write_with_fuse_bufvec = false;
  init(opts);
  verify_unlink_open_file_writable();
}

TEST_F(Ossfs2HdfsUnlinkHiddenTest, verify_rename_over_open_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_rename_over_open_dst();
}
