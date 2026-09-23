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

class Ossfs2HdfsFallocateFtruncateTest : public OssHdfsTestSuite {
 protected:
  // fallocate: extend file size.
  void verify_fallocate_extend() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "falloc_ext", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Write some initial data.
    const char *data = "hello";
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // fallocate to extend to 1MB.
    r = fs_->fallocate(nodeid, 0, 1024 * 1024, handle);
    ASSERT_EQ(r, 0);

    // Verify new size.
    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_size, (off_t)(1024 * 1024));

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // fallocate: existing content is preserved.
  void verify_fallocate_keep_content() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "falloc_keep", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const char *data = "preserved data";
    size_t data_len = strlen(data);
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(data, data_len, 0);
    ASSERT_EQ(w, (ssize_t)data_len);

    // Flush to commit data before fallocate.
    r = fs_->flush(nodeid, handle);
    ASSERT_EQ(r, 0);

    // Extend to 4KB.
    r = fs_->fallocate(nodeid, 0, 4096, handle);
    ASSERT_EQ(r, 0);

    // Read back original content.
    char buf[32] = {};
    ssize_t n = file->pread(buf, data_len, 0);
    ASSERT_EQ(n, (ssize_t)data_len);
    ASSERT_EQ(std::string(buf, n), "preserved data");

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate (via setattr + FUSE_SET_ATTR_SIZE): shrink file.
  void verify_ftruncate_shrink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_shrink", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Write 8KB of data.
    const size_t initial_size = 8192;
    char *buf = new char[initial_size];
    DEFER(delete[] buf);
    memset(buf, 'X', initial_size);
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(buf, initial_size, 0);
    ASSERT_EQ(w, (ssize_t)initial_size);

    // Truncate to 4KB via setattr.
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 4096;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    // Verify new size.
    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_size, (off_t)4096);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate: extend file (fills with zeros).
  void verify_ftruncate_extend() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_extend", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Write 1KB.
    const char *data = "start";
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // Truncate to 16KB.
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 16384;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_size, (off_t)16384);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate: truncate to zero.
  void verify_ftruncate_to_zero() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_zero", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Write some data.
    const char *data = "will be truncated";
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // Truncate to 0.
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 0;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_size, (off_t)0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate via setattr on non-zero size.
  void verify_ftruncate_via_setattr() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_setattr", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Write 16KB.
    const size_t write_size = 16384;
    char *buf = new char[write_size];
    DEFER(delete[] buf);
    memset(buf, 'A', write_size);
    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(buf, write_size, 0);
    ASSERT_EQ(w, (ssize_t)write_size);

    // Truncate to 2KB.
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 2048;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    struct stat st2;
    r = fs_->getattr(nodeid, &st2);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st2.st_size, (off_t)2048);

    // Verify the first 2KB still has the data.
    char verify[2048];
    ssize_t n = file->pread(verify, 2048, 0);
    ASSERT_EQ(n, (ssize_t)2048);
    ASSERT_EQ(verify[0], 'A');
    ASSERT_EQ(verify[2047], 'A');

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate shrink closes and reopens the handle. The reopen must not
  // replay O_TRUNC, otherwise the just-truncated file is deleted and
  // recreated empty while the inode still believes it holds the content.
  void verify_ftruncate_shrink_after_trunc_open() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_reopen", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t data_size = 8192;
    char *buf = new char[data_size];
    DEFER(delete[] buf);
    memset(buf, 'P', data_size);

    auto file = get_file_from_handle(handle);
    ASSERT_EQ(file->pwrite(buf, data_size, 0), (ssize_t)data_size);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen with open(2)-style O_TRUNC (no O_CREAT).
    bool keep_cache = false;
    void *trunc_handle = nullptr;
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &trunc_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto tfile = get_file_from_handle(trunc_handle);

    ASSERT_EQ(tfile->pwrite(buf, data_size, 0), (ssize_t)data_size);

    // Shrink to 4KB via setattr; internally close -> truncate -> reopen.
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 4096;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(trunc_handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    // The first 4KB must survive the shrink.
    char verify[4096];
    memset(verify, 0, sizeof(verify));
    ssize_t n = tfile->pread(verify, 4096, 0);
    ASSERT_EQ(n, (ssize_t)4096);
    ASSERT_EQ(verify[0], 'P');
    ASSERT_EQ(verify[4095], 'P');

    r = fs_->release(nodeid, tfile);
    ASSERT_EQ(r, 0);

    // The backend must hold the truncated content.
    void *rd_handle = nullptr;
    r = fs_->open(nodeid, O_RDONLY, &rd_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    memset(verify, 0, sizeof(verify));
    n = get_file_from_handle(rd_handle)->pread(verify, 4096, 0);
    ASSERT_EQ(n, (ssize_t)4096);
    ASSERT_EQ(verify[4095], 'P');
    r = fs_->release(nodeid, get_file_from_handle(rd_handle));
    ASSERT_EQ(r, 0);
  }
};

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_fallocate_extend) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fallocate_extend();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_fallocate_keep_content) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fallocate_keep_content();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_ftruncate_shrink) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_shrink();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_ftruncate_extend) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_extend();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_ftruncate_to_zero) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_to_zero();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_ftruncate_via_setattr) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_via_setattr();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_ftruncate_shrink_after_trunc_open) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_shrink_after_trunc_open();
}
