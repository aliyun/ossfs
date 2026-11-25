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

  // Size accounting must use the high-water mark (max written end), not the
  // seek cursor; a shrink is judged against the true end-of-data.
  void verify_random_write_shrink_uses_highwater() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "rw_highwater", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);

    // Fill [0,108) contiguously, then overwrite at 10 retreats the cursor to
    // 18 while the true end-of-data stays at 108.
    char big[108];
    memset(big, 'H', sizeof(big));
    ASSERT_EQ(file->pwrite(big, sizeof(big), 0), (ssize_t)sizeof(big));

    char lo[8];
    memset(lo, 'L', sizeof(lo));
    ASSERT_EQ(file->pwrite(lo, sizeof(lo), 10), (ssize_t)sizeof(lo));

    // Flush commits the writes; cursor at 18, true end at 108.
    ASSERT_EQ(file->fdatasync(), 0);

    // Shrink to 60: real size 108 > 60, so this must truncate. A cursor-based
    // current_size would misread it as a no-op extend.
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 60;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    // Release (close) commits the stream so the length is reader-visible.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen read-only: a real shrink drops the high write, so offset 100 is
    // EOF; a misclassified no-op extend would leave that byte.
    bool keep_cache = false;
    void *rd_handle = nullptr;
    ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &rd_handle, &keep_cache), 0);
    auto rfile = get_file_from_handle(rd_handle);

    struct stat st2;
    ASSERT_EQ(fs_->getattr(nodeid, &st2), 0);
    EXPECT_EQ(st2.st_size, (off_t)60)
        << "shrink below the true high-water was misclassified as an extend";

    // The retained low write must still be readable.
    char verify[8];
    memset(verify, 0, sizeof(verify));
    ssize_t n = rfile->pread(verify, sizeof(verify), 10);
    ASSERT_EQ(n, (ssize_t)sizeof(verify));
    EXPECT_EQ(verify[0], 'L');

    ASSERT_EQ(fs_->release(nodeid, rfile), 0);
  }

  // A fallocate the handle treats as a no-op (new_end within the buffered
  // high-water) must not bump the cached attr.size in the fs layer.
  void verify_fallocate_noop_does_not_change_size() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "falloc_noop", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const size_t n0 = 1000;
    char *buf = new char[n0];
    DEFER(delete[] buf);
    memset(buf, 'x', n0);
    ssize_t w = file->pwrite(buf, n0, 0);  // buffered: keeps attr.size stale
    ASSERT_EQ(w, (ssize_t)n0);

    struct stat before;
    ASSERT_EQ(fs_->getattr(nodeid, &before), 0);
    // Pin the precondition: the write stays uncommitted (dirty), so getattr
    // returns the cached attr.size (0) rather than the backend size.
    ASSERT_EQ(before.st_size, (off_t)0)
        << "buffered write must still be uncommitted for this case";

    // new_end (600) <= buffered high-water: the handle does nothing.
    r = fs_->fallocate(nodeid, 500, 100, handle);
    ASSERT_EQ(r, 0);

    struct stat after;
    ASSERT_EQ(fs_->getattr(nodeid, &after), 0);
    ASSERT_EQ(after.st_size, before.st_size)
        << "no-op fallocate must not mutate the cached size";

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // POSIX fallocate(2) with length 0 is a no-op and must not extend the file.
  void verify_fallocate_zero_length_is_noop() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "falloc_zero", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    struct stat before;
    ASSERT_EQ(fs_->getattr(nodeid, &before), 0);

    r = fs_->fallocate(nodeid, 10, 0, handle);
    ASSERT_EQ(r, 0);

    struct stat after;
    ASSERT_EQ(fs_->getattr(nodeid, &after), 0);
    ASSERT_EQ(after.st_size, before.st_size)
        << "fallocate with length 0 must not extend the file";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // Size straight from the backend, so a stale cached attr cannot mask it.
  // -1 on failure keeps a stat error from looking like an empty file.
  off_t backend_file_size(const std::string &path) {
    struct stat backend_st = {};
    std::string etag;
    if (PERFORM_BACKGROUND_OBJ_REQUEST(fs_, stat, path, &backend_st, &etag) !=
        0) {
      ADD_FAILURE() << "backend stat failed: " << path;
      return -1;
    }
    return backend_st.st_size;
  }

  // Resize the backend object directly, simulating an external client.
  void resize_backend_file(const std::string &path, off_t target_size) {
    off_t cur = backend_file_size(path);
    if (target_size > cur) {
      RawObjHandle *raw = nullptr;
      ASSERT_EQ(PERFORM_BACKGROUND_OBJ_REQUEST(fs_, open_object, path, O_WRONLY,
                                               (mode_t)0777, &raw),
                0);
      DEFER({
        if (raw) {
          raw->close(path);
          delete raw;
        }
      });
      ASSERT_EQ(raw->fallocate(path, cur, target_size - cur), 0);
      ASSERT_EQ(raw->close(path), 0);
    } else if (target_size < cur) {
      ASSERT_EQ(
          PERFORM_BACKGROUND_OBJ_REQUEST(fs_, truncate_object, path,
                                         static_cast<size_t>(target_size)),
          0);
    }
  }

  // Build the stale-attr precondition: a clean inode caching cached_size
  // while the backend already holds backend_size. Caller owns the handle.
  void setup_stale_attr_file(const char *name, off_t cached_size,
                             off_t backend_size, uint64_t *parent,
                             uint64_t *nodeid, void **handle,
                             std::string *path) {
    *parent = get_test_dir_parent();
    struct stat st;
    void *h = nullptr;
    ASSERT_EQ(create_and_flush(*parent, name, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                               nodeid, &st, &h),
              0);

    char *buf = new char[cached_size];
    DEFER(delete[] buf);
    memset(buf, 'S', cached_size);
    ASSERT_EQ(get_file_from_handle(h)->pwrite(buf, cached_size, 0),
              (ssize_t)cached_size);

    // A dirty inode reports its attrs as always valid, so leave dirty first.
    ASSERT_EQ(fs_->release(*nodeid, get_file_from_handle(h)), 0);

    *path = join_paths(nodeid_to_path(*parent), name);
    resize_backend_file(*path, backend_size);

    // Reopen with no lookup/getattr in between, so the cached size stays.
    bool keep_cache = false;
    ASSERT_EQ(fs_->open(*nodeid, O_RDWR, handle, &keep_cache), 0);

    // Pin the premise itself: if open() ever starts refreshing attrs, fail
    // here instead of letting the caller pass for the wrong reason.
    auto *inode =
        static_cast<FileInode *>(get_file_from_handle(*handle)->get_inode());
    ASSERT_EQ(inode->attr.size, static_cast<uint64_t>(cached_size))
        << "cached size is not stale";
    ASSERT_FALSE(inode->is_dirty) << "the premise inode must be clean";
  }

  // Truncating to the stale cached size must still shrink the backend;
  // before the fix both branches were skipped and this was a silent no-op.
  void verify_ftruncate_to_stale_cached_size_shrinks_backend() {
    const off_t cached_size = 4096;
    const off_t backend_size = 1024 * 1024;
    uint64_t parent = 0, nodeid = 0;
    void *handle = nullptr;
    std::string path;
    setup_stale_attr_file("trunc_stale_noop", cached_size, backend_size,
                          &parent, &nodeid, &handle, &path);
    DEFER(fs_->forget(parent, 1));
    DEFER(if (nodeid) fs_->forget(nodeid, 1));
    DEFER(if (handle) fs_->release(nodeid, get_file_from_handle(handle)));
    ASSERT_FALSE(testing::Test::HasFatalFailure())
        << "setup precondition failed";
    ASSERT_EQ(backend_file_size(path), backend_size);

    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = cached_size;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    ASSERT_EQ(fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0),
              0);

    ASSERT_EQ(backend_file_size(path), cached_size)
        << "ftruncate reported success but the backend was not truncated";
  }

  // A target between the stale cached size and the real size must shrink,
  // not take the extend branch and fallocate into existing data.
  void verify_ftruncate_extend_misjudge_shrinks_backend() {
    const off_t cached_size = 4096;
    const off_t backend_size = 1024 * 1024;
    const off_t target_size = 8192;
    uint64_t parent = 0, nodeid = 0;
    void *handle = nullptr;
    std::string path;
    setup_stale_attr_file("trunc_stale_extend", cached_size, backend_size,
                          &parent, &nodeid, &handle, &path);
    DEFER(fs_->forget(parent, 1));
    DEFER(if (nodeid) fs_->forget(nodeid, 1));
    DEFER(if (handle) fs_->release(nodeid, get_file_from_handle(handle)));
    ASSERT_FALSE(testing::Test::HasFatalFailure())
        << "setup precondition failed";
    ASSERT_EQ(backend_file_size(path), backend_size);

    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = target_size;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    ASSERT_EQ(fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0), 0)
        << "a valid shrink was misjudged as an extend and failed";

    ASSERT_EQ(backend_file_size(path), target_size);
  }

  // A required extension is skipped when the cached size is stale and larger
  // than the real one, leaving the file silently short of the requested end.
  void verify_fallocate_extends_after_external_shrink() {
    const off_t cached_size = 65536;
    const off_t backend_size = 1024;
    const off_t new_end = 8192;
    uint64_t parent = 0, nodeid = 0;
    void *handle = nullptr;
    std::string path;
    setup_stale_attr_file("falloc_stale_large", cached_size, backend_size,
                          &parent, &nodeid, &handle, &path);
    DEFER(fs_->forget(parent, 1));
    DEFER(if (nodeid) fs_->forget(nodeid, 1));
    DEFER(if (handle) fs_->release(nodeid, get_file_from_handle(handle)));
    ASSERT_FALSE(testing::Test::HasFatalFailure())
        << "setup precondition failed";
    ASSERT_EQ(backend_file_size(path), backend_size);

    // new_end sits inside the stale cached size but beyond the real one.
    ASSERT_EQ(
        fs_->fallocate(nodeid, backend_size, new_end - backend_size, handle),
        0);

    ASSERT_EQ(backend_file_size(path), new_end)
        << "the extension the real size still needed was skipped";
  }

  // A dirty inode owns its size, so the refresh must stay off even with
  // attr_timeout = 0: adopting a foreign size would turn this no-op truncate
  // into a shrink that closes the in-flight writer and truncates underneath it.
  void verify_dirty_inode_skips_backend_refresh() {
    const off_t cached_size = 4096;
    const off_t backend_size = 1024 * 1024;
    uint64_t parent = 0, nodeid = 0;
    void *handle = nullptr;
    std::string path;
    setup_stale_attr_file("trunc_stale_dirty", cached_size, backend_size,
                          &parent, &nodeid, &handle, &path);
    DEFER(fs_->forget(parent, 1));
    DEFER(if (nodeid) fs_->forget(nodeid, 1));
    DEFER(if (handle) fs_->release(nodeid, get_file_from_handle(handle)));
    ASSERT_FALSE(testing::Test::HasFatalFailure())
        << "setup precondition failed";
    ASSERT_EQ(backend_file_size(path), backend_size);

    // One byte at the cached end turns the inode dirty while leaving the
    // handle's high-water mark exactly at the stale cached size.
    auto *file = get_file_from_handle(handle);
    ASSERT_EQ(file->pwrite("D", 1, cached_size - 1), (ssize_t)1);

    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = cached_size;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    ASSERT_EQ(fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0),
              0);

    ASSERT_EQ(backend_file_size(path), backend_size)
        << "a dirty inode was refreshed and the backend truncated under it";
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

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_random_write_shrink_uses_highwater) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_random_write_shrink_uses_highwater();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_fallocate_noop_does_not_change_size) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fallocate_noop_does_not_change_size();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest, verify_fallocate_zero_length_is_noop) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fallocate_zero_length_is_noop();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_ftruncate_to_stale_cached_size_shrinks_backend) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;       // decisions must see the backend value
  opts.close_to_open = false;  // otherwise open() erases the stale premise
  init(opts);
  verify_ftruncate_to_stale_cached_size_shrinks_backend();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_ftruncate_extend_misjudge_shrinks_backend) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  opts.close_to_open = false;
  init(opts);
  verify_ftruncate_extend_misjudge_shrinks_backend();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_fallocate_extends_after_external_shrink) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  opts.close_to_open = false;
  init(opts);
  verify_fallocate_extends_after_external_shrink();
}

TEST_F(Ossfs2HdfsFallocateFtruncateTest,
       verify_dirty_inode_skips_backend_refresh) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 0;
  opts.close_to_open = false;
  init(opts);
  verify_dirty_inode_skips_backend_refresh();
}
