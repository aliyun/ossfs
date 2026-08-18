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

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <thread>

#include "fs/file_writer.h"
#include "fs/random_write_context.h"
#include "test_suite.h"

DEFINE_string(write_file_cache_dir, "/tmp/ossfs2_rw_staging",
              "write file cache dir");

class Ossfs2RandomWriteTest : public OssOnlyTestSuite {
 protected:
  void SetUp() override {
    OssOnlyTestSuite::SetUp();
    std::system(("mkdir -p " + FLAGS_write_file_cache_dir).c_str());
  }

  uint64_t crc_of(const std::string &data) {
    return cal_crc64(0, (void *)data.data(), data.size());
  }

  // The ".fuse_hiddenXXX" name hide_inode generates for nodeid at seq
  // (same format as libfuse).
  std::string hidden_name_of(uint64_t nodeid, uint32_t seq) {
    char buf[64];
    snprintf(buf, sizeof(buf), ".fuse_hidden%08x%08x%08x",
             (unsigned int)(nodeid >> 32), (unsigned int)(nodeid & 0xffffffff),
             seq);
    return buf;
  }

  // No ".fuse_hiddenXXX" object may be left behind in the test prefix.
  void expect_no_hidden_objects() {
    auto objects =
        get_list_objects(get_test_osspath(""), FLAGS_oss_bucket_prefix);
    for (const auto &obj : objects) {
      EXPECT_TRUE(obj.find(".fuse_hidden") == std::string::npos)
          << "leftover hidden object: " << obj;
    }
  }

  // Write a small local file used as the source of upload_file decoys.
  std::string make_local_decoy(const std::string &name) {
    std::string path = join_paths(test_path_, name);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    EXPECT_GE(fd, 0);
    if (fd >= 0) {
      EXPECT_EQ(::write(fd, "d", 1), 1);
      ::close(fd);
    }
    return path;
  }

  // Open a fresh local mirror file at test_path_/<name>.mirror for RW.
  // Removes any leftover from a previous run.
  int open_fresh_mirror(const std::string &name) {
    std::string path = test_path_ + name + ".mirror";
    ::unlink(path.c_str());
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0600);
    EXPECT_GE(fd, 0) << "open mirror " << path << " errno=" << errno;
    return fd;
  }

  // Open an existing local file (e.g. the one previously used as the
  // pre-populated remote source) for RW so subsequent pwrites mirror to it.
  int reopen_mirror(const std::string &path) {
    int fd = ::open(path.c_str(), O_RDWR);
    EXPECT_GE(fd, 0) << "reopen mirror " << path << " errno=" << errno;
    return fd;
  }

  // Mirror-write to both the FS handle and the local fd. Returns whatever
  // write_to_file_handle returned. On positive return, the same number of
  // bytes is written to the mirror; mismatch fails the test.
  ssize_t pwrite_mirror(void *fh, int local_fd, const char *buf, size_t len,
                        off_t off) {
    ssize_t w = write_to_file_handle(fh, buf, len, off);
    if (w > 0) {
      size_t total = 0;
      while (total < static_cast<size_t>(w)) {
        ssize_t r = ::pwrite(local_fd, buf + total,
                             static_cast<size_t>(w) - total, off + total);
        if (r < 0 && errno == EINTR) continue;
        EXPECT_GT(r, 0) << "local pwrite errno=" << errno;
        if (r <= 0) return w;
        total += static_cast<size_t>(r);
      }
    }
    return w;
  }

  // Truncate the mirror to match an O_TRUNC / setattr-truncate on the FS.
  void truncate_mirror(int local_fd, off_t size) {
    EXPECT_EQ(::ftruncate(local_fd, size), 0)
        << "ftruncate mirror errno=" << errno;
  }

  // Compare the OSS object's CRC64 metadata against the local mirror's
  // current on-disk content.
  void assert_remote_matches_local(const std::string &filename, int local_fd) {
    struct stat st;
    ASSERT_EQ(::fstat(local_fd, &st), 0) << "fstat mirror";
    size_t size = static_cast<size_t>(st.st_size);

    std::string content(size, '\0');
    size_t total = 0;
    while (total < size) {
      ssize_t r = ::pread(local_fd, &content[total], size - total, total);
      if (r < 0 && errno == EINTR) continue;
      ASSERT_GT(r, 0) << "pread mirror at " << total;
      total += static_cast<size_t>(r);
    }

    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(size), meta["Content-Length"]) << filename;
    ASSERT_EQ(std::to_string(crc_of(content)), meta["X-Oss-Hash-Crc64ecma"])
        << filename;
  }

  // Write `size` bytes of repeating random 1 MiB blocks through both the FS
  // handle and the mirror fd.
  void write_repeating_data(void *handle, int local_fd, uint64_t size) {
    const size_t kBufSize = 1024 * 1024;
    std::string buf = random_string(kBufSize);
    for (uint64_t off = 0; off < size; off += kBufSize) {
      auto w = pwrite_mirror(handle, local_fd, buf.data(), kBufSize,
                             static_cast<off_t>(off));
      ASSERT_EQ(w, static_cast<ssize_t>(kBufSize));
    }
  }

  void verify_empty_create_flush() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_empty";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_flush_put() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_short_put";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kFileSize = 128 * 1024;  // < upload_buffer_size → flush_put
    const size_t kSlice = 4 * 1024;       // 32 slices total
    std::string buf = random_string(kFileSize);

    std::vector<size_t> offsets;
    for (size_t off = 0; off < kFileSize; off += kSlice) offsets.push_back(off);
    std::shuffle(offsets.begin(), offsets.end(),
                 std::mt19937(static_cast<unsigned>(nodeid)));

    for (size_t off : offsets) {
      size_t len = std::min(kSlice, kFileSize - off);
      auto w = pwrite_mirror(handle, local_fd, buf.data() + off, len,
                             static_cast<off_t>(off));
      ASSERT_EQ(w, (ssize_t)len);
    }

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Out-of-order pwrites within a fresh multi-MiB file. Forces several
  // DIRTY_FULL chunks and some PARTIAL chunks.
  void verify_out_of_order_writes_multipart() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_out_of_order";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kFileSize = 5 * 1024 * 1024 + 7;  // not aligned
    std::string buf = random_string(kFileSize);

    const size_t kSlice = 256 * 1024;
    std::vector<size_t> offsets;
    for (size_t off = 0; off < kFileSize; off += kSlice) offsets.push_back(off);
    std::reverse(offsets.begin(), offsets.end());

    for (size_t off : offsets) {
      size_t len = std::min(kSlice, kFileSize - off);
      auto w = pwrite_mirror(handle, local_fd, buf.data() + off, len,
                             static_cast<off_t>(off));
      ASSERT_EQ(w, (ssize_t)len);
    }

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Sparse extension: pwrite at an offset far beyond the (zero) initial
  // remote size. Bytes in the gap must show up as zeros on OSS.
  void verify_sparse_extension_into_hole() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_sparse_extend";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kPayloadOff = 5 * 1024 * 1024;
    const size_t kPayloadSize = 64 * 1024;
    std::string payload = random_string(kPayloadSize);
    auto w = pwrite_mirror(handle, local_fd, payload.data(), kPayloadSize,
                           static_cast<off_t>(kPayloadOff));
    ASSERT_EQ(w, (ssize_t)kPayloadSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_partial_write_triggers_get_on_write() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_get_on_write";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, /*size_MB=*/4);
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    // The local source file IS our mirror; reopen for RW.
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Overwrite 4 KiB at offset 2.5 MiB → chunk 1 (CLEAN within remote)
    // becomes PARTIAL after GET-on-write.
    const size_t kPatchOff = 2 * 1024 * 1024 + 512 * 1024;
    const size_t kPatchSize = 4 * 1024;
    std::string patch = random_string(kPatchSize);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatchSize,
                           static_cast<off_t>(kPatchOff));
    ASSERT_EQ(w, (ssize_t)kPatchSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Whole-chunk overwrite must NOT trigger GET-on-write (no need to fetch
  // the chunk; user data covers it entirely).
  void verify_full_chunk_overwrite_skips_get() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_full_chunk_overwrite";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);  // 4 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    std::string patch = random_string(CS);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), CS, 0);
    ASSERT_EQ(w, (ssize_t)CS);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Pwrite spanning multiple chunks: first / last chunks become PARTIAL
  // (require GET if pre-existed), middle chunks become DIRTY_FULL.
  void verify_pwrite_spans_chunks() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_span_chunks";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 8);  // 8 MiB → 4 chunks (chunk_size 2MiB)
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Write [1.5 MiB, 6.5 MiB): chunks 0 (PARTIAL tail), 1+2 (DIRTY_FULL),
    // 3 (PARTIAL head).
    const size_t kOff = 1536 * 1024;
    const size_t kLen = 5 * 1024 * 1024;
    std::string patch = random_string(kLen);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kLen,
                           static_cast<off_t>(kOff));
    ASSERT_EQ(w, (ssize_t)kLen);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // O_TRUNC handling in random-write mode, across three sub-cases:
  //   1. O_TRUNC on an existing remote object, no writes → remote empty.
  //   2. O_TRUNC then a small pwrite → remote holds only the new bytes.
  //   3. O_TRUNC on a shared, already-dirty ctx → exercises the non-first-ctx
  //      branch of OssRandomWriter::open (ftruncate(staging, 0) + erase
  //      chunks).
  void verify_o_trunc() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // ── Sub-cases 1 & 2: existing remote object, opened sequentially. ──
    {
      std::string filename = "rw_otrunc";
      std::string local_file = test_path_ + filename + ".src";
      create_random_file(local_file, 4);
      int rr =
          upload_file(local_file, parent_path + std::string("/") + filename,
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(rr, 0);
      int local_fd = reopen_mirror(local_file);
      DEFER(::close(local_fd));

      uint64_t nodeid = 0;
      struct stat st;
      int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      // Sub-case 1: O_TRUNC with no writes → remote becomes empty.
      {
        void *handle = nullptr;
        bool unused = false;
        r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle, &unused);
        ASSERT_EQ(r, 0);
        truncate_mirror(local_fd, 0);
        r = fs_->release(nodeid, get_file_from_handle(handle));
        ASSERT_EQ(r, 0);
        assert_remote_matches_local(filename, local_fd);
      }

      // Sub-case 2: O_TRUNC on the now-empty remote, then pwrite small
      // payload → remote contains only the new bytes (no leftover from any
      // earlier state).
      {
        void *handle = nullptr;
        bool unused = false;
        r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle, &unused);
        ASSERT_EQ(r, 0);
        truncate_mirror(local_fd, 0);

        const size_t kSize = 64 * 1024;
        std::string data = random_string(kSize);
        auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
        ASSERT_EQ(w, (ssize_t)kSize);

        r = fs_->release(nodeid, get_file_from_handle(handle));
        ASSERT_EQ(r, 0);
        assert_remote_matches_local(filename, local_fd);
      }
    }

    // ── Sub-case 3: O_TRUNC on a shared, already-dirty ctx. ──
    {
      std::string filename = "rw_otrunc_shared";
      uint64_t nodeid = 0;
      void *handle_a = nullptr;
      struct stat st;
      int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &handle_a);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      int local_fd = open_fresh_mirror(filename);
      DEFER(::close(local_fd));

      const size_t kASize = 1024 * 1024;
      std::string da = random_string(kASize);
      auto wa = pwrite_mirror(handle_a, local_fd, da.data(), kASize, 0);
      ASSERT_EQ(wa, (ssize_t)kASize);

      // Handle B opens with O_TRUNC; mirror gets truncated to 0.
      void *handle_b = nullptr;
      bool unused = false;
      r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle_b, &unused);
      ASSERT_EQ(r, 0);
      truncate_mirror(local_fd, 0);

      const size_t kBSize = 4 * 1024;
      std::string db = random_string(kBSize);
      auto wb = pwrite_mirror(handle_b, local_fd, db.data(), kBSize, 0);
      ASSERT_EQ(wb, (ssize_t)kBSize);

      r = fs_->release(nodeid, get_file_from_handle(handle_a));
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle_b));
      ASSERT_EQ(r, 0);
      assert_remote_matches_local(filename, local_fd);
    }
  }

  // Full lifecycle of RandomWriteContext across multiple scenarios.
  void verify_ctx_torn_down_after_release() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_lifecycle";
    uint64_t nodeid = 0;
    void *handle_a = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_a)->get_inode());
    ASSERT_NE(inode, nullptr);

    // ── Step 1+2: ctx created by open(O_CREAT), pwrite makes it dirty ──
    ASSERT_NE(inode->rw_ctx, nullptr) << "ctx must exist after create";
    ASSERT_EQ(inode->rw_ctx->ref_count, 1);

    const size_t kSize = 8 * 1024;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle_a, data.c_str(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_TRUE(inode->is_dirty) << "inode should be dirty after pwrite";
    ASSERT_GE(inode->rw_ctx->staging_fd, 0) << "staging_fd should be open";

    // ── Step 3: second handle shares ctx, ref_count bumps ──
    void *handle_b = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_b, &unused);
    ASSERT_EQ(r, 0);
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_EQ(inode->rw_ctx->ref_count, 2);

    // ── Step 4: release handle_b → ref_count=1, ctx survives ──
    // Note: handle_b's close triggers flush (is_dirty=true from handle_a).
    // After flush succeeds, mark_clean() clears is_dirty. This is expected:
    // the data handle_a wrote has been persisted by handle_b's flush.
    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    ASSERT_NE(inode->rw_ctx, nullptr)
        << "ctx must survive while handle_a still holds a ref";
    ASSERT_EQ(inode->rw_ctx->ref_count, 1);

    // ── Step 5: release handle_a → ref_count=0, ctx torn down ──
    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(inode->rw_ctx, nullptr)
        << "ctx must be torn down after last writer releases";
    EXPECT_FALSE(inode->is_dirty)
        << "inode must be clean after all writers close";

    // ── Step 6: re-open same inode → brand new ctx ──
    void *handle_c = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_c, &unused);
    ASSERT_EQ(r, 0);
    ASSERT_NE(inode->rw_ctx, nullptr) << "fresh ctx must be created on re-open";
    ASSERT_EQ(inode->rw_ctx->ref_count, 1);
    // remote_size must reflect what was successfully flushed earlier.
    EXPECT_EQ(inode->rw_ctx->remote_size, (uint64_t)kSize)
        << "new ctx should snapshot the current remote size";

    // Write something to exercise the new ctx, then release.
    std::string data2 = random_string(kSize);
    w = write_to_file_handle(handle_c, data2.c_str(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    // ── Step 7: release → ctx torn down again ──
    r = fs_->release(nodeid, get_file_from_handle(handle_c));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(inode->rw_ctx, nullptr)
        << "ctx must be torn down after second cycle";
    EXPECT_FALSE(inode->is_dirty);
  }

  // Multipart fast path: rewrite only chunk 0, leave the rest of a
  // multi-MiB remote intact. The unmodified parts must end up reusing
  // upload_part_copy.
  void verify_partial_rewrite_uses_part_copy() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_partial_rewrite";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 6);  // 6 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    std::string patch = random_string(CS);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), CS, 0);
    ASSERT_EQ(w, (ssize_t)CS);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Same chunk written many times: state is DIRTY_FULL after the first
  // whole-chunk write; subsequent writes within it must not re-trigger
  // GET-on-write or change state machine behavior.
  void verify_repeated_writes_same_chunk() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_repeat_same_chunk";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    std::string base = random_string(CS);
    auto w1 = pwrite_mirror(handle, local_fd, base.data(), CS, 0);
    ASSERT_EQ(w1, (ssize_t)CS);

    for (int i = 0; i < 100; ++i) {
      size_t off = static_cast<size_t>(rand()) % (CS - 256);
      size_t len = 1 + (static_cast<size_t>(rand()) % 256);
      std::string s = random_string(len);
      auto w = pwrite_mirror(handle, local_fd, s.data(), len,
                             static_cast<off_t>(off));
      ASSERT_EQ(w, (ssize_t)len);
    }

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Cross flush boundary: flush mid-stream, then continue writing.
  void verify_flush_mid_cycle_then_continue() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_flush_mid";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kFirst = 64 * 1024;
    const size_t kSecond = 64 * 1024;
    std::string a = random_string(kFirst);
    std::string b = random_string(kSecond);

    auto w1 = pwrite_mirror(handle, local_fd, a.data(), kFirst, 0);
    ASSERT_EQ(w1, (ssize_t)kFirst);

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);

    auto w2 = pwrite_mirror(handle, local_fd, b.data(), kSecond,
                            static_cast<off_t>(kFirst));
    ASSERT_EQ(w2, (ssize_t)kSecond);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_multi_handle_cooperative_write() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_multi_handle_coop";
    uint64_t nodeid = 0;
    void *handle_a = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    // A dirties the shared ctx on first write; ref_count of ctx is 1.
    const size_t kASize = 256 * 1024;
    std::string da = random_string(kASize);
    auto wa = pwrite_mirror(handle_a, local_fd, da.data(), kASize, 0);
    ASSERT_EQ(wa, (ssize_t)kASize);

    // B opens WITHOUT O_TRUNC — must share the dirty ctx with A and bump
    // ref_count to 2 without recreating the staging fd.
    void *handle_b = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_b, &unused);
    ASSERT_EQ(r, 0);

    // B's writes go through the SAME ctx + staging file. ctx is already
    // dirty so B doesn't need to grab the path lock for first-dirty.
    const size_t kBOff = 1 * 1024 * 1024;
    const size_t kBSize = 128 * 1024;
    std::string db = random_string(kBSize);
    auto wb = pwrite_mirror(handle_b, local_fd, db.data(), kBSize,
                            static_cast<off_t>(kBOff));
    ASSERT_EQ(wb, (ssize_t)kBSize);

    // Release A first: this triggers flush of the merged
    // staging contents (A + B). ctx becomes clean but its ref_count is
    // still 1 (held by B), so the ctx and staging survive.
    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);

    // After A's release, ctx is clean. A subsequent B write should be
    // legal: it re-dirties the ctx.
    const size_t kB2Off = 2 * 1024 * 1024;
    const size_t kB2Size = 64 * 1024;
    std::string db2 = random_string(kB2Size);
    auto wb2 = pwrite_mirror(handle_b, local_fd, db2.data(), kB2Size,
                             static_cast<off_t>(kB2Off));
    ASSERT_EQ(wb2, (ssize_t)kB2Size);

    // B's release drops ref_count to 0 and tears the ctx down. The final
    // remote content includes A's first burst, B's mid-stream write, and
    // B's post-A-release tail extension.
    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_continuous_fsync_cycles() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_continuous_fsync";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    // Three append-style bursts, each separated by an fsync. The second
    // and third fsyncs see remote_size > 0, so the prefix parts can
    // be uploaded via upload_part_copy (the file's contents up to the
    // previous flush boundary are already in OSS as a complete object).
    const size_t kBurst = 1536 * 1024;  // 1.5 MiB, straddles part 1/2
    for (int round = 0; round < 3; ++round) {
      std::string data = random_string(kBurst);
      auto w = pwrite_mirror(handle, local_fd, data.data(), kBurst,
                             static_cast<off_t>(round * kBurst));
      ASSERT_EQ(w, (ssize_t)kBurst);

      r = fsync_file_handle(handle, /*datasync=*/true);
      ASSERT_EQ(r, 0);
      assert_remote_matches_local(filename, local_fd);
    }

    // Final small write + release: still consistent after release path.
    const size_t kTail = 8 * 1024;
    std::string tail = random_string(kTail);
    auto wt = pwrite_mirror(handle, local_fd, tail.data(), kTail,
                            static_cast<off_t>(3 * kBurst));
    ASSERT_EQ(wt, (ssize_t)kTail);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // A single CLEAN chunk that straddles remote_size. With default
  // chunk_size=2 MiB, pre-populate the remote with 1.5 MiB (so chunk 0
  // is CLEAN with [0, 1.5MiB) backed by OSS and [1.5MiB, 2MiB) being a
  // hole), then dirty chunk 1 only. flush_put / flush_multipart must
  // walk chunk 0 via materialize_range and stitch GET (head) + zero-fill
  // (tail) within the same chunk slice.
  void verify_chunk_straddles_remote_boundary() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_chunk_straddles";
    std::string local_file = test_path_ + filename + ".src";

    // 1.5 MiB random file — sub-chunk so chunk 0 covers remote tail +
    // hole. create_random_file takes whole MiB; manually craft instead.
    {
      ::unlink(local_file.c_str());
      int fd = ::open(local_file.c_str(), O_RDWR | O_CREAT, 0600);
      ASSERT_GE(fd, 0);
      const size_t kRemoteSize = 1536 * 1024;
      std::string buf = random_string(kRemoteSize);
      ssize_t w = ::pwrite(fd, buf.data(), kRemoteSize, 0);
      ASSERT_EQ(w, (ssize_t)kRemoteSize);
      ::close(fd);
    }
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Write inside chunk 1 only — leave chunk 0 CLEAN. remote_size
    // is 1.5 MiB, so chunk 0 (=[0, 2 MiB)) crosses the remote boundary.
    const uint64_t CS = fs_->options_.random_write_chunk_size;
    ASSERT_EQ(CS, 2u * 1024 * 1024) << "test assumes default 2MiB chunk";
    const size_t kPatchOff = 2u * 1024 * 1024 + 4096;  // chunk 1
    const size_t kPatchSize = 4 * 1024;
    std::string patch = random_string(kPatchSize);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatchSize,
                           static_cast<off_t>(kPatchOff));
    ASSERT_EQ(w, (ssize_t)kPatchSize);

    // flush at release: chunk 0 is rebuilt by materialize_range which
    // emits a GET for [0, 1.5MiB) and zeros for [1.5MiB, 2MiB).
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_flush_failure_loses_data_cleanly() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_flush_fail";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Ensure the empty object actually exists on OSS (create_and_flush
    // has a 50% chance of skipping the flush). An explicit fdatasync
    // guarantees the empty put_object lands.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    const size_t kSize = 128 * 1024;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.c_str(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    // Inject OSS failure so flush (inside release/close) will fail.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // release → close → flush fails → data lost, error returned.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_NE(r, 0) << "release should propagate flush failure";

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // lookup the inode again — attr cache may have been invalidated by
    // mark_clean, so this may go to OSS. The object must exist (we
    // fdatasync'd above) and be empty (the 128KB flush failed).
    struct stat st2;
    uint64_t nodeid2 = 0;
    r = fs_->lookup(parent, filename.c_str(), &nodeid2, &st2);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));

    // attr.size should reflect the remote state (0), not the local-only
    // inflated 128KB.
    ASSERT_EQ(st2.st_size, 0)
        << "attr.size should revert to remote size after flush failure";

    // A fresh open + write + release (without fault) must succeed cleanly
    // on the same inode — proving no state corruption was left behind.
    void *handle2 = nullptr;
    bool unused = false;
    r = fs_->open(nodeid2, O_RDWR, &handle2, &unused);
    ASSERT_EQ(r, 0);

    const size_t kSize2 = 64 * 1024;
    std::string data2 = random_string(kSize2);
    w = write_to_file_handle(handle2, data2.c_str(), kSize2, 0);
    ASSERT_EQ(w, (ssize_t)kSize2);

    r = fs_->release(nodeid2, get_file_from_handle(handle2));
    ASSERT_EQ(r, 0) << "second write cycle must succeed after clean reset";

    // Verify the remote object matches the second write only.
    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(kSize2), meta["Content-Length"]);
    uint64_t expected_crc = cal_crc64(0, (void *)data2.data(), kSize2);
    ASSERT_EQ(std::to_string(expected_crc), meta["X-Oss-Hash-Crc64ecma"]);
  }

  // Hole-only part region in a multipart upload. Pre-populate a small
  // remote, then pwrite far past it: intermediate parts are entirely
  // beyond remote_size and must be uploaded as zero-filled parts.
  void verify_multipart_with_hole_parts() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_hole_parts";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 1);  // 1 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const size_t kPayloadOff = 6 * 1024 * 1024;
    const size_t kPayloadSize = 4 * 1024;
    std::string payload = random_string(kPayloadSize);
    auto w = pwrite_mirror(handle, local_fd, payload.data(), kPayloadSize,
                           static_cast<off_t>(kPayloadOff));
    ASSERT_EQ(w, (ssize_t)kPayloadSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_multipart_hole_parts_upload_zeros() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_mp_hole_zeros";
    std::string local_file = test_path_ + filename + ".src";

    // Pre-populate remote with 512 KB (less than chunk_size=2MiB, less than
    // upload_buffer_size=1MiB which we'll configure for multipart).
    {
      ::unlink(local_file.c_str());
      int fd = ::open(local_file.c_str(), O_RDWR | O_CREAT, 0600);
      ASSERT_GE(fd, 0);
      const size_t kRemoteSize = 512 * 1024;
      std::string buf = random_string(kRemoteSize);
      ssize_t w = ::pwrite(fd, buf.data(), kRemoteSize, 0);
      ASSERT_EQ(w, (ssize_t)kRemoteSize);
      ::close(fd);
    }
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Write 4 KB at offset 4 MiB. With upload_buffer_size=1MiB this produces
    // 5 parts: part 1=[0,1M), part 2=[1M,2M), part 3=[2M,3M), part 4=[3M,4M),
    // part 5=[4M,4M+4K). Parts 2-4 are entirely beyond remote_size(512KB)
    // and contain only CLEAN chunks → refill_clean_chunk sees pos>=remote_end
    // and returns 0; upload_part reads zeros from staging fd.
    const size_t kPayloadOff = 4 * 1024 * 1024;
    const size_t kPayloadSize = 4 * 1024;
    std::string payload = random_string(kPayloadSize);
    auto w = pwrite_mirror(handle, local_fd, payload.data(), kPayloadSize,
                           static_cast<off_t>(kPayloadOff));
    ASSERT_EQ(w, (ssize_t)kPayloadSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // A short staging pwrite followed by a hard failure must fail the whole
  // write, keep a previously-clean file clean, and succeed on retry.
  void verify_partial_pwrite_fails_and_retry_succeeds() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_partial_pwrite_fail";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // create_and_flush randomly skips the flush; force a clean start so the
    // failed-write rollback below is exercised deterministically.
    ASSERT_EQ(fs_->flush(nodeid, get_file_from_handle(handle)), 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_FALSE(inode->is_dirty);

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kSize = 64 * 1024;
    std::string data = random_string(kSize);

    // Short_Write fires on the 1st pwrite call (skip=0, run=1): halves r.
    // Fail fires on the 2nd pwrite call (skip=1, run=1): sets r=-1/EIO.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Short_Write,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/1));

    ssize_t w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_LT(w, 0) << "short staging write must fail the whole pwrite";

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Pwrite_Staging_Short_Write);
    g_fault_injector->clear_injection(FaultInjectionId::FI_Pwrite_Staging_Fail);

    // The failed write must not leave the previously-clean file dirty.
    ASSERT_FALSE(inode->is_dirty);

    auto w2 = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w2, (ssize_t)kSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_open_create_failure_no_leak() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const std::string good_dir = fs_->options_.temp_dir;
    const std::string bad_dir = good_dir + "/no_such_subdir_for_test";
    // Make sure the bad dir really doesn't exist.
    std::system(("rm -rf " + bad_dir).c_str());

    // ── Part A: OssFs::creat failure path inside create_internal ──
    {
      std::string filename = "rw_creat_open_fail";
      fs_->options_.temp_dir = bad_dir;

      uint64_t nodeid = 0;
      void *handle = nullptr;
      struct stat st;
      int r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid, &st, &handle);
      ASSERT_LT(r, 0) << "creat must fail when staging dir is missing";
      ASSERT_EQ(handle, nullptr) << "handle must not be set on failure";

      // Restore the dir and confirm no remote object was created and no
      // local inode was leaked into the global map (lookup → -ENOENT).
      fs_->options_.temp_dir = good_dir;
      uint64_t lookup_id = 0;
      struct stat st2;
      r = fs_->lookup(parent, filename.c_str(), &lookup_id, &st2);
      ASSERT_EQ(r, -ENOENT) << "no remote object must be left behind";

      // Retry creat — must succeed (no leftover state from the failure).
      r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0) << "creat after restoring dir must succeed";
      DEFER(fs_->forget(nodeid, 1));
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }

    // ── Part B: OssFs::open failure path ──
    {
      std::string filename = "rw_open_fail";
      uint64_t nodeid = 0;
      void *handle = nullptr;
      struct stat st;
      int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      auto inode =
          static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
      ASSERT_NE(inode, nullptr);

      // Drop the writer side cleanly so rw_ctx is torn down and we have
      // a deterministic baseline for the failure-state assertions below.
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      ASSERT_EQ(inode->rw_ctx, nullptr);
      ASSERT_EQ(inode->open_ref_cnt, 0);

      // Inject failure: bad staging dir → create_staging returns -ENOENT.
      fs_->options_.temp_dir = bad_dir;

      void *failed_handle = nullptr;
      bool unused = false;
      r = fs_->open(nodeid, O_RDWR, &failed_handle, &unused);
      ASSERT_LT(r, 0) << "open must propagate staging failure";
      EXPECT_EQ(failed_handle, nullptr) << "fh must not be set on failure";
      EXPECT_EQ(inode->rw_ctx, nullptr)
          << "rw_ctx must remain nullptr when create_staging fails";
      EXPECT_EQ(inode->open_ref_cnt, 0)
          << "open_ref_cnt must not be bumped on open failure";
      EXPECT_FALSE(inode->is_dirty)
          << "inode must remain clean when the writer never opened";

      // Restore the dir, retry open → must succeed and leave clean state.
      fs_->options_.temp_dir = good_dir;
      void *handle2 = nullptr;
      r = fs_->open(nodeid, O_RDWR, &handle2, &unused);
      ASSERT_EQ(r, 0) << "open after restoring dir must succeed";
      ASSERT_NE(handle2, nullptr);
      EXPECT_EQ(inode->open_ref_cnt, 1);
      EXPECT_NE(inode->rw_ctx, nullptr);

      r = fs_->release(nodeid, get_file_from_handle(handle2));
      ASSERT_EQ(r, 0);
      EXPECT_EQ(inode->rw_ctx, nullptr);
      EXPECT_EQ(inode->open_ref_cnt, 0);
    }
  }

  void verify_flush_multipart_error_not_swallowed_by_abort() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_multipart_abort_error";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    // Ensure baseline: empty object on OSS, clean state.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    // Write > upload_buffer_size (1 MiB) to force flush_multipart path.
    const size_t kSize = 1536 * 1024;  // 1.5 MiB → 2 parts
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    // With parallel upload, all parts race for run_count, so use unlimited
    // to ensure every thread's retries are exhausted.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(/*run_count=*/std::numeric_limits<uint32_t>::max(),
                       /*skip_count=*/1));

    // fdatasync → flush_multipart: init OK, upload_part fails, abort OK.
    // The original upload error must still be propagated to the caller.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_NE(r, 0) << "flush must propagate upload_part failure even "
                       "when abort_multipart succeeds";

    // Dirty state must be fully preserved for retry.
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty)
        << "inode must remain dirty after multipart flush failure";
    ASSERT_NE(inode->rw_ctx, nullptr)
        << "ctx must survive after multipart flush failure";
    ASSERT_GT(inode->rw_ctx->chunks.dirty_chunk_count(), 0u)
        << "chunks must be preserved for retry after multipart failure";

    // Clear injection — retry must succeed.
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "retry flush must succeed after fault cleared";

    // Release and verify final content.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  void verify_flush_multipart_copy_failure_propagated() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // 1. Pre-populate 3 MiB file on OSS (3 parts with 1 MiB buffer).
    std::string filename = "rw_multipart_copy_fail";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 3);  // 3 MiB
    int rr = upload_file(local_file, parent_path + "/" + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    // 2. Open existing file, dirty only part 3 (offset 2 MiB).
    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const size_t kPatchOff = 2 * 1024 * 1024;
    const size_t kPatchLen = 1024 * 1024;  // 1 MiB
    std::string patch = random_string(kPatchLen);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatchLen,
                           static_cast<off_t>(kPatchOff));
    ASSERT_EQ(w, (ssize_t)kPatchLen);

    // 3. Inject fault: skip init (1 call), fail all subsequent calls.
    //    Parts 1,2 are CLEAN -> upload_part_copy; Part 3 is DIRTY ->
    //    upload_part. Use unlimited run_count to cover all parallel threads'
    //    retries.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(/*run_count=*/std::numeric_limits<uint32_t>::max(),
                       /*skip_count=*/1));

    // 4. fdatasync -> flush_multipart fails.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_NE(r, 0) << "flush must propagate upload_part_copy failure";

    // 5. Dirty state preserved for retry.
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty);
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_GT(inode->rw_ctx->chunks.dirty_chunk_count(), 0u);

    // 6. Clear injection — retry must succeed.
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "retry flush must succeed after fault cleared";

    // 7. Release and verify data integrity.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Flush failure is retryable via fsync: inject an OSS error on the first
  // fdatasync, verify it fails but is_dirty is preserved; clear injection
  // and retry fdatasync — this time it must succeed.
  void verify_flush_failure_is_retryable() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_flush_retry";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kSize = 128 * 1024;
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    // Inject OSS failure.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // First fdatasync fails.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_NE(r, 0) << "flush should fail while fault is injected";

    // Dirty state must be preserved for retry.
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty)
        << "inode must remain dirty after transient flush failure";
    ASSERT_NE(inode->rw_ctx, nullptr) << "ctx must survive after flush failure";
    ASSERT_GT(inode->rw_ctx->chunks.dirty_chunk_count(), 0u)
        << "chunks must be preserved for retry";

    // Clear injection — OSS calls succeed again.
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // Retry fdatasync — must succeed now.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "retry flush must succeed after fault cleared";

    // Release cleanly.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);

    // ── Part 2: internal HTTP retry (flush_no_multipart) ──
    // Write again, inject only ONE HTTP-attempt failure so do_http_call's
    // internal retry recovers transparently — flush succeeds directly.
    handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    std::string data2 = random_string(kSize);
    w = pwrite_mirror(handle, local_fd, data2.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "flush must succeed when internal HTTP retry recovers";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);

    // ── Part 3: internal HTTP retry (flush_multipart) ──
    // Write > upload_buffer_size to trigger multipart path; inject one
    // HTTP-attempt failure so do_http_call's internal retry recovers.
    handle = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const size_t kMultipartSize = 1536 * 1024;  // 1.5 MiB → 2 parts
    std::string data3 = random_string(kMultipartSize);
    w = pwrite_mirror(handle, local_fd, data3.data(), kMultipartSize, 0);
    ASSERT_EQ(w, (ssize_t)kMultipartSize);

    // skip_count=1: let init_multipart_upload through.
    // run_count=1: fail one HTTP attempt inside upload_part's do_http_call;
    // its internal retry succeeds.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(/*run_count=*/1, /*skip_count=*/1));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0)
        << "multipart flush must succeed when internal HTTP retry recovers";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // ── Disk space limit tests ──
  uint64_t get_disk_avail_bytes() {
    struct statvfs vfs;
    EXPECT_EQ(::statvfs(test_path_.c_str(), &vfs), 0);
    return static_cast<uint64_t>(vfs.f_bavail) *
           static_cast<uint64_t>(vfs.f_frsize);
  }

  // Global staging disk usage tracked by the fs.
  uint64_t staging_disk_usage() {
    return fs_->staging_disk_usage_.load(std::memory_order_relaxed);
  }

  // On-disk blocks (in bytes) backing a random-write handle's staging file.
  uint64_t staging_fd_disk_bytes(void *handle) {
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    int fd = inode->rw_ctx->staging_fd;
    struct stat st;
    EXPECT_EQ(::fstat(fd, &st), 0);
    return static_cast<uint64_t>(st.st_blocks) * S_BLKSIZE;
  }

  // The staging fd backing a random-write handle.
  int staging_fd_of(void *handle) {
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    return inode->rw_ctx->staging_fd;
  }

  void verify_disk_space() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // ── Part A: pwrite path ──
    {
      std::string filename = "rw_diskfull_pwrite";
      uint64_t nodeid = 0;
      void *handle = nullptr;
      struct stat st;
      int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;
      const size_t kWriteSize = 64 * 1024;  // 64 KiB per write
      std::string data = random_string(kWriteSize);

      EXPECT_EQ(staging_disk_usage(), 0u);

      // Phase 1: temp_dir_free_bytes is small enough — writes succeed.
      fs_->options_.temp_dir_free_bytes = 1024;  // 1 KiB, trivially satisfiable
      for (int i = 0; i < 4; i++) {
        ssize_t w = write_to_file_handle(handle, data.data(), kWriteSize,
                                         static_cast<off_t>(i * kWriteSize));
        ASSERT_EQ(w, (ssize_t)kWriteSize)
            << "write #" << i << " must succeed with low diskfree";
      }

      EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

      // Phase 2: set temp_dir_free_bytes to actual_avail, so
      // reserve_randwrite_disk_budget sees avail < reserved + diskfree.
      // Sleep past the refresh window so the cached snapshot expires
      // naturally: the check below runs a real fstatvfs (right after we
      // sample avail) and is guaranteed to reject the write. Writes inside
      // the window are covered by verify_disk_budget_throttled_refresh and
      // verify_disk_budget_cross_file_growth.
      ::usleep(150 * 1000);
      uint64_t usage_before_enospc = staging_disk_usage();
      uint64_t avail = get_disk_avail_bytes();
      fs_->options_.temp_dir_free_bytes = avail;
      ssize_t w = write_to_file_handle(handle, data.data(), kWriteSize,
                                       static_cast<off_t>(4 * kWriteSize));
      ASSERT_EQ(w, -ENOSPC)
          << "pwrite must return -ENOSPC when diskfree >= avail";
      EXPECT_EQ(staging_disk_usage(), usage_before_enospc);

      // Phase 3: restore; keep writing until tracked usage grows past any
      // speculative preallocation.
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
      const size_t kGrowSize = 1024 * 1024;
      std::string grow_data = random_string(kGrowSize);
      off_t grow_off = static_cast<off_t>(4 * kWriteSize);
      for (int i = 0; i < 8 && staging_disk_usage() <= usage_before_enospc;
           i++, grow_off += kGrowSize) {
        w = write_to_file_handle(handle, grow_data.data(), kGrowSize, grow_off);
        ASSERT_EQ(w, (ssize_t)kGrowSize)
            << "pwrite must succeed after diskfree restored";
      }
      EXPECT_GT(staging_disk_usage(), usage_before_enospc);
      EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      // Last writer closed: staging usage is released back to zero.
      EXPECT_EQ(staging_disk_usage(), 0u);
    }

    // ── Part B: fetch_chunks (GET-on-write) path ──
    {
      // Pre-populate a multi-chunk file on OSS (4 MiB = 2 chunks with 2MiB).
      std::string filename = "rw_diskfull_fetch";
      std::string local_file = test_path_ + filename + ".src";
      create_random_file(local_file, 4);  // 4 MiB
      int rr = upload_file(local_file, parent_path + "/" + filename,
                           FLAGS_oss_bucket_prefix);
      ASSERT_EQ(rr, 0);
      int local_fd = reopen_mirror(local_file);
      DEFER(::close(local_fd));

      // Open the existing file for random write.
      uint64_t nodeid = 0;
      struct stat st_buf;
      int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st_buf);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      void *handle = nullptr;
      bool unused = false;
      r = fs_->open(nodeid, O_RDWR, &handle, &unused);
      ASSERT_EQ(r, 0);

      const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;

      // Phase 1: low diskfree — partial write into chunk 0 triggers
      // fetch_chunks and succeeds.
      fs_->options_.temp_dir_free_bytes = 1024;
      const size_t kPatchLen = 512;
      std::string patch = random_string(kPatchLen);
      ssize_t w = write_to_file_handle(handle, patch.data(), kPatchLen, 1024);
      ASSERT_EQ(w, (ssize_t)kPatchLen)
          << "partial write (triggering fetch) must succeed with low diskfree";

      EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

      // Phase 2: raise diskfree to actual_avail — partial write into chunk 1
      // triggers fetch_chunks which fails the space check. The cache is
      // expired naturally via sleep, see Part A.
      ::usleep(150 * 1000);
      uint64_t usage_before_enospc = staging_disk_usage();
      uint64_t avail = get_disk_avail_bytes();
      fs_->options_.temp_dir_free_bytes = avail;
      const size_t kChunkSize = fs_->options_.random_write_chunk_size;
      w = write_to_file_handle(handle, patch.data(), kPatchLen,
                               static_cast<off_t>(kChunkSize + 1024));
      ASSERT_EQ(w, -ENOSPC)
          << "fetch_chunks must return -ENOSPC when diskfree >= avail";
      // A failed fetch must not change tracked usage.
      EXPECT_EQ(staging_disk_usage(), usage_before_enospc);

      // Phase 3: restore — retry succeeds.
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
      w = write_to_file_handle(handle, patch.data(), kPatchLen,
                               static_cast<off_t>(kChunkSize + 1024));
      ASSERT_EQ(w, (ssize_t)kPatchLen);
      // chunk 1 fetched too — usage grew and still matches on-disk blocks.
      EXPECT_GT(staging_disk_usage(), usage_before_enospc);
      EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      EXPECT_EQ(staging_disk_usage(), 0u);
    }

    // ── Part C: flush refill_range path ──
    // A CLEAN chunk refilled from OSS during flush is subject to the same
    // disk-space check; when the disk is full the flush must fail with -ENOSPC
    // and stay retryable (dirty state preserved).
    {
      // Pre-populate a 2-chunk file on OSS (4 MiB, chunk_size 2 MiB).
      std::string filename = "rw_diskfull_refill";
      std::string local_file = test_path_ + filename + ".src";
      create_random_file(local_file, 4);  // 4 MiB
      int rr = upload_file(local_file, parent_path + "/" + filename,
                           FLAGS_oss_bucket_prefix);
      ASSERT_EQ(rr, 0);
      int local_fd = reopen_mirror(local_file);
      DEFER(::close(local_fd));

      uint64_t nodeid = 0;
      struct stat st_buf;
      int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st_buf);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      void *handle = nullptr;
      bool unused = false;
      r = fs_->open(nodeid, O_RDWR, &handle, &unused);
      ASSERT_EQ(r, 0);

      const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;

      // Dirty only chunk 0 (partial write) with low diskfree; chunk 1 stays
      // CLEAN and must be refilled from OSS at flush time.
      fs_->options_.temp_dir_free_bytes = 1024;
      const size_t kPatchLen = 512;
      std::string patch = random_string(kPatchLen);
      ssize_t w = write_to_file_handle(handle, patch.data(), kPatchLen, 1024);
      ASSERT_EQ(w, (ssize_t)kPatchLen);
      // Dirtying chunk 0 fetched it into staging.
      EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

      // Raise diskfree to actual avail so the flush-time refill of CLEAN
      // chunk 1 fails. The cache is expired naturally via sleep, see Part A.
      ::usleep(150 * 1000);
      uint64_t usage_before_enospc = staging_disk_usage();
      uint64_t avail = get_disk_avail_bytes();
      fs_->options_.temp_dir_free_bytes = avail;
      r = fsync_file_handle(handle, /*datasync=*/true);
      ASSERT_EQ(r, -ENOSPC)
          << "flush refill_range must return -ENOSPC when diskfree >= avail";

      // Dirty state must be preserved for retry.
      auto inode =
          static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
      ASSERT_TRUE(inode->is_dirty)
          << "inode must remain dirty after flush refill ENOSPC";
      // A failed refill must not change tracked usage.
      EXPECT_EQ(staging_disk_usage(), usage_before_enospc);

      // Restore — flush succeeds on retry.
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
      r = fsync_file_handle(handle, /*datasync=*/true);
      ASSERT_EQ(r, 0) << "flush must succeed after diskfree restored";
      // Refill of CLEAN chunk 1 grew the staging file; usage still matches.
      EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
      EXPECT_EQ(staging_disk_usage(), 0u);
    }
  }

  // With a long refresh window, only the staging_disk_usage_ growth
  // compensation keeps the ENOSPC check accurate.
  void verify_disk_budget_throttled_refresh() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_throttled_budget";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const uint64_t saved_interval = fs_->staging_avail_refresh_ns_;
    const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;
    DEFER({
      fs_->staging_avail_refresh_ns_ = saved_interval;
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    });

    // A window long enough that no fstatvfs re-runs below.
    fs_->staging_avail_refresh_ns_ = 60ULL * 1000 * 1000 * 1000;  // 60s
    fs_->staging_avail_ts_ns_ = 0;  // force the very next write to refresh

    const size_t kWriteSize = 64 * 1024;
    std::string data = random_string(kWriteSize);

    // Phase 1: warm the cache once, then keep writing inside the window.
    fs_->options_.temp_dir_free_bytes = 1024;
    for (int i = 0; i < 4; i++) {
      ssize_t w = write_to_file_handle(handle, data.data(), kWriteSize,
                                       static_cast<off_t>(i * kWriteSize));
      ASSERT_EQ(w, (ssize_t)kWriteSize)
          << "write #" << i << " must succeed with low diskfree";
    }
    EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

    // Phase 2: pin diskfree to the effective avail. Without the growth
    // compensation the raw cached avail would pass the budget, so ENOSPC
    // here proves the compensation works; no fstatvfs runs inside the
    // window, so external disk activity cannot affect this check.
    uint64_t usage_before_enospc = staging_disk_usage();
    uint64_t effective = 0;
    ASSERT_EQ(fs_->staging_disk_avail(staging_fd_of(handle), &effective), 0);
    fs_->options_.temp_dir_free_bytes = effective;
    ssize_t w = write_to_file_handle(handle, data.data(), kWriteSize,
                                     static_cast<off_t>(4 * kWriteSize));
    ASSERT_EQ(w, -ENOSPC)
        << "write inside the refresh window must still see our own growth";
    EXPECT_EQ(staging_disk_usage(), usage_before_enospc);

    // Phase 3: restore budget; keep writing until tracked usage grows past
    // any speculative preallocation.
    fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    const size_t kGrowSize = 1024 * 1024;
    std::string grow_data = random_string(kGrowSize);
    off_t grow_off = static_cast<off_t>(4 * kWriteSize);
    for (int i = 0; i < 8 && staging_disk_usage() <= usage_before_enospc;
         i++, grow_off += kGrowSize) {
      w = write_to_file_handle(handle, grow_data.data(), kGrowSize, grow_off);
      ASSERT_EQ(w, (ssize_t)kGrowSize)
          << "pwrite must succeed after diskfree restored";
    }
    EXPECT_GT(staging_disk_usage(), usage_before_enospc);
    EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(staging_disk_usage(), 0u);
  }

  // With a FRESH cached snapshot (inside the refresh window), growth of
  // ANOTHER file's staging bytes must still be compensated so the ENOSPC
  // check stays accurate without any re-running of fstatvfs.
  void verify_disk_budget_cross_file_growth() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid_a = 0, nodeid_b = 0;
    void *handle_a = nullptr;
    void *handle_b = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "rw_cross_growth_a", CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &nodeid_a, &st, &handle_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_a, 1));
    r = create_and_flush(parent, "rw_cross_growth_b", CREATE_BASE_FLAGS, 0777,
                         0, 0, 0, &nodeid_b, &st, &handle_b);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_b, 1));

    const uint64_t saved_interval = fs_->staging_avail_refresh_ns_;
    const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;
    DEFER({
      fs_->staging_avail_refresh_ns_ = saved_interval;
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    });

    // A window long enough that no fstatvfs re-runs below.
    fs_->staging_avail_refresh_ns_ = 60ULL * 1000 * 1000 * 1000;  // 60s
    fs_->staging_avail_ts_ns_ = 0;  // force the very next write to refresh

    const size_t kWriteSize = 64 * 1024;
    std::string data = random_string(kWriteSize);

    // Warm the avail cache via A with a satisfiable budget.
    fs_->options_.temp_dir_free_bytes = 1024;
    for (int i = 0; i < 4; i++) {
      ssize_t w = write_to_file_handle(handle_a, data.data(), kWriteSize,
                                       static_cast<off_t>(i * kWriteSize));
      ASSERT_EQ(w, (ssize_t)kWriteSize);
    }

    // B grows the staging usage by 1 MiB INSIDE the refresh window.
    const size_t kGrowSize = 1024 * 1024;
    std::string grow_data = random_string(kGrowSize);
    ssize_t w = write_to_file_handle(handle_b, grow_data.data(), kGrowSize, 0);
    ASSERT_EQ(w, (ssize_t)kGrowSize);

    // Pin diskfree to the effective avail. Without the growth compensation
    // the raw cached avail would pass the budget, so ENOSPC here proves
    // ANOTHER file's growth is compensated inside the window.
    uint64_t usage_before_enospc = staging_disk_usage();
    uint64_t effective = 0;
    ASSERT_EQ(fs_->staging_disk_avail(staging_fd_of(handle_a), &effective), 0);
    fs_->options_.temp_dir_free_bytes = effective;
    w = write_to_file_handle(handle_a, data.data(), kWriteSize,
                             static_cast<off_t>(4 * kWriteSize));
    ASSERT_EQ(w, -ENOSPC)
        << "another file's staging growth must be compensated inside the "
           "refresh window";
    EXPECT_EQ(staging_disk_usage(), usage_before_enospc);

    // Restore budget — A's write succeeds again.
    fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    w = write_to_file_handle(handle_a, data.data(), kWriteSize,
                             static_cast<off_t>(4 * kWriteSize));
    ASSERT_EQ(w, (ssize_t)kWriteSize)
        << "write must succeed after diskfree restored";

    r = fs_->release(nodeid_a, get_file_from_handle(handle_a));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid_b, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(staging_disk_usage(), 0u);
  }

  // Inside the refresh window the avail snapshot never re-runs. The CLEAN
  // chunks refilled during flush must still be accounted incrementally so
  // the flush-time budget check fails with ENOSPC once their growth eats
  // the budget; without the mid-flush accounting every refill check would
  // see the frozen usage and the flush would wrongly succeed.
  void verify_disk_budget_refill_growth_in_window() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // Pre-populate a 4-chunk file on OSS; chunk 0 gets dirtied below,
    // chunks 1..3 stay CLEAN and must be refilled from OSS at flush time.
    const uint64_t chunk_size = fs_->options_.random_write_chunk_size;
    const uint64_t kChunks = 4;
    std::string filename = "rw_refill_budget";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, chunk_size * kChunks / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + "/" + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    uint64_t nodeid = 0;
    struct stat st_buf;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st_buf);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const uint64_t saved_interval = fs_->staging_avail_refresh_ns_;
    const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;
    DEFER({
      fs_->staging_avail_refresh_ns_ = saved_interval;
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    });

    // A window long enough that no fstatvfs re-runs below.
    fs_->staging_avail_refresh_ns_ = 60ULL * 1000 * 1000 * 1000;  // 60s
    fs_->staging_avail_ts_ns_ = 0;  // force the very next write to refresh

    // Dirty only chunk 0 (partial write) with a satisfiable budget; this
    // warms the avail snapshot and fetches chunk 0 into staging.
    fs_->options_.temp_dir_free_bytes = 1024;
    const size_t kPatchLen = 1024;
    std::string patch = random_string(kPatchLen);
    ssize_t w = write_to_file_handle(handle, patch.data(), kPatchLen, 0);
    ASSERT_EQ(w, (ssize_t)kPatchLen);
    EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

    // Pin the budget INSIDE the window: effective avail minus the growth
    // the flush refill will produce. Refills of chunks 1..2 must pass
    // (boundary-tight), and chunk 3 must then hit ENOSPC because the
    // already-refilled bytes are compensated without any fstatvfs re-run.
    uint64_t effective = 0;
    ASSERT_EQ(fs_->staging_disk_avail(staging_fd_of(handle), &effective), 0);
    ASSERT_GT(effective, 3 * chunk_size) << "test disk has too little room";
    fs_->options_.temp_dir_free_bytes = effective - 2 * chunk_size;

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, -ENOSPC)
        << "flush refill growth inside the refresh window must be accounted";

    // Dirty state must be preserved for retry, and the mid-flush growth of
    // the already-downloaded CLEAN chunks must be visible and accurate.
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty)
        << "inode must remain dirty after flush refill ENOSPC";
    EXPECT_GT(staging_disk_usage(), chunk_size);
    EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

    // Restore — flush succeeds on retry.
    fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "flush must succeed after diskfree restored";
    EXPECT_EQ(staging_disk_usage(), staging_fd_disk_bytes(handle));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(staging_disk_usage(), 0u);
  }

  // OS threads randomly write to their own files concurrently in the
  // default refresh mode, exercising the concurrent election and cache-hit
  // paths of staging_disk_avail.
  void verify_concurrent_random_writes() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kConcurrency = 8;
    const size_t kWriteSize = 64 * 1024;
    const int kChunksPerFile = 4;

    struct WriterCtx {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      std::vector<std::string> chunks;
    };
    std::vector<WriterCtx> ctxs(kConcurrency);

    for (int i = 0; i < kConcurrency; i++) {
      std::string filename = "rw_conc_write_" + std::to_string(i);
      struct stat st;
      int r =
          create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &ctxs[i].nodeid, &st, &ctxs[i].handle);
      ASSERT_EQ(r, 0);
      for (int c = 0; c < kChunksPerFile; c++) {
        ctxs[i].chunks.push_back(random_string(kWriteSize));
      }
    }
    DEFER({
      for (auto &ctx : ctxs) {
        fs_->forget(ctx.nodeid, 1);
      }
    });

    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < kConcurrency; i++) {
      threads.emplace_back([&, i]() {
        INIT_PHOTON();
        auto &ctx = ctxs[i];
        for (int c = 0; c < kChunksPerFile; c++) {
          ssize_t w =
              write_to_file_handle(ctx.handle, ctx.chunks[c].data(), kWriteSize,
                                   static_cast<off_t>(c * kWriteSize));
          EXPECT_EQ(w, (ssize_t)kWriteSize) << "writer " << i << " chunk " << c;
          if (w != (ssize_t)kWriteSize) {
            failed = true;
            return;
          }
        }
        // Read back and verify the content of each chunk.
        std::string buf(kWriteSize, '\0');
        for (int c = 0; c < kChunksPerFile; c++) {
          ssize_t r = read_from_handle(ctx.handle, buf.data(), kWriteSize,
                                       static_cast<off_t>(c * kWriteSize));
          EXPECT_EQ(r, (ssize_t)kWriteSize) << "writer " << i << " chunk " << c;
          if (r != (ssize_t)kWriteSize || buf != ctx.chunks[c]) {
            failed = true;
            return;
          }
        }
      });
    }
    for (auto &t : threads) t.join();
    ASSERT_FALSE(failed);

    for (auto &ctx : ctxs) {
      int r = fs_->release(ctx.nodeid, get_file_from_handle(ctx.handle));
      ASSERT_EQ(r, 0);
    }
    EXPECT_EQ(staging_disk_usage(), 0u);
  }

  // Concurrent writers hit a stale avail cache at the same moment; every
  // writer must still see an accurate avail (losers run their own fstatvfs
  // instead of serving the stale snapshot).
  void verify_concurrent_disk_budget_stale_refresh() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kConcurrency = 8;
    const size_t kWriteSize = 64 * 1024;

    std::vector<uint64_t> nodeids(kConcurrency, 0);
    std::vector<void *> handles(kConcurrency, nullptr);
    std::string data = random_string(kWriteSize);

    for (int i = 0; i < kConcurrency; i++) {
      std::string filename = "rw_conc_budget_" + std::to_string(i);
      struct stat st;
      int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeids[i], &st, &handles[i]);
      ASSERT_EQ(r, 0);
    }
    DEFER({
      for (int i = 0; i < kConcurrency; i++) {
        fs_->forget(nodeids[i], 1);
      }
    });

    const uint64_t saved_interval = fs_->staging_avail_refresh_ns_;
    const uint64_t saved_free_bytes = fs_->options_.temp_dir_free_bytes;
    DEFER({
      fs_->staging_avail_refresh_ns_ = saved_interval;
      fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    });

    // A window long enough that the cache never re-refreshes below.
    fs_->staging_avail_refresh_ns_ = 60ULL * 1000 * 1000 * 1000;  // 60s

    // All writers write concurrently and must all observe `expect`.
    auto write_all = [&](off_t off, ssize_t expect) -> bool {
      std::atomic<bool> mismatch{false};
      std::vector<std::thread> threads;
      for (int i = 0; i < kConcurrency; i++) {
        threads.emplace_back([&, i]() {
          INIT_PHOTON();
          ssize_t w =
              write_to_file_handle(handles[i], data.data(), kWriteSize, off);
          EXPECT_EQ(w, expect) << "writer " << i;
          if (w != expect) mismatch = true;
        });
      }
      for (auto &t : threads) t.join();
      return !mismatch;
    };

    // Phase 1: stale-cache burst with a satisfiable budget; every write
    // succeeds.
    fs_->staging_avail_ts_ns_ = 0;  // force a stale-cache burst
    fs_->options_.temp_dir_free_bytes = 1024;
    ASSERT_TRUE(write_all(0, static_cast<ssize_t>(kWriteSize)));

    // Phase 2: stale-cache burst with diskfree pinned to the real avail;
    // every writer must get -ENOSPC no matter who wins the election.
    fs_->staging_avail_ts_ns_ = 0;
    uint64_t usage_before_enospc = staging_disk_usage();
    fs_->options_.temp_dir_free_bytes = get_disk_avail_bytes();
    ASSERT_TRUE(write_all(static_cast<off_t>(kWriteSize), -ENOSPC));
    EXPECT_EQ(staging_disk_usage(), usage_before_enospc);

    // Phase 3: restore budget; concurrent writes succeed again.
    fs_->options_.temp_dir_free_bytes = saved_free_bytes;
    ASSERT_TRUE(write_all(static_cast<off_t>(kWriteSize),
                          static_cast<ssize_t>(kWriteSize)));

    for (int i = 0; i < kConcurrency; i++) {
      int r = fs_->release(nodeids[i], get_file_from_handle(handles[i]));
      ASSERT_EQ(r, 0);
    }
    EXPECT_EQ(staging_disk_usage(), 0u);
  }

  void verify_read_fresh_after_prefetch_write_cycle() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // ── Set up an 8 MiB remote object with known OLD content. ──
    std::string filename = "rw_read_fresh_after_prefetch";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, /*size_MB=*/8);
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Target a 64 KiB window inside chunk 1 (a partial overwrite → the chunk
    // is fetched into staging and marked DIRTY).
    const off_t kTargetOff = 3 * 1024 * 1024;
    const size_t kPatchSize = 64 * 1024;
    std::string old_bytes(kPatchSize, '\0');
    std::string got(kPatchSize, '\0');

    // ── Step 1: open read handle, warm the cache at the target (OLD). ──
    void *handle_r = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &handle_r, &unused);
    ASSERT_EQ(r, 0);

    ssize_t n =
        read_from_handle(handle_r, old_bytes.data(), kPatchSize, kTargetOff);
    ASSERT_EQ(n, (ssize_t)kPatchSize) << "warm-up read";

    // ── Step 2: open write handle, overwrite the target with NEW content. ──
    std::string new_bytes = random_string(kPatchSize);
    ASSERT_NE(new_bytes, old_bytes) << "patch must differ to be meaningful";

    void *handle_w = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_w, &unused);
    ASSERT_EQ(r, 0);

    auto w = write_to_file_handle(handle_w, new_bytes.data(), kPatchSize,
                                  kTargetOff);
    ASSERT_EQ(w, (ssize_t)kPatchSize);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_w)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_TRUE(inode->is_dirty);

    // ── Step 3: read via the read handle *during* the dirty window. ──
    // This enters the random-write route, arms the one-shot drop, and must
    // return NEW data (served from the writer's staging file, cross-handle).
    n = read_from_handle(handle_r, got.data(), kPatchSize, kTargetOff);
    ASSERT_EQ(n, (ssize_t)kPatchSize) << "in-window read";
    EXPECT_EQ(got, new_bytes) << "in-window read must see staged NEW data";

    // ── Step 4: release write handle → flush to OSS, rw_ctx torn down. ──
    r = fs_->release(nodeid, get_file_from_handle(handle_w));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr) << "ctx torn down after last writer";
    ASSERT_FALSE(inode->is_dirty) << "inode clean after flush";

    // ── Step 5: read again on the normal cache path. Must be NEW. ──
    // If the drop in step 3 had not run, the stale prefetched OLD block would
    // still be cached on this handle and served here.
    got.assign(kPatchSize, '\0');
    n = read_from_handle(handle_r, got.data(), kPatchSize, kTargetOff);
    ASSERT_EQ(n, (ssize_t)kPatchSize) << "post-flush read";
    EXPECT_EQ(got, new_bytes)
        << "post-flush read served stale prefetched data (drop did not run)";

    r = fs_->release(nodeid, get_file_from_handle(handle_r));
    ASSERT_EQ(r, 0);
  }

  // Read-while-write coverage: with an active dirty window, a concurrent read
  // handle (and the writer's own O_RDWR handle) must see the correct data from
  // every source the router can pick. Finally, after flush + rw_ctx teardown,
  void verify_read_while_write_mixed_sources() {
    const uint64_t CS = fs_->options_.random_write_chunk_size;

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // ── 8 MiB (== 4 chunks when CS==2MiB) remote object with OLD content. ──
    std::string filename = "rw_read_while_write";
    std::string local_file = test_path_ + filename + ".src";
    const uint64_t kOrigSize = 4 * CS;
    create_random_file(local_file, /*size_MB=*/kOrigSize / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    // Local source is the ground truth for OLD (remote) bytes.
    int src_fd = ::open(local_file.c_str(), O_RDONLY);
    ASSERT_GE(src_fd, 0) << "open src errno=" << errno;
    DEFER(::close(src_fd));
    auto read_src = [&](off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = ::pread(src_fd, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_src off=" << off;
      return s;
    };
    auto read_fh = [&](void *fh, off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = read_from_handle(fh, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_fh off=" << off << " len=" << len;
      return s;
    };

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // ── Read handle warms its cache over the whole file (OLD prefetched). ──
    void *handle_r = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &handle_r, &unused);
    ASSERT_EQ(r, 0);
    for (uint64_t off = 0; off < kOrigSize; off += CS) {
      std::string warm = read_fh(handle_r, off, 64 * 1024);
      ASSERT_EQ(warm, read_src(off, 64 * 1024)) << "warm off=" << off;
    }

    // ── Open write handle; overwrite the LAST 32 KiB of chunk 1 (NEW). ──
    // Partial overwrite -> chunk 1 becomes DIRTY (staged w/ get-on-write).
    const size_t kNewSize = 32 * 1024;
    const off_t kNewOff = static_cast<off_t>(2 * CS - kNewSize);  // end of chk1
    std::string new_bytes = random_string(kNewSize);

    void *handle_w = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_w, &unused);
    ASSERT_EQ(r, 0);
    auto w =
        write_to_file_handle(handle_w, new_bytes.data(), kNewSize, kNewOff);
    ASSERT_EQ(w, (ssize_t)kNewSize);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_w)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_TRUE(inode->is_dirty);

    // ── Reads while the window is dirty, via the separate read handle. ──
    // (a) DIRTY: the overwritten bytes -> NEW (from staging).
    EXPECT_EQ(read_fh(handle_r, kNewOff, kNewSize), new_bytes) << "dirty read";
    // (b) DIRTY, non-overwritten part of chunk 1 -> OLD (staged get-on-write).
    EXPECT_EQ(read_fh(handle_r, CS, 4096), read_src(CS, 4096))
        << "dirty chunk untouched remainder";
    // (c) CLEAN chunk 0, never written -> OLD (direct OSS).
    EXPECT_EQ(read_fh(handle_r, 0, 4096), read_src(0, 4096)) << "clean read";
    // (d) Span crossing chunk1(DIRTY)->chunk2(CLEAN): NEW || OLD(chunk2 head).
    EXPECT_EQ(read_fh(handle_r, kNewOff, 2 * kNewSize),
              new_bytes + read_src(2 * CS, kNewSize))
        << "mixed dirty+clean span";
    // (e) The writer's own O_RDWR handle reads its staged data too.
    EXPECT_EQ(read_fh(handle_w, kNewOff, kNewSize), new_bytes)
        << "self read via write handle";

    // ── Sparse extension: write past EOF, opening a hole [kOrigSize, extOff).
    const size_t kExtSize = 16 * 1024;
    const off_t kExtOff = static_cast<off_t>(kOrigSize + 512 * 1024);
    std::string ext_bytes = random_string(kExtSize);
    w = write_to_file_handle(handle_w, ext_bytes.data(), kExtSize, kExtOff);
    ASSERT_EQ(w, (ssize_t)kExtSize);

    const std::string zeros4k(4096, '\0');
    // (f) Hole inside the freshly-extended (DIRTY) chunk -> zeros.
    EXPECT_EQ(read_fh(handle_r, static_cast<off_t>(kOrigSize), 4096), zeros4k)
        << "hole read";
    // (g) The extended payload -> NEW2.
    EXPECT_EQ(read_fh(handle_r, kExtOff, kExtSize), ext_bytes) << "ext read";
    // (h) At/after EOF -> 0 bytes.
    {
      char c = 0;
      ssize_t g = read_from_handle(handle_r, &c, 1,
                                   static_cast<off_t>(kExtOff) + kExtSize);
      EXPECT_EQ(g, 0) << "read at EOF must return 0";
    }

    // ── Flush + tear down the window. ──
    r = fs_->release(nodeid, get_file_from_handle(handle_w));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // ── Post-flush: same reads on the NORMAL path must stay consistent. ──
    EXPECT_EQ(read_fh(handle_r, kNewOff, kNewSize), new_bytes)
        << "post-flush dirty region stale";
    EXPECT_EQ(read_fh(handle_r, 0, 4096), read_src(0, 4096))
        << "post-flush clean region";
    EXPECT_EQ(read_fh(handle_r, kExtOff, kExtSize), ext_bytes)
        << "post-flush extended payload";
    EXPECT_EQ(read_fh(handle_r, static_cast<off_t>(kOrigSize), 4096), zeros4k)
        << "post-flush hole must read as zeros";

    r = fs_->release(nodeid, get_file_from_handle(handle_r));
    ASSERT_EQ(r, 0);
  }

  // Read-while-write served by OssDirectReader (prefetch disabled, so
  // create_oss_reader() yields a DirectReader instead of an OssCachedReader).
  // Exercises the random-write per-chunk routing on the DIRECT read path:
  //   DIRTY chunk -> staging, CLEAN chunk -> direct OSS, hole -> zeros,
  // plus the non-dirty fall-through where DirectReader reads straight from OSS.
  void verify_direct_reader_read_while_write() {
    const uint64_t CS = fs_->options_.random_write_chunk_size;

    // This test is only meaningful with prefetch disabled; otherwise the read
    // path would be served by OssCachedReader.
    ASSERT_FALSE(fs_->enable_prefetching())
        << "test requires prefetch_concurrency == 0 to force DirectReader";

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // ── 8 MiB (== 4 chunks when CS==2MiB) remote object with OLD content. ──
    std::string filename = "rw_direct_read_while_write";
    std::string local_file = test_path_ + filename + ".src";
    const uint64_t kOrigSize = 4 * CS;
    create_random_file(local_file, /*size_MB=*/kOrigSize / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    // Local source is the ground truth for OLD (remote) bytes.
    int src_fd = ::open(local_file.c_str(), O_RDONLY);
    ASSERT_GE(src_fd, 0) << "open src errno=" << errno;
    DEFER(::close(src_fd));
    auto read_src = [&](off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = ::pread(src_fd, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_src off=" << off;
      return s;
    };
    auto read_fh = [&](void *fh, off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = read_from_handle(fh, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_fh off=" << off << " len=" << len;
      return s;
    };

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // ── Separate read-only handle (also served by OssDirectReader). ──
    void *handle_r = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &handle_r, &unused);
    ASSERT_EQ(r, 0);

    // Before any write the file is clean: DirectReader reads straight from OSS.
    EXPECT_EQ(read_fh(handle_r, 0, 4096), read_src(0, 4096))
        << "pre-write clean read via DirectReader";

    // ── Open write handle; overwrite the LAST 32 KiB of chunk 1 (NEW). ──
    // Partial overwrite -> chunk 1 becomes DIRTY (staged w/ get-on-write).
    const size_t kNewSize = 32 * 1024;
    const off_t kNewOff = static_cast<off_t>(2 * CS - kNewSize);  // end of chk1
    std::string new_bytes = random_string(kNewSize);

    void *handle_w = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_w, &unused);
    ASSERT_EQ(r, 0);
    auto w =
        write_to_file_handle(handle_w, new_bytes.data(), kNewSize, kNewOff);
    ASSERT_EQ(w, (ssize_t)kNewSize);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_w)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_TRUE(inode->is_dirty);

    // ── Reads while dirty, through the read-only DirectReader handle. ──
    // (a) DIRTY: the overwritten bytes -> NEW (from staging).
    EXPECT_EQ(read_fh(handle_r, kNewOff, kNewSize), new_bytes)
        << "dirty read (staging)";
    // (b) DIRTY, non-overwritten part of chunk 1 -> OLD (staged get-on-write).
    EXPECT_EQ(read_fh(handle_r, CS, 4096), read_src(CS, 4096))
        << "dirty chunk untouched remainder";
    // (c) CLEAN chunk 0, never written -> OLD (direct OSS).
    EXPECT_EQ(read_fh(handle_r, 0, 4096), read_src(0, 4096))
        << "clean read (direct OSS)";
    // (d) Span crossing chunk1(DIRTY)->chunk2(CLEAN): NEW || OLD(chunk2 head).
    EXPECT_EQ(read_fh(handle_r, kNewOff, 2 * kNewSize),
              new_bytes + read_src(2 * CS, kNewSize))
        << "mixed dirty+clean span";
    // (e) The writer's own O_RDWR handle reads its staged data too.
    EXPECT_EQ(read_fh(handle_w, kNewOff, kNewSize), new_bytes)
        << "self read via write handle";

    // ── Sparse extension: write past EOF, opening a hole [kOrigSize, extOff).
    const size_t kExtSize = 16 * 1024;
    const off_t kExtOff = static_cast<off_t>(kOrigSize + 512 * 1024);
    std::string ext_bytes = random_string(kExtSize);
    w = write_to_file_handle(handle_w, ext_bytes.data(), kExtSize, kExtOff);
    ASSERT_EQ(w, (ssize_t)kExtSize);

    const std::string zeros4k(4096, '\0');
    // (f) Hole inside the freshly-extended (DIRTY) chunk -> zeros.
    EXPECT_EQ(read_fh(handle_r, static_cast<off_t>(kOrigSize), 4096), zeros4k)
        << "hole read -> zeros";
    // (g) The extended payload -> NEW2.
    EXPECT_EQ(read_fh(handle_r, kExtOff, kExtSize), ext_bytes) << "ext read";
    // (h) At/after EOF -> 0 bytes.
    {
      char c = 0;
      ssize_t g = read_from_handle(handle_r, &c, 1,
                                   static_cast<off_t>(kExtOff) + kExtSize);
      EXPECT_EQ(g, 0) << "read at EOF must return 0";
    }

    // ── Flush + tear down the window. ──
    r = fs_->release(nodeid, get_file_from_handle(handle_w));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // ── Post-flush: the same reads now fall through to direct OSS reads. ──
    EXPECT_EQ(read_fh(handle_r, kNewOff, kNewSize), new_bytes)
        << "post-flush overwritten region";
    EXPECT_EQ(read_fh(handle_r, 0, 4096), read_src(0, 4096))
        << "post-flush clean region";
    EXPECT_EQ(read_fh(handle_r, kExtOff, kExtSize), ext_bytes)
        << "post-flush extended payload";
    EXPECT_EQ(read_fh(handle_r, static_cast<off_t>(kOrigSize), 4096), zeros4k)
        << "post-flush hole must read as zeros";

    r = fs_->release(nodeid, get_file_from_handle(handle_r));
    ASSERT_EQ(r, 0);
  }

  // Hole zero-fill from the CLEAN branch, not the staging sparse path.
  void verify_read_hole_in_clean_chunk_beyond_remote_size() {
    const uint64_t CS = fs_->options_.random_write_chunk_size;

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_read_hole_clean_beyond";
    std::string local_file = test_path_ + filename + ".src";
    const uint64_t kOrigSize = 2 * CS;
    create_random_file(local_file, /*size_MB=*/kOrigSize / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));
    auto read_src = [&](off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = ::pread(local_fd, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_src off=" << off;
      return s;
    };
    auto read_fh = [&](void *fh, off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = read_from_handle(fh, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_fh off=" << off << " len=" << len;
      return s;
    };

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const size_t kPayloadSize = 4 * 1024;
    const off_t kPayloadOff = static_cast<off_t>(kOrigSize + 2 * CS + 4096);
    std::string payload = random_string(kPayloadSize);
    auto w = pwrite_mirror(handle, local_fd, payload.data(), kPayloadSize,
                           kPayloadOff);
    ASSERT_EQ(w, (ssize_t)kPayloadSize);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_TRUE(inode->is_dirty);

    const std::string zeros4k(4096, '\0');
    EXPECT_EQ(read_fh(handle, static_cast<off_t>(kOrigSize) + 4096, 4096),
              zeros4k)
        << "hole in clean chunk beyond remote_size must be zero-filled";
    EXPECT_EQ(read_fh(handle, static_cast<off_t>(kOrigSize + CS), CS + 8192),
              std::string(CS + 4096, '\0') + payload)
        << "span across clean hole chunks into dirty chunk";
    // Remote-backed region stays intact during the dirty window.
    EXPECT_EQ(read_fh(handle, 0, 4096), read_src(0, 4096)) << "clean head";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // CLEAN chunk straddling remote_size: OSS-backed head, zero-filled tail.
  void verify_read_hole_in_clean_chunk_straddling_remote_size() {
    const uint64_t CS = fs_->options_.random_write_chunk_size;

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // Unaligned remote size so chunk 0 straddles remote_size.
    std::string filename = "rw_read_hole_straddle";
    std::string local_file = test_path_ + filename + ".src";
    const size_t kRemoteSize = 1536 * 1024;
    ASSERT_LT(kRemoteSize, CS);
    {
      ::unlink(local_file.c_str());
      int fd = ::open(local_file.c_str(), O_RDWR | O_CREAT, 0600);
      ASSERT_GE(fd, 0);
      std::string buf = random_string(kRemoteSize);
      ssize_t w = ::pwrite(fd, buf.data(), kRemoteSize, 0);
      ASSERT_EQ(w, (ssize_t)kRemoteSize);
      ::close(fd);
    }
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));
    auto read_src = [&](off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = ::pread(local_fd, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_src off=" << off;
      return s;
    };
    auto read_fh = [&](void *fh, off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = read_from_handle(fh, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_fh off=" << off << " len=" << len;
      return s;
    };

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Dirty only chunk 1 so chunk 0 stays CLEAN and straddling.
    const size_t kPatchSize = 4 * 1024;
    const off_t kPatchOff = static_cast<off_t>(CS + 4096);
    std::string patch = random_string(kPatchSize);
    auto w =
        pwrite_mirror(handle, local_fd, patch.data(), kPatchSize, kPatchOff);
    ASSERT_EQ(w, (ssize_t)kPatchSize);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_TRUE(inode->is_dirty);

    const std::string zeros4k(4096, '\0');
    const off_t kRemoteEnd = static_cast<off_t>(kRemoteSize);
    EXPECT_EQ(read_fh(handle, kRemoteEnd, 4096), zeros4k)
        << "tail past remote_size must be zero-filled";
    EXPECT_EQ(read_fh(handle, kRemoteEnd - 4096, 8192),
              read_src(kRemoteEnd - 4096, 4096) + zeros4k)
        << "mixed OSS + zero-fill read across the remote boundary";
    EXPECT_EQ(read_fh(handle, kPatchOff, kPatchSize), patch)
        << "dirty payload from staging";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // setattr(SIZE) resize on an OPEN file.
  void verify_truncate_resize_open() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_resize";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 8);  // 8 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto set_size = [&](off_t new_size) {
      struct stat sz = {};
      sz.st_size = new_size;
      ASSERT_EQ(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0);
      truncate_mirror(local_fd, new_size);
    };

    // Shrink to an unaligned size, then grow past it (hole must be zeros).
    set_size(3 * 1024 * 1024 + 777);
    set_size(6 * 1024 * 1024 + 13);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // truncate(path) on a NOT-open file.
  void verify_truncate_standalone() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_standalone";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);  // 4 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto set_size = [&](off_t new_size) {
      struct stat sz = {};
      sz.st_size = new_size;
      ASSERT_EQ(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0);
      truncate_mirror(local_fd, new_size);
    };

    // Shrink then grow, never opening the file; each setattr flushes remotely.
    set_size(1000 * 1024);  // shrink
    assert_remote_matches_local(filename, local_fd);

    set_size(3 * 1024 * 1024);  // grow with a hole
    assert_remote_matches_local(filename, local_fd);
  }

  // Shrink into the middle of the file, then pwrite past the new EOF. The
  // gap [new_size, write_off) must read back as zeros, NOT the stale remote
  // bytes that used to live there -- this exercises the remote_size cap.
  void verify_truncate_shrink_then_rewrite_hole() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_rewrite_hole";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 6);  // 6 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Shrink to 2 MiB, dropping remote bytes [2 MiB, 6 MiB).
    const off_t kNewSize = 2 * 1024 * 1024;
    struct stat sz = {};
    sz.st_size = kNewSize;
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);
    truncate_mirror(local_fd, kNewSize);

    // Write past a hole: [3 MiB, 3 MiB + 4 KiB). Region [2 MiB, 3 MiB) is a
    // hole that must be zeros, not the old remote content.
    const off_t kWriteOff = 3 * 1024 * 1024;
    const size_t kWriteSize = 4 * 1024;
    std::string data = random_string(kWriteSize);
    auto w =
        pwrite_mirror(handle, local_fd, data.data(), kWriteSize, kWriteOff);
    ASSERT_EQ(w, (ssize_t)kWriteSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Truncate-to-0 on an OPEN, dirty file must reset the shared staging without
  // returning EBUSY (the non-random-write path would reject this).
  void verify_truncate_to_zero_open_dirty() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_trunc_zero";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const size_t kSize = 512 * 1024;
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    struct stat sz = {};
    sz.st_size = 0;
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);
    truncate_mirror(local_fd, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Regression: setattr(SIZE) on a file opened O_RDWR but not yet written used
  // to mark the inode dirty via a transient (non-handle) writer, so the next
  // read was mis-routed through the random-write dirty path.
  void verify_truncate_open_clean_then_read() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_open_clean_read";
    std::string local_file = test_path_ + filename + ".src";
    const size_t kOrigSize = 4 * 1024 * 1024;  // 4 MiB remote object
    create_random_file(local_file, kOrigSize / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    int src_fd = ::open(local_file.c_str(), O_RDONLY);
    ASSERT_GE(src_fd, 0) << "open src errno=" << errno;
    DEFER(::close(src_fd));
    auto read_src = [&](off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = ::pread(src_fd, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_src off=" << off;
      return s;
    };
    auto read_fh = [&](void *fh, off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = read_from_handle(fh, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_fh off=" << off << " len=" << len;
      return s;
    };

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Open O_RDWR but do NOT write: rw_ctx exists, inode is clean.
    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // Grow via setattr on the open-but-clean file. The fix flushes
    // synchronously so the inode returns to CLEAN -- reads then use the
    // normal (non-dirty) OSS path.
    const off_t kGrownSize = 6 * 1024 * 1024;  // adds a 2 MiB zero-filled hole
    {
      struct stat sz = {};
      sz.st_size = kGrownSize;
      ASSERT_EQ(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0);
    }
    truncate_mirror(local_fd, kGrownSize);

    // The open handle is retained, but the inode is clean again (flushed).
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // Reads must NOT abort and must be correct: original bytes preserved, the
    // grown region reads back as zeros.
    const std::string zeros4k(4096, '\0');
    EXPECT_EQ(read_fh(handle, 0, 4096), read_src(0, 4096))
        << "original head after grow";
    EXPECT_EQ(read_fh(handle, static_cast<off_t>(kOrigSize) - 4096, 4096),
              read_src(static_cast<off_t>(kOrigSize) - 4096, 4096))
        << "original tail after grow";
    EXPECT_EQ(read_fh(handle, static_cast<off_t>(kOrigSize), 4096), zeros4k)
        << "grown hole must read as zeros";

    // Shrink to an unaligned size; still clean, reads stay correct.
    const off_t kShrunkSize = 1 * 1024 * 1024 + 777;
    {
      struct stat sz = {};
      sz.st_size = kShrunkSize;
      ASSERT_EQ(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0);
    }
    truncate_mirror(local_fd, kShrunkSize);
    ASSERT_FALSE(inode->is_dirty);
    EXPECT_EQ(read_fh(handle, 0, 4096), read_src(0, 4096))
        << "head after shrink";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Companion to the read case: after setattr(SIZE) on an open-but-clean file
  // flushes synchronously and clears the shared rw_ctx, the SAME handle must
  // stay usable for writes -- re-dirtying the ctx, writing
  // both inside the original range and into the grown hole, and flushing
  // correctly on release. Guards against the eager flush corrupting the shared
  // ctx (stale chunks / remote_size) while a write handle is still open.
  void verify_truncate_open_clean_then_write() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_open_clean_write";
    std::string local_file = test_path_ + filename + ".src";
    const size_t kOrigSize = 4 * 1024 * 1024;  // 4 MiB remote object
    create_random_file(local_file, kOrigSize / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Open O_RDWR but do NOT write: rw_ctx exists, inode is clean.
    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // Grow via setattr -> synchronous flush -> inode clean, rw_ctx retained.
    const off_t kGrownSize = 6 * 1024 * 1024;  // adds a 2 MiB zero-filled hole
    {
      struct stat sz = {};
      sz.st_size = kGrownSize;
      ASSERT_EQ(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0);
    }
    truncate_mirror(local_fd, kGrownSize);
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // The handle must still be writable. Write inside the original range...
    const size_t kPatch = 64 * 1024;
    std::string patch = random_string(kPatch);
    auto w =
        pwrite_mirror(handle, local_fd, patch.data(), kPatch, 1 * 1024 * 1024);
    ASSERT_EQ(w, (ssize_t)kPatch);
    // ...and into the grown hole [kOrigSize, kGrownSize) (rest stays zeros).
    std::string patch2 = random_string(kPatch);
    w = pwrite_mirror(handle, local_fd, patch2.data(), kPatch,
                      static_cast<off_t>(kOrigSize) + 512 * 1024);
    ASSERT_EQ(w, (ssize_t)kPatch);

    // Writing re-dirtied the inode.
    ASSERT_TRUE(inode->is_dirty);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);
    assert_remote_matches_local(filename, local_fd);
  }

  // Regression for the truncate crash: setattr(SIZE) on an open-but-clean
  // file runs a sync flush through a transient writer; if that flush fails
  // the inode is left dirty with no handle owning the dirtiness. Reads must
  // not abort (they route through rw_ctx/staging), must serve the truncated
  // view, and a later release must flush the truncation.
  void verify_truncate_open_clean_flush_failure_no_crash() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_open_clean_flush_fail";
    std::string local_file = test_path_ + filename + ".src";
    const size_t kOrigSize = 4 * 1024 * 1024;  // 4 MiB remote object
    create_random_file(local_file, kOrigSize / (1024 * 1024));
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    int src_fd = ::open(local_file.c_str(), O_RDONLY);
    ASSERT_GE(src_fd, 0) << "open src errno=" << errno;
    DEFER(::close(src_fd));
    auto read_src = [&](off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = ::pread(src_fd, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_src off=" << off;
      return s;
    };
    auto read_fh = [&](void *fh, off_t off, size_t len) {
      std::string s(len, '\0');
      ssize_t g = read_from_handle(fh, s.data(), len, off);
      EXPECT_EQ(g, (ssize_t)len) << "read_fh off=" << off << " len=" << len;
      return s;
    };

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Open O_RDWR but do NOT write: rw_ctx exists and the inode is clean.
    // This is the input state the transient-writer truncate assumes.
    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // Shrink via setattr with OSS broken: the transient writer marks the
    // inode dirty, then its sync flush fails.
    const off_t kShrunkSize = 3 * 1024 * 1024;
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    {
      struct stat sz = {};
      sz.st_size = kShrunkSize;
      ASSERT_NE(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0)
          << "setattr must propagate the flush failure";
    }
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // The failed flush leaves the inode dirty with no handle owning the
    // dirtiness (random mode carries it all in rw_ctx) -- exactly the state
    // that used to abort the next read.
    ASSERT_TRUE(inode->is_dirty);
    ASSERT_NE(inode->rw_ctx, nullptr);

    // The truncation was not persisted remotely.
    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(kOrigSize), meta["Content-Length"]);

    // Reads must NOT abort and must serve the truncated local view: the kept
    // prefix matches the original bytes and EOF sits at the new size.
    truncate_mirror(local_fd, kShrunkSize);
    EXPECT_EQ(read_fh(handle, 0, 4096), read_src(0, 4096))
        << "head after failed shrink";
    EXPECT_EQ(read_fh(handle, kShrunkSize - 4096, 4096),
              read_src(kShrunkSize - 4096, 4096))
        << "tail at the new size after failed shrink";
    {
      char over = 0;
      ssize_t g = read_from_handle(handle, &over, 1, kShrunkSize);
      EXPECT_EQ(g, 0) << "read past the truncated size must hit EOF";
    }

    // The residual dirty state is still flushable: release re-uploads the
    // truncated object successfully.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);
    assert_remote_matches_local(filename, local_fd);
  }

  // flush() releases the local staging blocks once the data is durable in OSS.
  // Repeatedly flush a multi-chunk file, then overwrite already-flushed
  // regions: each flush must empty staging (on-disk blocks and global usage
  // both zero), and every read afterwards must stay byte-for-byte consistent
  // with a local mirror. Sub-chunk overwrites into flushed regions force
  // GET-on-write to refill the freshly-emptied staging from OSS, exercising the
  // round-trip.
  void verify_flush_releases_staging_and_stays_consistent() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_flush_release_consistency";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    ASSERT_EQ(CS, 2u * 1024 * 1024) << "test assumes default 2MiB chunk";
    const size_t kFileSize = 5 * 1024 * 1024 + 512 * 1024;  // spans 3+ chunks

    // Verify the whole file through a FRESH read handle (not the write handle,
    // whose reader caches remote_size from open time). Reads may return
    // partially, so loop over 256 KiB slices and compare each to the mirror.
    // A fresh handle reflects the current inode state: clean chunks stream from
    // OSS, dirty chunks (refilled by GET-on-write) stream from staging.
    // TODO: also verify correctness by reading back through the write handle
    // itself (write -> fsync -> read on the same handle), once the reader's
    // stale remote_size after in-place flush is fixed.
    auto read_all_and_compare = [&](const char *phase) {
      void *rh = nullptr;
      bool unused = false;
      ASSERT_EQ(fs_->open(nodeid, O_RDONLY, &rh, &unused), 0)
          << phase << ": open read handle";
      DEFER(fs_->release(nodeid, get_file_from_handle(rh)));

      const size_t kSlice = 256 * 1024;
      std::string got(kSlice, '\0');
      std::string want(kSlice, '\0');
      for (size_t off = 0; off < kFileSize;) {
        size_t len = std::min(kSlice, kFileSize - off);
        ssize_t n = read_from_handle(rh, &got[0], len, static_cast<off_t>(off));
        ASSERT_GT(n, 0) << phase << ": read at " << off << " returned " << n;
        ssize_t m = ::pread(local_fd, &want[0], n, static_cast<off_t>(off));
        ASSERT_EQ(m, n) << phase << ": mirror short read at " << off;
        ASSERT_EQ(0, memcmp(got.data(), want.data(), n))
            << phase << ": data mismatch at " << off;
        off += n;
      }
    };

    // Initial full population in 256 KiB slices (tail is chunk-unaligned).
    {
      const size_t kSlice = 256 * 1024;
      std::string data = random_string(kFileSize);
      for (size_t off = 0; off < kFileSize; off += kSlice) {
        size_t len = std::min(kSlice, kFileSize - off);
        auto w = pwrite_mirror(handle, local_fd, data.data() + off, len,
                               static_cast<off_t>(off));
        ASSERT_EQ(w, (ssize_t)len);
      }
    }

    for (int round = 0; round < 5; ++round) {
      r = fsync_file_handle(handle, /*datasync=*/true);
      ASSERT_EQ(r, 0) << "round " << round << ": fsync failed";

      EXPECT_EQ(staging_fd_disk_bytes(handle), 0u)
          << "round " << round << ": staging not released after flush";
      EXPECT_EQ(staging_disk_usage(), 0u)
          << "round " << round << ": global staging usage nonzero after flush";

      assert_remote_matches_local(filename, local_fd);
      read_all_and_compare("post-flush");

      // Sub-chunk overwrites into already-flushed regions force GET-on-write
      // to re-fetch the chunk from OSS into the freshly-emptied staging.
      const size_t kPatch = 100 * 1024;  // sub-chunk, unaligned
      for (int p = 0; p < 3; ++p) {
        // 640KiB stride lands each patch in a different chunk; the +12345
        // offset keeps every write chunk-unaligned. Wrap to 0 near EOF.
        off_t off = static_cast<off_t>((round + p) * 640 * 1024 + 12345);
        if (off + static_cast<off_t>(kPatch) > static_cast<off_t>(kFileSize)) {
          off = 0;
        }
        std::string patch = random_string(kPatch);
        auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatch, off);
        ASSERT_EQ(w, (ssize_t)kPatch);
      }

      EXPECT_GT(staging_fd_disk_bytes(handle), 0u)
          << "round " << round << ": staging empty after overwrite";
      read_all_and_compare("mid-cycle");
    }

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Non-last writer closes with flush failure: the inode must stay dirty for
  // retry via the remaining handle.
  void verify_flush_failure_multi_writer_keeps_dirty_for_retry() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_multi_writer_flush_fail";
    uint64_t nodeid = 0;
    void *handle_a = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fsync_file_handle(handle_a, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_a)->get_inode());
    ASSERT_NE(inode, nullptr);

    const size_t kSizeA = 128 * 1024;
    std::string data_a = random_string(kSizeA);
    auto wa = pwrite_mirror(handle_a, local_fd, data_a.data(), kSizeA, 0);
    ASSERT_EQ(wa, (ssize_t)kSizeA);
    ASSERT_TRUE(inode->is_dirty);

    void *handle_b = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_b, &unused);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx->ref_count, 2);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_NE(r, 0);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    ASSERT_TRUE(inode->is_dirty) << "inode must stay dirty for retry";
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_EQ(inode->rw_ctx->ref_count, 1);

    // The remaining handle retries the flush on release.
    const size_t kSizeB = 64 * 1024;
    std::string data_b = random_string(kSizeB);
    auto wb = pwrite_mirror(handle_b, local_fd, data_b.data(), kSizeB,
                            static_cast<off_t>(kSizeA));
    ASSERT_EQ(wb, (ssize_t)kSizeB);

    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    assert_remote_matches_local(filename, local_fd);
  }

  // Rename on a dirty random-write file: dirty_fh is never set in random
  // mode, so rename must flush staged data via a transient writer.
  void verify_rename_flushes_via_transient_writer() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_rename_transient_flush";
    uint64_t nodeid = 0;
    void *handle_a = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle_a);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fsync_file_handle(handle_a, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_a)->get_inode());

    const size_t kSize = 128 * 1024;
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle_a, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_TRUE(inode->is_dirty);

    void *handle_b = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_b, &unused);
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_NE(r, 0);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    ASSERT_TRUE(inode->is_dirty);
    ASSERT_NE(inode->rw_ctx, nullptr);

    std::string new_filename = "rw_rename_transient_flush_new";
    r = fs_->rename(parent, filename.c_str(), parent, new_filename.c_str(), 0);
    ASSERT_EQ(r, 0) << "rename must flush via transient writer";
    ASSERT_FALSE(inode->is_dirty);

    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->rw_ctx, nullptr);

    auto meta = get_file_meta(new_filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(kSize), meta["Content-Length"]);
    uint64_t expected_crc = cal_crc64(0, (void *)data.data(), kSize);
    ASSERT_EQ(std::to_string(expected_crc), meta["X-Oss-Hash-Crc64ecma"]);
  }

  // A wholly-failed first pwrite must roll the inode back to clean; otherwise
  // is_dirty stays set and subsequent reads are mis-routed through the
  // random-write dirty path.
  void verify_failed_first_write_rolls_back_clean() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    ASSERT_EQ(CS % 1024, 0u);

    std::string filename = "rw_failed_first_write";
    std::string local_file = test_path_ + filename + ".src";
    // Two full chunks: a CS write at 0 wholly covers chunk 0 (no GET-on-write).
    create_random_file(local_file, /*size_MB=*/0, /*offset=*/(2 * CS) / 1024);
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_FALSE(inode->is_dirty);

    // Fail the first staging write (the user-data pwrite of chunk 0).
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));
    std::string data = random_string(CS);
    ssize_t w = write_to_file_handle(handle, data.data(), CS, 0);
    g_fault_injector->clear_injection(FaultInjectionId::FI_Pwrite_Staging_Fail);
    ASSERT_LT(w, 0) << "injected staging write must fail with an error code";

    // Nothing persisted, so the inode must roll back to clean (pre-fix: stuck
    // dirty with an empty chunk map).
    ASSERT_FALSE(inode->is_dirty);
    ASSERT_EQ(inode->rw_ctx->chunks.dirty_chunk_count(), 0u);

    // Read a CLEAN region; pre-fix this was mis-routed through the
    // random-write dirty path.
    const size_t kRead = 64 * 1024;
    std::string got(kRead, '\0');
    ssize_t rd = read_from_handle(handle, got.data(), kRead, 0);
    ASSERT_EQ(rd, (ssize_t)kRead);
    std::string want(kRead, '\0');
    ssize_t mr = ::pread(local_fd, want.data(), kRead, 0);
    ASSERT_EQ(mr, (ssize_t)kRead);
    ASSERT_EQ(got, want) << "read must return original remote data";

    // No dirty state means release re-uploads nothing; remote stays original.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(inode->is_dirty);
    assert_remote_matches_local(filename, local_fd);
  }

  // After flush (mark_clean), cache must be dropped so that a read handle with
  // stale cached data fetches fresh data from OSS instead of serving old cache.
  void verify_cache_dropped_on_mark_clean() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // Upload 4 MiB file with known OLD content.
    std::string filename = "rw_cache_drop_mark_clean";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, /*size_MB=*/4);
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    int local_fd = ::open(local_file.c_str(), O_RDWR);
    ASSERT_GE(local_fd, 0);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const off_t kTargetOff = 1024 * 1024;  // 1 MiB offset
    const size_t kPatchSize = 64 * 1024;   // 64 KiB
    std::string old_bytes(kPatchSize, '\0');
    std::string got(kPatchSize, '\0');

    // Step 1: open read handle, warm cache at target with OLD data.
    void *handle_r = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDONLY, &handle_r, &unused);
    ASSERT_EQ(r, 0);

    ssize_t n =
        read_from_handle(handle_r, old_bytes.data(), kPatchSize, kTargetOff);
    ASSERT_EQ(n, (ssize_t)kPatchSize) << "warm-up read failed";

    // Step 2: open write handle, overwrite target with NEW data.
    std::string new_bytes = random_string(kPatchSize);
    ASSERT_NE(new_bytes, old_bytes);

    // Mirror write to local file.
    ssize_t w = ::pwrite(local_fd, new_bytes.data(), kPatchSize, kTargetOff);
    ASSERT_EQ(w, (ssize_t)kPatchSize);

    void *handle_w = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_w, &unused);
    ASSERT_EQ(r, 0);

    w = write_to_file_handle(handle_w, new_bytes.data(), kPatchSize,
                             kTargetOff);
    ASSERT_EQ(w, (ssize_t)kPatchSize);

    // Step 3: release write handle -> flush -> mark_clean -> cache drop.
    r = fs_->release(nodeid, get_file_from_handle(handle_w));
    ASSERT_EQ(r, 0);

    FileInode *inode =
        static_cast<FileInode *>(get_file_from_handle(handle_r)->get_inode());
    ASSERT_FALSE(inode->is_dirty) << "inode should be clean after flush";
    EXPECT_FALSE(inode->etag.empty()) << "etag should be updated after flush";

    // Step 4: read on read handle. Without cache drop in mark_clean, this
    // would return the stale cached OLD data. With the fix, it must fetch
    // fresh NEW data from OSS.
    got.assign(kPatchSize, '\0');
    n = read_from_handle(handle_r, got.data(), kPatchSize, kTargetOff);
    ASSERT_EQ(n, (ssize_t)kPatchSize) << "post-flush read failed";
    EXPECT_EQ(got, new_bytes) << "post-flush read returned stale cached data; "
                                 "mark_clean() cache drop did not work";

    r = fs_->release(nodeid, get_file_from_handle(handle_r));
    ASSERT_EQ(r, 0);
  }

  // Regression: after a rename, truncate must refresh rw_ctx->upload_path,
  // else its sync flush re-creates the object at the stale pre-rename path.
  void verify_truncate_after_rename_uses_new_path() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string src_name = "rw_trunc_rename_src";
    std::string dst_name = "rw_trunc_rename_dst";
    std::string local_file = test_path_ + src_name + ".src";
    create_random_file(local_file, 4);  // 4 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + src_name,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, src_name.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Open O_RDWR so rw_ctx holds the src path.
    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);

    // Dirty the file so rename flushes it clean but keeps rw_ctx (and its
    // stale upload_path) alive while the handle stays open.
    const size_t kPatch = 64 * 1024;
    std::string patch = random_string(kPatch);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatch, 0);
    ASSERT_EQ(w, (ssize_t)kPatch);
    ASSERT_TRUE(inode->is_dirty);

    r = fs_->rename(parent, src_name.c_str(), parent, dst_name.c_str(), 0);
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(inode->is_dirty);  // rename flushed the dirty data
    ASSERT_NE(inode->rw_ctx, nullptr);

    // Truncate via setattr; truncate() must propagate the new path to rw_ctx.
    const off_t kNewSize = 2 * 1024 * 1024;
    {
      struct stat sz = {};
      sz.st_size = kNewSize;
      ASSERT_EQ(fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE), 0);
    }
    truncate_mirror(local_fd, kNewSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Renamed object holds the truncated content; nothing left at the old path.
    assert_remote_matches_local(dst_name, local_fd);
    auto src_meta = get_file_meta(src_name, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", src_meta["Content-Length"])
        << "truncate flushed to the stale pre-rename path";
  }

  // Regression: reopening with O_TRUNC after a rename must refresh
  // upload_path, else the flush uploads to the stale pre-rename path.
  void verify_o_trunc_after_rename_uses_new_path() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string src_name = "rw_otrunc_rename_src";
    std::string dst_name = "rw_otrunc_rename_dst";
    std::string local_file = test_path_ + src_name + ".src";
    create_random_file(local_file, 4);  // 4 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + src_name,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, src_name.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // First handle keeps rw_ctx alive across the rename.
    void *handle_a = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_a, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_a)->get_inode());

    // Dirty the file so rename flushes it clean but keeps rw_ctx (and its
    // stale upload_path) alive while the handle stays open.
    const size_t kPatch = 64 * 1024;
    std::string patch = random_string(kPatch);
    auto w = pwrite_mirror(handle_a, local_fd, patch.data(), kPatch, 0);
    ASSERT_EQ(w, (ssize_t)kPatch);
    r = fs_->rename(parent, src_name.c_str(), parent, dst_name.c_str(), 0);
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(inode->is_dirty);
    ASSERT_NE(inode->rw_ctx, nullptr);

    void *handle_b = nullptr;
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle_b, &unused);
    ASSERT_EQ(r, 0);
    truncate_mirror(local_fd, 0);

    const size_t kSize = 128 * 1024;
    std::string data = random_string(kSize);
    w = pwrite_mirror(handle_b, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_EQ(r, 0);

    // The renamed object holds the new content; nothing at the old path.
    assert_remote_matches_local(dst_name, local_fd);
    auto src_meta = get_file_meta(src_name, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", src_meta["Content-Length"])
        << "O_TRUNC flush went to the stale pre-rename path";
  }

  // Regression (close-to-open): while a handle keeps rw_ctx alive, another
  // client rewrites the remote object. Reopening must refresh the stale
  // remote_size snapshot, else GET-on-write treats the grown region as a
  // hole and the flush zeros the remote bytes between the old and the new
  // size.
  void verify_close_to_open_refreshes_remote_size() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_cto_remote_size";
    std::string remote_path = parent_path + std::string("/") + filename;

    // v1: 4 MiB remote object.
    std::string local_a = test_path_ + filename + ".a";
    create_random_file(local_a, 4);
    ASSERT_EQ(upload_file(local_a, remote_path, FLAGS_oss_bucket_prefix), 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // First handle creates rw_ctx with remote_size == 4 MiB and stays open
    // across the external rewrite below.
    void *handle_a = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_a, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_a)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);

    // Another client rewrites the object to a different 8 MiB version.
    std::string local_b = test_path_ + filename + ".b";
    create_random_file(local_b, 8);
    ASSERT_EQ(upload_file(local_b, remote_path, FLAGS_oss_bucket_prefix), 0);

    // The reopen's close-to-open HEAD observes the new remote version.
    void *handle_b = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_b, &unused);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->attr.size, 8ULL * 1024 * 1024);

    // Mirror file B, then patch offset 6 MiB: inside v2 but beyond the stale
    // 4 MiB snapshot.
    int local_fd = reopen_mirror(local_b);
    DEFER(::close(local_fd));
    const size_t kPatch = 64 * 1024;
    std::string patch = random_string(kPatch);
    auto w = pwrite_mirror(handle_b, local_fd, patch.data(), kPatch,
                           6 * 1024 * 1024);
    ASSERT_EQ(w, (ssize_t)kPatch);

    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_EQ(r, 0);

    // Remote must keep all of v2's bytes plus the patch.
    assert_remote_matches_local(filename, local_fd);
  }

  // Regression (close-to-open): same bug, remote shrunk instead of grown.
  // The stale snapshot makes GET-on-write fetch a range beyond the real
  // EOF, so writes into the stale-valid region fail until the snapshot is
  // refreshed.
  void verify_close_to_open_refreshes_remote_size_shrink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_cto_remote_shrink";
    std::string remote_path = parent_path + std::string("/") + filename;

    // v1: 8 MiB remote object.
    std::string local_a = test_path_ + filename + ".a";
    create_random_file(local_a, 8);
    ASSERT_EQ(upload_file(local_a, remote_path, FLAGS_oss_bucket_prefix), 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // First handle creates rw_ctx with remote_size == 8 MiB and stays open
    // across the external rewrite below.
    void *handle_a = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_a, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle_a)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);

    // Another client rewrites the object to a shorter 4 MiB version.
    std::string local_b = test_path_ + filename + ".b";
    create_random_file(local_b, 4);
    ASSERT_EQ(upload_file(local_b, remote_path, FLAGS_oss_bucket_prefix), 0);

    // The reopen's close-to-open HEAD observes the new remote version.
    void *handle_b = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_b, &unused);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->attr.size, 4ULL * 1024 * 1024);

    // Mirror file B, then write at offset 5 MiB: a hole under the true
    // 4 MiB remote, but inside the stale 8 MiB snapshot.
    int local_fd = reopen_mirror(local_b);
    DEFER(::close(local_fd));
    const size_t kPatch = 64 * 1024;
    std::string patch = random_string(kPatch);
    auto w = pwrite_mirror(handle_b, local_fd, patch.data(), kPatch,
                           5 * 1024 * 1024);
    ASSERT_EQ(w, (ssize_t)kPatch);

    r = fs_->release(nodeid, get_file_from_handle(handle_b));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle_a));
    ASSERT_EQ(r, 0);

    // Remote must be v2 plus the zero-filled hole and the patch.
    assert_remote_matches_local(filename, local_fd);
  }

  // Regression: a getattr that refreshes attr.size must also resync
  // rw_ctx->remote_size, else the next flush zero-fills [old, new size).
  void verify_getattr_refreshes_remote_size_without_reopen() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_getattr_remote_size";
    std::string remote_path = parent_path + std::string("/") + filename;

    std::string local_a = test_path_ + filename + ".a";
    create_random_file(local_a, 4);
    ASSERT_EQ(upload_file(local_a, remote_path, FLAGS_oss_bucket_prefix), 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Stays open across the external rewrite below.
    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_EQ(inode->rw_ctx->remote_size, 4ULL * 1024 * 1024);

    // Another client rewrites the object to a larger version.
    std::string local_b = test_path_ + filename + ".b";
    create_random_file(local_b, 8);
    ASSERT_EQ(upload_file(local_b, remote_path, FLAGS_oss_bucket_prefix), 0);

    struct stat gst = {};
    r = fs_->getattr(nodeid, &gst);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(inode->attr.size, 8ULL * 1024 * 1024);
    ASSERT_FALSE(inode->is_dirty);
    EXPECT_EQ(inode->rw_ctx->remote_size, 8ULL * 1024 * 1024)
        << "rw_ctx->remote_size not resynced after getattr refresh";

    // Patch the tail: inside v2 but beyond the stale 4 MiB snapshot. Writing
    // at EOF extends staging to attr.size so the flush succeeds and silently
    // zero-fills the gap instead of failing.
    int local_fd = reopen_mirror(local_b);
    DEFER(::close(local_fd));
    const size_t kPatch = 64 * 1024;
    std::string patch = random_string(kPatch);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatch,
                           8 * 1024 * 1024 - kPatch);
    ASSERT_EQ(w, (ssize_t)kPatch);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Remote must keep all of v2's bytes plus the patch.
    assert_remote_matches_local(filename, local_fd);
  }

  // Regression: GET-on-write against a remotely EMPTIED object must fail and
  // roll back, not silently zero-fill the chunk and upload zeros later.
  void verify_get_on_write_against_emptied_remote() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_remote_emptied";
    std::string remote_path = parent_path + std::string("/") + filename;

    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 1);  // 1 MiB
    ASSERT_EQ(upload_file(local_file, remote_path, FLAGS_oss_bucket_prefix), 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Stays open across the external replacement below.
    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_EQ(inode->rw_ctx->remote_size, 1ULL * 1024 * 1024);

    std::string empty_file = test_path_ + filename + ".empty";
    create_zero_file(empty_file, 0);
    ASSERT_EQ(upload_file(empty_file, remote_path, FLAGS_oss_bucket_prefix), 0);

    // Partial write into CLEAN chunk 0 triggers GET-on-write on the emptied
    // object (stale remote_size still says 1 MiB).
    const size_t kPatch = 1024;
    std::string patch = random_string(kPatch);
    ssize_t w = write_to_file_handle(handle, patch.data(), kPatch, 0);
    ASSERT_EQ(w, -EINVAL);
    ASSERT_FALSE(inode->is_dirty);
    EXPECT_EQ(inode->rw_ctx->chunks.dirty_chunk_count(), 0u);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Remote must still be the empty object the other client created.
    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(meta["Content-Length"], "0");
  }

  // Regression: a short write into a whole-covered CLEAN chunk must fail
  // without marking it DIRTY or altering visible data.
  void verify_short_write_fails_without_marking_dirty() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_short_write_repair";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);  // 4 MiB -> 2 chunks (2 MiB each)
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    std::string data = random_string(CS);  // whole-cover overwrite of chunk 0

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Short_Write,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/1));

    ssize_t w = write_to_file_handle(handle, data.data(), CS, 0);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Pwrite_Staging_Short_Write);
    g_fault_injector->clear_injection(FaultInjectionId::FI_Pwrite_Staging_Fail);

    ASSERT_LT(w, 0) << "short staging write must fail the whole pwrite";

    // Chunk 0 must stay CLEAN; reads still return the original bytes.
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_FALSE(inode->rw_ctx->chunks.is_dirty(0));

    std::string got(static_cast<size_t>(CS), '\0');
    ssize_t n = read_from_handle(handle, got.data(), CS, 0);
    ASSERT_EQ(n, (ssize_t)CS);
    std::string orig(static_cast<size_t>(CS), '\0');
    ssize_t mr = ::pread(local_fd, &orig[0], CS, 0);
    ASSERT_EQ(mr, (ssize_t)CS);
    EXPECT_EQ(got, orig) << "failed short write altered visible data";

    auto w2 = pwrite_mirror(handle, local_fd, data.data(), CS, 0);
    ASSERT_EQ(w2, (ssize_t)CS);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Regression: a failed write must leave GET-on-write fetched chunks CLEAN.
  void verify_failed_write_on_dirty_file_keeps_fetched_chunk_clean() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_dirty_failed_fetch";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);  // 4 MiB -> 2 chunks (2 MiB each)
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    const uint64_t CS = fs_->options_.random_write_chunk_size;

    // Make the file DIRTY: patch chunk 0 (partial cover -> GET-on-write).
    const size_t kPatch = 512;
    const off_t kPatchOff = 1024;
    std::string patch = random_string(kPatch);
    auto w0 = pwrite_mirror(handle, local_fd, patch.data(), kPatch, kPatchOff);
    ASSERT_EQ(w0, (ssize_t)kPatch);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty);
    ASSERT_TRUE(inode->rw_ctx->chunks.is_dirty(0));

    // Short-write then fail the next write after chunk 1 is fetched.
    patch = random_string(kPatch);
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Short_Write,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Pwrite_Staging_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/1));
    ssize_t w = write_to_file_handle(handle, patch.data(), kPatch,
                                     CS + kPatchOff);  // chunk 1
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Pwrite_Staging_Short_Write);
    g_fault_injector->clear_injection(FaultInjectionId::FI_Pwrite_Staging_Fail);
    ASSERT_LT(w, 0) << "staging write failure must fail the whole pwrite";

    // Chunk 1 must stay CLEAN despite the partial staging write.
    ASSERT_TRUE(inode->is_dirty);
    ASSERT_TRUE(inode->rw_ctx->chunks.is_dirty(0));
    ASSERT_FALSE(inode->rw_ctx->chunks.is_dirty(1));

    // Both regions still read correct content: chunk 0 from staging, chunk 1
    // from OSS.
    std::string got(static_cast<size_t>(CS), '\0');
    std::string orig(static_cast<size_t>(CS), '\0');
    ssize_t n = read_from_handle(handle, got.data(), CS, 0);
    ASSERT_EQ(n, (ssize_t)CS);
    ASSERT_EQ(::pread(local_fd, &orig[0], CS, 0), (ssize_t)CS);
    EXPECT_EQ(got, orig) << "dirty chunk 0 content must survive";

    n = read_from_handle(handle, got.data(), CS, static_cast<off_t>(CS));
    ASSERT_EQ(n, (ssize_t)CS);
    ASSERT_EQ(::pread(local_fd, &orig[0], CS, CS), (ssize_t)CS);
    EXPECT_EQ(got, orig) << "clean chunk 1 must still read the OSS content";

    // Retry succeeds and chunk 1 becomes dirty with the full patch.
    auto w2 =
        pwrite_mirror(handle, local_fd, patch.data(), kPatch, CS + kPatchOff);
    ASSERT_EQ(w2, (ssize_t)kPatch);
    ASSERT_TRUE(inode->rw_ctx->chunks.is_dirty(1));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // GET-on-write failure: clean file rolls back, dirty file keeps prior
  // data; retry succeeds in both cases.
  void verify_get_chunk_fail_on_write_is_retryable() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_get_chunk_fail";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);  // 4 MiB -> 2 chunks (2 MiB each)
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

    // ── Phase A: GET failure on a clean file ──
    const size_t kSize = 64 * 1024;
    std::string data = random_string(kSize);
    g_fault_injector->set_injection(
        FaultInjectionId::FI_RandomWrite_Get_Chunk_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));

    ssize_t w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_LT(w, 0) << "GET-on-write failure must fail the pwrite";

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_RandomWrite_Get_Chunk_Fail);

    ASSERT_FALSE(inode->is_dirty) << "failed write must roll a clean file back";
    ASSERT_EQ(inode->rw_ctx->chunks.dirty_chunk_count(), 0u);

    std::string got(kSize, '\0');
    ssize_t n = read_from_handle(handle, got.data(), kSize, 0);
    ASSERT_EQ(n, (ssize_t)kSize);
    std::string orig(kSize, '\0');
    ASSERT_EQ(::pread(local_fd, &orig[0], kSize, 0), (ssize_t)kSize);
    EXPECT_EQ(got, orig) << "failed GET-on-write altered visible data";

    auto w2 = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w2, (ssize_t)kSize);

    // ── Phase B: GET failure on an already-dirty file ──
    // Chunk 0 is now dirty; a partial write into chunk 1 needs a GET.
    const uint64_t CS = fs_->options_.random_write_chunk_size;
    const off_t kChunk1Off = static_cast<off_t>(CS + 32 * 1024);
    std::string data2 = random_string(kSize);
    g_fault_injector->set_injection(
        FaultInjectionId::FI_RandomWrite_Get_Chunk_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));

    w = write_to_file_handle(handle, data2.data(), kSize, kChunk1Off);
    ASSERT_LT(w, 0);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_RandomWrite_Get_Chunk_Fail);

    ASSERT_TRUE(inode->is_dirty) << "prior dirty state must survive";
    ASSERT_TRUE(inode->rw_ctx->chunks.is_dirty(0));
    ASSERT_FALSE(inode->rw_ctx->chunks.is_dirty(1));

    got.assign(kSize, '\0');
    n = read_from_handle(handle, got.data(), kSize, 0);
    ASSERT_EQ(n, (ssize_t)kSize);
    EXPECT_EQ(got, data) << "failed write clobbered earlier dirty data";

    w2 = pwrite_mirror(handle, local_fd, data2.data(), kSize, kChunk1Off);
    ASSERT_EQ(w2, (ssize_t)kSize);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Init failure: flush fails without touching OSS, dirty state preserved,
  // retry succeeds.
  void verify_init_multipart_fail_is_retryable() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_init_multipart_fail";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    // Write > upload_buffer_size (1 MiB) to force the multipart path.
    const size_t kSize = 1536 * 1024;
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_RandomWrite_Init_Multipart_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_NE(r, 0) << "flush must propagate init_multipart failure";

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty)
        << "inode must remain dirty after init failure";
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_GT(inode->rw_ctx->chunks.dirty_chunk_count(), 0u);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_RandomWrite_Init_Multipart_Fail);

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "retry flush must succeed after fault cleared";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Complete failure after all parts uploaded: abort runs, error propagated,
  // dirty state preserved, retry succeeds.
  void verify_complete_multipart_fail_aborts_and_retries() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_complete_multipart_fail";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    const size_t kSize = 1536 * 1024;
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_RandomWrite_Complete_Multipart_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_NE(r, 0) << "flush must propagate complete_multipart failure";

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_dirty)
        << "inode must remain dirty after complete failure";
    ASSERT_NE(inode->rw_ctx, nullptr);
    ASSERT_GT(inode->rw_ctx->chunks.dirty_chunk_count(), 0u);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_RandomWrite_Complete_Multipart_Fail);

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "retry flush must succeed after fault cleared";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Bogus-offset appends must land at EOF, also after in-place writes.
  void verify_o_append_offsets_to_eof() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_o_append";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 3);  // 3 MiB spans multiple chunks
    int r = upload_file(local_file, parent_path + std::string("/") + filename,
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    const uint64_t CS = fs_->options_.random_write_chunk_size;
    off_t eof = st.st_size;

    // CS + 4 KiB crosses a chunk boundary to exercise GET-on-write.
    std::string data1 = random_string(CS + 4096);
    ssize_t w = write_to_file_handle(handle, data1.data(), data1.size(), 0);
    ASSERT_EQ(w, (ssize_t)data1.size());
    ASSERT_EQ(::pwrite(local_fd, data1.data(), data1.size(), eof),
              (ssize_t)data1.size());

    std::string got(data1.size(), '\0');
    ssize_t n = read_from_handle(handle, got.data(), got.size(), eof);
    ASSERT_EQ(n, (ssize_t)got.size());
    EXPECT_EQ(got, data1) << "append with offset 0 did not land at EOF";
    eof += static_cast<off_t>(data1.size());

    std::string data2 = random_string(64 * 1024);
    w = write_to_file_handle(handle, data2.data(), data2.size(), 1234);
    ASSERT_EQ(w, (ssize_t)data2.size());
    ASSERT_EQ(::pwrite(local_fd, data2.data(), data2.size(), eof),
              (ssize_t)data2.size());

    got.assign(data2.size(), '\0');
    n = read_from_handle(handle, got.data(), got.size(), eof);
    ASSERT_EQ(n, (ssize_t)got.size());
    EXPECT_EQ(got, data2) << "append with mid-file offset did not land at EOF";
    eof += static_cast<off_t>(data2.size());

    // In-place write via a second handle; appends must still hit EOF.
    void *handle_rw = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle_rw, &unused);
    ASSERT_EQ(r, 0);

    const size_t kPatch = 512;
    const off_t kPatchOff = 1024;
    std::string patch = random_string(kPatch);
    w = pwrite_mirror(handle_rw, local_fd, patch.data(), kPatch, kPatchOff);
    ASSERT_EQ(w, (ssize_t)kPatch);

    std::string data3 = random_string(8 * 1024);
    w = write_to_file_handle(handle, data3.data(), data3.size(), kPatchOff);
    ASSERT_EQ(w, (ssize_t)data3.size());
    ASSERT_EQ(::pwrite(local_fd, data3.data(), data3.size(), eof),
              (ssize_t)data3.size());
    eof += static_cast<off_t>(data3.size());

    got.resize(static_cast<size_t>(eof));
    n = read_from_handle(handle, got.data(), got.size(), 0);
    ASSERT_EQ(n, (ssize_t)got.size());
    std::string orig(got.size(), '\0');
    ASSERT_EQ(::pread(local_fd, &orig[0], orig.size(), 0),
              (ssize_t)orig.size());
    EXPECT_EQ(got, orig) << "file content diverged from append-only mirror";

    r = fs_->release(nodeid, get_file_from_handle(handle_rw));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }
  // 100 KB base part size → 1 GiB needs 10486 parts > 10000, forcing
  // enlargement.
  void verify_dynamic_part_size_enlargement() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_dyn_part_enlarge";
    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    void *handle = nullptr;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    write_repeating_data(handle, local_fd, 1024ULL * 1024 * 1024);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0) << "flush must enlarge the part size instead of EFBIG";
    assert_remote_matches_local(filename, local_fd);
  }

  // Same enlargement; pre-populated 1 GiB remote with two dirty patches
  // exercises upload_part_copy + upload_part mixed path.
  void verify_dynamic_part_size_copy_mix() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    const uint64_t kFileSize = 1024ULL * 1024 * 1024;
    std::string filename = "rw_dyn_part_copy_mix";
    std::string local_file = test_path_ + filename + ".src";
    {
      ::unlink(local_file.c_str());
      int fd = ::open(local_file.c_str(), O_RDWR | O_CREAT, 0600);
      ASSERT_GE(fd, 0);
      const size_t kBufSize = 1024 * 1024;
      std::string buf = random_string(kBufSize);
      for (uint64_t off = 0; off < kFileSize; off += kBufSize) {
        ssize_t w = ::pwrite(fd, buf.data(), kBufSize, static_cast<off_t>(off));
        ASSERT_EQ(w, static_cast<ssize_t>(kBufSize));
      }
      ::close(fd);
    }
    int rr = upload_file(local_file, parent_path + "/" + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    std::string patch = random_string(4096);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), patch.size(),
                           static_cast<off_t>(256 * 1024));
    ASSERT_EQ(w, static_cast<ssize_t>(patch.size()));
    w = pwrite_mirror(handle, local_fd, patch.data(), patch.size(),
                      static_cast<off_t>(kFileSize - 8 * 1024));
    ASSERT_EQ(w, static_cast<ssize_t>(patch.size()));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // Unlink a dirty random-write file while it is open: the file is hidden
  // (dirty data flushed to the ".fuse_hiddenXXX" object) instead of being
  // deleted. Further writes still land in staging, but the last release must
  // mark the inode stale before close (flush skipped, data discarded), then
  // delete the hidden object and release all staging blocks.
  void verify_unlink_dirty_randwrite_discards_flush() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_unlink_dirty";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

    // Dirty the file, then unlink it while the handle is still open.
    const size_t kSizeA = 128 * 1024;
    std::string data_a = random_string(kSizeA);
    auto w = pwrite_mirror(handle, local_fd, data_a.data(), kSizeA, 0);
    ASSERT_EQ(w, (ssize_t)kSizeA);
    ASSERT_TRUE(inode->is_dirty);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);

    // The object moved to the hidden name, carrying the flushed dirty data.
    std::string hidden = hidden_name_of(nodeid, 0);
    EXPECT_EQ(
        "", get_file_meta(filename, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_NE("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Writes keep succeeding locally after unlink.
    const size_t kSizeB = 64 * 1024;
    std::string data_b = random_string(kSizeB);
    w = pwrite_mirror(handle, local_fd, data_b.data(), kSizeB,
                      static_cast<off_t>(kSizeA));
    ASSERT_EQ(w, (ssize_t)kSizeB);
    EXPECT_GT(staging_disk_usage(), 0u);

    // Release marks stale before close (flush skipped), then deletes the
    // hidden object.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(inode->is_dirty);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    EXPECT_EQ(staging_disk_usage(), 0u) << "staging not released after close";

    // Neither the original name nor the hidden object may survive.
    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", meta["Content-Length"]);
    EXPECT_EQ("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);
  }

  // Unlink a clean random-write file while open, then write through the
  // still-open handle: the new data must stay local-only and vanish on
  // release -- the hidden object is deleted and never resurrected.
  void verify_unlink_clean_then_write_no_resurrection() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_unlink_clean_write";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_FALSE(inode->is_dirty);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);
    std::string hidden = hidden_name_of(nodeid, 0);
    EXPECT_NE("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Re-dirty the unlinked file through the open handle.
    const size_t kSize = 64 * 1024;
    std::string data = random_string(kSize);
    auto w = pwrite_mirror(handle, local_fd, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_TRUE(inode->is_dirty);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(inode->is_dirty);
    ASSERT_EQ(inode->rw_ctx, nullptr);
    EXPECT_EQ(staging_disk_usage(), 0u);

    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", meta["Content-Length"])
        << "stale flush resurrected the deleted object";
    EXPECT_EQ("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"])
        << "hidden object not deleted on the last release";
  }

  // Reads and writes keep working after the file is hidden: clean data is
  // served from the hidden remote object, new writes land in staging and
  // read back correctly.
  void verify_read_write_after_unlink_hidden() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_rw_after_unlink";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Build clean remote content, then hide it.
    const size_t kSize = 512 * 1024;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_hidden);

    // Clean read: served from the hidden remote object.
    std::string buf(kSize, '\0');
    auto got = read_from_handle(handle, buf.data(), kSize, 0);
    ASSERT_EQ(got, (ssize_t)kSize);
    ASSERT_EQ(buf, data);

    // Overwrite the middle, then read back the merged view.
    const off_t kPatchOff = 256 * 1024;
    const size_t kPatchSize = 64 * 1024;
    std::string patch = random_string(kPatchSize);
    w = write_to_file_handle(handle, patch.data(), kPatchSize, kPatchOff);
    ASSERT_EQ(w, (ssize_t)kPatchSize);
    std::string expected = data;
    expected.replace(kPatchOff, kPatchSize, patch);
    got = read_from_handle(handle, buf.data(), kSize, 0);
    ASSERT_EQ(got, (ssize_t)kSize);
    ASSERT_EQ(buf, expected);

    // Everything vanishes on the last release.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ("", get_file_meta(hidden_name_of(nodeid, 0),
                                FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // Unlink a file held by two handles: the hidden object must survive the
  // first release and be deleted only by the last one.
  void verify_unlink_open_multi_handle_last_release_deletes() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_unlink_multi_handle";
    uint64_t nodeid = 0;
    void *h1 = nullptr, *h2 = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &h1);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &h2, &unused);
    ASSERT_EQ(r, 0);
    r = fsync_file_handle(h1, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(h1)->get_inode());
    std::string hidden = hidden_name_of(nodeid, 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);
    EXPECT_NE("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // First release: the hidden object must survive.
    r = fs_->release(nodeid, get_file_from_handle(h1));
    ASSERT_EQ(r, 0);
    EXPECT_NE("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Last release deletes the hidden object.
    r = fs_->release(nodeid, get_file_from_handle(h2));
    ASSERT_EQ(r, 0);
    EXPECT_EQ("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ(
        "", get_file_meta(filename, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // Rename onto an opened dst: the dst is hidden first, the src takes the
  // name, and the hidden object is deleted when the dst handle is released.
  void verify_rename_over_open_dst_hides_dst() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string src_name = "rw_hide_src", dst_name = "rw_hide_dst";
    uint64_t src_nodeid = 0, dst_nodeid = 0;
    void *src_handle = nullptr, *dst_handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, src_name.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &src_nodeid, &st, &src_handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(src_nodeid, 1));
    r = create_and_flush(parent, dst_name.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &dst_nodeid, &st, &dst_handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_nodeid, 1));

    // Distinct src content to tell the overwrite apart.
    std::string data = random_string(64 * 1024);
    auto w = write_to_file_handle(src_handle, data.data(), data.size(), 0);
    ASSERT_EQ(w, (ssize_t)data.size());
    r = fsync_file_handle(src_handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);
    r = fsync_file_handle(dst_handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    auto dst_inode =
        static_cast<FileInode *>(get_file_from_handle(dst_handle)->get_inode());
    std::string hidden = hidden_name_of(dst_nodeid, 0);

    r = fs_->rename(parent, src_name.c_str(), parent, dst_name.c_str(), 0);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(dst_inode->is_hidden);
    ASSERT_FALSE(dst_inode->is_stale);
    EXPECT_NE("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // The dst name now serves the src content.
    auto meta = get_file_meta(dst_name, FLAGS_oss_bucket_prefix);
    EXPECT_EQ(std::to_string(data.size()), meta["Content-Length"]);

    // Releasing the last handle of the hidden dst deletes the hidden object.
    r = fs_->release(dst_nodeid, get_file_from_handle(dst_handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ("",
              get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"]);
    r = fs_->release(src_nodeid, get_file_from_handle(src_handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // The first hidden name collides with a pre-existing remote object; hide
  // must retry with the next seq and succeed, leaving the decoy untouched.
  void verify_hide_conflict_retry_succeeds() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_hide_conflict_retry";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    // Plant a decoy at the first hidden name (seq 0) to force -EEXIST.
    std::string decoy0 = hidden_name_of(nodeid, 0);
    std::string decoy_local = make_local_decoy("hide_decoy_retry");
    ASSERT_EQ(upload_file(decoy_local, decoy0, FLAGS_oss_bucket_prefix), 0);
    DEFER(delete_file(decoy0, FLAGS_oss_bucket_prefix));

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_hidden);

    // Seq 0 is occupied by the decoy; hide must land on seq 1.
    std::string hidden1 = hidden_name_of(nodeid, 1);
    EXPECT_NE(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_NE("",
              get_file_meta(decoy0, FLAGS_oss_bucket_prefix)["Content-Length"])
        << "decoy object must not be touched by the retry";
  }

  // All 10 hidden names occupied: hide exhausts its retries, unlink returns
  // -EBUSY, and the file stays fully usable afterwards.
  void verify_hide_conflict_exhausted_returns_ebusy() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_hide_conflict_ebusy";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    std::string decoy_local = make_local_decoy("hide_decoy_ebusy");
    constexpr uint32_t kMaxRetry = 10;
    for (uint32_t seq = 0; seq < kMaxRetry; ++seq) {
      ASSERT_EQ(upload_file(decoy_local, hidden_name_of(nodeid, seq),
                            FLAGS_oss_bucket_prefix),
                0);
    }
    DEFER(for (uint32_t seq = 0; seq < kMaxRetry; ++seq) {
      delete_file(hidden_name_of(nodeid, seq), FLAGS_oss_bucket_prefix);
    });

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, -EBUSY);
    ASSERT_FALSE(inode->is_hidden);
    ASSERT_FALSE(inode->is_stale);

    // The file stays fully usable after the failed hide.
    std::string data = random_string(32 * 1024);
    auto w = write_to_file_handle(handle, data.data(), data.size(), 0);
    ASSERT_EQ(w, (ssize_t)data.size());
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        std::to_string(data.size()),
        get_file_meta(filename, FLAGS_oss_bucket_prefix)["Content-Length"]);
  }

  // Unlink without any open handle keeps the classic delete path even in
  // random-write mode: no hide, the inode goes stale directly.
  void verify_unlink_closed_file_random_mode_no_hide() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_unlink_closed";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

    // Close the only handle first, then unlink the closed file.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_stale);
    ASSERT_FALSE(inode->is_hidden);
    EXPECT_EQ(
        "", get_file_meta(filename, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ("", get_file_meta(hidden_name_of(nodeid, 0),
                                FLAGS_oss_bucket_prefix)["Content-Length"]);
    expect_no_hidden_objects();
  }

  // Default (sequential) write mode: unlink-while-open keeps the legacy
  // behavior -- the remote object is deleted immediately, no hiding.
  void verify_unlink_open_default_mode_no_hide() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_unlink_default_mode";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_stale);
    ASSERT_FALSE(inode->is_hidden);
    EXPECT_EQ("", get_file_meta(hidden_name_of(nodeid, 0),
                                FLAGS_oss_bucket_prefix)["Content-Length"]);

    // The original name never comes back.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(filename, FLAGS_oss_bucket_prefix)["Content-Length"]);
    uint64_t unused_nodeid = 0;
    struct stat unused_st;
    EXPECT_EQ(fs_->lookup(parent, filename.c_str(), &unused_nodeid, &unused_st),
              -ENOENT);
    expect_no_hidden_objects();
  }

  // Concurrent unlink and releases of the same opened file must never
  // deadlock; every round must end with no hidden object left behind.
  void verify_concurrent_unlink_release_no_deadlock() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kRounds = 10;
    for (int round = 0; round < kRounds; ++round) {
      std::string filename = "rw_conc_unlink_" + std::to_string(round);
      uint64_t nodeid = 0;
      void *h1 = nullptr, *h2 = nullptr;
      struct stat st;
      int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &h1);
      ASSERT_EQ(r, 0);
      bool unused = false;
      ASSERT_EQ(fs_->open(nodeid, O_RDWR, &h2, &unused), 0);
      ASSERT_EQ(fsync_file_handle(h1, /*datasync=*/true), 0);

      std::atomic<int> unlink_ret{0}, rel1_ret{0}, rel2_ret{0};
      std::vector<std::thread> threads;
      threads.emplace_back([&]() {
        INIT_PHOTON();
        unlink_ret = fs_->unlink(parent, filename.c_str());
      });
      threads.emplace_back([&]() {
        INIT_PHOTON();
        rel1_ret = fs_->release(nodeid, get_file_from_handle(h1));
      });
      threads.emplace_back([&]() {
        INIT_PHOTON();
        rel2_ret = fs_->release(nodeid, get_file_from_handle(h2));
      });
      for (auto &t : threads) t.join();
      ASSERT_EQ(unlink_ret, 0) << "round " << round;
      ASSERT_EQ(rel1_ret, 0) << "round " << round;
      ASSERT_EQ(rel2_ret, 0) << "round " << round;
      ASSERT_EQ(fs_->forget(nodeid, 1), 0);
    }
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // Rename-overwrite and unlink racing on the same opened dst: both paths
  // may hide; whatever the interleaving, there must be no deadlock and no
  // hidden object left behind.
  void verify_concurrent_rename_unlink_open_dst_no_deadlock() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kRounds = 8;
    for (int round = 0; round < kRounds; ++round) {
      std::string dst_name = "rw_conc_ru_dst_" + std::to_string(round);
      std::string src_name = "rw_conc_ru_src_" + std::to_string(round);
      uint64_t dst_nodeid = 0, src_nodeid = 0;
      void *dst_handle = nullptr, *src_handle = nullptr;
      struct stat st;
      ASSERT_EQ(create_and_flush(parent, dst_name.c_str(), CREATE_BASE_FLAGS,
                                 0777, 0, 0, 0, &dst_nodeid, &st, &dst_handle),
                0);
      ASSERT_EQ(create_and_flush(parent, src_name.c_str(), CREATE_BASE_FLAGS,
                                 0777, 0, 0, 0, &src_nodeid, &st, &src_handle),
                0);
      // Close src so only the opened dst is hideable.
      ASSERT_EQ(fs_->release(src_nodeid, get_file_from_handle(src_handle)), 0);
      ASSERT_EQ(fsync_file_handle(dst_handle, /*datasync=*/true), 0);

      std::atomic<int> rename_ret{0}, unlink_ret{0};
      std::thread t1([&]() {
        INIT_PHOTON();
        rename_ret =
            fs_->rename(parent, src_name.c_str(), parent, dst_name.c_str(), 0);
      });
      std::thread t2([&]() {
        INIT_PHOTON();
        unlink_ret = fs_->unlink(parent, dst_name.c_str());
      });
      t1.join();
      t2.join();
      ASSERT_EQ(rename_ret, 0) << "round " << round;
      ASSERT_EQ(unlink_ret, 0) << "round " << round;

      // Release the dst handle; any hidden object is deleted here.
      ASSERT_EQ(fs_->release(dst_nodeid, get_file_from_handle(dst_handle)), 0);

      // Clean whatever survived at the dst name (closed -> direct delete).
      // -ESTALE: the racing pair already removed the child (rename hid the
      // open dst, then unlink marked the src inode occupying the name stale).
      int r = fs_->unlink(parent, dst_name.c_str());
      ASSERT_TRUE(r == 0 || r == -ENOENT || r == -ESTALE)
          << "round " << round << " r " << r;
      ASSERT_EQ(fs_->forget(dst_nodeid, 1), 0);
      ASSERT_EQ(fs_->forget(src_nodeid, 1), 0);
    }
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // Multiple sources are concurrently renamed onto the same dst name while a
  // reader keeps reading the opened dst through its handle. No deadlock, the
  // open reader keeps seeing its original data (the hidden inode stays
  // alive), and dst ends up holding exactly one source's content.
  void verify_concurrent_multi_src_rename_to_same_dst_with_reader() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kRounds = 3;
    const size_t kSize = 64 * 1024;
    for (int round = 0; round < kRounds; ++round) {
      std::string dst_name = "rw_conc_mr_dst_" + std::to_string(round);
      uint64_t dst_nodeid = 0;
      void *dst_handle = nullptr;
      struct stat st;
      ASSERT_EQ(create_and_flush(parent, dst_name.c_str(), CREATE_BASE_FLAGS,
                                 0777, 0, 0, 0, &dst_nodeid, &st, &dst_handle),
                0);
      std::string dst_data = random_string(kSize);
      ASSERT_EQ(write_to_file_handle(dst_handle, dst_data.data(), kSize, 0),
                (ssize_t)kSize);
      ASSERT_EQ(fsync_file_handle(dst_handle, /*datasync=*/true), 0);

      // Three closed sources with distinct contents.
      const int kSrcCnt = 3;
      std::vector<uint64_t> src_nodeids(kSrcCnt, 0);
      std::vector<uint64_t> src_crcs;
      for (int i = 0; i < kSrcCnt; ++i) {
        std::string src_name =
            "rw_conc_mr_src_" + std::to_string(round) + "_" + std::to_string(i);
        void *src_handle = nullptr;
        ASSERT_EQ(
            create_and_flush(parent, src_name.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &src_nodeids[i], &st, &src_handle),
            0);
        std::string data = random_string(kSize);
        ASSERT_EQ(write_to_file_handle(src_handle, data.data(), kSize, 0),
                  (ssize_t)kSize);
        ASSERT_EQ(fsync_file_handle(src_handle, /*datasync=*/true), 0);
        src_crcs.push_back(crc_of(data));
        ASSERT_EQ(
            fs_->release(src_nodeids[i], get_file_from_handle(src_handle)), 0);
      }

      // Reader keeps reading the opened dst through its handle; after the
      // first rename hides the dst inode, reads are served from the hidden
      // object and must stay intact.
      std::atomic<bool> stop{false};
      std::atomic<bool> read_ok{true};
      std::thread reader([&]() {
        INIT_PHOTON();
        std::string buf(kSize, '\0');
        while (!stop.load()) {
          auto got = read_from_handle(dst_handle, buf.data(), kSize, 0);
          if (got != (ssize_t)kSize || buf != dst_data) {
            read_ok = false;
            break;
          }
        }
      });

      // All three sources race onto the same dst name.
      std::vector<std::thread> renamers;
      std::vector<int> rename_rets(kSrcCnt, -1);
      for (int i = 0; i < kSrcCnt; ++i) {
        renamers.emplace_back([&, i]() {
          INIT_PHOTON();
          std::string src_name = "rw_conc_mr_src_" + std::to_string(round) +
                                 "_" + std::to_string(i);
          rename_rets[i] = fs_->rename(parent, src_name.c_str(), parent,
                                       dst_name.c_str(), 0);
        });
      }
      for (auto &t : renamers) t.join();
      for (int i = 0; i < kSrcCnt; ++i) {
        ASSERT_EQ(rename_rets[i], 0) << "round " << round << " src " << i;
      }

      stop = true;
      reader.join();
      ASSERT_TRUE(read_ok) << "round " << round;

      // Last release of the hidden dst inode deletes the hidden object.
      ASSERT_EQ(fs_->release(dst_nodeid, get_file_from_handle(dst_handle)), 0);

      // The dst name must hold exactly one source's content.
      uint64_t winner_crc = 0;
      ASSERT_EQ(read_file_in_folder(parent, dst_name, &winner_crc),
                (ssize_t)kSize)
          << "round " << round;
      ASSERT_TRUE(std::find(src_crcs.begin(), src_crcs.end(), winner_crc) !=
                  src_crcs.end())
          << "round " << round;

      int r = fs_->unlink(parent, dst_name.c_str());
      ASSERT_EQ(r, 0) << "round " << round;
      ASSERT_EQ(fs_->forget(dst_nodeid, 1), 0);
      for (auto nodeid : src_nodeids) ASSERT_EQ(fs_->forget(nodeid, 1), 0);
    }
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // Regression for the delete-under-lock fix: a rename racing the last
  // release moves another file onto the hidden name; either interleaving
  // must keep that file's data alive (the rename is serialized with the
  // delete by the dst inode lock).
  void verify_release_delete_serialized_with_rename_to_hidden_name() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int kRounds = 5;
    for (int round = 0; round < kRounds; ++round) {
      std::string fname = "rw_race_f_" + std::to_string(round);
      std::string xname = "rw_race_x_" + std::to_string(round);
      uint64_t nodeid = 0, x_nodeid = 0;
      void *handle = nullptr, *x_handle = nullptr;
      struct stat st;
      ASSERT_EQ(create_and_flush(parent, fname.c_str(), CREATE_BASE_FLAGS, 0777,
                                 0, 0, 0, &nodeid, &st, &handle),
                0);
      ASSERT_EQ(fsync_file_handle(handle, /*datasync=*/true), 0);
      ASSERT_EQ(create_and_flush(parent, xname.c_str(), CREATE_BASE_FLAGS, 0777,
                                 0, 0, 0, &x_nodeid, &st, &x_handle),
                0);
      ASSERT_EQ(fsync_file_handle(x_handle, /*datasync=*/true), 0);
      ASSERT_EQ(fs_->release(x_nodeid, get_file_from_handle(x_handle)), 0);

      auto inode =
          static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

      // Hide f, then race: last release of f (deletes the hidden object)
      // vs a rename moving x onto the hidden name.
      ASSERT_EQ(fs_->unlink(parent, fname.c_str()), 0);
      ASSERT_TRUE(inode->is_hidden);
      std::string hidden(inode->name);

      std::atomic<int> rel_ret{0}, ren_ret{0};
      std::thread t1([&]() {
        INIT_PHOTON();
        rel_ret = fs_->release(nodeid, get_file_from_handle(handle));
      });
      std::thread t2([&]() {
        INIT_PHOTON();
        ren_ret = fs_->rename(parent, xname.c_str(), parent, hidden.c_str(), 0);
      });
      t1.join();
      t2.join();
      ASSERT_EQ(rel_ret, 0) << "round " << round;
      ASSERT_EQ(ren_ret, 0) << "round " << round;

      // x's data must survive at the hidden name under both interleavings.
      EXPECT_NE(
          "", get_file_meta(hidden, FLAGS_oss_bucket_prefix)["Content-Length"])
          << "round " << round << ": racing rename lost its data";
      ASSERT_EQ(delete_file(hidden, FLAGS_oss_bucket_prefix), 0);
      ASSERT_EQ(fs_->forget(nodeid, 1), 0);
      ASSERT_EQ(fs_->forget(x_nodeid, 1), 0);
    }
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // Chained renames onto the same dst name: each rename hides the current
  // occupant when it is still open, the seq counter keeps advancing, and
  // every hidden object is reclaimed by the last release of its own owner.
  void verify_chained_rename_over_open_dst_hides_each() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string dst_name = "rw_chain_dst";
    uint64_t dst_nodeid = 0;
    void *dst_handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, dst_name.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &dst_nodeid, &st, &dst_handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dst_nodeid, 1));
    auto dst_inode =
        static_cast<FileInode *>(get_file_from_handle(dst_handle)->get_inode());

    // Step 1: closed src1 onto the open dst -> the dst is hidden (seq 0).
    uint64_t src1_nodeid = 0;
    void *src1_handle = nullptr;
    ASSERT_EQ(create_and_flush(parent, "rw_chain_src1", CREATE_BASE_FLAGS, 0777,
                               0, 0, 0, &src1_nodeid, &st, &src1_handle),
              0);
    DEFER(fs_->forget(src1_nodeid, 1));
    ASSERT_EQ(fs_->release(src1_nodeid, get_file_from_handle(src1_handle)), 0);
    ASSERT_EQ(fs_->rename(parent, "rw_chain_src1", parent, dst_name.c_str(), 0),
              0);
    ASSERT_TRUE(dst_inode->is_hidden);
    std::string hidden0 = hidden_name_of(dst_nodeid, 0);
    EXPECT_NE(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Step 2: open src2 onto dst -> the closed src1 occupant is replaced
    // directly (no hide), the open src2 takes the dst name.
    uint64_t src2_nodeid = 0;
    void *src2_handle = nullptr;
    ASSERT_EQ(create_and_flush(parent, "rw_chain_src2", CREATE_BASE_FLAGS, 0777,
                               0, 0, 0, &src2_nodeid, &st, &src2_handle),
              0);
    DEFER(fs_->forget(src2_nodeid, 1));
    auto src2_inode = static_cast<FileInode *>(
        get_file_from_handle(src2_handle)->get_inode());
    ASSERT_EQ(fs_->rename(parent, "rw_chain_src2", parent, dst_name.c_str(), 0),
              0);
    ASSERT_FALSE(src2_inode->is_hidden);
    EXPECT_EQ(hidden_name_of(dst_nodeid, 0), hidden0)
        << "seq must not advance without a hide";

    // Step 3: closed src3 onto dst whose occupant (src2) is open -> src2 is
    // hidden with the next seq.
    uint64_t src3_nodeid = 0;
    void *src3_handle = nullptr;
    ASSERT_EQ(create_and_flush(parent, "rw_chain_src3", CREATE_BASE_FLAGS, 0777,
                               0, 0, 0, &src3_nodeid, &st, &src3_handle),
              0);
    DEFER(fs_->forget(src3_nodeid, 1));
    ASSERT_EQ(fs_->release(src3_nodeid, get_file_from_handle(src3_handle)), 0);
    ASSERT_EQ(fs_->rename(parent, "rw_chain_src3", parent, dst_name.c_str(), 0),
              0);
    ASSERT_TRUE(src2_inode->is_hidden);
    std::string hidden1 = hidden_name_of(src2_nodeid, 1);
    EXPECT_NE(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Both hidden objects coexist until their owners release.
    r = fs_->release(dst_nodeid, get_file_from_handle(dst_handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_NE(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);
    r = fs_->release(src2_nodeid, get_file_from_handle(src2_handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // The dst name finally serves src3's content.
    auto meta = get_file_meta(dst_name, FLAGS_oss_bucket_prefix);
    EXPECT_NE("", meta["Content-Length"]);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // The hidden file is a regular directory entry: if the user unlinks it
  // while its handle is still open, it is hidden again under a new seq, and
  // the last release reclaims the final hidden object.
  void verify_unlink_hidden_file_again_hides_nested() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_nested_hide";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    ASSERT_EQ(fsync_file_handle(handle, /*datasync=*/true), 0);

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(inode->is_hidden);
    std::string hidden0 = hidden_name_of(nodeid, 0);
    ASSERT_NE(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // The user unlinks the hidden name itself; the still-open inode is
    // hidden again under the next seq, and the first hidden name vanishes.
    r = fs_->unlink(parent, hidden0.c_str());
    ASSERT_EQ(r, 0);
    std::string hidden1 = hidden_name_of(nodeid, 1);
    EXPECT_EQ(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_NE(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden1, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // If the user renames the hidden file to a normal name, the last release
  // must still delete it: a logically-unlinked file can never be
  // resurrected under a user-chosen name.
  void verify_rename_hidden_file_then_release_deletes_new_name() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_rename_hidden";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    ASSERT_EQ(fsync_file_handle(handle, /*datasync=*/true), 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    std::string hidden0 = hidden_name_of(nodeid, 0);
    ASSERT_NE(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // The user "rescues" the hidden file by renaming it to a normal name.
    std::string rescued = "rw_rescued";
    r = fs_->rename(parent, hidden0.c_str(), parent, rescued.c_str(), 0);
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_NE(
        "", get_file_meta(rescued, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // The last release still deletes it under the new name.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ("",
              get_file_meta(rescued, FLAGS_oss_bucket_prefix)["Content-Length"])
        << "logically-unlinked file must not survive under a new name";
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // The hidden object may vanish out-of-band (deleted via the OSS console or
  // ossutil); the last release must tolerate the missing object and succeed.
  void verify_release_tolerates_hidden_object_deleted_externally() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_hidden_gone";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    ASSERT_EQ(fsync_file_handle(handle, /*datasync=*/true), 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    std::string hidden0 = hidden_name_of(nodeid, 0);
    ASSERT_NE(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Out-of-band delete bypassing the mount.
    ASSERT_EQ(delete_file(hidden0, FLAGS_oss_bucket_prefix), 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // ftruncate through an open handle after unlink hid the file: the
  // truncate succeeds on the hidden inode, keeps it logically dead, and the
  // last release still deletes the hidden object.
  void verify_ftruncate_after_unlink_hidden() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_ftrunc_hidden";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t kSize = 128 * 1024;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_EQ(fsync_file_handle(handle, /*datasync=*/true), 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    ASSERT_TRUE(inode->is_hidden);
    std::string hidden0 = hidden_name_of(nodeid, 0);

    // ftruncate on the hidden inode must succeed.
    struct stat sz = {};
    sz.st_size = 48 * 1024;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    // Size updated, reads past the new size are cut off.
    struct stat st2;
    ASSERT_EQ(fs_->getattr(nodeid, &st2), 0);
    ASSERT_EQ(st2.st_size, 48 * 1024);
    std::string buf(kSize, '\0');
    auto got = read_from_handle(handle, buf.data(), kSize, 0);
    ASSERT_EQ(got, 48 * 1024);
    ASSERT_TRUE(std::equal(buf.begin(), buf.begin() + got, data.begin()));

    // The file stays logically dead; the last release deletes the hidden
    // object.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // truncate(2) by path on a hidden name: the hidden object is still alive,
  // so the truncate applies to it, and the owner's last release still
  // deletes it.
  void verify_truncate_hidden_name_by_path() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_trunc_hidden";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t kSize = 64 * 1024;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_EQ(fsync_file_handle(handle, /*datasync=*/true), 0);

    r = fs_->unlink(parent, filename.c_str());
    ASSERT_EQ(r, 0);
    std::string hidden0 = hidden_name_of(nodeid, 0);
    ASSERT_NE(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);

    // Resolve the hidden name and truncate it by path.
    uint64_t hid_nodeid = 0;
    struct stat hst;
    r = fs_->lookup(parent, hidden0.c_str(), &hid_nodeid, &hst);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(hid_nodeid, 1));
    ASSERT_EQ(hid_nodeid, nodeid);

    struct stat sz = {};
    sz.st_size = 16 * 1024;
    r = fs_->setattr(hid_nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);

    // The owner's last release still deletes the hidden object.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    EXPECT_EQ(
        "", get_file_meta(hidden0, FLAGS_oss_bucket_prefix)["Content-Length"]);
    EXPECT_EQ(staging_disk_usage(), 0u);
    expect_no_hidden_objects();
  }

  // While a file is dirty, reading a clean chunk goes straight to OSS; a
  // failing OSS request must propagate to the reader, and the read must
  // recover once the failure clears.
  void verify_read_while_write_clean_chunk_oss_failure() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_read_fi";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);  // 4 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Dirty chunk 0 so reads route through the random-write dirty path.
    const size_t kPatch = 4096;
    std::string patch = random_string(kPatch);
    auto w = write_to_file_handle(handle, patch.data(), kPatch, 0);
    ASSERT_EQ(w, (ssize_t)kPatch);

    const size_t kRead = 8 * 1024;
    std::string buf(kRead, '\0');

    // The dirty chunk reads back from staging.
    auto got = read_from_handle(handle, buf.data(), kRead, 0);
    ASSERT_EQ(got, (ssize_t)kRead);
    ASSERT_EQ(buf.compare(0, kPatch, patch), 0);

    // OSS read of a clean chunk fails -> error propagated.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    got = read_from_handle(handle, buf.data(), kRead, 3 * 1024 * 1024);
    ASSERT_LT(got, 0) << "clean-chunk read must propagate OSS failure";
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    got = read_from_handle(handle, buf.data(), kRead, 3 * 1024 * 1024);
    ASSERT_EQ(got, (ssize_t)kRead);

    // The retried read must return the original remote data.
    std::string expect(kRead, '\0');
    int local_fd = ::open(local_file.c_str(), O_RDONLY);
    ASSERT_GE(local_fd, 0) << "open source file " << local_file;
    DEFER(::close(local_fd));
    ASSERT_EQ(::pread(local_fd, expect.data(), kRead, 3 * 1024 * 1024),
              (ssize_t)kRead);
    ASSERT_EQ(buf, expect) << "clean-chunk read returned corrupted data";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // Path-refresh coverage around rename: writer get_is_dirty, pwrite
  // phase-A refresh, directory rename with a dirty file inside, and the
  // transient flush failure during rename.
  void verify_rename_path_refresh_scenarios() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // ── Part A: get_is_dirty + pwrite phase-A upload_path refresh ──
    std::string name_a = "rw_path_src";
    std::string name_b = "rw_path_dst";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, name_a.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto *file = get_file_from_handle(handle);
    auto *ofile = dynamic_cast<OssFileHandle *>(file);
    ASSERT_NE(ofile, nullptr);
    auto inode = static_cast<FileInode *>(file->get_inode());

    const size_t kSize = 4096;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_TRUE(ofile->get_is_dirty());
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);
    ASSERT_FALSE(ofile->get_is_dirty());

    // Rename the clean file; rw_ctx keeps the stale pre-rename path until
    // the next dirty write refreshes it (phase A).
    r = fs_->rename(parent, name_a.c_str(), parent, name_b.c_str(), 0);
    ASSERT_EQ(r, 0);
    ASSERT_NE(inode->rw_ctx, nullptr);
    std::string stale_path = inode->rw_ctx->upload_path;

    w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);
    ASSERT_EQ(inode->rw_ctx->upload_path, nodeid_to_path(nodeid))
        << "phase-A must refresh upload_path after rename";
    ASSERT_NE(inode->rw_ctx->upload_path, stale_path);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    auto meta_b = get_file_meta(name_b, FLAGS_oss_bucket_prefix);
    EXPECT_EQ(std::to_string(kSize), meta_b["Content-Length"]);
    auto meta_a = get_file_meta(name_a, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", meta_a["Content-Length"]);

    // ── Part B: directory rename flushes the dirty file inside ──
    std::string dir_src = "rw_dir_src";
    std::string dir_dst = "rw_dir_dst";
    uint64_t dir_nodeid = 0;
    r = fs_->mkdir(parent, dir_src.c_str(), 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir_nodeid, 1));

    std::string name_f = "inner_file";
    uint64_t nodeid_f = 0;
    void *handle_f = nullptr;
    r = create_and_flush(dir_nodeid, name_f.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid_f, &st, &handle_f);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_f, 1));
    w = write_to_file_handle(handle_f, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    r = fs_->rename(parent, dir_src.c_str(), parent, dir_dst.c_str(), 0);
    ASSERT_EQ(r, 0) << "dir rename must flush dirty inodes underneath";

    r = fs_->release(nodeid_f, get_file_from_handle(handle_f));
    ASSERT_EQ(r, 0);

    uint64_t dir2_nodeid = 0;
    r = fs_->lookup(parent, dir_dst.c_str(), &dir2_nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(dir2_nodeid, 1));
    uint64_t f2_nodeid = 0;
    struct stat st_f;
    r = fs_->lookup(dir2_nodeid, name_f.c_str(), &f2_nodeid, &st_f);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(f2_nodeid, 1));
    EXPECT_EQ(static_cast<size_t>(st_f.st_size), kSize);

    // ── Part C: rename transient writer flush failure ──
    std::string name_g = "rw_flushfail_src";
    std::string name_h = "rw_flushfail_dst";
    uint64_t nodeid_g = 0;
    void *handle_g = nullptr;
    r = create_and_flush(parent, name_g.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0,
                         0, &nodeid_g, &st, &handle_g);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid_g, 1));
    w = write_to_file_handle(handle_g, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    // Fail the transient flush; the injection must outlive the HTTP retries.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    r = fs_->rename(parent, name_g.c_str(), parent, name_h.c_str(), 0);
    ASSERT_LT(r, 0) << "rename must fail when the transient flush fails";
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    r = fs_->rename(parent, name_g.c_str(), parent, name_h.c_str(), 0);
    ASSERT_EQ(r, 0);

    // The dirty data must have been flushed to the new path by the rename,
    // and nothing may remain at the old path.
    auto meta_h = get_file_meta(name_h, FLAGS_oss_bucket_prefix);
    EXPECT_EQ(std::to_string(kSize), meta_h["Content-Length"])
        << "dirty data must be flushed to the rename destination";
    EXPECT_EQ(std::to_string(crc_of(data)), meta_h["X-Oss-Hash-Crc64ecma"]);
    auto meta_g = get_file_meta(name_g, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("", meta_g["Content-Length"])
        << "old path must not keep the object after rename";

    r = fs_->release(nodeid_g, get_file_from_handle(handle_g));
    ASSERT_EQ(r, 0);
  }

  // Standalone (file not open) truncate whose transient writer fails to
  // open because the staging dir disappeared.
  void verify_standalone_truncate_open_failure() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "rw_trunc_openfail";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 1);  // 1 MiB
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const std::string good_dir = fs_->options_.temp_dir;
    fs_->options_.temp_dir = good_dir + "/no_such_subdir_for_trunc";

    struct stat sz = {};
    sz.st_size = 512 * 1024;
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_LT(r, 0) << "truncate must fail when the transient open fails";

    fs_->options_.temp_dir = good_dir;
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);

    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    EXPECT_EQ(std::to_string(512 * 1024), meta["Content-Length"]);
  }

  // flush_multipart must return -EFBIG when the base part size itself
  // exceeds the OSS 5 GiB part limit.
  void verify_flush_efbig_when_base_part_too_large() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "rw_efbig";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    // Guarantee the empty object exists remotely (create_and_flush may skip
    // the flush); the EFBIG close below reverts attr.size to remote_size.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    const size_t kSize = 4096;
    std::string data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.data(), kSize, 0);
    ASSERT_EQ(w, (ssize_t)kSize);

    // Inflate the logical size past the oversized base part without writing
    // staging bytes; the flush must reject it with EFBIG.
    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());
    {
      std::unique_lock<std::shared_mutex> wl(inode->inode_lock);
      inode->attr.size = 7ULL * 1024 * 1024 * 1024;
    }

    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, -EFBIG);

    // close() drops the unflushed data and reverts to the remote size.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_LT(r, 0);
    EXPECT_FALSE(inode->is_dirty);
    EXPECT_EQ(inode->rw_ctx, nullptr);

    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    EXPECT_EQ("0", meta["Content-Length"]);
  }

  // Multipart part-level faults: the refill GET of a dirty part fails, and
  // a copy part returns without a CRC64 header.
  void verify_multipart_refill_and_copy_crc_faults() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    // 4 MiB remote -> two 2 MiB parts; each part spans two 1 MiB chunks so
    // the dirty part still contains clean chunks to refill.
    std::string filename = "rw_mp_part_fi";
    std::string local_file = test_path_ + filename + ".src";
    create_random_file(local_file, 4);
    int rr = upload_file(local_file, parent_path + std::string("/") + filename,
                         FLAGS_oss_bucket_prefix);
    ASSERT_EQ(rr, 0);
    int local_fd = reopen_mirror(local_file);
    DEFER(::close(local_fd));

    uint64_t nodeid = 0;
    struct stat st;
    int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    // Dirty chunk 0: part 1 takes the refill+upload path, part 2 stays a
    // server-side copy.
    const size_t kPatch = 4096;
    std::string patch = random_string(kPatch);
    auto w = pwrite_mirror(handle, local_fd, patch.data(), kPatch, 0);
    ASSERT_EQ(w, (ssize_t)kPatch);

    // (a) skip the init call, then fail the first refill GET.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call,
        FaultInjection(std::numeric_limits<uint32_t>::max(), 1));
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_LT(r, 0) << "flush must fail when a part refill fails";
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    // Retry: flush succeeds, dirty state retained until then.
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0);

    // (b) copy part response without CRC64 header: flush still succeeds
    // and completes without the whole-file CRC.
    w = pwrite_mirror(handle, local_fd, patch.data(), kPatch, 0);
    ASSERT_EQ(w, (ssize_t)kPatch);
    g_fault_injector->set_injection(
        FaultInjectionId::FI_RandomWrite_Copy_Part_No_Crc);
    r = fsync_file_handle(handle, /*datasync=*/true);
    ASSERT_EQ(r, 0) << "missing copy-part CRC must not fail the flush";
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_RandomWrite_Copy_Part_No_Crc);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }

  // The random_write_max_file_size cap rejects any truncate/pwrite that
  // leaves the file above the limit with EFBIG and no state change, while
  // operations at or below the limit succeed. Shrinks that stay above the
  // limit are rejected too (such a size can never be flushed).
  void verify_max_file_size_limit() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const uint64_t kLimit = fs_->options_.random_write_max_file_size;
    ASSERT_EQ(kLimit, 8ULL * 1024 * 1024) << "test assumes the TEST_F limit";
    const uint64_t saved_limit = kLimit;
    DEFER({ fs_->options_.random_write_max_file_size = saved_limit; });

    std::string filename = "rw_max_size";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));

    auto inode =
        static_cast<FileInode *>(get_file_from_handle(handle)->get_inode());

    // create_and_flush may skip the flush, leaving the fresh file dirty;
    // a rejected op must keep whatever state it found.
    const bool dirty_at_start = inode->is_dirty;

    // Truncate extending past the limit fails, leaving no state behind.
    struct stat sz = {};
    sz.st_size = static_cast<off_t>(2 * kLimit);
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, -EFBIG);
    EXPECT_EQ(inode->attr.size, 0u);
    EXPECT_EQ(inode->is_dirty, dirty_at_start);
    EXPECT_EQ(staging_disk_usage(), 0u);

    // Pwrite crossing the limit fails before touching staging.
    const size_t kLen = 2 * 1024 * 1024 + 4096;
    std::string data = random_string(kLen);
    ssize_t w =
        write_to_file_handle(handle, data.data(), kLen,
                             static_cast<off_t>(kLimit - 2 * 1024 * 1024));
    ASSERT_EQ(w, -EFBIG);
    EXPECT_EQ(inode->is_dirty, dirty_at_start);
    EXPECT_EQ(staging_disk_usage(), 0u);

    // Boundary: a write ending exactly at the limit succeeds.
    const size_t kWriteSize = 2 * 1024 * 1024;
    std::string data2 = random_string(kWriteSize);
    w = write_to_file_handle(handle, data2.data(), kWriteSize,
                             static_cast<off_t>(kLimit - kWriteSize));
    ASSERT_EQ(w, (ssize_t)kWriteSize);
    ASSERT_EQ(::pwrite(local_fd, data2.data(), kWriteSize,
                       static_cast<off_t>(kLimit - kWriteSize)),
              (ssize_t)kWriteSize);

    // Boundary: truncate extending exactly to the limit succeeds.
    sz.st_size = static_cast<off_t>(kLimit);
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);
    truncate_mirror(local_fd, kLimit);

    // A shrink that stays above the limit is rejected; raising the limit
    // first makes the oversized size reachable, then restoring it re-arms
    // the cap.
    fs_->options_.random_write_max_file_size = 2 * kLimit;
    sz.st_size = static_cast<off_t>(2 * kLimit);
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);
    truncate_mirror(local_fd, 2 * kLimit);

    fs_->options_.random_write_max_file_size = saved_limit;
    sz.st_size = static_cast<off_t>(kLimit + kLimit / 2);
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, -EFBIG)
        << "shrink that stays above the limit must be rejected";
    EXPECT_EQ(inode->attr.size, 2 * kLimit);

    // A shrink below the limit succeeds; pwrite at EOF (== the limit)
    // is rejected because it would extend past it.
    sz.st_size = static_cast<off_t>(kLimit / 2);
    r = fs_->setattr(nodeid, &sz, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);
    truncate_mirror(local_fd, kLimit / 2);

    const char one = 'x';
    w = write_to_file_handle(handle, &one, 1, static_cast<off_t>(kLimit));
    ASSERT_EQ(w, -EFBIG);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }
};

// Parameterized on file size to cover both flush routes: at most one base
// part takes the single-shot put_object path, more than one takes the
// multipart upload_part path.
class Ossfs2RandomWriteFlushRetryCrcTest
    : public Ossfs2RandomWriteTest,
      public ::testing::WithParamInterface<size_t> {
 protected:
  // A body writer failing mid-transfer after CRC-accumulating some bytes
  // must not corrupt the CRC64: the HTTP-layer retry re-invokes the body
  // writer, which must recompute the CRC from scratch so the verify against
  // the server-side CRC still passes.
  void flush_retry_crc_after_body_writer_partial_fail(
      const std::string &filename, size_t size) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat stbuf;
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &stbuf, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Fully dirty file so every part takes the upload-from-fd path with
    // CRC64 verification.
    int local_fd = open_fresh_mirror(filename);
    DEFER(::close(local_fd));
    write_repeating_data(handle, local_fd, size);

    // Fail exactly one body-writer chunk write; the SDK retry re-sends the
    // whole body.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_Upload_BodyWriter_Partial_Fail,
        FaultInjection(/*run_count=*/1, /*skip_count=*/0));
    r = fsync_file_handle(handle, /*datasync=*/true);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Upload_BodyWriter_Partial_Fail);
    ASSERT_EQ(r, 0)
        << "flush must survive a body-writer failure retried by the SDK";

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    assert_remote_matches_local(filename, local_fd);
  }
};

TEST_F(Ossfs2RandomWriteTest, verify_empty_create_flush) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_empty_create_flush();
}

TEST_F(Ossfs2RandomWriteTest, verify_flush_put) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_flush_put();
}

TEST_F(Ossfs2RandomWriteTest, verify_out_of_order_writes_multipart) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_out_of_order_writes_multipart();
}

TEST_F(Ossfs2RandomWriteTest, verify_sparse_extension_into_hole) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_sparse_extension_into_hole();
}

TEST_F(Ossfs2RandomWriteTest, verify_partial_write_triggers_get_on_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_partial_write_triggers_get_on_write();
}

TEST_F(Ossfs2RandomWriteTest, verify_full_chunk_overwrite_skips_get) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_full_chunk_overwrite_skips_get();
}

TEST_F(Ossfs2RandomWriteTest, verify_pwrite_spans_chunks) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_pwrite_spans_chunks();
}

TEST_F(Ossfs2RandomWriteTest, verify_o_trunc) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_o_trunc();
}

TEST_F(Ossfs2RandomWriteTest, verify_ctx_torn_down_after_release) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_ctx_torn_down_after_release();
}

TEST_F(Ossfs2RandomWriteTest, verify_partial_rewrite_uses_part_copy) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_partial_rewrite_uses_part_copy();
}

TEST_F(Ossfs2RandomWriteTest, verify_repeated_writes_same_chunk) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_repeated_writes_same_chunk();
}

TEST_F(Ossfs2RandomWriteTest, verify_flush_mid_cycle_then_continue) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_flush_mid_cycle_then_continue();
}

TEST_F(Ossfs2RandomWriteTest, verify_multipart_with_hole_parts) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_multipart_with_hole_parts();
}

TEST_F(Ossfs2RandomWriteTest, verify_multi_handle_cooperative_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_multi_handle_cooperative_write();
}

TEST_F(Ossfs2RandomWriteTest, verify_continuous_fsync_cycles) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  // Shrink part size so the file flushes via multipart (not flush_put),
  // exercising the upload_part_copy fast path across fsync cycles.
  opts.upload_buffer_size = 1 * 1024 * 1024;
  // Pin chunk_size so base_part_size stays 1 MiB (align_up would otherwise
  // raise it to the 2 MiB default chunk and reroute the flush to PutObject).
  opts.random_write_chunk_size = 1 * 1024 * 1024;
  init(opts);
  verify_continuous_fsync_cycles();
}

TEST_F(Ossfs2RandomWriteTest, verify_chunk_straddles_remote_boundary) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_chunk_straddles_remote_boundary();
}

TEST_F(Ossfs2RandomWriteTest, verify_flush_failure_loses_data_cleanly) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_flush_failure_loses_data_cleanly();
}

TEST_F(Ossfs2RandomWriteTest, verify_flush_failure_is_retryable) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  init(opts);
  verify_flush_failure_is_retryable();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_flush_multipart_error_not_swallowed_by_abort) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  // Pin chunk_size so base_part_size stays 1 MiB (align_up would otherwise
  // raise it to the 2 MiB default chunk and reroute the flush to PutObject).
  opts.random_write_chunk_size = 1 * 1024 * 1024;
  init(opts);
  verify_flush_multipart_error_not_swallowed_by_abort();
}

TEST_F(Ossfs2RandomWriteTest, verify_partial_pwrite_fails_and_retry_succeeds) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_partial_pwrite_fails_and_retry_succeeds();
}

TEST_F(Ossfs2RandomWriteTest, verify_failed_first_write_rolls_back_clean) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_failed_first_write_rolls_back_clean();
}

TEST_F(Ossfs2RandomWriteTest, verify_open_create_failure_no_leak) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_open_create_failure_no_leak();
}

TEST_F(Ossfs2RandomWriteTest, verify_flush_multipart_copy_failure_propagated) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  init(opts);
  verify_flush_multipart_copy_failure_propagated();
}

TEST_F(Ossfs2RandomWriteTest, verify_multipart_hole_parts_upload_zeros) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  init(opts);
  verify_multipart_hole_parts_upload_zeros();
}

TEST_F(Ossfs2RandomWriteTest, verify_read_fresh_after_prefetch_write_cycle) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_read_fresh_after_prefetch_write_cycle();
}

TEST_F(Ossfs2RandomWriteTest, verify_read_while_write_mixed_sources) {
  SET_TEST_MODE(kTestOss);
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_read_while_write_mixed_sources();
}

TEST_F(Ossfs2RandomWriteTest, verify_direct_reader_read_while_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.prefetch_concurrency = 0;  // disable prefetch -> force OssDirectReader
  init(opts);
  verify_direct_reader_read_while_write();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_read_hole_in_clean_chunk_beyond_remote_size) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_read_hole_in_clean_chunk_beyond_remote_size();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_read_hole_in_clean_chunk_straddling_remote_size) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_read_hole_in_clean_chunk_straddling_remote_size();
}

TEST_F(Ossfs2RandomWriteTest, verify_disk_space) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_disk_space();
}

TEST_F(Ossfs2RandomWriteTest, verify_disk_budget_throttled_refresh) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_disk_budget_throttled_refresh();
}

TEST_F(Ossfs2RandomWriteTest, verify_disk_budget_cross_file_growth) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_disk_budget_cross_file_growth();
}

TEST_F(Ossfs2RandomWriteTest, verify_disk_budget_refill_growth_in_window) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_disk_budget_refill_growth_in_window();
}

TEST_F(Ossfs2RandomWriteTest, verify_concurrent_random_writes) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_concurrent_random_writes();
}

TEST_F(Ossfs2RandomWriteTest, verify_concurrent_disk_budget_stale_refresh) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_concurrent_disk_budget_stale_refresh();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_resize_open) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_resize_open();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_standalone) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_standalone();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_shrink_then_rewrite_hole) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_shrink_then_rewrite_hole();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_to_zero_open_dirty) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_to_zero_open_dirty();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_open_clean_then_read) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.prefetch_concurrency = 0;  // disable prefetch -> force OssDirectReader
  init(opts);
  verify_truncate_open_clean_then_read();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_open_clean_then_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_open_clean_then_write();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_truncate_open_clean_flush_failure_no_crash) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_open_clean_flush_failure_no_crash();
}

// Multipart flush path: file (5.5 MiB) > upload_buffer_size (1 MiB).
TEST_F(Ossfs2RandomWriteTest,
       verify_flush_releases_staging_and_stays_consistent_multipart) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;  // force multipart flush path
  init(opts);
  verify_flush_releases_staging_and_stays_consistent();
}

// Single-PUT flush path: file (5.5 MiB) <= upload_buffer_size (8 MiB default),
// yet still spans multiple chunks so GET-on-write is exercised on refill.
TEST_F(Ossfs2RandomWriteTest,
       verify_flush_releases_staging_and_stays_consistent_single_put) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 8 * 1024 * 1024;  // force flush_no_multipart path
  init(opts);
  verify_flush_releases_staging_and_stays_consistent();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_flush_failure_multi_writer_keeps_dirty_for_retry) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_flush_failure_multi_writer_keeps_dirty_for_retry();
}

TEST_F(Ossfs2RandomWriteTest, verify_rename_flushes_via_transient_writer) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_rename_flushes_via_transient_writer();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_after_rename_uses_new_path) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_after_rename_uses_new_path();
}

TEST_F(Ossfs2RandomWriteTest, verify_o_trunc_after_rename_uses_new_path) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_o_trunc_after_rename_uses_new_path();
}

TEST_F(Ossfs2RandomWriteTest, verify_close_to_open_refreshes_remote_size) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.close_to_open = true;
  init(opts);
  verify_close_to_open_refreshes_remote_size();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_close_to_open_refreshes_remote_size_shrink) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.close_to_open = true;
  init(opts);
  verify_close_to_open_refreshes_remote_size_shrink();
}

TEST_F(Ossfs2RandomWriteTest, verify_short_write_fails_without_marking_dirty) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_short_write_fails_without_marking_dirty();
}

TEST_F(Ossfs2RandomWriteTest, verify_unlink_dirty_randwrite_discards_flush) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_unlink_dirty_randwrite_discards_flush();
}

TEST_F(Ossfs2RandomWriteTest, verify_unlink_clean_then_write_no_resurrection) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_unlink_clean_then_write_no_resurrection();
}

TEST_F(Ossfs2RandomWriteTest, verify_read_write_after_unlink_hidden) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_read_write_after_unlink_hidden();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_unlink_open_multi_handle_last_release_deletes) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_unlink_open_multi_handle_last_release_deletes();
}

TEST_F(Ossfs2RandomWriteTest, verify_rename_over_open_dst_hides_dst) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_rename_over_open_dst_hides_dst();
}

TEST_F(Ossfs2RandomWriteTest, verify_hide_conflict_retry_succeeds) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_hide_conflict_retry_succeeds();
}

TEST_F(Ossfs2RandomWriteTest, verify_hide_conflict_exhausted_returns_ebusy) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_hide_conflict_exhausted_returns_ebusy();
}

TEST_F(Ossfs2RandomWriteTest, verify_unlink_closed_file_random_mode_no_hide) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_unlink_closed_file_random_mode_no_hide();
}

// Default (sequential-write) mode must keep the legacy stale semantics:
// no hidden object is ever created.
TEST_F(Ossfs2RandomWriteTest, verify_unlink_open_default_mode_no_hide) {
  INIT_PHOTON();
  init(OssFsOptions{});
  verify_unlink_open_default_mode_no_hide();
}

TEST_F(Ossfs2RandomWriteTest, verify_concurrent_unlink_release_no_deadlock) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_concurrent_unlink_release_no_deadlock();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_concurrent_rename_unlink_open_dst_no_deadlock) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_concurrent_rename_unlink_open_dst_no_deadlock();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_concurrent_multi_src_rename_to_same_dst_with_reader) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_concurrent_multi_src_rename_to_same_dst_with_reader();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_release_delete_serialized_with_rename_to_hidden_name) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_release_delete_serialized_with_rename_to_hidden_name();
}

TEST_F(Ossfs2RandomWriteTest, verify_chained_rename_over_open_dst_hides_each) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_chained_rename_over_open_dst_hides_each();
}

TEST_F(Ossfs2RandomWriteTest, verify_unlink_hidden_file_again_hides_nested) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_unlink_hidden_file_again_hides_nested();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_rename_hidden_file_then_release_deletes_new_name) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_rename_hidden_file_then_release_deletes_new_name();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_release_tolerates_hidden_object_deleted_externally) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_release_tolerates_hidden_object_deleted_externally();
}

TEST_F(Ossfs2RandomWriteTest, verify_ftruncate_after_unlink_hidden) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_ftruncate_after_unlink_hidden();
}

TEST_F(Ossfs2RandomWriteTest, verify_truncate_hidden_name_by_path) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_truncate_hidden_name_by_path();
}

TEST_F(Ossfs2RandomWriteTest, verify_cache_dropped_on_mark_clean) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_cache_dropped_on_mark_clean();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_failed_write_on_dirty_file_keeps_fetched_chunk_clean) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_failed_write_on_dirty_file_keeps_fetched_chunk_clean();
}

TEST_F(Ossfs2RandomWriteTest, verify_get_chunk_fail_on_write_is_retryable) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_get_chunk_fail_on_write_is_retryable();
}

TEST_F(Ossfs2RandomWriteTest, verify_init_multipart_fail_is_retryable) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  // Pin chunk_size so base_part_size stays 1 MiB (align_up would otherwise
  // raise it to the 2 MiB default chunk and reroute the flush to PutObject).
  opts.random_write_chunk_size = 1 * 1024 * 1024;
  init(opts);
  verify_init_multipart_fail_is_retryable();
}

TEST_F(Ossfs2RandomWriteTest,
       verify_complete_multipart_fail_aborts_and_retries) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  // Pin chunk_size so base_part_size stays 1 MiB (align_up would otherwise
  // raise it to the 2 MiB default chunk and reroute the flush to PutObject).
  opts.random_write_chunk_size = 1 * 1024 * 1024;
  init(opts);
  verify_complete_multipart_fail_aborts_and_retries();
}

TEST_F(Ossfs2RandomWriteTest, verify_o_append_offsets_to_eof) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_o_append_offsets_to_eof();
}

// 100 KB base forces enlargement at 1 GiB (10000 * 100 KB ~ 977 MiB threshold).
TEST_F(Ossfs2RandomWriteTest, verify_dynamic_part_size_enlargement) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 100 * 1024;
  opts.random_write_chunk_size = 100 * 1024;
  init(opts);
  verify_dynamic_part_size_enlargement();
}

TEST_F(Ossfs2RandomWriteTest, verify_dynamic_part_size_copy_mix) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 100 * 1024;
  opts.random_write_chunk_size = 100 * 1024;
  init(opts);
  verify_dynamic_part_size_copy_mix();
}

// attr_timeout = 0 forces getattr to HEAD the remote on every call.
TEST_F(Ossfs2RandomWriteTest,
       verify_getattr_refreshes_remote_size_without_reopen) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.attr_timeout = 0;
  init(opts);
  verify_getattr_refreshes_remote_size_without_reopen();
}

TEST_F(Ossfs2RandomWriteTest, verify_get_on_write_against_emptied_remote) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_get_on_write_against_emptied_remote();
}

TEST(RandomWritePartSizeCalcTest, boundaries) {
  using OssFileSystem::calc_random_write_part_size;
  const uint64_t MiB = 1024ULL * 1024;
  const uint64_t GiB = 1024ULL * MiB;
  const uint64_t kMaxPartNumber = 10000;
  const uint64_t kMaxPartSize = 5 * GiB;

  EXPECT_EQ(8 * MiB, calc_random_write_part_size(1 * GiB, 8 * MiB, 2 * MiB));
  EXPECT_EQ(9 * MiB, calc_random_write_part_size(1 * GiB, 9 * MiB, 3 * MiB));
  EXPECT_EQ(4 * MiB, calc_random_write_part_size(1 * GiB, 4 * MiB, 4 * MiB));

  uint64_t file_size = kMaxPartNumber * 8 * MiB;
  EXPECT_EQ(8 * MiB, calc_random_write_part_size(file_size, 8 * MiB, 2 * MiB));

  uint64_t enlarged =
      calc_random_write_part_size(file_size + 1, 8 * MiB, 2 * MiB);
  EXPECT_EQ(10 * MiB, enlarged);
  EXPECT_EQ(0u, enlarged % (2 * MiB));
  EXPECT_LE((file_size + 1 + enlarged - 1) / enlarged, kMaxPartNumber);

  EXPECT_EQ(0u, calc_random_write_part_size(kMaxPartNumber * kMaxPartSize + 1,
                                            8 * MiB, 2 * MiB));

  EXPECT_EQ(0u, calc_random_write_part_size(1 * GiB, 6 * GiB, 6 * GiB));
}

TEST_F(Ossfs2RandomWriteTest, verify_read_while_write_clean_chunk_oss_failure) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_read_while_write_clean_chunk_oss_failure();
}

TEST_F(Ossfs2RandomWriteTest, verify_rename_path_refresh_scenarios) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_rename_path_refresh_scenarios();
}

TEST_F(Ossfs2RandomWriteTest, verify_standalone_truncate_open_failure) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  init(opts);
  verify_standalone_truncate_open_failure();
}

TEST_F(Ossfs2RandomWriteTest, verify_flush_efbig_when_base_part_too_large) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 6ULL * 1024 * 1024 * 1024;
  init(opts);
  verify_flush_efbig_when_base_part_too_large();
}

TEST_F(Ossfs2RandomWriteTest, verify_multipart_refill_and_copy_crc_faults) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 2 * 1024 * 1024;
  opts.random_write_chunk_size = 1 * 1024 * 1024;
  init(opts);
  verify_multipart_refill_and_copy_crc_faults();
}

INSTANTIATE_TEST_SUITE_P(FlushPaths, Ossfs2RandomWriteFlushRetryCrcTest,
                         ::testing::Values(static_cast<size_t>(1024 * 1024),
                                           static_cast<size_t>(4 * 1024 *
                                                               1024)));

TEST_P(Ossfs2RandomWriteFlushRetryCrcTest,
       verify_flush_retry_crc_after_body_writer_partial_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 2 * 1024 * 1024;
  opts.random_write_chunk_size = 1 * 1024 * 1024;
  init(opts);
  ASSERT_EQ(fs_->random_write_base_part_size(), 2ULL * 1024 * 1024)
      << "test assumes the TEST_F upload_buffer_size";
  flush_retry_crc_after_body_writer_partial_fail("rw_body_retry", GetParam());
}

TEST_F(Ossfs2RandomWriteTest, verify_max_file_size_limit) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.temp_dir = test_path_;
  opts.random_write_max_file_size = 8 * 1024 * 1024;
  init(opts);
  verify_max_file_size_limit();
}
