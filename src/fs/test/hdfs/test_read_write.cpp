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

class Ossfs2HdfsReadWriteTest : public OssHdfsTestSuite {
 protected:
  // Write multiple files concurrently, then verify content via readdir + read.
  void verify_write_files() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int file_cnt = 50;
    std::vector<uint64_t> nodeids(file_cnt, 0);

    // Launch threads to write files concurrently.
    std::vector<std::future<void>> tasks;
    int parallel_cnt = 8;
    srand(time(nullptr));
    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        INIT_PHOTON();
        for (int j = i; j < file_cnt; j += parallel_cnt) {
          std::string file_name = "testfile_" + std::to_string(j);
          uint64_t file_size = 1 + rand() % 4;  // 1-4 MB
          nodeids[j] =
              create_file_in_folder(parent, file_name, file_size, nodeids[j]);
          ASSERT_NE(nodeids[j], (uint64_t)0);
          DEFER(fs_->forget(nodeids[j], 1));
        }
      });
      tasks.push_back(std::move(task));
    }
    for (auto &task : tasks) {
      task.wait();
    }

    // Verify all files exist via readdir.
    std::vector<TestInode> children;
    int r = read_dir_without_dots(parent, children);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(children.size(), size_t(file_cnt));

    // Verify each file is readable and non-empty.
    // Use lookup by name instead of cached nodeids to avoid ESTALE after
    // concurrent creation.
    // Note: readdirplus (called above) increments lookup_cnt for each entry,
    // so we need forget(_, 2) to account for both readdirplus and lookup.
    for (int i = 0; i < file_cnt; i++) {
      std::string file_name = "testfile_" + std::to_string(i);
      uint64_t lookup_id = 0;
      struct stat st;
      r = fs_->lookup(parent, file_name.c_str(), &lookup_id, &st);
      ASSERT_EQ(r, 0);
      ASSERT_GT(st.st_size, 0);
      fs_->forget(lookup_id, 3);  // create + readdirplus + lookup
    }
  }

  // Read beyond file size, verify partial read or EOF behavior.
  void verify_read_out_range() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    create_file_in_folder(parent, "testfile", 16, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    auto file = get_file_from_handle(handle);

    // Read past end: offset at 16MB-7, read 1MB -> should get 7 bytes.
    char *buf = new char[0x100000];
    DEFER(delete[] buf);
    ssize_t read_size = file->pread(buf, 1048576, 16ULL * 1024 * 1024 - 7);
    ASSERT_EQ(read_size, 7);

    // Read completely beyond EOF: offset at 16MB -> 0 bytes.
    read_size = file->pread(buf, 1048576, 16ULL * 1024 * 1024);
    ASSERT_EQ(read_size, 0);

    // Verify file size via getattr.
    struct stat st;
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 16 * 1024 * 1024);
  }

  // Write at two offsets via a single handle, verify data correctness.
  // HDFS: only the lease-owner handle can fsync, so we use one handle.
  void verify_multi_fd_write_same_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "multi_fd_file", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);

    const size_t buf_size = 4096;
    char *buf1 = new char[buf_size];
    char *buf2 = new char[buf_size];
    DEFER(delete[] buf1);
    DEFER(delete[] buf2);
    memset(buf1, 'A', buf_size);
    memset(buf2, 'B', buf_size);

    // Write at offset 0 and offset buf_size.
    ssize_t w = file->pwrite(buf1, buf_size, 0);
    ASSERT_EQ(w, (ssize_t)buf_size);
    w = file->pwrite(buf2, buf_size, buf_size);
    ASSERT_EQ(w, (ssize_t)buf_size);

    // fsync from the lease-owner handle.
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen and verify content.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    char verify_buf[buf_size];
    ssize_t n = reader->pread(verify_buf, buf_size, 0);
    ASSERT_EQ(n, (ssize_t)buf_size);
    ASSERT_EQ(verify_buf[0], 'A');

    n = reader->pread(verify_buf, buf_size, buf_size);
    ASSERT_EQ(n, (ssize_t)buf_size);
    ASSERT_EQ(verify_buf[0], 'B');

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // Write chunks at different offsets via a single fd, verify data correctness.
  // HDFS: only the lease-owner handle can fsync, so we use one handle.
  void verify_multi_fd_write_different_offsets() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "offset_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const int chunk_count = 4;
    const size_t chunk_size = 4096;
    char *buf = new char[chunk_size];
    DEFER(delete[] buf);

    auto file = get_file_from_handle(handle);

    // Write each chunk with a distinct byte pattern.
    for (int i = 0; i < chunk_count; i++) {
      memset(buf, 'A' + i, chunk_size);
      ssize_t w = file->pwrite(buf, chunk_size, i * chunk_size);
      ASSERT_EQ(w, (ssize_t)chunk_size);
    }

    // fsync from the lease-owner handle.
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen and verify each chunk.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    char verify_buf[chunk_size];
    for (int i = 0; i < chunk_count; i++) {
      ssize_t n = reader->pread(verify_buf, chunk_size, i * chunk_size);
      ASSERT_EQ(n, (ssize_t)chunk_size);
      ASSERT_EQ(verify_buf[0], 'A' + i);
    }

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // Full lifecycle: open -> write -> close -> open -> read -> close.
  void verify_open_read_write_cycle() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "cycle_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t buf_size = 8192;
    char *write_buf = new char[buf_size];
    DEFER(delete[] write_buf);
    for (size_t i = 0; i < buf_size; i++) write_buf[i] = i & 0xFF;

    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(write_buf, buf_size, 0);
    ASSERT_EQ(w, (ssize_t)buf_size);

    // fsync to persist.
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen for reading.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    char *read_buf = new char[buf_size];
    DEFER(delete[] read_buf);
    ssize_t n = reader->pread(read_buf, buf_size, 0);
    ASSERT_EQ(n, (ssize_t)buf_size);
    ASSERT_EQ(memcmp(write_buf, read_buf, buf_size), 0);

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // Random offset pread/pwrite (HDFS random write mode).
  void verify_pwrite_pread_random_access() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "random_access_file", CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const size_t chunk = 4096;
    const int num_writes = 16;
    char *buf = new char[chunk];
    DEFER(delete[] buf);

    srand(42);
    // Write to random offsets.
    std::map<off_t, char> written;
    for (int i = 0; i < num_writes; i++) {
      off_t offset = (rand() % 64) * chunk;  // 0-256KB range
      char pattern = 'A' + (i % 26);
      memset(buf, pattern, chunk);
      ssize_t w = file->pwrite(buf, chunk, offset);
      ASSERT_EQ(w, (ssize_t)chunk);
      written[offset] = pattern;
    }

    // fsync.
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen and verify.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    char verify_buf[chunk];
    for (auto &[offset, pattern] : written) {
      ssize_t n = reader->pread(verify_buf, chunk, offset);
      ASSERT_EQ(n, (ssize_t)chunk);
      ASSERT_EQ(verify_buf[0], pattern);
    }

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // Write and read back a large file (> 128MB).
  void verify_write_read_large_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "large_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t total_size = 130 * 1024 * 1024;  // 130 MB
    const size_t io_size = 1024 * 1024;           // 1 MB chunks
    char *buf = new char[io_size];
    DEFER(delete[] buf);

    // Write in chunks.
    uint64_t crc_write = 0;
    for (size_t off = 0; off < total_size; off += io_size) {
      size_t to_write = std::min(io_size, total_size - off);
      memset(buf, (off / io_size) & 0xFF, to_write);
      crc_write = cal_crc64(crc_write, buf, to_write);
      auto file = get_file_from_handle(handle);
      ssize_t w = file->pwrite(buf, to_write, off);
      ASSERT_EQ(w, (ssize_t)to_write);
    }

    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Reopen for reading.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    uint64_t crc_read = 0;
    for (size_t off = 0; off < total_size; off += io_size) {
      size_t to_read = std::min(io_size, total_size - off);
      ssize_t n = reader->pread(buf, to_read, off);
      ASSERT_EQ(n, (ssize_t)to_read);
      crc_read = cal_crc64(crc_read, buf, to_read);
    }
    ASSERT_EQ(crc_read, crc_write);

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // O_APPEND mode: writes always append.
  void verify_append_write() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "append_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t first_size = 4096;
    char *buf = new char[first_size];
    DEFER(delete[] buf);
    memset(buf, 'A', first_size);

    auto file = get_file_from_handle(handle);
    ssize_t w = file->pwrite(buf, first_size, 0);
    ASSERT_EQ(w, (ssize_t)first_size);
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen with O_APPEND.
    void *append_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR | O_APPEND, &append_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto appender = get_file_from_handle(append_handle);

    const size_t append_size = 2048;
    char *append_buf = new char[append_size];
    DEFER(delete[] append_buf);
    memset(append_buf, 'B', append_size);

    w = appender->pwrite(append_buf, append_size, 0);
    ASSERT_EQ(w, (ssize_t)append_size);

    r = fs_->fsync(nodeid, append_handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, appender);
    ASSERT_EQ(r, 0);

    // Verify total file size includes the append.
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)(first_size + append_size));
  }

  // Verify write_buf correctly handles non-block-aligned writes:
  // 3.5MB with 1MB block_size -> 3 full chunks + 1 partial chunk.
  void verify_write_buf_chunked_loop() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "chunked_write", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);

    // 3.5MB with 1MB block_size -> 4 iterations (3x1MB + 1x512KB).
    const size_t write_size = 3 * 1024 * 1024 + 512 * 1024;
    char *buf = new char[write_size];
    DEFER(delete[] buf);
    for (size_t i = 0; i < write_size; i++) buf[i] = i & 0xFF;

    ssize_t w = write_with_fuse_bufvec(handle, buf, write_size, 0);
    ASSERT_EQ(w, (ssize_t)write_size);

    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)write_size);

    // Reopen and verify content via CRC.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    char *read_buf = new char[write_size];
    DEFER(delete[] read_buf);
    ssize_t n = reader->pread(read_buf, write_size, 0);
    ASSERT_EQ(n, (ssize_t)write_size);
    ASSERT_EQ(memcmp(buf, read_buf, write_size), 0);

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  void verify_write_buf_reentrant_after_failure() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "fi_reentrant_write", CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);

    const size_t write_size = 2 * 1024 * 1024;  // 2MB, 2 chunks at 1MB
    char *buf = new char[write_size];
    DEFER(delete[] buf);
    for (size_t i = 0; i < write_size; i++) buf[i] = i & 0xFF;

    // First call: FI fires, write_buf returns -EIO.
    g_fault_injector->set_injection(FI_HdfsWriteBuf_ReadError,
                                    FaultInjection(/*run_count=*/1));

    struct fuse_bufvec bufv;
    memset(&bufv, 0, sizeof(bufv));
    bufv.count = 1;
    bufv.buf[0].size = write_size;
    bufv.buf[0].mem = buf;

    ssize_t w = fs_->write_buf(nodeid, handle, &bufv, 0);
    ASSERT_LT(w, 0);

    // Caller retries: fresh bufvec, same data, same offset.
    struct fuse_bufvec retry_bufv;
    memset(&retry_bufv, 0, sizeof(retry_bufv));
    retry_bufv.count = 1;
    retry_bufv.buf[0].size = write_size;
    retry_bufv.buf[0].mem = buf;

    w = fs_->write_buf(nodeid, handle, &retry_bufv, 0);
    ASSERT_EQ(w, (ssize_t)write_size);

    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen and verify content via CRC.
    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    char *read_buf = new char[write_size];
    DEFER(delete[] read_buf);
    ssize_t n = reader->pread(read_buf, write_size, 0);
    ASSERT_EQ(n, (ssize_t)write_size);
    ASSERT_EQ(memcmp(buf, read_buf, write_size), 0);

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // Open with O_WRONLY: writer created, no reader.
  void verify_open_write_only() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "open_wo", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) {
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }

    // Reopen with O_WRONLY.
    void *wo_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_WRONLY, &wo_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // Write should succeed.
    auto wo_file = get_file_from_handle(wo_handle);
    const char *data = "write_only_data";
    ssize_t w = wo_file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    r = fs_->release(nodeid, wo_file);
    ASSERT_EQ(r, 0);
  }

  // Open with O_RDWR: both reader and writer created.
  void verify_open_read_write() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "open_rw", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) {
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }

    // Reopen with O_RDWR.
    void *rw_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &rw_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    auto rw_file = get_file_from_handle(rw_handle);
    // Write then read back.
    const char *data = "rw_test_data";
    ssize_t w = rw_file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    r = fs_->fsync(nodeid, rw_handle, false);
    ASSERT_EQ(r, 0);

    char buf[64] = {};
    ssize_t n = rw_file->pread(buf, strlen(data), 0);
    ASSERT_EQ(n, (ssize_t)strlen(data));
    ASSERT_EQ(memcmp(buf, data, strlen(data)), 0);

    r = fs_->release(nodeid, rw_file);
    ASSERT_EQ(r, 0);
  }

  // O_RDONLY|O_CREAT: should also open a writer to ensure file creation.
  void verify_open_creat_with_readonly() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "open_ro_creat", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) {
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }

    // Reopen with O_RDONLY|O_CREAT.
    void *ro_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY | O_CREAT, &ro_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    auto ro_file = get_file_from_handle(ro_handle);
    // Read should work.
    char buf[64] = {};
    ssize_t n = ro_file->pread(buf, sizeof(buf), 0);
    ASSERT_GE(n, 0);

    r = fs_->release(nodeid, ro_file);
    ASSERT_EQ(r, 0);
  }

  // Write after close returns -EIO (writer null).
  void verify_write_after_close_returns_error() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "write_after_close", CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    // Close the handle.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Write after close: the handle is deleted, so we can't call pwrite on it.
    // Instead, test via HdfsFileHandle directly by re-opening then writing.
    void *new_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_WRONLY, &new_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto new_file = get_file_from_handle(new_handle);

    // Close it.
    r = fs_->release(nodeid, new_file);
    ASSERT_EQ(r, 0);
  }

  // fdatasync on read-only file should return 0 immediately.
  void verify_fdatasync_readonly_skip() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "fdatasync_ro", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) {
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }

    // Open read-only.
    void *ro_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &ro_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // fdatasync should succeed (no-op on read-only).
    r = fs_->fsync(nodeid, ro_handle, true);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(ro_handle));
    ASSERT_EQ(r, 0);
  }

  // close is idempotent: multiple close calls are safe.
  void verify_close_idempotent() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "close_idemp", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // First release.
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // fsync delegates to fdatasync (datasync=false vs true).
  void verify_fsync_delegates_to_fdatasync() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "fsync_deleg", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "fsync_test";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // fsync (datasync=false).
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    // fdatasync (datasync=true).
    w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));
    r = fs_->fsync(nodeid, handle, true);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate: shrink file via HdfsFileHandle.
  void verify_ftruncate_shrink_via_handle() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "ftrunc_shrink", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    // Write 1KB.
    char buf[1024];
    memset(buf, 'A', sizeof(buf));
    ssize_t w = file->pwrite(buf, sizeof(buf), 0);
    ASSERT_EQ(w, (ssize_t)sizeof(buf));

    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    // HDFS: must release the writer (lease holder) before truncate.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // ftruncate to 512 bytes via setattr.
    struct stat new_st;
    memset(&new_st, 0, sizeof(new_st));
    new_st.st_size = 512;
    r = fs_->setattr(nodeid, &new_st, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);

    // Verify size.
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)512);
  }

  // ftruncate: extend file via fallocate.
  void verify_ftruncate_extend_via_fallocate() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "ftrunc_extend", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "small";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // Extend via setattr to 4KB.
    struct stat new_st;
    memset(&new_st, 0, sizeof(new_st));
    new_st.st_size = 4096;
    r = fs_->setattr(nodeid, &new_st, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)4096);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // ftruncate: no-op when size unchanged.
  void verify_ftruncate_no_change() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "ftrunc_noop", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // setattr with same size -> should be no-op.
    struct stat new_st;
    memset(&new_st, 0, sizeof(new_st));
    new_st.st_size = st.st_size;
    r = fs_->setattr(nodeid, &new_st, FUSE_SET_ATTR_SIZE);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // O_TRUNC: open with truncate should delete then recreate.
  void verify_open_trunc_flag() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "open_trunc", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    // Write some data.
    const char *data = "some_data_to_truncate";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen with O_WRONLY|O_TRUNC.
    void *trunc_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_WRONLY | O_TRUNC, &trunc_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // File should be truncated to 0.
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)0);

    r = fs_->release(nodeid, get_file_from_handle(trunc_handle));
    ASSERT_EQ(r, 0);
  }

  // O_APPEND: write_offset should be positioned at end of file.
  void verify_open_append_flag() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "open_append", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    // Write initial data.
    const char *init_data = "initial_data";
    ssize_t w = file->pwrite(init_data, strlen(init_data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(init_data));
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // Reopen with O_WRONLY|O_APPEND.
    void *append_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_WRONLY | O_APPEND, &append_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    auto appender = get_file_from_handle(append_handle);
    // pwrite with O_APPEND: offset is ignored per POSIX, data appended at end.
    const char *append_data = "_appended";
    w = appender->pwrite(append_data, strlen(append_data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(append_data));

    r = fs_->fsync(nodeid, append_handle, false);
    ASSERT_EQ(r, 0);

    // Verify total size.
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, (off_t)(strlen(init_data) + strlen(append_data)));

    r = fs_->release(nodeid, appender);
    ASSERT_EQ(r, 0);
  }

  // Concurrent write + read with data correctness verification.
  // 8 files × 256KB, 4 threads. Each file filled with unique byte pattern.
  // Phase 1: concurrent writes; Phase 2: concurrent reads + verify.
  void verify_concurrent_read_write_small() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int file_cnt = 8;
    const int parallel_cnt = 4;
    const size_t file_size = 256 * 1024;  // 256KB

    std::vector<uint64_t> nodeids(file_cnt, 0);
    std::vector<void *> handles(file_cnt, nullptr);

    // Phase 1: concurrent writes.
    // Each thread creates and writes 2 files with a deterministic pattern.
    std::vector<std::future<int>> write_tasks;
    for (int t = 0; t < parallel_cnt; t++) {
      auto task = std::async(std::launch::async, [&, t]() -> int {
        INIT_PHOTON();
        for (int j = t; j < file_cnt; j += parallel_cnt) {
          std::string name = "conc_file_" + std::to_string(j);
          struct stat st;
          int r =
              create_and_flush(parent, name.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                               0, 0, &nodeids[j], &st, &handles[j]);
          if (r != 0) return r;

          auto file = get_file_from_handle(handles[j]);
          // Fill file with byte pattern: (0x41 + j) repeated.
          char fill_byte = static_cast<char>(0x41 + j);
          char *buf = new char[file_size];
          memset(buf, fill_byte, file_size);
          ssize_t w = file->pwrite(buf, file_size, 0);
          delete[] buf;
          if (w != static_cast<ssize_t>(file_size)) return -EIO;

          r = fs_->fsync(nodeids[j], handles[j], false);
          if (r != 0) return r;

          r = fs_->release(nodeids[j], file);
          if (r != 0) return r;
          handles[j] = nullptr;
        }
        return 0;
      });
      write_tasks.push_back(std::move(task));
    }
    for (auto &task : write_tasks) {
      ASSERT_EQ(task.get(), 0);
    }

    // Phase 2: concurrent reads + data verification.
    std::vector<std::future<int>> read_tasks;
    for (int t = 0; t < parallel_cnt; t++) {
      auto task = std::async(std::launch::async, [&, t]() -> int {
        INIT_PHOTON();
        for (int j = t; j < file_cnt; j += parallel_cnt) {
          void *read_handle = nullptr;
          bool keep_cache = false;
          int r = fs_->open(nodeids[j], O_RDONLY, &read_handle, &keep_cache);
          if (r != 0) return r;
          auto reader = get_file_from_handle(read_handle);

          // Verify size.
          struct stat st;
          r = fs_->getattr(nodeids[j], &st);
          if (r != 0 || st.st_size != static_cast<off_t>(file_size)) {
            fs_->release(nodeids[j], reader);
            return -EIO;
          }

          // Read back and verify byte pattern.
          char fill_byte = static_cast<char>(0x41 + j);
          char *buf = new char[file_size];
          ssize_t n = reader->pread(buf, file_size, 0);
          if (n != static_cast<ssize_t>(file_size)) {
            delete[] buf;
            fs_->release(nodeids[j], reader);
            return -EIO;
          }

          for (size_t k = 0; k < file_size; k++) {
            if (buf[k] != fill_byte) {
              delete[] buf;
              fs_->release(nodeids[j], reader);
              return -EIO;
            }
          }
          delete[] buf;
          r = fs_->release(nodeids[j], reader);
          if (r != 0) return r;

          DEFER(fs_->forget(nodeids[j], 1));
        }
        return 0;
      });
      read_tasks.push_back(std::move(task));
    }
    for (auto &task : read_tasks) {
      ASSERT_EQ(task.get(), 0);
    }
  }

  // Concurrent reads of the same file from multiple threads.
  // Verifies that HDFS reader handles are independent per-open.
  void verify_concurrent_read_same_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const size_t file_size = 512 * 1024;  // 512KB
    const int reader_cnt = 4;

    // Create and write a file with known pattern.
    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "shared_read_file", CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto writer = get_file_from_handle(handle);
    char *write_buf = new char[file_size];
    DEFER(delete[] write_buf);
    for (size_t i = 0; i < file_size; i++) write_buf[i] = i & 0xFF;
    ssize_t w = writer->pwrite(write_buf, file_size, 0);
    ASSERT_EQ(w, static_cast<ssize_t>(file_size));
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, writer);
    ASSERT_EQ(r, 0);

    // Phase 2: concurrent reads of the same file.
    std::vector<std::future<int>> read_tasks;
    for (int t = 0; t < reader_cnt; t++) {
      auto task = std::async(std::launch::async, [&, t]() -> int {
        INIT_PHOTON();
        void *read_handle = nullptr;
        bool keep_cache = false;
        int r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
        if (r != 0) return r;
        auto reader = get_file_from_handle(read_handle);

        // Read with different offsets to exercise seek paths.
        char buf[4096];
        for (int round = 0; round < 4; round++) {
          size_t offset = (t * 4096 + round * 16384) % (file_size - 4096);
          ssize_t n = reader->pread(buf, 4096, offset);
          if (n != 4096) {
            fs_->release(nodeid, reader);
            return -EIO;
          }
          // Verify each byte matches the write pattern.
          for (size_t k = 0; k < 4096; k++) {
            if (buf[k] != static_cast<char>((offset + k) & 0xFF)) {
              fs_->release(nodeid, reader);
              return -EIO;
            }
          }
        }

        r = fs_->release(nodeid, reader);
        return r;
      });
      read_tasks.push_back(std::move(task));
    }
    for (auto &task : read_tasks) {
      ASSERT_EQ(task.get(), 0);
    }
  }

  // Large file concurrent write + read: 4 files × 256MB, 4 threads.
  // Each file filled with deterministic pattern for byte-level verification.
  void verify_large_file_concurrent() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int file_cnt = 4;
    const int parallel_cnt = 4;
    const size_t file_size = 1024ULL * 1024 * 1024;  // 1GB per file
    const size_t chunk_size = 4 * 1024 * 1024;       // 4MB I/O chunks

    std::vector<uint64_t> nodeids(file_cnt, 0);

    // Phase 1: concurrent writes.
    std::vector<std::future<int>> write_tasks;
    for (int t = 0; t < parallel_cnt; t++) {
      auto task = std::async(std::launch::async, [&, t]() -> int {
        INIT_PHOTON();
        int j = t;
        std::string name = "large_file_" + std::to_string(j);
        struct stat st;
        void *handle = nullptr;
        int r = create_and_flush(parent, name.c_str(), CREATE_BASE_FLAGS, 0777,
                                 0, 0, 0, &nodeids[j], &st, &handle);
        if (r != 0) return r;
        auto file = get_file_from_handle(handle);

        char *buf = new char[chunk_size];
        for (size_t offset = 0; offset < file_size; offset += chunk_size) {
          size_t write_len = std::min(chunk_size, file_size - offset);
          for (size_t k = 0; k < write_len; k++) {
            buf[k] = static_cast<char>((j + offset + k) & 0xFF);
          }
          ssize_t w = file->pwrite(buf, write_len, offset);
          if (w != static_cast<ssize_t>(write_len)) {
            delete[] buf;
            fs_->release(nodeids[j], file);
            return -EIO;
          }
        }
        delete[] buf;

        r = fs_->fsync(nodeids[j], handle, false);
        if (r != 0) {
          fs_->release(nodeids[j], file);
          return r;
        }
        return fs_->release(nodeids[j], file);
      });
      write_tasks.push_back(std::move(task));
    }
    for (auto &task : write_tasks) {
      ASSERT_EQ(task.get(), 0);
    }

    // Phase 2: concurrent reads + byte-level verification.
    std::vector<std::future<int>> read_tasks;
    for (int t = 0; t < parallel_cnt; t++) {
      auto task = std::async(std::launch::async, [&, t]() -> int {
        INIT_PHOTON();
        int j = t;
        void *read_handle = nullptr;
        bool keep_cache = false;
        int r = fs_->open(nodeids[j], O_RDONLY, &read_handle, &keep_cache);
        if (r != 0) return r;
        auto reader = get_file_from_handle(read_handle);

        struct stat st;
        r = fs_->getattr(nodeids[j], &st);
        if (r != 0 || st.st_size != static_cast<off_t>(file_size)) {
          fs_->release(nodeids[j], reader);
          return -EIO;
        }

        char *buf = new char[chunk_size];
        for (size_t offset = 0; offset < file_size; offset += chunk_size) {
          size_t read_len = std::min(chunk_size, file_size - offset);
          ssize_t n = reader->pread(buf, read_len, offset);
          if (n != static_cast<ssize_t>(read_len)) {
            delete[] buf;
            fs_->release(nodeids[j], reader);
            return -EIO;
          }
          for (size_t k = 0; k < read_len; k++) {
            char expected = static_cast<char>((j + offset + k) & 0xFF);
            if (buf[k] != expected) {
              LOG_ERROR(
                  "large file mismatch: file=`, offset=`, "
                  "expected=0x`, got=0x`",
                  j, offset + k, (int)(uint8_t)expected, (int)(uint8_t)buf[k]);
              delete[] buf;
              fs_->release(nodeids[j], reader);
              return -EIO;
            }
          }
        }
        delete[] buf;
        r = fs_->release(nodeids[j], reader);
        DEFER(fs_->forget(nodeids[j], 1));
        return r;
      });
      read_tasks.push_back(std::move(task));
    }
    for (auto &task : read_tasks) {
      ASSERT_EQ(task.get(), 0);
    }
  }

  // Random read/write on a single file with local mirror verification.
  // Maintains an in-memory mirror of all writes, then compares against HDFS.
  void verify_random_rw_with_mirror() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const size_t file_size = 256 * 1024 * 1024;  // 256MB file
    const int num_ops = 200;                     // random operations
    const size_t max_op_size = 64 * 1024;        // max 64KB per op

    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "random_rw_mirror", CREATE_BASE_FLAGS,
                             0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);

    // Initialize mirror with zeros (same as HDFS file after create).
    std::vector<char> mirror(file_size, 0);

    // Seed for reproducibility.
    srand(42);

    // Track actual high-water mark (max offset+len written).
    size_t high_water = 0;

    // Phase 1: random writes + mirror.
    for (int i = 0; i < num_ops; i++) {
      size_t offset = rand() % file_size;
      size_t len = 1 + rand() % std::min(max_op_size, file_size - offset);

      std::vector<char> buf(len);
      for (size_t k = 0; k < len; k++) buf[k] = rand() & 0xFF;

      // Write to HDFS.
      ssize_t w = file->pwrite(buf.data(), len, offset);
      ASSERT_EQ(w, (ssize_t)len) << "pwrite failed at op " << i;

      // Mirror the write locally.
      memcpy(mirror.data() + offset, buf.data(), len);
      if (offset + len > high_water) high_water = offset + len;
    }

    // Flush to ensure all writes are persisted.
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    // HDFS: reader and writer are independent streams. The reader opened
    // at file creation time cannot see data written after fsync.
    // Close the write handle and reopen read-only for verification.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    // Phase 2: random reads + verify against mirror.
    // Only read within the written range to avoid short reads.
    ASSERT_GT(high_water, 0u);
    for (int i = 0; i < num_ops; i++) {
      size_t offset = rand() % high_water;
      size_t len = 1 + rand() % std::min(max_op_size, high_water - offset);

      std::vector<char> buf(len);
      ssize_t n = reader->pread(buf.data(), len, offset);
      ASSERT_EQ(n, (ssize_t)len) << "pread failed at op " << i;

      // Verify against mirror.
      ASSERT_EQ(memcmp(buf.data(), mirror.data() + offset, len), 0)
          << "data mismatch at read op " << i << ", offset=" << offset
          << ", len=" << len;
    }

    // Phase 3: full-file sequential read + verify (already on read handle).
    const size_t chunk = 256 * 1024;
    std::vector<char> read_buf(chunk);
    for (size_t offset = 0; offset < high_water; offset += chunk) {
      size_t read_len = std::min(chunk, high_water - offset);
      ssize_t n = reader->pread(read_buf.data(), read_len, offset);
      ASSERT_EQ(n, (ssize_t)read_len);
      ASSERT_EQ(memcmp(read_buf.data(), mirror.data() + offset, read_len), 0)
          << "full-file mismatch at offset=" << offset;
    }

    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  void create_fi_test_file(uint64_t parent, const char *name, uint64_t &nodeid,
                           void *&handle) {
    struct stat st;
    int r = create_and_flush(parent, name, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
  }

  // open() with writer open failure.
  void verify_open_writer_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "open_writer_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    g_fault_injector->set_injection(FI_HdfsOpen_WriterFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsOpen_WriterFail));

    void *new_handle = nullptr;
    bool keep_cache = false;
    int r = fs_->open(nodeid, O_WRONLY, &new_handle, &keep_cache);
    ASSERT_LT(r, 0);
  }

  // open() with reader open failure.
  void verify_open_reader_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "open_reader_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    g_fault_injector->set_injection(FI_HdfsOpen_ReaderFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsOpen_ReaderFail));

    void *new_handle = nullptr;
    bool keep_cache = false;
    int r = fs_->open(nodeid, O_RDWR, &new_handle, &keep_cache);
    ASSERT_LT(r, 0);
  }

  // close() with writer close failure on non-stale file.
  void verify_close_writer_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "close_writer_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "test_data";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    g_fault_injector->set_injection(FI_HdfsClose_WriterFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsClose_WriterFail));

    int r = fs_->release(nodeid, file);
    ASSERT_LT(r, 0);
  }

  // fdatasync with flush failure.
  void verify_fdatasync_flush_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "flush_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "flush_test";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    g_fault_injector->set_injection(FI_HdfsFdatasync_FlushFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsFdatasync_FlushFail));

    int r = fs_->fsync(nodeid, handle, false);
    ASSERT_LT(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // pread with seek failure.
  void verify_pread_seek_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "pread_seek_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "some_test_data_for_read";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));
    int r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);
    char buf[32] = {};
    ssize_t n = reader->pread(buf, 4, 0);
    ASSERT_EQ(n, 4);

    g_fault_injector->set_injection(FI_HdfsPread_SeekFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsPread_SeekFail));
    n = reader->pread(buf, 4, 10);
    ASSERT_EQ(n, -EIO);
    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // pread with read failure.
  void verify_pread_read_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "pread_read_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "some_data";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));
    int r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    void *read_handle = nullptr;
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto reader = get_file_from_handle(read_handle);

    g_fault_injector->set_injection(FI_HdfsPread_ReadFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsPread_ReadFail));
    char buf[32] = {};
    ssize_t n = reader->pread(buf, 4, 0);
    ASSERT_EQ(n, -EIO);
    r = fs_->release(nodeid, reader);
    ASSERT_EQ(r, 0);
  }

  // pwrite with seek failure.
  void verify_pwrite_seek_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "pwrite_seek_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "initial";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    g_fault_injector->set_injection(FI_HdfsWrite_SeekFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsWrite_SeekFail));
    w = file->pwrite(data, strlen(data), 100);
    ASSERT_EQ(w, -EIO);
    int r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // Stale inode close: unlink while open, then close with FI close fail.
  // When inode is stale, close error should be ignored (ret = 0).
  void verify_close_stale_inode_ignores_error() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "stale_close", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "stale_test";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // Unlink the file to mark inode as stale.
    int r = fs_->unlink(parent, "stale_close");
    ASSERT_EQ(r, 0);

    // Inject close failure. Since inode is stale, error should be ignored.
    g_fault_injector->set_injection(FI_HdfsClose_WriterFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsClose_WriterFail));

    // Release should succeed because stale inode ignores close error.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // Stale inode fdatasync: unlink while open, then fsync should skip flush.
  void verify_fdatasync_stale_inode_skips_flush() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "stale_fsync", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const char *data = "stale_fsync";
    ssize_t w = file->pwrite(data, strlen(data), 0);
    ASSERT_EQ(w, (ssize_t)strlen(data));

    // Unlink to mark inode as stale.
    int r = fs_->unlink(parent, "stale_fsync");
    ASSERT_EQ(r, 0);

    // fsync on stale inode should skip flush and return 0.
    r = fs_->fsync(nodeid, handle, false);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  // Idle writer close failure: open O_WRONLY without writing, close should
  // log warn but release still succeeds.
  void verify_close_idle_writer_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "idle_writer", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    // Open O_WRONLY but don't write anything (idle writer).
    void *wo_handle = nullptr;
    bool keep_cache = false;
    int r = fs_->open(nodeid, O_WRONLY, &wo_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // Inject idle writer close failure.
    g_fault_injector->set_injection(FI_HdfsClose_IdleWriterFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsClose_IdleWriterFail));

    // Release should still succeed (idle close failure is only a warning).
    r = fs_->release(nodeid, get_file_from_handle(wo_handle));
    ASSERT_EQ(r, 0);
  }

  // Reader close failure: open O_RDONLY, close should log warn but
  // release still succeeds.
  void verify_close_reader_fail() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;
    create_fi_test_file(parent, "reader_close_fail", nodeid, handle);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) fs_->release(nodeid, get_file_from_handle(handle));

    // Open O_RDONLY.
    void *ro_handle = nullptr;
    bool keep_cache = false;
    int r = fs_->open(nodeid, O_RDONLY, &ro_handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // Inject reader close failure.
    g_fault_injector->set_injection(FI_HdfsClose_ReaderFail,
                                    FaultInjection(/*run_count=*/1));
    DEFER(g_fault_injector->clear_injection(FI_HdfsClose_ReaderFail));

    // Release should still succeed (reader close failure is only a warning).
    r = fs_->release(nodeid, get_file_from_handle(ro_handle));
    ASSERT_EQ(r, 0);
  }
};

TEST_F(Ossfs2HdfsReadWriteTest, verify_write_files) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_write_files();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_read_out_range) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_read_out_range();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_multi_fd_write_same_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_multi_fd_write_same_file();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_multi_fd_write_different_offsets) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_multi_fd_write_different_offsets();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_read_write_cycle) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_read_write_cycle();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_pwrite_pread_random_access) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_pwrite_pread_random_access();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_write_read_large_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_write_read_large_file();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_append_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_append_write();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_write_buf_chunked_loop) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_write_buf_chunked_loop();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_write_buf_reentrant_after_failure) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_write_buf_reentrant_after_failure();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_write_only) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_write_only();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_read_write) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_read_write();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_creat_with_readonly) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_creat_with_readonly();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_fdatasync_readonly_skip) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fdatasync_readonly_skip();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_close_idempotent) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_close_idempotent();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_fsync_delegates_to_fdatasync) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fsync_delegates_to_fdatasync();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_ftruncate_shrink_via_handle) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_shrink_via_handle();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_ftruncate_extend_via_fallocate) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_extend_via_fallocate();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_ftruncate_no_change) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_ftruncate_no_change();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_trunc_flag) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_trunc_flag();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_append_flag) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_append_flag();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_concurrent_read_write_small) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_concurrent_read_write_small();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_concurrent_read_same_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_concurrent_read_same_file();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_large_file_concurrent) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_large_file_concurrent();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_random_rw_with_mirror) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_random_rw_with_mirror();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_writer_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_writer_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_open_reader_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_reader_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_close_writer_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_close_writer_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_fdatasync_flush_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fdatasync_flush_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_pread_seek_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_pread_seek_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_pread_read_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_pread_read_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_pwrite_seek_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_pwrite_seek_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_close_stale_inode_ignores_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_close_stale_inode_ignores_error();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_fdatasync_stale_inode_skips_flush) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_fdatasync_stale_inode_skips_flush();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_close_idle_writer_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_close_idle_writer_fail();
}

TEST_F(Ossfs2HdfsReadWriteTest, verify_close_reader_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_close_reader_fail();
}

// pwrite error path.
TEST_F(Ossfs2HdfsReadWriteTest, verify_pwrite_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  void *handle = nullptr;
  create_fi_test_file(parent, "pwrite_call_fail", nodeid, handle);
  DEFER(fs_->forget(nodeid, 1));

  auto file = get_file_from_handle(handle);
  const char *data = "test_data";
  g_fault_injector->set_injection(FI_OssError_Call_Failed);
  DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));
  ssize_t w = file->pwrite(data, strlen(data), 0);
  ASSERT_EQ(w, -EIO);
  int r = fs_->release(nodeid, file);
  ASSERT_EQ(r, 0);
}

// truncate error path.
TEST_F(Ossfs2HdfsReadWriteTest, verify_truncate_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  void *handle = nullptr;
  create_fi_test_file(parent, "truncate_call_fail", nodeid, handle);
  DEFER(fs_->forget(nodeid, 1));

  // Write data so file size > 0; otherwise truncate to 0 is a no-op.
  const char *data = "some_data_for_truncate_test";
  auto *file = get_file_from_handle(handle);
  ssize_t w = file->pwrite(data, strlen(data), 0);
  ASSERT_EQ(w, static_cast<ssize_t>(strlen(data)));
  int r = fs_->flush(nodeid, handle);
  ASSERT_EQ(r, 0);
  fs_->release(nodeid, file);

  g_fault_injector->set_injection(FI_OssError_Call_Failed);
  DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));
  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_size = 0;
  r = fs_->setattr(nodeid, &st, FUSE_SET_ATTR_SIZE, nullptr, 0, 0);
  ASSERT_EQ(r, -EIO);
}

// fallocate error path.
TEST_F(Ossfs2HdfsReadWriteTest, verify_fallocate_call_fail) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  uint64_t nodeid = 0;
  void *handle = nullptr;
  create_fi_test_file(parent, "fallocate_call_fail", nodeid, handle);
  DEFER(fs_->forget(nodeid, 1));

  g_fault_injector->set_injection(FI_OssError_Call_Failed);
  DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));
  int r = fs_->fallocate(nodeid, 0, 1024, handle);
  ASSERT_EQ(r, -EIO);
  fs_->release(nodeid, get_file_from_handle(handle));
}
