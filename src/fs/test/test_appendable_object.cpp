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

class Ossfs2AppendableObjectTest : public Ossfs2TestSuite {
 protected:
  void verify_enable_appendable_object() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;

    std::string filepath = "testfile_appendable";

    // no need O_APPEND, we always use appendable object in this mode
    int r = create_and_flush(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);

    fs_->release(nodeid, get_file_from_handle(handle));
    DEFER(fs_->forget(nodeid, 1));

    auto meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ("0", meta["Content-Length"]);

    bool unused = 0;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto write_file = get_file_from_handle(handle);

    const int buffer_size = 1024 * 1024;
    char *mem = (char *)malloc(buffer_size * 4);
    DEFER(free(mem));
    for (int i = 0; i < buffer_size * 4; i++) {
      mem[i] = random_string(1)[0];
    }

    off_t write_offset = 0;
    size_t file_size = 0;

    // append 1048576 bytes (one upload buffer)
    r = write_file->pwrite(mem, 1048576, write_offset);
    ASSERT_EQ(r, 1048576);

    file_size += r;

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ("1048576", meta["Content-Length"]);

    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto read_file = get_file_from_handle(handle);
    char *read_buf = (char *)malloc(buffer_size * 4);
    DEFER(free(read_buf));

    r = read_file->pread(read_buf, buffer_size, write_offset);
    ASSERT_EQ(r, buffer_size);

    for (size_t i = 0; i < file_size; i++) {
      ASSERT_EQ(read_buf[i], mem[i]);
    }

    write_offset += 1048576;

    // test invalid offset
    r = write_file->pwrite(mem, 444, 111);
    ASSERT_TRUE(r < 0);

    // append 444 bytes
    r = write_file->pwrite(mem + write_offset, 444, write_offset);
    ASSERT_EQ(r, 444);
    file_size += r;

    r = read_file->pread(read_buf + write_offset, 444, write_offset);
    ASSERT_EQ(r, 444);

    write_offset += 444;

    for (size_t i = 0; i < file_size; i++) {
      ASSERT_EQ(read_buf[i], mem[i]);
    }

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ("1048576", meta["Content-Length"]);

    // flush data to oss
    r = write_file->fsync();
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ(std::to_string(file_size), meta["Content-Length"]);

    // append 4097 bytes
    r = write_file->pwrite(mem + write_offset, 4097, write_offset);
    ASSERT_EQ(r, 4097);
    file_size += r;

    // read cross one buffer and both clean and dirty data
    r = read_file->pread(read_buf + 524288, 1048576, 524288);
    ASSERT_EQ(r, ssize_t(file_size - 524288));

    for (size_t i = 0; i < file_size; i++) {
      ASSERT_EQ(read_buf[i], mem[i]);
    }

    write_offset += 4097;
    r = write_file->pwrite(mem + write_offset, 555555, write_offset);
    ASSERT_EQ(r, 555555);
    file_size += r;

    // flush and append 4097 bytes again
    r = write_file->fsync();
    ASSERT_EQ(r, 0);

    write_offset += 555555;
    r = write_file->pwrite(mem + write_offset, 4097, write_offset);
    ASSERT_EQ(r, 4097);
    file_size += r;

    // read in dirty buffer index without dirty data
    r = read_file->pread(read_buf + 1048576, 4096, 1048576);
    ASSERT_EQ(r, 4096);

    for (size_t i = 1048576; i < 1048576 + 4096; i++) {
      ASSERT_EQ(read_buf[i], mem[i]);
    }

    r = fs_->release(nodeid, read_file);
    ASSERT_EQ(r, 0);

    // use new reader to read whole file with 4096 io_size
    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    read_file = get_file_from_handle(handle);
    memset(read_buf, 0, buffer_size * 4);

    size_t read = 0;
    while (read < file_size) {
      auto read_size = std::min(file_size - read, (size_t)4096);
      r = read_file->pread(read_buf + read, read_size, read);
      ASSERT_EQ(r, (ssize_t)read_size);

      read += r;
    }

    for (size_t i = 0; i < file_size; i++) {
      ASSERT_EQ(read_buf[i], mem[i]);
    }

    r = fs_->release(nodeid, read_file);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, write_file);
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ(std::to_string(file_size), meta["Content-Length"]);

    // test write after create
    uint64_t nodeid2 = 0;
    void *handle2 = nullptr;
    std::string filepath2 = "testfile_appendable_2";
    r = create_and_flush(parent, filepath2.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid2, &st, &handle2);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));

    r = write_to_file_handle(handle2, "hello", 5, 0);
    ASSERT_EQ(r, 5);

    r = fs_->release(nodeid2, get_file_from_handle(handle2));
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath2, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ("5", meta["Content-Length"]);
  }

  void verify_enable_appendable_object_auto_switch() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    void *handle = nullptr;

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    std::string filepath = "testfile_appendable_auto_switch";
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    auto meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Normal", meta["X-Oss-Object-Type"]);

    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(nodeid, 1));

    bool unused = false;
    r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto file = get_file_from_handle(handle);
    auto random_data = random_string(1024 * 111);
    ssize_t ret =
        file->pwrite(random_data.c_str(), random_data.size(), st.st_size);
    ASSERT_EQ(ret, static_cast<ssize_t>(random_data.size()));

    off_t offset = 0;
    uint64_t crc64 = 0;
    auto read_buf = new char[1024 * 1024];
    DEFER(delete[] read_buf);

    auto file_size = st.st_size + static_cast<ssize_t>(random_data.size());
    while (offset < file_size) {
      ssize_t read_size =
          std::min(file_size - offset, static_cast<off_t>(1024 * 1024));
      ret = file->pread(read_buf, read_size, offset);
      ASSERT_EQ(ret, read_size);
      crc64 = cal_crc64(crc64, read_buf, read_size);
      offset += read_size;
    }

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);

    r = fs_->unlink(parent, filepath.c_str());
    ASSERT_EQ(r, 0);

    // create a file bigger than auto switch threshold
    create_random_file(local_file, 9);
    r = upload_file(local_file, join_paths(parent_path, filepath),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Normal", meta["X-Oss-Object-Type"]);

    uint64_t nodeid2 = 0;
    r = fs_->lookup(parent, filepath.c_str(), &nodeid2, &st);
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(nodeid2, 1));

    r = fs_->open(nodeid2, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    ret = file->pwrite(random_data.c_str(), random_data.size(), st.st_size);
    ASSERT_EQ(ret, -ENOTSUP);

    r = fs_->release(nodeid2, file);
    ASSERT_EQ(r, 0);

    // test open with O_TRUNC
    r = fs_->open(nodeid2, O_RDWR | O_TRUNC | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    ASSERT_EQ((dynamic_cast<OssFileHandle *>(file))->get_is_dirty(), false);

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ("0", meta["Content-Length"]);

    ret = file->pwrite(random_data.c_str(), random_data.size(), 0);
    ASSERT_EQ(ret, static_cast<ssize_t>(random_data.size()));

    void *buf =
        const_cast<void *>(static_cast<const void *>(random_data.c_str()));
    crc64 = cal_crc64(0, buf, random_data.size());

    r = file->fsync();
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ(std::to_string(random_data.size()), meta["Content-Length"]);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);

    r = fs_->release(nodeid2, file);
    ASSERT_EQ(r, 0);

    // test create empty file
    std::string filepath2 = "testfile_appendable_auto_switch_2";
    uint64_t nodeid3 = 0;
    r = create_and_flush(parent, filepath2.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid3, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid3, 1));

    r = fs_->release(nodeid3, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath2, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);

    // test append to 0 size normal file
    std::string filepath3 = "testfile_appendable_auto_switch_3";
    std::string local_file3 = join_paths(test_path_, filepath3);
    std::string cmd = "touch " + local_file3;
    system(cmd.c_str());

    r = upload_file(local_file3, join_paths(parent_path, filepath3),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath3, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Normal", meta["X-Oss-Object-Type"]);
    ASSERT_EQ("0", meta["Content-Length"]);

    uint64_t nodeid4 = 0;
    r = fs_->lookup(parent, filepath3.c_str(), &nodeid4, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid4, 1));

    r = fs_->open(nodeid4, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    ret = file->pwrite(random_data.c_str(), random_data.size(), 0);
    ASSERT_EQ(ret, static_cast<ssize_t>(random_data.size()));
    crc64 = cal_crc64(0, buf, random_data.size());

    r = fs_->release(nodeid4, file);
    ASSERT_EQ(r, 0);

    meta = get_file_meta(filepath3, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ(std::to_string(random_data.size()), meta["Content-Length"]);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);
  }

  void verify_appendable_switch_steps_with_oss_error() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    void *handle = nullptr;
    uint64_t nodeid = 0;

    auto parent_path = nodeid_to_path(parent);
    std::string file_name = "testfile";
    std::string local_file = join_paths(test_path_, file_name);
    create_random_file(local_file, 1);
    std::ifstream rf(local_file);
    const size_t file_size = 1024 * 1024;
    char *buf = new char[file_size];
    DEFER(delete[] buf);
    rf.read(buf, file_size);
    auto crc64_old = cal_crc64(0, buf, file_size);

    auto upload_test_file_in_dir = [&](const std::string &dir_path) {
      auto filepath = join_paths(dir_path, file_name);
      int r = upload_file(local_file, join_paths(parent_path, filepath),
                          FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);

      auto meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
      ASSERT_EQ("Normal", meta["X-Oss-Object-Type"]);

      r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);

      bool unused = false;
      r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
      ASSERT_EQ(r, 0);
    };

    // switch steps
    // 1. rename remote file to tmpfile
    // 2. delete old file
    // 3. do switch (get tmp and append)
    // 4. delete tmpfile (ignore error)
    std::string tmp_prefix = ".ossfs_hidden_file_" + file_name;
    for (int i = 1; i <= 6; i++) {
      std::string dir_path = "test-" + std::to_string(i);
      upload_test_file_in_dir(dir_path);

      auto fault = FaultInjectionId::FI_OssError_Failed_Without_Call;
      g_fault_injector->set_injection(fault, FaultInjection(1 << 31, i));

      auto file = get_file_from_handle(handle);
      auto random_data = random_string(1024);
      void *buf =
          const_cast<void *>(static_cast<const void *>(random_data.c_str()));
      auto crc64 = cal_crc64(crc64_old, buf, random_data.size());
      int ret =
          file->pwrite(random_data.c_str(), random_data.size(), st.st_size);

      g_fault_injector->clear_injection(fault);

      int r = fs_->release(nodeid, file);
      ASSERT_EQ(r, 0);

      auto files = get_list_objects(join_paths(parent_path, dir_path),
                                    FLAGS_oss_bucket_prefix);
      int dir_size = files.size();

      if (i == 1) {
        // copy error, old file in dir
        ASSERT_EQ(dir_size, 1);
      } else if (i == 2) {
        // copy success but delete old file error, old file and tmp in dir
        ASSERT_EQ(dir_size, 2);
      } else if (i == 3) {
        // delete old file success but get tmp error, tmp in dir
        ASSERT_EQ(dir_size, 1);
      } else if (i == 4) {
        // get tmp success but switch to appendable error, tmp in dir
        ASSERT_EQ(dir_size, 1);
      } else if (i == 5) {
        // switch success but delete tmp error, tmp and new file in dir
        ASSERT_EQ(dir_size, 2);
      } else if (i == 6) {
        // delete tmp success
        ASSERT_EQ(dir_size, 1);
      }

      if (i < 5) {
        // append error
        // all files in dir are normal and have same crc64 as old file
        ASSERT_EQ(ret, -EIO);
        for (auto &file_path : files) {
          auto path = join_paths(dir_path, file_path);
          auto meta = get_file_meta(path, FLAGS_oss_bucket_prefix);
          ASSERT_EQ("Normal", meta["X-Oss-Object-Type"]);
          ASSERT_EQ(std::to_string(crc64_old), meta["X-Oss-Hash-Crc64ecma"]);
        }
      } else {
        // switch to appendable success
        // new data is written to buffer and will be uploaded when file close
        // new testfile is appendable and has new crc64
        ASSERT_EQ(ret, static_cast<int>(random_data.size()));
        auto path = join_paths(dir_path, file_name);
        auto meta = get_file_meta(path, FLAGS_oss_bucket_prefix);
        ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
        ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);
        // check tmpfile if exists
        for (auto &file_path : files) {
          if (file_path.substr(0, tmp_prefix.size()) == tmp_prefix) {
            auto path = join_paths(dir_path, file_path);
            auto meta = get_file_meta(path, FLAGS_oss_bucket_prefix);
            ASSERT_EQ("Normal", meta["X-Oss-Object-Type"]);
            ASSERT_EQ(std::to_string(crc64_old), meta["X-Oss-Hash-Crc64ecma"]);
          }
        }
      }

      fs_->forget(nodeid, 1);
    }
  }

  void verify_read_appendable_object_with_write_failure() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    srand(time(nullptr));

    std::string filename = "testfile";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = 0;

    r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle_write = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle_write, &unused);
    if (r != 0) {
      LOG_ERROR("failed to open file, r is `", r);
    }
    auto file = get_file_from_handle(handle_write);
    int total_written = 0;

    const int buffer_size = fs_->options_.upload_buffer_size;

    auto write_fh = [&](size_t write_size) {
      std::vector<char> buf(write_size);
      int r = file->pwrite(buf.data(), write_size, total_written);
      total_written += write_size;
      return r;
    };

    // First write and flush.
    int write_size = buffer_size + rand() % (buffer_size / 4) + 1;
    r = write_fh(write_size);
    ASSERT_EQ(r, write_size);
    r = file->fdatasync();
    ASSERT_EQ(r, 0);

    // Second write, the data will remains in the write buffer.
    write_size = rand() % (buffer_size / 4) + 1;
    r = write_fh(write_size);
    ASSERT_EQ(r, write_size);

    // Third write with oss error, the data in the second write will be
    // discarded.
    g_fault_injector->set_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);
    write_size = buffer_size;
    r = write_fh(write_size);
    ASSERT_LT(r, 0);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Failed_Without_Call);

    auto file_write = file;
    DEFER(fs_->release(nodeid, file_write));

    // Only the data in the first write can be read.
    int read_size = 1048576;
    int total_read = 0;
    char *read_buf = new char[read_size];
    DEFER(delete[] read_buf);
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    file = get_file_from_handle(handle);
    while (total_read < total_written) {
      int r = file->pread(read_buf, read_size, total_read);
      if (r > 0) {
        total_read += r;
      } else {
        break;
      }
    }

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  void verify_random_read_write_appendable_object() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "testfile";
    int target_size = (rand() % 128 + 64) * 1024 * 1024;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;

    // create an appendable file
    int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // Write thread
    auto write_task =
        std::async(std::launch::async, [&]() -> std::pair<uint64_t, bool> {
          INIT_PHOTON();
          int total_written = 0;
          uint64_t local_crc = 0;
          void *handle_write = nullptr;
          bool success = true, unused = false;
          int r = fs_->open(nodeid, O_RDWR, &handle_write, &unused);
          if (r != 0) {
            success = false;
            LOG_ERROR("failed to open file, r is `", r);
          }
          auto file = get_file_from_handle(handle_write);

          while (success && total_written < target_size) {
            int sleep_time = rand() % 40 + 10;
            int write_size =
                rand() % (1024 * 1024) + 1;  // Random size between 1B and 1MB
            write_size = std::min(write_size, target_size - total_written);
            char *buf = new char[write_size];
            for (int i = 0; i < write_size; i++) {
              buf[i] = random_string(1)[0];
            }  // Fill buffer with dummy data

            int r = file->pwrite(buf, write_size, total_written);
            if (r != write_size) {
              success = false;
              LOG_ERROR("failed to write ` bytes to file, r is `", write_size,
                        r);
            }

            local_crc = cal_crc64(local_crc, buf, write_size);
            total_written += write_size;
            delete[] buf;

            // Randomly perform read or getattr operations
            if (rand() % 2 == 0) {  // 50% chance to perform read
              char *read_buf = new char[write_size];
              r = file->pread(read_buf, write_size, total_written - write_size);
              if (r != write_size) {
                success = false;
                LOG_ERROR("failed to read ` bytes from file, r is `",
                          write_size, r);
              }
              delete[] read_buf;
            } else {  // 50% chance to perform getattr
              struct stat stbuf = {};
              r = fs_->getattr(nodeid, &stbuf);
              if (r != 0 || stbuf.st_size != total_written) {
                success = false;
                LOG_ERROR(
                    "failed to getattr, r is `, stbuf.size is `, total_written "
                    "is `",
                    r, stbuf.st_size, total_written);
              }
            }

            // Randomly release and reopen the file
            if (rand() % 3 == 0) {
              r = fs_->release(nodeid, file);
              if (r != 0) {
                LOG_ERROR("failed to release file, r is `", r);
                success = false;
              }
              handle_write = 0;
              r = fs_->open(nodeid, O_RDWR, &handle_write, &unused);
              if (r != 0) {
                LOG_ERROR("failed to open file, r is `", r);
                success = false;
              }
              file = get_file_from_handle(handle_write);
            }

            photon::thread_usleep(sleep_time);  // Sleep for 10-50 us
          }
          r = fs_->release(nodeid, file);
          if (r < 0) {
            LOG_ERROR("failed to release file, r is `", r);
            success = false;
          }
          return {local_crc, success};
        });

    // Read threads
    std::vector<std::future<std::pair<uint64_t, bool>>> read_tasks;
    int parallel_cnt = 64;
    for (int i = 0; i < parallel_cnt; i++) {
      auto task =
          std::async(std::launch::async, [&]() -> std::pair<uint64_t, bool> {
            INIT_PHOTON();
            int total_read = 0;
            uint64_t local_crc = 0;
            void *handle_read = nullptr;
            bool success = true, unused = false;
            int r = fs_->open(nodeid, O_RDONLY, &handle_read, &unused);
            if (r < 0) {
              LOG_ERROR("failed to open file, r is `", r);
              success = false;
            }
            auto file_read = get_file_from_handle(handle_read);

            while (success && total_read < target_size) {
              int sleep_time = rand() % 5 + 1;
              int read_size =
                  rand() % (1024 * 1024) + 1;  // Random size between 1B and 1MB
              read_size = std::min(read_size, target_size - total_read);
              char *buf = new char[read_size];

              r = file_read->pread(buf, read_size, total_read);
              if (r != read_size) {
                // If read fails, retry until the correct amount of data is read
                while (r != read_size) {
                  LOG_INFO(
                      "retry to read ` bytes from file, r is `, offset is `",
                      read_size, r, total_read);
                  photon::thread_usleep(1000000);  // Sleep 1s for each retry
                  r = file_read->pread(buf, read_size, total_read);
                }
              }
              local_crc = cal_crc64(local_crc, buf, read_size);
              total_read += read_size;
              delete[] buf;

              // Randomly release and reopen the file
              if (rand() % 3 == 0) {
                r = fs_->release(nodeid, file_read);
                if (r != 0) {
                  LOG_ERROR("failed to release file, r is `", r);
                  success = false;
                }
                handle_read = 0;
                r = fs_->open(nodeid, O_RDONLY, &handle_read, &unused);
                if (r != 0) {
                  LOG_ERROR("failed to open file, r is `", r);
                  success = false;
                }
                file_read = get_file_from_handle(handle_read);
              }

              photon::thread_usleep(sleep_time);  // Sleep for 1-5 us
            }
            r = fs_->release(nodeid, file_read);
            if (r < 0) {
              LOG_ERROR("failed to release file, r is `", r);
              success = false;
            }
            return {local_crc, success};
          });
      read_tasks.push_back(std::move(task));
    }

    // Wait for write task to complete
    auto write_res = write_task.get();
    ASSERT_TRUE(write_res.second);

    // Wait for read tasks to complete and compare CRCs
    for (auto &task : read_tasks) {
      auto read_res = task.get();
      ASSERT_TRUE(read_res.second);
      ASSERT_EQ(write_res.first, read_res.first);
    }

    auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
    ASSERT_EQ("Appendable", meta["X-Oss-Object-Type"]);
    ASSERT_EQ(std::to_string(target_size), meta["Content-Length"]);
    ASSERT_EQ(std::to_string(write_res.first), meta["X-Oss-Hash-Crc64ecma"]);
  }

  void verify_oss_error_during_append_appendable_object() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto random_data = random_string(1024 * 1024);

    // 1. test AppendObject failed
    size_t offset = 0;

    {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      std::string path = "testfile";

      auto r = create_and_flush(parent, path.c_str(), CREATE_BASE_FLAGS, 0644,
                                0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      auto file = get_file_from_handle(handle);
      DEFER(fs_->release(nodeid, file));

      r = file->pwrite(random_data.c_str(), random_data.size(), 0);
      ASSERT_EQ(r, static_cast<ssize_t>(random_data.size()));
      offset += r;

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_EQ(r, -ETIMEDOUT);

      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);
    }

    // 2. test HeadObject failed
    {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      std::string path = "testfile";

      auto r = fs_->lookup(parent, path.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      bool unused = false;
      r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
      ASSERT_EQ(r, 0);

      auto file = get_file_from_handle(handle);
      DEFER(fs_->release(nodeid, file));

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_EQ(r, -ETIMEDOUT);

      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);
    }
  }

  void verify_write_to_flushed_stale_file() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    size_t offset = 0;

    auto random_data = random_string(512 * 1024);

    uint64_t nodeid = 0;
    void *handle = nullptr;
    std::string path = "testfile";

    auto r = create_and_flush(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0,
                              0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, file));

    r = file->pwrite(random_data.c_str(), random_data.size(), 0);
    ASSERT_EQ(r, static_cast<ssize_t>(random_data.size()));
    offset += r;

    r = fs_->unlink(parent, path.c_str());
    ASSERT_EQ(r, 0);

    r = file->fsync();
    ASSERT_EQ(r, 0);

    r = file->pwrite(random_data.c_str(), random_data.size(), offset);
    ASSERT_EQ(r, -EIO);

    r = stat_file(nodeid_to_path(nodeid), FLAGS_oss_bucket_prefix);
    ASSERT_NE(r, 0);
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, -ESTALE);
  }
};

TEST_F(Ossfs2AppendableObjectTest, verify_enable_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_enable_appendable_object();
}

TEST_F(Ossfs2AppendableObjectTest,
       verify_enable_appendable_object_auto_switch) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  opts.appendable_object_autoswitch_threshold = 8 * 1024 * 1024;
  init(opts);
  verify_enable_appendable_object_auto_switch();
}

TEST_F(Ossfs2AppendableObjectTest,
       verify_appendable_switch_steps_with_oss_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_appendable_switch_steps_with_oss_error();
}

TEST_F(Ossfs2AppendableObjectTest, verify_random_read_write_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_random_read_write_appendable_object();
}

TEST_F(Ossfs2AppendableObjectTest,
       verify_oss_error_during_append_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 131072;
  init(opts);
  verify_oss_error_during_append_appendable_object();
}

TEST_F(Ossfs2AppendableObjectTest,
       verify_read_appendable_object_with_write_failure) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_read_appendable_object_with_write_failure();
}

TEST_F(Ossfs2AppendableObjectTest, verify_write_to_flushed_stale_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_write_to_flushed_stale_file();
}
