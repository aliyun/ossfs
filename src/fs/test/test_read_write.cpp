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

class Ossfs2ReadWriteTest : public Ossfs2TestSuite {
 protected:
  void verify_write_files() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int file_cnt = 103;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    std::vector<uint64_t> file_crcs(file_cnt, 0);

    // launch 64 threads to write small files concurrently
    std::vector<std::future<void>> tasks;
    int parallel_cnt = 64;
    srand(time(nullptr));
    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        INIT_PHOTON();
        for (int j = i; j < file_cnt; j += parallel_cnt) {
          std::string file_name = "testfile_" + std::to_string(j);
          uint64_t file_size = 1 + rand() % 64;
          int file_draft = 1 + rand() % 1023;

          file_crcs[j] = create_file_in_folder(parent, file_name, file_size,
                                               nodeids[j], file_draft);
          ASSERT_TRUE(file_crcs[j] > 0);

          DEFER(fs_->forget(nodeids[j], 1));
        }
      });
      tasks.push_back(std::move(task));
    }

    for (auto &task : tasks) {
      task.wait();
    }

    auto result =
        get_list_objects(get_test_osspath(""), FLAGS_oss_bucket_prefix);
    ASSERT_EQ((size_t)file_cnt, result.size());
    LOG_INFO("write ` files in the dir", file_cnt);

    for (int i = 0; i < file_cnt; i++) {
      std::string file_name = "testfile_" + std::to_string(i);
      auto file_meta = get_file_meta(file_name, FLAGS_oss_bucket_prefix);

      ASSERT_EQ(std::to_string(file_crcs[i]),
                file_meta["X-Oss-Hash-Crc64ecma"]);
    }
  }

  void verify_read_out_of_range() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    create_file_in_folder(parent, "testfile", 128, nodeid);

    void *handle = nullptr;
    bool unused;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto file = get_file_from_handle(handle);

    char *buf_1MB = new char[0x100000];
    DEFER(delete[] buf_1MB);

    ssize_t read_size = file->pread(buf_1MB, 1048576, 128ULL * 1024 * 1024 - 7);
    ASSERT_EQ(read_size, 7);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    struct stat st;
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 128 * 1024 * 1024);

    std::string random_file = join_paths(test_path_, "random_fs_test.dat");
    create_random_file(random_file, 99, 0);

    auto parent_path = nodeid_to_path(parent);
    upload_file(random_file, join_paths(parent_path, "testfile"),
                FLAGS_oss_bucket_prefix);

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 128 * 1024 * 1024);

    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    read_size = file->pread(buf_1MB, 1048576, 128ULL * 1024 * 1024 - 7);
    ASSERT_EQ(read_size, -EINVAL);

    read_size = file->pread(buf_1MB, 1048576, 99ULL * 1024 * 1024 - 7);
    ASSERT_EQ(read_size, -EINVAL);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid, 1);

    r = fs_->lookup(parent, "testfile", &nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 99 * 1024 * 1024);

    DEFER(fs_->forget(nodeid, 1));

    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);

    std::ifstream rf(random_file);
    uint64_t crc64 = 0, local_crc64 = 0;
    uint64_t total_size = 99 * 1024 * 1024;
    uint64_t offset = 0;

    size_t buf_size = IO_SIZE;  // read/write IO
    while (offset < total_size) {
      buf_size = std::min(buf_size, total_size - offset);
      rf.read(buf_1MB, buf_size);
      local_crc64 = cal_crc64(local_crc64, buf_1MB, buf_size);

      read_size = file->pread(buf_1MB, buf_size, offset);
      ASSERT_EQ(read_size, static_cast<ssize_t>(buf_size));

      crc64 = cal_crc64(crc64, buf_1MB, buf_size);
      offset += buf_size;
    }

    ASSERT_EQ(crc64, local_crc64);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  void verify_transmission_control() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::vector<std::future<ssize_t>> tasks;
    int parallel_cnt = 64;
    std::atomic<bool> is_stopping(false);

    std::vector<uint64_t> file_crcs(parallel_cnt, 0);
    srand(time(nullptr));
    for (int i = 0; i < parallel_cnt; ++i) {
      auto task = std::async(
          std::launch::async,
          [&](int thread_id) -> ssize_t {
            INIT_PHOTON();
            std::string file_name = "testfile_" + std::to_string(thread_id);
            uint64_t file_size = 1 + rand() % 64;
            int file_draft = 1 + rand() % 1023;

            uint64_t nodeid = 0;
            file_crcs[thread_id] = create_file_in_folder(
                parent, file_name, file_size, nodeid, file_draft);
            DEFER(fs_->forget(nodeid, 1));

            if (file_crcs[thread_id] == 0) {
              return -EIO;
            }

            return 0;
          },
          i);
      tasks.push_back(std::move(task));
    }

    for (auto &task : tasks) {
      ASSERT_EQ(0, task.get());
    }

    tasks.clear();

    for (int i = 0; i < parallel_cnt; ++i) {
      auto task = std::async(
          std::launch::async,
          [&](int thread_id) -> ssize_t {
            INIT_PHOTON();
            std::string file_name = "testfile_" + std::to_string(thread_id);
            uint64_t crc64 = 0;
            while (!is_stopping) {
              ssize_t r = read_file_in_folder(parent, file_name, &crc64);
              if (r < 0) return r;
              if (crc64 != file_crcs[thread_id]) {
                LOG_ERROR("read crc64 ", crc64, " expected ",
                          file_crcs[thread_id]);
                return -EIO;
              }
            }

            return 0;
          },
          i);
      tasks.push_back(std::move(task));
    }

    // read for 30 seconds
    std::this_thread::sleep_for(std::chrono::seconds(30));
    is_stopping = true;

    for (auto &task : tasks) {
      ASSERT_EQ(0, task.get());
    }
  }

  void verify_write_with_fuse_write_buf_failed() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filepath = "testfile";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;

    int r = create_and_flush(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    uint64_t crc64 = 0;

    for (int i = 0; i < 13; ++i) {
      std::string data = random_string(1048576);
      r = write_with_fuse_bufvec(handle, data.c_str(), 1048576, i * 1048576);
      EXPECT_EQ(r, 1048576);
      crc64 = crc64ecma(data.c_str(), 1048576, crc64);
    }

    // test partial write
    size_t file_size = 13 * 1048576;
    for (int i = 0; i < 10; i++) {
      std::string data = random_string(1048576);
      auto partial_write_size = rand() % 1048576 + 1;
      r = write_with_fuse_bufvec_with_err(handle, data.c_str(), 1048576,
                                          file_size, partial_write_size);
      EXPECT_TRUE(r > 0);
      crc64 = crc64ecma(data.c_str(), r, crc64);
      file_size += r;
    }

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    auto meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);

    // open and write with error
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    std::string data = random_string(1048576);
    r = write_with_fuse_bufvec_with_err(handle, data.c_str(), 1048576,
                                        file_size, 0);
    EXPECT_NE(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, -EIO);
  }

  // read_mode in [ONCE, RANDOM, ALL_FILES_PER_WORKER]
  void verify_concurrent_write_and_read(std::string_view read_mode = "ONCE",
                                        const int file_cnt = 103,
                                        const int parallel_cnt = 64) {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    std::vector<uint64_t> nodeids(file_cnt, 0);
    std::vector<uint64_t> file_crcs(file_cnt, 0);
    // launch 64 threads to write small files concurrently
    std::vector<std::future<void>> tasks;
    srand(time(nullptr));
    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        INIT_PHOTON();
        for (int j = i; j < file_cnt; j += parallel_cnt) {
          std::string file_name = "testfile_" + std::to_string(j);
          uint64_t file_size = 1 + rand() % 64;
          int file_draft = 1 + rand() % 1023;
          file_crcs[j] = create_file_in_folder(parent, file_name, file_size,
                                               nodeids[j], file_draft);
          ASSERT_TRUE(file_crcs[j] > 0);
          DEFER(fs_->forget(nodeids[j], 1));
        }
      });
      tasks.push_back(std::move(task));
    }
    for (auto &task : tasks) {
      task.wait();
    }
    auto result =
        get_list_objects(get_test_osspath(""), FLAGS_oss_bucket_prefix);
    ASSERT_EQ((size_t)file_cnt, result.size());
    LOG_INFO("write ` files in the dir", file_cnt);
    for (int i = 0; i < file_cnt; i++) {
      std::string file_name = "testfile_" + std::to_string(i);
      auto file_meta = get_file_meta(file_name, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(std::to_string(file_crcs[i]),
                file_meta["X-Oss-Hash-Crc64ecma"]);
    }
    tasks.clear();
    std::vector<uint64_t> file_read_crcs(file_cnt, 0);
    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        INIT_PHOTON();
        auto verify_fn = [&](const std::string &file_name, int index) {
          uint64_t crc64 = 0;
          ssize_t r = read_file_in_folder(parent, file_name, &crc64);
          ASSERT_TRUE(r >= 0);
          ASSERT_TRUE(file_crcs[index] == crc64);
        };

        if (read_mode == "ALL_FILES_PER_WORKER" || read_mode == "RANDOM") {
          for (int j = 0; j < file_cnt; j++) {
            int index = j;
            std::string file_name = "testfile_" + std::to_string(j);
            if (read_mode == "RANDOM") {
              index = rand() % file_cnt;
              file_name = "testfile_" + std::to_string(index);
            }

            verify_fn(file_name, index);
          }
        } else {
          // default is ONCE
          for (int j = i; j < file_cnt; j += parallel_cnt) {
            std::string file_name = "testfile_" + std::to_string(j);
            verify_fn(file_name, j);
          }
        }
      });
      tasks.push_back(std::move(task));
    }
    for (auto &task : tasks) {
      task.wait();
    }
  }

  void verify_min_reserved_buffer_size_per_file() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::map<uint64_t, std::pair<void *, std::string>> handle_map;
    for (int i = 0; i < 65; i++) {
      std::string file_name = "testfile_r1_" + std::to_string(i);
      uint64_t nodeid = 0;
      struct stat st;
      void *handle = nullptr;
      auto r = create_and_flush(parent, file_name.c_str(), CREATE_BASE_FLAGS,
                                0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      handle_map[nodeid] = {handle, file_name};
    }

    DEFER({
      for (auto &handle : handle_map) {
        fs_->release(handle.first, get_file_from_handle(handle.second.first));
        fs_->unlink(parent, handle.second.second.c_str());
        fs_->forget(handle.first, 1);
      }
    });

    ASSERT_EQ(fs_->download_buffers_->used_blocks(), 0ULL);
  }

  void verify_write_with_oss_error() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::set<uint64_t> forget_inodes;
    DEFER(for (auto nodeid : forget_inodes) { fs_->forget(nodeid, 1); });

    std::set<std::pair<uint64_t, void *>> handle_table;
    DEFER(for (auto &pair
               : handle_table) {
      fs_->release(pair.first, get_file_from_handle(pair.second));
    });

    std::string filepath = "testfile";
    auto parent_path = nodeid_to_path(parent);

    // test empty file upload
    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0,
                       0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    forget_inodes.insert(nodeid);
    handle_table.insert({nodeid, handle});

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    handle_table.erase({nodeid, handle});
    ASSERT_NE(r, 0);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, -ENOENT);

    // test init multipart failed
    uint64_t nodeid2 = 0;
    r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid2, &st, &handle);
    ASSERT_EQ(r, 0);
    forget_inodes.insert(nodeid2);
    handle_table.insert({nodeid2, handle});

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);
    char *buf = new char[1048576];
    DEFER(delete[] buf);

    size_t off = 0;
    for (int i = 0; i < 7; i++) {
      auto w_size = write_to_file_handle(handle, buf, 1048576, off);
      ASSERT_EQ(w_size, 1048576);
      off += 1048576;
    }

    auto w_size = write_to_file_handle(handle, buf, 1048576, off);
    ASSERT_EQ(w_size, -ETIMEDOUT);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    r = fs_->release(nodeid2, get_file_from_handle(handle));
    handle_table.erase({nodeid2, handle});
    ASSERT_NE(r, 0);

    r = fs_->getattr(nodeid2, &st);
    ASSERT_EQ(r, -ENOENT);

    // test multipart upload failed
    uint64_t nodeid3 = 0;
    r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid3, &st, &handle);
    ASSERT_EQ(r, 0);
    forget_inodes.insert(nodeid3);
    handle_table.insert({nodeid3, handle});

    off = 0;
    for (int i = 0; i < 15; i++) {
      auto w_size = write_to_file_handle(handle, buf, 1048576, off);
      ASSERT_EQ(w_size, 1048576);
      off += 1048576;
    }

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);

    w_size = write_to_file_handle(handle, buf, 1048576, off);
    ASSERT_EQ(w_size, 1048576);

    // wait for upload
    std::this_thread::sleep_for(std::chrono::seconds(1));

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    r = fs_->release(nodeid3, get_file_from_handle(handle));
    handle_table.erase({nodeid3, handle});
    ASSERT_NE(r, 0);

    r = fs_->getattr(nodeid3, &st);
    ASSERT_EQ(r, -ENOENT);

    // TODO: enhance test merge remote failed
    uint64_t nodeid4 = 0;
    r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid4, &st, &handle);
    ASSERT_EQ(r, 0);
    forget_inodes.insert(nodeid4);
    handle_table.insert({nodeid4, handle});

    off = 0;
    for (int i = 0; i < 10; i++) {
      auto w_size = write_to_file_handle(handle, buf, 1048576, off);
      ASSERT_EQ(w_size, 1048576);
      off += 1048576;
    }

    r = fs_->release(nodeid4, get_file_from_handle(handle));
    handle_table.erase({nodeid4, handle});
    ASSERT_EQ(r, 0);

    bool unused = false;
    r = fs_->open(nodeid4, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    handle_table.insert({nodeid4, handle});

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);
    w_size = write_to_file_handle(handle, buf, 1048576, off);
    ASSERT_EQ(w_size, -ETIMEDOUT);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);

    // test complete upload failed
    for (int i = 0; i < 6; i++) {
      auto w_size = write_to_file_handle(handle, buf, 1048576, off);
      ASSERT_EQ(w_size, 1048576);
      off += 1048576;
    }

    // wait for upload
    std::this_thread::sleep_for(std::chrono::seconds(1));
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);

    r = fs_->release(nodeid4, get_file_from_handle(handle));
    handle_table.erase({nodeid4, handle});
    ASSERT_NE(r, 0);
  }

  void verify_readwrite_archive_and_ia_object() {
    // 1. test Archive object
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 43);

    std::string filepath = "testfile-archive";
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = set_file_meta(join_paths(parent_path, filepath), kOssMetaStorageClass,
                      kOssSCArchive, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid = 0;
    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    bool unused = 0;
    void *handle = nullptr;
    r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto file = get_file_from_handle(handle);
    auto random_data = random_string(1024 * 111);
    ssize_t ret =
        file->pwrite(random_data.c_str(), random_data.size(), st.st_size);
    ASSERT_EQ(ret, -ENOTSUP);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    char *buf = new char[111 * 1024];
    DEFER(delete[] buf);

    file = get_file_from_handle(handle);
    ret = file->pread(buf, 1024 * 111, 0);
    ASSERT_EQ(ret, -EACCES);

    ret = file->pread(buf, 1024 * 111, 1048576 * 11 - 3);
    ASSERT_EQ(ret, -EACCES);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    // 2. test IA object
    filepath = "testfile-ia";
    r = upload_file(local_file, join_paths(parent_path, filepath),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    r = set_file_meta(join_paths(parent_path, filepath), kOssMetaStorageClass,
                      kOssSCIA, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid2 = 0;
    r = fs_->lookup(parent, filepath.c_str(), &nodeid2, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));

    r = fs_->open(nodeid2, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    ret = file->pwrite(random_data.c_str(), random_data.size(), st.st_size);
    ASSERT_EQ(ret, static_cast<ssize_t>(random_data.size()));

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    r = fs_->open(nodeid2, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    ret = file->pread(buf, 1024 * 111, 0);
    ASSERT_EQ(ret, 1024 * 111);

    ret = file->pread(buf, 1024 * 111, 1048576 * 11 - 3);
    ASSERT_EQ(ret, 1024 * 111);

    r = fs_->release(nodeid2, file);
    ASSERT_EQ(r, 0);
  }

  void verify_read_dirty_fd() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    struct stat st;
    void *handle = nullptr;
    uint64_t nodeid = 0;
    int r = fs_->creat(parent, "testfile", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                       &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    char buf[255];
    auto file = get_file_from_handle(handle);
    r = file->pread(buf, 255, 0);
    ASSERT_EQ(r, -EBUSY);

    r = file->pwrite(buf, 255, 0);
    ASSERT_EQ(r, 255);

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    r = file->pread(buf, 255, 0);
    ASSERT_EQ(r, 255);

    r = file->pwrite(buf, 255, 255);
    ASSERT_EQ(r, 255);

    r = file->pread(buf, 255, 0);
    ASSERT_EQ(r, -EBUSY);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  void verify_write_to_immutable_handle(const std::string &suffix) {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto random_data = random_string(1024 * 1024);

    // 1. test create empty file
    {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      std::string path = "testfile-empty" + suffix;
      auto r = fs_->creat(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0, 0,
                          0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      auto file = get_file_from_handle(handle);
      DEFER(fs_->release(nodeid, file));

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      r = file->fsync();
      ASSERT_EQ(r, -ETIMEDOUT);

      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      r = file->pwrite(random_data.c_str(), random_data.size(), 0);
      ASSERT_EQ(r, -EIO);

      r = file->fsync();
      ASSERT_EQ(r, -EIO);
    }

    // 2. test file start with offset 0
    {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      off_t offset = 0;
      std::string path = "testfile-1" + suffix;
      auto r = fs_->creat(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0, 0,
                          0, &nodeid, &st, &handle);

      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      auto file = get_file_from_handle(handle);
      DEFER(fs_->release(nodeid, file));

      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_EQ(r, static_cast<ssize_t>(random_data.size()));
      offset += random_data.size();

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      r = file->fsync();
      ASSERT_NE(r, 0);

      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      // the file is not exist on OSS, so merge operation will fail
      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_TRUE(r < 0);

      // try again
      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_TRUE(r < 0);

      r = file->fsync();
      ASSERT_NE(r, 0);
    }

    // 3. test file start with offset non-zero
    {
      uint64_t nodeid = 0;
      void *handle = nullptr;
      off_t offset = 0;
      std::string path = "testfile-2" + suffix;
      auto r = fs_->creat(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0, 0,
                          0, &nodeid, &st, &handle);

      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      auto file = get_file_from_handle(handle);
      DEFER(fs_->release(nodeid, file));

      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_EQ(r, static_cast<ssize_t>(random_data.size()));
      offset += random_data.size();

      r = file->fsync();
      ASSERT_EQ(r, 0);

      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_EQ(r, static_cast<ssize_t>(random_data.size()));
      offset += random_data.size();

      g_fault_injector->set_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      r = file->fsync();
      ASSERT_NE(r, 0);

      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Timeout);

      // we cannot read data from OSS, so merge will fail
      r = file->pwrite(random_data.c_str(), random_data.size(), offset);
      ASSERT_TRUE(r < 0);
    }
  }

  void verify_init_multipart_error() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto random_data = random_string(1024 * 1024);

    uint64_t nodeid = 0;
    void *handle = nullptr;
    std::string path = "testfile";

    auto r = create_and_flush(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0,
                              0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    DEFER(fs_->release(nodeid, file));

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Timeout);

    r = file->pwrite(random_data.c_str(), random_data.size(), 0);
    ASSERT_EQ(r, -ETIMEDOUT);

    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Timeout);
  }

  void verify_read_dirty_object_with_oss_error() {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    std::string path = "testfile";

    auto r = create_and_flush(parent, path.c_str(), CREATE_BASE_FLAGS, 0644, 0,
                              0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    const int size_1 = 1280;
    std::string write_str = random_string(size_1);
    char *write_buf = const_cast<char *>(write_str.c_str());
    r = file->pwrite(write_buf, size_1, 0);
    ASSERT_EQ(r, size_1);

    // release to upload buffer
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    auto meta = get_file_meta(path, FLAGS_oss_bucket_prefix);
    uint64_t crc64 = cal_crc64(0, write_buf, size_1);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);

    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    file = get_file_from_handle(handle);

    // will write to dirty buffer
    const int size_2 = 256;
    r = file->pwrite(write_buf, size_2, size_1);
    ASSERT_EQ(r, size_2);
    crc64 = cal_crc64(crc64, write_buf, size_2);

    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    const int size_overall = size_1 + size_2;
    char read_buf[size_overall];
    {
      // 1. read from remote file
      int r = file->pread(read_buf, size_overall, 0);
      ASSERT_EQ(r, -EIO);
    }
    {
      // 2. read from remote file, but corresponding buffer index is
      // dirty buffer index because of new data
      int r = file->pread(read_buf, size_overall - 1024, 1024);
      ASSERT_EQ(r, -EIO);
    }
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);
    {
      // 3. check read from dirty inode failed,
      // and not fallback to read from oss
      const int retry_times = oss_options_.retry_times;
      g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed,
                                      FaultInjection(retry_times + 1));
      int r = file->pread(read_buf, size_overall, 0);
      ASSERT_EQ(r, -EIO);
      g_fault_injector->clear_injection(
          FaultInjectionId::FI_OssError_Call_Failed);
    }
    {
      // 4. check read no dirty data (E_NO_DIRTY_DATA) when read from dirty
      // inode
      memset(read_buf, 0, sizeof(read_buf));
      int r = file->pread(read_buf, size_1, 0);
      ASSERT_EQ(r, size_1);
      ASSERT_EQ(memcmp(read_buf, write_buf, size_1), 0);
    }

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
    meta = get_file_meta(path, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(std::to_string(crc64), meta["X-Oss-Hash-Crc64ecma"]);
  }

  void verify_read_repeatedly_with_prefetch() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    uint64_t nodeid = 0;
    const size_t file_size = 3072;
    auto crc64 = create_file_in_folder(parent, "testfile", file_size, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto file = get_file_from_handle(handle);
    const size_t size = 1024 * 1024;
    char buf[size];

    g_fault_injector->set_injection(FaultInjectionId::FI_First_Prefetch_Delay);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_First_Prefetch_Delay));
    // first read
    void *pin_buffer = nullptr;
    uint64_t crc64_read = 0;
    for (size_t i = 0; i < file_size; i++) {
      size_t offset = size * i;
      ssize_t r = file->pin(offset, size, &pin_buffer);
      if (r > 0) {
        memcpy(buf, pin_buffer, r);
        file->unpin(offset);
      } else {
        r = file->pread(buf, size, offset);
      }
      ASSERT_EQ(r, static_cast<ssize_t>(size));
      crc64_read = cal_crc64(crc64_read, buf, size);
    }
    ASSERT_EQ(crc64, crc64_read);

    // read 5 times next, and check the number of cache hits
    for (int j = 0; j < 5; j++) {
      size_t cache_hit_cnt = 0;
      uint64_t crc64_read = 0;
      for (size_t i = 0; i < file_size; i++) {
        size_t offset = size * i;
        ssize_t r = file->pin(offset, size, &pin_buffer);
        if (r > 0) {
          cache_hit_cnt++;
          memcpy(buf, pin_buffer, r);
          file->unpin(offset);
        } else {
          r = file->pread(buf, size, offset);
        }
        ASSERT_EQ(r, static_cast<ssize_t>(size));
        crc64_read = cal_crc64(crc64_read, buf, size);
      }
      LOG_INFO("cache hit cnt: `", cache_hit_cnt);
      ASSERT_GT(cache_hit_cnt, file_size * 8 / 10);
      ASSERT_EQ(crc64, crc64_read);
    }

    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
  }

  void verify_direct_read_oss() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    int file_cnt = 103;
    std::vector<uint64_t> nodeids(file_cnt, 0);
    std::vector<uint64_t> file_crcs(file_cnt, 0);

    // launch 64 threads to write small files concurrently
    std::vector<std::future<void>> tasks;
    int parallel_cnt = 64;
    srand(time(nullptr));
    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        INIT_PHOTON();
        for (int j = i; j < file_cnt; j += parallel_cnt) {
          std::string file_name = "testfile_" + std::to_string(j);
          uint64_t file_size = 1 + rand() % 64;
          int file_draft = 1 + rand() % 1023;

          file_crcs[j] = create_file_in_folder(parent, file_name, file_size,
                                               nodeids[j], file_draft);
          ASSERT_TRUE(file_crcs[j] > 0);

          DEFER(fs_->forget(nodeids[j], 1));
        }
      });
      tasks.push_back(std::move(task));
    }

    for (auto &task : tasks) {
      task.wait();
    }

    auto result =
        get_list_objects(get_test_osspath(""), FLAGS_oss_bucket_prefix);
    ASSERT_EQ((size_t)file_cnt, result.size());
    LOG_INFO("write ` files in the dir", file_cnt);

    for (int i = 0; i < file_cnt; i++) {
      std::string file_name = "testfile_" + std::to_string(i);
      auto file_meta = get_file_meta(file_name, FLAGS_oss_bucket_prefix);

      ASSERT_EQ(std::to_string(file_crcs[i]),
                file_meta["X-Oss-Hash-Crc64ecma"]);
    }

    tasks.clear();
    std::vector<uint64_t> file_read_crcs(file_cnt, 0);
    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        INIT_PHOTON();
        for (int j = i; j < file_cnt; j += parallel_cnt) {
          std::string file_name = "testfile_" + std::to_string(j);
          ssize_t r =
              read_file_in_folder(parent, file_name, &file_read_crcs[j]);
          ASSERT_TRUE(r >= 0);
        }
      });
      tasks.push_back(std::move(task));
    }

    for (auto &task : tasks) {
      task.wait();
    }

    for (int i = 0; i < file_cnt; i++) {
      ASSERT_EQ(file_crcs[i], file_read_crcs[i]);
    }
  }

  void verify_parallel_read_one_fd() {
    srand(time(nullptr));

    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    const size_t file_size_MB = 1823 + rand() % 128;
    auto crc64 =
        create_file_in_folder(parent, "testfile", file_size_MB, nodeid);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);

    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    auto read_once = [=](void *handle, uint64_t file_size,
                         uint64_t *out_crc64) -> ssize_t {
      uint64_t offset = 0;

      uint64_t buf_size = IO_SIZE;
      char *buf = new char[IO_SIZE];
      DEFER(delete[] buf);

      uint64_t crc64 = 0;
      while (offset < file_size) {
        auto read_size = std::min(buf_size, file_size - offset);
        auto r = read_from_handle(handle, buf, read_size, offset);
        if (r < 0) return r;

        crc64 = cal_crc64(crc64, buf, r);
        offset += r;
      }

      *out_crc64 = crc64;
      return 0;
    };

    std::vector<std::future<int>> tasks;
    int parallel_cnt = 16;

    for (int i = 0; i < parallel_cnt; i++) {
      auto task = std::async(std::launch::async, [&, i]() {
        std::this_thread::sleep_for(std::chrono::seconds(rand() % 5 + 1));
        INIT_PHOTON();
        uint64_t out_crc64 = 0;
        int r = read_once(handle, file_size_MB * 1024 * 1024, &out_crc64);
        if (r < 0) return r;
        if (out_crc64 != crc64) {
          LOG_ERROR("crc mismatch ` vs `", crc64, out_crc64);
          return -EIO;
        }

        return 0;
      });

      tasks.push_back(std::move(task));
    }

    for (auto &task : tasks) {
      ASSERT_EQ(0, task.get());
    }
  }

  void verify_oss_error_during_append_normal_object(uint64_t file_size_MB) {
    struct stat st;
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filepath = "testfile";
    auto parent_path = nodeid_to_path(parent);
    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, file_size_MB);
    int r = upload_file(local_file, join_paths(parent_path, filepath),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    auto meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    auto crc = meta["X-Oss-Hash-Crc64ecma"];

    uint64_t nodeid = 0;
    r = fs_->lookup(parent, filepath.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);

    DEFER(fs_->forget(nodeid, 1));

    void *fh = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &fh, &unused);
    ASSERT_EQ(r, 0);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Download_Failed_During_Merge_Remote_Data);

    char *buf = new char[1048576];
    DEFER(delete[] buf);
    ssize_t write_size = (static_cast<IFileHandleFuseLL *>(fh))
                             ->pwrite(buf, 1048576, st.st_size);
    ASSERT_EQ(write_size, -EIO);

    fs_->release(nodeid, get_file_from_handle(fh));

    meta = get_file_meta(filepath, FLAGS_oss_bucket_prefix);
    ASSERT_EQ(crc, meta["X-Oss-Hash-Crc64ecma"]);
  }

  void verify_oss_multipart_upload_limit() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // i == 0 for aligned write and i == 1 for unaligned write
    for (int i = 0; i < 2; i++) {
      struct stat st;
      uint64_t nodeid = 0;
      void *handle = nullptr;
      std::string filename = "testfile_" + std::to_string(i);
      int r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS,
                               0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      char *buf_1MB = new char[1048576];
      DEFER(delete[] buf_1MB);

      ssize_t write_size = 1048576 - i;
      uint64_t offset = 0;
      for (uint64_t i = 0; i < 10000; i++) {
        r = write_to_file_handle(handle, buf_1MB, write_size, offset);
        EXPECT_EQ(r, write_size);
        offset += write_size;
      }

      r = write_to_file_handle(handle, buf_1MB, write_size, offset);
      EXPECT_EQ(r, -EFBIG);

      r = fs_->getattr(nodeid, &st);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(st.st_size, static_cast<ssize_t>(offset));

      r = fs_->release(nodeid, get_file_from_handle(handle));
      EXPECT_EQ(r, 0);

      r = fs_->getattr(nodeid, &st);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(st.st_size, static_cast<ssize_t>(offset));
    }
  }

  void verify_create_and_write_with_different_handle(bool appendable = false) {
    uint64_t parent = get_test_dir_parent();

    std::vector<uint64_t> nodeids = {parent};
    DEFER(for (auto nodeid : nodeids) { fs_->forget(nodeid, 1); });
    auto release_all = [&](uint64_t nodeid,
                           const std::vector<void *> &handles) {
      for (auto handle : handles) {
        int r = fs_->release(nodeid, get_file_from_handle(handle));
        ASSERT_EQ(r, 0);
      }
    };

    auto check_file_content = [&](uint64_t nodeid, std::string_view content) {
      void *handle = nullptr;
      bool unused = false;
      int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
      ASSERT_EQ(r, 0);
      char buf[1024];
      r = read_from_handle(handle, buf, sizeof(buf), 0);
      ASSERT_EQ(r, (ssize_t)content.size());
      ASSERT_EQ(memcmp(buf, content.data(), content.size()), 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    };

    // 1. create, open and write something
    struct stat st;
    std::string filepath = "testfile1";
    uint64_t nodeid = 0;
    void *handle1 = nullptr;
    int r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0,
                       0, &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    nodeids.push_back(nodeid);

    void *handle2 = nullptr;
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle2, &unused);
    ASSERT_EQ(r, 0);

    r = write_to_file_handle(handle2, "hello", 5, 0);
    ASSERT_EQ(r, 5);

    void *handle3 = nullptr;
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle3, &unused);
    ASSERT_EQ(r, -EBUSY);
    r = fs_->open(nodeid, O_RDWR, &handle3, &unused);
    ASSERT_EQ(r, 0);
    r = write_to_file_handle(handle1, "hello", 5, 5);
    ASSERT_EQ(r, -EBUSY);
    r = write_to_file_handle(handle3, "hello", 5, 5);
    ASSERT_EQ(r, -EBUSY);

    release_all(nodeid, {handle1, handle2, handle3});

    void *handle4 = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle4, &unused);
    ASSERT_EQ(r, 0);
    r = write_to_file_handle(handle4, "hello", 5, 5);
    ASSERT_EQ(r, 5);
    release_all(nodeid, {handle4});
    check_file_content(nodeid, "hellohello");

    // 2. create, truncate and not write
    filepath = "testfile2";
    r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    nodeids.push_back(nodeid);
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle2, &unused);
    ASSERT_EQ(r, 0);

    release_all(nodeid, {handle1, handle2});
    check_file_content(nodeid, "");

    // 3. create handle1 and write handle2, but error during write handle2,
    // then handle1 is allowed to write
    filepath = "testfile3";
    r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    nodeids.push_back(nodeid);

    r = fs_->open(nodeid, O_RDWR, &handle2, &unused);
    ASSERT_EQ(r, 0);
    r = write_to_file_handle(handle2, "hello", 5, 1);
    ASSERT_EQ(r, -EINVAL);
    r = write_to_file_handle(handle1, "hello", 5, 0);
    ASSERT_EQ(r, 5);

    release_all(nodeid, {handle1, handle2});
    check_file_content(nodeid, "hello");

    // 4. create, write with rename dirty dir
    uint64_t dir_nodeid = 0;
    std::string dir_path = "testdir";
    r = fs_->mkdir(parent, dir_path.c_str(), 0777, 0, 0, 0, &dir_nodeid, &st);
    ASSERT_EQ(r, 0);
    nodeids.push_back(dir_nodeid);
    filepath = "testfile4";
    r = fs_->creat(dir_nodeid, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0,
                   0, &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    nodeids.push_back(nodeid);

    g_fault_injector->set_injection(
        FaultInjectionId::FI_Force_Flush_Dirty_Handle_Delay);
    auto write_task = std::async(std::launch::async, [&]() {
      r = fs_->open(nodeid, O_RDWR, &handle2, &unused);
      ASSERT_EQ(r, 0);
      r = write_to_file_handle(handle2, "hello", 5, 0);
      ASSERT_EQ(r, 5);
      release_all(nodeid, {handle2});
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::string dir_path2 = "testdir2";
    r = fs_->rename(parent, dir_path.c_str(), parent, dir_path2.c_str(), 0);
    ASSERT_EQ(r, 0);

    write_task.wait();
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_Force_Flush_Dirty_Handle_Delay);
    release_all(nodeid, {handle1});
    check_file_content(nodeid, "hello");

    auto parent_path = nodeid_to_path(parent);
    r = stat_file(join_paths(parent_path, join_paths(dir_path2, filepath)),
                  FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    // 5. write nothing to an empty file, and write again
    filepath = "testfile5";
    r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                   &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    nodeids.push_back(nodeid);
    release_all(nodeid, {handle1});

    r = fs_->open(nodeid, O_RDWR, &handle2, &unused);
    ASSERT_EQ(r, 0);
    r = write_to_file_handle(handle2, "", 0, 0);
    ASSERT_EQ(r, 0);

    r = fs_->open(nodeid, O_RDWR, &handle3, &unused);
    ASSERT_EQ(r, 0);
    r = write_to_file_handle(handle3, "hello", 5, 0);
    ASSERT_EQ(r, 5);
    r = write_to_file_handle(handle2, "hello", 5, 5);
    ASSERT_EQ(r, -EBUSY);

    release_all(nodeid, {handle2, handle3});
    check_file_content(nodeid, "hello");
  }

  void verify_prefetch_chunks(int round) {
    srand(time(nullptr));

    std::string file_prefix = "testfile_" + std::to_string(round) + "_";
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int prefetch_chunks_per_file =
        fs_->options_.prefetch_concurrency_per_file * 3;

    uint64_t max_prefetch_chunks = 0;
    int total_file_count = rand() % 4 + 8;

    if (fs_->options_.prefetch_chunks == 0) {
      max_prefetch_chunks = fs_->options_.prefetch_concurrency * 3;
    } else if (fs_->options_.prefetch_chunks > 0) {
      // we always allocate at least one prefetch chunks, so max prefetch
      // total blocks is aligned downward to prefetch_chunks
      max_prefetch_chunks = fs_->options_.prefetch_chunks;
      total_file_count = std::max(
          total_file_count,
          fs_->options_.prefetch_chunks / prefetch_chunks_per_file + 2);
    } else {
      max_prefetch_chunks = total_file_count * prefetch_chunks_per_file;
    }

    for (int i = 0; i < total_file_count; i++) {
      uint64_t nodeid = 0;
      int file_size_base_MB =
          (prefetch_chunks_per_file * fs_->options_.prefetch_chunk_size) /
              1048576 +
          1;
      int file_size_MB = file_size_base_MB + rand() % 10;
      int file_draft = 1 + rand() % 1023;
      auto crc = create_file_in_folder(parent, file_prefix + std::to_string(i),
                                       file_size_MB, nodeid, file_draft);
      ASSERT_TRUE(crc > 0);
      DEFER(fs_->forget(nodeid, 1));
    }

    std::array<int, 2> parallel_cnts = {1, 8};

    for (auto parallel_cnt : parallel_cnts) {
      ASSERT_EQ(fs_->download_buffers_->used_blocks(), 0ULL);
      std::vector<std::future<void>> tasks;

      std::vector<void *> fds(total_file_count, 0);
      std::vector<uint64_t> nodeids(total_file_count, 0);

      for (int i = 0; i < parallel_cnt; i++) {
        auto task = std::async(std::launch::async, [&, i]() {
          INIT_PHOTON();
          for (int j = i; j < total_file_count; j += parallel_cnt) {
            struct stat st;
            std::string file_name = file_prefix + std::to_string(j);
            int r = fs_->lookup(parent, file_name.c_str(), &nodeids[j], &st);
            ASSERT_EQ(r, 0);

            bool unused;
            r = fs_->open(nodeids[j], O_RDWR, &fds[j], &unused);
            ASSERT_EQ(r, 0);

            r = read_file_content(fds[j], st.st_size, file_name);
            ASSERT_EQ(r, 0);
          }
        });
        tasks.push_back(std::move(task));
      }

      for (auto &task : tasks) {
        task.wait();
      }

      ASSERT_EQ(fs_->download_buffers_->used_blocks(),
                max_prefetch_chunks * (fs_->options_.prefetch_chunk_size /
                                       fs_->options_.cache_block_size));

      for (int i = 0; i < total_file_count; i++) {
        fs_->release(nodeids[i], get_file_from_handle(fds[i]));
        fs_->forget(nodeids[i], 1);
      }
    }
  }

  void verify_close_to_open_with_inode_cache() {
    fs_->options_.close_to_open = true;
    for (int i = 0; i < 10; i++) {
      fs_->options_.bind_cache_to_inode = i % 2;

      uint64_t parent = get_test_dir_parent();
      DEFER(fs_->forget(parent, 1));

      // 1. create a file
      struct stat st;
      std::string filepath = "testfile-" + std::to_string(i);
      uint64_t nodeid = 0;
      void *handle1 = nullptr;
      int r = fs_->creat(parent, filepath.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid, &st, &handle1);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      r = write_to_file_handle(handle1, "hello", 5, 0);
      ASSERT_EQ(r, 5);

      r = fs_->release(nodeid, get_file_from_handle(handle1));
      ASSERT_EQ(r, 0);

      // 2. read from handle1 and data will be cached
      bool unused;
      r = fs_->open(nodeid, O_RDONLY, &handle1, &unused);
      ASSERT_EQ(r, 0);
      DEFER(fs_->release(nodeid, get_file_from_handle(handle1)));

      std::string buf;
      buf.resize(5);
      LOG_INFO("read from handle1 `", buf.size());
      r = read_from_handle(handle1, const_cast<char *>(buf.c_str()), buf.size(),
                           0);
      ASSERT_EQ(r, 5);
      ASSERT_EQ(buf, "hello");

      // do not close handle1 and overwrite with handle2
      void *handle2 = nullptr;
      r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle2, &unused);
      ASSERT_EQ(r, 0);

      std::string new_data = random_string(rand() % 10 + 1);
      r = write_to_file_handle(handle2, new_data.c_str(), new_data.size(), 0);
      ASSERT_EQ(r, static_cast<ssize_t>(new_data.size()));

      r = fs_->release(nodeid, get_file_from_handle(handle2));
      ASSERT_EQ(r, 0);

      // 2. open the file again, we should read the new content
      void *handle3 = nullptr;
      r = fs_->open(nodeid, O_RDWR, &handle3, &unused);
      ASSERT_EQ(r, 0);

      buf.resize(new_data.size());
      r = read_from_handle(handle3, const_cast<char *>(buf.c_str()), buf.size(),
                           0);
      ASSERT_EQ(r, static_cast<ssize_t>(new_data.size()));
      ASSERT_EQ(buf, new_data);

      r = fs_->release(nodeid, get_file_from_handle(handle3));
      ASSERT_EQ(r, 0);
    }
  }

  void verify_open_with_append_flag() {
    for (int i = 0; i < 2; i++) {
      fs_->options_.enable_appendable_object = i;

      uint64_t parent = get_test_dir_parent();
      DEFER(fs_->forget(parent, 1));

      std::string filename = "testfile-" + std::to_string(i);
      uint64_t nodeid = 0;
      void *handle = nullptr;
      struct stat st;
      int r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS | O_APPEND,
                         0777, 0, 0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));

      r = write_to_file_handle(handle, "hello", 5, 0);
      EXPECT_EQ(r, 5);

      r = write_to_file_handle(handle, "world", 5, 1);
      EXPECT_EQ(r, 5);

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      bool unused;
      r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
      ASSERT_EQ(r, 0);

      r = write_to_file_handle(handle, "append", 6, 4);
      EXPECT_EQ(r, 6);

      r = write_to_file_handle(handle, "test", 4, 1000);
      EXPECT_EQ(r, 4);

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      char buf[IO_SIZE];
      r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
      ASSERT_EQ(r, 0);

      r = read_from_handle(handle, buf, IO_SIZE, 0);
      ASSERT_EQ(r, 20);

      ASSERT_EQ(std::string(buf, r), "helloworldappendtest");

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }
  }

  void verify_local_change_with_existing_fh() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::string filename = "testfile";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS | O_APPEND,
                       0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    r = write_to_file_handle(handle, "hello", 5, 0);
    EXPECT_EQ(r, 5);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    void *reader_handle = nullptr;
    bool unused;
    r = fs_->open(nodeid, O_RDONLY, &reader_handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(reader_handle)));

    char buf[IO_SIZE];
    r = read_from_handle(reader_handle, buf, IO_SIZE, 0);
    ASSERT_EQ(r, 5);
    ASSERT_EQ(std::string(buf, r), "hello");

    void *writer_handle = nullptr;
    {
      r = fs_->open(nodeid, O_RDWR | O_APPEND, &writer_handle, &unused);
      ASSERT_EQ(r, 0);
      DEFER(fs_->release(nodeid, get_file_from_handle(writer_handle)));

      r = write_to_file_handle(writer_handle, "world", 5, 5);
      EXPECT_EQ(r, 5);
    }

    r = read_from_handle(reader_handle, buf, IO_SIZE, 0);
    ASSERT_EQ(r, 10);
    ASSERT_EQ(std::string(buf, r), "helloworld");

    const uint64_t max_size = 20ULL << 20;
    for (int i = 0; i < 5; i++) {
      std::string random_data = random_string(rand() % max_size + 1);

      r = fs_->open(nodeid, O_RDWR | O_TRUNC, &writer_handle, &unused);
      ASSERT_EQ(r, 0);

      r = write_to_file_handle(writer_handle, random_data.c_str(),
                               random_data.size(), 0);
      EXPECT_EQ(r, static_cast<ssize_t>(random_data.size()));

      r = fs_->release(nodeid, get_file_from_handle(writer_handle));
      ASSERT_EQ(r, 0);

      std::string read_data;
      r = read_file_content(reader_handle, random_data.size(), filename,
                            &read_data);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(random_data, read_data);
    }
  }

  void verify_remote_change_with_existing_fh() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "testfile";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS | O_APPEND,
                       0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    r = write_to_file_handle(handle, "hello", 5, 0);
    EXPECT_EQ(r, 5);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    void *reader_handle = nullptr;
    bool unused;
    r = fs_->open(nodeid, O_RDONLY, &reader_handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(reader_handle)));

    char buf[IO_SIZE];
    r = read_from_handle(reader_handle, buf, IO_SIZE, 0);
    ASSERT_EQ(r, 5);
    ASSERT_EQ(std::string(buf, r), "hello");

    std::ofstream tmpfile("tmpfile", std::ios::out | std::ios::trunc);
    DEFER(unlink("tmpfile"));

    tmpfile << "helloworld";
    tmpfile.close();

    r = upload_file("tmpfile", join_paths(parent_path, "testfile"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
    ASSERT_EQ(r, 0);
    fs_->forget(nodeid, 1);

    r = read_from_handle(reader_handle, buf, IO_SIZE, 0);
    ASSERT_EQ(r, 10);
    ASSERT_EQ(std::string(buf, r), "helloworld");

    const uint64_t max_size = 20ULL << 20;
    for (int i = 0; i < 5; i++) {
      std::string random_data = random_string(rand() % max_size + 1);

      tmpfile.open("tmpfile", std::ios::out | std::ios::trunc);
      tmpfile << random_data;
      tmpfile.close();

      r = upload_file("tmpfile", join_paths(parent_path, "testfile"),
                      FLAGS_oss_bucket_prefix);
      ASSERT_EQ(r, 0);
      std::this_thread::sleep_for(std::chrono::seconds(1));

      r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);
      fs_->forget(nodeid, 1);

      std::string read_data;
      r = read_file_content(reader_handle, random_data.size(), filename,
                            &read_data);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(random_data, read_data);
    }
  }

  void verify_remote_change_with_existing_fh_parallel() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string filename = "testfile";
    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS | O_APPEND,
                       0777, 0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    int concurrency = 16;
    std::vector<void *> reader_handles;

    for (int i = 0; i < concurrency; i++) {
      void *handle = nullptr;
      bool unused;
      int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
      ASSERT_EQ(r, 0);

      reader_handles.push_back(handle);
    }

    DEFER({
      for (auto handle : reader_handles) {
        fs_->release(nodeid, get_file_from_handle(handle));
      }
    });

    std::vector<std::thread> threads;
    photon::rwlock rwlock;
    std::atomic<bool> is_stopping = {false};
    uint64_t g_epoch = 0;
    uint64_t g_file_size = 0;

    // start readers
    for (int i = 0; i < concurrency; i++) {
      threads.emplace_back([&, i]() {
        INIT_PHOTON();
        char buf[IO_SIZE];
        uint64_t epoch = 0;
        uint64_t offset = 0;
        while (!is_stopping) {
          size_t read_size = 0;
          {
            photon::scoped_rwlock _(rwlock, photon::RLOCK);
            if (epoch != g_epoch) {
              epoch = g_epoch;
            }

            if (offset >= g_file_size) {
              offset = 0;
            }

            read_size = std::min(IO_SIZE, g_file_size - offset);
            ssize_t r =
                read_from_handle(reader_handles[i], buf, read_size, offset);
            if (r != static_cast<ssize_t>(read_size)) is_stopping = true;
            ASSERT_EQ(r, static_cast<ssize_t>(read_size));

            offset += read_size;
          }

          for (size_t i = 0; i < read_size; i++) {
            if (static_cast<uint64_t>(buf[i]) < epoch) {
              LOG_ERROR("Fail to verify data buf[i] = `, epoch = `", buf[i],
                        epoch);
            }
            ASSERT_TRUE(static_cast<uint64_t>(buf[i]) >= epoch);
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
      });
    }

    // start writer
    uint64_t max_file_size = 256ULL << 20;
    std::vector<uint64_t> test_file_sizes = {1, 1234567, 1234500, 1000000,
                                             77777};
    for (int i = 0; i < 25; i++) {
      test_file_sizes.push_back(rand() % max_file_size + 1);
    }

    for (size_t i = 0; i < test_file_sizes.size() && !is_stopping; i++) {
      uint64_t file_size = test_file_sizes[i];
      std::string data(IO_SIZE, (char)(i + 1));
      std::ofstream tmpfile("tmpfile", std::ios::out | std::ios::trunc);
      DEFER(unlink("tmpfile"));
      uint64_t offset = 0;
      while (offset < file_size) {
        auto write_size = std::min(IO_SIZE, file_size - offset);
        tmpfile << data.substr(0, write_size);
        offset += write_size;
      }
      tmpfile.close();

      std::this_thread::sleep_for(std::chrono::seconds(1));

      photon::scoped_rwlock _(rwlock, photon::WLOCK);
      r = upload_file("tmpfile", join_paths(parent_path, "testfile"),
                      FLAGS_oss_bucket_prefix);

      g_epoch = i;
      g_file_size = file_size;
      r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
      ASSERT_EQ(r, 0);
      fs_->forget(nodeid, 1);
    }

    is_stopping = true;
    for (auto &thread : threads) {
      thread.join();
    }
  }

 private:
  int read_file_content(void *fh, size_t file_size, std::string_view file_name,
                        std::string *out = nullptr) {
    if (out) out->clear();

    char buf[IO_SIZE];
    size_t buf_size = IO_SIZE;
    uint64_t offset = 0;
    while (offset < file_size) {
      buf_size = std::min(buf_size, file_size - offset);
      auto read_size = read_from_handle(fh, buf, buf_size, offset);
      if (read_size != static_cast<ssize_t>(buf_size)) {
        LOG_ERROR("Failed to read file content of `, r = `", file_name,
                  read_size);
        return -EIO;
      }

      if (out) {
        out->append(buf, read_size);
      }

      offset += buf_size;
    }
    return 0;
  }
};

TEST_F(Ossfs2ReadWriteTest, verify_write_files) {
  INIT_PHOTON();

  OssFsOptions opts;
  init(opts);
  verify_write_files();
}

TEST_F(Ossfs2ReadWriteTest, verify_read_out_of_range) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 1048576 * 2;
  opts.prefetch_chunk_size = 1048576 * 2;
  opts.attr_timeout = 30;
  init(opts);
  verify_read_out_of_range();
}

TEST_F(Ossfs2ReadWriteTest, verify_transmission_control) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_transmission_control = true;
  opts.tc_max_latency_threshold_us = 100000;
  init(opts);
  verify_transmission_control();
}

TEST_F(Ossfs2ReadWriteTest, verify_write_with_fuse_write_buf_failed) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_write_with_fuse_write_buf_failed();
}

TEST_F(Ossfs2ReadWriteTest, verify_min_reserved_buffer_size_per_file) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.min_reserved_buffer_size_per_file = 0;
  init(opts);
  verify_min_reserved_buffer_size_per_file();

  // test data consistency
  verify_concurrent_write_and_read();
}

TEST_F(Ossfs2ReadWriteTest, verify_write_with_oss_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_concurrency = 1;
  opts.prefetch_concurrency = 1;
  init(opts);
  verify_write_with_oss_error();
}

TEST_F(Ossfs2ReadWriteTest, verify_readwrite_archive_and_ia_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_readwrite_archive_and_ia_object();
}

TEST_F(Ossfs2ReadWriteTest, verify_read_dirty_fd) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_read_dirty_fd();
}

TEST_F(Ossfs2ReadWriteTest, verify_write_to_immutable_handle_normal) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_write_to_immutable_handle(".normal");
}

TEST_F(Ossfs2ReadWriteTest, verify_write_to_immutable_handle_multipart) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 131072;
  init(opts);
  verify_write_to_immutable_handle(".multipart");
}

TEST_F(Ossfs2ReadWriteTest, verify_init_multipart_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 131072;
  init(opts);
  verify_init_multipart_error();
}

TEST_F(Ossfs2ReadWriteTest, verify_read_dirty_object_with_oss_error) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1024;
  init(opts);
  verify_read_dirty_object_with_oss_error();
}

TEST_F(Ossfs2ReadWriteTest, verify_read_repeatedly_with_prefetch) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.prefetch_chunk_size = 1048576 * 8;
  opts.cache_refill_unit = 1048576;
  init(opts);
  verify_read_repeatedly_with_prefetch();
}

TEST_F(Ossfs2ReadWriteTest, verify_direct_read_oss) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.prefetch_concurrency = 0;
  // prefetch chunks should be ignored
  opts.prefetch_chunks = 128;
  init(opts);
  verify_direct_read_oss();
}

TEST_F(Ossfs2ReadWriteTest, verify_parallel_read_one_fd) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_parallel_read_one_fd();
}

TEST_F(Ossfs2ReadWriteTest, verify_oss_error_during_append_normal_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_oss_error_during_append_normal_object(3);

  destroy();
  init(opts);
  verify_oss_error_during_append_normal_object(115);
}

TEST_F(Ossfs2ReadWriteTest, verify_oss_multipart_upload_limit) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 1048576;
  init(opts);
  verify_oss_multipart_upload_limit();
}

TEST_F(Ossfs2ReadWriteTest, verify_create_and_write_with_different_handle) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_create_and_write_with_different_handle();
}

TEST_F(Ossfs2ReadWriteTest,
       verify_create_and_write_with_different_handle_for_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  init(opts);
  verify_create_and_write_with_different_handle(true);
}

TEST_F(Ossfs2ReadWriteTest, verify_default_prefetch_chunks) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.max_total_reserved_buffer_count = 0;

  opts.prefetch_chunk_size = 1024 * 1024 * 2;
  opts.prefetch_concurrency = 16;
  opts.prefetch_concurrency_per_file = 4;
  init(opts);
  verify_prefetch_chunks(1);
  destroy();

  opts.prefetch_chunk_size = 1024 * 1024;
  opts.prefetch_concurrency = 8;
  opts.prefetch_concurrency_per_file = 2;
  init(opts);
  verify_prefetch_chunks(2);
}

TEST_F(Ossfs2ReadWriteTest, verify_sepcified_prefetch_chunks) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.max_total_reserved_buffer_count = 0;
  opts.prefetch_chunk_size = 1024 * 1024;
  opts.prefetch_concurrency = 8;
  opts.prefetch_concurrency_per_file = 2;

  opts.prefetch_chunks = 16;
  init(opts);
  verify_prefetch_chunks(1);
  destroy();

  opts.prefetch_chunks = 31;
  init(opts);
  verify_prefetch_chunks(2);
  destroy();

  opts.prefetch_chunks = -1;
  init(opts);
  verify_prefetch_chunks(3);
  destroy();

  opts.prefetch_chunk_size = 1024 * 1024 * 2;
  opts.prefetch_chunks = 15;
  init(opts);
  verify_prefetch_chunks(4);
}

TEST_F(Ossfs2ReadWriteTest, verify_inode_cache) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.bind_cache_to_inode = true;
  init(opts, 100, "", true);

  verify_concurrent_write_and_read("ALL_FILES_PER_WORKER", 53, 32);
}

TEST_F(Ossfs2ReadWriteTest, verify_inode_cache_random_select) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.bind_cache_to_inode = true;
  init(opts, 100, "", true);

  verify_concurrent_write_and_read("RANDOM", 25, 16);
}

TEST_F(Ossfs2ReadWriteTest, verify_close_to_open_with_inode_cache) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  verify_close_to_open_with_inode_cache();
}

TEST_F(Ossfs2ReadWriteTest, verify_open_with_append_flag) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  verify_open_with_append_flag();
}

TEST_F(Ossfs2ReadWriteTest, verify_local_change_with_existing_fh) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);

  verify_local_change_with_existing_fh();
}

TEST_F(Ossfs2ReadWriteTest, verify_remote_change_with_existing_fh) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  init(opts);

  verify_remote_change_with_existing_fh();
}

TEST_F(Ossfs2ReadWriteTest, verify_remote_change_with_existing_fh_parallel) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  init(opts);

  verify_remote_change_with_existing_fh_parallel();
}
