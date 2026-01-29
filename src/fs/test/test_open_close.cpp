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

class Ossfs2OpenCloseTest : public Ossfs2TestSuite {
 protected:
  void verify_open() {
    auto test_dir_nodeid = get_test_dir_parent();
    int r = 0;
    DEFER(fs_->forget(test_dir_nodeid, 1));

    uint64_t parent;
    struct stat st;
    r = fs_->mkdir(test_dir_nodeid, "workdir", 0777, 0, 0, 0, &parent, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(parent, 1));

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);

    auto parent_path = nodeid_to_path(parent);
    r = upload_file(local_file, join_paths(parent_path, "file-internal"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid;
    void *handle = nullptr, *handle2 = nullptr;
    // create inode1 for testdir/workdir/file-internal
    r = fs_->lookup(parent, "file-internal", &nodeid, &st);
    ASSERT_EQ(r, 0);

    bool keep_page_cache = false;
    // open with truncate flag
    r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle, &keep_page_cache);
    ASSERT_EQ(r, 0);

    // verify this file can be written at the beginning
    auto file = (OssFileHandle *)(handle);
    auto random_data = random_string(1 << 20);
    r = file->pwrite(random_data.c_str(), 1 << 20, 0);
    ASSERT_EQ(r, 1 << 20);

    // verify this file is dirty and cannot be truncated
    ASSERT_TRUE(file->get_is_dirty());
    auto iter = fs_->global_inodes_map_.find(nodeid);
    ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
    ASSERT_TRUE(static_cast<FileInode *>(iter->second)->is_dirty_file());
    r = fs_->open(nodeid, O_TRUNC, &handle, &keep_page_cache);
    ASSERT_EQ(r, -EBUSY);

    // verify this file cannot be written via 2 handles
    r = fs_->open(nodeid, O_RDWR, &handle2, &keep_page_cache);
    ASSERT_EQ(r, 0);
    auto file2 = get_file_from_handle(handle2);
    r = file2->pwrite(random_data.c_str(), 1 << 20, 0);
    ASSERT_EQ(r, -EBUSY);
    r = fs_->release(nodeid, get_file_from_handle(handle2));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    fs_->forget(nodeid, 1);
  }

  void verify_filehandle_release() {
    std::vector<uint64_t> nodeids;
    std::mutex mtx;

    DEFER({
      std::lock_guard<std::mutex> lock(mtx);
      for (auto nodeid : nodeids) {
        fs_->forget(nodeid, 1);
      }
    });

    std::queue<std::pair<uint64_t, void *>> queue;
    std::atomic<bool> is_stopping(false);
    std::atomic<bool> open_finished(false);

    std::thread release_thread([&]() {
      INIT_PHOTON();
      while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        if (open_finished && queue.empty()) break;
        if (queue.empty()) {
          mtx.unlock();
          photon::thread_usleep(3);
          mtx.lock();
        } else {
          uint64_t nodeid = queue.front().first;
          void *fh = queue.front().second;
          queue.pop();
          mtx.unlock();
          auto file = get_file_from_handle(fh);
          fs_->release(nodeid, file);
          mtx.lock();
        }
      }
    });
    DEFER(release_thread.join());

    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::vector<std::future<void>> tasks;
    for (int i = 0; i < 16; ++i) {
      auto task = std::async(
          std::launch::async,
          [&](int thread_id) {
            INIT_PHOTON();
            uint64_t nodeid = 0, crc64 = 0;
            crc64 = create_file_in_folder(
                parent, "file" + std::to_string(thread_id), 1, nodeid);
            mtx.lock();
            nodeids.push_back(nodeid);
            mtx.unlock();

            while (!is_stopping) {
              void *fh = nullptr;
              bool unused = false;
              int r = fs_->open(nodeid, O_RDONLY, &fh, &unused);
              ASSERT_EQ(r, 0);

              auto file = get_file_from_handle(fh);
              void *mem;
              ssize_t ret = file->pin(0, 1048576, &mem);
              ASSERT_EQ(ret, 1048576);

              auto read_crc = crc64ecma(mem, 1048576, 0);
              ASSERT_EQ(read_crc, crc64);

              mtx.lock();
              queue.push(std::make_pair(nodeid, fh));
              mtx.unlock();

              int random_time = rand() % 100 + 1;
              photon::thread_usleep(random_time);

              file->unpin(0);
            }
          },
          i);

      tasks.push_back(std::move(task));
    }

    // running 1 minutes
    std::this_thread::sleep_for(std::chrono::seconds(60));

    is_stopping = true;
    for (auto &task : tasks) {
      task.wait();
    }

    open_finished = true;
  }

  void verify_close_to_open() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = upload_file(local_file, join_paths(parent_path, "testfile"),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    r = fs_->lookup(parent, "testfile", &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    create_random_file(local_file, 11);
    r = upload_file(local_file, join_paths(parent_path, "testfile"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 11 * 1048576);

    char buf[4096];
    auto write_size = write_to_file_handle(handle, buf, 4096, st.st_size);
    ASSERT_EQ(write_size, 4096);

    // open dirty file
    void *new_handle = nullptr;
    r = fs_->open(nodeid, O_RDWR, &new_handle, &unused);
    ASSERT_EQ(r, 0);

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 11 * 1048576 + 4096);

    fs_->release(nodeid, get_file_from_handle(new_handle));
  }

  void verify_close_to_open_stale() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    auto parent_path = nodeid_to_path(parent);

    std::string local_file = join_paths(test_path_, "local_file");
    create_random_file(local_file, 3);
    int r = upload_file(local_file, join_paths(parent_path, "testfile"),
                        FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);

    uint64_t nodeid = 0;
    void *handle = nullptr, *handle1 = nullptr;
    struct stat st;
    r = fs_->lookup(parent, "testfile", &nodeid, &st);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    // 1. stat returns err not being ENOENT
    bool unused = false;
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_Call_Failed);
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, -EIO);
    g_fault_injector->clear_injection(
        FaultInjectionId::FI_OssError_Call_Failed);

    // 2. stat returns err ENOENT, open_ref_cnt != 0
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);
    auto iter = fs_->global_inodes_map_.find(nodeid);
    ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
    ASSERT_EQ(iter->second->open_ref_cnt, 1);

    r = delete_file(join_paths(parent_path, "testfile"),
                    FLAGS_oss_bucket_prefix);
    ASSERT_EQ(r, 0);
    r = fs_->open(nodeid, O_RDWR, &handle1, &unused);
    ASSERT_EQ(r, -ENOENT);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    ASSERT_EQ(iter->second->open_ref_cnt, 0);
    ASSERT_FALSE(iter->second->is_stale.load());

    // 3. stat returns err ENOENT, open_ref_cnt == 0
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, -ENOENT);

    // inode is marked stale
    iter = fs_->global_inodes_map_.find(nodeid);
    ASSERT_TRUE(iter != fs_->global_inodes_map_.end());
    ASSERT_TRUE(iter->second->is_stale.load());
  }

  void verify_max_total_reserved_buffer_count() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    const int total_file_count =
        fs_->options_.max_total_reserved_buffer_count + 3;

    srand(time(nullptr));
    for (int i = 0; i < total_file_count; i++) {
      uint64_t nodeid = 0;
      int file_size_MB = 1 + rand() % 10;
      int file_draft = 1 + rand() % 1023;
      auto crc = create_file_in_folder(parent, "testfile-" + std::to_string(i),
                                       file_size_MB, nodeid, file_draft);
      ASSERT_TRUE(crc > 0);
      DEFER(fs_->forget(nodeid, 1));
    }

    // open in one thread
    {
      std::vector<void *> fds(total_file_count, nullptr);
      std::vector<uint64_t> nodeids(total_file_count, 0);
      for (int i = 0; i < total_file_count; i++) {
        struct stat st;
        std::string filepath = "testfile-" + std::to_string(i);
        int r = fs_->lookup(parent, filepath.c_str(), &nodeids[i], &st);
        ASSERT_EQ(r, 0);
        bool unused;
        r = fs_->open(nodeids[i], O_RDWR, &fds[i], &unused);
        ASSERT_EQ(r, 0);
      }

      ASSERT_EQ(fs_->download_buffers_->used_blocks(),
                fs_->options_.max_total_reserved_buffer_count);

      for (int i = 0; i < total_file_count; i++) {
        fs_->release(nodeids[i], get_file_from_handle(fds[i]));
        fs_->forget(nodeids[i], 1);
      }
    }

    ASSERT_EQ(fs_->download_buffers_->used_blocks(), 0ULL);

    // open parallel
    {
      std::vector<void *> fds(total_file_count, nullptr);
      std::vector<uint64_t> nodeids(total_file_count, 0);
      std::vector<std::future<void>> tasks;
      int parallel_count = 8;
      for (int i = 0; i < parallel_count; i++) {
        auto task = std::async(std::launch::async, [&, i]() {
          INIT_PHOTON();
          for (int j = i; j < total_file_count; j += parallel_count) {
            struct stat st;
            std::string filepath = "testfile-" + std::to_string(j);
            int r = fs_->lookup(parent, filepath.c_str(), &nodeids[j], &st);
            ASSERT_EQ(r, 0);
            bool unused;
            r = fs_->open(nodeids[j], O_RDWR, &fds[j], &unused);
            ASSERT_EQ(r, 0);
          }
        });
        tasks.push_back(std::move(task));
      }

      for (auto &task : tasks) {
        task.wait();
      }

      ASSERT_EQ(fs_->download_buffers_->used_blocks(),
                fs_->options_.max_total_reserved_buffer_count);

      for (int i = 0; i < total_file_count; i++) {
        fs_->release(nodeids[i], get_file_from_handle(fds[i]));
        fs_->forget(nodeids[i], 1);
      }
    }
  }

  void verify_wait_for_prefetching_with_pin_read() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    srand(time(nullptr));
    uint64_t nodeid = 0;
    int file_size_MB = 255;
    int file_draft = 1 + rand() % 1023;
    auto crc = create_file_in_folder(parent, "testfile", file_size_MB, nodeid,
                                     file_draft);
    ASSERT_TRUE(crc > 0);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused;
    int r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto file = get_file_from_handle(handle);
    char *buf = new char[1048576];
    DEFER(delete[] buf);

    const int64_t io_size = 4096;
    uint64_t offset = 0;
    for (int i = 0; i < 10; i++) {
      ssize_t ret = file->pread(buf, io_size, offset);
      ASSERT_EQ(ret, io_size);
      offset += io_size;
    }

    photon::thread_usleep(20000);

    auto reader = (dynamic_cast<OssFileHandle *>(file))->reader_.get();
    auto cache_file = dynamic_cast<OssCachedReader *>(reader);
    ASSERT_NE(cache_file->running_download_tasks_.load(), 0ULL);

    void *mem;
    ssize_t ret = file->pin(0, io_size, &mem);
    ASSERT_EQ(ret, io_size);

    // close before unpin
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);

    ASSERT_EQ(cache_file->running_download_tasks_.load(), 0ULL);

    file->unpin(0);
  }

  void verify_open_truncate() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    {
      // 1. open non-empty file with O_TRUNC and close
      std::string filename = "testfile";
      uint64_t crc = create_file_in_folder(parent, filename, 2, nodeid, 1);
      auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(std::to_string(crc), meta["X-Oss-Hash-Crc64ecma"]);
      DEFER(fs_->forget(nodeid, 1));

      bool unused;
      int r = fs_->open(nodeid, O_RDWR | O_TRUNC, &handle, &unused);
      ASSERT_EQ(r, 0);
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(meta["Content-Length"], "0");
    }

    {
      // 2. create file and close
      std::string filename = "testfile2";
      int r = fs_->creat(parent, filename.c_str(), CREATE_BASE_FLAGS, 0644, 0,
                         0, 0, &nodeid, &st, &handle);
      ASSERT_EQ(r, 0);
      DEFER(fs_->forget(nodeid, 1));
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      auto meta = get_file_meta(filename, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(meta["Content-Length"], "0");
    }
  }
};

TEST_F(Ossfs2OpenCloseTest, verify_open) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open();
}

TEST_F(Ossfs2OpenCloseTest, verify_filehandle_release) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_filehandle_release();
}

TEST_F(Ossfs2OpenCloseTest, verify_close_to_open) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.close_to_open = true;
  opts.attr_timeout = 3;
  init(opts);
  verify_close_to_open();
}

TEST_F(Ossfs2OpenCloseTest, verify_close_to_open_stale) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.close_to_open = true;
  opts.attr_timeout = 0;
  init(opts);
  verify_close_to_open_stale();
}

TEST_F(Ossfs2OpenCloseTest, verify_max_total_reserved_buffer_count) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.max_total_reserved_buffer_count = 32;
  init(opts);
  verify_max_total_reserved_buffer_count();
}

TEST_F(Ossfs2OpenCloseTest, verify_wait_for_prefetching_with_pin_read) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.prefetch_chunk_size = 1024 * 1024;
  opts.prefetch_concurrency_per_file = 256;
  opts.seq_read_detect_count = 1;
  init(opts);
  verify_wait_for_prefetching_with_pin_read();
}

TEST_F(Ossfs2OpenCloseTest, verify_open_truncate) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_open_truncate();
}

TEST_F(Ossfs2OpenCloseTest, verify_open_truncate_for_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  init(opts);
  verify_open_truncate();
}
