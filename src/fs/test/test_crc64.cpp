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

#include "common/crc64_combine.h"
#include "test_suite.h"

class Ossfs2CRC64Test : public OssOnlyTestSuite {
 protected:
  void verify_crc64_performance() {
    uint64_t crc = 0;
    uint64_t crc_photon = 0;

    auto now_us = []() -> uint64_t {
      return std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
          .count();
    };

    auto random_data = random_string(1024 * 1024);

    int round = 4096;
    uint64_t start = now_us();

    for (int i = 0; i < round; i++) {
      void *buf =
          const_cast<void *>(static_cast<const void *>(random_data.c_str()));
      crc = cal_crc64(crc, buf, random_data.length());
    }

    uint64_t end = now_us();
    LOG_INFO("crc64: `, time: ` us", crc, end - start);

    start = now_us();

    for (int i = 0; i < round; i++) {
      crc_photon =
          crc64ecma(random_data.c_str(), random_data.length(), crc_photon);
    }

    end = now_us();
    LOG_INFO("crc64_photon: `, time: ` us", crc_photon, end - start);

    ASSERT_EQ(crc, crc_photon);
  }

  void verify_enable_crc64() {
    g_fault_injector->set_injection(FaultInjectionId::FI_Modify_Write_Buffer);

    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // small file for put object
    struct stat st;
    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "smallfile", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto file = get_file_from_handle(handle);
    auto ret = write_to_file_handle(handle, "test", 4, 0);
    ASSERT_EQ(ret, 4);

    ret = fs_->release(nodeid, file);
    ASSERT_EQ(ret, -EIO);

    // we only return error when crc64 is mismatch, but broken file already
    // uploaded to oss
    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 4);

    // append write with put object
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    ret = write_to_file_handle(handle, "append", 6, 4);
    ASSERT_EQ(ret, 6);

    ret = fs_->release(nodeid, file);
    ASSERT_EQ(ret, -EIO);

    r = fs_->getattr(nodeid, &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, 10);

    // write big file
    uint64_t nodeid2 = 0;
    r = create_and_flush(parent, "bigfile", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid2, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid2, 1));

    file = get_file_from_handle(handle);
    off_t offset = 0;
    for (int i = 0; i < 128; i++) {
      auto random_str = random_string(1024 * 1024);
      ret = write_to_file_handle(handle, random_str.c_str(), random_str.size(),
                                 offset);
      if (!fs_->options_.enable_appendable_object) {
        ASSERT_TRUE(ret == static_cast<ssize_t>(random_str.size()));
      } else if (ret < 0) {
        break;
      }
      offset += random_str.size();
    }

    ret = fs_->release(nodeid2, file);
    ASSERT_EQ(ret, -EIO);

    photon::thread_usleep(1000000);

    r = fs_->getattr(nodeid2, &st);
    ASSERT_EQ(r, 0);

    r = fs_->unlink(parent, "bigfile");
    ASSERT_EQ(r, 0);

    // append to big file
    g_fault_injector->clear_injection(FaultInjectionId::FI_Modify_Write_Buffer);
    uint64_t nodeid3 = 0;
    r = create_and_flush(parent, "bigfile", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                         &nodeid3, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid3, 1));
    file = get_file_from_handle(handle);
    offset = 0;
    for (int i = 0; i < 128; i++) {
      auto random_str = random_string(1024 * 1024);
      ret = write_to_file_handle(handle, random_str.c_str(), random_str.size(),
                                 offset);
      ASSERT_TRUE(ret == static_cast<ssize_t>(random_str.size()));
      offset += random_str.size();
    }

    // append tail
    ret = file->pwrite("tail", 4, offset);

    ret = fs_->release(nodeid3, file);
    ASSERT_EQ(ret, 0);

    g_fault_injector->set_injection(FaultInjectionId::FI_Modify_Write_Buffer);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Modify_Write_Buffer));

    r = fs_->open(nodeid3, O_RDWR | O_APPEND, &handle, &unused);
    ASSERT_EQ(r, 0);

    file = get_file_from_handle(handle);
    offset = 128ULL * 1024 * 1024 + 4;
    for (int i = 0; i < 128; i++) {
      auto random_str = random_string(1024 * 1024);
      ret = write_to_file_handle(handle, random_str.c_str(), random_str.size(),
                                 offset);
      if (!fs_->options_.enable_appendable_object) {
        ASSERT_TRUE(ret == static_cast<ssize_t>(random_str.size()));
      } else if (ret < 0) {
        break;
      }
      offset += random_str.size();
    }

    ret = fs_->release(nodeid3, file);
    ASSERT_EQ(ret, -EIO);

    if (!fs_->options_.enable_appendable_object) {
      photon::thread_usleep(1000000);
      r = fs_->getattr(nodeid3, &st);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(st.st_size, 256LL * 1024 * 1024 + 4);
    }
  }

  void verify_crc64_combine() {
    auto crc_of = [](const std::string &d) {
      return crc64ecma(d.data(), d.size(), 0);
    };

    // 2-part combine
    auto a = random_string(1024);
    auto b = random_string(2048);
    ASSERT_EQ(crc_of(a + b), crc64ecma_combine(crc_of(a), crc_of(b), b.size()));

    // 3-part combine (simulates multipart upload)
    auto p1 = random_string(1024 * 1024);
    auto p2 = random_string(1024 * 1024);
    auto p3 = random_string(512 * 1024);
    uint64_t whole = crc_of(p1 + p2 + p3);
    uint64_t c = crc64ecma_combine(crc_of(p1), crc_of(p2), p2.size());
    c = crc64ecma_combine(c, crc_of(p3), p3.size());
    ASSERT_EQ(whole, c);

    // edge: 1-byte parts
    auto x = random_string(1);
    auto y = random_string(1);
    ASSERT_EQ(crc_of(x + y), crc64ecma_combine(crc_of(x), crc_of(y), y.size()));

    // edge: len2 = 0 returns crc1
    ASSERT_EQ(crc_of(a), crc64ecma_combine(crc_of(a), 0, 0));

    // unequal sizes (last part smaller, like real multipart)
    auto big = random_string(3 * 1024 * 1024);
    auto tail = random_string(777);
    ASSERT_EQ(crc_of(big + tail),
              crc64ecma_combine(crc_of(big), crc_of(tail), tail.size()));
  }

  void verify_crc64_combine_extended() {
    auto crc_of = [](const std::string &d) {
      return crc64ecma(d.data(), d.size(), 0);
    };

    // empty first part: CRC("" || B) == CRC(B)
    auto b = random_string(4096);
    ASSERT_EQ(crc_of(b), crc64ecma_combine(0, crc_of(b), b.size()));

    // 10-part chain (simulates large multipart upload)
    std::vector<std::string> parts;
    std::string whole;
    for (int i = 0; i < 10; i++) {
      parts.push_back(random_string(512 * 1024 + i * 137));
      whole += parts[i];
    }
    uint64_t expected = crc_of(whole);
    uint64_t c = crc_of(parts[0]);
    for (int i = 1; i < 10; i++) {
      c = crc64ecma_combine(c, crc_of(parts[i]), parts[i].size());
    }
    ASSERT_EQ(expected, c);

    // cross-grouping: combine [0..4] and [5..9] independently, then merge
    uint64_t g1 = crc_of(parts[0]);
    for (int i = 1; i < 5; i++) {
      g1 = crc64ecma_combine(g1, crc_of(parts[i]), parts[i].size());
    }
    uint64_t g2 = crc_of(parts[5]);
    for (int i = 6; i < 10; i++) {
      g2 = crc64ecma_combine(g2, crc_of(parts[i]), parts[i].size());
    }
    size_t g2_len = 0;
    for (int i = 5; i < 10; i++) g2_len += parts[i].size();
    ASSERT_EQ(expected, crc64ecma_combine(g1, g2, g2_len));
  }

  void verify_random_write_crc64_put() {
    g_fault_injector->set_injection(FaultInjectionId::FI_Modify_Staging_Data);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Modify_Staging_Data));

    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "rw_crc_put", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    auto data = random_string(128 * 1024);
    auto w = write_to_file_handle(handle, data.c_str(), data.size(), 0);
    ASSERT_EQ(w, static_cast<ssize_t>(data.size()));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, -EIO);
  }

  void verify_random_write_crc64_multipart() {
    g_fault_injector->set_injection(FaultInjectionId::FI_Modify_Staging_Data);
    DEFER(g_fault_injector->clear_injection(
        FaultInjectionId::FI_Modify_Staging_Data));

    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "rw_crc_mp", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t kSize = 3 * 1024 * 1024;
    auto data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.c_str(), data.size(), 0);
    ASSERT_EQ(w, static_cast<ssize_t>(data.size()));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, -EIO);
  }

  void verify_random_write_crc64_multipart_copy_part() {
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    void *handle = nullptr;
    struct stat st;
    int r = create_and_flush(parent, "rw_crc_copy", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));

    const size_t kSize = 3 * 1024 * 1024;
    auto data = random_string(kSize);
    auto w = write_to_file_handle(handle, data.c_str(), data.size(), 0);
    ASSERT_EQ(w, static_cast<ssize_t>(data.size()));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);

    // Re-open and modify only the first 4KB; most parts use upload_part_copy.
    bool unused = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &unused);
    ASSERT_EQ(r, 0);

    auto patch = random_string(4096);
    w = write_to_file_handle(handle, patch.c_str(), patch.size(), 0);
    ASSERT_EQ(w, static_cast<ssize_t>(patch.size()));

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  void verify_append_to_object_without_remote_crc64() {
    for (int i = 0; i < 2; i++) {
      // Second iteration exercises the appendable-object write path.
      OssFsOptions opts;
      opts.enable_appendable_object = (i == 1);
      destroy();
      init(opts);

      g_fault_injector->set_injection(FaultInjectionId::FI_OssError_No_Crc64);
      auto parent = get_test_dir_parent();
      DEFER(fs_->forget(parent, 1));

      uint64_t nodeid = 0;
      uint64_t crc64 = create_file_in_folder(
          parent, "testfile_" + std::to_string(i), 1, nodeid);
      ASSERT_TRUE(crc64 > 0);
      DEFER(fs_->forget(nodeid, 1));

      void *handle = nullptr;
      bool unused = false;
      int r = fs_->open(nodeid, O_RDWR, &handle, &unused);
      ASSERT_EQ(r, 0);

      const uint64_t max_io_size = 1048576;
      uint64_t offset = 1048576;
      uint64_t target_size = 64 * 1048576 + rand() % 4096;
      while (offset < target_size) {
        auto data = random_string(max_io_size);
        uint64_t write_size = std::min(max_io_size, target_size - offset);
        ssize_t r =
            write_to_file_handle(handle, data.c_str(), write_size, offset);
        EXPECT_EQ(r, static_cast<ssize_t>(write_size));
        offset += r;
      }

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);

      r = fs_->open(nodeid, O_RDWR, &handle, &unused);
      ASSERT_EQ(r, 0);

      offset = target_size;
      target_size = 117 * 1048576 + rand() % 4096;
      while (offset < target_size) {
        auto data = random_string(max_io_size);
        uint64_t write_size = std::min(max_io_size, target_size - offset);
        int r = write_to_file_handle(handle, data.c_str(), write_size, offset);
        EXPECT_EQ(r, static_cast<ssize_t>(write_size));
        offset += r;
      }

      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }
  }
};

TEST_F(Ossfs2CRC64Test, verify_crc64_performance) {
  verify_crc64_performance();
}

TEST_F(Ossfs2CRC64Test, verify_crc64_combine) {
  verify_crc64_combine();
}

TEST_F(Ossfs2CRC64Test, verify_crc64_combine_extended) {
  verify_crc64_combine_extended();
}

TEST_F(Ossfs2CRC64Test, verify_enable_crc64) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_enable_crc64();
}

TEST_F(Ossfs2CRC64Test, verify_enable_crc64_with_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  opts.enable_appendable_object = true;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_enable_crc64();
}

TEST_F(Ossfs2CRC64Test, verify_append_to_object_without_remote_crc64) {
  INIT_PHOTON();
  OssFsOptions opts;
  SET_TEST_MODE(kTestOss | kTestHdfs);
  init(opts);
  verify_append_to_object_without_remote_crc64();
}

TEST_F(Ossfs2CRC64Test, verify_random_write_crc64_put) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  opts.temp_dir = test_path_;
  SET_TEST_MODE(kTestOss);
  init(opts);
  verify_random_write_crc64_put();
}

TEST_F(Ossfs2CRC64Test, verify_random_write_crc64_multipart) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  SET_TEST_MODE(kTestOss);
  init(opts);
  verify_random_write_crc64_multipart();
}

TEST_F(Ossfs2CRC64Test, verify_random_write_crc64_multipart_copy_part) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  opts.temp_dir = test_path_;
  opts.upload_buffer_size = 1 * 1024 * 1024;
  SET_TEST_MODE(kTestOss);
  init(opts);
  verify_random_write_crc64_multipart_copy_part();
}
