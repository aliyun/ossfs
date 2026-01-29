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

class Ossfs2CRC64Test : public Ossfs2TestSuite {
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

  void verify_append_to_object_without_remote_crc64() {
    g_fault_injector->set_injection(FaultInjectionId::FI_OssError_No_Crc64);
    auto parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    for (int i = 0; i < 2; i++) {
      if (i == 1) {
        // test appendable object
        fs_->options_.enable_appendable_object = true;
      }

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

TEST_F(Ossfs2CRC64Test, verify_enable_crc64) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  init(opts);
  verify_enable_crc64();
}

TEST_F(Ossfs2CRC64Test, verify_enable_crc64_with_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.attr_timeout = 1;
  opts.enable_crc64 = true;
  opts.enable_appendable_object = true;
  init(opts);
  verify_enable_crc64();
}

TEST_F(Ossfs2CRC64Test, verify_append_to_object_without_remote_crc64) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_append_to_object_without_remote_crc64();
}
