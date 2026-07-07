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

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include "common/shm_handle.h"

namespace {
using namespace common;

std::string get_test_shm_name(const std::string &suffix) {
  return "ossfs2_ut_shm_handle_" + suffix;
}

TEST(ShmHandleTest, open_and_close) {
  auto name = get_test_shm_name("open_close");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::create(name, size, O_RDWR, shm);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(shm, nullptr);
  ASSERT_NE(shm->ptr(), nullptr);
  ASSERT_EQ(shm->size(), size);

  // Write and read back
  memset(shm->ptr(), 0xAB, size);
  EXPECT_EQ(static_cast<uint8_t *>(shm->ptr())[100], 0xAB);

  shm->close();

  // Reopen with open (this should fail since we didn't create it)
  std::unique_ptr<ShmHandle> shm2;
  ret = ShmHandle::open(name, O_RDWR, shm2);
  EXPECT_EQ(ret, 0);
  EXPECT_NE(shm2, nullptr);
}

TEST(ShmHandleTest, open_non_existing_without_create) {
  auto name = get_test_shm_name("nonexist");
  ShmHandle::unlink(name);

  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::open(name, O_RDWR, shm);  // try to open non-existing
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(shm, nullptr);
}

TEST(ShmHandleTest, create_and_open_shm) {
  auto name = get_test_shm_name("create_open");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  // Create shared memory
  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::create(name, size, O_RDWR, shm);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(shm, nullptr);
  ASSERT_NE(shm->ptr(), nullptr);
  ASSERT_EQ(shm->size(), size);

  // Write data
  memset(shm->ptr(), 0xCD, size);
  EXPECT_EQ(static_cast<uint8_t *>(shm->ptr())[200], 0xCD);

  // Close and reopen
  shm.reset();
  int ret2 = ShmHandle::open(name, O_RDWR, shm);
  EXPECT_EQ(ret2, 0);
  EXPECT_NE(shm, nullptr);
  EXPECT_EQ(shm->size(), size);
  EXPECT_EQ(static_cast<uint8_t *>(shm->ptr())[200], 0xCD);
}

TEST(ShmHandleTest, unlink) {
  auto name = get_test_shm_name("unlink");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  // Create
  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::create(name, size, O_RDWR, shm);
  ASSERT_EQ(ret, 0);

  // Unlink while mapped
  ret = ShmHandle::unlink(name);
  EXPECT_EQ(ret, 0);

  // Should still be usable (POSIX behavior)
  memset(shm->ptr(), 0xCD, size);
  EXPECT_EQ(static_cast<uint8_t *>(shm->ptr())[200], 0xCD);

  // After unmapping, if we try to create with same name, it should work as if
  // it doesn't exist
  shm.reset();
  std::unique_ptr<ShmHandle> shm2;
  ret = ShmHandle::open(name, O_RDWR, shm2);
  EXPECT_EQ(ret, -ENOENT);  // not found because it was unlinked
}

TEST(ShmHandleTest, unlink_non_existing) {
  auto name = get_test_shm_name("unlink_nonexist");
  ShmHandle::unlink(name);

  int ret = ShmHandle::unlink(name);
  EXPECT_EQ(ret, 0);
}

TEST(ShmHandleTest, escape_name) {
  EXPECT_EQ(ShmHandle::escape_shm_name("abc"), "/abc");
  EXPECT_EQ(ShmHandle::escape_shm_name("/abc"), "/.abc");
  EXPECT_EQ(ShmHandle::escape_shm_name(""), "/");
  EXPECT_EQ(ShmHandle::escape_shm_name("/"), "/.");
}

}  // namespace