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

#include "common/shm_handle.h"

namespace {
using namespace common;

std::string get_test_shm_name(const std::string &suffix) {
  return "ossfs2_ut_shm_handle_" + suffix;
}

TEST(ShmHandleTest, OpenAndClose) {
  auto name = get_test_shm_name("open_close");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::open(name, size, O_CREAT | O_RDWR, shm);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(shm, nullptr);
  ASSERT_NE(shm->ptr(), nullptr);
  ASSERT_EQ(shm->size(), size);

  // Write and read back
  memset(shm->ptr(), 0xAB, size);
  EXPECT_EQ(static_cast<uint8_t *>(shm->ptr())[100], 0xAB);

  shm->close();

  // Verify it's still on disk (not unlinked)
  std::unique_ptr<ShmHandle> shm2;
  ret = ShmHandle::open(name, size, O_RDWR, shm2);
  EXPECT_EQ(ret, 0);
  EXPECT_NE(shm2, nullptr);
}

TEST(ShmHandleTest, OpenNonExistingWithoutCreate) {
  auto name = get_test_shm_name("nonexist");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::open(name, size, O_RDWR, shm);  // no O_CREAT
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(shm, nullptr);
}

TEST(ShmHandleTest, OpenWithInvalidSize) {
  auto name = get_test_shm_name("invalid_size");
  ShmHandle::unlink(name);

  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::open(name, 0, O_CREAT | O_RDWR, shm);
  EXPECT_EQ(ret, -1);
  EXPECT_EQ(shm, nullptr);
}

TEST(ShmHandleTest, Unlink) {
  auto name = get_test_shm_name("unlink");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  // Create
  std::unique_ptr<ShmHandle> shm;
  int ret = ShmHandle::open(name, size, O_CREAT | O_RDWR, shm);
  ASSERT_EQ(ret, 0);

  // Unlink while mapped
  ret = ShmHandle::unlink(name);
  EXPECT_EQ(ret, 0);

  // Should still be usable (POSIX behavior)
  memset(shm->ptr(), 0xCD, size);
  EXPECT_EQ(static_cast<uint8_t *>(shm->ptr())[200], 0xCD);

  // After unmapping, name should be gone
  shm.reset();
  std::unique_ptr<ShmHandle> shm2;
  ret = ShmHandle::open(name, size, O_RDWR, shm2);
  EXPECT_EQ(ret, -ENOENT);  // not found
}

TEST(ShmHandleTest, UnlinkNonExisting) {
  auto name = get_test_shm_name("unlink_nonexist");
  ShmHandle::unlink(name);

  int ret = ShmHandle::unlink(name);
  EXPECT_EQ(ret, 0);
}

TEST(ShmHandleTest, MoveSemantics) {
  auto name = get_test_shm_name("move");
  ShmHandle::unlink(name);
  const size_t size = 4096;

  std::unique_ptr<ShmHandle> shm1;
  ASSERT_EQ(ShmHandle::open(name, size, O_CREAT | O_RDWR, shm1), 0);

  // Move to shm2
  auto shm2 = std::move(shm1);
  EXPECT_EQ(shm1, nullptr);
  ASSERT_NE(shm2, nullptr);

  // Use moved object
  memset(shm2->ptr(), 0xEF, size);
  EXPECT_EQ(static_cast<uint8_t *>(shm2->ptr())[300], 0xEF);

  // Destroy shm2 → should unmap
  shm2.reset();

  // Reopen to verify cleanup
  std::unique_ptr<ShmHandle> shm3;
  EXPECT_EQ(ShmHandle::open(name, size, O_RDWR, shm3), 0);
}

TEST(ShmHandleTest, EscapeName) {
  EXPECT_EQ(ShmHandle::escape_shm_name("abc"), "/abc");
  EXPECT_EQ(ShmHandle::escape_shm_name("/abc"), "/.abc");
  EXPECT_EQ(ShmHandle::escape_shm_name(""), "/");
  EXPECT_EQ(ShmHandle::escape_shm_name("/"), "/.");
}

}  // namespace