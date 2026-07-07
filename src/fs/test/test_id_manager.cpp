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

#include <atomic>

#include "common/shm_handle.h"
#include "common/utils.h"
#include "fs/id_manager.h"

namespace {
using OssFileSystem::IIdManager;

class IdManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(IdManagerTest, create_heap_id_manager) {
  // two times
  for (int i = 0; i < 2; ++i) {
    auto id_mgr = OssFileSystem::create_heap_id_manager();
    ASSERT_NE(nullptr, id_mgr);
    EXPECT_EQ(id_mgr->get_start_id(), 2ULL);
    EXPECT_EQ(id_mgr->next_id(), 2ULL);
    EXPECT_EQ(id_mgr->next_id(), 3ULL);
  }
}

}  // namespace