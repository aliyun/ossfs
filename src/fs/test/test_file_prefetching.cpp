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

#include "fs/file.h"
#include "fs/file_prefetching.h"
#include "fs/file_reader.h"
#include "test_suite.h"

using namespace OssFileSystem;

class Ossfs2PrefetchWindowTest : public Ossfs2TestSuite {
 protected:
  void verify_cached_reader_dynamic_max_window() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    // Create a test file
    uint64_t nodeid = 0;
    uint64_t crc =
        create_file_in_folder(parent, "prefetch_test_file", 10, nodeid, 0);
    ASSERT_GT(crc, 0ULL);
    DEFER(fs_->forget(nodeid, 1));

    // Open the file to get a reader
    void *handle = nullptr;
    bool unused = false;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    auto oss_file = dynamic_cast<OssFileHandle *>(get_file_from_handle(handle));
    ASSERT_NE(oss_file, nullptr);
    auto reader = dynamic_cast<OssCachedReader *>(oss_file->reader_.get());
    ASSERT_NE(reader, nullptr);

    const size_t configured_max = 100 * 1024 * 1024;  // 100MB

    // Layer 1: usage < 0.5 (low usage) - should return full configured_max
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.0),
              configured_max);
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.3),
              configured_max);
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.49),
              configured_max);

    // Layer 2: 0.5 <= usage < 0.9 (medium usage) - linear reduction
    // At 0.5 (low threshold): should return 100% of configured_max
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.5),
              configured_max);

    // At 0.7 (middle): should return 75% of configured_max
    size_t expected_75 = static_cast<size_t>(configured_max * 0.75);
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.7), expected_75);

    // At 0.89 (near high threshold): should return ~51.25% of configured_max
    // ratio = 1.0 - (0.89 - 0.5) / (0.9 - 0.5) * (1.0 - 0.5)
    //        = 1.0 - 0.39 / 0.4 * 0.5
    //       	= 1.0 - 0.4875 = 0.5125
    size_t expected_51_25 = static_cast<size_t>(configured_max * 0.5125);
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.89),
              expected_51_25);

    // Layer 3: usage >= 0.9 (high usage) - should return prefetch_chunk_size_ *
    // 4
    size_t prefetch_chunk_size = reader->prefetch_chunk_size_;
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.9),
              prefetch_chunk_size * 4);
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 0.95),
              prefetch_chunk_size * 4);
    EXPECT_EQ(reader->get_dynamic_max_window(configured_max, 1.0),
              prefetch_chunk_size * 4);
  }

  void verify_cached_reader_dynamic_expansion_factor() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    uint64_t crc =
        create_file_in_folder(parent, "expansion_test_file", 10, nodeid, 0);
    ASSERT_GT(crc, 0ULL);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    auto oss_file = dynamic_cast<OssFileHandle *>(get_file_from_handle(handle));
    ASSERT_NE(oss_file, nullptr);
    auto reader = dynamic_cast<OssCachedReader *>(oss_file->reader_.get());
    ASSERT_NE(reader, nullptr);

    // Layer 1: usage < 0.5 (low usage) - should return 2.0 (aggressive)
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.0), 2.0);
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.3), 2.0);
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.49), 2.0);

    // Layer 2: 0.5 <= usage < 0.9 (medium usage) - should return 1.5 (moderate)
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.5), 1.5);
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.7), 1.5);
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.89), 1.5);

    // Layer 3: usage >= 0.9 (high usage) - should return 1.25 (conservative)
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.9), 1.25);
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(0.95), 1.25);
    EXPECT_DOUBLE_EQ(reader->get_dynamic_expansion_factor(1.0), 1.25);
  }

  void verify_prefetch_distance_threshold() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    uint64_t nodeid = 0;
    uint64_t crc =
        create_file_in_folder(parent, "threshold_test_file", 10, nodeid, 0);
    ASSERT_GT(crc, 0ULL);
    DEFER(fs_->forget(nodeid, 1));

    void *handle = nullptr;
    bool unused = false;
    int r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
    ASSERT_EQ(r, 0);
    DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

    auto oss_file = dynamic_cast<OssFileHandle *>(get_file_from_handle(handle));
    ASSERT_NE(oss_file, nullptr);
    auto reader = dynamic_cast<OssCachedReader *>(oss_file->reader_.get());
    ASSERT_NE(reader, nullptr);

    // Set prefetch_window_size to 100 for easy testing
    reader->prefetch_window_size_ = 100;

    // Test case 1: next_prefetch_off <= next_read_off should return false
    reader->next_prefetch_off_ = 100;
    reader->next_read_off_ = 100;
    EXPECT_FALSE(reader->is_prefetch_too_far_ahead());

    reader->next_prefetch_off_ = 50;
    reader->next_read_off_ = 100;
    EXPECT_FALSE(reader->is_prefetch_too_far_ahead());

    // Test case 2: distance < prefetch_window_size should return false
    reader->next_prefetch_off_ = 150;
    reader->next_read_off_ = 100;
    EXPECT_FALSE(reader->is_prefetch_too_far_ahead());

    // Test case 3: distance == prefetch_window_size should return true
    reader->next_prefetch_off_ = 200;
    reader->next_read_off_ = 100;
    EXPECT_TRUE(reader->is_prefetch_too_far_ahead());

    // Test case 4: distance > prefetch_window_size should return true
    reader->next_prefetch_off_ = 250;
    reader->next_read_off_ = 100;
    EXPECT_TRUE(reader->is_prefetch_too_far_ahead());
  }
};

TEST_F(Ossfs2PrefetchWindowTest, verify_cached_reader_dynamic_max_window) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_cached_reader_dynamic_max_window();
}

TEST_F(Ossfs2PrefetchWindowTest,
       verify_cached_reader_dynamic_expansion_factor) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_cached_reader_dynamic_expansion_factor();
}

TEST_F(Ossfs2PrefetchWindowTest, verify_prefetch_distance_threshold) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_prefetch_distance_threshold();
}
