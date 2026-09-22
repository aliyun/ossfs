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

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "options.h"

constexpr uint8_t kModeAll = OptionsRegistry::kModeAll;
constexpr uint8_t kModeHdfs = OptionsRegistry::kModeHdfs;
constexpr uint8_t kModeOss = OptionsRegistry::kModeOss;

namespace {

uint8_t find_mode(std::string_view name) {
  for (const auto &category : OptionsRegistry::get_all_options()) {
    for (const auto &option : category) {
      if (option.name == name) return option.modes;
    }
  }
  ADD_FAILURE() << "option not registered: " << name;
  return 0;
}

bool contains(const std::vector<std::string> &v, std::string_view name) {
  return std::find(v.begin(), v.end(), name) != v.end();
}

}  // namespace

TEST(OptionModes, RegisteredMetadata) {
  EXPECT_EQ(find_mode("default_permissions"), kModeHdfs);
  EXPECT_EQ(find_mode("enable_xattr"), kModeHdfs);
  EXPECT_EQ(find_mode("hdfs_set_owner_on_create"), kModeHdfs);
  EXPECT_EQ(find_mode("oss_hdfs_client_options"), kModeHdfs);

  EXPECT_EQ(find_mode("enable_crc64"), kModeOss);
  EXPECT_EQ(find_mode("enable_ipv6"), kModeOss);
  EXPECT_EQ(find_mode("temp_dir"), kModeOss);
  EXPECT_EQ(find_mode("disk_data_cache_dir"), kModeOss);
  EXPECT_EQ(find_mode("http_proxy"), kModeOss);
  EXPECT_EQ(find_mode("ram_role"), kModeOss);
  EXPECT_EQ(find_mode("credential_process"), kModeOss);
  EXPECT_EQ(find_mode("upload_buffer_size"), kModeOss);

  EXPECT_EQ(find_mode("attr_timeout"), kModeAll);
  EXPECT_EQ(find_mode("oss_endpoint"), kModeAll);
  EXPECT_EQ(find_mode("inode_cache_eviction_threshold"), kModeAll);
  EXPECT_EQ(find_mode("max_inode_cache_count"), kModeAll);
}

TEST(OptionModes, NoResultWhenNothingExplicitlySet) {
  auto never_set = [](std::string_view) { return false; };
  EXPECT_TRUE(
      OptionsRegistry::get_inapplicable_options(false, never_set).empty());
  EXPECT_TRUE(
      OptionsRegistry::get_inapplicable_options(true, never_set).empty());
}

TEST(OptionModes, InapplicableOptionsByPredicate) {
  std::set<std::string, std::less<>> explicitly_set = {
      "enable_crc64", "default_permissions", "attr_timeout"};
  auto is_set = [&](std::string_view name) {
    return explicitly_set.count(name) > 0;
  };

  auto oss_side = OptionsRegistry::get_inapplicable_options(false, is_set);
  EXPECT_TRUE(contains(oss_side, "default_permissions"));
  EXPECT_FALSE(contains(oss_side, "enable_crc64"));
  EXPECT_FALSE(contains(oss_side, "attr_timeout"));

  auto hdfs_side = OptionsRegistry::get_inapplicable_options(true, is_set);
  EXPECT_TRUE(contains(hdfs_side, "enable_crc64"));
  EXPECT_FALSE(contains(hdfs_side, "default_permissions"));
  EXPECT_FALSE(contains(hdfs_side, "attr_timeout"));
}

TEST(OptionModes, AllExplicitlySetMatchesModeMetadata) {
  auto always_set = [](std::string_view) { return true; };

  auto oss_side = OptionsRegistry::get_inapplicable_options(false, always_set);
  EXPECT_TRUE(contains(oss_side, "default_permissions"));
  EXPECT_FALSE(contains(oss_side, "enable_crc64"));
  EXPECT_FALSE(contains(oss_side, "attr_timeout"));

  auto hdfs_side = OptionsRegistry::get_inapplicable_options(true, always_set);
  EXPECT_TRUE(contains(hdfs_side, "enable_crc64"));
  EXPECT_FALSE(contains(hdfs_side, "default_permissions"));
  EXPECT_FALSE(contains(hdfs_side, "attr_timeout"));

  for (const auto &name : hdfs_side) {
    EXPECT_EQ(find_mode(name) & kModeHdfs, 0) << name;
  }
  for (const auto &name : oss_side) {
    EXPECT_EQ(find_mode(name) & kModeOss, 0) << name;
  }
}
