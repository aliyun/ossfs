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
  EXPECT_EQ(find_mode("enable_xattr"), kModeHdfs);
  EXPECT_EQ(find_mode("hdfs_set_owner_on_create"), kModeHdfs);
  EXPECT_EQ(find_mode("oss_hdfs_client_options"), kModeHdfs);

  EXPECT_EQ(find_mode("enable_crc64"), kModeOss);
  EXPECT_EQ(find_mode("oss_additional_headers"), kModeOss);
  EXPECT_EQ(find_mode("enable_ipv6"), kModeOss);
  EXPECT_EQ(find_mode("temp_dir"), kModeOss);
  EXPECT_EQ(find_mode("disk_data_cache_dir"), kModeOss);
  EXPECT_EQ(find_mode("http_proxy"), kModeOss);
  EXPECT_EQ(find_mode("ram_role"), kModeOss);
  EXPECT_EQ(find_mode("credential_process"), kModeOss);
  EXPECT_EQ(find_mode("upload_buffer_size"), kModeOss);

  EXPECT_EQ(find_mode("default_permissions"), kModeAll);
  EXPECT_EQ(find_mode("attr_timeout"), kModeAll);
  EXPECT_EQ(find_mode("oss_endpoint"), kModeAll);
  EXPECT_EQ(find_mode("inode_cache_eviction_threshold"), kModeAll);
  EXPECT_EQ(find_mode("max_inode_cache_count"), kModeAll);
  EXPECT_EQ(find_mode("skip_check_bucket"), kModeAll);
}

TEST(OptionModes, AdditionalHeaders) {
  EXPECT_EQ(OptionsRegistry::kSensitiveOptions.count("oss_additional_headers"),
            1ULL);
  gflags::FlagSaver flag_saver;
  const std::string value =
      "Host=ZXhhbXBsZS5jb20="
      ";x-myapp_id=dHJhaW5pbmc="
      ";x-oss-copy-source=c291cmNl"
      ";X-Myapp_Id=b3RoZXI="
      ";x-oss-job=YTti"
      ";x-oss-path=QzpcdG1w"
      ";x-myapp-text=5Lit5paHCXZhbHVl";
  EXPECT_FALSE(
      gflags::SetCommandLineOption("oss_additional_headers", value.c_str())
          .empty());
  EXPECT_TRUE(gflags::ReadFlagsFromString(
      "--oss_additional_headers=" + value + "\n", "", false));
  EXPECT_EQ(FLAGS_oss_additional_headers, value);
  for (const auto &input :
       {"bad name=dmFsdWU=", "=dmFsdWU=", "x=copper", "x=YQ", "x=YQBi"}) {
    EXPECT_TRUE(
        gflags::SetCommandLineOption("oss_additional_headers", input).empty());
    EXPECT_EQ(FLAGS_oss_additional_headers, value);
  }
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
      "enable_crc64", "enable_xattr", "attr_timeout"};
  auto is_set = [&](std::string_view name) {
    return explicitly_set.count(name) > 0;
  };

  auto oss_side = OptionsRegistry::get_inapplicable_options(false, is_set);
  EXPECT_TRUE(contains(oss_side, "enable_xattr"));
  EXPECT_FALSE(contains(oss_side, "enable_crc64"));
  EXPECT_FALSE(contains(oss_side, "attr_timeout"));
  EXPECT_FALSE(contains(oss_side, "default_permissions"));

  auto hdfs_side = OptionsRegistry::get_inapplicable_options(true, is_set);
  EXPECT_TRUE(contains(hdfs_side, "enable_crc64"));
  EXPECT_FALSE(contains(hdfs_side, "enable_xattr"));
  EXPECT_FALSE(contains(hdfs_side, "attr_timeout"));
}

TEST(OptionModes, AllExplicitlySetMatchesModeMetadata) {
  auto always_set = [](std::string_view) { return true; };

  auto oss_side = OptionsRegistry::get_inapplicable_options(false, always_set);
  EXPECT_TRUE(contains(oss_side, "enable_xattr"));
  EXPECT_FALSE(contains(oss_side, "enable_crc64"));
  EXPECT_FALSE(contains(oss_side, "attr_timeout"));
  EXPECT_FALSE(contains(oss_side, "default_permissions"));

  auto hdfs_side = OptionsRegistry::get_inapplicable_options(true, always_set);
  EXPECT_TRUE(contains(hdfs_side, "enable_crc64"));
  EXPECT_FALSE(contains(hdfs_side, "enable_xattr"));
  EXPECT_FALSE(contains(hdfs_side, "attr_timeout"));
  EXPECT_FALSE(contains(hdfs_side, "default_permissions"));

  for (const auto &name : hdfs_side) {
    EXPECT_EQ(find_mode(name) & kModeHdfs, 0) << name;
  }
  for (const auto &name : oss_side) {
    EXPECT_EQ(find_mode(name) & kModeOss, 0) << name;
  }
}
