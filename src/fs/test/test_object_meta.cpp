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

static const std::vector<std::string_view> mime_suffixes = {
    "3gp",  "3gpp", "7z",   "ai",    "apk",   "asf", "asx",  "atom",    "avi",
    "bin",  "bmp",  "cco",  "crt",   "css",   "deb", "der",  "dll",     "dmg",
    "doc ", "ear",  "eot",  "eps",   "exe",   "flv", "gif",  "hqx",     "htc",
    "htm",  "html", "ico",  "img",   "iso",   "jad", "jar",  "jardiff", "jng",
    "jnlp", "jpeg", "jpg",  "js",    "kar",   "kml", "kmz",  "m3u8",    "m4a",
    "m4v",  "mid",  "midi", "mml",   "mng",   "mov", "mp3",  "mp4",     "mpeg",
    "mpg",  "msi",  "msm",  "msp",   "ogg",   "pdb", "pdf",  "pem",     "pl",
    "pm",   "png",  "ppt",  "prc",   "ps",    "ra",  "rar",  "rpm",     "rss",
    "rtf",  "run",  "sea",  "shtml", "sit",   "svg", "svgz", "swf",     "tcl",
    "tif",  "tiff", "tk",   "ts",    "txt",   "war", "wbmp", "webm",    "webp",
    "wgz",  "wml",  "wmlc", "wmv",   "xhtml", "xls", "xml",  "xpi",     "zip",
};

class Ossfs2ObjectMetaTest : public Ossfs2TestSuite {
 protected:
  void verify_mime_type() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    auto create_and_check = [&](const std::string &name,
                                const std::string &type) {
      uint64_t nodeid = 0;
      auto crc64 = create_file_in_folder(parent, name, 1, nodeid);
      DEFER(fs_->forget(nodeid, 1));

      auto file_meta = get_file_meta(name, FLAGS_oss_bucket_prefix);
      ASSERT_EQ(type, file_meta["Content-Type"]);
      ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
    };

    // 1. empty suffix
    {
      std::string name = "testfile_" + random_string(5);
      create_and_check(name, "application/octet-stream");
      create_and_check("abc.", "application/octet-stream");
      create_and_check("a.b.c.", "application/octet-stream");
      create_and_check("a.b.c..", "application/octet-stream");
    }

    // 2. invalid suffix
    {
      std::string name = "testfile_" + random_string(5) + ".txt2";
      create_and_check(name, "application/octet-stream");
    }

    // 3. all suffix and random upper case
    std::vector<std::future<void>> tasks;
    for (auto suf : mime_suffixes) {
      auto task = std::async(std::launch::async, [&, suf]() {
        INIT_PHOTON();
        std::string suffix = std::string(suf);
        if (rand() % 2) {
          std::transform(suffix.begin(), suffix.end(), suffix.begin(),
                         ::toupper);
        }
        std::string name = "testfile_" + random_string(5) + "." + suffix;
        create_and_check(name, std::string(lookup_mime_type(suf)));
      });
      tasks.push_back(std::move(task));
    }

    // 4. check rename and multipart
    for (auto suf : mime_suffixes) {
      auto task = std::async(std::launch::async, [&, suf]() {
        INIT_PHOTON();
        std::string name = "testfile_" + random_string(5);
        uint64_t nodeid = 0;
        auto crc64 = create_file_in_folder(parent, name, 5, nodeid);
        DEFER(fs_->forget(nodeid, 1));

        auto file_meta = get_file_meta(name, FLAGS_oss_bucket_prefix);
        ASSERT_EQ("application/octet-stream", file_meta["Content-Type"]);
        ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);

        std::string new_suf = std::string(suf);
        if (rand() % 2) {
          std::transform(new_suf.begin(), new_suf.end(), new_suf.begin(),
                         ::toupper);
        }
        std::string new_name = "testfile_" + random_string(5) + "." + new_suf;
        int r = fs_->rename(parent, name.c_str(), parent, new_name.c_str(), 0);
        ASSERT_EQ(r, 0);

        file_meta = get_file_meta(new_name, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(lookup_mime_type(suf), file_meta["Content-Type"]);
        ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
      });
      tasks.push_back(std::move(task));
    }

    // 5. rename and copy meta with same suffix
    for (auto suf : mime_suffixes) {
      auto task = std::async(std::launch::async, [&, suf]() {
        INIT_PHOTON();
        std::string name = "testfile." + std::string(suf);
        uint64_t nodeid = 0;
        auto crc64 = create_file_in_folder(parent, name, 1, nodeid);
        DEFER(fs_->forget(nodeid, 1));

        auto meta_key = "X-Oss-Meta-Test";
        auto meta_value = random_string(5);
        int r =
            set_file_meta(name, meta_key, meta_value, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);

        auto file_meta = get_file_meta(name, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(lookup_mime_type(suf), file_meta["Content-Type"]);
        ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
        ASSERT_EQ(meta_value, file_meta[meta_key]);

        std::string new_name =
            "testfile_" + random_string(5) + "." + std::string(suf);
        r = fs_->rename(parent, name.c_str(), parent, new_name.c_str(), 0);
        ASSERT_EQ(r, 0);

        file_meta = get_file_meta(new_name, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(lookup_mime_type(suf), file_meta["Content-Type"]);
        ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
        ASSERT_EQ(meta_value, file_meta[meta_key]);
      });
      tasks.push_back(std::move(task));
    }

    for (auto &task : tasks) task.wait();
  }

  void verify_meta_copy_without_set_mime() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));

    std::vector<std::future<void>> tasks;
    for (auto suf : mime_suffixes) {
      auto task = std::async(std::launch::async, [&, suf]() {
        INIT_PHOTON();
        std::string name = "testfile_" + random_string(5);
        uint64_t nodeid = 0;
        auto crc64 = create_file_in_folder(parent, name, 5, nodeid);
        DEFER(fs_->forget(nodeid, 1));

        auto meta_key = "X-Oss-Meta-Test";
        auto meta_value = random_string(5);
        int r =
            set_file_meta(name, meta_key, meta_value, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);

        auto file_meta = get_file_meta(name, FLAGS_oss_bucket_prefix);
        ASSERT_EQ("application/octet-stream", file_meta["Content-Type"]);
        ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
        ASSERT_EQ(meta_value, file_meta[meta_key]);

        std::string new_suf = std::string(suf);
        if (rand() % 2) {
          std::transform(new_suf.begin(), new_suf.end(), new_suf.begin(),
                         ::toupper);
        }
        std::string new_name = "testfile_" + random_string(5) + "." + new_suf;
        r = fs_->rename(parent, name.c_str(), parent, new_name.c_str(), 0);
        ASSERT_EQ(r, 0);

        file_meta = get_file_meta(new_name, FLAGS_oss_bucket_prefix);
        ASSERT_EQ("application/octet-stream", file_meta["Content-Type"]);
        ASSERT_EQ(std::to_string(crc64), file_meta["X-Oss-Hash-Crc64ecma"]);
        ASSERT_EQ(meta_value, file_meta[meta_key]);
      });
      tasks.push_back(std::move(task));
    }
    for (auto &task : tasks) task.wait();
  }
};

TEST_F(Ossfs2ObjectMetaTest, verify_mime_type) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.upload_buffer_size = 1048576;
  opts.set_mime_for_rename_dst = true;
  init(opts);
  verify_mime_type();
}

TEST_F(Ossfs2ObjectMetaTest, verify_mime_type_with_appendable_object) {
  INIT_PHOTON();
  OssFsOptions opts;
  opts.enable_appendable_object = true;
  opts.upload_buffer_size = 1048576;
  opts.set_mime_for_rename_dst = true;
  init(opts);
  verify_mime_type();
}

TEST_F(Ossfs2ObjectMetaTest, verify_meta_copy_without_set_mime) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_meta_copy_without_set_mime();
}