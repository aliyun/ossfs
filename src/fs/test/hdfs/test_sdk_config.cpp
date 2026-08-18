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

#include <filesystem>
#include <fstream>

#include "fs/test/test_suite.h"
#include "oss/obj_store.h"

class Ossfs2HdfsSdkConfigTest : public OssHdfsTestSuite {};

TEST_F(Ossfs2HdfsSdkConfigTest, verify_append_no_overwrite_default) {
  ObjStoreOptions opts;
  opts.hdfs_client_options = "key1=val1";
  // overwrite=false (default): existing key should NOT be overwritten.
  opts.append_hdfs_client_option("key1", "val2");
  ASSERT_EQ(opts.hdfs_client_options, "key1=val1");
}

TEST_F(Ossfs2HdfsSdkConfigTest, verify_append_overwrite) {
  ObjStoreOptions opts;
  opts.hdfs_client_options = "key1=val1";
  // overwrite=true: existing key should be replaced.
  opts.append_hdfs_client_option("key1", "val2", true);
  ASSERT_EQ(opts.hdfs_client_options, "key1=val1,key1=val2");
}

TEST_F(Ossfs2HdfsSdkConfigTest, verify_append_multiple_options) {
  ObjStoreOptions opts;
  opts.append_hdfs_client_option("key1", "val1");
  opts.append_hdfs_client_option("key2", "val2");
  opts.append_hdfs_client_option("key3", "val3");
  ASSERT_EQ(opts.hdfs_client_options, "key1=val1,key2=val2,key3=val3");
}

TEST_F(Ossfs2HdfsSdkConfigTest, verify_append_empty_base) {
  ObjStoreOptions opts;
  // Appending to empty hdfs_client_options should not produce a leading comma.
  opts.append_hdfs_client_option("key1", "val1");
  ASSERT_EQ(opts.hdfs_client_options, "key1=val1");
}

TEST_F(Ossfs2HdfsSdkConfigTest,
       verify_append_no_overwrite_preserves_other_keys) {
  ObjStoreOptions opts;
  opts.hdfs_client_options = "key1=val1,key2=val2";
  // Adding a new key should work even when overwrite=false.
  opts.append_hdfs_client_option("key3", "val3");
  ASSERT_EQ(opts.hdfs_client_options, "key1=val1,key2=val2,key3=val3");
}

// Helper: write a string to a temp file and return its path.
static std::string write_temp_config(const std::string &content,
                                     const std::string &suffix = ".ini") {
  auto path = std::filesystem::temp_directory_path() /
              ("ossfs2_sdk_test_" + std::to_string(getpid()) + suffix);
  std::ofstream f(path);
  f << content;
  f.close();
  return path.string();
}

// [jindosdk] section options are loaded from config file.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_sdk_config_file_jindosdk_section) {
  auto path = write_temp_config(
      "[jindosdk]\n"
      "logger.appender=console\n"
      "fs.oss.retry.count=1\n");
  DEFER(std::filesystem::remove(path));

  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "sdk.config.file=" + path;
  init(opts);

  // Verify filesystem works (options were applied without crash).
  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "config_test", CREATE_BASE_FLAGS, 0777, 0, 0,
                           0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Only [jindosdk] section is applied; other sections are ignored.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_sdk_config_file_section_filter) {
  auto path = write_temp_config(
      "[other]\n"
      "fs.oss.retry.count=99\n"
      "[jindosdk]\n"
      "logger.appender=console\n"
      "[another]\n"
      "fs.oss.timeout.millisecond=1\n");
  DEFER(std::filesystem::remove(path));

  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "sdk.config.file=" + path;
  init(opts);

  // Filesystem should work: non-jindosdk sections were ignored.
  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "section_filter", CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Comments (# and !) and blank lines are skipped.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_sdk_config_file_comment_and_blank) {
  auto path = write_temp_config(
      "[jindosdk]\n"
      "# This is a comment\n"
      "! This is also a comment\n"
      "\n"
      "logger.appender=console\n"
      "   \n"  // blank line with whitespace
      "fs.oss.retry.count=3\n");
  DEFER(std::filesystem::remove(path));

  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "sdk.config.file=" + path;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "comment_test", CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Key/val whitespace is trimmed.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_sdk_config_file_whitespace_handling) {
  auto path = write_temp_config(
      "[jindosdk]\n"
      "  logger.appender  =  console  \n");
  DEFER(std::filesystem::remove(path));

  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "sdk.config.file=" + path;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "ws_test", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                           &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Invalid line (no '=') is skipped with WARN.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_sdk_config_file_invalid_line) {
  auto path = write_temp_config(
      "[jindosdk]\n"
      "this_is_invalid\n"
      "logger.appender=console\n");
  DEFER(std::filesystem::remove(path));

  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "sdk.config.file=" + path;
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "invalid_line", CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Missing config file: WARN logged, no crash.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_sdk_config_file_missing) {
  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ =
      "sdk.config.file=/nonexistent/path/config.ini,logger.appender=console";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "missing_cfg", CREATE_BASE_FLAGS, 0777, 0, 0,
                           0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Reserved keys (fs.oss.endpoint, fs.oss.accessKeyId, fs.oss.accessKeySecret)
// are rejected from user options.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_reserved_keys_rejected) {
  INIT_PHOTON();
  OssFsOptions opts;
  // Try to set reserved keys; should be rejected with WARN but not crash.
  hdfs_client_options_ =
      "fs.oss.endpoint=evil.endpoint,logger.appender=console";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "reserved_keys", CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Inline options override config file values for the same key.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_inline_overrides_config_file) {
  auto path = write_temp_config(
      "[jindosdk]\n"
      "logger.appender=file\n");
  DEFER(std::filesystem::remove(path));

  INIT_PHOTON();
  OssFsOptions opts;
  // Config file sets file, inline sets console. Inline should win.
  hdfs_client_options_ = "sdk.config.file=" + path + ",logger.appender=console";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "override_test", CREATE_BASE_FLAGS, 0777, 0,
                           0, 0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Multiple comma-separated inline options.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_apply_sdk_options_comma_separated) {
  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "logger.appender=console,fs.oss.retry.count=1";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "comma_test", CREATE_BASE_FLAGS, 0777, 0, 0,
                           0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));
  r = fs_->release(nodeid, get_file_from_handle(handle));
  ASSERT_EQ(r, 0);
}

// Verify CRC64 checksum can be disabled.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_option_checksum_crc64_disable) {
  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "fs.oss.checksum.crc64.enable=false";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "crc64_off", CREATE_BASE_FLAGS, 0777, 0, 0,
                           0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));

  auto file = get_file_from_handle(handle);
  const char *data = "crc64_disabled_test";
  ssize_t w = file->pwrite(data, strlen(data), 0);
  ASSERT_EQ(w, (ssize_t)strlen(data));
  r = fs_->fsync(nodeid, handle, false);
  ASSERT_EQ(r, 0);

  // Reopen and verify content.
  r = fs_->release(nodeid, file);
  ASSERT_EQ(r, 0);
  void *read_handle = nullptr;
  bool keep_cache = false;
  r = fs_->open(nodeid, O_RDONLY, &read_handle, &keep_cache);
  ASSERT_EQ(r, 0);
  char buf[64] = {};
  ssize_t n = get_file_from_handle(read_handle)->pread(buf, strlen(data), 0);
  ASSERT_EQ(n, (ssize_t)strlen(data));
  ASSERT_EQ(memcmp(buf, data, strlen(data)), 0);
  r = fs_->release(nodeid, get_file_from_handle(read_handle));
  ASSERT_EQ(r, 0);
}

// Verify MD5 checksum can be enabled.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_option_checksum_md5_enable) {
  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "fs.oss.checksum.md5.enable=true";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "md5_on", CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                           &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));

  auto file = get_file_from_handle(handle);
  const char *data = "md5_enabled_test";
  ssize_t w = file->pwrite(data, strlen(data), 0);
  ASSERT_EQ(w, (ssize_t)strlen(data));
  r = fs_->fsync(nodeid, handle, false);
  ASSERT_EQ(r, 0);
  r = fs_->release(nodeid, file);
  ASSERT_EQ(r, 0);
}

// Verify small write buffer size works correctly.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_option_write_buffer_size) {
  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "fs.oss.write.buffer.size=65536";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "small_buf", CREATE_BASE_FLAGS, 0777, 0, 0,
                           0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));

  auto file = get_file_from_handle(handle);
  // Write 1MB with 64KB buffer -> multiple chunks.
  const size_t write_size = 1024 * 1024;
  char *buf = new char[write_size];
  DEFER(delete[] buf);
  for (size_t i = 0; i < write_size; i++) buf[i] = i & 0xFF;
  ssize_t w = file->pwrite(buf, write_size, 0);
  ASSERT_EQ(w, (ssize_t)write_size);
  r = fs_->fsync(nodeid, handle, false);
  ASSERT_EQ(r, 0);
  r = fs_->release(nodeid, file);
  ASSERT_EQ(r, 0);
}

// Verify small memory pool works.
TEST_F(Ossfs2HdfsSdkConfigTest, verify_option_memory_buffer_limit) {
  INIT_PHOTON();
  OssFsOptions opts;
  hdfs_client_options_ = "fs.oss.memory.buffer.size.max.mb=64";
  init(opts);

  uint64_t parent = get_test_dir_parent();
  DEFER(fs_->forget(parent, 1));
  struct stat st;
  uint64_t nodeid = 0;
  void *handle = nullptr;
  int r = create_and_flush(parent, "mem_limit", CREATE_BASE_FLAGS, 0777, 0, 0,
                           0, &nodeid, &st, &handle);
  ASSERT_EQ(r, 0);
  DEFER(fs_->forget(nodeid, 1));

  auto file = get_file_from_handle(handle);
  const char *data = "memory_limit_test";
  ssize_t w = file->pwrite(data, strlen(data), 0);
  ASSERT_EQ(w, (ssize_t)strlen(data));
  r = fs_->fsync(nodeid, handle, false);
  ASSERT_EQ(r, 0);
  r = fs_->release(nodeid, file);
  ASSERT_EQ(r, 0);
}
