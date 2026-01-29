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

#pragma once

#include <errno.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <linux/fs.h>
#include <photon/common/alog.h>
#include <photon/common/checksum/crc64ecma.h>
#include <photon/common/string_view.h>
#include <photon/photon.h>

#include <fstream>
#include <queue>
#include <random>

#include "common/filesystem.h"
#include "common/fuse.h"
#include "common/test/test_common.h"
#include "common/utils.h"
#include "fs/file.h"
#include "fs/fs.h"
#include "test_util.h"

using namespace OssFileSystem;

#define EXECUTOR_QUEUE_OPTION \
  { 16, 1024 }

DECLARE_string(oss_endpoint);
DECLARE_string(oss_bucket);
DECLARE_string(oss_bucket_prefix);
DECLARE_string(oss_access_key_id);
DECLARE_string(oss_access_key_secret);
DECLARE_uint64(oss_request_timeout_ms);
DECLARE_bool(enable_locking_debug_logs);
DECLARE_bool(write_with_fuse_bufvec);

extern const std::string kOssMetaStorageClass;
extern const std::string kOssSCStandard;
extern const std::string kOssSCIA;
extern const std::string kOssSCArchive;
extern const std::string kOssSCColdArchive;
extern const std::string kOssSCDeepColdArchive;

extern const std::string kUserAgentPrefix;

std::string random_string(int length);

//
// Base test suite class for OssFs filesystem tests.
// Requires ossutils to verify the correctness of the filesystem.
// This class is not thread-safe.
//
// To use this class, you need to set the following gflags options:
//   --oss_access_key_id = <your access key id>
//   --oss_access_key_secret = <your access key secret>
//   --oss_bucket = <your bucket name>
//   --oss_endpoint = <your endpoint>
//
// For ossutils, see more details in
// https://help.aliyun.com/zh/oss/developer-reference/ossutil-overview
//
class Ossfs2TestSuite : public ::testing::Test {
 public:
  void SetUp() override;
  void TearDown() override;

 protected:
  struct TestInode {
    uint64_t nodeid = 0;
    std::string name;
    TestInode(uint64_t nodeid, const std::string &name)
        : nodeid(nodeid), name(name) {}
  };

  class AuditableOssFs final : public OssFs {
   public:
    AuditableOssFs(const OssFsOptions &options, BackgroundVCpuEnv bg_vcpu_env)
        : OssFs(options, bg_vcpu_env) {}

    virtual ~AuditableOssFs() {}

    virtual int lookup(uint64_t parent, std::string_view name, uint64_t *nodeid,
                       struct stat *stbuf) override {
      LOG_DEBUG("LOOKUP. parent: `, name `", parent, name);
      return OssFs::lookup(parent, name, nodeid, stbuf);
    }

    virtual int forget(uint64_t nodeid, uint64_t nlookup) override {
      LOG_DEBUG("FORGET. nodeid: `, nlookup: `", nodeid, nlookup);
      return OssFs::forget(nodeid, nlookup);
    }

    virtual int getattr(uint64_t nodeid, struct stat *stbuf) override {
      LOG_DEBUG("GETATTR. nodeid: `", nodeid);
      return OssFs::getattr(nodeid, stbuf);
    }

    virtual int setattr(uint64_t nodeid, struct stat *stbuf,
                        int to_set) override {
      LOG_DEBUG("SETATTR. nodeid `, to_set `", nodeid, to_set);
      return OssFs::setattr(nodeid, stbuf, to_set);
    }

    virtual int open(uint64_t nodeid, int flags, void **fh,
                     bool *keep_page_cache) override {
      LOG_DEBUG(
          "OPEN. nodeid: `, flags: `, read_only: `, truncate: `, append: `",
          nodeid, flags, (flags & O_ACCMODE) == O_RDONLY, (flags & O_TRUNC) > 0,
          (flags & O_APPEND) > 0);
      return OssFs::open(nodeid, flags, fh, keep_page_cache);
    }

    virtual int release(uint64_t nodeid, void *fp) override {
      LOG_DEBUG("RELEASE. nodeid `", nodeid);
      return OssFs::release(nodeid, fp);
    }

    virtual int readdir(uint64_t nodeid, off_t off, void *dh,
                        int (*filler)(void *ctx, uint64_t nodeid,
                                      const char *name,
                                      const struct stat *stbuf, off_t off),
                        void *filler_ctx, int (*is_interrupted)(void *ctx),
                        bool readdirplus, void *interrupted_ctx) override {
      LOG_DEBUG("READDIR. nodeid: `, off: `", nodeid, off);
      return OssFs::readdir(nodeid, off, dh, filler, filler_ctx, is_interrupted,
                            readdirplus, interrupted_ctx);
    }
  };

  void init(OssFsOptions fs_opts, int max_list_ret = -1,
            std::string bind_ips = "", bool preserve_input_options = false);

  int do_init(OssFsOptions fs_opts, int max_list_ret = -1,
              std::string bind_ips = "", bool preserve_input_options = false);

  void destroy();
  void remount();

  const uint64_t IO_SIZE = 64 * 1024;
  const mode_t CREATE_BASE_FLAGS = O_CREAT | O_RDWR;
  const std::string GTEST_BASE_DIR_PREFIX = "gtest_";

  static int filler(void *ctx, const uint64_t nodeid, const char *name,
                    const struct stat *stbuf, off_t off);
  static std::string get_ip_by_interface(std::string interface);
  // exec shell cmd, this function do not has stderr.
  static std::string exec(const char *cmd);

  AuditableOssFs *fs_ = nullptr;
  BackgroundVCpuEnv bg_vcpu_env_;

  uint64_t root_nodeid_ = 1;
  std::string test_path_ = "/tmp/OssFs2Test/";
  std::string gtest_base_dir = "";
  OssAdapterOptions oss_options_;

  // help functions
 protected:
  std::string nodeid_to_path(uint64_t nodeid);
  int read_dir(uint64_t parent, std::vector<TestInode> &childs);
  int read_dir_without_dots(uint64_t parent, std::vector<TestInode> &childs);

  void create_random_file(const std::string &filepath, int size_MB,
                          int offset = 0);
  void create_zero_file(const std::string &filepath, int size_KB,
                        int offset = 0);
  void clean_test_dir();
  void setup_test_dir();

  // testdir -> gtest_base_dir/testdir
  std::string get_test_osspath(const std::string &subpath);

  uint64_t get_test_dir_parent(bool create = true);

  uint64_t create_file_in_folder(uint64_t parent, const std::string &filename,
                                 uint64_t size_MB, uint64_t &nodeid,
                                 int drift = 0);

  // when create one file from fuse mountpoint, we will recieve fuse_create
  // and fuse_flush
  int create_and_flush(uint64_t parent, const char *name, int flags,
                       mode_t mode, uid_t uid, gid_t gid, mode_t umask,
                       uint64_t *nodeid, struct stat *stbuf, void **fh);

  uint64_t write_file_in_folder(uint64_t parent, const std::string &filename,
                                uint64_t size_MB, uint64_t &nodeid,
                                int drift = 0);

  uint64_t write_dirty_file(uint64_t parent, const std::string &filename,
                            uint64_t size_MB, uint64_t &nodeid, void *&handle,
                            int drift = 0);

  // continuously write upto run_time_in_secs
  uint64_t write_file_intervally(uint64_t nodeid, const std::string &local_file,
                                 size_t total_size, int run_time_in_secs,
                                 int long_delay_cnt = 0);

  inline IFileHandleFuseLL *get_file_from_handle(void *fh) {
    return (IFileHandleFuseLL *)fh;
  }

  ssize_t read_from_handle(void *fh, char *buf, size_t size, off_t offset);

  ssize_t read_file_in_folder(uint64_t parent, const std::string &filename,
                              uint64_t *out_crc64 = nullptr);

  ssize_t write_to_file_handle(void *fh, const char *buf, size_t size,
                               off_t offset);
  ssize_t write_with_fuse_bufvec(void *fh, const char *buf, size_t size,
                                 off_t offset);
  ssize_t write_with_fuse_bufvec_with_err(void *fh, const char *buf,
                                          size_t size, off_t offset,
                                          size_t partial_write_size);

  // functions for ossutil
  std::string lookup_ossutil();
  int upload_file(const std::string &local_file, const std::string &target,
                  const std::string &oss_prefix = "");
  int copy_file(const std::string &src, const std::string &dst,
                const std::string &oss_prefix = "");
  int delete_file(const std::string &target,
                  const std::string &oss_prefix = "");
  int create_dir(const std::string &target, const std::string &oss_prefix = "");
  int delete_dir(const std::string &target, const std::string &oss_prefix = "");
  int stat_file(const std::string &target, const std::string &oss_prefix = "");
  std::map<std::string, std::string> get_file_meta(
      const std::string &target, const std::string &oss_prefix = "");
  int set_file_meta(const std::string &target, const std::string &key,
                    const std::string &value,
                    const std::string &oss_prefix = "");
  // Just list object under target
  // The results does not include the target itself and objects in its subdirs
  // If list is a file, the output is empty
  std::vector<std::string> get_list_objects(const std::string &target,
                                            const std::string &oss_prefix = "",
                                            bool include_self = false);
  // we will use rand prefix when dir_prefix or file_prefix is empty
  int upload_file_tree(int depth, int width, int files,
                       const std::string &local_file,
                       const std::string &dir_prefix,
                       const std::string &file_prefix,
                       const std::string &object_prefix = "");
  int create_oss_symlink(const std::string &object, const std::string &link,
                         const std::string &oss_prefix = "");
  int read_oss_symlink(const std::string &object, std::string &link,
                       const std::string &oss_prefix = "");

  int get_random_max_keys() {
    srand(time(nullptr));
    int randnum = rand();
    int max_keys = (randnum & 1) ? (randnum % 5 + 1) : 100;
    LOG_INFO("max_keys: `", max_keys);
    return max_keys;
  }
};
