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

#include <signal.h>

#include <map>
#include <string>
#include <vector>

// for oss client
DEFINE_string(oss_endpoint, "", "");
DEFINE_string(oss_bucket, "", "");
DEFINE_string(oss_bucket_prefix, "", "");
DEFINE_string(oss_access_key_id, "", "");
DEFINE_string(oss_access_key_secret, "", "");
DEFINE_uint64(oss_request_timeout_ms, 32000, "oss request timeout");

DEFINE_bool(write_with_fuse_bufvec, true, "");

const std::string kOssMetaStorageClass = "X-Oss-Storage-Class";
const std::string kOssSCStandard = "Standard";
const std::string kOssSCIA = "IA";
const std::string kOssSCArchive = "Archive";
const std::string kOssSCColdArchive = "ColdArchive";
const std::string kOssSCDeepColdArchive = "DeepColdArchive";

const std::string kUserAgentPrefix = "InvalidRamrole";

std::string random_string(int length) {
  static std::string charset =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
  std::string result;
  result.resize(length);

  for (int i = 0; i < length; i++)
    result[i] = charset[rand() % charset.length()];

  return result;
}

static int block_all_signal(sigset_t *oldset) {
  sigset_t sigset;
  int r = sigfillset(&sigset);
  if (r != 0) return r;
  r = sigprocmask(SIG_SETMASK, &sigset, oldset);
  return r;
}

int Ossfs2TestSuite::filler(void *ctx, const uint64_t nodeid, const char *name,
                            const struct stat *stbuf, off_t off) {
  std::vector<TestInode> *nodes =
      reinterpret_cast<std::vector<TestInode> *>(ctx);
  nodes->emplace_back(nodeid, name);
  // we get all the dir entries one time
  return 0;
}

void Ossfs2TestSuite::SetUp() {
  g_fault_injector.reset(new FaultInjector);

  // Code here will be called immediately after the constructor (right
  // before each test).
  LOG_INFO("Test setup.");
  clean_test_dir();
  setup_test_dir();

#if defined(__SANITIZE_ADDRESS__)
  LOG_INFO("AddressSanitizer is enabled.");
#endif

  auto case_name =
      ::testing::UnitTest::GetInstance()->current_test_info()->name();
  gtest_base_dir = GTEST_BASE_DIR_PREFIX + case_name;
  LOG_INFO("Test base dir: `", gtest_base_dir);

  srand(time(nullptr));
  FLAGS_write_with_fuse_bufvec = rand() % 2;
  LOG_INFO("write_with_fuse_bufvec: `", FLAGS_write_with_fuse_bufvec);

  delete_dir("", FLAGS_oss_bucket_prefix);
  int r = create_dir("", FLAGS_oss_bucket_prefix);
  ASSERT_EQ(r, 0);
}

void Ossfs2TestSuite::TearDown() {
  if (fs_ == nullptr) return;

  // Code here will be called immediately after each test (right
  // before the destructor).
  auto remain_inode_count = fs_->global_inodes_map_.size();
  if (remain_inode_count != 1) {
    for (auto it : fs_->global_inodes_map_) {
      LOG_ERROR("Inode ` lookup cnt ` ref cnt `", it.first,
                it.second->lookup_cnt, it.second->ref_ctr);
    }
  }

  EXPECT_EQ(remain_inode_count, (size_t)1);

  if (fs_->enable_staged_cache()) {
    uint64_t expected_remained_staged_nodes =
        fs_->get_inode_eviction_threshold() - 1;
    if (fs_->staged_inodes_cache_->size() > expected_remained_staged_nodes) {
      LOG_ERROR("Staged cache size `, greater than expected: `",
                fs_->staged_inodes_cache_->size(),
                expected_remained_staged_nodes);
    }

    EXPECT_LE(fs_->staged_inodes_cache_->size(),
              (size_t)(expected_remained_staged_nodes));
  }

  EXPECT_EQ(fs_->active_file_handles_, (size_t)0);
  auto dirty_nodeids = fs_->get_dirty_nodeids().size();
  destroy();

  g_fault_injector->clear_all_injections();

  ASSERT_EQ(dirty_nodeids, (size_t)0);
}

void Ossfs2TestSuite::init(OssFsOptions fs_opts, int max_list_ret,
                           std::string bind_ips, bool preserve_input_options) {
  ASSERT_EQ(fs_opts.cache_type, CacheType::kFhCache);
  ASSERT_EQ(do_init(fs_opts, max_list_ret, bind_ips, preserve_input_options),
            0);
}

int Ossfs2TestSuite::do_init(OssFsOptions fs_opts, int max_list_ret,
                             std::string bind_ips,
                             bool preserve_input_options) {
  std::string endpoint = FLAGS_oss_endpoint;
  std::string bucket = FLAGS_oss_bucket;
  std::string prefix = FLAGS_oss_bucket_prefix;
  std::string accessKeyId = FLAGS_oss_access_key_id;
  std::string accessKeySecret = FLAGS_oss_access_key_secret;
  auto timeout_ms = FLAGS_oss_request_timeout_ms;

  if (!fs_opts.ram_role.empty() || !fs_opts.credential_process.empty()) {
    // ignore ak and sk when ram role or process is sepcified
    accessKeyId.clear();
    accessKeySecret.clear();
  }

  bool use_list_obj_v2 = rand() % 2;
  bg_vcpu_env_.bg_oss_client_env = new OssFileSystem::BGVCpuOssClientEnv;
  int vcpu_num = rand() % 4 + 1;
  LOG_INFO("create ` background vcpu oss client", vcpu_num);

  for (int i = 0; i < vcpu_num; i++) {
    sigset_t oldset;
    int bas = block_all_signal(&oldset);
    DEFER(if (bas == 0) sigprocmask(SIG_SETMASK, &oldset, NULL));

    auto executor = new photon::Executor(
        OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE, {}, EXECUTOR_QUEUE_OPTION);
    auto oss_client = executor->perform([&]() {
      OssFileSystem::OssAdapterOptions options;
      if (max_list_ret != -1) {
        options.max_list_ret_cnt = max_list_ret;
      }
      options.user_agent = kUserAgentPrefix + MACRO_STR(OSSFS_VERSION_ID);
      static bool init_oss_signature = false;
      static std::string oss_region;
      if (!init_oss_signature && (rand() % 2)) {
        // oss-cn-hangzhou-xxx
        size_t start_pos = -1, end_pos = -1;
        if (endpoint.find("oss-") != std::string::npos) {
          start_pos = endpoint.find("oss-") + 4;
          if (endpoint.find('-', start_pos) != std::string::npos) {
            end_pos = endpoint.find('-', start_pos);
            end_pos++;
            while (end_pos < endpoint.length()) {
              if (endpoint[end_pos] == '-' || endpoint[end_pos] == '.') {
                oss_region = endpoint.substr(start_pos, end_pos - start_pos);
                LOG_INFO("use oss v4 signature with region `", oss_region);
                break;
              }
              end_pos++;
            }
          }
        }
      }
      init_oss_signature = true;
      options.region = oss_region;
      options.bind_ips = bind_ips;
      options.request_timeout_us = timeout_ms * 1000;
      options.endpoint = endpoint;
      options.bucket = bucket;
      options.prefix = prefix;
      options.use_list_obj_v2 = use_list_obj_v2;
      options.use_auth_cache = rand() % 2;
      options.enable_symlink = fs_opts.enable_symlink;
      oss_options_ = options;
      return new_oss_client(accessKeyId.c_str(), accessKeySecret.c_str(),
                            oss_options_);
    });

    bg_vcpu_env_.bg_oss_client_env->add_oss_client_env(executor, oss_client);
  }

  if (!preserve_input_options) {
    fs_opts.bind_cache_to_inode = rand() % 2;
  }
  LOG_INFO("bind_cache_to_inode is: `",
           fs_opts.bind_cache_to_inode ? "enabled" : "disabled");
  fs_ = new AuditableOssFs(fs_opts, bg_vcpu_env_);
  root_nodeid_ = fs_->mp_inode_->nodeid;

  return fs_->init();
}

void Ossfs2TestSuite::destroy() {
  if (fs_) delete fs_;
  if (bg_vcpu_env_.bg_oss_client_env) delete bg_vcpu_env_.bg_oss_client_env;
}

void Ossfs2TestSuite::remount() {
  destroy();
  OssFsOptions opts;
  init(opts);
}

std::string Ossfs2TestSuite::exec(const char *cmd) {
  std::array<char, 512> buffer;
  std::string result;
  std::unique_ptr<FILE, int (*)(FILE *)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

std::string Ossfs2TestSuite::get_ip_by_interface(std::string interface) {
  auto cmd = "ifconfig " + interface +
             " | grep 'inet ' | awk '{print $2}' | tr -d '\n'";
  return exec(cmd.c_str());
}

std::string Ossfs2TestSuite::nodeid_to_path(uint64_t nodeid) {
  if (nodeid == root_nodeid_) return "/";

  std::string path;
  Inode *inode = nullptr;
  std::vector<Inode *> nodes;
  nodes.reserve(10);
  path.reserve(OssFileSystem::OSS_MAX_PATH_LEN + 1);

  std::lock_guard<std::mutex> l(fs_->inodes_map_lck_);
  while (nodeid != kMountPointNodeId) {
    auto it = fs_->global_inodes_map_.find(nodeid);
    inode = it->second;

    nodes.push_back(inode);
    nodeid = inode->parent_nodeid;
  }

  for (auto it = nodes.rbegin(); it != nodes.rend(); ++it) {
    path.append("/").append((*it)->name);
  }
  return path;
}

int Ossfs2TestSuite::read_dir(uint64_t parent, std::vector<TestInode> &childs) {
  void *dirp = nullptr;
  int r = fs_->opendir(parent, &dirp);
  if (r < 0) return r;

  // fill all the nodes one time
  r = fs_->readdir(parent, 0, dirp, filler, &childs, nullptr, true, nullptr);
  if (r < 0) return r;

  r = fs_->releasedir(parent, dirp);
  return r;
}

int Ossfs2TestSuite::read_dir_without_dots(uint64_t parent,
                                           std::vector<TestInode> &childs) {
  std::vector<TestInode> tmp;
  int r = read_dir(parent, tmp);
  if (r < 0) return r;

  for (auto &child : tmp) {
    if (child.name != "." && child.name != "..") childs.push_back(child);
  }

  return 0;
}

void Ossfs2TestSuite::create_random_file(const std::string &filepath,
                                         int size_MB, int offset) {
  std::string cmd = "/bin/dd if=/dev/urandom of='" + filepath +
                    "' bs=1k count=" + std::to_string(size_MB * 1024 + offset);
  std::system(cmd.c_str());
}

void Ossfs2TestSuite::create_zero_file(const std::string &filepath, int size_KB,
                                       int offset) {
  std::string cmd = "/bin/dd if=/dev/zero of='" + filepath +
                    "' bs=1k count=" + std::to_string(size_KB + offset);
  std::system(cmd.c_str());
}

void Ossfs2TestSuite::clean_test_dir() {
  std::string cmd = "rm -rf " + test_path_;
  std::system(cmd.c_str());
}

void Ossfs2TestSuite::setup_test_dir() {
  std::string cmd = "mkdir -p " + test_path_;
  std::system(cmd.c_str());
}

std::string Ossfs2TestSuite::get_test_osspath(const std::string &subpath) {
  // for test base root
  if (subpath == "/") {
    return gtest_base_dir + "/";
  }

  std::string path = remove_prepend_backslash(subpath);
  if (path.size() < gtest_base_dir.size()) {
    return join_paths(gtest_base_dir, path);
  } else {
    if (path.substr(0, gtest_base_dir.size()) == gtest_base_dir) {
      return path;
    } else {
      return join_paths(gtest_base_dir, path);
    }
  }
}

uint64_t Ossfs2TestSuite::get_test_dir_parent(bool create) {
  struct stat st;
  uint64_t parent = 0;

  int r = fs_->lookup(root_nodeid_, gtest_base_dir.c_str(), &parent, &st);
  if (r == -ENOENT && create) {
    r = fs_->mkdir(root_nodeid_, gtest_base_dir.c_str(), 0777, 0, 0, 0, &parent,
                   &st);
  }

  if (r == 0) {
    return parent;
  }

  return r;
}

uint64_t Ossfs2TestSuite::create_file_in_folder(uint64_t parent,
                                                const std::string &filename,
                                                uint64_t size_MB,
                                                uint64_t &nodeid, int drift) {
  struct stat st;
  void *handle = nullptr;
  int r = -1;
  while (r < 0) {
    r = create_and_flush(parent, filename.c_str(), CREATE_BASE_FLAGS, 0777, 0,
                         0, 0, &nodeid, &st, &handle);
    EXPECT_EQ(r, 0);
    if (r < 0) {
      LOG_ERROR("creat ` failed", filename);
      return -1;
    }
  }
  // the caller needs to do forget

  r = fs_->release(nodeid, reinterpret_cast<IFileHandleFuseLL *>(handle));
  if (size_MB == 0 && drift == 0) {
    return 0;
  }

  return write_file_in_folder(parent, filename, size_MB, nodeid, drift);
}

uint64_t Ossfs2TestSuite::write_file_in_folder(uint64_t parent,
                                               const std::string &filename,
                                               uint64_t size_MB,
                                               uint64_t &nodeid, int drift) {
  void *handle = nullptr;
  bool keep_page_cache = false;
  int r = fs_->open(nodeid, O_RDWR, &handle, &keep_page_cache);
  char *buf_1MB = new char[0x100000];
  size_t buf_size = IO_SIZE;  // read/write IO

  // write the content of a random file
  std::string random_file = join_paths(test_path_, filename);
  create_random_file(random_file, size_MB, drift);
  std::ifstream rf(random_file);
  uint64_t crc64 = 0;
  uint64_t total_size = size_MB * 1024 * 1024 + drift;
  uint64_t offset = 0;
  while (offset < total_size) {
    buf_size = std::min(buf_size, total_size - offset);
    rf.read(buf_1MB, buf_size);

    r = write_to_file_handle(handle, buf_1MB, buf_size, offset);

    if (r < 0) {
      LOG_ERROR("write failed");
      break;
    }

    crc64 = cal_crc64(crc64, buf_1MB, buf_size);
    offset += buf_size;
  }

  r = fs_->release(nodeid, get_file_from_handle(handle));

  delete[] buf_1MB;
  return crc64;
}

uint64_t Ossfs2TestSuite::write_dirty_file(uint64_t parent,
                                           const std::string &filename,
                                           uint64_t size_MB, uint64_t &nodeid,
                                           void *&handle, int drift) {
  bool keep_page_cache = false;
  int r = fs_->open(nodeid, O_RDWR, &handle, &keep_page_cache);
  char *buf_1MB = new char[0x100000];
  size_t buf_size = IO_SIZE;  // read/write IO

  // write the content of a random file
  std::string random_file = join_paths(test_path_, filename);
  create_random_file(random_file, size_MB, drift);
  std::ifstream rf(random_file);
  uint64_t crc64 = 0;
  uint64_t total_size = size_MB * 1024 * 1024 + drift;
  uint64_t offset = 0;
  while (offset < total_size) {
    buf_size = std::min(buf_size, total_size - offset);
    rf.read(buf_1MB, buf_size);

    IFileHandleFuseLL *file = static_cast<IFileHandleFuseLL *>(handle);
    r = file->pwrite(buf_1MB, buf_size, offset);

    if (r < 0) {
      LOG_ERROR("write failed");
      break;
    }

    crc64 = cal_crc64(crc64, buf_1MB, buf_size);
    offset += buf_size;
  }

  delete[] buf_1MB;
  return crc64;
}

uint64_t Ossfs2TestSuite::write_file_intervally(uint64_t nodeid,
                                                const std::string &local_file,
                                                size_t total_size,
                                                int run_time_in_secs,
                                                int long_delay_cnt) {
  int write_times = 200;
  char buf_1MB[1024 * 1024] = {0};
  void *handle = nullptr;
  bool unused = false;
  int r = fs_->open(nodeid, O_RDWR, &handle, &unused);
  if (r < 0) return 0;
  DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

  uint64_t offset = 0, crc64 = 0;
  size_t buf_size = (total_size + write_times - 1) / write_times;
  if (buf_size > 1024 * 1024) buf_size = 1024 * 1024;
  LOG_INFO("` file size is ` bytes buf size is ` bytes", nodeid, total_size,
           buf_size);
  std::ifstream rf(local_file);

  for (int i = 0; i < write_times; i++) {
    buf_size = std::min(buf_size, total_size - offset);
    rf.read(buf_1MB, buf_size);

    int r = write_to_file_handle(handle, buf_1MB, buf_size, offset);

    if (r < 0) {
      LOG_ERROR("write failed");
      return 0;
    }

    crc64 = cal_crc64(crc64, buf_1MB, buf_size);
    offset += buf_size;

    photon::thread_usleep(run_time_in_secs * 1000 * 1000 / write_times);
    LOG_INFO("write ` bytes to offset `", buf_size, offset);

    // add some long delay to let the file stay in clean state when running in
    // rename cases
    if (long_delay_cnt > 0) {
      if (rand() % 5 == 0) {
        long_delay_cnt--;
        photon::thread_usleep(500 * 1000);  // 500ms
      }
    }
  }

  LOG_INFO("write completed with crc64 `", crc64);
  return crc64;
}

ssize_t Ossfs2TestSuite::read_from_handle(void *fh, char *buf, size_t size,
                                          off_t offset) {
  auto file = get_file_from_handle(fh);
  void *pin_buffer = nullptr;
  ssize_t r = file->pin(offset, size, &pin_buffer);
  if (r > 0) {
    memcpy(buf, pin_buffer, r);
    file->unpin(offset);
    return r;
  }

  return file->pread(buf, size, offset);
}

ssize_t Ossfs2TestSuite::read_file_in_folder(uint64_t parent,
                                             const std::string &filename,
                                             uint64_t *out_crc64) {
  struct stat st;
  uint64_t nodeid = 0;
  int r = fs_->lookup(parent, filename.c_str(), &nodeid, &st);
  if (r < 0) return r;

  DEFER(fs_->forget(nodeid, 1));

  void *handle = nullptr;
  bool unused = false;
  r = fs_->open(nodeid, O_RDONLY, &handle, &unused);
  if (r < 0) return r;

  DEFER(fs_->release(nodeid, get_file_from_handle(handle)));

  uint64_t offset = 0;
  uint64_t size = st.st_size;
  uint64_t buf_size = IO_SIZE;
  char *buf = new char[IO_SIZE];
  DEFER(delete[] buf);

  uint64_t crc64 = 0;
  while (offset < size) {
    auto read_size = std::min(buf_size, size - offset);
    r = read_from_handle(handle, buf, read_size, offset);
    if (r < 0) return r;

    crc64 = cal_crc64(crc64, buf, r);
    offset += r;
  }

  if (out_crc64) *out_crc64 = crc64;
  return size;
}

ssize_t Ossfs2TestSuite::write_to_file_handle(void *fh, const char *buf,
                                              size_t size, off_t offset) {
  ssize_t r = 0;
  auto file = get_file_from_handle(fh);
  if (FLAGS_write_with_fuse_bufvec) {
    int retry_cnt = 0, max_retry_cnt = 100;
  retry:
    try {
      r = write_with_fuse_bufvec(fh, buf, size, offset);
    } catch (const std::exception &e) {
      LOG_ERROR("write fuse bufvec throw: `, retry `", e.what(), ++retry_cnt);
      if (retry_cnt < max_retry_cnt) {
        usleep(rand() % 10000 + 1000);
        goto retry;
      }
      return -1;
    }
  } else {
    r = file->pwrite(buf, size, offset);
  }

  return r;
}

ssize_t Ossfs2TestSuite::write_with_fuse_bufvec(void *fh, const char *buf,
                                                size_t size, off_t offset) {
  auto file = get_file_from_handle(fh);
  int pipe_fd[2] = {0};
  if (pipe(pipe_fd) < 0) {
    LOG_ERROR("pipe failed");
    return -1;
  }
  DEFER({
    close(pipe_fd[0]);
    close(pipe_fd[1]);
  });

  struct fuse_bufvec buf_vec;
  buf_vec.buf[0].flags = FUSE_BUF_IS_FD;
  buf_vec.buf[0].fd = pipe_fd[0];
  buf_vec.buf[0].pos = 0;
  buf_vec.buf[0].size = size;
  buf_vec.count = 1;
  buf_vec.idx = 0;
  buf_vec.off = 0;

  auto writer = std::async(std::launch::async, [&]() -> ssize_t {
    ssize_t r = write(pipe_fd[1], buf, size);
    if (r < 0) {
      return -errno;
    }
    return r;
  });

  ssize_t r1 = file->write_buf(&buf_vec, offset);
  if (r1 < 0) {
    close(pipe_fd[0]);
    close(pipe_fd[1]);
    writer.get();
    return r1;
  }
  ssize_t r2 = writer.get();

  if (r2 != static_cast<ssize_t>(size)) {
    LOG_ERROR("write failed r2: `", r2);
    return r2;
  }
  return r1;
}

ssize_t Ossfs2TestSuite::write_with_fuse_bufvec_with_err(
    void *fh, const char *buf, size_t size, off_t offset,
    size_t partial_write_size) {
  auto file = get_file_from_handle(fh);
  int pipe_fd[2] = {0};
  if (pipe(pipe_fd) < 0) {
    LOG_ERROR("pipe failed");
    return -1;
  }

  struct fuse_bufvec buf_vec;
  buf_vec.buf[0].flags = FUSE_BUF_IS_FD;
  buf_vec.buf[0].fd = pipe_fd[0];
  buf_vec.buf[0].pos = 0;
  buf_vec.buf[0].size = size;
  buf_vec.count = 1;
  buf_vec.idx = 0;
  buf_vec.off = 0;

  auto writer = std::async(std::launch::async, [&]() -> ssize_t {
    // write partial data
    ssize_t r = write(pipe_fd[1], buf, partial_write_size);
    if (r < 0) {
      LOG_ERROR("write failed");
      return -errno;
    }

    // wait a moment for read at least 1 bytes from pipe
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // close pipe
    close(pipe_fd[0]);
    close(pipe_fd[1]);
    return r;
  });

  ssize_t r1 = file->write_buf(&buf_vec, offset);
  writer.get();
  return r1;
}

int Ossfs2TestSuite::create_and_flush(uint64_t parent, const char *name,
                                      int flags, mode_t mode, uid_t uid,
                                      gid_t gid, mode_t umask, uint64_t *nodeid,
                                      struct stat *stbuf, void **fh) {
  int r =
      fs_->creat(parent, name, flags, mode, uid, gid, umask, nodeid, stbuf, fh);
  if (r != 0) return r;

  if (rand() % 2) {
    return 0;
  }

  LOG_INFO("do flush after create (`, `)", parent, name);
  return get_file_from_handle(*fh)->fsync();
}

std::string Ossfs2TestSuite::lookup_ossutil() {
  char buf[256];
  int c = readlink("/proc/self/exe", buf, 256);
  std::string path(buf, c);
  for (int i = 0; i < 3; i++) {
    path = path.substr(0, path.rfind('/'));
  }

  std::vector<std::string> ossutil_find_paths = {
      path + "/ossutil64",        path + "/ossutil",
      "/usr/bin/ossutil64",       "/usr/bin/ossutil",
      "/usr/local/bin/ossutil64", "/usr/local/bin/ossutil"};

  for (const auto &p : ossutil_find_paths) {
    struct stat st_buf;
    int r = stat(p.c_str(), &st_buf);
    if (r == 0) {
      return p;
    }
  }

  LOG_ERROR("can't find ossutil");
  return std::string();
}

int Ossfs2TestSuite::upload_file(const std::string &local_file,
                                 const std::string &target,
                                 const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint + " cp " +
        local_file + " \"oss://" + FLAGS_oss_bucket + "/" + osspath +
        "\" --force";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to upload: `", target);
    return r;
  }

  LOG_INFO("upload: `", target);
  return r;
}

int Ossfs2TestSuite::copy_file(const std::string &src, const std::string &dst,
                               const std::string &oss_prefix) {
  std::string src_path = join_paths(oss_prefix, get_test_osspath(src));
  std::string dst_path = join_paths(oss_prefix, get_test_osspath(dst));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " cp \"oss://" + FLAGS_oss_bucket + "/" + src_path + "\" \"oss://" +
        FLAGS_oss_bucket + "/" + dst_path + "\" --force";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to copy: `", src, " to ", dst);
    return r;
  }

  LOG_INFO("copy: ` to `", src, " to ", dst);
  return r;
}

int Ossfs2TestSuite::delete_file(const std::string &target,
                                 const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " rm \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\" -f";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to delete: `", target);
    return r;
  }

  return r;
}

int Ossfs2TestSuite::create_dir(const std::string &target,
                                const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " mkdir \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\"";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to mkdir: `", target);
    return r;
  }

  LOG_INFO("mkdir: `", target);

  return r;
}

int Ossfs2TestSuite::delete_dir(const std::string &target,
                                const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " rm \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\" -r -f";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to rmdir: `", target);
    return r;
  }
  LOG_INFO("rmdir: `", target);

  return 0;
}

int Ossfs2TestSuite::stat_file(const std::string &target,
                               const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " stat \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\"";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to stat: `", target);
    return r;
  }

  LOG_INFO("stat: `", target);
  return r;
}

std::map<std::string, std::string> Ossfs2TestSuite::get_file_meta(
    const std::string &target, const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();
  std::map<std::string, std::string> res;
  int retry_times = 3;
  std::string output;

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " stat \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\"";
retry:
  try {
    output = exec(cmd.c_str());
  } catch (const std::exception &e) {
    output = std::string("Error: ") + e.what();
  }
  LOG_DEBUG("get meta: `, output: `", target, "\n" + output);
  if (output.empty() || output.find("Error: ") != std::string::npos) {
    if (retry_times-- > 0) {
      goto retry;
    }
    return res;
  }

  output.erase(std::remove(output.begin(), output.end(), ' '), output.end());
  std::stringstream ss(output);
  std::string line;
  while (std::getline(ss, line, '\n')) {
    auto pos = line.find(":");
    if (pos == std::string::npos) {
      continue;
    }
    res[line.substr(0, pos)] = line.substr(pos + 1);
  }

  return res;
}

int Ossfs2TestSuite::set_file_meta(const std::string &target,
                                   const std::string &key,
                                   const std::string &value,
                                   const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " set-meta \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\" " + key +
        ":" + value + " --update";

  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to set meta: `", target);
    return r;
  }

  LOG_INFO("set meta: `", target);
  return r;
}

std::vector<std::string> Ossfs2TestSuite::get_list_objects(
    const std::string &target, const std::string &oss_prefix,
    bool include_self) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(target));
  std::string cmd;
  std::string ossutil = lookup_ossutil();
  std::vector<std::string> res;

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " ls \"oss://" + FLAGS_oss_bucket + "/" + osspath + "/\" -s -d";

  std::string out = exec(cmd.c_str());
  std::stringstream ss(out);
  std::string line;
  std::string str_prefix = "oss://" + FLAGS_oss_bucket + "/" + osspath + "/";

  while (std::getline(ss, line, '\n')) {
    if (line.find(str_prefix) != std::string::npos) {
      if (!include_self && line == str_prefix) continue;
      res.emplace_back(line.substr(str_prefix.length()));
    } else {
      break;
    }
  }

  return res;
}

int Ossfs2TestSuite::upload_file_tree(int depth, int width, int files,
                                      const std::string &local_file,
                                      const std::string &dir_prefix,
                                      const std::string &file_prefix,
                                      const std::string &object_prefix) {
  LOG_INFO("UploadFileTree: {depth, width, files} = {`, `, `}", depth, width,
           files);

  std::string dir_name;
  int success = 0;
  std::function<void(int)> upload_file_tree_recur = [&](int d) {
    if (d == depth) {
      std::set<std::string> used_file_name;
      for (int i = 0; i < files; i++) {
        std::string filename = join_paths(object_prefix, dir_name);
        if (file_prefix.empty()) {
          // do not use same filename under same dir.
          std::string rand_name;

          while (true) {
            rand_name = random_string(32);
            LOG_INFO("generate random string `", rand_name);
            if (used_file_name.find(rand_name) == used_file_name.end()) {
              used_file_name.insert(rand_name);
              break;
            }
          };

          filename += rand_name;
        } else {
          filename += file_prefix + std::to_string(i);
        }

        LOG_INFO("upload file ", filename);
        int r = upload_file(local_file, filename, FLAGS_oss_bucket_prefix);
        ASSERT_EQ(r, 0);
        success++;
      }
      return;
    }

    for (int i = 0; i < width; i++) {
      std::string old_dir_name = dir_name;
      dir_name += dir_prefix + std::to_string(i) + "/";
      upload_file_tree_recur(d + 1);
      dir_name = old_dir_name;
    }
  };

  upload_file_tree_recur(0);

  if (success == pow(width, depth) * files) {
    return 0;
  }

  return -1;
}

int Ossfs2TestSuite::create_oss_symlink(const std::string &object,
                                        const std::string &link,
                                        const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(object));
  std::string cmd;
  std::string ossutil = lookup_ossutil();

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " create-symlink \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\" " +
        "\"" + link + "\"";
  int r;
  if ((r = system(cmd.c_str())) != 0) {
    LOG_ERROR("Fail to create symlink: `", object);
    return r;
  }

  LOG_INFO("create symlink ` -> `", object, link);
  return r;
}

int Ossfs2TestSuite::read_oss_symlink(const std::string &object,
                                      std::string &link,
                                      const std::string &oss_prefix) {
  std::string osspath = join_paths(oss_prefix, get_test_osspath(object));
  std::string cmd;
  std::string ossutil = lookup_ossutil();
  std::map<std::string, std::string> res;
  int retry_times = 3;
  std::string output;

  cmd = ossutil + " -i " + FLAGS_oss_access_key_id + " -k " +
        FLAGS_oss_access_key_secret + " -e " + FLAGS_oss_endpoint +
        " read-symlink \"oss://" + FLAGS_oss_bucket + "/" + osspath + "\"";

retry:
  try {
    output = exec(cmd.c_str());
  } catch (const std::exception &e) {
    output = std::string("Error: ") + e.what();
  }
  LOG_DEBUG("read symlink: `, output: `", object, "\n" + output);
  if (output.empty() || output.find("Error: ") != std::string::npos) {
    if (retry_times-- > 0) {
      goto retry;
    }
    return -1;
  }

  output.erase(std::remove(output.begin(), output.end(), ' '), output.end());
  std::stringstream ss(output);
  std::string line;
  while (std::getline(ss, line, '\n')) {
    auto pos = line.find(":");
    if (pos == std::string::npos) {
      continue;
    }
    res[line.substr(0, pos)] = line.substr(pos + 1);
  }

  if (res.find("X-Oss-Symlink-Target") != res.end()) {
    link = res["X-Oss-Symlink-Target"];
    return 0;
  }

  return -1;
}
