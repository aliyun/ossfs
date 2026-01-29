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

#include <CLI11.h>
#include <malloc.h>
#include <photon/common/executor/executor.h>
#include <photon/io/signal.h>
#include <pwd.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <tuple>

#include "admin/cli_stats.h"
#include "admin/http_server.h"
#include "common/fstab.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/utils.h"
#include "fs/fs.h"
#include "fuse_adapter_ll.h"
#include "options.h"
#include "oss/oss_adapter.h"

#define EXECUTOR_QUEUE_OPTION \
  { 16, 1024 }

#define DEV_STDOUT "/dev/stdout"
#define LOG_FILE_PREFIX "ossfs2.log"

static const std::string kUserAgentPrefix = "ossfs2-";

static OssFileSystem::BackgroundVCpuEnv g_bg_vcpu_env;
static std::string g_mountpoint;
static bool g_log_to_stdout = false;

static int block_all_signal(sigset_t *oldset) {
  sigset_t sigset;
  int r = sigfillset(&sigset);
  if (r != 0) return r;
  r = sigprocmask(SIG_SETMASK, &sigset, oldset);
  return r;
}

std::tuple<std::string, std::string> get_oss_credentials() {
  // Use env vars if they are not specified in the command line or the config
  // file.
  if (gflags::GetCommandLineFlagInfoOrDie("oss_access_key_id").is_default ||
      gflags::GetCommandLineFlagInfoOrDie("oss_access_key_secret").is_default) {
    std::string access_key_id, access_key_secret;
    char *env_access_key_id = std::getenv("OSS_ACCESS_KEY_ID");
    char *env_access_key_secret = std::getenv("OSS_ACCESS_KEY_SECRET");
    if (env_access_key_id != nullptr) access_key_id.assign(env_access_key_id);
    if (env_access_key_secret != nullptr)
      access_key_secret.assign(env_access_key_secret);

    return std::make_tuple(access_key_id, access_key_secret);
  }

  return std::make_tuple(FLAGS_oss_access_key_id, FLAGS_oss_access_key_secret);
}

static OssFileSystem::OssAdapter *create_oss_client(
    photon::Executor *eth, const std::string &access_key_id,
    const std::string &access_key_secret,
    const OssFileSystem::OssAdapterOptions &options) {
  return eth->perform([&]() {
    OssFileSystem::OssAdapter *ossfs = nullptr;

    ossfs = OssFileSystem::new_oss_client(access_key_id.c_str(),
                                          access_key_secret.c_str(), options);

    return ossfs;
  });
}

int create_background_oss_clients() {
  const std::string &endpoint = FLAGS_oss_endpoint;
  const std::string &bucket = FLAGS_oss_bucket;
  std::string prefix = remove_prepend_backslash(FLAGS_oss_bucket_prefix);
  auto [access_key_id, access_key_secret] = get_oss_credentials();

  if ((access_key_id.empty() || access_key_secret.empty()) &&
      FLAGS_ram_role.empty() && FLAGS_credential_process.empty()) {
    LOG_ERROR(
        "empty credential, please set ak/sk, ram_role or credential_process");
    return -EINVAL;
  }

  // Reset ak and sk if ram_role/process is specified.
  if (!FLAGS_ram_role.empty() || !FLAGS_credential_process.empty()) {
    access_key_id.clear();
    access_key_secret.clear();
  }

  OssFileSystem::OssAdapterOptions options;
  options.max_list_ret_cnt = FLAGS_max_list_ret_count;
  options.use_list_obj_v2 = FLAGS_use_list_obj_v2;
  options.user_agent = kUserAgentPrefix + MACRO_STR(OSSFS_VERSION_ID);
  options.region = FLAGS_oss_region;
  options.request_timeout_us = FLAGS_oss_request_timeout_ms * 1000;
  options.endpoint = endpoint;
  options.bucket = bucket;
  // TODO: validate all ip addresses that can access OSS.
  options.bind_ips = FLAGS_bind_ips;
  options.prefix = prefix;
  options.use_auth_cache = FLAGS_use_auth_cache;
  options.enable_symlink = FLAGS_enable_symlink;
  options.proxy = FLAGS_http_proxy;

  sigset_t oldset;
  int bas = block_all_signal(&oldset);
  DEFER(if (bas == 0) sigprocmask(SIG_SETMASK, &oldset, NULL));

  g_bg_vcpu_env.bg_oss_client_env = new OssFileSystem::BGVCpuOssClientEnv;

  auto hw_concurrency = std::thread::hardware_concurrency();
  if (hw_concurrency > 0 &&
      gflags::GetCommandLineFlagInfoOrDie("oss_vcpu_count").is_default &&
      FLAGS_oss_vcpu_count > hw_concurrency) {
    FLAGS_oss_vcpu_count = hw_concurrency;
    LOG_DEBUG("oss_vcpu_count is set to ", hw_concurrency);
  }

  for (unsigned i = 0; i < FLAGS_oss_vcpu_count; i++) {
    auto executor = new photon::Executor(
        OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE, {}, EXECUTOR_QUEUE_OPTION);
    auto oss_client =
        create_oss_client(executor, access_key_id, access_key_secret, options);
    g_bg_vcpu_env.bg_oss_client_env->add_oss_client_env(executor, oss_client);
  }

  return 0;
}

static int start_http_server(bool &is_stopping) {
  INIT_PHOTON();
  return OssFileSystem::Admin::start_http_server(is_stopping, FLAGS_metrics_ip,
                                                 uint16_t(FLAGS_metrics_port));
}

static uint64_t get_sys_total_memory() {
  FILE *meminfo = fopen("/proc/meminfo", "r");
  if (meminfo == NULL) {
    LOG_ERROR("Failed to get total memory, `", strerror(errno));
    return 0;
  }

  char line[256];
  size_t total_memory = 0;

  while (fgets(line, sizeof(line), meminfo)) {
    if (strncmp(line, "MemTotal:", 9) == 0) {
      // Skip "MemTotal:" and spaces
      char *mem_value = line + 9;
      while (*mem_value == ' ') {
        mem_value++;
      }

      total_memory = strtoul(mem_value, NULL, 10);

      // Convert from kB to bytes
      total_memory *= 1024;
      break;
    }
  }

  fclose(meminfo);
  return total_memory;
}

static int adjust_fs_options_with_mem_limit(
    OssFileSystem::OssFsOptions *fs_options) {
  uint64_t total_mem_limit = parse_bytes_string(FLAGS_total_mem_limit).value();
  if (total_mem_limit == 0) {
    auto total_mem = get_sys_total_memory();
    if (total_mem == 0) {
      LOG_WARN("Failed to get total memory");
      return 0;
    }

    // Only set for system total memory less than 16GB.
    if (total_mem >= 16ULL * 1024 * 1024 * 1024) {
      return 0;
    }

    total_mem_limit = align_down(total_mem / 2, 1048576);
    LOG_WARN(
        "total_mem_limit is not set, use half of system total memory ` Bytes",
        total_mem_limit);
  }

  // at least 256MB
  if (total_mem_limit < 256ULL * 1024 * 1024) {
    LOG_WARN("total_mem_limit is too small, force set to 256MB");
    total_mem_limit = 256ULL * 1024 * 1024;
  }

  // use 64MB for threads
  if (fs_options->memory_data_cache_size == 0) {
    total_mem_limit -= 64ULL * 1024 * 1024;
  }

  return OssFileSystem::OssFsOptions::apply_mem_limit(
      fs_options, total_mem_limit, FLAGS_total_rw_mem_ratio);
}

static int init_fs_options(OssFileSystem::OssFsOptions *fs_options) {
  fs_options->readonly = FLAGS_ro;
  fs_options->attr_timeout = FLAGS_attr_timeout;
  fs_options->close_to_open = FLAGS_close_to_open;

  fs_options->oss_negative_cache_size = FLAGS_oss_negative_cache_size;
  fs_options->oss_negative_cache_timeout = FLAGS_oss_negative_cache_timeout;

  fs_options->prefetch_chunk_size =
      parse_bytes_string(FLAGS_prefetch_chunk_size).value();
  fs_options->prefetch_concurrency = FLAGS_prefetch_concurrency;
  fs_options->prefetch_chunks = FLAGS_prefetch_chunks;

  fs_options->prefetch_concurrency_per_file =
      std::min(FLAGS_prefetch_concurrency_per_file,
               static_cast<int>(fs_options->prefetch_concurrency));

  fs_options->upload_buffer_size =
      align_up(parse_bytes_string(FLAGS_upload_buffer_size).value(),
               fs_options->cache_block_size);
  fs_options->upload_concurrency = FLAGS_upload_concurrency;
  fs_options->upload_copy_concurrency = FLAGS_upload_copy_concurrency;
  fs_options->enable_appendable_object = FLAGS_enable_appendable_object;
  fs_options->appendable_object_autoswitch_threshold =
      FLAGS_appendable_object_autoswitch_threshold;

  fs_options->readdir_remember_count = FLAGS_max_list_ret_count;

  fs_options->allow_rename_dir = FLAGS_allow_rename_dir;
  fs_options->rename_dir_concurrency = FLAGS_rename_dir_concurrency;
  fs_options->rename_dir_limit = FLAGS_rename_dir_limit;

  fs_options->enable_crc64 = FLAGS_enable_crc64;
  fs_options->enable_transmission_control = FLAGS_enable_transmission_control;
  fs_options->tc_max_latency_threshold_us = FLAGS_tc_max_latency_threshold_us;
  fs_options->seq_read_detect_count = FLAGS_seq_read_detect_count;
  fs_options->inode_cache_eviction_threshold =
      FLAGS_inode_cache_eviction_threshold;
  fs_options->inode_cache_eviction_interval_ms =
      FLAGS_inode_cache_eviction_interval_ms;
  fs_options->max_inode_cache_count = FLAGS_max_inode_cache_count;

  fs_options->enable_symlink = FLAGS_enable_symlink;

  if (gflags::GetCommandLineFlagInfoOrDie("uid").is_default) {
    fs_options->uid = getuid();
  } else {
    fs_options->uid = FLAGS_uid;
  }

  if (gflags::GetCommandLineFlagInfoOrDie("gid").is_default) {
    fs_options->gid = getgid();
  } else {
    fs_options->gid = FLAGS_gid;
  }

  fs_options->dir_mode = std::strtol(FLAGS_dir_mode.c_str(), nullptr, 8);
  fs_options->file_mode = std::strtol(FLAGS_file_mode.c_str(), nullptr, 8);

  fs_options->set_mime_for_rename_dst = FLAGS_set_mime_for_rename_dst;
  fs_options->ram_role = FLAGS_ram_role;
  fs_options->credential_process = FLAGS_credential_process;

  mallopt(M_TRIM_THRESHOLD, 64 * 1024 * 1024);
  fs_options->cache_refill_unit = fs_options->cache_block_size;
  fs_options->min_reserved_buffer_size_per_file =
      FLAGS_min_reserved_buffer_size_per_file;

  fs_options->memory_data_cache_size =
      parse_bytes_string(FLAGS_memory_data_cache_size).value();

  if (fs_options->memory_data_cache_size > 0) {
    fs_options->bind_cache_to_inode = true;
    if (fs_options->prefetch_concurrency == 0) {
      LOG_ERROR("prefetch_concurrency must be greater than 0.");
      return -EINVAL;
    }

    if (fs_options->prefetch_chunks > 0) {
      LOG_INFO("Memory cache is enabled, prefetch_chunks will be overwritted.");
    }

    uint64_t prefetch_chunks =
        fs_options->memory_data_cache_size / fs_options->prefetch_chunk_size;
    if (prefetch_chunks == 0) {
      LOG_ERROR("Memory cache size is too small.");
      return -EINVAL;
    }
    fs_options->prefetch_chunks = prefetch_chunks;
  }

  if (adjust_fs_options_with_mem_limit(fs_options) < 0) return -EINVAL;

  fs_options->enable_admin_server = FLAGS_enable_admin_server;
  return 0;
}

static void parse_fuse_options() {
  FuseLLOptions fuse_opt;
  fuse_opt.attr_timeout = FLAGS_fuse_attr_timeout == -1
                              ? FLAGS_attr_timeout
                              : FLAGS_fuse_attr_timeout;
  fuse_opt.entry_timeout = FLAGS_fuse_entry_timeout == -1
                               ? FLAGS_attr_timeout
                               : FLAGS_fuse_entry_timeout;
  fuse_opt.negative_timeout = FLAGS_negative_timeout;
  fuse_opt.readdirplus = FLAGS_readdirplus;
  if (FLAGS_attr_timeout == 0 && fuse_opt.attr_timeout == 0 &&
      fuse_opt.entry_timeout == 0) {
    fuse_opt.readdirplus = false;
  }

  fuse_opt.ignore_fsync = FLAGS_ignore_fsync;

  if (FLAGS_close_to_open && !FLAGS_sync_upload) {
    FLAGS_sync_upload = true;
    LOG_WARN("close_to_open is enabled, sync_upload is ignored.");
  }
  fuse_opt.ignore_flush = !FLAGS_sync_upload;

  set_fuse_ll_options(fuse_opt);
}

static void dump_mount_options() {
  std::vector<gflags::CommandLineFlagInfo> flags;
  gflags::GetAllFlags(&flags);
  auto all_options = OptionsRegistry::get_all_options();
  std::vector<std::pair<std::string, std::string>> common_options;
  std::vector<std::pair<std::string, std::string>> experimental_options;

  for (const auto &options_category : all_options) {
    for (const auto &option : options_category) {
      if (OptionsRegistry::kSensitiveOptions.find(option.name) !=
          OptionsRegistry::kSensitiveOptions.end()) {
        continue;
      }

      auto flag = gflags::GetCommandLineFlagInfoOrDie(option.name.c_str());
      if (option.hidden && flag.is_default) {
        continue;
      }

      if (option.experimental) {
        experimental_options.emplace_back(flag.name, flag.current_value);
      } else {
        common_options.emplace_back(flag.name, flag.current_value);
      }
    }
  }

  LOG_INFO("Ossfs2 version: `(`)", MACRO_STR(OSSFS_VERSION_ID),
           std::string_view(MACRO_STR(OSSFS_COMMIT_ID)).substr(0, 8));
  LOG_INFO("Mount options: ");

  for (const auto &it : common_options) {
    LOG_INFO("Add mount option: --`=`", it.first, it.second);
  }

  for (const auto &it : experimental_options) {
    LOG_INFO("Add mount option(experimental): --`=`", it.first, it.second);
  }
}

static void trim_mount_options() {
  if (FLAGS_skip_trim_options) return;

  std::vector<gflags::CommandLineFlagInfo> flags;
  gflags::GetAllFlags(&flags);
  for (auto &flag : flags) {
    if (!flag.is_default && flag.type == "string") {
      gflags::SetCommandLineOption(
          flag.name.c_str(),
          std::string(trim_string_view(flag.current_value)).c_str());
    }
  }
}

static std::string get_log_file_base_dir(const std::string &root) {
  std::string base = root;
  auto uid = getuid();
  struct passwd *pw = getpwuid(uid);
  if (pw) {
    base = join_paths(base, std::string(pw->pw_name));
  } else {
    base = join_paths(base, std::to_string(uid));
  }
  return base;
}

static void init_logger() {
  int log_level = FLAGS_log_level == "debug" ? ALOG_DEBUG : ALOG_INFO;

  if (!g_log_to_stdout) {
    std::string base = FLAGS_log_dir;
    base = get_log_file_base_dir(base);
    auto r = access(base.c_str(), R_OK | W_OK | X_OK);
    if (r != 0) r = mkdir(base.c_str(), 0755);
    if (r == 0) {
      // TODO: use filelock to avoid log files being overwritten by multiple
      // instances
      base = join_paths(base, LOG_FILE_PREFIX);
      if (FLAGS_enable_photon_logs) {
        log_output_file(base.c_str(), FLAGS_log_file_max_size,
                        FLAGS_log_file_max_count);
        log_output_level = log_level;
      } else {
        set_ossfs_log_setting(base.c_str(), FLAGS_log_file_max_size,
                              FLAGS_log_file_max_count, -1UL, log_level);
        set_default_logger_output_to_ossfs_log_file(ALOG_ERROR);
      }
    }
  } else {
    if (FLAGS_enable_photon_logs) {
      log_output_level = log_level;
    } else {
      log_output_level = ALOG_ERROR;
      set_ossfs_log_to_stdout(log_level);
    }
  }
}

static void test_handler(int sig) {
  LOG_INFO("[TestSignalHandler] thread ` got `", syscall(SYS_gettid), sig);
}

static int set_one_signal_handler(int sig, void (*handler)(int)) {
  struct sigaction sa;
  struct sigaction old_sa;

  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = handler;
  sigemptyset(&(sa.sa_mask));
  sa.sa_flags = 0;

  if (sigaction(sig, NULL, &old_sa) == -1) {
    LOG_ERROR("cannot get old signal handler");
    return -1;
  }

  if (old_sa.sa_handler == SIG_DFL && sigaction(sig, &sa, NULL) == -1) {
    LOG_ERROR("cannot set signal handler");
    return -1;
  }
  return 0;
}

static int create_ossfs_and_run_fuse(struct fuse_args &args,
                                     struct fuse_cmdline_opts &fuse_opts,
                                     int pipefd) {
  init_logger();

  int err = -1;
  char completed = 1;
  struct fuse_session *session = nullptr;
  struct fuse_lowlevel_ops *fs_ops_ll;
  OssFileSystem::OssFsOptions fs_options;
  OssFileSystem::OssFs *fs = nullptr;
  std::thread *http_server_thread = nullptr;
  bool is_stopping = false;

  err = create_background_oss_clients();
  if (err != 0) {
    LOG_ERROR(
        "create_background_oss_clients failed, please check your oss config");
    goto exit;
  }

#if defined(__SANITIZE_ADDRESS__)
  LOG_INFO("AddressSanitizer is enabled.");
#endif

  dump_mount_options();
  err = init_fs_options(&fs_options);
  if (err != 0) {
    goto exit;
  }

  fs_options.mountpath = fuse_opts.mountpoint;
  fs = new OssFileSystem::OssFs(fs_options, g_bg_vcpu_env);
  if (fs == nullptr) {
    LOG_ERROR("Failed to allocate OssFs object");
    goto exit;
  }

  err = fs->init();
  if (err != 0) {
    LOG_ERROR("Init OssFileSystem failed with error `", err);
    completed = static_cast<char>(err);
    goto exit;
  }

  if (FLAGS_metrics_port > 0) {
    http_server_thread =
        new std::thread([&]() { start_http_server(is_stopping); });
    LOG_INFO("http server started");
  }

  parse_fuse_options();
  set_fuse_ll_fs(fs);
  fs_ops_ll = get_fuse_ll_oper();

  if ((session = fuse_session_new(&args, fs_ops_ll,
                                  sizeof(struct fuse_lowlevel_ops), fs)) ==
      nullptr) {
    LOG_ERROR("fuse_session_new failed with error: `", strerror(errno));
    goto exit;
  }

  if (fuse_set_signal_handlers(session) != 0) {
    LOG_ERROR("set_signal_handlers failed with error: `", strerror(errno));
    goto sighandler_error;
  }

  if (FLAGS_enable_test_signal_handler) {
    if (set_one_signal_handler(SIGUSR1, test_handler) != 0) {
      LOG_ERROR("set_test_signal_handlers failed with error: `",
                strerror(errno));
      goto sighandler_error;
    }
  }

  if ((fuse_session_mount(session, fuse_opts.mountpoint)) != 0) {
    LOG_ERROR("fuse_session_mount failed with error: `", strerror(errno));
    goto mnt_error;
  }

  completed = 0;

  if (!FLAGS_f) {
    write(pipefd, &completed, sizeof(completed));
  }

  fs->set_fuse_session(session);

  err = fuse_session_loop_mt_with_photon(session, FLAGS_fuse_threads);

  fuse_session_unmount(session);

mnt_error:
  fuse_remove_signal_handlers(session);

sighandler_error:
  fuse_session_destroy(session);

exit:
  if (fs) delete fs;
  is_stopping = true;

  if (http_server_thread) {
    http_server_thread->join();
    delete http_server_thread;
  }

  if (g_bg_vcpu_env.bg_oss_client_env) delete g_bg_vcpu_env.bg_oss_client_env;

  if (!FLAGS_f) {
    write(pipefd, &completed, sizeof(completed));
  }

  return err;
}

static void log_exit_error(char r) {
  if (r != 0) {
    if (g_log_to_stdout) {
      fprintf(stderr,
              "ERROR: failed to mount ossfs2, see more details in logs\n");
    } else {
      fprintf(
          stderr,
          "ERROR: failed to mount ossfs2, see more details in log dir: %s\n",
          get_log_file_base_dir(FLAGS_log_dir).c_str());
    }
    return;
  }

  if (!FLAGS_f) {
    auto oss_path = join_paths(
        FLAGS_oss_bucket, remove_prepend_backslash(FLAGS_oss_bucket_prefix));
    printf("Mount oss://%s to %s successfully\n", oss_path.c_str(),
           g_mountpoint.c_str());
  }
}

static int validate_log_dir(const std::string &mountpoint,
                            const std::string &log_dir) {
  if (g_log_to_stdout) return 0;

  // 1. convert log dir to absolute path
  if (log_dir.empty()) {
    fprintf(stderr, "ERROR: log_dir cannot be empty\n");
    return -1;
  }

  // Users could not find the real path if they use relative path, reject it.
  if (log_dir[0] != '/') {
    fprintf(stderr, "ERROR: log_dir must be absolute path: %s\n",
            log_dir.c_str());
    return -1;
  }

  std::error_code ec;
  std::filesystem::path abs_log_dir_path =
      std::filesystem::absolute(log_dir, ec);
  if (ec) {
    fprintf(stderr, "ERROR: cannot get absolute path of log_dir: %s, err: %s\n",
            log_dir.c_str(), ec.message().c_str());
    return -1;
  }
  std::string abs_log_dir_str = abs_log_dir_path.lexically_normal().string();
  if (abs_log_dir_str.back() == '/') abs_log_dir_str.pop_back();

  std::filesystem::path abs_mountpoint =
      std::filesystem::absolute(mountpoint, ec);
  if (ec) {
    fprintf(stderr,
            "ERROR: cannot get absolute path of mountpoint: %s, err: %s\n",
            mountpoint.c_str(), ec.message().c_str());
    return -1;
  }
  std::string abs_mountpoint_str = abs_mountpoint.lexically_normal().string();
  if (abs_mountpoint_str.back() == '/') abs_mountpoint_str.pop_back();

  // Check deadlock.
  if (abs_mountpoint_str == abs_log_dir_str) {
    fprintf(stderr, "ERROR: log_dir cannot be the same as MOUNTPOINT: %s\n",
            abs_log_dir_str.c_str());
    return -1;
  } else if (is_subdir(abs_mountpoint_str, abs_log_dir_str)) {
    fprintf(stderr, "ERROR: log_dir %s cannot be subdir of MOUNTPOINT: %s\n",
            abs_log_dir_str.c_str(), abs_mountpoint_str.c_str());
    return -1;
  } else if (is_subdir(abs_log_dir_str, abs_mountpoint_str)) {
    fprintf(stderr, "ERROR: MOUNTPOINT %s cannot be subdir of log_dir: %s\n",
            abs_mountpoint_str.c_str(), abs_log_dir_str.c_str());
    return -1;
  }

  int r = ::access(log_dir.c_str(), R_OK | W_OK | X_OK);
  if (r == 0) {
    if (!std::filesystem::is_directory(log_dir, ec)) {
      if (ec) {
        fprintf(stderr, "ERROR: cannot check mode of log_dir: %s, err: %s\n",
                log_dir.c_str(), ec.message().c_str());
      } else {
        fprintf(stderr, "ERROR: log_dir: %s is not a directory\n",
                log_dir.c_str());
      }
      return -1;
    }
  } else {
    auto omask = ::umask(0);
    r = ::mkdir(log_dir.c_str(), 0777);
    if (r != 0) {
      fprintf(stderr, "ERROR: cannot create log_dir: %s, err: %s\n",
              log_dir.c_str(), strerror(errno));
      return -1;
    }
    ::umask(omask);
  }

  FLAGS_log_dir = abs_log_dir_str;
  g_mountpoint = abs_mountpoint_str;
  return 0;
}

static void do_show_mount_help() {
  auto print_one = [](const OptionsRegistry::OptionAttribute &option) {
    if (option.hidden || option.experimental) return;
    auto flag = gflags::GetCommandLineFlagInfoOrDie(option.name.c_str());

    const int name_width = 48;
    const int max_width = 120;
    std::string name_part = "--" + flag.name;
    std::string type_part = ". Type: " + flag.type;
    std::string default_part;
    if (flag.type == "string") {
      default_part = ". Default: \"" + flag.default_value + "\"";
    } else {
      default_part = ". Default: " + flag.default_value;
    }

    std::string info_part = flag.description + type_part + default_part;

    printf("  %-*s", name_width, name_part.c_str());

    bool first_line = true;
    const int indent = 50;  // 2 spaces + 48 name_width
    size_t start = 0;
    while (start < info_part.length()) {
      size_t end = start + (max_width - indent);
      if (end >= info_part.length()) {
        end = info_part.length();
      } else {
        size_t space_pos = info_part.rfind(' ', end);
        if (space_pos != std::string::npos && space_pos > start) {
          end = space_pos;
        }
      }

      printf("%*s%.*s\n", first_line ? 0 : indent, "",
             static_cast<int>(end - start), info_part.c_str() + start);
      first_line = false;
      start = end;

      while (start < info_part.length() && info_part[start] == ' ') {
        start++;
      }
    }
  };

  auto has_visiable_options =
      [](const std::vector<OptionsRegistry::OptionAttribute> &options) {
        for (const auto &option : options) {
          if (!option.hidden && !option.experimental) {
            return true;
          }
        }
        return false;
      };

  auto all_options = OptionsRegistry::get_all_options();
  printf("Usage: ossfs2 mount [MOUNTPOINT] [OPTIONS]\n\n");
  printf("Positionals:\n  %-48sTarget directory to mount to. Type: %s.\n\n",
         "MOUNTPOINT", "string");
  printf("GeneralOptions:\n");
  printf("  %-48sConfigure file path. Type: %s. Default: %s\n", "-c,--conf",
         "string", "\"\"");

  for (const auto &option : all_options[0]) {
    print_one(option);
  }

  for (size_t i = 1; i < all_options.size(); i++) {
    if (!has_visiable_options(all_options[i])) continue;
    printf("\n%s:\n", OptionsRegistry::kCategoryNames[i].data());
    for (const auto &option : all_options[i]) {
      print_one(option);
    }
  }
}

int main(int argc, char *argv[]) {
  int r = 0;
  bool stats_continue = false;
  std::string mount_config = "", mountpoint, metrics_filter;
  bool show_version = false, show_mount_help = false;
  uint64_t pid = 0, interval = 1;
  CLI::App app("This is used to mount an OSS bucket to a local directory.");
  CLI::App *sub_mount = app.add_subcommand(
      "mount", "Mount an OSS bucket to the target directory.");
  CLI::App *sub_stats = app.add_subcommand("stats", "Manage metrics of ossfs2.")
                            ->group("");  // make it hidden
  CLI::App *sub_stats_set =
      sub_stats->add_subcommand("set", "Enable metrics of ossfs2.")
          ->group("Subcommands");
  CLI::App *sub_stats_show =
      sub_stats->add_subcommand("show", "Show metrics of ossfs2.")
          ->group("Subcommands");

  sub_mount->add_option("-c,--conf", mount_config, "Configure file path")
      ->check(CLI::ExistingFile);
  sub_mount->add_option("MOUNTPOINT", mountpoint, "Directory to mount to")
      ->check(CLI::ExistingDirectory);
  sub_mount->allow_extras();
  sub_mount->set_help_flag("--original-help",
                           "Print this help message and exit");
  sub_mount->add_flag("-h,--help", show_mount_help,
                      "Show help information for mount cmd");

  sub_stats_show->add_flag("-l,--live", stats_continue,
                           "Show metrics continuously");
  sub_stats_show->add_option("-p,--pid", pid, "Show metrics of specified pid");
  sub_stats_show->add_option("-i,--interval", interval,
                             "Interval in seconds to show metrics");

  sub_stats_set->add_option("-p,--pid", pid, "Enable metrics of specified pid");
  sub_stats_set->add_option(
      "-f,--filter", metrics_filter,
      "Enable metrics by filter (e.g. fs, oss)\n"
      "Set empty or not set will have io metrics enabled only");

  app.add_flag("-v,--version", show_version, "Show version");

  if (is_fstab_args(argc, argv)) {
    printf("Using 'fstab' style options.\n");
    auto args = parse_fstab(argc, argv);
    // We need to reverse the args for CLI11 parse std::vector<std::string>.
    std::reverse(args.begin(), args.end());
    CLI11_PARSE(app, args);
  } else {
    CLI11_PARSE(app, argc, argv);
  }

  if (show_version) {
    printf("Version ID: %s \n", MACRO_STR(OSSFS_VERSION_ID));
    printf("Commit  ID: %s \n", MACRO_STR(OSSFS_COMMIT_ID));
    printf("Build Time: %s \n", MACRO_STR(OSSFS_BUILD_TIME));
    return 0;
  }

  // TODO: should we throw an error if the flags are not be defined?
  // Currently, we ignore them. See more details in
  // https://github.com/gflags/gflags/issues/61
  gflags::SetCommandLineOption("flagfile", mount_config.c_str());

  if (sub_mount->parsed()) {
    if (show_mount_help) {
      do_show_mount_help();
      return 0;
    }

    auto opts = sub_mount->remaining();
    std::vector<char *> cmd_args;
    for (auto opt : opts) {
      cmd_args.push_back(strdup(opt.c_str()));
    }
    DEFER({
      for (auto opt : cmd_args) {
        free(opt);
      }
    });

    int fuse_argc = 2 + cmd_args.size();  // program + mountpath + others
    // fuse_args will be freed in fuse_opt_free_args(&args) below.
    char **fuse_args = (char **)malloc(fuse_argc * sizeof(char *));
    fuse_args[0] = argv[0];
    fuse_args[1] = const_cast<char *>(mountpoint.c_str());
    for (size_t i = 0; i < cmd_args.size(); i++) {
      fuse_args[i + 2] = cmd_args[i];
    }

    gflags::ParseCommandLineFlags(&fuse_argc, &fuse_args, true);
    trim_mount_options();

    struct fuse_cmdline_opts fuse_opts = {0};
    struct fuse_args args = FUSE_ARGS_INIT(fuse_argc, fuse_args);
    if (fuse_parse_cmdline(&args, &fuse_opts) != 0) return -1;

    g_log_to_stdout =
        FLAGS_log_dir == DEV_STDOUT ||
        (gflags::GetCommandLineFlagInfoOrDie("log_dir").is_default && FLAGS_f);

    if (validate_log_dir(mountpoint, FLAGS_log_dir) != 0) return -1;

    if (!FLAGS_nonempty) {
      struct dirent *ent;
      DIR *dh = opendir(mountpoint.c_str());
      if (dh == NULL) {
        fprintf(stderr, "ERROR: failed to open MOUNTPOINT: %s: %s\n",
                mountpoint.c_str(), strerror(errno));
        return -1;
      }
      while ((ent = readdir(dh)) != NULL) {
        if (strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0) {
          closedir(dh);
          fprintf(stderr, "ERROR: MOUNTPOINT directory %s is not empty.\n",
                  mountpoint.c_str());
          return -1;
        }
      }
      closedir(dh);
    }

    if (FLAGS_d) {
      fuse_opt_add_arg(&args, "-odebug");
      FLAGS_f = true;
    }

    if (FLAGS_ro) {
      fuse_opt_add_arg(&args, "-oro");
    }

    if (FLAGS_allow_other) {
      fuse_opt_add_arg(&args, "-oallow_other");
    }

    fuse_opt_add_arg(&args, "-odefault_permissions");

    int pipefd[2];
    if (pipe(pipefd)) return -1;
    daemonize(FLAGS_f, pipefd[0], log_exit_error);

    r = create_ossfs_and_run_fuse(args, fuse_opts, pipefd[1]);
    close(pipefd[0]);
    close(pipefd[1]);
    free(fuse_opts.mountpoint);
    fuse_opt_free_args(&args);

    if (FLAGS_f) log_exit_error(r);
  } else if (sub_stats->parsed()) {
    log_output_level = ALOG_FATAL;

    // process Ctrl+c for loop mode
    photon::block_all_signal();
    photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_NONE);
    DEFER(photon::fini());
    photon::sync_signal(SIGINT, [](int signum) { exit(0); });
    photon::sync_signal(SIGTERM, [](int signum) { exit(0); });

    if (sub_stats_set->parsed()) {
      return OssFileSystem::Admin::stats_set_command_handler(pid,
                                                             metrics_filter);
    } else if (sub_stats_show->parsed()) {
      return OssFileSystem::Admin::stats_show_command_handler(stats_continue,
                                                              pid, interval);
    } else {
      printf("%s", sub_stats->help().c_str());
      return -1;
    }
  } else {
    printf("%s", app.help().c_str());
    return -1;
  }
  return r;
}
