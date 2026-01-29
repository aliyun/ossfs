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

#include "options.h"

#include <limits>

#include "common/utils.h"

OptionsRegistry::ContainerType OptionsRegistry::options_;
const std::set<std::string> OptionsRegistry::kSensitiveOptions = {
    "oss_access_key_id",
    "oss_access_key_secret",
};

const std::string_view OptionsRegistry::kCategoryNames[] = {
    "GeneralOptions",        "FileSystemOptions", "OssBucketOptions",
    "OssCredentialsOptions", "CachingOptions",    "OssClientOptions",
    "LoggingOptions",        "AdvancedOptions",
};

static_assert(sizeof(OptionsRegistry::kCategoryNames) /
                      sizeof(std::string_view) ==
                  OptionsRegistry::OptionCategory::kCount,
              "kCategoryNames size mismatch");

// ==================== General options ====================
DEFINE_OPTION(d, bool, false, "Run ossfs2 in foreground debugging mode",
              kGeneralOptions, false, false);
DEFINE_OPTION(f, bool, false, "Run ossfs2 in the foreground mode",
              kGeneralOptions, false, false);
DEFINE_OPTION(nonempty, bool, false, "Enable mounting to a non-empty directory",
              kGeneralOptions, true, false);
DEFINE_OPTION(total_mem_limit, string, "0",
              "The total memory soft limit. 0 means no limit", kGeneralOptions,
              false, false);

static bool validate_bytes_string(const char *flagname,
                                  const std::string &value) {
  return parse_bytes_string(value).has_value();
}
DEFINE_validator(total_mem_limit, &validate_bytes_string);

DEFINE_OPTION(total_rw_mem_ratio, double, 0.6,
              "The ratio of total memory allocated for read/write operations",
              kGeneralOptions, true, true);
static bool validate_total_rw_mem_ratio(const char *flagname, double value) {
  return value > 0 && value < 1.0;
}
DEFINE_validator(total_rw_mem_ratio, &validate_total_rw_mem_ratio);

// ==================== FileSystem Options ====================
DEFINE_OPTION(ro, bool, false, "Run ossfs2 in read-only mode",
              kFileSystemOptions, false, false);

DEFINE_OPTION(uid, uint64, 0, "Owner user UID", kFileSystemOptions, false,
              false);
static bool validate_gid_uid(const char *flagname, uint64_t value) {
  return value >= 0 && value <= std::numeric_limits<uint32_t>::max();
}
DEFINE_validator(uid, &validate_gid_uid);

DEFINE_OPTION(gid, uint64, 0, "Owner group GID", kFileSystemOptions, false,
              false);
DEFINE_validator(gid, &validate_gid_uid);

DEFINE_OPTION(dir_mode, string, "0777", "Directory permissions(octal format)",
              kFileSystemOptions, false, false);
static bool validate_mode(const char *flagname, const std::string &value) {
  bool flag = true;
  if (value.size() != 4) {
    flag = false;
  } else if (value.size() == 4 && value[0] != '0') {
    flag = false;
  } else {
    for (auto &ch : value) {
      if (ch >= '8' || ch < '0') {
        flag = false;
        break;
      }
    }
  }
  return flag;
}
DEFINE_validator(dir_mode, &validate_mode);

DEFINE_OPTION(file_mode, string, "0777", "File permissions(octal format)",
              kFileSystemOptions, false, false);
DEFINE_validator(file_mode, &validate_mode);

DEFINE_OPTION(fuse_threads, int32, 128, "The number of FUSE worker threads",
              kFileSystemOptions, false, true);
static bool validate_fuse_threads(const char *flagname, int32_t value) {
  return value >= 1 && value <= 1024;
}
DEFINE_validator(fuse_threads, &validate_fuse_threads);

DEFINE_OPTION(readdirplus, bool, true, "Enable FUSE readdirplus optimization",
              kFileSystemOptions, false, false);
DEFINE_OPTION(ignore_fsync, bool, true, "Ignore FUSE fsync requests",
              kFileSystemOptions, true, true);
DEFINE_OPTION(allow_mark_dir_stale_recursively, bool, false,
              "Allow marking directory cache as stale recursively",
              kFileSystemOptions, true, true);
DEFINE_OPTION(allow_rename_dir, bool, true, "Allow renaming directories",
              kFileSystemOptions, true, true);
DEFINE_OPTION(
    rename_dir_limit, uint64, 2000000,
    "The maximum number of entries during a single rename-dir operation",
    kFileSystemOptions, false, false);

DEFINE_OPTION(allow_other, bool, true,
              "Allow other users, including root, to access the filesystem",
              kFileSystemOptions, false, false);

DEFINE_OPTION(seq_read_detect_count, int32, 3,
              "The number of sequential reads required to trigger prefetching",
              kFileSystemOptions, false, true);

DEFINE_OPTION(enable_symlink, bool, false, "Enable symlink support",
              kFileSystemOptions, false, false);

static bool validate_seq_read_detect_count(const char *flagname,
                                           int32_t value) {
  return value >= 0 && value <= 64;
}
DEFINE_validator(seq_read_detect_count, &validate_seq_read_detect_count);

// ==================== Oss Bucket Options ====================
DEFINE_OPTION(oss_endpoint, string, "", "OSS endpoint", kOssBucketOptions,
              false, false);
static bool validate_nonempty_string(const char *flagname,
                                     const std::string &value) {
  return !value.empty();
}
DEFINE_validator(oss_endpoint, &validate_nonempty_string);

DEFINE_OPTION(oss_bucket, string, "", "OSS bucket name", kOssBucketOptions,
              false, false);
DEFINE_validator(oss_bucket, &validate_nonempty_string);

DEFINE_OPTION(oss_bucket_prefix, string, "", "OSS bucket prefix path",
              kOssBucketOptions, false, false);
DEFINE_OPTION(oss_region, string, "",
              "OSS region ID. Used with the OSS signature v4",
              kOssBucketOptions, false, false);

// ==================== Oss Credential Options ====================
DEFINE_OPTION(oss_access_key_id, string, "", "OSS access key ID",
              kOssCredentialsOptions, false, false);
DEFINE_OPTION(oss_access_key_secret, string, "", "OSS access key secret",
              kOssCredentialsOptions, false, false);

DEFINE_OPTION(ram_role, string, "", "RAM role name", kOssCredentialsOptions,
              false, false);

DEFINE_OPTION(credential_process, string, "",
              "External credential process command", kOssCredentialsOptions,
              false, false);

static bool validate_credential_process(const char *flagname,
                                        const std::string &value) {
  if (value.empty()) return true;

  // should be absolute path
  if (value[0] != '/') {
    return false;
  }

  // should not contain $, reject environment variables
  if (value.find('$') != std::string::npos) {
    return false;
  }

  return true;
}

DEFINE_validator(credential_process, &validate_credential_process);

// ==================== Caching options ====================
DEFINE_OPTION(attr_timeout, uint64, 60, "Attribute cache timeout in seconds",
              kCachingOptions, false, false);
DEFINE_OPTION(negative_timeout, uint64, 0, "Negative cache timeout in seconds",
              kCachingOptions, false, false);

DEFINE_OPTION(fuse_attr_timeout, int64, -1,
              "FUSE attribute cache timeout in seconds. -1 means to set it the "
              "same as attr_timeout",
              kCachingOptions, false, true);
static bool validate_fuse_attr_timeout(const char *flagname, int64_t value) {
  return value == -1 || value >= 0;
}
DEFINE_validator(fuse_attr_timeout, &validate_fuse_attr_timeout);

DEFINE_OPTION(fuse_entry_timeout, int64, -1,
              "FUSE entry cache timeout in seconds. -1 means to set it the "
              "same as attr_timeout",
              kCachingOptions, false, true);
DEFINE_validator(fuse_entry_timeout, &validate_fuse_attr_timeout);

DEFINE_OPTION(close_to_open, bool, false, "Enable close-to-open consistency",
              kCachingOptions, false, false);

DEFINE_OPTION(inode_cache_eviction_threshold, uint64, 0,
              "Inode cache eviction threshold. 0 means no eviction",
              kCachingOptions, false, true);
DEFINE_OPTION(inode_cache_eviction_interval_ms, uint64, 30000,
              "Inode cache eviction interval in milliseconds", kCachingOptions,
              false, true);
DEFINE_OPTION(max_inode_cache_count, uint64, 0,
              "Maximum number of cached inodes", kCachingOptions, true, true);

// oss_negative_cache_timeout should be smaller than attr_timeout
DEFINE_OPTION(oss_negative_cache_timeout, uint64, 0,
              "OSS negative cache timeout in seconds", kCachingOptions, false,
              false);
// 10000 entries cost at most about 20 MiB (assume fullpath's length is 1023)
DEFINE_OPTION(oss_negative_cache_size, uint64, 10000,
              "The maximum number of entries in the OSS negative cache",
              kCachingOptions, false, false);

DEFINE_OPTION(cache_type, string, "standard",
              "Cache type. Valid values: standard", kCachingOptions, true,
              true);
static bool validate_cache_type(const char *flagname,
                                const std::string &value) {
  return value == "standard";
}
DEFINE_validator(cache_type, &validate_cache_type);

DEFINE_OPTION(
    memory_data_cache_size, string, "0",
    "Memory data cache size. This mode is Optimized for AI model loading. It "
    "speeds up file reads via concurrent access from multiple file "
    "descriptors. NOTICE: Currently cache is released once all file "
    "descriptors are closed",
    kCachingOptions, false, false);

DEFINE_validator(memory_data_cache_size, &validate_bytes_string);

// ==================== Oss Client Options ====================
DEFINE_OPTION(upload_buffer_size, string, "8388608", "Upload buffer size",
              kOssClientOptions, false, false);
static bool validate_upload_buffer_size(const char *flagname,
                                        const std::string &value) {
  auto size = parse_bytes_string(value);
  if (!size.has_value()) return false;
  return size.value() >= 1048576 && size.value() <= 5ULL * 1024 * 1024 * 1024;
}
DEFINE_validator(upload_buffer_size, &validate_upload_buffer_size);

DEFINE_OPTION(upload_concurrency, int32, 64, "Concurrency for multipart upload",
              kOssClientOptions, false, false);
static bool validate_upload_concurrency(const char *flagname, int32_t value) {
  return value >= 1 && value <= 1024;
}
DEFINE_validator(upload_concurrency, &validate_upload_concurrency);

DEFINE_OPTION(upload_copy_concurrency, int32, 64,
              "Concurrency for multipart copy", kOssClientOptions, false,
              false);
DEFINE_validator(upload_copy_concurrency, &validate_upload_concurrency);

DEFINE_OPTION(prefetch_concurrency, int32, 256, "Global prefetch concurrency",
              kOssClientOptions, false, false);
static bool validate_prefetch_concurrency(const char *flagname, int32_t value) {
  return value >= 0 && value <= 4096;
}
DEFINE_validator(prefetch_concurrency, &validate_prefetch_concurrency);

DEFINE_OPTION(prefetch_concurrency_per_file, int32, 64,
              "Prefetch concurrency per file", kOssClientOptions, false, false);
static bool validate_prefetch_concurrency_per_file(const char *flagname,
                                                   int32_t value) {
  return value >= 1 && value <= 4096;
}
DEFINE_validator(prefetch_concurrency_per_file,
                 &validate_prefetch_concurrency_per_file);

DEFINE_OPTION(prefetch_chunk_size, string, "8388608",
              "Size of each Prefetch chunk", kOssClientOptions, false, false);
static bool validate_prefetch_chunk_size(const char *flagname,
                                         const std::string &value) {
  auto size = parse_bytes_string(value);
  if (!size.has_value()) return false;
  return size.value() >= 1048576 && size.value() <= 128ULL * 1024 * 1024;
}
DEFINE_validator(prefetch_chunk_size, &validate_prefetch_chunk_size);

DEFINE_OPTION(prefetch_chunks, int32, 0,
              "The total prefetch chunk count. 0 means "
              "auto(prefetch_concurrency * 3), -1 means unlimited",
              kOssClientOptions, false, false);
static bool validate_prefetch_chunks(const char *flagname, int32_t value) {
  return value >= -1;
}
DEFINE_validator(prefetch_chunks, &validate_prefetch_chunks);

DEFINE_OPTION(min_reserved_buffer_size_per_file, uint64, 1048576,
              "The minimum reserved buffer size per file", kOssClientOptions,
              false, true);
static bool validate_min_reserved_buffer_size_per_file(const char *flagname,
                                                       uint64_t value) {
  return value == 0 || value == 1048576;
}
DEFINE_validator(min_reserved_buffer_size_per_file,
                 &validate_min_reserved_buffer_size_per_file);

DEFINE_OPTION(enable_appendable_object, bool, false,
              "Using appendable object for upload with AppendObject API",
              kOssClientOptions, false, false);
DEFINE_OPTION(appendable_object_autoswitch_threshold, uint64, 0,
              "Threshold for automatically switching to appendable object",
              kOssClientOptions, false, false);
static bool validate_appendable_object_autoswitch_threshold(
    const char *flagname, uint64_t value) {
  return value >= 0 && value <= 5ULL * 1024 * 1024 * 1024;
}
DEFINE_validator(appendable_object_autoswitch_threshold,
                 &validate_appendable_object_autoswitch_threshold);

DEFINE_OPTION(sync_upload, bool, true,
              "Synchronized uploading before file closing", kFileSystemOptions,
              true, false);

DEFINE_OPTION(enable_crc64, bool, true,
              "Enable CRC64 checksum verification when uploading files",
              kOssClientOptions, false, false);

DEFINE_OPTION(oss_vcpu_count, uint64, 8, "The number of OSS background vCPUs",
              kOssClientOptions, false, true);
static bool validate_oss_vcpu_count(const char *flagname, uint64_t value) {
  return value >= 1 && value <= 128;
}
DEFINE_validator(oss_vcpu_count, &validate_oss_vcpu_count);

DEFINE_OPTION(use_list_obj_v2, bool, true, "Use ListObject API v2",
              kOssClientOptions, true, true);
DEFINE_OPTION(
    max_list_ret_count, int32, 100,
    "The maximum number of the items returned by a single ListObject request",
    kOssClientOptions, true, true);
static bool validate_max_list_ret_count(const char *flagname, int32_t value) {
  return value >= 100 && value <= 1000;
}
DEFINE_validator(max_list_ret_count, &validate_max_list_ret_count);

DEFINE_OPTION(oss_request_timeout_ms, uint64, 60000,
              "OSS request timeout in milliseconds", kOssClientOptions, false,
              false);
static bool validate_oss_request_timeout_ms(const char *flagname,
                                            uint64_t value) {
  // [1s, 900s]
  return value >= 1000 && value <= 900000;
}
DEFINE_validator(oss_request_timeout_ms, &validate_oss_request_timeout_ms);

DEFINE_OPTION(bind_ips, string, "",
              "Comma-separated list of IPs to bind(e.g. 127.0.0.1,127.0.0.2)",
              kOssClientOptions, false, false);
DEFINE_OPTION(set_mime_for_rename_dst, bool, false,
              "Set MIME type for rename-object requests", kOssClientOptions,
              true, true);
DEFINE_OPTION(use_auth_cache, bool, false, "Use OSS authentication cache",
              kOssClientOptions, true, true);

DEFINE_OPTION(rename_dir_concurrency, int32, 128,
              "Concurrency for directory rename operations", kOssClientOptions,
              true, true);
static bool validate_rename_dir_concurrency(const char *flagname,
                                            int32_t value) {
  return value >= 1 && value <= 1024;
}
DEFINE_validator(rename_dir_concurrency, &validate_rename_dir_concurrency);

DEFINE_OPTION(enable_transmission_control, bool, true,
              "Enable network transmission control", kOssClientOptions, false,
              true);
DEFINE_OPTION(tc_max_latency_threshold_us, uint64, 5000000,
              "Maximum allowed latency threshold for transmission control in "
              "microseconds",
              kOssClientOptions, false, true);

DEFINE_OPTION(http_proxy, string, "", "The HTTP proxy to use",
              kOssClientOptions, true, true);

// ==================== Logging options ====================
DEFINE_OPTION(log_dir, string, "/tmp/ossfs2/", "The directory for log files",
              kLoggingOptions, false, false);
DEFINE_OPTION(log_level, string, "info",
              "The log level. Valid values: info and debug", kLoggingOptions,
              false, false);
DEFINE_OPTION(log_file_max_size, uint64, 64 * 1024 * 1024,
              "Maximum size of a single log file", kLoggingOptions, false,
              false);
DEFINE_OPTION(log_file_max_count, uint64, 8,
              "The maximum count of log files to keep", kLoggingOptions, false,
              false);

// ==================== Advanced options ====================
DEFINE_OPTION(enable_photon_logs, bool, false, "Enable photon logs",
              kAdvancedOptions, true, true);
DEFINE_OPTION(skip_trim_options, bool, false, "Skip trim options",
              kAdvancedOptions, true, true);

DEFINE_OPTION(
    metrics_port, int32, 0,
    "HTTP server port number for metrics. 0 means not to start the HTTP server",
    kAdvancedOptions, true, true);
static bool validate_http_server_port(const char *flagname, int32_t value) {
  return value >= 0 && value <= 65535;
}
DEFINE_validator(metrics_port, &validate_http_server_port);

DEFINE_OPTION(metrics_ip, string, "127.0.0.1",
              "The IP address to expose the metrics server on",
              kAdvancedOptions, true, true);

DEFINE_OPTION(enable_test_signal_handler, bool, false,
              "Enable test signal handler for SIGUSR1", kAdvancedOptions, true,
              true);

DEFINE_OPTION(enable_admin_server, bool, true, "Enable admin server",
              kAdvancedOptions, true, true);
