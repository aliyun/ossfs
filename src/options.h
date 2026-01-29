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

#include <gflags/gflags.h>

#include <array>
#include <map>
#include <set>
#include <string_view>

class OptionRegistrar;
class OptionsRegistry {
 public:
  enum OptionCategory {
    kGeneralOptions,
    kFileSystemOptions,
    kOssBucketOptions,
    kOssCredentialsOptions,
    kCachingOptions,
    kOssClientOptions,
    kLoggingOptions,
    kAdvancedOptions,
    kCount,
  };

  static const std::string_view kCategoryNames[];

  struct OptionAttribute {
    std::string name;
    bool hidden = false;
    bool experimental = false;
  };

  using ContainerType =
      std::array<std::vector<OptionAttribute>, OptionCategory::kCount>;

  // not thread safe
  static const ContainerType &get_all_options() {
    return options_;
  }

  const static std::set<std::string> kSensitiveOptions;

 private:
  static ContainerType options_;

  friend class OptionRegistrar;
};

class OptionRegistrar {
 public:
  OptionRegistrar(const std::string &name,
                  OptionsRegistry::OptionCategory category, bool hidden = false,
                  bool experimental = false) {
    OptionsRegistry::options_[category].push_back({name, hidden, experimental});
  }
};

#define DEFINE_OPTION(name, type, default_value, help_string, category,      \
                      hidden, experimental)                                  \
  DEFINE_##type(name, default_value, help_string);                           \
  namespace {                                                                \
  static OptionRegistrar o_##name(#name,                                     \
                                  OptionsRegistry::OptionCategory::category, \
                                  hidden, experimental);                     \
  }

// ==================== General options ====================
DECLARE_bool(d);
DECLARE_bool(f);
DECLARE_bool(nonempty);
DECLARE_string(total_mem_limit);
DECLARE_double(total_rw_mem_ratio);

// ==================== FileSystem Options ====================
DECLARE_bool(ro);
DECLARE_uint64(uid);
DECLARE_uint64(gid);
DECLARE_string(dir_mode);
DECLARE_string(file_mode);
DECLARE_int32(fuse_threads);
DECLARE_bool(readdirplus);
DECLARE_bool(ignore_fsync);
DECLARE_bool(allow_mark_dir_stale_recursively);
DECLARE_bool(allow_rename_dir);
DECLARE_uint64(rename_dir_limit);
DECLARE_bool(allow_other);
DECLARE_int32(seq_read_detect_count);
DECLARE_bool(sync_upload);
DECLARE_bool(enable_symlink);

// ==================== Oss Bucket Options ====================
DECLARE_string(oss_endpoint);
DECLARE_string(oss_bucket);
DECLARE_string(oss_bucket_prefix);
DECLARE_string(oss_region);

// ==================== Oss Credential Options ====================
DECLARE_string(oss_access_key_id);
DECLARE_string(oss_access_key_secret);
DECLARE_string(ram_role);
DECLARE_string(credential_process);

// ==================== Caching options ====================
DECLARE_uint64(attr_timeout);
DECLARE_uint64(negative_timeout);
DECLARE_int64(fuse_attr_timeout);
DECLARE_int64(fuse_entry_timeout);
DECLARE_bool(close_to_open);
DECLARE_uint64(inode_cache_eviction_threshold);
DECLARE_uint64(inode_cache_eviction_interval_ms);
DECLARE_uint64(max_inode_cache_count);
DECLARE_uint64(oss_negative_cache_timeout);
DECLARE_uint64(oss_negative_cache_size);
DECLARE_string(cache_type);
DECLARE_string(memory_data_cache_size);

// ==================== Oss Client Options ====================
DECLARE_string(upload_buffer_size);
DECLARE_int32(upload_concurrency);
DECLARE_int32(upload_copy_concurrency);
DECLARE_int32(prefetch_concurrency);
DECLARE_int32(prefetch_concurrency_per_file);
DECLARE_string(prefetch_chunk_size);
DECLARE_int32(prefetch_chunks);
DECLARE_uint64(min_reserved_buffer_size_per_file);
DECLARE_bool(enable_appendable_object);
DECLARE_uint64(appendable_object_autoswitch_threshold);
DECLARE_bool(enable_crc64);
DECLARE_uint64(oss_vcpu_count);
DECLARE_bool(use_list_obj_v2);
DECLARE_int32(max_list_ret_count);
DECLARE_uint64(oss_request_timeout_ms);
DECLARE_string(bind_ips);
DECLARE_bool(set_mime_for_rename_dst);
DECLARE_bool(use_auth_cache);
DECLARE_int32(rename_dir_concurrency);
DECLARE_bool(enable_transmission_control);
DECLARE_uint64(tc_max_latency_threshold_us);
DECLARE_string(http_proxy);

// ==================== Logging options ====================
DECLARE_string(log_dir);
DECLARE_string(log_level);
DECLARE_uint64(log_file_max_size);
DECLARE_uint64(log_file_max_count);

// ==================== Advanced options ====================
DECLARE_bool(enable_photon_logs);
DECLARE_bool(skip_trim_options);
DECLARE_int32(metrics_port);
DECLARE_string(metrics_ip);
DECLARE_bool(enable_test_signal_handler);
DECLARE_bool(enable_admin_server);
