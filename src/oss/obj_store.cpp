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

#include "obj_store.h"

#include <string>

#include "oss_hdfs_store.h"
#include "oss_store.h"

namespace OssFileSystem {

static constexpr std::string_view kHdfsEndpointSuffix = "oss-dls.aliyuncs.com";

bool is_hdfs_endpoint(std::string_view endpoint) {
  if (endpoint.size() < kHdfsEndpointSuffix.size()) return false;
  return endpoint.substr(endpoint.size() - kHdfsEndpointSuffix.size()) ==
         kHdfsEndpointSuffix;
}

IObjStore *new_obj_store(const char *key, const char *key_secret,
                         const ObjStoreOptions &options) {
  if (is_hdfs_endpoint(options.endpoint)) {
    return new_oss_hdfs_store(key, key_secret, options);
  }
  return new_oss_store(key, key_secret, options);
}
}  // namespace OssFileSystem
