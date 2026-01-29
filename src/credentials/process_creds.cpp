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

#include "process_creds.h"

#include "common/logger.h"
#include "common/utils.h"

namespace OssFileSystem {

ProcessCredentialsProvider::ProcessCredentialsProvider(std::string_view cmd)
    : cmd_(cmd) {
  int r = split_command_tokens(cmd_, args_);
  if (r != 0) {
    LOG_ERROR("Failed to split command ` with r `", cmd_, r);
  }
}

int ProcessCredentialsProvider::get_credentials(OssCredentials &out_creds,
                                                time_t &expiration) {
  if (args_.empty()) return -EINVAL;

  std::string stdout_output, stderr_output;
  int r = run_process_safe(args_, stdout_output, stderr_output, 1048576, 10);
  if (r != 0) {
    LOG_ERROR("Failed to run command '`' with r `, stderr is `", cmd_, r,
              stderr_output);
    return r;
  }

  CredentialsParser::Result creds_map;
  r = CredentialsParser::from_json(stdout_output, creds_map);
  if (r != 0) {
    LOG_ERROR("failed to parse credential from cmd: '`', error: `", cmd_, r);
    return r;
  }

  LOG_INFO("Success to get credential, expiration: `",
           creds_map[CredentialsParser::kExpiration]);

  out_creds = {creds_map[CredentialsParser::kAccessKeyId],
               creds_map[CredentialsParser::kAccessKeySecret],
               creds_map[CredentialsParser::kSecurityToken]};
  if (creds_map[CredentialsParser::kExpiration].empty()) {
    expiration = -1;
  } else {
    expiration = CredentialsParser::expiration_to_time(
        creds_map[CredentialsParser::kExpiration]);
  }

  return 0;
}
};  // namespace OssFileSystem
