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

#include "ram_role_creds.h"

#include <photon/common/estring.h>
#include <photon/net/http/client.h>
#include <photon/thread/thread.h>

#include <sstream>

#include "common/logger.h"
#include "common/macros.h"
#include "common/utils.h"

namespace OssFileSystem {

using Verb = photon::net::http::Verb;

std::string_view kECSMetadataTokenUrl =
    "http://100.100.100.200/latest/api/token";
std::string_view kECSMetadataTokenHeaderTTL =
    "X-aliyun-ecs-metadata-token-ttl-seconds";
std::string_view kECSMetadataTokenHeader = "X-aliyun-ecs-metadata-token";
std::string_view kECSMetadataTokenDefaultTTL = "180";
std::string_view kRamUrlBase =
    "http://100.100.100.200/latest/meta-data/ram/security-credentials/";

static int get_ecs_meta(std::string_view url, std::string &resp) {
  RELEASE_ASSERT(photon::CURRENT);

  auto client = photon::net::http::new_http_client();
  DEFER(delete client);

  client->timeout(10000000);  // 10s

  std::string token;

  // Get ECS metadata token.
  {
    auto op = client->new_operation(Verb::PUT, kECSMetadataTokenUrl);
    DEFER(op->destroy());

    op->req.headers.insert(kECSMetadataTokenHeaderTTL,
                           kECSMetadataTokenDefaultTTL);

    if (op->call() != 0) {
      int saved_errno = errno;
      LOG_ERROR("failed to get ecs metadata token: `, error: `",
                kECSMetadataTokenUrl, saved_errno);
      return -saved_errno;
    }

    if (op->status_code == 200) {
      token.resize(op->resp.headers.content_length());
      auto ret = op->resp.read(&token[0], token.size());
      if (ret != static_cast<ssize_t>(token.size())) {
        LOG_ERROR("failed to get ecs metadata token: `, error: `",
                  kECSMetadataTokenUrl, errno);
        return -EIO;
      }
    } else {
      LOG_WARN("failed to get ecs metadata token: `, error: `",
               kECSMetadataTokenUrl, op->status_code);
    }
  }

  // Get ECS metadata.
  {
    auto op = client->new_operation(Verb::GET, url);
    DEFER(op->destroy());

    if (!token.empty()) {
      op->req.headers.insert(kECSMetadataTokenHeader, token);
    }

    if (op->call() != 0) {
      int saved_errno = errno;
      LOG_ERROR("failed to get ecs metadata: `, error: `", url, saved_errno);
      return -saved_errno;
    }

    if (op->status_code != 200) {
      LOG_ERROR("failed to get ecs metadata: `, status: `", url,
                op->status_code);
      return -EIO;
    }

    resp.clear();
    resp.resize(op->resp.headers.content_length());
    auto ret = op->resp.read(&resp[0], resp.size());
    if (ret != static_cast<ssize_t>(resp.size())) {
      LOG_ERROR("failed to read ecs metadata: `, error: `", url, errno);
      return -EIO;
    }
  }

  return 0;
}

RamRoleCredentialsProvider::RamRoleCredentialsProvider(
    std::string_view ram_role) {
  estring_view esv(ram_role);
  if (esv.starts_with("http://")) {
    url_ = std::string(ram_role);
  } else {
    url_ = kRamUrlBase + std::string(ram_role);
  }
}

int RamRoleCredentialsProvider::get_credentials(OssCredentials &out_creds,
                                                time_t &expiration) {
  std::string response;
  int r = get_ecs_meta(url_, response);
  if (r != 0) {
    return r;
  }

  RamCredMap creds_map;
  r = CredentialsParser::from_json(response, creds_map);
  if (r != 0) {
    LOG_ERROR("failed to parse ram role: `, error: `", url_, r);
    return r;
  }

  LOG_INFO("Success to get ram role creds, expiration: `",
           creds_map[CredentialsParser::kExpiration]);

  out_creds = {creds_map[CredentialsParser::kAccessKeyId],
               creds_map[CredentialsParser::kAccessKeySecret],
               creds_map[CredentialsParser::kSecurityToken]};
  expiration = CredentialsParser::expiration_to_time(
      creds_map[CredentialsParser::kExpiration]);
  return 0;
}

};  // namespace OssFileSystem
