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

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace SSLProbe {

inline bool setup_ssl_env() {
  const char *env_name = "SSL_CERT_FILE";
  const char *current_val = std::getenv(env_name);

  if (current_val) {
    std::filesystem::path p(current_val);
    std::error_code ec;
    if (std::filesystem::is_regular_file(p, ec) && !ec) {
      return true;
    }
  }

  static const std::vector<std::string_view> paths = {
      "/etc/ssl/certs/ca-certificates.crt",     // Debian, Ubuntu, Gentoo
      "/etc/pki/tls/certs/ca-bundle.crt",       // RHEL, CentOS, Fedora
      "/etc/ssl/ca-bundle.pem",                 // OpenSUSE
      "/etc/pki/tls/cacert.pem",                // RHEL/CentOS
      "/etc/ssl/cert.pem",                      // Alpine Linux, Arch Linux
      "/var/lib/ca-certificates/ca-bundle.pem"  // OpenSUSE Tumbleweed
  };

  for (const auto path_str : paths) {
    std::filesystem::path p(path_str);
    std::error_code ec;

    if (std::filesystem::is_regular_file(p, ec) && !ec) {
      if (setenv(env_name, p.string().c_str(), 1) == 0) {
        return true;
      } else {
        return false;
      }
    }
  }

  return false;
}

}  // namespace SSLProbe
