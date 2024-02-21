/*
 * ossfs -  FUSE-based file system backed by Alibaba Cloud OSS
 *
 * Copyright(C) 2007 Takeshi Nakatani <ggtakec.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <sys/types.h>
#include <string>

//-------------------------------------------------------------------
// Global variables
//-------------------------------------------------------------------
bool foreground                   = false;
bool nomultipart                  = false;
bool pathrequeststyle             = false;
bool complement_stat              = false;
bool noxmlns                      = true;
bool direct_read                  = false;

int direct_read_max_prefetch_thread_count = 64;

std::string program_name;
std::string service_path          = "/";
std::string s3host                = "https://oss-cn-hangzhou.aliyuncs.com";
std::string endpoint              = "cn-hangzhou";
std::string cipher_suites;
std::string instance_name;

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
