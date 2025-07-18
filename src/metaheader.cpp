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

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>

#include "common.h"
#include "s3fs.h"
#include "metaheader.h"
#include "string_util.h"

static const struct timespec DEFAULT_TIMESPEC = {-1, 0};
static bool simulate_mtime_ns_with_crc64 = false;

void set_simulate_mtime_ns_with_crc64(bool simulate)
{
    simulate_mtime_ns_with_crc64 = simulate;
}

//-------------------------------------------------------------------
// Utility functions for convert
//-------------------------------------------------------------------
static struct timespec cvt_string_to_time(const char *str)
{
    // [NOTE]
    // In rclone, there are cases where ns is set to x-oss-meta-mtime
    // with floating point number. ossfs uses x-oss-meta-mtime by
    // truncating the floating point or less (in seconds or less) to
    // correspond to this.
    //
    std::string strmtime;
    long nsec = 0;
    if(str && '\0' != *str){
        strmtime = str;
        std::string::size_type pos = strmtime.find('.', 0);
        if(std::string::npos != pos){
            nsec = cvt_strtoofft(strmtime.substr(pos + 1).c_str(), /*base=*/ 10);
            strmtime.erase(pos);
        }
    }
    struct timespec ts = {static_cast<time_t>(cvt_strtoofft(strmtime.c_str(), /*base=*/ 10)), nsec};
    return ts;
}

static struct timespec get_time(const headers_t& meta, const char *header)
{
    headers_t::const_iterator iter;
    if(meta.end() == (iter = meta.find(header))){
        return DEFAULT_TIMESPEC;
    }
    return cvt_string_to_time((*iter).second.c_str());
}

// There is an option auto_cache in LibFuse, which decides whether to keep the pagecache
// according to the mtime (both sec and nsec). It is recommended to turn on 
// simulate_mtime_ns_with_crc64 when using auto_cache.
// We don't control whether to keep pagecache by ossfs rather than LibFuse with Etag,
// because Etag is stored in the StatCache, whose lifecycle is not consistent with
// the file. We might not be able to get the local ETag when open(). 
struct timespec get_mtime(const headers_t& meta, bool overcheck, bool noextendedmeta)
{
    #define SET_TV_NSEC_IF_NEEDED(t) do { \
        if (simulate_mtime_ns_with_crc64 && 0 == t.tv_nsec) { \
            const unsigned long long MAX_NSEC = 1000000000; \
            unsigned long long crc64 = get_crc64(meta); \
            if (crc64 >= MAX_NSEC) { \
                crc64 = crc64 % MAX_NSEC; \
            } \
            t.tv_nsec = crc64; \
        } \
    } while (0)

    if (!noextendedmeta) {
        struct timespec t = get_time(meta, "x-oss-meta-mtime");
        if(0 < t.tv_sec){
            SET_TV_NSEC_IF_NEEDED(t);
            return t;
        }
        t = get_time(meta, "x-oss-meta-goog-reserved-file-mtime");
        if(0 < t.tv_sec){
            SET_TV_NSEC_IF_NEEDED(t);
            return t;
        }
    }
    if(overcheck){
        struct timespec ts = {get_lastmodified(meta), 0};
        SET_TV_NSEC_IF_NEEDED(ts);
        return ts;
    }

    struct timespec t = DEFAULT_TIMESPEC;
    SET_TV_NSEC_IF_NEEDED(t);
    return t;
}

struct timespec get_ctime(const headers_t& meta, bool overcheck, bool noextendedmeta)
{
    if (!noextendedmeta) {
        struct timespec t = get_time(meta, "x-oss-meta-ctime");
        if(0 < t.tv_sec){
            return t;
        }
    }
    if(overcheck){
        struct timespec ts = {get_lastmodified(meta), 0};
        return ts;
    }
    return DEFAULT_TIMESPEC;
}

struct timespec get_atime(const headers_t& meta, bool overcheck, bool noextendedmeta)
{
    if (!noextendedmeta) {
        struct timespec t = get_time(meta, "x-oss-meta-atime");
        if(0 < t.tv_sec){
            return t;
        }
    }
    if(overcheck){
        struct timespec ts = {get_lastmodified(meta), 0};
        return ts;
    }
    return DEFAULT_TIMESPEC;
}

unsigned long long get_crc64(const headers_t& meta)
{
    headers_t::const_iterator iter = meta.find("x-oss-hash-crc64ecma");
    if(meta.end() == iter){
        return 0;
    }
    return cvt_strtoull((*iter).second.c_str(), /*base=*/ 10);
}


off_t get_size(const char *s)
{
    return cvt_strtoofft(s, /*base=*/ 10);
}

off_t get_size(const headers_t& meta)
{
    headers_t::const_iterator iter = meta.find("Content-Length");
    if(meta.end() == iter){
        return 0;
    }
    return get_size((*iter).second.c_str());
}

mode_t get_mode(const char *s, int base)
{
    return static_cast<mode_t>(cvt_strtoofft(s, base));
}

mode_t get_mode(const headers_t& meta, const std::string& strpath, bool checkdir, bool forcedir, bool noextendedmeta)
{
    mode_t mode     = 0;
    bool   isS3sync = false;
    headers_t::const_iterator iter;

    if(noextendedmeta){
        //get file type from mode
        if(meta.end() != (iter = meta.find("x-oss-meta-mode"))){
            mode = get_mode((*iter).second.c_str());
        }
        // remove permissions
        mode &= S_IFMT;
        // set permissions to default
        if(mode & S_IFMT){
            if(S_ISLNK(mode)){
                mode |= 0777;
            }else if(S_ISDIR(mode)){
                mode |= 0750;
            }else if(S_ISCHR(mode) || S_ISBLK(mode)){
                mode |= 0644;
            }else{
                mode |= 0640;
            }
        }else{
            mode = (!strpath.empty() && '/' == *strpath.rbegin()) ? 0750 : 0640;
        }
    }else if(meta.end() != (iter = meta.find("x-oss-meta-mode"))){
        mode = get_mode((*iter).second.c_str());
    }else if(meta.end() != (iter = meta.find("x-oss-meta-permissions"))){ // for s3sync
        mode = get_mode((*iter).second.c_str());
        isS3sync = true;
    }else if(meta.end() != (iter = meta.find("x-oss-meta-goog-reserved-posix-mode"))){ // for GCS
        mode = get_mode((*iter).second.c_str(), 8);
    }else{
        // If another tool creates an object without permissions, default to owner
        // read-write and group readable.
        mode = (!strpath.empty() && '/' == *strpath.rbegin()) ? 0750 : 0640;
    }

    // Checking the new ossfs symlink format
    if(!(mode & S_IFMT)){
        if(meta.end() != meta.find("x-oss-meta-symlink-target")) {
            mode |= (S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO);
        }
    }

    // Checking the bitmask, if the last 3 bits are all zero then process as a regular
    // file type (S_IFDIR or S_IFREG), otherwise return mode unmodified so that S_IFIFO,
    // S_IFSOCK, S_IFCHR, S_IFLNK and S_IFBLK devices can be processed properly by fuse.
    if(!(mode & S_IFMT)){
        if(!isS3sync){
            if(checkdir){
                if(forcedir){
                    mode |= S_IFDIR;
                }else{
                    if(meta.end() != (iter = meta.find("Content-Type"))){
                        std::string strConType = (*iter).second;
                        // Leave just the mime type, remove any optional parameters (eg charset)
                        std::string::size_type pos = strConType.find(';');
                        if(std::string::npos != pos){
                            strConType.erase(pos);
                        }
                        if(strConType == "application/x-directory" || strConType == "httpd/unix-directory"){
                            // Nextcloud uses this MIME type for directory objects when mounting bucket as external Storage
                            mode |= S_IFDIR;
                        }else if(!strpath.empty() && '/' == *strpath.rbegin()){
                            // If complement lack stat mode, when the object has '/' character at end of name
                            // it should be directory. This follows ossfs's behavior.
                            if(complement_stat || strConType == "binary/octet-stream" || strConType == "application/octet-stream"){
                                mode |= S_IFDIR;
                            }else{
                                mode |= S_IFREG;
                            }
                        }else{
                            mode |= S_IFREG;
                        }
                    }else{
                      mode |= S_IFREG;
                    }
                }
            }
            // If complement lack stat mode, when it's mode is not set any permission,
            // the object is added minimal mode only for read permission.
            if(complement_stat && 0 == (mode & (S_IRWXU | S_IRWXG | S_IRWXO))){
                mode |= (S_IRUSR | (0 == (mode & S_IFDIR) ? 0 : S_IXUSR));
            }
        }else{
            if(!checkdir){
                // cut dir/reg flag.
                mode &= ~S_IFDIR;
                mode &= ~S_IFREG;
            }
        }
    }
    return mode;
}

uid_t get_uid(const char *s)
{
    return static_cast<uid_t>(cvt_strtoofft(s, /*base=*/ 0));
}

uid_t get_uid(const headers_t& meta, bool nometa)
{
    headers_t::const_iterator iter;
    if (nometa) {
        return geteuid();
    }else if(meta.end() != (iter = meta.find("x-oss-meta-uid"))){
        return get_uid((*iter).second.c_str());
    }else if(meta.end() != (iter = meta.find("x-oss-meta-owner"))){ // for s3sync
        return get_uid((*iter).second.c_str());
    }else if(meta.end() != (iter = meta.find("x-oss-meta-goog-reserved-posix-uid"))){ // for GCS
        return get_uid((*iter).second.c_str());
    }else{
        return geteuid();
    }
}

gid_t get_gid(const char *s)
{
    return static_cast<gid_t>(cvt_strtoofft(s, /*base=*/ 0));
}

gid_t get_gid(const headers_t& meta, bool nometa)
{
    headers_t::const_iterator iter;
    if (nometa) {
        return getegid();
    }else if(meta.end() != (iter = meta.find("x-oss-meta-gid"))){
        return get_gid((*iter).second.c_str());
    }else if(meta.end() != (iter = meta.find("x-oss-meta-group"))){ // for s3sync
        return get_gid((*iter).second.c_str());
    }else if(meta.end() != (iter = meta.find("x-oss-meta-goog-reserved-posix-gid"))){ // for GCS
        return get_gid((*iter).second.c_str());
    }else{
        return getegid();
    }
}

blkcnt_t get_blocks(off_t size)
{
    return size / 512 + 1;
}

time_t cvtIAMExpireStringToTime(const char* s)
{
    struct tm tm;
    if(!s){
        return 0L;
    }
    memset(&tm, 0, sizeof(struct tm));
    strptime(s, "%Y-%m-%dT%H:%M:%S", &tm);
    return timegm(&tm); // GMT
}

time_t get_lastmodified(const char* s)
{
    struct tm tm;
    if(!s){
        return -1;
    }
    memset(&tm, 0, sizeof(struct tm));
    strptime(s, "%a, %d %b %Y %H:%M:%S %Z", &tm);
    return timegm(&tm); // GMT
}

time_t get_lastmodified(const headers_t& meta)
{
    headers_t::const_iterator iter = meta.find("Last-Modified");
    if(meta.end() == iter){
        return -1;
    }
    return get_lastmodified((*iter).second.c_str());
}

static const char *s_wday[] = {
    "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
};

static const char *s_mon[] = {
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
};
std::string utc_to_gmt(const char* s)
{
    struct tm tm;
    char date[128];
    if(!s){
        return std::string("");
    }
    memset(&tm, 0, sizeof(struct tm));
    strptime(s, "%Y-%m-%dT%H:%M:%S", &tm);
    sprintf(date, "%s, %.2d %s %.4d %.2d:%.2d:%.2d GMT", 
        s_wday[tm.tm_wday], tm.tm_mday, s_mon[tm.tm_mon], 1900 + tm.tm_year, 
        tm.tm_hour, tm.tm_min, tm.tm_sec);
    return std::string(date);
}

//
// Returns it whether it is an object with need checking in detail.
// If this function returns true, the object is possible to be directory
// and is needed checking detail(searching sub object).
//
bool is_need_check_obj_detail(const headers_t& meta)
{
    headers_t::const_iterator iter;

    // directory object is Content-Length as 0.
    if(0 != get_size(meta)){
        return false;
    }
    // if the object has x-oss-meta information, checking is no more.
    if(meta.end() != meta.find("x-oss-meta-mode")  ||
       meta.end() != meta.find("x-oss-meta-mtime") ||
       meta.end() != meta.find("x-oss-meta-ctime") ||
       meta.end() != meta.find("x-oss-meta-atime") ||
       meta.end() != meta.find("x-oss-meta-uid")   ||
       meta.end() != meta.find("x-oss-meta-gid")   ||
       meta.end() != meta.find("x-oss-meta-owner") ||
       meta.end() != meta.find("x-oss-meta-group") ||
       meta.end() != meta.find("x-oss-meta-permissions") )
    {
        return false;
    }
    // if there is not Content-Type, or Content-Type is "x-directory",
    // checking is no more.
    if(meta.end() == (iter = meta.find("Content-Type"))){
        return false;
    }
    if("application/x-directory" == (*iter).second){
        return false;
    }
    return true;
}

// [NOTE]
// If add_noexist is false and the key does not exist, it will not be added.
//
bool merge_headers(headers_t& base, const headers_t& additional, bool add_noexist)
{
    bool added = false;
    for(headers_t::const_iterator iter = additional.begin(); iter != additional.end(); ++iter){
        if(add_noexist || base.find(iter->first) != base.end()){
            base[iter->first] = iter->second;
            added             = true;
        }
    }
    return added;
}

off_t get_symlink_size(const headers_t& meta)
{
    headers_t::const_iterator iter = meta.find("x-oss-meta-symlink-target");
    if(meta.end() == iter){
        return 0;
    }

    std::string::size_type pos1 = iter->second.find(':', 0);
    if(std::string::npos == pos1){
        return 0;
    }

    std::string::size_type pos2 = iter->second.find(':', pos1 + 1);
    if(std::string::npos == pos2){
        return 0;
    }

    if (pos1 + 1 == pos2) {
        return 0;
    }
    std::string size = iter->second.substr(pos1 + 1, pos2 - (pos1 + 1));

    off_t ret = get_size(size.c_str());
    return ret < 0 ? 0 : ret;
}


/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
