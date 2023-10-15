/*
 * s3fs - FUSE-based file system backed by Amazon S3
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
#include <errno.h>
#include <stdint.h>
#include <string>
#include <string.h>

#include "s3fs_logger.h"
#include "streamreader.h"
#include "curl.h"

static const off_t MIN_BUFFER_SIZE         = 128*1024;
#define ROUND_UP(v, a) (((v) + (a) -1)/(a)*(a))
#define ROUND_DOWN(v, a) ((v)/(a)*(a))

BufferReader::BufferReader(const char* tpath, off_t fsize, off_t buffsize)
    :path(tpath), file_size(fsize), buff(NULL), buff_offset(0), buff_bytes(0)
{
    if (file_size > MIN_BUFFER_SIZE) {
        off_t s = ROUND_UP(file_size, 128*1024);
        s = std::min(s, buffsize);
        buff_capacity = s;
    }
    S3FS_PRN_DBG("[path=%s][file_size=%lld][buff_capacity=%lld]", path.c_str(), static_cast<long long>(file_size), static_cast<long long>(buff_capacity));
}

BufferReader::~BufferReader()
{
    if (buff != NULL) {
        free(buff);
        buff = NULL;
    }
    S3FS_PRN_DBG("[buff=%p][buff=%p]", path.c_str(), buff);
}
  
ssize_t BufferReader::Read(char *b, off_t start, off_t size)
{
    S3FS_PRN_DBG("[path=%s][b=%p][start=%lld][size=%lld]", path.c_str(), b, static_cast<long long>(start), static_cast<long long>(size));

    if (!b || size <= 0 || start >= file_size) {
        return 0;
    }

    if (file_size <= MIN_BUFFER_SIZE) {
        return ReadFromStream(b, start, size);
    }

    if (buff == NULL) {
        buff = (char *)malloc(buff_capacity);
        buff_offset = 0;
        buff_bytes = 0;
    }

    S3FS_PRN_DBG("[path=%s][buff=%p][buff_offset=%lld][buff_bytes=%lld]", path.c_str(), buff, static_cast<long long>(buff_offset), static_cast<long long>(buff_bytes));

    // can't malloc buffer, read from server
    if (buff == NULL) {
        return ReadFromStream(b, start, size);
    }

    //load into prefetch_buffer
    if (!(buff_offset <= start && (start + size) <= (buff_offset + buff_bytes))) {
        buff_offset = start;
        off_t remains = std::min(file_size - buff_offset, buff_capacity);
        ssize_t got = ReadFromStream(buff, buff_offset, remains);
        if (got < 0) {
            return got;
        }
        buff_bytes = got;
    }

    //read from prefetch_buffer 
    off_t bend = buff_offset + buff_bytes;
    off_t end = start + size;
    end = std::min(bend, end);
    size_t rsize = (size_t)(end - start);
    S3FS_PRN_DBG("[path=%s][rsize=%lld]", path.c_str(), static_cast<long long>(rsize));
    memcpy(b, buff + (start - buff_offset), rsize);
    return (ssize_t)rsize;
}

ssize_t BufferReader::ReadFromStream(char *b, off_t start, off_t size)
{
    S3FS_PRN_DBG("[path=%s][b=%p][start=%lld][size=%lld]", path.c_str(), b, static_cast<long long>(start), static_cast<long long>(size));
    S3fsCurl s3fscurl;
    ssize_t rsize;
    int result = s3fscurl.GetObjectRequestStream(path.c_str(), b, start, size, rsize);
    if(0 != result){
        S3FS_PRN_ERR("could not download. start(%lld), size(%zu), errno(%d)", static_cast<long long int>(start), size, result);
        return -errno;
    }
    return rsize;
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
