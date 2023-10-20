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
#include "autolock.h"
#include "threadpoolman.h"

static const off_t MIN_BUFFER_SIZE         = 128*1024;
#define ROUND_UP(v, a) (((v) + (a) -1)/(a)*(a))
#define ROUND_DOWN(v, a) ((v)/(a)*(a))

static const off_t MEM_PAGE_SIZE         = 2*1024*1024;
static const off_t PREFETCH_SIZE         = 4*1024*1024;
static const off_t PREFETCH_COUNT        = 4;


BufferReader::BufferReader(const char* tpath, off_t fsize, off_t buffsize)
    :path(tpath), file_size(fsize), buff(NULL), buff_offset(0), buff_bytes(0)
{
    next_rd_offset = 0;
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

ssize_t BufferReader::ReadN(char *b, off_t start, off_t size)
{
    if (next_rd_offset != start) {
        S3FS_PRN_DBG("BufferReader::ReadN offset not equal [path=%s][b=%p][start=%lld][next_rd_offset=%lld]", path.c_str(), b, 
            static_cast<long long>(start), static_cast<long long>(next_rd_offset));
    }

    S3FS_PRN_DBG("begin [path=%s][b=%p][start=%lld][size=%lld][next_rd_offset=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(next_rd_offset));
    ssize_t ret = Read(b, start, size);
    S3FS_PRN_DBG("end [path=%s][b=%p][start=%lld][ret=%lld]", path.c_str(), b, static_cast<long long>(start), static_cast<long long>(ret));
    if (ret > 0) {
        next_rd_offset = start + size;
    }
     return ret;
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

AsyncPrefechBuffer::AsyncPrefechBuffer(const char* p, off_t start, off_t fsize)
:path(p), file_size(fsize), offset(start)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    if (0 != pthread_mutex_init(&mLock, &attr)) {
        S3FS_PRN_ERR("init lock failed");
    }

    if (0 != pthread_cond_init(&mCond, NULL)) {
        S3FS_PRN_ERR("init cond failed");
    }

    S3FS_PRN_DBG("[path=%s][start=%lld][fsize=%lld]", path.c_str(), static_cast<long long>(start), static_cast<long long>(fsize));
    reset();
}

AsyncPrefechBuffer::~AsyncPrefechBuffer()
{
    for(mempage_list_t::iterator it = pages.begin(); it != pages.end(); it++) {
        if (it->data != NULL) {
            free(it->data);
        }
    }
}

off_t AsyncPrefechBuffer::Init(off_t prefetchsize)
{
    S3FS_PRN_DBG("begin [path=%s][prefetchsize=%lld]", path.c_str(), static_cast<long long>(prefetchsize));
    AutoLock lock(&mLock);
    clear();
    off_t remians = std::min(file_size - offset, prefetchsize);
    prefetch_size = remians;

    if (remians == 0) {
        return remians;
    }

    int cnt = (int)(ROUND_UP(remians, MEM_PAGE_SIZE)/MEM_PAGE_SIZE);
    off_t start = offset;
    bool flag = false;
    for (int i = 0; i < cnt; i++) {
        char *p = (char *)malloc(MEM_PAGE_SIZE);
        if (p == NULL) {
            flag = true;
            break;
        }
        mempage page(p, start, MEM_PAGE_SIZE);
        start += MEM_PAGE_SIZE;
        pages.push_back(page);
    }

    if (flag) {
        //fail
        clear();
        return 0;
    }
    reset();

    S3FS_PRN_DBG("end [path=%s][page=%d]", path.c_str(), cnt);

    return remians;
}

void AsyncPrefechBuffer::reset()
{
    nextoffset = offset;
    riter = pages.begin();
    witer = pages.begin();
}

void AsyncPrefechBuffer::clear()
{
    
}

void AsyncPrefechBuffer::Reset()
{
    AutoLock lock(&mLock);
    reset();
}

void AsyncPrefechBuffer::Clear()
{
    AutoLock lock(&mLock);
    clear();
}

ssize_t AsyncPrefechBuffer::Read(char *b, off_t start, off_t size)
{
    AutoLock lock(&mLock);
    if (riter == pages.end()) {
        return 0;
    }

    if (riter == witer) {
        pthread_cond_wait(&mCond, &mLock);  
    }

    off_t rcnt = 0;
    for (;riter != pages.end() && riter != witer; riter++) {
        off_t s = start + rcnt;
        if (riter->offset <= s && s < riter->next()) {
            off_t n = std::min(riter->next() - s, size - rcnt);
            memcpy((b + rcnt), riter->data + (s - riter->offset), n);
            rcnt += n;
        }
    }
    nextoffset += rcnt;
    return rcnt;
}

bool AsyncPrefechBuffer::Empty()
{
    AutoLock lock(&mLock);
    if (riter == pages.end()) {
        return true;
    }
    return false;
}

void* AsyncPrefechBuffer::PrefetchThreadWorker(void* arg)
{
    AsyncPrefechBuffer*  thiz = static_cast<AsyncPrefechBuffer*>(arg);
    if(!thiz){
        return reinterpret_cast<void*>(-EIO);
    }
    S3FS_PRN_INFO3("Prefech Thread [path=%s][offset=%lld][prefetch_size=%lld]", thiz->path.c_str(), static_cast<long long>(thiz->offset), static_cast<long long>(thiz->prefetch_size));
/*
    int result = s3fscurl.GetObjectRequestStream(path.c_str(), b, start, size, rsize);
    if(0 != result){
        S3FS_PRN_ERR("could not download. start(%lld), size(%zu), errno(%d)", static_cast<long long int>(start), size, result);
        return -errno;
    }
    return rsize;
*/    
    return NULL;
}

bool AsyncPrefechBuffer::Prefech()
{
    S3FS_PRN_DBG("Prefech start");

    // make parameter for thread pool
    thpoolman_param* ppoolparam  = new thpoolman_param;
    ppoolparam->args             = this;
    ppoolparam->psem             = NULL;
    ppoolparam->pfunc            = AsyncPrefechBuffer::PrefetchThreadWorker;

    // setup instruction
    if(!ThreadPoolMan::Instruct(ppoolparam)){
        S3FS_PRN_ERR("failed setup instruction for uploading.");
        delete ppoolparam;
        return false;
    }
    S3FS_PRN_DBG("Prefech end");
    return true;
}



AsyncPrefechBufferV2::AsyncPrefechBufferV2()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    if (0 != pthread_mutex_init(&mLock, &attr)) {
        S3FS_PRN_ERR("init lock failed");
    }

    if (0 != pthread_cond_init(&mCond, NULL)) {
        S3FS_PRN_ERR("init cond failed");
    }
    reset();
    data = NULL;
    capacity = 0;
}

AsyncPrefechBufferV2::~AsyncPrefechBufferV2()
{
    if (data) {
        free(data);
        data = NULL;
    }
}

void AsyncPrefechBufferV2::reset()
{
    path = "";
    file_size = -1;
    offset = -1;
    rdoff = -1;
    wdoff = -1;
    fetchst = 0;
    fetchresult = 0;
    endoffset = -1;
    next_rd_offset = -1;
}


off_t AsyncPrefechBufferV2::Init(const char* tpath, off_t fsize, off_t start, off_t prefetchsize)
{
    reset();
    path = tpath;
    file_size = fsize;
    offset = start;
    next_rd_offset = start;
    S3FS_PRN_DBG("begin [path=%s][start=%lld][prefetchsize=%lld]", path.c_str(), 
        static_cast<long long>(start), static_cast<long long>(prefetchsize));
    off_t remians = std::min(file_size - offset, prefetchsize);
    off_t prefetch_size = ROUND_UP(remians, MEM_PAGE_SIZE);

    if (data != NULL && prefetch_size > capacity) {
        free(data);
        data = NULL;
        capacity = 0;
    }

    if (data == NULL) {
        data = (char *)malloc(prefetch_size);
        capacity = prefetch_size;
    }
    fetchsize = prefetch_size;

    if (data == NULL) {
        //TODO use BufferedReader
    }

    S3FS_PRN_DBG("end [path=%s][capacity=%lld][fetchsize=%lld]", path.c_str(), static_cast<long long>(capacity), static_cast<long long>(fetchsize));
    return fetchsize;
}

void* AsyncPrefechBufferV2::PrefetchThreadWorker(void* arg)
{
    AsyncPrefechBufferV2*  thiz = static_cast<AsyncPrefechBufferV2*>(arg);
    if(!thiz){
        return reinterpret_cast<void*>(-EIO);
    }
    S3FS_PRN_INFO3("Prefech Thread [path=%s][offset=%lld][prefetch_size=%lld]", thiz->path.c_str(), static_cast<long long>(thiz->offset), static_cast<long long>(thiz->fetchsize));
    ssize_t rsize;
    S3fsCurl s3fscurl;
    filepart_write_cb fn = (filepart_write_cb)AsyncPrefechBufferV2::WriteCallbck;
    int result = s3fscurl.GetObjectRequestStream(thiz->path.c_str(), thiz->data, thiz->offset, thiz->fetchsize, rsize,
        fn, thiz);
    if(0 != result){
        S3FS_PRN_ERR("could not download. start(%lld), size(%zu), errno(%d)", static_cast<long long int>(thiz->offset), thiz->fetchsize, result);
        return reinterpret_cast<void*>(-EIO);;
    }
    thiz->endoffset = thiz->offset + rsize;
    thiz->fetchst = 2;
    thiz->fetchresult = result;
    thiz->UpdateWritePtr(thiz->offset + rsize);
    return NULL;
}

void AsyncPrefechBufferV2::WriteCallbck(void* arg, off_t start, off_t size)
{
    //S3FS_PRN_DBG("[start:%lld][size:%lld]", static_cast<long long int>(start), static_cast<long long int>(size));
    AsyncPrefechBufferV2*  thiz = static_cast<AsyncPrefechBufferV2*>(arg);
    off_t newoff = start + size;
    if ((thiz->wdoff < 0 && newoff > 256 * 1024)|| thiz->wdoff + 512*1024 <=  start + size) {
        thiz->UpdateWritePtr(start + size);
    }
}

bool AsyncPrefechBufferV2::Prefech()
{
    S3FS_PRN_DBG("Prefech start");

    // make parameter for thread pool
    thpoolman_param* ppoolparam  = new thpoolman_param;
    ppoolparam->args             = this;
    ppoolparam->psem             = NULL;
    ppoolparam->pfunc            = AsyncPrefechBufferV2::PrefetchThreadWorker;

    // setup instruction
    if(!ThreadPoolMan::Instruct(ppoolparam)){
        S3FS_PRN_ERR("failed setup instruction for uploading.");
        delete ppoolparam;
        return false;
    }
    S3FS_PRN_DBG("Prefech end");
    return true;
}

off_t AsyncPrefechBufferV2::UpdateWritePtr(off_t wp)
{
    S3FS_PRN_DBG("[old wp:%lld][new wp:%lld]", static_cast<long long int>(wdoff), static_cast<long long int>(wp));
    wdoff = wp;
    pthread_cond_broadcast(&mCond);
    return wp;
}

ssize_t AsyncPrefechBufferV2::Read(char *b, off_t start, off_t size)
{
    AutoLock lock(&mLock);
    S3FS_PRN_DBG("AsyncPrefechBufferV2::Read begin [path=%s][b=%p][start=%lld][size=%lld][rdoff=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(rdoff));
    if (wdoff == -1 || start > wdoff ) {
        pthread_cond_wait(&mCond, &mLock);  
    }

    

    //read from prefetch_buffer 
    off_t bend = wdoff;
    off_t end = start + size;
    end = std::min(bend, end);
    size_t rsize = (size_t)(end - start);
    memcpy(b, data + (start - offset), rsize);
    rdoff = start + rsize;
    S3FS_PRN_DBG("AsyncPrefechBufferV2::Read end [path=%s][rsize=%lld][rdoff=%lld]", path.c_str(), 
        static_cast<long long>(rsize), static_cast<long long>(rdoff));
    return (ssize_t)rsize;
}

ssize_t AsyncPrefechBufferV2::ReadN(char *b, off_t start, off_t size)
{
    if (next_rd_offset != start) {
        S3FS_PRN_DBG("AsyncPrefechBufferV2::ReadN offset not equal [path=%s][b=%p][start=%lld][next_rd_offset=%lld]", path.c_str(), b, 
            static_cast<long long>(start), static_cast<long long>(next_rd_offset));
    }
    S3FS_PRN_DBG("begin [path=%s][b=%p][start=%lld][size=%lld][next_rd_offset=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(next_rd_offset));
    ssize_t ret = ReadNInner(b, start, size);
    S3FS_PRN_DBG("end [path=%s][b=%p][start=%lld][ret=%lld]", path.c_str(), b, static_cast<long long>(start), static_cast<long long>(ret));
    if (ret > 0) {
        next_rd_offset = start + size;
    }
    return ret;
}

ssize_t AsyncPrefechBufferV2::ReadNInner(char *b, off_t start, off_t size)
{
    //not in the buffer
    if (!(offset <= start && (start < (offset + fetchsize)))) {
        S3FS_PRN_DBG("not in buffer [path=%s][b=%p][start=%lld][size=%lld][offset=%lld][fetchsize=%lld]", 
            path.c_str(), b, 
            static_cast<long long>(start), static_cast<long long>(size), 
            static_cast<long long>(offset), static_cast<long long>(fetchsize));
        return 0;
    }

    off_t remains = std::min((offset + fetchsize) - start, size);
    S3FS_PRN_DBG("begin[path=%s][b=%p][start=%lld][size=%lld][remains=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(remains));

    off_t off = 0;
    while (remains > 0) {
        ssize_t cnt = Read(b + off, start + off, remains);
        if (cnt < 0) {
            return cnt;
        }
        if (cnt == 0 && (endoffset != -1) && (start + off) >= endoffset) {
            break;
        }
        remains -= (off_t)cnt;
        off += (off_t)cnt;
    }
    S3FS_PRN_DBG("end[path=%s][b=%p][start=%lld][size=%lld][off=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(off));
    return (ssize_t)off;
}

PrefechReader::PrefechReader(const char* tpath, off_t fsize)
    :path(tpath), file_size(fsize)
{
    next_rd_offset = 0;
    remains = fsize;
    for(int i = 0; i < PREFETCH_COUNT && remains > 0; i++) {
        AsyncPrefechBufferV2 * buff = new AsyncPrefechBufferV2();
        buff->Init(path.c_str(), fsize, i * PREFETCH_SIZE, PREFETCH_SIZE);
        buffers.push_back(buff);
        buff->Prefech();
        if (remains < PREFETCH_SIZE) {
            remains = 0;
        } else {
            remains -= PREFETCH_SIZE;
        }
    }
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    if (0 != pthread_mutex_init(&mLock, &attr)) {
        S3FS_PRN_ERR("init lock failed");
    }
}

PrefechReader::~PrefechReader()
{

}

ssize_t PrefechReader::ReadN(char *b, off_t start, off_t size)
{
    if (next_rd_offset != start) {
        S3FS_PRN_DBG("PrefechReader::ReadN offset not equal [path=%s][b=%p][start=%lld][next_rd_offset=%lld]", path.c_str(), b, 
            static_cast<long long>(start), static_cast<long long>(next_rd_offset));
    }
    S3FS_PRN_DBG("begin [path=%s][b=%p][start=%lld][size=%lld][next_rd_offset=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(next_rd_offset));
    ssize_t ret = ReadNInner(b, start, size);
    S3FS_PRN_DBG("end [path=%s][b=%p][start=%lld][ret=%lld]", path.c_str(), b, static_cast<long long>(start), static_cast<long long>(ret));
    if (ret > 0) {
        next_rd_offset = start + size;
    }
    return ret;
}

ssize_t PrefechReader::ReadNInner(char *b, off_t start, off_t size)
{
    AutoLock lock(&mLock);
    off_t gots = std::min(file_size - start, size);
    off_t off = 0;
    S3FS_PRN_DBG("begin[path=%s][b=%p][start=%lld][size=%lld][gots=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(gots));    
    while (gots > 0) {
        ssize_t cnt = buffers.front()->ReadN(b + off, start + off, gots);
        if (cnt < 0) {
            return cnt;
        }

        gots -= (off_t)cnt;
        off += (off_t)cnt;

        if (gots > 0) {
            S3FS_PRN_DBG("mid[path=%s][b=%p][start=%lld][size=%lld][gots=%lld][remains=%lld]", path.c_str(), b, 
                static_cast<long long>(start), static_cast<long long>(size), 
                static_cast<long long>(gots), static_cast<long long>(remains));          
            AsyncPrefechBufferV2 * buff = buffers.front();
            buffers.pop_front();
            S3FS_PRN_DBG("mid -1");
            if (remains > 0) {
                off_t next = file_size - remains;
                S3FS_PRN_DBG("mid next:%lld", static_cast<long long>(next));
                buff->Init(path.c_str(), file_size, next, PREFETCH_SIZE);
                S3FS_PRN_DBG("mid - 2");
                if (remains <= PREFETCH_SIZE) {
                    remains = 0;
                } else {
                    remains -= PREFETCH_SIZE;
                }
                buff->Prefech();
                buffers.push_back(buff);
            }
        }
    }
    S3FS_PRN_DBG("end[path=%s][b=%p][start=%lld][size=%lld][off=%lld]", path.c_str(), b, 
        static_cast<long long>(start), static_cast<long long>(size), static_cast<long long>(off));  

    return off;
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
