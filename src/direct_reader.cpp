/*
 * ossfs -  FUSE-based file system backed by Alibaba Cloud OSS
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
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

#include "direct_reader.h"
#include "string_util.h"

//-------------------------------------------------------------------
// Symbols
//-------------------------------------------------------------------
static const off_t MIN_CHUNK_SIZE = 1 * 1024 * 1024;
static const off_t MAX_CHUNK_SIZE = 32 * 1024 * 1024;
static const uint64_t MIN_PREFETCH_CACHE_LIMITS = 128 * 1024 * 1024;

//-------------------------------------------------------------------
// Class Chunk
//-------------------------------------------------------------------
std::atomic<uint64_t> Chunk::cache_usage = ATOMIC_VAR_INIT(0);
bool Chunk::cache_usage_check() 
{
    return cache_usage < DirectReader::GetPrefetchCacheLimits();
}

//-------------------------------------------------------------------
// Class DirectReader
//-------------------------------------------------------------------
off_t     DirectReader::chunk_size                    = 4 * 1024 * 1024;       // default
int       DirectReader::prefetch_chunk_count          = 32;                    // default 
uint64_t  DirectReader::prefetch_cache_limits         = 1024 * 1024 * 1024;    // default
int       DirectReader::backward_chunks               = 1;
uint64_t  DirectReader::direct_read_local_file_cache_size = 0;   // by default data will not be written to the disk

bool DirectReader::SetChunkSize(off_t size)
{
    size = size * 1024 * 1024;
    if (size < MIN_CHUNK_SIZE || size > MAX_CHUNK_SIZE) {
        return false;
    }

    DirectReader::chunk_size = size;
    return true;

}

bool DirectReader::SetPrefetchChunkCount(int count)
{
    DirectReader::prefetch_chunk_count = count;
    return true;
}

bool DirectReader::SetPrefetchCacheLimits(uint64_t limit)
{
    limit = limit * 1024 * 1024;
    if (limit < MIN_PREFETCH_CACHE_LIMITS) {
        return false;
    }

    DirectReader::prefetch_cache_limits = limit;
    return true;
}
bool DirectReader::SetDirectReadLocalFileCacheSizeMB(uint64_t limit) {
    limit = limit * 1024 * 1024;
    DirectReader::direct_read_local_file_cache_size = limit;
    return true;
}

bool DirectReader::SetBackwardChunks(int chunk_num) {
    if (chunk_num < 0) {
        return false;
    }
    DirectReader::backward_chunks = chunk_num;
    return true;
}

//-------------------------------------------------------------------
// Class methods for DirectReader
//-------------------------------------------------------------------
DirectReader::DirectReader(const std::string& path, off_t size) : 
    filepath(path), filesize(size), prefetched_sem(0), instruct_count(0), completed_count(0),
    is_direct_read_lock_init(false)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
#if S3FS_PTHREAD_ERRORCHECK
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#endif
    int result;
    if (0 != (result = pthread_mutex_init(&direct_read_lock, &attr))) {
        S3FS_PRN_CRIT("failed to init direct_read_lock: %d", result);
        abort();
    }

    is_direct_read_lock_init = true;
}

DirectReader::~DirectReader() 
{
    CancelAllPrefetchThreads();
    ReleaseChunks();
    if(is_direct_read_lock_init){
      int result;
      if(0 != (result = pthread_mutex_destroy(&direct_read_lock))){
          S3FS_PRN_CRIT("failed to destroy upload_list_lock: %d", result);
          abort();
      }
      is_direct_read_lock_init = false;
    }
}

bool DirectReader::Prefetch(off_t start, off_t len)
{
    S3FS_PRN_DBG("Prefetch worker[start=%ld][len=%ld]", start, len);
    if (start >= filesize || len == 0) {
        return false;
    }
    
    DirectReadParam* direct_read_param  = new DirectReadParam;
    direct_read_param->direct_reader    = this;
    direct_read_param->start            = start;
    direct_read_param->len              = len;
    direct_read_param->is_sync_download = false;

    thpoolman_param* poolparam = new thpoolman_param;
    poolparam->args            = direct_read_param;
    poolparam->psem            = &prefetched_sem;
    poolparam->pfunc           = direct_read_worker;

    if (!ThreadPoolMan::Instruct(poolparam)) {
        S3FS_PRN_ERR("failed setup instruction for uploading.");
        delete direct_read_param;
        delete poolparam;
        return false;
    }
    
    ++instruct_count; // already lock outside

    return true;
}

void DirectReader::WaitAllPrefetchThreadsExit() 
{
    bool is_loop = true;
    {
        AutoLock auto_lock(&direct_read_lock);
        if(0 == instruct_count && 0 == completed_count){
            is_loop = false;
        }
    }

    while(is_loop){
        // need to wait the worker exiting
        prefetched_sem.wait();
        {
            AutoLock auto_lock(&direct_read_lock);
            if(0 < completed_count){
                --completed_count;
            }
            if(0 == instruct_count && 0 == completed_count){
                // break loop
                is_loop = false;
            }
        }
    }

    return;
}

void DirectReader::CancelAllPrefetchThreads()
{   
    S3FS_PRN_DBG("Cancel All Threads[instruct_count:%d]", instruct_count);
    bool need_cancel = false;
    {
        AutoLock auto_lock(&direct_read_lock);
        if(0 < instruct_count){
            S3FS_PRN_WARN("The prefetch thread is running, so cancel them and wait for the end.");
            need_cancel = true;
        }
    }
    if(need_cancel){
        WaitAllPrefetchThreadsExit();
    }

    return;
}

bool DirectReader::CompleteInstruction(AutoLock::Type type)
{
    AutoLock lock(&direct_read_lock, type);
    
    if (0 >= instruct_count) {
        S3FS_PRN_ERR("Internal error: instruct_count caused an underflow.");
        return false;
    }

    --instruct_count;
    ++completed_count;
    
    S3FS_PRN_DBG("CompleteInstruction[instruct_count=%d][completed_count=%d]", instruct_count, completed_count);

    return true;
}

void DirectReader::CleanUpChunks() 
{
    CancelAllPrefetchThreads();
    ReleaseChunks();

    return;
}

void DirectReader::ReleaseChunks() 
{
    AutoLock lock(&direct_read_lock);
    for (std::map<uint32_t, Chunk*>::iterator it = chunks.begin(); it!= chunks.end(); it++) {
        delete it->second;
        it->second = NULL;
    }
    chunks.clear();
}

void* direct_read_worker(void* arg) 
{
    DirectReadParam* direct_read_param = static_cast<DirectReadParam*>(arg);
    if (!direct_read_param || !(direct_read_param->direct_reader)) {
        // keep consistence with s3fs
        if (direct_read_param) {
            delete direct_read_param;
        }
        return reinterpret_cast<void*>(-EIO);
    }   
    
    DirectReader* direct_reader = direct_read_param->direct_reader;
    off_t start = direct_read_param->start;
    off_t len   = direct_read_param->len;
    bool  is_sync_download = direct_read_param->is_sync_download;
    
    
    S3FS_PRN_DBG("prefetch worker[pid=%lu][path=%s][start=%ld][len=%ld]", pthread_self(), direct_reader->filepath.c_str(), start, len);
    
    S3fsCurl s3fscurl;
    ssize_t rsize;

    Chunk* chunk = new Chunk(start, len); 
    uint32_t chunk_id = start / DirectReader::GetChunkSize();
    int result = s3fscurl.GetObjectStreamRequest(direct_reader->filepath.c_str(),
                                                 chunk->buf, start, len, rsize);

    if(0 != result){
        S3FS_PRN_ERR("failed to get object stream[pid=%lu][path=%s][start=%ld][len=%ld]", pthread_self(), direct_reader->filepath.c_str(), start, len);
        delete chunk;
        chunk = NULL;

        if(!is_sync_download) {
            AutoLock lock(&direct_reader->direct_read_lock);
            direct_reader->ongoing_prefetches.erase(chunk_id);
            direct_reader->CompleteInstruction(AutoLock::ALREADY_LOCKED);
        }

        delete direct_read_param;
        return reinterpret_cast<void*>(-EIO);
    }

    AutoLock lock(&direct_reader->direct_read_lock, is_sync_download ? AutoLock::ALREADY_LOCKED : AutoLock::NONE);
    
    if (!direct_reader->chunks.count(chunk_id)) {
        S3FS_PRN_DBG("add new chunk[pid=%lu][path=%s][chunkid=%d][start=%ld][len=%ld]", pthread_self(), direct_reader->filepath.c_str(), chunk_id, start, len);
        direct_reader->chunks[start/DirectReader::GetChunkSize()] = chunk;
    } else {
        S3FS_PRN_DBG("chunk already exist[pid=%lu][path=%s][chunkid=%d][start=%ld][len=%ld]", pthread_self(), direct_reader->filepath.c_str(), chunk_id, start, len);
        delete chunk;
        chunk = NULL;
    }

    if (!is_sync_download) {
        direct_reader->ongoing_prefetches.erase(chunk_id);
        direct_reader->CompleteInstruction(AutoLock::ALREADY_LOCKED);
    }

    delete direct_read_param;
    return NULL;
}