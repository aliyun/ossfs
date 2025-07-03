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


#ifndef S3FS_PREFETCH_READER_H_
#define S3FS_PREFETCH_READER_H_

#include <string>
#include <map>
#include <stdint.h>
#include <atomic>
#include <set>

#include "threadpoolman.h"
#include "s3fs_logger.h"
#include "memorypool.h"
#include "autolock.h"
#include "curl.h"

void* direct_read_worker(void* arg);

struct Chunk
{
    off_t offset;
    off_t size;
    char* buf;

    Chunk(off_t off, off_t size) : offset(off), size(size)    
    {
        buf = MemoryPool::memoryPool->Allocate();
        memset(buf, 0, size);
        cache_usage += size;
    }

    ~Chunk() {
        if (buf) {
            MemoryPool::memoryPool->Deallocate(buf);
            buf = NULL;
            cache_usage -= size;
        }
    }
    static std::atomic<uint64_t> cache_usage;
    static bool cache_usage_check();
};

class DirectReader 
{
    friend void* direct_read_worker(void* arg); 

    private:
        void WaitAllPrefetchThreadsExit();
        void CancelAllPrefetchThreads();
        void ReleaseChunks();
        bool CompleteInstruction(AutoLock::Type type = AutoLock::NONE);

        static off_t                chunk_size;
        static int                  prefetch_chunk_count;
        static uint64_t             prefetch_cache_limits;
        static int                  backward_chunks;
        static uint64_t             direct_read_local_file_cache_size;

        const std::string           filepath;    // used to request data from oss, and If the file is renamed or deleted during reading, 
                                                 // ossfs will exit direct read mode and no loner direct reading data from oss again.

        const off_t                 filesize;    // equal to the size when the file is opened and will not change again.
        
        // following three members are used for waiting all prefetch threads exit, keep consistence with s3fs.
        Semaphore                   prefetched_sem;      
        int                         instruct_count = 0;      // num of prefetch workers in threadpool's instruction_list
        int                         completed_count = 0;
        
        bool                        is_direct_read_lock_init = false;
        bool                        need_abort = false;
   
    public:
        static bool SetChunkSize(off_t size);
        static bool SetPrefetchChunkCount(int count);
        static bool SetPrefetchCacheLimits(uint64_t limit);
        
        static off_t GetChunkSize() { return DirectReader::chunk_size; }
        static int GetPrefetchChunkCount() { return DirectReader::prefetch_chunk_count; }
        static uint64_t GetPrefetchCacheLimits() { return DirectReader::prefetch_cache_limits; }

        static bool SetDirectReadLocalFileCacheSizeMB(uint64_t limit);
        static uint64_t GetDirectReadLocalFileCacheSize() { return DirectReader::direct_read_local_file_cache_size; }

        static bool SetBackwardChunks(int chunk_num);
        static int GetBackwardChunks() { return DirectReader::backward_chunks; }

        explicit DirectReader(const std::string& path, off_t size);
        ~DirectReader();

        bool Prefetch(off_t start, off_t len);
        off_t GetFileSize() { return filesize; };
        void CleanUpChunks(); 
        bool CanGenerateTask(uint32_t chunkid);

        // following members used outside (generating prefetch task and releasing chunks)
        pthread_mutex_t             direct_read_lock;
        std::map<uint32_t, Chunk*>  chunks;
        std::set<uint32_t>          ongoing_prefetches;
};

struct DirectReadParam {
    DirectReader* direct_reader;
    off_t start = 0;
    off_t len = 0;
    bool is_sync_download = false;
};



#endif // S3FS_PREFETCH_READER_H_