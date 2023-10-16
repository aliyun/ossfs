/*
 * s3fs - FUSE-based file system backed by Amazon S3
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

#ifndef OSSFS_STREAMREADER_H_
#define OSSFS_STREAMREADER_H_

#include "types.h"
#include <list>

struct mempage
{
    char*  data;
    off_t  offset;
    off_t  bytes;
    off_t  capacity;

    mempage(char *ptr, off_t start, off_t cap) :
        data(ptr), offset(start), bytes(0), capacity(cap)
    {
    }
    off_t next() const
    {
        return (offset + bytes);
    }
    off_t end() const
    {
        return (0 < bytes ? offset + bytes - 1 : 0);
    }
};
typedef std::list<struct mempage> mempage_list_t;

class BufferReader
{
    private:
        std::string  path;
        off_t        file_size;

        char*        buff;
        off_t        buff_offset;
        off_t        buff_bytes;
        off_t        buff_capacity;

        off_t        next_rd_offset;

    private:
        ssize_t ReadFromStream(char *buff, off_t start, off_t size);

    protected:
 
    public:
        explicit BufferReader(const char* path, off_t fsize, off_t max_buffsize);
        ~BufferReader();

        ssize_t Read(char *b, off_t start, off_t size);
        ssize_t ReadN(char *b, off_t start, off_t size);
};

class AsyncPrefechBuffer
{
    private:
        std::string    path;
        off_t          file_size;
        off_t          offset;
        mempage_list_t pages;
        mempage_list_t::iterator riter;
        mempage_list_t::iterator witer;
        off_t          nextoffset;
        off_t          prefetch_size;

        pthread_mutex_t mLock;
        pthread_cond_t mCond;

    private:
        void reset();
        void clear();

        static void* PrefetchThreadWorker(void* arg);

    protected:
 
    public:
        explicit AsyncPrefechBuffer(const char* path, off_t start, off_t fsize);
        ~AsyncPrefechBuffer();

        off_t Init(off_t prefetchsize);
        void Reset();
        void Clear();
        ssize_t Read(char *b, off_t start, off_t size);
        bool Empty();
        bool Prefech();
};

class AsyncPrefechBufferV2
{
    private:
        std::string    path;
        off_t          file_size;
        off_t          offset;
        char*          data;
        off_t          capacity;
        off_t          fetchsize;

        int            fetchst;  // 0 init, 1 fetching, 2 done
        int            fetchresult;
        off_t          endoffset;

        off_t          next_rd_offset;
        off_t          rdoff;
        off_t          wdoff;

        pthread_mutex_t mLock;
        pthread_cond_t mCond;

    private:
        static void* PrefetchThreadWorker(void* arg);
        static void WriteCallbck(void* arg, off_t start, off_t size);
        void reset();

    protected:
 
    public:
        explicit AsyncPrefechBufferV2();
        ~AsyncPrefechBufferV2();
        off_t Init(const char* path, off_t fsize, off_t start, off_t prefetchsize);
        bool Prefech();
        off_t UpdateWritePtr(off_t wp);
        ssize_t Read(char *b, off_t start, off_t size);
        ssize_t ReadNInner(char *b, off_t start, off_t size);
        ssize_t ReadN(char *b, off_t start, off_t size);

        off_t Next() {return offset + fetchsize;}

};

class PrefechReader
{
    private:
        std::string  path;
        off_t        file_size;
        off_t        remains;

        off_t        next_rd_offset;

        std::list< AsyncPrefechBufferV2 *> buffers;
        pthread_mutex_t mLock;

    public:
        explicit PrefechReader(const char* path, off_t fsize);
        ~PrefechReader();
        ssize_t ReadNInner(char *b, off_t start, off_t size);
        ssize_t ReadN(char *b, off_t start, off_t size);
};

#endif // OSSFS_STREAMREADER_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
