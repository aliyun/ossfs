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

#ifndef S3FS_FDCACHE_FDINFO_H_
#define S3FS_FDCACHE_FDINFO_H_

#include "fdcache_untreated.h"
#include "direct_reader.h"

//------------------------------------------------
// Class PseudoFdInfo
//------------------------------------------------
class PseudoFdInfo
{
    private:
        int             pseudo_fd;
        int             physical_fd;
        int             flags;              // flags at open
        std::string     upload_id;
        filepart_list_t upload_list;
        UntreatedParts  untreated_list;     // list of untreated parts that have been written and not yet uploaded(for streamupload)
        etaglist_t      etag_entities;      // list of etag string and part number entities(to maintain the etag entity even if MPPART_INFO is destroyed)

        bool            is_lock_init;
        pthread_mutex_t upload_list_lock;   // protects upload_id and upload_list

        pthread_mutex_t direct_read_lock;
        bool            is_direct_read;
        off_t           last_read_tail;
        int             prefetch_cnt;
        DirectReader*   direct_reader_mgr;
        uint64_t        loaded_size = 0;

    private:
        bool Clear();
        void GeneratePrefetchTask(uint32_t start_prefetch_chunk, uint32_t prefetch_cnt);
        uint32_t GetPrefetchCount(off_t offset, size_t size);
    public:
        PseudoFdInfo(int fd = -1, int open_flags = 0, bool is_direct_read = false, std::string path = "", off_t size = 0);
        ~PseudoFdInfo();

        int GetPhysicalFd() const { return physical_fd; }
        int GetPseudoFd() const { return pseudo_fd; }
        int GetFlags() const { return flags; }
        bool Writable() const;
        bool Readable() const;

        bool Set(int fd, int open_flags);
        bool ClearUploadInfo(bool is_clear_part = false, bool lock_already_held = false);
        bool InitialUploadInfo(const std::string& id);

        bool IsUploading() const { return !upload_id.empty(); }
        bool GetUploadId(std::string& id) const;
        bool GetEtaglist(etaglist_t& list);

        bool AppendUploadPart(off_t start, off_t size, bool is_copy = false, etagpair** ppetag = NULL);

        void ClearUntreated(bool lock_already_held = false);
        bool ClearUntreated(off_t start, off_t size);
        bool GetUntreated(off_t& start, off_t& size, off_t max_size, off_t min_size = MIN_MULTIPART_SIZE);
        bool GetLastUntreated(off_t& start, off_t& size, off_t max_size, off_t min_size = MIN_MULTIPART_SIZE);
        bool AddUntreated(off_t start, off_t size);

        ssize_t DirectReadAndPrefetch(char* bytes, off_t start, size_t size);
        void ExitDirectRead();
        void AddLoadedSize(off_t size) { loaded_size += size; }
        uint64_t GetLoadedSize() { return loaded_size; }
};

typedef std::map<int, class PseudoFdInfo*> fdinfo_map_t;

#endif // S3FS_FDCACHE_FDINFO_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
