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
#include <cassert>
#include <algorithm>

#include "common.h"
#include "s3fs.h"
#include "fdcache_fdinfo.h"
#include "fdcache_pseudofd.h"
#include "autolock.h"

//------------------------------------------------
// PseudoFdInfo methods
//------------------------------------------------
PseudoFdInfo::PseudoFdInfo(int fd, int open_flags, bool is_direct_read, std::string path, off_t size) : pseudo_fd(-1), physical_fd(fd), flags(0),
    is_direct_read(is_direct_read), last_read_tail(0), prefetch_cnt(0), direct_reader_mgr(NULL) //, is_lock_init(false)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
#if S3FS_PTHREAD_ERRORCHECK
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#endif
    int result;
    if(0 != (result = pthread_mutex_init(&upload_list_lock, &attr))){
        S3FS_PRN_CRIT("failed to init upload_list_lock: %d", result);
        abort();
    }

    if(0 != (result = pthread_mutex_init(&direct_read_lock, &attr))){
        S3FS_PRN_CRIT("failed to init seq_stream_read_lock: %d", result);
        abort();
    }

    is_lock_init = true;

    if(-1 != physical_fd){
        pseudo_fd = PseudoFdManager::Get();
        flags     = open_flags;
    }

    if(is_direct_read){
        direct_reader_mgr = new DirectReader(path, size);
    }
}

PseudoFdInfo::~PseudoFdInfo()
{
    if (direct_reader_mgr){
        delete direct_reader_mgr;
        direct_reader_mgr = NULL;
    }

    if(is_lock_init){
        int result;
        if(0 != (result = pthread_mutex_destroy(&direct_read_lock))){
            S3FS_PRN_CRIT("failed to destroy seq_stream_read_lock: %d", result);
            abort();
        }

        if(0 != (result = pthread_mutex_destroy(&upload_list_lock))){
            S3FS_PRN_CRIT("failed to destroy upload_list_lock: %d", result);
            abort();
        }
        is_lock_init = false;
    }
    Clear();
}

bool PseudoFdInfo::Clear()
{
    if(-1 != pseudo_fd){
        PseudoFdManager::Release(pseudo_fd);
    }
    pseudo_fd   = -1;
    physical_fd = -1;

    return true;
}

bool PseudoFdInfo::Set(int fd, int open_flags)
{
    if(-1 == fd){
        return false;
    }
    Clear();
    physical_fd = fd;
    pseudo_fd   = PseudoFdManager::Get();
    flags       = open_flags;

    return true;
}

bool PseudoFdInfo::Writable() const
{
    if(-1 == pseudo_fd){
        return false;
    }
    if(0 == (flags & (O_WRONLY | O_RDWR))){
        return false;
    }
    return true;
}

bool PseudoFdInfo::Readable() const
{
    if(-1 == pseudo_fd){
        return false;
    }
    // O_RDONLY is 0x00, it means any pattern is readable.
    return true;
}

bool PseudoFdInfo::ClearUploadInfo(bool is_cancel_mp, bool lock_already_held)
{
    AutoLock auto_lock(&upload_list_lock, lock_already_held ? AutoLock::ALREADY_LOCKED : AutoLock::NONE);

    if(is_cancel_mp){
        // [TODO]
        // If we have any uploaded parts, we should delete them here.
        // We haven't implemented it yet, but it will be implemented in the future.
        // (User can delete them in the utility mode of s3fs.)
        //
        S3FS_PRN_INFO("Implementation of cancellation process for multipart upload is awaited.");
    }

    upload_id.erase();
    upload_list.clear();
    ClearUntreated(true);

    return true;
}

bool PseudoFdInfo::InitialUploadInfo(const std::string& id)
{
    AutoLock auto_lock(&upload_list_lock);

    if(!ClearUploadInfo(true, true)){
        return false;
    }
    upload_id = id;
    return true;
}

bool PseudoFdInfo::GetUploadId(std::string& id) const
{
    if(!IsUploading()){
        S3FS_PRN_ERR("Multipart Upload has not started yet.");
        return false;
    }
    id = upload_id;
    return true;
}

bool PseudoFdInfo::GetEtaglist(etaglist_t& list)
{
    if(!IsUploading()){
        S3FS_PRN_ERR("Multipart Upload has not started yet.");
        return false;
    }

    AutoLock auto_lock(&upload_list_lock);

    list.clear();
    for(filepart_list_t::const_iterator iter = upload_list.begin(); iter != upload_list.end(); ++iter){
        if(iter->petag){
            list.push_back(*(iter->petag));
        }else{
            S3FS_PRN_ERR("The pointer to the etag string is null(internal error).");
            return false;
        }
    }
    return !list.empty();
}

// [NOTE]
// This method adds a part for a multipart upload.
// The added new part must be an area that is exactly continuous with the
// immediately preceding part.
// An error will occur if it is discontinuous or if it overlaps with an
// existing area.
//
bool PseudoFdInfo::AppendUploadPart(off_t start, off_t size, bool is_copy, etagpair** ppetag)
{
    if(!IsUploading()){
        S3FS_PRN_ERR("Multipart Upload has not started yet.");
        return false;
    }

    AutoLock auto_lock(&upload_list_lock);
    off_t    next_start_pos = 0;
    if(!upload_list.empty()){
        next_start_pos = upload_list.back().startpos + upload_list.back().size;
    }
    if(start != next_start_pos){
        S3FS_PRN_ERR("The expected starting position for the next part is %lld, but %lld was specified.", static_cast<long long int>(next_start_pos), static_cast<long long int>(start));
        return false;
    }

    // make part number
    int partnumber = static_cast<int>(upload_list.size()) + 1;

    // add new part
    etag_entities.push_back(etagpair(NULL, partnumber));        // [NOTE] Create the etag entity and register it in the list.
    etagpair&   etag_entity = etag_entities.back();
    filepart    newpart(false, physical_fd, start, size, is_copy, &etag_entity);
    upload_list.push_back(newpart);

    // set etag pointer
    if(ppetag){
        *ppetag = &etag_entity;
    }

    return true;
}

void PseudoFdInfo::ClearUntreated(bool lock_already_held)
{
    AutoLock auto_lock(&upload_list_lock, lock_already_held ? AutoLock::ALREADY_LOCKED : AutoLock::NONE);

    untreated_list.ClearAll();
}

bool PseudoFdInfo::ClearUntreated(off_t start, off_t size)
{
    AutoLock auto_lock(&upload_list_lock);

    return untreated_list.ClearParts(start, size);
}

bool PseudoFdInfo::GetUntreated(off_t& start, off_t& size, off_t max_size, off_t min_size)
{
    AutoLock auto_lock(&upload_list_lock);

    return untreated_list.GetPart(start, size, max_size, min_size);
}

bool PseudoFdInfo::GetLastUntreated(off_t& start, off_t& size, off_t max_size, off_t min_size)
{
    AutoLock auto_lock(&upload_list_lock);

    return untreated_list.GetLastUpdatedPart(start, size, max_size, min_size);
}

bool PseudoFdInfo::AddUntreated(off_t start, off_t size)
{
    AutoLock auto_lock(&upload_list_lock);

    return untreated_list.AddPart(start, size);
}

void PseudoFdInfo::ExitDirectRead()
{
    AutoLock auto_lock(&direct_read_lock);

    // Once is_direct_read is set to false, it will not be reset again until the file is reopend.
    is_direct_read = false;
    last_read_tail = 0;
    prefetch_cnt = 0;

    // clean up chunks
    if(direct_reader_mgr != NULL){
        direct_reader_mgr->CleanUpChunks();
    }

    return;
}

uint32_t PseudoFdInfo::GetPrefetchCount(off_t offset, size_t size)
{
    S3FS_PRN_DBG("GetPrefetchCount[offset=%ld][size=%ld][last_read_tail=%ld]", offset, size, last_read_tail);

    AutoLock auto_lock(&direct_read_lock);

    if(!is_direct_read){
        return 0;
    }

    const off_t chunk_size = DirectReader::GetChunkSize();
    // if the offset is out of order but still in the previous or next chunk's range,
    // it is considered to be a sequential read. ossfs will double the prefetched data
    // until it reaches the maximum value
    if(last_read_tail == 0 
       || offset == last_read_tail 
       || (offset + chunk_size >= last_read_tail && last_read_tail + chunk_size >= offset)){
        last_read_tail = offset + static_cast<off_t>(size);
        if(prefetch_cnt == 0){
            prefetch_cnt = 1;
        }else{
            prefetch_cnt = std::min(prefetch_cnt * 2, DirectReader::GetPrefetchChunkCount());
        }
    }else{
        last_read_tail = 0;
        prefetch_cnt = 0;
    }

    S3FS_PRN_DBG("GetPrefetchCount[pseudo_fd=%d][offset=%ld][size=%ld][last_read_tail=%ld][prefetch_cnt=%d]", pseudo_fd, offset, size, last_read_tail, prefetch_cnt);
    return prefetch_cnt;
}

ssize_t PseudoFdInfo::DirectReadAndPrefetch(char* bytes, off_t start, size_t size)
{
    S3FS_PRN_DBG("direct read and prefetch[pseudo_fd=%d][physical_fd=%d][offset=%lld][size=%zu]", pseudo_fd, physical_fd, static_cast<long long int>(start), size);

    ssize_t rsize = 0;
    off_t offset = start;
    size_t readsize = size;

    const off_t chunk_size = DirectReader::GetChunkSize();
    const uint32_t max_prefetch_chunks = DirectReader::GetPrefetchChunkCount();
    off_t file_size = direct_reader_mgr->GetFileSize();
    if(size == 0 || start >= file_size){
        return 0;
    }

    uint32_t chunkid_start = offset / chunk_size;
    uint32_t chunkid_end = (offset + readsize - 1) / chunk_size;
    for (uint32_t id = chunkid_start; id <= chunkid_end; id++) {
        off_t chunk_off = offset % chunk_size;
        size_t chunk_len = std::min(readsize, static_cast<size_t>(chunk_size-chunk_off));

        // avoid reading the file out of filesize
        size_t real_read_size = 0;
        if (id * chunk_size + chunk_off >= file_size) {
            real_read_size = 0;
        } else if(id * chunk_size + chunk_off + static_cast<off_t>(chunk_len) > file_size){
            real_read_size = file_size - id * chunk_size - chunk_off;
        } else {
            real_read_size = chunk_len;
        }

        AutoLock auto_lock(&direct_reader_mgr->direct_read_lock);

        // Release chunks to reduce memory usage
        for (auto iter = direct_reader_mgr->chunks.begin(); iter!= direct_reader_mgr->chunks.end(); ) {
            // keep the one before the current chunk without releasing it. Because we assume that when 
            // the read offset is in the previous chunk, it is still read sequentially.
            // keep chunks in [id-1, id+max_prefetch_chunks]
            uint32_t chunkid = iter->first;
            if(chunkid + 1 >= id && chunkid <= id + max_prefetch_chunks){
                iter++;
            } else {
                S3FS_PRN_DBG("release chunk[chunkid=%d]", chunkid);
                delete iter->second;
                iter->second = NULL;
                iter = direct_reader_mgr->chunks.erase(iter);
            }
        }

        if (direct_reader_mgr->chunks.count(id)) {
            S3FS_PRN_DBG("reading from buffer[chunkid=%d][offset=%ld][chunk_off=%ld][real_read_size=%ld]", id, offset, chunk_off, real_read_size);
            assert(chunk_off + static_cast<off_t>(real_read_size) <= direct_reader_mgr->chunks[id]->size);
            memcpy(bytes, direct_reader_mgr->chunks[id]->buf + chunk_off, real_read_size);
        } else {
            // if the chunk does not exist, we should download it from oss directly.
            S3FS_PRN_DBG("reading from cloud[chunkid=%d][start=%ld][chunk_off=%ld][real_read_size=%ld]", id, offset, chunk_off,real_read_size);
            off_t direct_read_size = std::min(chunk_size, file_size - id * chunk_size);
            DirectReadParam* direct_read_param = new DirectReadParam;
            direct_read_param->len = direct_read_size;
            direct_read_param->start = id * chunk_size;
            direct_read_param->direct_reader = direct_reader_mgr;
            direct_read_param->is_sync_download = true;
            direct_read_worker(static_cast<void*>(direct_read_param));
            
            if (direct_reader_mgr->chunks.count(id)) {
                assert(chunk_off + static_cast<off_t>(real_read_size) <= direct_reader_mgr->chunks[id]->size);
                memcpy(bytes, direct_reader_mgr->chunks[id]->buf + chunk_off, real_read_size);
            } else {
                return -EIO;
            }
        }

        if (real_read_size < chunk_len) { 
            // finish reading the file.
            return rsize + real_read_size;   
        }

        offset += chunk_len;
        readsize -= chunk_len;
        bytes += chunk_len;
        rsize += chunk_len;
    }

    if(max_prefetch_chunks != 0){
        uint32_t prefetch_cnt = GetPrefetchCount(start, size);
        GeneratePrefetchTask(chunkid_end, prefetch_cnt);
    }

    return rsize;
}

void PseudoFdInfo::GeneratePrefetchTask(uint32_t start_prefetch_chunk, uint32_t prefetch_cnt)
{
    const off_t chunk_size = DirectReader::GetChunkSize();
    const off_t file_size = direct_reader_mgr->GetFileSize();
    uint32_t max_prefetch_chunk = (file_size - 1) / chunk_size; 
    uint32_t last_prefetch_chunk = std::min(max_prefetch_chunk, start_prefetch_chunk + prefetch_cnt);

    AutoLock auto_lock(&direct_reader_mgr->direct_read_lock);

    // if ongoing_prefetch is not 0, it means thera are some prefetch tasks generated last time running. 
    // skip generate new tasks here inorder to avoid prefetch a chunk twice.
    if (direct_reader_mgr->ongoing_prefetch == 0) {
        direct_reader_mgr->ongoing_prefetch = last_prefetch_chunk - start_prefetch_chunk;
    } else {
        last_prefetch_chunk = start_prefetch_chunk;
    }

    S3FS_PRN_DBG("generate prefetch task[start_chunk=%d][end_chunk=%d]", start_prefetch_chunk+1, last_prefetch_chunk);
    for (uint32_t i = start_prefetch_chunk + 1 ; i <= last_prefetch_chunk; i++) {
        if (!direct_reader_mgr->chunks.count(i) && Chunk::cache_usage_check()) {
            off_t prefetch_size = std::min(chunk_size, file_size - i * chunk_size);
            if (!direct_reader_mgr->Prefetch(i * chunk_size, prefetch_size)) {
                direct_reader_mgr->ongoing_prefetch--;
            }
        } else {
            direct_reader_mgr->ongoing_prefetch--;
        }
    }
    return;
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
