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

#ifndef S3FS_CACHE_H_
#define S3FS_CACHE_H_

#include "metaheader.h"

//-------------------------------------------------------------------
// Structure
//-------------------------------------------------------------------
//
// Struct for stats cache
//
struct stat_cache_entry {
    struct stat       stbuf;
    unsigned long     hit_count;
    struct timespec   cache_date;
    headers_t         meta;
    bool              isforce;
    bool              noobjcache;  // Flag: cache is no object for no listing.
    unsigned long     notruncate;  // 0<:   not remove automatically at checking truncate
    bool              isfake;   // Flag: meta is built from listobject result.

    stat_cache_entry() : hit_count(0), isforce(false), noobjcache(false), notruncate(0L), isfake(false)
    {
        memset(&stbuf, 0, sizeof(struct stat));
        cache_date.tv_sec  = 0;
        cache_date.tv_nsec = 0;
        meta.clear();
    }
};

typedef std::map<std::string, stat_cache_entry*> stat_cache_t; // key=path

//
// Struct for symbolic link cache
//
struct symlink_cache_entry {
    std::string       link;
    unsigned long     hit_count;
    struct timespec   cache_date;  // The function that operates timespec uses the same as Stats

    symlink_cache_entry() : link(""), hit_count(0)
    {
      cache_date.tv_sec  = 0;
      cache_date.tv_nsec = 0;
    }
};

typedef std::map<std::string, symlink_cache_entry*> symlink_cache_t;

//-------------------------------------------------------------------
// Class StatCache
//-------------------------------------------------------------------
// [NOTE] About Symbolic link cache
// The Stats cache class now also has a symbolic link cache.
// It is possible to take out the Symbolic link cache in another class,
// but the cache out etc. should be synchronized with the Stats cache
// and implemented in this class.
// Symbolic link cache size and timeout use the same settings as Stats
// cache. This simplifies user configuration, and from a user perspective,
// the symbolic link cache appears to be included in the Stats cache.
//
class StatCache
{
    private:
        static StatCache       singleton;
        static pthread_mutex_t stat_cache_lock;
        stat_cache_t           stat_cache;
        bool                   IsExpireTime;
        bool                   IsExpireIntervalType;    // if this flag is true, cache data is updated at last access time.
        time_t                 ExpireTime;
        unsigned long          CacheSize;
        bool                   IsCacheNoObject;
        symlink_cache_t        symlink_cache;
        bool                   IsNoExtendedMeta;
        off_t                  CheckSizeForMeta;

    private:
        StatCache();
        ~StatCache();

        void Clear();
        bool GetStat(const std::string& key, struct stat* pst, headers_t* meta, bool overcheck, const char* petag, bool* pisforce, bool *pisfake);
        // Truncate stat cache
        bool TruncateCache();
        // Truncate symbolic link cache
        bool TruncateSymlink();

    public:
        // Reference singleton
        static StatCache* getStatCacheData()
        {
            return &singleton;
        }

        // Attribute
        unsigned long GetCacheSize() const;
        unsigned long SetCacheSize(unsigned long size);
        time_t GetExpireTime() const;
        time_t SetExpireTime(time_t expire, bool is_interval = false);
        time_t UnsetExpireTime();
        bool SetCacheNoObject(bool flag);
        bool EnableCacheNoObject()
        {
            return SetCacheNoObject(true);
        }
        bool DisableCacheNoObject()
        {
            return SetCacheNoObject(false);
        }
        bool GetCacheNoObject() const
        {
            return IsCacheNoObject;
        }

        bool SetNoExtendedMeta(bool flag, off_t check_size);

        bool EnableNoExtendedMeta()
        {
            return SetNoExtendedMeta(true, 0);
        }
        bool DisableNoExtendedMeta()
        {
            return SetNoExtendedMeta(false, 0);
        }

        // Get stat cache
        bool GetStat(const std::string& key, struct stat* pst, headers_t* meta, bool overcheck = true, bool* pisforce = NULL, bool *pisfake = NULL)
        {
            return GetStat(key, pst, meta, overcheck, NULL, pisforce, pisfake);
        }
        bool GetStat(const std::string& key, struct stat* pst, bool overcheck = true)
        {
            return GetStat(key, pst, NULL, overcheck, NULL, NULL, NULL);
        }
        bool GetStat(const std::string& key, headers_t* meta, bool overcheck = true)
        {
            return GetStat(key, NULL, meta, overcheck, NULL, NULL, NULL);
        }
        bool HasStat(const std::string& key, bool overcheck = true)
        {
            return GetStat(key, NULL, NULL, overcheck, NULL, NULL, NULL);
        }
        bool HasStat(const std::string& key, const char* etag, bool overcheck = true)
        {
            return GetStat(key, NULL, NULL, overcheck, etag, NULL, NULL);
        }

        // Cache For no object
        bool IsNoObjectCache(const std::string& key, bool overcheck = true);
        bool AddNoObjectCache(const std::string& key);

        // Add stat cache
        bool AddStat(const std::string& key, headers_t& meta, bool forcedir = false, bool no_truncate = false, bool isfake = false);

        // Update meta stats
        bool UpdateMetaStats(const std::string& key, headers_t& meta);

        // Change no truncate flag
        void ChangeNoTruncateFlag(const std::string& key, bool no_truncate);

        // Delete stat cache
        bool DelStat(const char* key, bool lock_already_held = false);
        bool DelStat(const std::string& key, bool lock_already_held = false)
        {
            return DelStat(key.c_str(), lock_already_held);
        }
        // Cache for symbolic link
        bool GetSymlink(const std::string& key, std::string& value);
        bool AddSymlink(const std::string& key, const std::string& value);
        bool DelSymlink(const char* key, bool lock_already_held = false);

        // header meta to stat
        bool ConvertMetaToStat(const std::string& strpath, const headers_t& meta, struct stat* pst, bool forcedir);

        // header meta to stat
        bool ToTimeStat(const headers_t& meta, struct stat* pst);
};


static inline bool is_check_meta(off_t size, off_t max_size) { return (size == 0) || (size <= max_size); }

#endif // S3FS_CACHE_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
