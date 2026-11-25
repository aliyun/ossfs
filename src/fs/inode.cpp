/*
 * Copyright 2025 The Ossfs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "inode.h"

#include <dirent.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>
#include <functional>

#include "common/filesystem.h"

namespace OssFileSystem {

static constexpr size_t kDefaultDirSize = 4096;

bool Inode::can_be_invalidated() const {
  return lookup_cnt == 0 && ref_ctr == 0;
}

void Inode::update_attr(uint64_t file_size, struct timespec file_mtime,
                        struct timespec file_atime) {
  // Dir size is never updated: fill_statbuf always reports a fixed size for
  // dirs.
  if (!is_dir()) {
    attr.size = file_size;
  }

  // A zero mtime means the backend did not provide one (e.g. OSS dir stats
  // are list-based probes without mtimes), so keep the local value.
  if (file_mtime.tv_sec != 0) {
    attr.mtime = file_mtime;
  }
  if (file_atime.tv_sec != 0) {
    set_atime(file_atime);
  }
  attr_time = time(nullptr);
}

void Inode::fill_statbuf(struct stat *stbuf) const {
  stbuf->st_ino = nodeid;
  stbuf->st_mode = get_mode();
  stbuf->st_uid = get_uid();
  stbuf->st_gid = get_gid();

  stbuf->st_nlink = S_ISDIR(stbuf->st_mode) ? 2 : 1;
  stbuf->st_size = S_ISDIR(stbuf->st_mode) ? kDefaultDirSize : attr.size;
  stbuf->st_mtim = attr.mtime;
  stbuf->st_atim = get_atime();
  stbuf->st_ctim = stbuf->st_mtim;
  stbuf->st_blksize = Attribute::DEFAULT_BLKSIZE;
  stbuf->st_blocks = stbuf->st_size >> 9;
}

InodeType Inode::dirent_type_to_inode_type(unsigned char dtype) {
  switch (dtype) {
    case DT_DIR:
      return InodeType::kDir;
    case DT_LNK:
      return InodeType::kSymlink;
    default:
      return InodeType::kFile;
  }
}

InodeType Inode::mode_to_inode_type(mode_t mode) {
  if (S_ISDIR(mode)) return InodeType::kDir;
  if (S_ISLNK(mode)) return InodeType::kSymlink;
  return InodeType::kFile;
}

std::string Inode::inode_type_to_string(InodeType type) {
  switch (type) {
    case InodeType::kDir:
      return "dir";
    case InodeType::kFile:
      return "file";
    case InodeType::kSymlink:
      return "symlink";
    default:
      return "unknown";
  }
}

void FileInode::refresh_etag(const struct stat *stbuf,
                             const CompactEtag &remote_etag) {
  if (is_data_changed(stbuf, remote_etag)) {
    invalidate_data_cache = true;
  }
  etag = remote_etag;
}

bool FileInode::is_data_changed(const struct stat *stbuf,
                                const CompactEtag &remote_etag) const {
  if (!etag.empty() && !remote_etag.empty()) {
    return !remote_etag.equals(etag) || uint64_t(stbuf->st_size) != attr.size;
  }

  // No etag available (HDFS, or never fetched): fall back to size + mtime.
  return uint64_t(stbuf->st_size) != attr.size ||
         stbuf->st_mtim.tv_nsec != attr.mtime.tv_nsec ||
         stbuf->st_mtim.tv_sec != attr.mtime.tv_sec;
}

DirChildren::~DirChildren() {
  free(buckets_);
}

Inode *DirChildren::find(std::string_view name) const {
  size_t i = bucketed() ? bucket_of(hash_name(name), nbuckets_) : 0;
  for (Inode *e = chain_head(i); e != nullptr; e = e->child_next) {
    if (e->name == name) return e;
  }
  return nullptr;
}

void DirChildren::insert_replace(Inode *inode) {
  Inode **slot = find_slot(inode->name);
  if (*slot == inode) return;
  if (*slot != nullptr) detach(slot);
  attach(slot, inode);
}

void DirChildren::insert_unique(Inode *inode) {
  Inode **slot = find_slot(inode->name);
  if (*slot != nullptr) return;
  attach(slot, inode);
}

void DirChildren::erase(std::string_view name, uint64_t nodeid) {
  Inode **slot = find_slot(name);
  if (*slot == nullptr || (*slot)->nodeid != nodeid) return;
  detach(slot);
  shrink_after_erase();
}

size_t DirChildren::hash_name(std::string_view name) {
  return std::hash<std::string_view>()(name);
}

size_t DirChildren::bucket_of(size_t hash, size_t nbuckets) {
  return hash & (nbuckets - 1);
}

// The slot holding `name`, or the empty slot it would be linked into.
Inode **DirChildren::find_slot(std::string_view name) {
  Inode **pp = &head_;
  if (bucketed()) pp = &buckets_[bucket_of(hash_name(name), nbuckets_)];
  for (; *pp != nullptr; pp = &(*pp)->child_next) {
    if ((*pp)->name == name) return pp;
  }
  return pp;
}

// slot is dead once this returns: growing can realloc buckets_ out from under
// it, which is why the link is written before the size is checked. Overwriting
// child_next would truncate a chain the inode is still linked into, so the
// caller must have unlinked it first.
void DirChildren::attach(Inode **slot, Inode *inode) {
  RELEASE_ASSERT(inode->child_next == nullptr);
  inode->child_next = *slot;
  *slot = inode;
  count_++;
  if (bucketed()) {
    if (count_ > nbuckets_) grow_buckets();
  } else if (count_ > kBucketizeAbove) {
    build_buckets();
  }
}

// Never shrinks: shrinking can free buckets_, and insert_replace keeps
// pointing at its slot across the detach.
void DirChildren::detach(Inode **slot) {
  Inode *e = *slot;
  *slot = e->child_next;
  e->child_next = nullptr;
  count_--;
}

void DirChildren::build_buckets() {
  Inode **buckets = static_cast<Inode **>(calloc(kMinBuckets, sizeof(Inode *)));
  if (buckets == nullptr) {
    // Staying a list costs lookup time, not correctness.
    LOG_ERROR("failed to allocate ` child index slots", kMinBuckets);
    return;
  }
  buckets_ = buckets;
  nbuckets_ = kMinBuckets;
  for (Inode *e = head_; e != nullptr;) {
    Inode *next = e->child_next;
    size_t b = bucket_of(hash_name(e->name), nbuckets_);
    e->child_next = buckets_[b];
    buckets_[b] = e;
    e = next;
  }
  head_ = nullptr;
}

void DirChildren::shrink_after_erase() {
  if (!bucketed()) return;
  if (count_ <= kFlattenBelow) {
    flatten_to_list();
    return;
  }
  if (nbuckets_ > kMinBuckets && count_ * 4 < nbuckets_) {
    merge_buckets();
  }
}

void DirChildren::flatten_to_list() {
  Inode *new_head = nullptr;
  for (size_t i = 0; i < nbuckets_; i++) {
    for (Inode *e = buckets_[i]; e != nullptr;) {
      Inode *next = e->child_next;
      e->child_next = new_head;
      new_head = e;
      e = next;
    }
  }
  head_ = new_head;
  free(buckets_);
  buckets_ = nullptr;
  nbuckets_ = 0;
}

// Doubling exposes one more hash bit, so a key in bucket i can only move to i
// or i + old. realloc leaves the old half where it is and only the new half
// needs zeroing, so the chains split in place.
void DirChildren::grow_buckets() {
  size_t old = nbuckets_;
  Inode **grown =
      static_cast<Inode **>(realloc(buckets_, old * 2 * sizeof(Inode *)));
  if (grown == nullptr) {
    // Only longer chains; still correct.
    LOG_ERROR("failed to grow child index to ` slots", old * 2);
    return;
  }
  buckets_ = grown;
  memset(buckets_ + old, 0, old * sizeof(Inode *));
  nbuckets_ = old * 2;

  for (size_t i = 0; i < old; i++) {
    Inode *stay = nullptr;
    Inode **stay_tail = &stay;
    Inode *move = nullptr;
    Inode **move_tail = &move;
    for (Inode *e = buckets_[i]; e != nullptr; e = e->child_next) {
      if (hash_name(e->name) & old) {
        *move_tail = e;
        move_tail = &e->child_next;
      } else {
        *stay_tail = e;
        stay_tail = &e->child_next;
      }
    }
    *stay_tail = nullptr;
    *move_tail = nullptr;
    buckets_[i] = stay;
    buckets_[i + old] = move;
  }
}

// Halving needs no hash at all: buckets i and i + half hold exactly the keys
// that map to i at half the size, so the two chains just splice.
void DirChildren::merge_buckets() {
  size_t half = nbuckets_ / 2;
  for (size_t i = 0; i < half; i++) {
    Inode **tail = &buckets_[i];
    while (*tail != nullptr) tail = &(*tail)->child_next;
    *tail = buckets_[i + half];
    buckets_[i + half] = nullptr;
  }
  nbuckets_ = half;
  // Purely an optimization; on failure keep the larger block.
  Inode **shrunk =
      static_cast<Inode **>(realloc(buckets_, half * sizeof(Inode *)));
  if (shrunk != nullptr) buckets_ = shrunk;
}

void DirInode::add_child_node(Inode *inode) {
  children.insert_replace(inode);
}

void DirInode::add_child_node_directly(Inode *inode) {
  children.insert_unique(inode);
}

bool DirInode::can_be_invalidated() const {
  // When we use drop_cache to evict inodes, parent_inode's forget may come
  // earlier than its descendants' forget. Therefore we need to check if
  // inode->children is empty here and if not, we will delete the paren_inode's
  // later recursively.
  return lookup_cnt == 0 && ref_ctr == 0 && children.empty();
}

bool DirInode::is_dir_empty(std::string *child_name) const {
  // There is a chance that a child was active when it's visited
  // and then becomes stale, so it may return false for an empty dir,
  // but it will never return true for a non-empty dir.
  Inode *live = children.find_if([](Inode *child) { return !child->is_stale; });
  if (live == nullptr) return true;
  if (child_name) child_name->assign(live->name);
  return false;
}

Inode *DirInode::find_child_node(std::string_view name) const {
  return children.find(name);
}

// Erase before the child_node->name changes (deleted or renamed), so the index
// never holds an inode whose name no longer matches its list/bucket position.
void DirInode::erase_child_node(std::string_view name, const uint64_t nodeid) {
  children.erase(name, nodeid);
}

}  // namespace OssFileSystem
