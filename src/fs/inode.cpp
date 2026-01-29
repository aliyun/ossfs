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

namespace OssFileSystem {

bool Inode::can_be_invalidated() const {
  return lookup_cnt == 0 && ref_ctr == 0;
}

void Inode::update_attr(uint64_t file_size, struct timespec file_mtime) {
  // No need to update dir attr.
  if (is_dir()) {
    return;
  }

  attr.size = file_size;
  attr.mtime = file_mtime;

  attr_time = time(nullptr);
}

void Inode::fill_statbuf(struct stat *stbuf) const {
  stbuf->st_ino = nodeid;
  stbuf->st_mode = Attribute::get_mode(type);
  if (nodeid == kMountPointNodeId) {
    stbuf->st_nlink = 2;
  } else {
    stbuf->st_nlink = 1;
  }
  stbuf->st_uid = Attribute::DEFAULT_UID;
  stbuf->st_gid = Attribute::DEFAULT_GID;
  stbuf->st_mtim = attr.mtime;
  stbuf->st_size = attr.size;

  stbuf->st_atim = stbuf->st_mtim;
  stbuf->st_ctim = stbuf->st_mtim;

  stbuf->st_blksize = 4096;
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

void FileInode::invalidate_data_cache_if_needed(const struct stat *stbuf,
                                                std::string_view remote_etag) {
  if (is_data_changed(stbuf, remote_etag)) {
    invalidate_data_cache = true;
  }
}

bool FileInode::is_data_changed(const struct stat *stbuf,
                                std::string_view remote_etag) const {
  return etag != remote_etag || uint64_t(stbuf->st_size) != attr.size ||
         stbuf->st_mtim.tv_nsec != attr.mtime.tv_nsec ||
         stbuf->st_mtim.tv_sec != attr.mtime.tv_sec;
}

// The key is always the string_view of inode->name. So take care when
// inode->name changes.
void DirInode::add_child_node(Inode *inode) {
  auto it = children.find(inode->name);
  // The value could refer to a different inode with the same string_view typed
  // key. So we need to erase the old one to make sure the key refers to the
  // value inode.
  if (it != children.end()) children.erase(it);
  children.emplace(inode->name, inode);
}

void DirInode::add_child_node_directly(Inode *inode) {
  assert(children.find(inode->name) == children.end());
  children.emplace(inode->name, inode);
}

// Protected by inodes_map_lck_.
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
  for (auto &child : children) {
    if (!(child.second->is_stale)) {
      if (child_name) child_name->assign(child.second->name);
      return false;
    }
  }

  return true;
}

Inode *DirInode::find_child_node(std::string_view name) const {
  auto it = children.find(name);
  if (it == children.end()) {
    return nullptr;
  }

  return it->second;
}

// Erase before the child_node->name changes (deleted or renamed), to make sure
// all of the keys in children are valid.
void DirInode::erase_child_node(std::string_view name, const uint64_t nodeid) {
  auto iter = children.find(name);
  if (iter == children.end() || iter->second->nodeid != nodeid) {
    return;
  }

  children.erase(iter);
}

}  // namespace OssFileSystem