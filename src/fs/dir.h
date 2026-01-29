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

#pragma once

#include <fcntl.h>

#include <functional>
#include <map>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "inode.h"
#include "oss/oss_adapter.h"
#include "test/class_declarations.h"

namespace OssFileSystem {

class OssFs;

class OssDirHandle {
 public:
  // Dir handle lock. Protect the fill_cnt_, cur_list_res_ and
  // dirty_children_ when multiple threads access the dir stream with the same
  // dir handle.
  std::mutex dir_lock_;

  OssDirHandle(OssFs *fs, DirInode *inode, const std::string &path,
               uint32_t last_resp_cap)
      : fs_(fs),
        inode_(inode),
        full_path_(path),
        has_tried_listing_(false),
        pos_(0),
        fill_cnt_(0),
        last_response_cap_(last_resp_cap) {}

  DirInode *inode() const {
    return inode_;
  }

  // get() is always preceded by a refresh_dir() or a next()
  const OssDirent *get() const {
    if (dirty_child_iter_ != dirty_children_.end()) {
      return &(dirty_child_iter_->second);
    }

    if (pos_ < cur_list_res_.size()) {
      return &cur_list_res_[pos_];
    }
    return nullptr;
  }
  int next(bool *need_construct_inodes = nullptr);

  off_t telldir() const {
    return fill_cnt_;
  }

  int refresh_dir(std::map<estring, OssDirent> &dirty_children);

  bool insert_pending_fill_nodeids(uint64_t nodeid) {
    auto it = pending_fill_nodeids_.find(nodeid);
    if (it != pending_fill_nodeids_.end()) {
      return false;
    }

    pending_fill_nodeids_.insert(nodeid);
    return true;
  }

  bool erase_pending_fill_nodeids(uint64_t nodeid) {
    auto it = pending_fill_nodeids_.find(nodeid);
    if (it == pending_fill_nodeids_.end()) {
      return false;
    }

    pending_fill_nodeids_.erase(it);
    return true;
  }

  void get_pending_fill_nodeids(std::unordered_set<uint64_t> &nodeids) {
    pending_fill_nodeids_.swap(nodeids);
  }

  bool out_of_order(off_t off, bool *need_construct_inodes = nullptr);

  void increment_fill_cnt() {
    ++fill_cnt_;
  }

  void get_cur_list_res(std::vector<OssDirent> &ents) const {
    ents = cur_list_res_;
  }

  std::string_view get_full_path() const {
    return full_path_;
  }

  size_t get_list_pos() const {
    return pos_;
  }

 private:
  OssFs *fs_ = nullptr;
  DirInode *inode_ = nullptr;
  const std::string full_path_;

  // cur_list_res_ saves the results of the last ListObj req,
  // and is cleared when readdir has filled all the entries.
  std::vector<OssDirent> cur_list_res_;
  std::string last_marker_;
  bool has_tried_listing_ = false;

  size_t pos_ = 0;
  off_t fill_cnt_ = 0;

  std::vector<OssDirent> last_response_;
  const size_t last_response_cap_ = 100;

  std::map<estring, OssDirent> dirty_children_;
  std::map<estring, OssDirent>::iterator dirty_child_iter_;

  std::unordered_set<uint64_t> pending_fill_nodeids_;

  DECLARE_TEST_FRIENDS_CLASSES;
};

};  // namespace OssFileSystem