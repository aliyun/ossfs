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

#include "dir.h"

#include "fs.h"

namespace OssFileSystem {

bool OssDirHandle::out_of_order(off_t off, bool *is_offset_tuned) {
  if (is_offset_tuned) *is_offset_tuned = false;

  if (off == fill_cnt_) return false;

  if (dirty_child_iter_ != dirty_children_.end()) return true;
  if (unlikely(off > fill_cnt_)) return true;

  // Only jumping backwards is supported.
  auto diff = fill_cnt_ - off;
  int index = (int)pos_ - 1;
  for (; index >= 0; index--) {
    if (!dirty_children_.count(cur_list_res_[index].name()) && (--diff == 0)) {
      // clang-format off
      LOG_INFO("[cur_list_res] Jump backwards to `, fill_cnt: `, cur_list_res size: `, pos_: `, last_response size: `",
          off, fill_cnt_, cur_list_res_.size(), pos_, last_response_.size());
      // clang-format on
      break;
    }
  }

  if (diff > 0 && !last_response_.empty()) {
    index = last_response_.size() - 1;
    for (; index >= 0; index--) {
      if (!dirty_children_.count(last_response_[index].name()) &&
          (--diff == 0)) {
        // clang-format off
        LOG_INFO("[last_response] Jump backwards to `, fill_cnt: `, cur_list_res size: `, pos_: `, last_response size: `",
            off, fill_cnt_, cur_list_res_.size(), pos_, last_response_.size());
        // clang-format on
        cur_list_res_.insert(cur_list_res_.begin(),
                             last_response_.begin() + index,
                             last_response_.end());
        last_response_.erase(last_response_.begin() + index);
        index = 0;
        break;
      }
    }
  }

  if (0 == diff) {
    pos_ = index;
    fill_cnt_ = off;
    if (is_offset_tuned) *is_offset_tuned = true;
    return false;
  }
  return true;
}

// If next() returns 0, the next get() will not return nullptr.
int OssDirHandle::next(bool *need_construct_inodes) {
  if (need_construct_inodes) *need_construct_inodes = false;

  // Case 1: there still are dirty children not loaded.
  if (dirty_child_iter_ != dirty_children_.end() &&
      (++dirty_child_iter_) != dirty_children_.end()) {
    return 1;
  }

  std::string_view entry_name;
  int r = 0;
  do {
    // Case 2: not list over, keep consuming cur_list_res_.
    ++pos_;
    if (pos_ < cur_list_res_.size()) {
      entry_name = cur_list_res_[pos_].name();
      continue;
    }

    // Case 3: cur_list_res_ has been all consumed.
    // Need to trigger a new ListObj req.
    pos_ = 0;
    // Must clear cur_list_res_ here. If the list is over and last marker is
    // empty, readdir() will return 0 this time. And we need to make sure the
    // next readdir() won't get anything.
    auto start_iter = cur_list_res_.begin();
    if (cur_list_res_.size() > last_response_cap_) {
      start_iter += cur_list_res_.size() - last_response_cap_;
    }
    last_response_.assign(start_iter, cur_list_res_.end());

    cur_list_res_.clear();
    if (!has_tried_listing_ || !last_marker_.empty()) {
      if (need_construct_inodes) *need_construct_inodes = false;
      do {
        r = fs_->get_one_list_results(full_path_, cur_list_res_, last_marker_);
        if (r < 0) return r;
      } while (!last_marker_.empty() && cur_list_res_.empty());
      has_tried_listing_ = true;

      if (!cur_list_res_.empty()) {
        if (need_construct_inodes) *need_construct_inodes = true;
        entry_name = cur_list_res_[pos_].name();
        continue;
      }
    }

    // Case 4: list is over, readdir is completed now.
    return 0;
  } while (dirty_children_.count(entry_name));
  return 1;
}

// Called every time when offset == 0.
// Make sure the next get() will not return nullptr if
// dirty_children_ is not empty or the list result is not empty.
int OssDirHandle::refresh_dir(std::map<estring, OssDirent> &dirty_children) {
  dirty_children_.swap(dirty_children);
  dirty_child_iter_ = dirty_children_.begin();
  fill_cnt_ = 0;
  pos_ = 0;

  last_marker_.clear();  // list from the beginning
  last_response_.clear();
  cur_list_res_.clear();
  has_tried_listing_ = false;

  if (!dirty_children_.empty()) return 0;

  // The order is seek_dir(refresh_dir) -> get() -> next(), so we should make
  // sure the next get() right after refresh_dir() can get a valid entry.
  // Therefore, if there are no dirty children, we should trigger a new ListObj
  // req to make sure the next get() can read from cur_list_res_[0].
  int r = 0;
  // Maybe the obj name is invalid and we get an empty cur_list_res_ (e.g.
  // max_keys=1). Retry until we get a non-empty cur_list_res_.
  do {
    r = fs_->get_one_list_results(full_path_, cur_list_res_, last_marker_);
    if (r < 0) return r;
  } while (!last_marker_.empty() && cur_list_res_.empty());
  has_tried_listing_ = true;

  return r;
}

}  // namespace OssFileSystem