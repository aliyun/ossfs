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

#include <gflags/gflags.h>

#include <climits>

#include "common/utils.h"
#include "fs.h"

DEFINE_bool(enable_locking_debug_logs, false, "enable locking debug logs");

namespace OssFileSystem {

#define PATHLOCK_WRITE -1
#define PATHLOCK_WAIT_OFFSET INT_MIN

struct LockQueueElement {
  struct LockQueueElement *next = nullptr;
  std::condition_variable cv;

  Inode *parent1 = nullptr;
  std::string *path1 = nullptr;
  std::string_view name1;
  Inode **wnode1 = nullptr;

  Inode *parent2 = nullptr;
  std::string *path2 = nullptr;
  std::string_view name2;
  Inode **wnode2 = nullptr;

  bool done = false;
  int err = 0;
};

static void unlock_path(Inode *parent, Inode *wnode, Inode *end) {
  if (wnode) {
    RELEASE_ASSERT(wnode->pathlock == PATHLOCK_WRITE);
    wnode->pathlock = 0;
    wnode->ref_ctr--;
  }

  for (auto inode = parent; inode->nodeid != kMountPointNodeId && inode != end;
       inode = inode->parent) {
    RELEASE_ASSERT(inode->pathlock != 0);
    RELEASE_ASSERT(inode->pathlock != PATHLOCK_WRITE);
    RELEASE_ASSERT(inode->pathlock != PATHLOCK_WAIT_OFFSET);
    inode->pathlock--;
    inode->ref_ctr--;
    if (inode->pathlock == PATHLOCK_WAIT_OFFSET) inode->pathlock = 0;
  }
}

static int try_get_path(Inode *parent, std::string_view name, std::string *path,
                        Inode **wnode) {
  RELEASE_ASSERT(parent);
  auto inode = parent;
  if (inode->is_stale) return -ESTALE;

  int err = 0;
  Inode *last = nullptr;
  std::string temp_path;
  std::vector<Inode *> nodes;
  if (path) nodes.reserve(10);
  if (!name.empty()) {
    RELEASE_ASSERT(inode->is_dir());
    // Find the child inode. It's safe here as the child map is also protected
    // by global inode map lock.
    last = static_cast<DirInode *>(inode)->find_child_node(name);
    if (last && !last->is_stale) {
      if (last->pathlock != 0) {
        if (last->pathlock > 0) last->pathlock += PATHLOCK_WAIT_OFFSET;
        return -EAGAIN;
      }
      last->pathlock = PATHLOCK_WRITE;
      last->ref_ctr++;
    } else {
      last = nullptr;
    }
    // Since we are with global map lock held, the parent-child view is
    // immutable. Just go through the lock acquire flow.
  } else {
    // This is the case we are trying to get write lock for getattr.
    if (wnode) {
      // Grab the write lock for the target node.
      if (inode->pathlock != 0) {
        if (inode->pathlock > 0) inode->pathlock += PATHLOCK_WAIT_OFFSET;
        return -EAGAIN;
      }
      inode->pathlock = PATHLOCK_WRITE;
      inode->ref_ctr++;
      if (path) nodes.push_back(inode);
      last = inode;
      parent = inode->parent;
      inode = parent;
    }
  }

  // Other nodes are with rlock.
  for (; inode->nodeid != kMountPointNodeId; inode = inode->parent) {
    err = -ESTALE;
    if (inode->is_stale) goto out_unlock;

    err = -EAGAIN;
    if (inode->pathlock < 0) goto out_unlock;
    inode->pathlock++;
    inode->ref_ctr++;
    if (path) nodes.push_back(inode);
  }

  if (path) {
    temp_path.reserve(OSS_MAX_PATH_LEN + 1);
    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it) {
      temp_path.append("/").append((*it)->name);
    }
    if (temp_path.empty()) temp_path.append("/");
    path->swap(temp_path);
  }
  if (wnode) *wnode = last;
  return 0;
out_unlock:
  unlock_path(parent, last, inode);
  return err;
}

static int try_get_path2(Inode *parent1, std::string_view name1, Inode *parent2,
                         std::string_view name2, std::string *path1,
                         std::string *path2, Inode **wnode1, Inode **wnode2) {
  int err = try_get_path(parent1, name1, path1, wnode1);
  if (!err) {
    err = try_get_path(parent2, name2, path2, wnode2);
    if (err) {
      Inode *wn1 = wnode1 ? *wnode1 : nullptr;
      unlock_path(parent1, wn1, nullptr);
      if (path1) *path1 = "";
      if (wnode1) *wnode1 = nullptr;
    }
  }
  return err;
}

static void queue_element_wakeup(LockQueueElement *qe) {
  if (qe->done) return;

  int err = 0;
  if (!qe->path2) {
    err = try_get_path(qe->parent1, qe->name1, qe->path1, qe->wnode1);
  } else {
    err = try_get_path2(qe->parent1, qe->name1, qe->parent2, qe->name2,
                        qe->path1, qe->path2, qe->wnode1, qe->wnode2);
  }

  if (err == -EAGAIN) return;

  qe->err = err;
  qe->done = true;
  qe->cv.notify_one();
}

static void wake_up_queued(LockQueueElement *head) {
  LockQueueElement *qe;

  for (qe = head; qe != nullptr; qe = qe->next) queue_element_wakeup(qe);
}

static void queue_path(LockQueueElement **head_ptr, LockQueueElement *qe) {
  LockQueueElement **qp;

  qe->done = false;
  qe->next = nullptr;
  for (qp = head_ptr; *qp != nullptr; qp = &(*qp)->next)
    ;
  *qp = qe;
}

static void dequeue_path(LockQueueElement **head_ptr, LockQueueElement *qe) {
  LockQueueElement **qp;

  for (qp = head_ptr; *qp != qe; qp = &(*qp)->next)
    ;
  *qp = qe->next;
}

static int wait_path(LockQueueElement **head_ptr, LockQueueElement *qe,
                     std::unique_lock<std::mutex> &l) {
  queue_path(head_ptr, qe);
  do {
    qe->cv.wait(l);
  } while (!qe->done);

  dequeue_path(head_ptr, qe);
  return qe->err;
}

// Lock inode and all its ancestors with read lock.
// Increase inode reference for nodeid.
const InodeRef OssFs::get_inode_ref(uint64_t nodeid,
                                    InodeRefPathType path_type) {
  InodeRef ref;
  std::unique_lock<std::mutex> l(inodes_map_lck_);
  auto it = global_inodes_map_.find(nodeid);
  if (it == global_inodes_map_.end()) return ref;
  auto inode = it->second;
  int err = 0;

  // Make sure inode exists after we drop the mutex and wait, so we don't
  // have to find the inode from map again after getting the lock.
  inode->ref_ctr++;
  bool should_try_invalidate = false;
  bool acquire_path = path_type != InodeRefPathType::kPathTypeNone;
  bool need_write_path = path_type == InodeRefPathType::kPathTypeWrite;
  if (acquire_path) {
    err = try_get_path(inode, {}, &ref.inode_path,
                       need_write_path ? &ref.inode : nullptr);
    if (err == -EAGAIN) {
      LockQueueElement qe;
      qe.parent1 = inode;
      qe.path1 = &ref.inode_path;
      qe.wnode1 = need_write_path ? &ref.inode : nullptr;
      err = wait_path(&path_lockq_, &qe, l);
    }
    inode->ref_ctr--;  // try_get_path will increase ref_ctr there and release
                       // the ref we add in this function
    if (err) {
      should_try_invalidate = inode->ref_ctr == 0;
    }
  }
  l.unlock();

  if (!err) {
    if (acquire_path) {
      if (FLAGS_enable_locking_debug_logs)
        LOG_DEBUG("locking ipath ` with `lock", ref.inode_path,
                  need_write_path ? "w" : "r");
      ref.acquire_write_path = need_write_path;
    }
    if (!ref.inode) ref.inode = inode;
  } else {
    LOG_ERROR("get inode ` failed with ` trying to invalidate `", nodeid, err,
              should_try_invalidate ? "true" : "false");
  }
  if (should_try_invalidate) try_invalidate_inode(nodeid, 0, true);
  return ref;
}

void OssFs::return_inode_ref(const InodeRef &ref) {
  if (!ref.inode) return;
  bool should_try_invalidate = false;
  uint64_t nodeid = 0;
  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    if (!ref.inode_path.empty()) {
      if (FLAGS_enable_locking_debug_logs)
        LOG_DEBUG("unlocking path ` with `lock", ref.inode_path,
                  ref.acquire_write_path ? "w" : "r");
      if (ref.acquire_write_path) {
        RELEASE_ASSERT(ref.inode->nodeid !=
                       kMountPointNodeId);  // root inode should not enter here
        unlock_path(ref.inode->parent, ref.inode, nullptr);
      } else {
        unlock_path(ref.inode, nullptr, nullptr);
      }
    } else {
      ref.inode->ref_ctr--;
    }
    should_try_invalidate = ref.inode->ref_ctr == 0;
    nodeid = ref.inode->nodeid;
    if (!ref.inode_path.empty() && path_lockq_) wake_up_queued(path_lockq_);
  }
  if (should_try_invalidate) try_invalidate_inode(nodeid, 0, true);
}

// Lock file/dir 'name' with wlock if it exits.
// Lock parent and all ancestors with read lock.
// Increase inode reference for parent and inode if it exists.
const ParentRef OssFs::get_inode_ref(uint64_t parent, std::string_view name) {
  ParentRef ref;
  std::unique_lock<std::mutex> l(inodes_map_lck_);
  auto it = global_inodes_map_.find(parent);
  if (it == global_inodes_map_.end()) return ref;
  RELEASE_ASSERT(it->second->is_dir());
  DirInode *parent_inode = static_cast<DirInode *>(it->second);
  int err = 0;

  // Make sure the inode exists after we drop the mutex and wait, so we don't
  // have to find the inode from map again after getting the lock.
  parent_inode->ref_ctr++;
  bool should_try_invalidate = false;
  err = try_get_path(parent_inode, name, &ref.parent_path, &ref.inode);
  if (err == -EAGAIN) {
    LockQueueElement qe;
    qe.parent1 = parent_inode;
    qe.name1 = name;
    qe.path1 = &ref.parent_path;
    qe.wnode1 = &ref.inode;
    err = wait_path(&path_lockq_, &qe, l);
  }
  parent_inode->ref_ctr--;
  if (err) {
    should_try_invalidate = parent_inode->ref_ctr == 0;
  }
  l.unlock();

  if (!err) {
    ref.parent = parent_inode;
    if (FLAGS_enable_locking_debug_logs)
      LOG_DEBUG("locking path ` and name ` with `lock", ref.parent_path, name,
                ref.inode ? "w" : "r");
  } else {
    LOG_ERROR("get inode ` and name ` failed with ` trying to invalidate `",
              parent, name, err, should_try_invalidate ? "true" : "false");
  }

  if (should_try_invalidate) try_invalidate_inode(parent, 0, true);
  return ref;
}

void OssFs::return_inode_ref(const ParentRef &ref) {
  if (!ref.parent) return;
  bool should_try_invalidate = false;
  uint64_t nodeid = 0;
  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    if (FLAGS_enable_locking_debug_logs)
      LOG_DEBUG("unlocking path ` and name with `lock", ref.parent_path,
                ref.inode ? "w" : "r");
    unlock_path(ref.parent, ref.inode, nullptr);
    if (ref.inode && ref.inode->ref_ctr == 0) {
      should_try_invalidate = true;
      nodeid = ref.inode->nodeid;
    } else if (ref.parent->ref_ctr == 0) {
      should_try_invalidate = true;
      nodeid = ref.parent->nodeid;
    }
    if (path_lockq_) wake_up_queued(path_lockq_);
  }
  if (should_try_invalidate) try_invalidate_inode(nodeid, 0, true);
}

const ParentRef2 OssFs::get_inode_ref(uint64_t parent1, std::string_view name1,
                                      uint64_t parent2,
                                      std::string_view name2) {
  ParentRef2 ref;
  std::unique_lock<std::mutex> l(inodes_map_lck_);
  auto it1 = global_inodes_map_.find(parent1);
  auto it2 = global_inodes_map_.find(parent2);
  if (it1 == global_inodes_map_.end() || it2 == global_inodes_map_.end()) {
    return ref;
  }

  RELEASE_ASSERT(it1->second->is_dir());
  RELEASE_ASSERT(it2->second->is_dir());
  DirInode *src_parent_inode = static_cast<DirInode *>(it1->second);
  DirInode *dst_parent_inode = static_cast<DirInode *>(it2->second);
  int err = 0;

  // Make sure the inode exists after we drop the mutex and wait, so we don't
  // have to find the inode from map again after getting the lock.
  src_parent_inode->ref_ctr++;
  dst_parent_inode->ref_ctr++;
  bool should_try_invalidate1 = false;
  bool should_try_invalidate2 = false;
  err = try_get_path2(src_parent_inode, name1, dst_parent_inode, name2,
                      &ref.ref1.parent_path, &ref.ref2.parent_path,
                      &ref.ref1.inode, &ref.ref2.inode);
  if (err == -EAGAIN) {
    LockQueueElement qe;
    qe.parent1 = src_parent_inode;
    qe.name1 = name1;
    qe.path1 = &ref.ref1.parent_path;
    qe.wnode1 = &ref.ref1.inode;
    qe.parent2 = dst_parent_inode;
    qe.name2 = name2;
    qe.path2 = &ref.ref2.parent_path;
    qe.wnode2 = &ref.ref2.inode;
    err = wait_path(&path_lockq_, &qe, l);
  }
  src_parent_inode->ref_ctr--;
  dst_parent_inode->ref_ctr--;
  if (err) {
    should_try_invalidate1 = src_parent_inode->ref_ctr == 0;
    should_try_invalidate2 = dst_parent_inode->ref_ctr == 0;
  }
  l.unlock();

  if (!err) {
    ref.ref1.parent = src_parent_inode;
    ref.ref2.parent = dst_parent_inode;
    if (FLAGS_enable_locking_debug_logs) {
      LOG_DEBUG("locking path1 ` and name1 ` with `lock", ref.ref1.parent_path,
                name1, ref.ref1.inode ? "w" : "r");
      LOG_DEBUG("locking path2 ` and name2 ` with `lock", ref.ref2.parent_path,
                name2, ref.ref2.inode ? "w" : "r");
    }
  } else {
    LOG_ERROR("get inode `:`  `:` failed with ` trying to invalidate ` `",
              parent1, name1, parent2, name2, err,
              should_try_invalidate1 ? "true" : "false",
              should_try_invalidate2 ? "true" : "false");
  }
  if (should_try_invalidate1) try_invalidate_inode(parent1, 0, true);
  if (should_try_invalidate2) try_invalidate_inode(parent2, 0, true);
  return ref;
}

void OssFs::return_inode_ref(const ParentRef2 &ref) {
  if (!ref.ref1.parent && !ref.ref2.parent) return;

  bool should_try_invalidate1 = false, should_try_invalidate2 = false;
  uint64_t nodeid1 = 0, nodeid2 = 0;
  {
    std::lock_guard<std::mutex> l(inodes_map_lck_);
    if (ref.ref1.parent) {
      if (FLAGS_enable_locking_debug_logs)
        LOG_DEBUG("unlocking path ` and name with `lock", ref.ref1.parent_path,
                  ref.ref1.inode ? "w" : "r");
      unlock_path(ref.ref1.parent, ref.ref1.inode, nullptr);
      if (ref.ref1.inode && ref.ref1.inode->ref_ctr == 0) {
        should_try_invalidate1 = true;
        nodeid1 = ref.ref1.inode->nodeid;
      } else if (ref.ref1.parent->ref_ctr == 0) {
        should_try_invalidate1 = true;
        nodeid1 = ref.ref1.parent->nodeid;
      }
    }
    if (ref.ref2.parent) {
      if (FLAGS_enable_locking_debug_logs)
        LOG_DEBUG("unlocking path ` and name with `lock", ref.ref2.parent_path,
                  ref.ref2.inode ? "w" : "r");
      unlock_path(ref.ref2.parent, ref.ref2.inode, nullptr);
      if (ref.ref2.inode && ref.ref2.inode->ref_ctr == 0) {
        should_try_invalidate2 = true;
        nodeid2 = ref.ref2.inode->nodeid;
      } else if (ref.ref2.parent->ref_ctr == 0) {
        should_try_invalidate2 = true;
        nodeid2 = ref.ref2.parent->nodeid;
      }
    }
    if (path_lockq_) wake_up_queued(path_lockq_);
  }
  if (should_try_invalidate1) try_invalidate_inode(nodeid1, 0, true);
  if (should_try_invalidate2) try_invalidate_inode(nodeid2, 0, true);
}

}  // namespace OssFileSystem
