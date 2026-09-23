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

#include <sys/file.h>

#include <chrono>
#include <thread>

#include "common/fault_injector.h"
#include "fs/file_hdfs.h"
#include "fs/test/test_suite.h"

class Ossfs2HdfsFlockTest : public OssHdfsTestSuite {
 protected:
  // Helper: create a file, close write stream, reopen for locking.
  // HDFS: any lock on a handle with an open write stream causes conflict.
  void open_test_file(const char *name, int flags, uint64_t &nodeid,
                      void *&handle) {
    uint64_t parent = get_test_dir_parent();
    struct stat st;
    int r = create_and_flush(parent, name, CREATE_BASE_FLAGS, 0777, 0, 0, 0,
                             &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    if (handle) {
      r = fs_->flush(nodeid, handle);
      ASSERT_EQ(r, 0);
      // Always release and reopen to close the write stream.
      r = fs_->release(nodeid, get_file_from_handle(handle));
      ASSERT_EQ(r, 0);
    }
    bool keep_cache = false;
    r = fs_->open(nodeid, flags, &handle, &keep_cache);
    ASSERT_EQ(r, 0);
  }

  // LOCK_EX: exclusive lock on a single handle.
  // Verify LOCK_EX and LOCK_UN calls succeed, followed by full close path.
  void verify_flock_exclusive() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle1 = nullptr;
    int r = create_and_flush(parent, "lock_file", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    // HDFS: release write handle and reopen to avoid lock conflict.
    if (handle1) {
      r = fs_->flush(nodeid, handle1);
      ASSERT_EQ(r, 0);
    }
    r = fs_->release(nodeid, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle1, &keep_cache);
    ASSERT_EQ(r, 0);

    // Acquire exclusive lock.
    r = fs_->flock(nodeid, handle1, LOCK_EX | LOCK_NB, 1);
    ASSERT_EQ(r, 0);

    // Unlock.
    r = fs_->flock(nodeid, handle1, LOCK_UN, 1);
    ASSERT_EQ(r, 0);
    r = fs_->flush(nodeid, handle1);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
  }

  // LOCK_SH: shared lock, multiple fds can hold it.
  void verify_flock_shared() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle1 = nullptr;
    int r = create_and_flush(parent, "shared_lock", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle1) {
      r = fs_->flush(nodeid, handle1);
      ASSERT_EQ(r, 0);
    }
    r = fs_->release(nodeid, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle1, &keep_cache);
    ASSERT_EQ(r, 0);

    void *handle2 = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle2, &keep_cache);
    ASSERT_EQ(r, 0);

    // Both fds acquire shared lock.
    r = fs_->flock(nodeid, handle1, LOCK_SH | LOCK_NB, 1);
    ASSERT_EQ(r, 0);
    r = fs_->flock(nodeid, handle2, LOCK_SH | LOCK_NB, 2);
    ASSERT_EQ(r, 0);

    // Unlock both.
    r = fs_->flock(nodeid, handle1, LOCK_UN, 1);
    ASSERT_EQ(r, 0);
    r = fs_->flock(nodeid, handle2, LOCK_UN, 2);
    ASSERT_EQ(r, 0);
    r = fs_->flush(nodeid, handle1);
    ASSERT_EQ(r, 0);
    r = fs_->flush(nodeid, handle2);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle2));
    ASSERT_EQ(r, 0);
  }

  // LOCK_UN: verify unlock call succeeds, followed by full close path.
  void verify_flock_unlock() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "unlock_file", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) {
      r = fs_->flush(nodeid, handle);
      ASSERT_EQ(r, 0);
    }
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // Acquire exclusive lock.
    r = fs_->flock(nodeid, handle, LOCK_EX | LOCK_NB, 1);
    ASSERT_EQ(r, 0);

    // Unlock should succeed.
    r = fs_->flock(nodeid, handle, LOCK_UN, 1);
    ASSERT_EQ(r, 0);
    r = fs_->flush(nodeid, handle);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // LOCK_NB: non-blocking mode returns error immediately if lock unavailable.
  void verify_flock_nonblocking() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle1 = nullptr;
    int r = create_and_flush(parent, "nb_lock", CREATE_BASE_FLAGS, 0777, 0, 0,
                             0, &nodeid, &st, &handle1);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle1) {
      r = fs_->flush(nodeid, handle1);
      ASSERT_EQ(r, 0);
    }
    r = fs_->release(nodeid, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle1, &keep_cache);
    ASSERT_EQ(r, 0);

    void *handle2 = nullptr;
    r = fs_->open(nodeid, O_RDWR, &handle2, &keep_cache);
    ASSERT_EQ(r, 0);

    // First fd acquires exclusive lock.
    r = fs_->flock(nodeid, handle1, LOCK_EX | LOCK_NB, 1);
    ASSERT_EQ(r, 0);

    // Second fd with LOCK_NB should fail immediately.
    r = fs_->flock(nodeid, handle2, LOCK_EX | LOCK_NB, 2);
    ASSERT_NE(r, 0);

    r = fs_->flock(nodeid, handle1, LOCK_UN, 1);
    ASSERT_EQ(r, 0);
    r = fs_->flush(nodeid, handle1);
    ASSERT_EQ(r, 0);

    r = fs_->release(nodeid, get_file_from_handle(handle1));
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle2));
    ASSERT_EQ(r, 0);
  }

  // Exclusive then shared lock on the same handle within one session.
  void verify_flock_release_on_close() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "close_lock", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    if (handle) {
      r = fs_->flush(nodeid, handle);
      ASSERT_EQ(r, 0);
    }
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &keep_cache);
    ASSERT_EQ(r, 0);

    // Acquire exclusive lock.
    r = fs_->flock(nodeid, handle, LOCK_EX | LOCK_NB, 1);
    ASSERT_EQ(r, 0);
    r = fs_->flock(nodeid, handle, LOCK_UN, 1);
    ASSERT_EQ(r, 0);

    // Acquire shared lock on the same handle (HDFS server releases
    // asynchronously, so retry with backoff up to 60s).
    bool acquired = false;
    for (int attempt = 0; attempt < 30; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2000));
      r = fs_->flock(nodeid, handle, LOCK_SH | LOCK_NB, 1);
      if (r == 0) {
        acquired = true;
        break;
      }
    }
    ASSERT_TRUE(acquired) << "Failed to re-acquire shared lock after unlock";
    r = fs_->flock(nodeid, handle, LOCK_UN, 1);
    ASSERT_EQ(r, 0);

    r = fs_->flush(nodeid, handle);
    ASSERT_EQ(r, 0);
    r = fs_->release(nodeid, get_file_from_handle(handle));
    ASSERT_EQ(r, 0);
  }

  // flock must survive ftruncate shrink (internal close -> truncate ->
  // reopen): the fd stays open, so BSD semantics require the lock to persist.
  void verify_flock_survives_ftruncate_shrink() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_lock", CREATE_BASE_FLAGS, 0777, 0,
                             0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    auto *file = get_file_from_handle(handle);

    // Write 8KB so the shrink path (close -> truncate -> reopen) is taken.
    const size_t initial_size = 8192;
    char buf[initial_size];
    memset(buf, 'L', initial_size);
    ASSERT_EQ(file->pwrite(buf, initial_size, 0), (ssize_t)initial_size);
    r = fs_->flush(nodeid, handle);
    ASSERT_EQ(r, 0);

    // HDFS: release and reopen to close the write stream before locking.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto *hfile = static_cast<HdfsFileHandle *>(get_file_from_handle(handle));

    // Acquire exclusive lock.
    r = fs_->flock(nodeid, handle, LOCK_EX | LOCK_NB, 1);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(hfile->holds_flock());

    // Shrink to 4KB via setattr (ftruncate).
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 4096;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_EQ(r, 0);

    // The fd is still open, the lock must not have been dropped by the
    // internal stream reopen.
    EXPECT_TRUE(hfile->holds_flock())
        << "ftruncate shrink silently released the fd's flock";

    // Explicit unlock and close path must still work afterwards.
    r = fs_->flock(nodeid, handle, LOCK_UN, 1);
    ASSERT_EQ(r, 0);
    EXPECT_FALSE(hfile->holds_flock());

    r = fs_->release(nodeid, hfile);
    ASSERT_EQ(r, 0);
  }

  // flock must also survive a failed ftruncate shrink: the lock is bound to
  // the fd, not the backend streams.
  void verify_flock_survives_ftruncate_failure() {
    uint64_t parent = get_test_dir_parent();
    DEFER(fs_->forget(parent, 1));
    struct stat st;

    uint64_t nodeid = 0;
    void *handle = nullptr;
    int r = create_and_flush(parent, "trunc_lock_fail", CREATE_BASE_FLAGS, 0777,
                             0, 0, 0, &nodeid, &st, &handle);
    ASSERT_EQ(r, 0);
    DEFER(fs_->forget(nodeid, 1));
    auto *file = get_file_from_handle(handle);

    // Write 8KB so the shrink path (close -> truncate -> reopen) is taken.
    const size_t initial_size = 8192;
    char buf[initial_size];
    memset(buf, 'F', initial_size);
    ASSERT_EQ(file->pwrite(buf, initial_size, 0), (ssize_t)initial_size);
    r = fs_->flush(nodeid, handle);
    ASSERT_EQ(r, 0);

    // HDFS: release and reopen to close the write stream before locking.
    r = fs_->release(nodeid, file);
    ASSERT_EQ(r, 0);
    bool keep_cache = false;
    r = fs_->open(nodeid, O_RDWR, &handle, &keep_cache);
    ASSERT_EQ(r, 0);
    auto *hfile = static_cast<HdfsFileHandle *>(get_file_from_handle(handle));

    // Acquire exclusive lock.
    r = fs_->flock(nodeid, handle, LOCK_EX | LOCK_NB, 1);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(hfile->holds_flock());

    // Fail the shrink after the streams are closed.
    g_fault_injector->set_injection(FI_OssError_Call_Failed);
    DEFER(g_fault_injector->clear_injection(FI_OssError_Call_Failed));
    struct stat new_stat;
    memset(&new_stat, 0, sizeof(new_stat));
    new_stat.st_size = 4096;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = reinterpret_cast<uint64_t>(handle);
    r = fs_->setattr(nodeid, &new_stat, FUSE_SET_ATTR_SIZE, &fi, 0, 0);
    ASSERT_NE(r, 0);
    g_fault_injector->clear_injection(FI_OssError_Call_Failed);

    // The fd is still open, so the lock must survive the failed shrink.
    EXPECT_TRUE(hfile->holds_flock())
        << "failed ftruncate shrink released the fd's flock";

    // Explicit unlock and close path must still work afterwards.
    r = fs_->flock(nodeid, handle, LOCK_UN, 1);
    ASSERT_EQ(r, 0);
    EXPECT_FALSE(hfile->holds_flock());

    r = fs_->release(nodeid, hfile);
    ASSERT_EQ(r, 0);
  }

  // F_SETLK + F_WRLCK: write lock via fcntl interface.
};

TEST_F(Ossfs2HdfsFlockTest, verify_flock_exclusive) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_exclusive();
}

TEST_F(Ossfs2HdfsFlockTest, verify_flock_shared) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_shared();
}

TEST_F(Ossfs2HdfsFlockTest, verify_flock_unlock) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_unlock();
}

TEST_F(Ossfs2HdfsFlockTest, verify_flock_nonblocking) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_nonblocking();
}

TEST_F(Ossfs2HdfsFlockTest, verify_flock_release_on_close) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_release_on_close();
}

TEST_F(Ossfs2HdfsFlockTest, verify_flock_survives_ftruncate_shrink) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_survives_ftruncate_shrink();
}

TEST_F(Ossfs2HdfsFlockTest, verify_flock_survives_ftruncate_failure) {
  INIT_PHOTON();
  OssFsOptions opts;
  init(opts);
  verify_flock_survives_ftruncate_failure();
}
