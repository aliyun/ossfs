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

#include "fuse_adapter_ll.h"

#include <fuse3/fuse_opt.h>
#include <limits.h>
#include <photon/fs/filesystem.h>
#include <photon/photon.h>
#include <photon/thread/thread-pool.h>
#include <semaphore.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"
#include "metric/metrics.h"

using OssFileSystem::Metric::MetricsType;

static IFileSystemFuseLL *fuse_ll_fs = nullptr;

static FuseLLOptions fuse_options;

void set_fuse_ll_fs(IFileSystemFuseLL *fs) {
  fuse_ll_fs = fs;
}

void set_fuse_ll_options(const FuseLLOptions &opts) {
  fuse_options = opts;
}

static inline IFileHandleFuseLL *get_file_handle(struct fuse_file_info *fi) {
  return reinterpret_cast<IFileHandleFuseLL *>(fi->fh);
}

static inline pid_t get_pid(fuse_req_t req) {
  return fuse_req_ctx(req)->pid;
}

struct readdir_ctx {
  fuse_req_t req;
  char *buf = nullptr;
  size_t size = 0;
  size_t pos = 0;
  bool plus = false;
};

static int dirent_filler(void *ctx, uint64_t nodeid, const char *name,
                         const struct stat *stbuf, off_t off) {
  struct readdir_ctx *c = (struct readdir_ctx *)ctx;

  size_t left = c->size - c->pos;
  size_t needed = 0;

  if (true == c->plus) {
    struct fuse_entry_param e;

    memset(&e, 0, sizeof(e));
    e.ino = nodeid;
    e.attr = *stbuf;
    e.attr_timeout = fuse_options.attr_timeout;
    e.entry_timeout = fuse_options.entry_timeout;
    needed =
        fuse_add_direntry_plus(c->req, c->buf + c->pos, left, name, &e, off);
  } else {
    needed = fuse_add_direntry(c->req, c->buf + c->pos, left, name, stbuf, off);
  }

  if (needed > left) {
    return -ENOSPC;
  }

  c->pos += needed;
  return 0;
}

struct interrupted_ctx {
  fuse_req_t req;
};

int is_interrupted(void *ctx) {
  struct interrupted_ctx *c = (struct interrupted_ctx *)ctx;
  int interrupted = fuse_req_interrupted(c->req);
  return interrupted != 0 ? true : false;
}

static void ossfs2_init(void *userdata, struct fuse_conn_info *conn) {
  conn->max_background = 128;
  conn->congestion_threshold = 96;
  conn->max_readahead = 1024UL * 1024 * 64;

  if (conn->capable & FUSE_CAP_ASYNC_READ) {
    conn->want |= FUSE_CAP_ASYNC_READ;
  }

  if (conn->capable & FUSE_CAP_SPLICE_MOVE) {
    // TODO: reopen after we have more test for kernel versions that
    // supports SPLICE_F_MOVE later.
    // https://man7.org/linux/man-pages/man2/splice.2.html
    //
    // FUSE_CAP_SPLICE_MOVE will set SPLICE_F_MOVE when using splice(), but
    // linux kernel does not use this flag after 2.6.21. So this flag was
    // ignored now.

    // conn->want |= FUSE_CAP_SPLICE_MOVE;
  }

  if (conn->capable & FUSE_CAP_PARALLEL_DIROPS) {
    conn->want |= FUSE_CAP_PARALLEL_DIROPS;
  }

  conn->want &= (~FUSE_CAP_READDIRPLUS_AUTO);
  if (fuse_options.readdirplus && (conn->capable & FUSE_CAP_READDIRPLUS)) {
    conn->want |= FUSE_CAP_READDIRPLUS;
  } else {
    conn->want &= (~FUSE_CAP_READDIRPLUS);
  }
}

static void ossfs2_destroy(void *userdata) {}

static void ossfs2_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
  LOG_DEBUG("LOOKUP. pid: `, parent: `, name: `", get_pid(req), parent, name);

  DECLARE_METRIC_LATENCY(lookup, MetricsType::kFsMetrics);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  int r = fuse_ll_fs->lookup(parent, name, &fe.ino, &fe.attr);
  if (r == 0) {
    fe.attr_timeout = fuse_options.attr_timeout;
    fe.entry_timeout = fuse_options.entry_timeout;
    fuse_reply_entry(req, &fe);
  } else {
    // TODO: optimize for cases we want to return ENOENT immediately.
    if (r == -ENOENT && fuse_options.negative_timeout > 0) {
      fe.ino = 0;
      fe.entry_timeout = fuse_options.negative_timeout;
      fuse_reply_entry(req, &fe);
    } else {
      fuse_reply_err(req, -r);
    }
  }
}

static void ossfs2_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
  LOG_DEBUG("FORGET. pid: `, parent: `, nlookup: `", get_pid(req), ino,
            nlookup);

  DECLARE_METRIC_LATENCY(forget, MetricsType::kFsMetrics);
  fuse_ll_fs->forget(ino, nlookup);
  fuse_reply_none(req);
}

static void ossfs2_getattr(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  LOG_DEBUG("GETATTR. pid: `, ino: `", get_pid(req), ino);

  DECLARE_METRIC_LATENCY(getattr, MetricsType::kFsMetrics);
  struct stat stbuf;
  memset(&stbuf, 0, sizeof(stbuf));
  int r = fuse_ll_fs->getattr(ino, &stbuf);
  if (r == 0) {
    fuse_reply_attr(req, &stbuf, fuse_options.attr_timeout);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                           int to_set, struct fuse_file_info *fi) {
  LOG_DEBUG("SETATTR. pid: `, ino: `, to_set: `", get_pid(req), ino, to_set);

  DECLARE_METRIC_LATENCY(setattr, MetricsType::kFsMetrics);
  int r = fuse_ll_fs->setattr(ino, attr, to_set);
  if (r == 0) {
    fuse_reply_attr(req, attr, fuse_options.attr_timeout);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_readlink(fuse_req_t req, fuse_ino_t ino) {
  LOG_DEBUG("READLINK. pid: `, ino: `", get_pid(req), ino);

  DECLARE_METRIC_LATENCY(readlink, MetricsType::kFsMetrics);
  char buf[PATH_MAX + 1] = {0};
  ssize_t r = fuse_ll_fs->readlink(ino, buf, sizeof(buf) - 1);
  if (r >= 0) {
    buf[r] = '\0';
    fuse_reply_readlink(req, buf);
    return;
  }

  fuse_reply_err(req, -r);
}

static void ossfs2_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                         mode_t mode) {
  LOG_DEBUG("MKDIR. pid: `, parent: `, name: `, mode: `", get_pid(req), parent,
            name, mode);

  DECLARE_METRIC_LATENCY(mkdir, MetricsType::kFsMetrics);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  // uid, gid, umask, mode are not supported actually.
  int r = fuse_ll_fs->mkdir(parent, name, mode, 0, 0, 0, &fe.ino, &fe.attr);

  if (r == 0) {
    fe.attr_timeout = fuse_options.attr_timeout;
    fe.entry_timeout = fuse_options.entry_timeout;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
  LOG_DEBUG("UNLINK. pid: `, parent: `, name: `", get_pid(req), parent, name);

  DECLARE_METRIC_LATENCY(unlink, MetricsType::kFsMetrics);
  int r = fuse_ll_fs->unlink(parent, name);
  fuse_reply_err(req, -r);
}

static void ossfs2_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
  LOG_DEBUG("RMDIR. pid: `, parent: `, name: `", get_pid(req), parent, name);

  DECLARE_METRIC_LATENCY(rmdir, MetricsType::kFsMetrics);
  int r = fuse_ll_fs->rmdir(parent, name);
  fuse_reply_err(req, -r);
}

static void ossfs2_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                           const char *name) {
  LOG_DEBUG("SYMLINK. pid: `, link: `, parent: `, name: `", get_pid(req), link,
            parent, name);

  DECLARE_METRIC_LATENCY(symlink, MetricsType::kFsMetrics);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));
  int r = fuse_ll_fs->symlink(parent, name, link, 0, 0, &fe.ino, &fe.attr);
  if (r == 0) {
    fe.attr_timeout = fuse_options.attr_timeout;
    fe.entry_timeout = fuse_options.entry_timeout;
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                          fuse_ino_t newparent, const char *newname,
                          unsigned int flags) {
  LOG_DEBUG(
      "RENAME. pid: `, parent: `, name: `, newparent: `, newname: `, flags: `",
      get_pid(req), parent, name, newparent, newname, flags);

  DECLARE_METRIC_LATENCY(rename, MetricsType::kFsMetrics);
  int r = fuse_ll_fs->rename(parent, name, newparent, newname, flags);
  fuse_reply_err(req, -r);
}

static void ossfs2_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                          mode_t mode, struct fuse_file_info *fi) {
  LOG_DEBUG("CREATE. pid: `, parent: `, name: `, mode: ` flags: `",
            get_pid(req), parent, name, mode, fi->flags);

  DECLARE_METRIC_LATENCY(create, MetricsType::kFsMetrics);
  struct fuse_entry_param fe;
  memset(&fe, 0, sizeof(fe));

  void *fh = nullptr;

  // Ignore uid, gid and umask.
  int r = fuse_ll_fs->creat(parent, name, fi->flags, mode, 0, 0, 0, &fe.ino,
                            &fe.attr, &fh);
  if (r == 0) {
    fi->fh = reinterpret_cast<uint64_t>(fh);
    if (fuse_reply_create(req, &fe, fi) == -ENOENT) {
      // -ENOENT means that the request was interrupted.
      LOG_ERROR("Fail to reply create request with parent: `, name: `", parent,
                name);
    }
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_open(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info *fi) {
  LOG_DEBUG("OPEN. pid: `, ino: ` flags: `", get_pid(req), ino, fi->flags);

  DECLARE_METRIC_LATENCY(open, MetricsType::kFsMetrics);
  bool keep_page_cache = false;
  void *fh = nullptr;
  int r = fuse_ll_fs->open(ino, fi->flags, &fh, &keep_page_cache);
  fi->keep_cache = (keep_page_cache ? 1 : 0);

  if ((fi->flags & O_ACCMODE) == O_RDONLY) {
    fi->noflush = 1;
  }

  if (r == 0) {
    fi->fh = reinterpret_cast<uint64_t>(fh);
    if (fuse_reply_open(req, fi) == -ENOENT) {
      // -ENOENT means that the request was interrupted.
      LOG_ERROR("Fail to reply open request with ino: `", ino);
    }
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                        struct fuse_file_info *fi) {
  auto time_before_req = std::chrono::steady_clock::now();
  struct fuse_bufvec bufv = FUSE_BUFVEC_INIT(size);
  void *mem;
  ssize_t r = 0;

  auto fh = get_file_handle(fi);
  r = fh->pin(off, size, &mem);
  if (r > 0) {
    bufv.buf[0].mem = mem;
    bufv.buf[0].size = r;
    fuse_reply_data(req, &bufv, FUSE_BUF_SPLICE_MOVE);
    fh->unpin(off);
    REPORT_ALL_METRIC_SUCCESSFUL(read, MetricsType::kIoMetrics, time_before_req,
                                 r);
    return;
  }

  int ret = posix_memalign(&mem, 4096, size);
  if (ret != 0) {
    fuse_reply_err(req, ENOMEM);
    REPORT_ALL_METRIC_FAILED(read, MetricsType::kIoMetrics, time_before_req);
    return;
  }

  bufv.buf[0].mem = mem;
  r = fh->pread(mem, size, off);
  if (r >= 0) {
    bufv.buf[0].size = r;
    fuse_reply_data(req, &bufv, FUSE_BUF_SPLICE_MOVE);
    REPORT_ALL_METRIC_SUCCESSFUL(read, MetricsType::kIoMetrics, time_before_req,
                                 r);
  } else {
    fuse_reply_err(req, -r);
    REPORT_ALL_METRIC_FAILED(read, MetricsType::kIoMetrics, time_before_req);
  }

  // cleanup
  free(mem);
}

static void ossfs2_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                         size_t size, off_t off, struct fuse_file_info *fi) {
  auto time_before_req = std::chrono::steady_clock::now();
  ssize_t r = get_file_handle(fi)->pwrite(buf, size, off);
  if (r >= 0) {
    fuse_reply_write(req, r);
    REPORT_ALL_METRIC_SUCCESSFUL(write, MetricsType::kIoMetrics,
                                 time_before_req, r);
  } else {
    fuse_reply_err(req, -r);
    REPORT_ALL_METRIC_FAILED(write, MetricsType::kIoMetrics, time_before_req);
  }
}

static void ossfs2_write_buf(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_bufvec *bufv, off_t off,
                             struct fuse_file_info *fi) {
  auto time_before_req = std::chrono::steady_clock::now();
  ssize_t r = get_file_handle(fi)->write_buf(bufv, off);
  if (r >= 0) {
    fuse_reply_write(req, r);
    REPORT_ALL_METRIC_SUCCESSFUL(write, MetricsType::kIoMetrics,
                                 time_before_req, r);
  } else {
    fuse_reply_err(req, -r);
    REPORT_ALL_METRIC_FAILED(write, MetricsType::kIoMetrics, time_before_req);
  }
}

static void ossfs2_flush(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi) {
  LOG_DEBUG("FLUSH. pid: `, ino: `", get_pid(req), ino);

  DECLARE_METRIC_LATENCY(flush, MetricsType::kFsMetrics);
  if (fuse_options.ignore_flush) {
    fuse_reply_err(req, 0);
    return;
  }

  int r = get_file_handle(fi)->fsync();
  if (r == 0) {
    fuse_reply_err(req, 0);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_release(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  LOG_DEBUG("RELEASE. pid: `, ino: `", get_pid(req), ino);

  DECLARE_METRIC_LATENCY(release, MetricsType::kFsMetrics);

  int r = fuse_ll_fs->release(ino, get_file_handle(fi));
  if (r == 0) {
    fuse_reply_err(req, 0);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                         struct fuse_file_info *fi) {
  DECLARE_METRIC_LATENCY(fsync, MetricsType::kFsMetrics);
  if (fuse_options.ignore_fsync) {
    fuse_reply_err(req, 0);
    return;
  }

  auto fh = get_file_handle(fi);
  int r = datasync ? fh->fdatasync() : fh->fsync();
  if (r == 0) {
    fuse_reply_err(req, 0);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_opendir(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  LOG_DEBUG("OPENDIR. pid: `, ino: `", get_pid(req), ino);

  DECLARE_METRIC_LATENCY(opendir, MetricsType::kFsMetrics);
  void *dh = nullptr;
  int r = fuse_ll_fs->opendir(ino, &dh);
  if (r != 0) {
    fuse_reply_err(req, -r);
    return;
  }

  fi->fh = reinterpret_cast<uint64_t>(dh);
  if (fuse_reply_open(req, fi) == -ENOENT) {
    // -ENOENT means that the request was interrupted.
    LOG_ERROR("Fail to reply open request with ino: `", ino);
  }
}

static void ossfs2_do_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                              off_t off, bool plus, struct fuse_file_info *fi) {
  LOG_DEBUG("READDIR. pid: `, ino: `, size: `, off: `, plus: `", get_pid(req),
            ino, size, off, plus);
  DECLARE_METRIC_LATENCY(readdir, MetricsType::kFsMetrics);
  void *d = reinterpret_cast<void *>(fi->fh);

  struct readdir_ctx rc;
  rc.req = req;
  rc.buf = new char[size];
  rc.size = size;
  rc.pos = 0;
  rc.plus = plus;

  struct interrupted_ctx ic;
  ic.req = req;

  int r = fuse_ll_fs->readdir(ino, off, d, dirent_filler, &rc, is_interrupted,
                              plus, &ic);
  if (r == 0 || r == -ENOSPC) {
    fuse_reply_buf(req, rc.buf, rc.pos);
  } else {
    fuse_reply_err(req, -r);
  }
  delete[] rc.buf;
}

static void ossfs2_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                           off_t off, struct fuse_file_info *fi) {
  ossfs2_do_readdir(req, ino, size, off, false, fi);
}

static void ossfs2_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size,
                               off_t off, struct fuse_file_info *fi) {
  ossfs2_do_readdir(req, ino, size, off, true, fi);
}

static void ossfs2_releasedir(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) {
  LOG_DEBUG("RELEASEDIR. pid: `, ino: `", get_pid(req), ino);
  void *d = reinterpret_cast<void *>(fi->fh);
  int r = fuse_ll_fs->releasedir(ino, d);
  fuse_reply_err(req, -r);
}

static void ossfs2_statfs(fuse_req_t req, fuse_ino_t ino) {
  DECLARE_METRIC_LATENCY(statfs, MetricsType::kFsMetrics);
  struct statvfs stbuf;
  memset(&stbuf, 0, sizeof(stbuf));

  int r = 0;

  if (0 == (r = fuse_ll_fs->statfs(&stbuf))) {
    fuse_reply_statfs(req, &stbuf);
  } else {
    fuse_reply_err(req, -r);
  }
}

static void ossfs2_access(fuse_req_t req, fuse_ino_t ino, int mask) {
  DECLARE_METRIC_LATENCY(access, MetricsType::kFsMetrics);
  fuse_reply_err(req, 0);
}

struct fuse_lowlevel_ops fuse_ll_ops = {
    .init = ossfs2_init,
    .destroy = ossfs2_destroy,
    .lookup = ossfs2_lookup,
    .forget = ossfs2_forget,
    .getattr = ossfs2_getattr,
    .setattr = ossfs2_setattr,
    .readlink = ossfs2_readlink,
    .mknod = nullptr,
    .mkdir = ossfs2_mkdir,
    .unlink = ossfs2_unlink,
    .rmdir = ossfs2_rmdir,
    .symlink = ossfs2_symlink,
    .rename = ossfs2_rename,
    .link = nullptr,
    .open = ossfs2_open,
    .read = ossfs2_read,
    .write = ossfs2_write,
    .flush = ossfs2_flush,
    .release = ossfs2_release,
    .fsync = ossfs2_fsync,
    .opendir = ossfs2_opendir,
    .readdir = ossfs2_readdir,
    .releasedir = ossfs2_releasedir,
    .fsyncdir = nullptr,
    .statfs = ossfs2_statfs,
    .setxattr = nullptr,
    .getxattr = nullptr,
    .listxattr = nullptr,
    .removexattr = nullptr,
    .access = ossfs2_access,
    .create = ossfs2_create,
    .getlk = nullptr,
    .setlk = nullptr,
    .bmap = nullptr,
    .ioctl = nullptr,
    .poll = nullptr,
    .write_buf = ossfs2_write_buf,
    .retrieve_reply = nullptr,
    .forget_multi = nullptr,
    .flock = nullptr,
    .fallocate = nullptr,
    .readdirplus = ossfs2_readdirplus,
    .copy_file_range = nullptr,
    .lseek = nullptr,
};

struct fuse_lowlevel_ops *get_fuse_ll_oper() {
  return &fuse_ll_ops;
};

static void do_fuse_loop(fuse_session *se) {
  struct fuse_buf fbuf;
  memset(&fbuf, 0, sizeof(fbuf));
  while (!fuse_session_exited(se)) {
    int res;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    res = fuse_session_receive_buf(se, &fbuf);
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    if (res == -EINTR) continue;
    if (res <= 0) {
      if (res < 0) {
        fuse_session_exit(se);
      }
      break;
    }

    fuse_session_process_buf(se, &fbuf);
  }

  if (fbuf.mem) {
    free(fbuf.mem);
    fbuf.mem = nullptr;
  }
}

struct fuse_loop_context {
  sem_t finish;
  fuse_session *se = nullptr;
};

static void *fuse_do_work(void *user_data) {
  photon::block_all_signal();
  photon::init(OSSFS_EVENT_ENGINE, photon::INIT_IO_NONE);

  fuse_loop_context *ctx = (fuse_loop_context *)user_data;
  do_fuse_loop(ctx->se);

  sem_post(&ctx->finish);

  // Calling photon::fini() after the thread has been pthread_cancel()-ed may
  // cause unexpected crashes, so we don't use DEFER to handle this.
  photon::fini();
  return nullptr;
}

int fuse_session_loop_mt_with_photon(struct fuse_session *se, int threads,
                                     void *pre_init(void *),
                                     void *cleanup(void *), void *user_data) {
  int res = 0;

  std::mutex mtx;
  std::vector<pthread_t> th_pthread_ids;
  fuse_loop_context ctx;

  ctx.se = se;
  sem_init(&ctx.finish, 0, 0);

  for (int i = 0; i < threads; ++i) {
    pthread_t thread_id;

    int res;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    res = pthread_create(&thread_id, &attr, fuse_do_work, &ctx);
    pthread_attr_destroy(&attr);
    if (res != 0) {
      LOG_ERROR("Fail to create thread: `", strerror(res));
      break;
    }

    th_pthread_ids.push_back(thread_id);
  }

  LOG_INFO("fuse_session_loop_mt_with_photon started threads: `", threads);

  if (res == 0) {
    while (!fuse_session_exited(se)) sem_wait(&ctx.finish);
  }

  for (auto &id : th_pthread_ids) pthread_cancel(id);
  for (auto &id : th_pthread_ids) pthread_join(id, nullptr);

  sem_destroy(&ctx.finish);
  return res;
}
