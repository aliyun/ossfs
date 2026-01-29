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

#include "file_writer.h"

#include <photon/common/checksum/crc64ecma.h>
#include <photon/common/iovector.h>
#include <unistd.h>

#include "common/fuse.h"
#include "error_codes.h"
#include "file.h"
#include "fs.h"
#include "metric/metrics.h"

static constexpr int kOssMaxPartNumber = 10000;
static constexpr size_t kOssMaxAppendableObjectSize = 5368709120ULL;  // 5GB

namespace OssFileSystem {

class OssWriter : public IWriter {
 public:
  OssWriter(OssFs *fs, std::string_view path, FileInode *inode, int flags)
      : fs_(fs), inode_(inode), upload_path_(path), open_flags_(flags) {}

  ~OssWriter();

  int open() override;

  ssize_t pwrite(size_t count, off_t offset, const void *buf,
                 struct fuse_bufvec *bufv, std::string *wpath) override;

  int flush() override;

  bool get_is_dirty() override {
    return is_dirty_;
  }

  bool get_is_immutable() override {
    return immutable_;
  }

  // The following functions need inode's rlock.
  ssize_t pread_from_upload_buffer(void *buf, size_t count,
                                   off_t offset) override;
  size_t calc_remote_size() override;

 private:
  struct AppendableStatus {
    bool is_appendable_object = false;
    off_t valid_buffer_offset = 0;
  };

  struct MultipartContext {
    OssWriter *writer;
    char *buf;
    size_t part_number;
  };

  static void *do_multipart_upload(void *args);
  static void *do_multipart_copy(void *args);

  // The following functions need inode's wlock.
  inline int check_writing_permission();
  inline void mark_dirty();
  inline void mark_clean();

  int complete_upload();
  int upload_empty_file();
  int complete_or_abort_multipart();
  int schedule_multipart_upload(size_t part_number);

  int merge_remote_data(off_t offset, size_t count);
  int switch_object_type_normal_to_appendable();
  int merge_remote_data_of_normal_object();

  int do_append_upload(size_t part_number);

  char *get_buffer();
  void free_buffer(char *ptr);

  bool verify_crc64();

  OssFs *fs_ = nullptr;
  FileInode *inode_ = nullptr;

  bool is_dirty_ = false;

  // We will delay updating current offset until the write actually happens,
  // -1 means not yet written. This is used for operation patterns like
  // open, truncate, write, close.
  off_t write_off_ = -1;

  char *buffer_ = nullptr;
  size_t buffer_size_ = 0;
  // Current buffer index, used for calculating write offset or upload part
  // number.
  size_t buffer_index_ = 0;

  // expected_crc64_ stores the crc64 of whole file.
  uint64_t expected_crc64_ = 0;
  bool has_crc64_ = true;

  void *upload_context_ = nullptr;
  std::atomic<uint64_t> running_upload_tasks_ = {0};

  // On write failure, we cannot recover or safely continue I/O.
  // Mark the file as immutable and reject all subsequent write, flush, and
  // fsync operations.
  std::atomic<bool> immutable_ = {false};

  AppendableStatus appendable_status_;
  bool already_head_ = false;

  // If a file is renamed during writing, upload_path_ will be updated to the
  // new path to ensure the file can be uploaded to the correct location.
  std::string upload_path_;

  int open_flags_ = 0;
};

static std::string random_string(int length) {
  static std::string charset =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
  std::string result;
  result.resize(length);

  for (int i = 0; i < length; i++)
    result[i] = charset[rand() % charset.length()];

  return result;
}

static const struct fuse_buf *fuse_bufvec_current(struct fuse_bufvec *bufv) {
  if (bufv->idx < bufv->count)
    return &bufv->buf[bufv->idx];
  else
    return NULL;
}

static int fuse_bufvec_advance(struct fuse_bufvec *bufv, size_t len) {
  const struct fuse_buf *buf = fuse_bufvec_current(bufv);

  if (!buf) return 0;

  bufv->off += len;
  RELEASE_ASSERT(bufv->off <= buf->size);
  if (bufv->off == buf->size) {
    RELEASE_ASSERT(bufv->idx < bufv->count);
    bufv->idx++;
    if (bufv->idx == bufv->count) return 0;
    bufv->off = 0;
  }

  return 1;
}

static ssize_t fuse_buf_to_buf_copy(char *dst, const struct fuse_buf *src,
                                    size_t src_off, size_t len) {
  ssize_t res = 0;
  size_t copied = 0;

  while (copied < len) {
    if (src->flags & FUSE_BUF_FD_SEEK) {
      res = pread(src->fd, dst + copied, len, src->pos + src_off);
    } else {
      res = read(src->fd, dst + copied, len);
    }
    if (res == -1) {
      if (!copied) return -errno;
      break;
    }
    if (res == 0) break;

    copied += res;
    if (!(src->flags & FUSE_BUF_FD_RETRY)) break;

    src_off += res;
    len -= res;
  }

  return copied;
}

static ssize_t fuse_bufvec_to_buf_copy(char *dst, struct fuse_bufvec *srcv,
                                       size_t len) {
  DECLARE_METRIC_LATENCY(fuse_bufv_copy, Metric::kInternalMetrics);
  size_t copied = 0;

  while (copied < len) {
    const struct fuse_buf *src = fuse_bufvec_current(srcv);

    size_t src_len = src->size - srcv->off;
    size_t copy_len = std::min(src_len, len - copied);
    ssize_t res = 0;

    if (src->flags & FUSE_BUF_IS_FD) {
      res = fuse_buf_to_buf_copy(dst + copied, src, srcv->off, copy_len);
      if (res < 0) {
        if (!copied) return res;
        break;
      }
    } else {
      memcpy(dst + copied, (char *)src->mem + srcv->off, copy_len);
      res = copy_len;
    }

    copied += res;
    if (!fuse_bufvec_advance(srcv, res)) break;
  }

  return copied;
}

OssWriter::~OssWriter() {
  while (running_upload_tasks_.load() > 0) {
    AUTO_USLEEP(1000);
  }

  RELEASE_ASSERT(buffer_ == nullptr);
}

int OssWriter::flush() {
  int r = complete_upload();
  if (IS_FAULT_INJECTION_ENABLED(FI_Data_Sync_Failed)) {
    r = -EIO;
  }
  if (r < 0) {
    LOG_ERROR("Failed to fdatasync file: `, nodeid: `, r: `", upload_path_,
              inode_->nodeid, r);
  }
  mark_clean();
  return r;
}

size_t OssWriter::calc_remote_size() {
  return inode_->attr.size -
         (buffer_size_ - appendable_status_.valid_buffer_offset);
}

ssize_t OssWriter::pread_from_upload_buffer(void *buf, size_t count,
                                            off_t offset) {
  RELEASE_ASSERT(fs_->options_.enable_appendable_object);

  if (unlikely(offset >= (int64_t)inode_->attr.size)) {
    return 0;
  }

  RELEASE_ASSERT(buffer_ != nullptr);

  size_t buffer_index = offset / fs_->options_.upload_buffer_size;
  RELEASE_ASSERT(buffer_index == buffer_index_);

  off_t buffer_offset = offset % fs_->options_.upload_buffer_size;
  RELEASE_ASSERT(buffer_offset >= appendable_status_.valid_buffer_offset);

  count = std::min(count, buffer_size_ - (size_t)buffer_offset);
  memcpy(static_cast<char *>(buf), buffer_ + buffer_offset, count);
  return count;
}

int OssWriter::check_writing_permission() {
  // inode dirty, handle dirty : allow writing
  // inode clean, handle clean : allow writing
  // inode dirty, handle clean : don't allow writing
  // inode clean, handle dirty : invalid case
  if (inode_->is_dirty && !get_is_dirty()) {
    LOG_ERROR("file: `, nodeid: ` has already been written!", upload_path_,
              inode_->nodeid);
    return -EBUSY;
  } else if (!inode_->is_dirty && get_is_dirty()) {
    // Should not enter this case.
    LOG_WARN("somewhat file: `, nodeid: ` is clean but file handle is dirty",
             upload_path_, inode_->nodeid);
    RELEASE_ASSERT(false);
  }

  return 0;
}

void OssWriter::mark_dirty() {
  if (!get_is_dirty()) {
    LOG_INFO("file: `, nodeid: ` is marked to dirty", upload_path_,
             inode_->nodeid);

    inode_->is_dirty = true;
    is_dirty_ = true;

    fs_->add_dirty_nodeid(inode_->nodeid);
  }
}

bool OssWriter::verify_crc64() {
  return fs_->options_.enable_crc64 && has_crc64_;
}

int OssWriter::open() {
  if (open_flags_ & O_CREAT) {
    mark_dirty();
  } else if (open_flags_ & O_TRUNC) {
    // Appendable objects should be deleted before writing.
    if (fs_->options_.enable_appendable_object) {
      int r = fs_->truncate_inode_data(inode_, upload_path_, 0);
      if (r < 0) {
        LOG_ERROR("fail to truncate file `, nodeid `, r `", upload_path_,
                  inode_->nodeid, r);
        return r;
      }
    } else {
      if (inode_->attr.size != 0) mark_dirty();
    }

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode_->etag.clear();
    inode_->update_attr(0, now);
  }
  return 0;
}

ssize_t OssWriter::pwrite(size_t count, off_t offset, const void *buf,
                          struct fuse_bufvec *bufv, std::string *wpath) {
  ssize_t r = 0;
  if (immutable_) {
    LOG_ERROR("file: `, nodeid: ` is immutable!", upload_path_, inode_->nodeid);
    return -EIO;
  }

  if (inode_->attr.size == 0 && !get_is_dirty() && inode_->is_dirty) {
    // A previously opened handle may have marked as dirty via O_TRUNC or
    // create, yet written no actual data (empty dirty handle). In this case, we
    // force-flush that handle to revert it to a clean state, thereby permitting
    // this handle to write.
    if (wpath == nullptr) {
      return -E_READ_PATH_NEEDED;
    } else {
      LOG_WARN("force flush dirty handle for empty file: `, nodeid: `",
               upload_path_, inode_->nodeid);
      auto dirty_file = inode_->dirty_fh;
      RELEASE_ASSERT(dirty_file);
      r = dirty_file->fdatasync_lock_held();
      if (r < 0) return r;
    }
    if (IS_FAULT_INJECTION_ENABLED(FI_Force_Flush_Dirty_Handle_Delay)) {
      AUTO_USLEEP(2'000'000);
    }
  }

  r = check_writing_permission();
  if (r < 0) {
    return r;
  }

  if (write_off_ == -1) {
    write_off_ = inode_->attr.size;
  }

  if (offset != write_off_) {
    if (open_flags_ & O_APPEND) {
      // clang-format off
      LOG_WARN("offset ` is not equal to file_end ` with O_APPEND, ignored. file: `, nodeid: `",
               offset, write_off_, upload_path_, inode_->nodeid);
      // clang-format on
      offset = write_off_;
    } else {
      // clang-format off
      LOG_ERROR("write not allow on append only file: `, nodeid: `, size: `, offset: `, file_end: `",
                upload_path_, inode_->nodeid, inode_->attr.size, offset, write_off_);
      // clang-format on
      return -EINVAL;
    }
  }

  if (!get_is_dirty()) {
    if (wpath == nullptr) {
      // This is the first write, grab the path lock to make sure do_write() can
      // work with rename_dir() well.
      return -E_READ_PATH_NEEDED;
    } else {
      if (!wpath->empty() && (*wpath) != upload_path_) {
        LOG_INFO("file: nodeid: ` reset path from ` to `", inode_->nodeid,
                 upload_path_, *wpath);
        upload_path_ = *wpath;
      }
    }
  }

  if (!get_is_dirty()) {
    if (write_off_ != 0 || fs_->options_.enable_appendable_object) {
      r = merge_remote_data(offset, count);
      if (r < 0) {
        return r;
      }
    }
  } else if (write_off_ == 0 && fs_->options_.enable_appendable_object &&
             open_flags_ & O_CREAT) {
    // Use appendable object forcely when enable_appendable_object is true.
    appendable_status_.is_appendable_object = true;
  }

  uint64_t buffer_off = buffer_size_;
  const uint64_t upload_buffer_size = fs_->options_.upload_buffer_size;

  if (!appendable_status_.is_appendable_object) {
    // The count is always > 0 and buffer_index_ starts from 0.
    size_t num_new_buffers = (buffer_off + count - 1) / upload_buffer_size;
    if (kOssMaxPartNumber < buffer_index_ + 1 + num_new_buffers) {
      LOG_ERROR("file: `, nodeid: ` has already reached the max part number: `",
                upload_path_, inode_->nodeid, kOssMaxPartNumber);
      return -EFBIG;
    }
  }

  size_t written = 0;
  while (written < count) {
    if (buffer_ == nullptr) {
      buffer_ = get_buffer();
    }

    size_t write_size =
        std::min(count - written, upload_buffer_size - buffer_off);

    if (buf) {
      memcpy(buffer_ + buffer_off, (char *)buf + written, write_size);
      r = write_size;
    } else {
      r = fuse_bufvec_to_buf_copy(buffer_ + buffer_off, bufv, write_size);
    }

    if (r < 0) {
      break;
    }

    if (verify_crc64()) {
      expected_crc64_ = crc64ecma(buffer_ + buffer_off, r, expected_crc64_);
    }

    if (IS_FAULT_INJECTION_ENABLED(FI_Modify_Write_Buffer)) {
      int index = 0;
      if (appendable_status_.is_appendable_object)
        index = appendable_status_.valid_buffer_offset;
      char old = '\0';
      do {
        old = buffer_[index];
        buffer_[index] = (buffer_[index] + 1) % 256;
      } while (buffer_[index] == old);
    }

    buffer_off += r;
    written += r;
    buffer_size_ += r;

    // Only occur when fuse_bufvec_to_buf_copy reads from FUSE device but
    // returns a size less than write_size.
    if (unlikely(r < static_cast<ssize_t>(write_size))) {
      // clang-format off
      LOG_ERROR(
          "Read partial data from FUSE device of file: `, nodeid: `, offset: `, count: `, actual: `",
          upload_path_, inode_->nodeid, offset + written - r, write_size, r);
      // clang-format on
      break;
    }

    if (buffer_off == upload_buffer_size) {
      if (appendable_status_.is_appendable_object) {
        r = do_append_upload(++buffer_index_);
      } else {
        r = schedule_multipart_upload(++buffer_index_);
      }

      if (r < 0) {
        break;
      }

      buffer_size_ = 0;
      buffer_off = 0;
    }
  }

  if (r < 0) {
    LOG_ERROR("Failed to write file: `, nodeid:`, r: `, offset: `, count: `",
              upload_path_, inode_->nodeid, r, offset, count);
    inode_->invalidate_data_cache = true;
    immutable_ = true;

    if (buffer_ != nullptr) {
      free_buffer(buffer_);
      buffer_ = nullptr;
    }

    return r;
  } else {
    inode_->attr.size = std::max(inode_->attr.size, offset + written);
    write_off_ = offset + written;
    mark_dirty();
  }

  return written;
}

int OssWriter::schedule_multipart_upload(size_t part_number) {
  int r = 0;
  if (upload_context_ == nullptr) {
    r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_init_multipart_upload,
                                       upload_path_, &upload_context_);
    if (r < 0) {
      LOG_ERROR("Failed to init multipart upload: `, nodeid: ` r: `",
                upload_path_, inode_->nodeid, r);
      return r;
    }
  }

  fs_->upload_sem_->wait(1);
  running_upload_tasks_.fetch_add(1);

  auto ctx = new MultipartContext;
  ctx->writer = this;
  ctx->part_number = part_number;
  ctx->buf = buffer_;
  buffer_ = nullptr;

  auto th = photon::thread_create(do_multipart_upload, ctx);
  photon::thread_migrate(th,
                         fs_->bg_vcpu_env_.bg_oss_client_env->get_vcpu_next());

  return r;
}

int OssWriter::complete_upload() {
  int r = 0;

  if (inode_->is_stale) {
    LOG_ERROR("aborting upload for deleted file: `", upload_path_);
    immutable_ = true;
    if (upload_context_) {
      complete_or_abort_multipart();
      upload_context_ = nullptr;
    } else if (buffer_) {
      free_buffer(buffer_);
      buffer_ = nullptr;
    }
    return 0;
  }

  if ((write_off_ == -1 && inode_->attr.size == 0) && get_is_dirty()) {
    write_off_ = 0;
    return upload_empty_file();
  }

  if (immutable_) {
    if (upload_context_) goto exit;
    return -EIO;
  }

  if (buffer_ == nullptr && upload_context_ == nullptr) {
    return 0;
  }

  if (buffer_ != nullptr) {
    auto background_env =
        fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next();

    r = background_env.executor->perform([&]() -> ssize_t {
      auto oss_client = background_env.oss_client;

      iovec iov{buffer_, buffer_size_};
      ssize_t ret = 0;
      uint64_t *expected_crc64 = verify_crc64() ? &expected_crc64_ : nullptr;
      if (upload_context_ != nullptr) {
        ret = oss_client->oss_upload_part(upload_context_, &iov, 1,
                                          ++buffer_index_);
        if (ret < 0) {
          LOG_ERROR("Failed to upload file: `, nodeid: `, part: ` r: `",
                    upload_path_, inode_->nodeid, buffer_index_, ret);
          immutable_ = true;
        }

        return 0;
      }

      if (appendable_status_.is_appendable_object) {
        off_t off = appendable_status_.valid_buffer_offset +
                    buffer_index_ * fs_->options_.upload_buffer_size;
        char *buf = buffer_ + appendable_status_.valid_buffer_offset;
        size_t upload_size =
            buffer_size_ - appendable_status_.valid_buffer_offset;
        iovec iov{buf, upload_size};
        ret = oss_client->oss_append_object(upload_path_, &iov, 1, off,
                                            expected_crc64);
      } else {
        ret = oss_client->oss_put_object(upload_path_, &iov, 1, expected_crc64);
      }

      if (ret < 0) {
        LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", upload_path_,
                  inode_->nodeid, ret);
        immutable_ = true;
      } else {
        ret = 0;
      }

      return ret;
    });

    free_buffer(buffer_);
    buffer_ = nullptr;

    // For an appendable object, we should store the next offset of valid data
    // after uploading data to keep the offset of buffer mapped to the object
    // data. For a normal object, we always download the last part of the object
    // to the buffer, so buffer_size_ will be updated during remote-data-merge.
    if (appendable_status_.is_appendable_object) {
      appendable_status_.valid_buffer_offset = buffer_size_;
    } else {
      buffer_size_ = 0;
    }
  }

exit:
  if (upload_context_ != nullptr) {
    r = complete_or_abort_multipart();
    upload_context_ = nullptr;
    buffer_index_ = 0;
  }

  return r;
}

void *OssWriter::do_multipart_upload(void *args) {
  auto ctx = static_cast<MultipartContext *>(args);
  thread_local auto oss_client =
      ctx->writer->fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client();

  iovec iov;
  iov.iov_base = ctx->buf;
  iov.iov_len = ctx->writer->fs_->options_.upload_buffer_size;
  int r = oss_client->oss_upload_part(ctx->writer->upload_context_, &iov, 1,
                                      ctx->part_number);
  if (r < 0) {
    LOG_ERROR("Failed to upload file: `, part: ` r: `",
              ctx->writer->inode_->nodeid, ctx->part_number, r);
    ctx->writer->immutable_ = true;
  }

  ctx->writer->fs_->upload_sem_->signal(1);
  ctx->writer->running_upload_tasks_.fetch_sub(1);

  ctx->writer->free_buffer(ctx->buf);

  delete ctx;
  return nullptr;
}

int OssWriter::complete_or_abort_multipart() {
  auto background_env =
      fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next();
  return background_env.executor->perform([&]() {
    int r = 0;
    while (running_upload_tasks_.load() > 0) {
      AUTO_USLEEP(1000);
    }

    auto oss_client = background_env.oss_client;
    if (!immutable_) {
      r = oss_client->oss_complete_multipart_upload(
          upload_context_, verify_crc64() ? &expected_crc64_ : nullptr);
      if (r < 0) {
        LOG_ERROR("Failed to complete multipart file: `, r: `", inode_->nodeid,
                  r);
        immutable_ = true;
      }
    } else {
      int abort_r = oss_client->oss_abort_multipart_upload(upload_context_);
      if (abort_r < 0) {
        LOG_ERROR("Failed to abort multipart upload: ` r: `", inode_->nodeid,
                  abort_r);
      }

      r = -EIO;

      if (buffer_) {
        free_buffer(buffer_);
        buffer_ = nullptr;
      }
    }

    return r;
  });
}

int OssWriter::upload_empty_file() {
  auto background_env =
      fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next();
  int r = background_env.executor->perform([&]() {
    auto oss_client = background_env.oss_client;

    ssize_t ret = 0;
    iovec iov{nullptr, 0};
    if (fs_->options_.enable_appendable_object) {
      ret = oss_client->oss_append_object(upload_path_, &iov, 1, 0,
                                          &expected_crc64_);
      if (ret == 0) appendable_status_.is_appendable_object = true;
    } else {
      ret = oss_client->oss_put_object(upload_path_, &iov, 1, &expected_crc64_);
    }
    if (ret < 0) {
      LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", upload_path_,
                inode_->nodeid, ret);
      immutable_ = true;
    }
    return ret;
  });

  return r;
}

char *OssWriter::get_buffer() {
  // TODO: use dynamic buffer size to support large file
  //       more intelligently.
  return fs_->upload_buffers_->allocate(1).front();
}

void OssWriter::free_buffer(char *ptr) {
  std::vector<char *> ptr_vec{ptr};
  fs_->upload_buffers_->deallocate(ptr_vec);
}

void OssWriter::mark_clean() {
  if (get_is_dirty()) {
    LOG_INFO("file: `, nodeid: `, size: ` is marked to clean", upload_path_,
             inode_->nodeid, inode_->attr.size);

    // Always reset flag after release().
    inode_->invalidate_data_cache = true;

    // After every write op, FUSE kernel will invalidate attr cache(see details
    // in fuse_perfrom_write), so we only need to reset attr_time and delay
    // updating until next getattr().
    inode_->attr_time = 0;

    is_dirty_ = false;
    inode_->is_dirty = false;

    RELEASE_ASSERT(inode_->dirty_fh != nullptr);
    inode_->dirty_fh = nullptr;
    fs_->erase_dirty_nodeid(inode_->nodeid);
  }
}

int OssWriter::merge_remote_data(off_t offset, size_t count) {
  if (!already_head_) {
    ObjectHeaderMeta obj_meta;
    int r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_head_object, upload_path_,
                                           obj_meta);
    if (r < 0) {
      LOG_ERROR("Failed to head file: `, nodeid: ` r: `", upload_path_,
                inode_->nodeid, r);
      return r;
    }

    // Archived objects not supported.
    if (obj_meta.storage_class != "Standard" &&
        obj_meta.storage_class != "IA") {
      LOG_ERROR("File: `, nodeid: `, is unsupported storge class: `",
                upload_path_, inode_->nodeid, obj_meta.storage_class);
      return -ENOTSUP;
    }

    has_crc64_ = obj_meta.has_crc64();
    expected_crc64_ = obj_meta.crc64;
    if (IS_FAULT_INJECTION_ENABLED(FI_OssError_No_Crc64)) {
      has_crc64_ = false;
      expected_crc64_ = 0;
    }
    if (!has_crc64_) {
      LOG_WARN("File: `, nodeid: `, has no crc64, will not check crc64",
               upload_path_, inode_->nodeid);
    }
    already_head_ = true;

    LOG_INFO(
        "Trying to merge file: `, inode: `, size: `, remote_size: `, type: `",
        upload_path_, inode_->nodeid, inode_->attr.size, obj_meta.size,
        obj_meta.type);

    inode_->attr.size = obj_meta.size;
    write_off_ = obj_meta.size;

    if (offset != write_off_) {
      LOG_ERROR("append only file: `, size: `, offset: `, write_off_: `",
                upload_path_, inode_->attr.size, offset, write_off_);
      return -EINVAL;
    }

    if (fs_->options_.enable_appendable_object &&
        obj_meta.type == kOssObjectTypeAppendable &&
        offset + count <= kOssMaxAppendableObjectSize) {
      appendable_status_.is_appendable_object = true;
      appendable_status_.valid_buffer_offset =
          obj_meta.size % fs_->options_.upload_buffer_size;
      buffer_size_ = appendable_status_.valid_buffer_offset;
      buffer_index_ = obj_meta.size / fs_->options_.upload_buffer_size;
    } else if (obj_meta.type == "Symlink") {
      return -ENOTSUP;
    }
  }

  if (!fs_->options_.enable_appendable_object) {
    return merge_remote_data_of_normal_object();
  }

  if (appendable_status_.is_appendable_object) {
    return 0;
  }

  if (inode_->attr.size <=
      fs_->options_.appendable_object_autoswitch_threshold) {
    return switch_object_type_normal_to_appendable();
  } else {
    return -ENOTSUP;
  }
}

int OssWriter::merge_remote_data_of_normal_object() {
  const size_t part_size = fs_->options_.upload_buffer_size;
  size_t copy_num_parts = inode_->attr.size / part_size;
  if (copy_num_parts > 0) {
    RELEASE_ASSERT(upload_context_ == nullptr && buffer_index_ == 0);
    int r = DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_init_multipart_upload,
                                           upload_path_, &upload_context_);
    if (r < 0) {
      LOG_ERROR("Failed to init multipart upload: `, nodeid: ` r: `",
                upload_path_, inode_->nodeid, r);
      return r;
    }

    for (size_t i = 0; i < copy_num_parts; i++) {
      fs_->upload_copy_sem_->wait(1);
      running_upload_tasks_.fetch_add(1);

      auto ctx = new MultipartContext;
      ctx->writer = this;
      ctx->buf = nullptr;
      ctx->part_number = ++buffer_index_;

      auto th = photon::thread_create(do_multipart_copy, ctx);
      photon::thread_migrate(
          th, fs_->bg_vcpu_env_.bg_oss_client_env->get_vcpu_next());
    }
  }

  // Download the remained data to the buffer.
  size_t remain_size = inode_->attr.size % part_size;
  RELEASE_ASSERT(buffer_ == nullptr && buffer_size_ == 0);

  buffer_ = get_buffer();

  iovec iov{buffer_, remain_size};
  IOVector bufv(&iov, 1);

  off_t offset = copy_num_parts * part_size;
  int r =
      DO_SYNC_BACKGROUND_OSS_REQUEST(fs_, oss_get_object_range, upload_path_,
                                     bufv.iovec(), bufv.iovcnt(), offset);
  if (IS_FAULT_INJECTION_ENABLED(FI_Download_Failed_During_Merge_Remote_Data)) {
    r = -EIO;
  }

  if (r < 0) {
    LOG_ERROR(
        "Failed to download file: `, nodeid: `, offset: `, count: `, r: `",
        upload_path_, inode_->nodeid, offset, remain_size, r);
    immutable_ = true;
    free_buffer(buffer_);
    buffer_ = nullptr;
  } else {
    buffer_size_ = remain_size;
  }

  return r;
}

void *OssWriter::do_multipart_copy(void *args) {
  auto ctx = static_cast<MultipartContext *>(args);
  thread_local auto oss_client =
      ctx->writer->fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client();

  off_t offset =
      (ctx->part_number - 1) * ctx->writer->fs_->options_.upload_buffer_size;
  size_t count = ctx->writer->fs_->options_.upload_buffer_size;

  int r = oss_client->oss_upload_part_copy(ctx->writer->upload_context_, offset,
                                           count, ctx->part_number);
  if (r < 0) {
    LOG_ERROR("Failed to upload file: `, part: ` r: `",
              ctx->writer->inode_->nodeid, ctx->part_number, r);
    ctx->writer->immutable_ = true;
  }
  ctx->writer->fs_->upload_copy_sem_->signal(1);
  ctx->writer->running_upload_tasks_.fetch_sub(1);

  delete ctx;
  return nullptr;
}

int OssWriter::do_append_upload(size_t part_number) {
  off_t off = appendable_status_.valid_buffer_offset +
              (part_number - 1) * fs_->options_.upload_buffer_size;
  char *buf = buffer_ + appendable_status_.valid_buffer_offset;
  size_t upload_size = buffer_size_ - appendable_status_.valid_buffer_offset;
  appendable_status_.valid_buffer_offset = 0;

  iovec iov{buf, upload_size};
  int r = DO_SYNC_BACKGROUND_OSS_REQUEST(
      fs_, oss_append_object, upload_path_, &iov, 1, off,
      verify_crc64() ? &expected_crc64_ : nullptr);
  if (r < 0) {
    LOG_ERROR("Failed to do append upload file: `, nodeid: `, r: `",
              upload_path_, inode_->nodeid, r);
  }

  return r;
}

int OssWriter::switch_object_type_normal_to_appendable() {
  auto generate_hidden_object_name_fn = [&](std::string *out) {
    // prefix + "_" + object name + "_" + random string + "_" + unix timestamp
    const std::string prefix = ".ossfs_hidden_file";
    const std::string random_str = random_string(8);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    long long timestamp_ms = ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    const std::string timestamp_str = std::to_string(timestamp_ms);

    // path starts with "/".
    std::string path(upload_path_);
    size_t pos = path.rfind("/");
    RELEASE_ASSERT(pos != std::string::npos);
    std::string parent = path.substr(0, pos);
    std::string name = path.substr(pos + 1);

    std::string new_name =
        prefix + "_" + name + "_" + random_str + "_" + timestamp_str;
    *out = parent + "/" + new_name;

    return 0;
  };

  const size_t part_size = fs_->options_.upload_buffer_size;
  size_t copy_num_parts = inode_->attr.size / part_size;

  RELEASE_ASSERT(buffer_ == nullptr && buffer_size_ == 0);
  buffer_ = get_buffer();

  auto background_env =
      fs_->bg_vcpu_env_.bg_oss_client_env->get_oss_client_env_next();
  int r = background_env.executor->perform([&]() {
    auto oss_client = background_env.oss_client;

    // 1. Rename the remote file to the tmpfile.
    // TODO: add retry if oss return error FileAlreadyExists
    bool need_copy = inode_->attr.size > 0;
    int r = 0;
    std::string tmp_file_path;
    r = generate_hidden_object_name_fn(&tmp_file_path);
    if (r < 0) return r;

    if (need_copy) {
      r = oss_client->oss_copy_object(upload_path_, tmp_file_path,
                                      false /*overwrite*/);
      if (r < 0) {
        LOG_ERROR("Failed to rename file: ` to `, nodeid: `, r: `",
                  upload_path_, tmp_file_path, inode_->nodeid, r);
        return r;
      }
    }

    // 2. Delete the old file.
    r = oss_client->oss_delete_object(upload_path_);
    if (r < 0) {
      LOG_ERROR("Failed to delete file: `, nodeid: `, r: `", upload_path_,
                inode_->nodeid, r);
      return r;
    }

    // 3. Doing switch.
    // TODO: use pipeline to optimize the speed
    has_crc64_ = true;
    expected_crc64_ = 0;
    buffer_size_ = inode_->attr.size % part_size;
    if (buffer_size_ > 0) copy_num_parts++;

    for (size_t i = 0; i < copy_num_parts; i++) {
      size_t copy_size = (buffer_size_ > 0 && i == copy_num_parts - 1)
                             ? buffer_size_
                             : part_size;

      iovec iov{buffer_, copy_size};
      IOVector bufv(&iov, 1);

      ssize_t ret = oss_client->oss_get_object_range(
          tmp_file_path, bufv.iovec(), bufv.iovcnt(), i * part_size);
      if (ret < 0) {
        // clang-format off
        LOG_ERROR(
            "Failed to download file: `, nodeid: `, offset: `, count: `, r: `",
            tmp_file_path, inode_->nodeid, i * part_size, part_size, ret);
        // clang-format on
        return static_cast<int>(ret);
      }

      uint64_t *crc64_ptr = nullptr;
      if (verify_crc64()) {
        expected_crc64_ = crc64ecma(buffer_, copy_size, expected_crc64_);
        crc64_ptr = &expected_crc64_;
      }

      ret = oss_client->oss_append_object(upload_path_, &iov, 1, i * part_size,
                                          crc64_ptr);
      if (ret < 0) {
        LOG_ERROR("Failed to append upload file: `, nodeid: `, r: `",
                  upload_path_, inode_->nodeid, ret);
        return static_cast<int>(ret);
      }
    }

    // 4. Delete the tmpfile and ignore error.
    if (need_copy) {
      r = oss_client->oss_delete_object(tmp_file_path.c_str());
      if (r < 0) {
        LOG_ERROR("Failed to delete tmpfile: `, nodeid: `, r: `", tmp_file_path,
                  inode_->nodeid, r);
      };
    }

    appendable_status_.valid_buffer_offset = buffer_size_;
    appendable_status_.is_appendable_object = true;
    buffer_index_ = inode_->attr.size / part_size;
    return 0;
  });

  if (r < 0) {
    free_buffer(buffer_);
    buffer_ = nullptr;
    buffer_size_ = 0;
  }

  return r;
}

std::unique_ptr<IWriter> create_oss_writer(OssFs *fs, std::string_view path,
                                           FileInode *inode, int flags,
                                           bool is_dirty) {
  return std::make_unique<OssWriter>(fs, path, inode, flags);
}

}  // namespace OssFileSystem