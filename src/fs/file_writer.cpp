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

#include <fcntl.h>
#include <photon/common/checksum/crc64ecma.h>
#include <photon/common/iovector.h>
#include <photon/common/utility.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <vector>

#include "common/crc64_combine.h"
#include "common/fuse.h"
#include "common/fuse_buf_utils.h"
#include "error_codes.h"
#include "file.h"
#include "fs.h"
#include "metric/metrics.h"
#include "random_write_context.h"

static constexpr int kOssMaxPartNumber = 10000;
static constexpr size_t kOssMaxAppendableObjectSize = 5368709120ULL;  // 5GB
static constexpr uint64_t kOssMaxPartSize = 5ULL * 1024 * 1024 * 1024;

namespace OssFileSystem {

uint64_t calc_random_write_part_size(uint64_t file_size,
                                     uint64_t base_part_size,
                                     uint64_t chunk_size) {
  if (base_part_size > kOssMaxPartSize) return 0;

  uint64_t num_parts = (file_size + base_part_size - 1) / base_part_size;
  if (num_parts <= static_cast<uint64_t>(kOssMaxPartNumber))
    return base_part_size;

  uint64_t min_needed = (file_size + kOssMaxPartNumber - 1) / kOssMaxPartNumber;
  uint64_t part_size = align_up(min_needed, chunk_size);
  return part_size <= kOssMaxPartSize ? part_size : 0;
}

static ssize_t pwrite_fd(int fd, const char *buf, size_t size, off_t off) {
  size_t total = 0;
  while (total < size) {
    ssize_t w = ::pwrite(fd, buf + total, size - total, off + total);
    if (w < 0 && errno == EINTR) continue;

    FAULT_INJECTION(FI_Pwrite_Staging_Short_Write, [&]() {
      if (w > 1) w /= 2;
    });
    FAULT_INJECTION(FI_Pwrite_Staging_Fail, [&]() {
      w = -EIO;
      errno = EIO;
    });

    if (w <= 0) {
      if (total > 0) return static_cast<ssize_t>(total);
      return (w == 0) ? -EIO : -errno;
    }
    total += w;
  }
  return static_cast<ssize_t>(total);
}

static int64_t get_fd_disk_bytes(int fd) {
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    LOG_ERROR_RETURN(0, r, "fail to fstat fd `, err: `", fd, r);
  }
  return static_cast<int64_t>(st.st_blocks) * S_BLKSIZE;
}

static std::string random_string(int length) {
  static std::string charset =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
  std::string result;
  result.resize(length);

  for (int i = 0; i < length; i++)
    result[i] = charset[rand() % charset.length()];

  return result;
}

static ssize_t fuse_bufvec_to_fd(int dest_fd, off_t dest_off, size_t count,
                                 struct fuse_bufvec *bufv) {
  // TODO: use upload_buffer.
  static constexpr size_t kTmpBufSize = 1 * 1024 * 1024;
  std::unique_ptr<char[]> tmp_buf(new char[kTmpBufSize]);

  ssize_t written = 0;
  while (written < static_cast<ssize_t>(count)) {
    size_t to_copy =
        std::min(count - static_cast<size_t>(written), kTmpBufSize);
    ssize_t copied = fuse_bufvec_to_buf_copy(tmp_buf.get(), bufv, to_copy);
    if (copied <= 0) break;

    ssize_t r = pwrite_fd(dest_fd, tmp_buf.get(), copied, dest_off + written);
    if (r < 0) return r;
    written += r;

    if (r < copied || copied < static_cast<ssize_t>(to_copy)) break;
  }

  return written;
}

class OssWriter : public IWriter {
 public:
  OssWriter(OssFs *fs, std::string_view path, FileInode *inode, int flags)
      : fs_(fs), inode_(inode), upload_path_(path), open_flags_(flags) {}

  void close() override {}

  size_t calc_remote_size() override {
    return 0;
  }

  bool get_is_dirty() override {
    return is_dirty_;
  }

  bool get_is_immutable() override {
    return immutable_;
  }

  ssize_t pread_from_local(void *buf, size_t count, off_t offset) override {
    return -ENOTSUP;
  }

  int truncate(uint64_t new_size) override {
    return -ENOTSUP;
  }

 protected:
  inline void waiting_upload_tasks() {
    while (running_upload_tasks_.load() > 0) {
      AUTO_USLEEP(1000);
    }
  }

  virtual void mark_dirty();
  virtual void mark_clean();

  OssFs *fs_ = nullptr;
  FileInode *inode_ = nullptr;

  // If a file is renamed during writing, upload_path_ will be updated to the
  // new path to ensure the file can be uploaded to the correct location.
  std::string upload_path_;

  int open_flags_ = 0;
  bool is_dirty_ = false;

  // On write failure, we cannot recover or safely continue I/O.
  // Mark the file as immutable and reject all subsequent write, flush, and
  // fsync operations.
  std::atomic<bool> immutable_ = {false};
  std::atomic<uint64_t> running_upload_tasks_ = {0};
};

// Memory-buffered sequential writer (supports normal multipart and appendable
// modes).
class OssSeqWriter : public OssWriter {
 public:
  OssSeqWriter(OssFs *fs, std::string_view path, FileInode *inode, int flags)
      : OssWriter(fs, path, inode, flags) {}

  ~OssSeqWriter() override;

  int open() override;
  ssize_t pwrite(size_t count, off_t offset, const void *buf,
                 struct fuse_bufvec *bufv, std::string *wpath) override;
  int flush() override;

 protected:
  // The following functions need inode's wlock.
  inline int check_writing_permission();
  inline int check_and_update_write_offset(off_t &offset);

  inline char *get_buffer();
  inline void free_buffer(char *ptr);
  inline void clear_current_buffer();
  inline bool verify_crc64();

  int complete_upload();
  int get_object_attributes_from_remote();
  int prepare_merge_remote_data(off_t &offset);

  virtual int check_file_size_limit(size_t count) = 0;

  virtual int on_first_write(off_t &offset, size_t &count) = 0;
  virtual void on_upload_fail_cleanup() = 0;

  virtual int truncate_internal() = 0;
  virtual int do_merge_remote_data() = 0;

  virtual int do_upload_buffer() = 0;
  virtual int do_upload_empty() = 0;
  virtual int do_upload_last_part() = 0;

  virtual int schedule_upload(size_t part_number) = 0;

  virtual size_t get_fault_injection_buffer_index() {
    return 0;
  };

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

  bool already_head_ = false;
  std::string object_type_;
};

// Streaming (Put/Multipart) object writer
class OssStreamingWriter : public OssSeqWriter {
 public:
  OssStreamingWriter(OssFs *fs, std::string_view path, FileInode *inode,
                     int flags)
      : OssSeqWriter(fs, path, inode, flags) {}

 private:
  int check_file_size_limit(size_t count) override;

  int on_first_write(off_t &offset, size_t &count) override;
  void on_upload_fail_cleanup() override;

  int truncate_internal() override;
  int do_merge_remote_data() override;

  int do_upload_buffer() override;
  int do_upload_empty() override;
  int do_upload_last_part() override;

  int schedule_upload(size_t part_number) override;

  struct MultipartContext {
    OssStreamingWriter *writer = nullptr;
    char *buf = nullptr;
    size_t part_number = 0;
  };

  static void *do_multipart_upload(void *args);
  static void *do_multipart_copy(void *args);

  int do_complete_multipart();
  void do_abort_multipart();
  int schedule_multipart_upload(size_t part_number);
  int merge_remote_data_of_normal_object();

  void *upload_context_ = nullptr;
  friend class OssSeqWriter;
};

class OssAppendableWriter : public OssSeqWriter {
 public:
  OssAppendableWriter(OssFs *fs, std::string_view path, FileInode *inode,
                      int flags)
      : OssSeqWriter(fs, path, inode, flags) {}

  ssize_t pread_from_local(void *buf, size_t count, off_t offset) override;
  size_t calc_remote_size() override;

 protected:
  int check_file_size_limit(size_t count) override;

  int on_first_write(off_t &offset, size_t &count) override;
  void on_upload_fail_cleanup() override{};

  int truncate_internal() override;
  int do_merge_remote_data() override;

  int do_upload_buffer() override;
  int do_upload_empty() override;
  int do_upload_last_part() override;

  int schedule_upload(size_t part_number) override;

  size_t get_fault_injection_buffer_index() override;

 private:
  int do_append_upload(size_t part_number);
  int switch_object_type_normal_to_appendable();

  off_t valid_buffer_offset = 0;
  friend class OssSeqWriter;
};

class OssRandomWriter : public OssWriter {
 public:
  OssRandomWriter(OssFs *fs, std::string_view path, FileInode *inode, int flags)
      : OssWriter(fs, path, inode, flags) {}

  int open() override;
  ssize_t pwrite(size_t count, off_t offset, const void *buf,
                 struct fuse_bufvec *bufv, std::string *wpath) override;
  int flush() override;
  void close() override;
  bool get_is_dirty() override;
  int truncate(uint64_t new_size) override;

 private:
  int create_staging(RandomWriteContext &ctx);
  void mark_dirty() override;
  void mark_clean() override;
  int fetch_chunks(const std::vector<uint64_t> &cids);

  // ── disk space protection ──
  int reserve_randwrite_disk_budget(uint64_t bytes);
  void release_randwrite_disk_budget(uint64_t bytes);
  int64_t refresh_staging_bytes();

  // Serializes publish_refill_clean_chunk_growth() across parallel
  // multipart refill threads.
  photon::mutex refill_clean_chunk_growth_mtx_;
  void publish_refill_clean_chunk_growth();

  // ── flush helpers ──
  int flush_no_multipart(uint64_t file_size);
  int flush_multipart(uint64_t file_size);
  int refill_range(uint64_t range_begin, uint64_t range_end,
                   IObjStore *obj_store);
  int refill_clean_chunk(uint64_t pos, uint64_t chunk_end,
                         IObjStore *obj_store);

  // ── parallel upload support ──
  std::atomic<int> upload_ret_ = {0};
  std::atomic<bool> crc_incomplete_ = {false};
  void set_upload_error(int err);

  struct MultipartContext {
    OssRandomWriter *writer = nullptr;
    void *upload_ctx = nullptr;
    uint64_t part_begin = 0;
    uint64_t part_end = 0;
    int part_number = 0;
    uint64_t *part_crcs = nullptr;
  };
  static void *do_upload_or_copy_part(void *args);
};

void OssWriter::mark_dirty() {
  if (!get_is_dirty()) {
    LOG_INFO("file: `, nodeid: ` is marked to dirty", upload_path_,
             inode_->nodeid);

    inode_->is_dirty = true;
    is_dirty_ = true;

    fs_->add_dirty_nodeid(inode_->nodeid);
  }
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

OssSeqWriter::~OssSeqWriter() {
  waiting_upload_tasks();
  RELEASE_ASSERT(buffer_ == nullptr);
}

int OssSeqWriter::flush() {
  int r = complete_upload();
  FAULT_INJECTION(FI_Data_Sync_Failed, [&]() { r = -EIO; });
  if (r < 0) {
    LOG_ERROR("Failed to fdatasync file: `, nodeid: `, r: `", upload_path_,
              inode_->nodeid, r);
  }
  mark_clean();
  return r;
}

int OssSeqWriter::check_writing_permission() {
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

int OssSeqWriter::check_and_update_write_offset(off_t &offset) {
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

  return 0;
}

bool OssSeqWriter::verify_crc64() {
  return fs_->options_.enable_crc64 && has_crc64_;
}

int OssSeqWriter::open() {
  if (open_flags_ & O_CREAT) {
    mark_dirty();
  } else if (open_flags_ & O_TRUNC) {
    int r = truncate_internal();
    if (r < 0) return r;

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode_->etag.clear();
    inode_->update_attr(0, now);
  }
  return 0;
}

ssize_t OssSeqWriter::pwrite(size_t count, off_t offset, const void *buf,
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
    FAULT_INJECTION(FI_Force_Flush_Dirty_Handle_Delay,
                    []() { AUTO_USLEEP(2'000'000); });
  }

  r = check_writing_permission();
  if (r < 0) return r;

  r = check_and_update_write_offset(offset);
  if (r < 0) return r;

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

    r = on_first_write(offset, count);
    if (r < 0) return r;
  }

  uint64_t buffer_off = buffer_size_;
  const uint64_t upload_buffer_size = fs_->options_.upload_buffer_size;

  r = check_file_size_limit(count);
  if (r < 0) return r;

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
      DECLARE_METRIC_LATENCY(fuse_bufv_copy, Metric::kInternalMetrics);
      r = fuse_bufvec_to_buf_copy(buffer_ + buffer_off, bufv, write_size);
    }

    if (r < 0) break;

    if (verify_crc64()) {
      expected_crc64_ = crc64ecma(buffer_ + buffer_off, r, expected_crc64_);
    }

    FAULT_INJECTION(FI_Modify_Write_Buffer, [&]() {
      size_t index = get_fault_injection_buffer_index();
      char old = '\0';
      do {
        old = buffer_[index];
        buffer_[index] = (buffer_[index] + 1) % 256;
      } while (buffer_[index] == old);
    });

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
      r = schedule_upload(++buffer_index_);
      if (r < 0) break;

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

int OssSeqWriter::complete_upload() {
  int r = 0;

  if (inode_->is_stale) {
    LOG_ERROR("aborting upload for deleted file: `", upload_path_);
    immutable_ = true;
    goto cleanup;
  }

  if (immutable_) {
    r = -EIO;
    goto cleanup;
  }

  if ((write_off_ == -1 && inode_->attr.size == 0) && get_is_dirty()) {
    write_off_ = 0;
    return do_upload_empty();
  }

  // Upload remaining buffer and do final completion
  r = do_upload_last_part();
  if (r < 0) {
    goto cleanup;
  }

  return 0;

cleanup:
  clear_current_buffer();
  on_upload_fail_cleanup();
  return r;
}

char *OssSeqWriter::get_buffer() {
  // TODO: use dynamic buffer size to support large file
  //       more intelligently.
  return fs_->upload_buffers_->allocate(1).front();
}

void OssSeqWriter::free_buffer(char *ptr) {
  std::vector<char *> ptr_vec{ptr};
  fs_->upload_buffers_->deallocate(ptr_vec);
}

void OssSeqWriter::clear_current_buffer() {
  if (buffer_) {
    free_buffer(buffer_);
    buffer_ = nullptr;
    buffer_size_ = 0;
  }
}

int OssSeqWriter::get_object_attributes_from_remote() {
  ObjHeaderMeta obj_meta;
  int r =
      PERFORM_BACKGROUND_OBJ_REQUEST(fs_, head_object, upload_path_, obj_meta);
  if (r < 0) {
    LOG_ERROR("Failed to head file: `, nodeid: ` r: `", upload_path_,
              inode_->nodeid, r);
    return r;
  }

  // Archived objects not supported.
  if (obj_meta.storage_class != "Standard" && obj_meta.storage_class != "IA") {
    LOG_ERROR("File: `, nodeid: `, is unsupported storge class: `",
              upload_path_, inode_->nodeid, obj_meta.storage_class);
    return -ENOTSUP;
  }

  has_crc64_ = obj_meta.has_crc64();
  expected_crc64_ = obj_meta.crc64;
  FAULT_INJECTION(FI_OssError_No_Crc64, [&]() {
    has_crc64_ = false;
    expected_crc64_ = 0;
  });
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

  // Store object type for derived class to use
  object_type_ = obj_meta.type;

  // Symlink is not supported for both normal and appendable writers
  if (object_type_ == kOssObjectTypeSymlink) {
    return -ENOTSUP;
  }

  return 0;
}

int OssSeqWriter::prepare_merge_remote_data(off_t &offset) {
  int r = 0;
  if (!already_head_) {
    r = get_object_attributes_from_remote();
    if (r < 0) return r;
  }

  return check_and_update_write_offset(offset);
}

int OssStreamingWriter::schedule_multipart_upload(size_t part_number) {
  int r = 0;
  if (upload_context_ == nullptr) {
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, init_multipart_upload, upload_path_,
                                       &upload_context_);
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
                         fs_->bg_vcpu_env_.bg_obj_store_env->get_vcpu_next());

  return r;
}

void *OssStreamingWriter::do_multipart_upload(void *args) {
  auto ctx = static_cast<MultipartContext *>(args);
  thread_local auto obj_store =
      ctx->writer->fs_->bg_vcpu_env_.bg_obj_store_env->get_obj_store();

  iovec iov;
  iov.iov_base = ctx->buf;
  iov.iov_len = ctx->writer->fs_->options_.upload_buffer_size;
  int r = obj_store->upload_part(ctx->writer->upload_context_, &iov, 1,
                                 ctx->part_number);
  if (r < 0) {
    LOG_ERROR("Failed to upload file: `, part: ` r: `",
              ctx->writer->inode_->nodeid, ctx->part_number, r);
    ctx->writer->immutable_ = true;
  }

  ctx->writer->free_buffer(ctx->buf);
  ctx->writer->fs_->upload_sem_->signal(1);
  ctx->writer->running_upload_tasks_.fetch_sub(1);

  delete ctx;
  return nullptr;
}

int OssStreamingWriter::do_complete_multipart() {
  RELEASE_ASSERT(upload_context_ != nullptr);

  waiting_upload_tasks();
  if (immutable_) {
    return -EIO;
  }

  int r = PERFORM_BACKGROUND_OBJ_REQUEST(
      fs_, complete_multipart_upload, upload_context_,
      verify_crc64() ? &expected_crc64_ : nullptr);
  if (r < 0) {
    LOG_ERROR("Failed to complete multipart file: `, r: `", inode_->nodeid, r);
    immutable_ = true;
  }

  upload_context_ = nullptr;
  buffer_index_ = 0;
  return r;
}

void OssStreamingWriter::do_abort_multipart() {
  RELEASE_ASSERT(immutable_);
  RELEASE_ASSERT(upload_context_ != nullptr);

  waiting_upload_tasks();
  int r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, abort_multipart_upload,
                                         upload_context_);
  if (r < 0) {
    LOG_ERROR("Failed to abort multipart upload: ` r: `", inode_->nodeid, r);
  }

  upload_context_ = nullptr;
  buffer_index_ = 0;
}

int OssStreamingWriter::do_upload_empty() {
  iovec iov{nullptr, 0};
  ssize_t r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, put_object, upload_path_,
                                             &iov, 1, &expected_crc64_);
  if (r < 0) {
    LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", upload_path_,
              inode_->nodeid, r);
    immutable_ = true;
    return r;
  }

  return 0;
}

int OssStreamingWriter::merge_remote_data_of_normal_object() {
  const size_t part_size = fs_->options_.upload_buffer_size;
  size_t copy_num_parts = inode_->attr.size / part_size;
  if (copy_num_parts > 0) {
    RELEASE_ASSERT(upload_context_ == nullptr && buffer_index_ == 0);
    int r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, init_multipart_upload,
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
          th, fs_->bg_vcpu_env_.bg_obj_store_env->get_vcpu_next());
    }
  }

  // Download the remained data to the buffer.
  size_t remain_size = inode_->attr.size % part_size;
  RELEASE_ASSERT(buffer_ == nullptr && buffer_size_ == 0);

  buffer_ = get_buffer();

  iovec iov{buffer_, remain_size};
  IOVector bufv(&iov, 1);

  off_t offset = copy_num_parts * part_size;
  int r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, get_object_range, upload_path_,
                                         bufv.iovec(), bufv.iovcnt(), offset);
  FAULT_INJECTION(FI_Download_Failed_During_Merge_Remote_Data,
                  [&]() { r = -EIO; });

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

void *OssStreamingWriter::do_multipart_copy(void *args) {
  auto ctx = static_cast<MultipartContext *>(args);
  thread_local auto obj_store =
      ctx->writer->fs_->bg_vcpu_env_.bg_obj_store_env->get_obj_store();

  off_t offset =
      (ctx->part_number - 1) * ctx->writer->fs_->options_.upload_buffer_size;
  size_t count = ctx->writer->fs_->options_.upload_buffer_size;

  int r = obj_store->upload_part_copy(ctx->writer->upload_context_, offset,
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

int OssStreamingWriter::do_upload_buffer() {
  iovec iov{buffer_, buffer_size_};
  ssize_t r = 0;
  uint64_t *expected_crc64 = verify_crc64() ? &expected_crc64_ : nullptr;

  if (upload_context_ != nullptr) {
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, upload_part, upload_context_, &iov,
                                       1, ++buffer_index_);
    if (r < 0) {
      LOG_ERROR("Failed to upload file: `, nodeid: `, part: ` r: `",
                upload_path_, inode_->nodeid, buffer_index_, r);
    }
  } else {
    r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, put_object, upload_path_, &iov, 1,
                                       expected_crc64);
    if (r < 0) {
      LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", upload_path_,
                inode_->nodeid, r);
    }
  }

  if (r < 0) {
    immutable_ = true;
    return r;
  }

  return 0;
}

int OssStreamingWriter::on_first_write(off_t &offset, size_t &count) {
  if (write_off_ != 0) {
    int r = prepare_merge_remote_data(offset);
    if (r < 0) return r;

    return do_merge_remote_data();
  }
  return 0;
}

int OssStreamingWriter::truncate_internal() {
  // Only mark dirty and overwrite during complete upload
  if (inode_->attr.size != 0) mark_dirty();
  return 0;
}

int OssStreamingWriter::check_file_size_limit(size_t count) {
  // Check if exceeds max part number (10000)
  uint64_t buffer_off = buffer_size_;
  const uint64_t upload_buffer_size = fs_->options_.upload_buffer_size;
  size_t num_new_buffers = (buffer_off + count - 1) / upload_buffer_size;
  if (kOssMaxPartNumber < buffer_index_ + 1 + num_new_buffers) {
    LOG_ERROR("file: `, nodeid: ` has already reached the max part number: `",
              upload_path_, inode_->nodeid, kOssMaxPartNumber);
    return -EFBIG;
  }
  return 0;
}

int OssStreamingWriter::schedule_upload(size_t part_number) {
  return schedule_multipart_upload(part_number);
}

void OssStreamingWriter::on_upload_fail_cleanup() {
  if (upload_context_) {
    do_abort_multipart();
  }
}

int OssStreamingWriter::do_upload_last_part() {
  int r = 0;

  if (buffer_ != nullptr) {
    r = do_upload_buffer();
    clear_current_buffer();
    if (r < 0) {
      return r;
    }
  }

  if (upload_context_ != nullptr) {
    FAULT_INJECTION(FaultInjectionId::FI_Complete_Multipart_Delay, [&]() {
      photon::thread_usleep(500'000);  // 500ms delay
    });
    r = do_complete_multipart();
  }

  return r;
}

int OssStreamingWriter::do_merge_remote_data() {
  return merge_remote_data_of_normal_object();
}

int OssAppendableWriter::do_upload_buffer() {
  off_t off =
      valid_buffer_offset + buffer_index_ * fs_->options_.upload_buffer_size;
  char *buf = buffer_ + valid_buffer_offset;
  size_t upload_size = buffer_size_ - valid_buffer_offset;
  iovec iov{buf, upload_size};

  uint64_t *expected_crc64 = verify_crc64() ? &expected_crc64_ : nullptr;
  ssize_t r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, append_object, upload_path_,
                                             &iov, 1, off, expected_crc64);
  if (r < 0) {
    LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", upload_path_,
              inode_->nodeid, r);
    immutable_ = true;
    return r;
  }

  return 0;
}

int OssAppendableWriter::do_upload_empty() {
  iovec iov{nullptr, 0};
  ssize_t r = PERFORM_BACKGROUND_OBJ_REQUEST(fs_, append_object, upload_path_,
                                             &iov, 1, 0, &expected_crc64_);
  if (r < 0) {
    LOG_ERROR("Failed to upload file: `, nodeid: ` r: `", upload_path_,
              inode_->nodeid, r);
    immutable_ = true;
    return r;
  }

  return 0;
}

int OssAppendableWriter::on_first_write(off_t &offset, size_t &count) {
  if (open_flags_ & O_CREAT) return 0;

  int r = prepare_merge_remote_data(offset);
  if (r < 0) return r;

  return do_merge_remote_data();
}

int OssAppendableWriter::truncate_internal() {
  // Appendable objects should be deleted before writing.
  int r = fs_->truncate_inode_data(inode_, upload_path_, 0);
  if (r < 0) {
    LOG_ERROR("fail to truncate file `, nodeid `, r `", upload_path_,
              inode_->nodeid, r);
    return r;
  }
  return 0;
}

int OssAppendableWriter::check_file_size_limit(size_t count) {
  // Check if exceeds max appendable object size (5GB)
  if (inode_->attr.size + count > kOssMaxAppendableObjectSize) {
    LOG_ERROR("file: `, nodeid: ` exceeds max appendable object size: `",
              upload_path_, inode_->nodeid, kOssMaxAppendableObjectSize);
    return -EFBIG;
  }
  return 0;
}

int OssAppendableWriter::schedule_upload(size_t part_number) {
  return do_append_upload(part_number);
}

size_t OssAppendableWriter::get_fault_injection_buffer_index() {
  return valid_buffer_offset;
}

int OssAppendableWriter::do_merge_remote_data() {
  if (object_type_ == kOssObjectTypeAppendable) {
    valid_buffer_offset = inode_->attr.size % fs_->options_.upload_buffer_size;
    buffer_size_ = valid_buffer_offset;
    buffer_index_ = inode_->attr.size / fs_->options_.upload_buffer_size;
    return 0;
  }

  // Need to switch from normal to appendable
  if (inode_->attr.size <=
      fs_->options_.appendable_object_autoswitch_threshold) {
    return switch_object_type_normal_to_appendable();
  } else {
    return -ENOTSUP;
  }
}

int OssAppendableWriter::do_upload_last_part() {
  int r = 0;

  if (buffer_ != nullptr) {
    r = do_upload_buffer();

    free_buffer(buffer_);
    buffer_ = nullptr;
    if (r < 0) {
      return r;
    }

    // For an appendable object, we should store the next offset of valid data
    // after uploading data to keep the offset of buffer mapped to the object
    // data. For a normal object, we always download the last part of the object
    // to the buffer, so buffer_size_ will be updated during remote-data-merge.
    valid_buffer_offset = buffer_size_;
  }

  return r;
}

size_t OssAppendableWriter::calc_remote_size() {
  size_t dirty_size = buffer_size_ - valid_buffer_offset;
  return inode_->attr.size - dirty_size;
}

ssize_t OssAppendableWriter::pread_from_local(void *buf, size_t count,
                                              off_t offset) {
  if (unlikely(offset >= (int64_t)inode_->attr.size)) {
    return 0;
  }

  RELEASE_ASSERT(buffer_ != nullptr);

  size_t buffer_index = offset / fs_->options_.upload_buffer_size;
  RELEASE_ASSERT(buffer_index == buffer_index_);

  off_t buffer_offset = offset % fs_->options_.upload_buffer_size;
  RELEASE_ASSERT(buffer_offset >= valid_buffer_offset);

  count = std::min(count, buffer_size_ - (size_t)buffer_offset);
  memcpy(static_cast<char *>(buf), buffer_ + buffer_offset, count);
  return count;
}

int OssAppendableWriter::do_append_upload(size_t part_number) {
  off_t off = valid_buffer_offset +
              (part_number - 1) * fs_->options_.upload_buffer_size;
  char *buf = buffer_ + valid_buffer_offset;
  size_t upload_size = buffer_size_ - valid_buffer_offset;
  valid_buffer_offset = 0;

  iovec iov{buf, upload_size};
  int r = PERFORM_BACKGROUND_OBJ_REQUEST(
      fs_, append_object, upload_path_, &iov, 1, off,
      verify_crc64() ? &expected_crc64_ : nullptr);
  if (r < 0) {
    LOG_ERROR("Failed to do append upload file: `, nodeid: `, r: `",
              upload_path_, inode_->nodeid, r);
  }

  return r;
}

int OssAppendableWriter::switch_object_type_normal_to_appendable() {
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
      fs_->bg_vcpu_env_.bg_obj_store_env->get_obj_store_env_next();
  int r = background_env.executor->perform([&]() {
    auto obj_store = background_env.obj_store;

    // 1. Rename the remote file to the tmpfile.
    // TODO: add retry if oss return error FileAlreadyExists
    bool need_copy = inode_->attr.size > 0;
    int r = 0;
    std::string tmp_file_path;
    r = generate_hidden_object_name_fn(&tmp_file_path);
    if (r < 0) return r;

    if (need_copy) {
      r = obj_store->copy_object(upload_path_, tmp_file_path,
                                 false /*overwrite*/);
      if (r < 0) {
        LOG_ERROR("Failed to rename file: ` to `, nodeid: `, r: `",
                  upload_path_, tmp_file_path, inode_->nodeid, r);
        return r;
      }
    }

    // 2. Delete the old file.
    r = obj_store->delete_object(upload_path_);
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

      ssize_t ret = obj_store->get_object_range(tmp_file_path, bufv.iovec(),
                                                bufv.iovcnt(), i * part_size);
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

      ret = obj_store->append_object(upload_path_, &iov, 1, i * part_size,
                                     crc64_ptr);
      if (ret < 0) {
        LOG_ERROR("Failed to append upload file: `, nodeid: `, r: `",
                  upload_path_, inode_->nodeid, ret);
        return static_cast<int>(ret);
      }
    }

    // 4. Delete the tmpfile and ignore error.
    if (need_copy) {
      r = obj_store->delete_object(tmp_file_path.c_str());
      if (r < 0) {
        LOG_ERROR("Failed to delete tmpfile: `, nodeid: `, r: `", tmp_file_path,
                  inode_->nodeid, r);
      };
    }

    valid_buffer_offset = buffer_size_;
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

// Inode wlock held outside.
int OssRandomWriter::open() {
  if (!inode_->rw_ctx) {  // first open
    auto ctx = std::make_unique<RandomWriteContext>(
        fs_->options_.random_write_chunk_size);
    int r = create_staging(*ctx);
    if (r < 0) return r;
    ctx->upload_path = upload_path_;
    ctx->remote_size = inode_->attr.size;
    inode_->rw_ctx = ctx.release();
  } else {  // already opened
    // Refresh upload_path in case a rename left it stale (O_TRUNC dirties the
    // inode below, which would skip pwrite's phase-A refresh this cycle).
    if (!upload_path_.empty() && inode_->rw_ctx->upload_path != upload_path_) {
      LOG_INFO("open: nodeid ` reset upload_path from ` to `", inode_->nodeid,
               inode_->rw_ctx->upload_path, upload_path_);
      inode_->rw_ctx->upload_path = upload_path_;
    }

    fs_->resync_randwrite_remote_size(inode_);

    if (open_flags_ & O_TRUNC) {
      int64_t old_bytes = inode_->rw_ctx->staging_disk_bytes;
      DEFER(fs_->staging_disk_usage_update(old_bytes, refresh_staging_bytes()));
      if (::ftruncate(inode_->rw_ctx->staging_fd, 0) < 0) {
        int r = -errno;
        LOG_ERROR("ftruncate staging for O_TRUNC failed: nodeid `, r `",
                  inode_->nodeid, r);
        return r;
      }
      inode_->rw_ctx->chunks.clear();
    }
  }

  inode_->rw_ctx->ref_count++;
  if (open_flags_ & O_TRUNC) {
    inode_->rw_ctx->remote_size = 0;
    inode_->etag.clear();
    inode_->attr.size = 0;
    mark_dirty();
  } else if (open_flags_ & O_CREAT) {
    mark_dirty();
  }

  return 0;
}

int OssRandomWriter::create_staging(RandomWriteContext &ctx) {
  std::string tmpl = fs_->options_.temp_dir + "/.ossfs2_rw_" +
                     std::to_string(inode_->nodeid) + "_XXXXXX";
  auto fd = ::mkostemp(tmpl.data(), O_CLOEXEC);
  if (fd < 0) {
    int r = -errno;
    LOG_ERROR("mkstemp failed for staging file: `, errno: `", tmpl, r);
    return r;
  }
  if (::unlink(tmpl.c_str()) < 0) {
    int r = -errno;
    LOG_ERROR("unlink staging file failed: `, errno: `", tmpl, r);
    ::close(fd);
    return r;
  }

  ctx.staging_fd = fd;
  return 0;
}

// Inode wlock held outside.
ssize_t OssRandomWriter::pwrite(size_t count, off_t offset, const void *buf,
                                struct fuse_bufvec *bufv, std::string *wpath) {
  // O_APPEND writes must land at EOF; the kernel usually enforces this, but
  // defend here like the sequential writer does.
  if (open_flags_ & O_APPEND) {
    const off_t eof = static_cast<off_t>(inode_->attr.size);
    if (offset != eof) {
      LOG_WARN("O_APPEND offsets to EOF, file: `, nodeid: `, offset ` -> `",
               upload_path_, inode_->nodeid, offset, eof);
      offset = eof;
    }
  }

  const uint64_t max_file_size = fs_->options_.random_write_max_file_size;
  const uint64_t off_u = static_cast<uint64_t>(offset);
  if (off_u > max_file_size || count > max_file_size - off_u) {
    LOG_ERROR(
        "pwrite exceeds max file size `: file: ` nodeid `, offset `, count `",
        max_file_size, upload_path_, inode_->nodeid, offset, count);
    return -EFBIG;
  }

  const bool was_clean = !inode_->is_dirty;

  // ── Phase A: first dirty write of this cycle ──
  if (!inode_->is_dirty) {
    if (wpath == nullptr) return -E_READ_PATH_NEEDED;
    if (!wpath->empty() && *wpath != inode_->rw_ctx->upload_path) {
      LOG_INFO("file: nodeid: ` reset upload_path from ` to `", inode_->nodeid,
               inode_->rw_ctx->upload_path, *wpath);
      inode_->rw_ctx->upload_path = *wpath;
    }
    mark_dirty();
  }

  // Roll back if nothing was persisted: a stuck is_dirty misroutes later reads
  // through the random-write dirty path.
  ssize_t written = 0;
  DEFER({
    if (was_clean && written <= 0) {
      inode_->rw_ctx->chunks.clear();
      mark_clean();
    }
  });

  const uint64_t chunk_size = inode_->rw_ctx->chunks.chunk_size();
  const uint64_t end = off_u + count;  // exclusive end

  // ── Phase B: identify CLEAN chunks that need GET-on-write ──
  // 1. CLEAN + (chunk-start ≥ remote_size): hole region, no GET needed.
  // 2. CLEAN + whole cover: pwrite fully overwrites the chunk, no GET needed.
  // 3. Otherwise, download the chunk.

  // TODO: can be simplified. as only sid && eid should matter.
  std::vector<uint64_t> need_fetch;
  for (uint64_t cid = off_u / chunk_size; cid * chunk_size < end; ++cid) {
    if (inode_->rw_ctx->chunks.is_dirty(cid)) {
      continue;
    }
    uint64_t chunk_begin = cid * chunk_size;
    bool whole_cover =
        (off_u <= chunk_begin) && (end >= chunk_begin + chunk_size);
    if (!whole_cover && chunk_begin < inode_->rw_ctx->remote_size) {
      need_fetch.push_back(cid);
    }
  }

  // ── Phase C: fetch the chunks ──
  int r = fetch_chunks(need_fetch);
  if (r < 0) return r;

  // ── Phase D: write user data to staging ──
  // pwrite_fd below may overwrite downloaded range in staging file, but it's OK
  // to conservatively reserve another `count` bytes (usually small).
  r = reserve_randwrite_disk_budget(count);
  if (r < 0) return r;
  DEFER(release_randwrite_disk_budget(count));

  int64_t old_staging_bytes = inode_->rw_ctx->staging_disk_bytes;

  const int staging_fd = inode_->rw_ctx->staging_fd;
  if (buf) {
    written =
        pwrite_fd(staging_fd, static_cast<const char *>(buf), count, offset);
  } else {
    written = fuse_bufvec_to_fd(staging_fd, offset, count, bufv);
  }
  if (written < static_cast<ssize_t>(count)) {
    LOG_ERROR(
        "pwrite staging failed: `, nodeid: `, off: `, count: `, written: `",
        inode_->rw_ctx->upload_path, inode_->nodeid, offset, count, written);
    // Whole-covered CLEAN chunks skipped GET in phase B, so a partially
    // written one would serve/upload zeros; fail with -EIO instead of
    // marking dirty. Also triggers the DEFER rollback when clean.
    if (written >= 0) written = -EIO;
    return written;
  }

  // ── Phase E: mark dirty chunks and extend file size ──
  if (written > 0) {
    fs_->staging_disk_usage_update(old_staging_bytes, refresh_staging_bytes());
    uint64_t end_written = static_cast<uint64_t>(offset) + written;
    for (uint64_t cid = static_cast<uint64_t>(offset) / chunk_size;
         cid * chunk_size < end_written; ++cid) {
      inode_->rw_ctx->chunks.mark_dirty(cid);
    }
    if (end_written > inode_->attr.size) {
      inode_->attr.size = end_written;
    }
  }

  return written;
}

// Inode wlock + path rlock held outside.
int OssRandomWriter::flush() {
  if (!inode_->is_dirty) return 0;

  if (inode_->is_stale) {
    LOG_ERROR("aborting flush for unlinked file: nodeid `", inode_->nodeid);
    mark_clean();
    return 0;
  }

  uint64_t file_size = inode_->attr.size;
  int r = file_size <= fs_->random_write_base_part_size()
              ? flush_no_multipart(file_size)
              : flush_multipart(file_size);

  // Refill publishers already chained growth into usage, so this last link
  // off the CURRENT cache is normally a no-op; it heals any cache drift and
  // yields the authoritative staging_bytes for the truncate deduction below.
  int64_t pre_bytes = inode_->rw_ctx->staging_disk_bytes;
  int64_t staging_bytes = refresh_staging_bytes();
  fs_->staging_disk_usage_update(pre_bytes, staging_bytes);

  if (r < 0) {
    LOG_ERROR("flush failed: `, nodeid `, size `", r, inode_->nodeid,
              file_size);
    return r;
  }

  inode_->rw_ctx->chunks.clear();
  inode_->rw_ctx->remote_size = file_size;

  // Data is durable in OSS now, so the staging blocks are dead: reads route to
  // OSS once chunks are clean, and a later partial write re-fetches via
  // GET-on-write regardless of what staging still holds. Drop the blocks to
  // free the constrained temp-dir disk. A truncate failure only leaks space
  // (the pre-existing behavior), so warn and continue.
  if (staging_bytes > 0) {
    if (::ftruncate(inode_->rw_ctx->staging_fd, 0) < 0) {
      int r = -errno;
      LOG_WARN("release staging after flush failed: nodeid `, err `",
               inode_->nodeid, r);
    } else {
      fs_->staging_disk_usage_update(staging_bytes, 0);
      inode_->rw_ctx->staging_disk_bytes = 0;
    }
  }

  mark_clean();
  return 0;
}

bool OssRandomWriter::get_is_dirty() {
  return inode_->is_dirty;
}

void OssRandomWriter::mark_dirty() {
  if (inode_->is_dirty) return;
  inode_->is_dirty = true;
  fs_->add_dirty_nodeid(inode_->nodeid);
  // Drop the prefetched cache at the clean->dirty transition so blocks from
  // the previous clean state can never be served while the file is dirty.
  if (inode_->cache) {
    auto *handle = inode_->cache->get(inode_->rw_ctx->upload_path, inode_->etag,
                                      inode_->attr.size);
    if (handle) {
      LOG_INFO("file: `, nodeid: ` marked dirty, drop prefetched cache",
               inode_->rw_ctx->upload_path, inode_->nodeid);
      handle->drop(inode_->rw_ctx->upload_path, inode_->etag,
                   inode_->attr.size);
      inode_->cache->release(handle, 0);
    }
  }
}

void OssRandomWriter::mark_clean() {
  if (!inode_->is_dirty) return;
  inode_->is_dirty = false;
  inode_->invalidate_data_cache = true;
  inode_->attr_time = 0;
  fs_->erase_dirty_nodeid(inode_->nodeid);
  // Drop cache after flush so subsequent reads fetch the new OSS version.
  if (inode_->cache) {
    auto *handle = inode_->cache->get(inode_->rw_ctx->upload_path, inode_->etag,
                                      inode_->attr.size);
    if (handle) {
      handle->drop(inode_->rw_ctx->upload_path, inode_->etag,
                   inode_->attr.size);
      inode_->cache->release(handle, 0);
    }
  }
  LOG_INFO("file: `, nodeid: `, size: ` is marked to clean (random)",
           inode_->rw_ctx->upload_path, inode_->nodeid, inode_->attr.size);
}

// TODO: also use staging_disk_usage_ to limit the disk space usage for current
//       current ossfs2 process.
// Invoked every time before data are written to staging file.
int OssRandomWriter::reserve_randwrite_disk_budget(uint64_t bytes) {
  uint64_t reserved_after = fs_->staging_reserved_add(bytes);
  uint64_t avail = 0;
  int r = fs_->staging_disk_avail(inode_->rw_ctx->staging_fd, &avail);
  if (r < 0) {
    fs_->staging_reserved_sub(bytes);
    return r;
  }

  if (avail < reserved_after + fs_->options_.temp_dir_free_bytes) {
    // clang-format off
    LOG_ERROR(
        "Staging disk space insufficient. Disk avail: `, need free space ` + reserved ` bytes",
        avail, fs_->options_.temp_dir_free_bytes, reserved_after);
    // clang-format on
    fs_->staging_reserved_sub(bytes);
    return -ENOSPC;
  }
  return 0;
}

void OssRandomWriter::release_randwrite_disk_budget(uint64_t bytes) {
  fs_->staging_reserved_sub(bytes);
}

int64_t OssRandomWriter::refresh_staging_bytes() {
  int64_t cur = get_fd_disk_bytes(inode_->rw_ctx->staging_fd);
  if (cur >= 0) {
    inode_->rw_ctx->staging_disk_bytes = cur;
  }
  return cur;
}

// Only called from refill_clean_chunk(): publish the refilled chunk's
// growth right after download (the sole off-wlock staging mutation site).
// Each publisher chains its delta off the shared cache under the mutex.
void OssRandomWriter::publish_refill_clean_chunk_growth() {
  SCOPED_LOCK(refill_clean_chunk_growth_mtx_);
  int64_t old_bytes = inode_->rw_ctx->staging_disk_bytes;
  int64_t cur_bytes = refresh_staging_bytes();
  fs_->staging_disk_usage_update(old_bytes, cur_bytes);
}

// Called from OssFileHandle::close() under inode wlock, AFTER flush() ran.
// The last writer deletes the ctx so its destructor closes staging_fd.
void OssRandomWriter::close() {
  if (!inode_->rw_ctx) return;

  --inode_->rw_ctx->ref_count;
  if (inode_->rw_ctx->ref_count == 0) {
    // Release staging disk usage before ctx destruction closes the fd. The
    // cache is exact here: flush() ran under the same wlock and every
    // mutating site refreshes it.
    int64_t final_bytes = inode_->rw_ctx->staging_disk_bytes;
    DEFER({
      if (final_bytes > 0) {
        fs_->staging_disk_usage_update(final_bytes, 0);
      }
    });

    // If flush failed, yet the user still closes this last handle, the dirty
    // data is irrecoverably lost. Clean up inode state.
    if (inode_->is_dirty) {
      LOG_ERROR("last writer closing with unflushed data, data lost: nodeid `",
                inode_->nodeid);
      inode_->attr.size = inode_->rw_ctx->remote_size;
      inode_->rw_ctx->chunks.clear();
      mark_clean();
    }
    delete inode_->rw_ctx;
    inode_->rw_ctx = nullptr;
  }
}

// Inode wlock held outside.
int OssRandomWriter::truncate(uint64_t new_size) {
  auto *ctx = inode_->rw_ctx;
  RELEASE_ASSERT(ctx);

  // Refresh upload_path in case a rename since the last write left it stale.
  if (!upload_path_.empty() && ctx->upload_path != upload_path_) {
    LOG_INFO("truncate: nodeid ` reset upload_path from ` to `", inode_->nodeid,
             ctx->upload_path, upload_path_);
    ctx->upload_path = upload_path_;
  }

  int64_t old_bytes = ctx->staging_disk_bytes;
  DEFER(fs_->staging_disk_usage_update(old_bytes, refresh_staging_bytes()));
  // ftruncate grow is sparse (no blocks) and shrink frees blocks, so no disk
  // budget reservation is needed here; just reconcile the accounting.
  if (::ftruncate(ctx->staging_fd, static_cast<off_t>(new_size)) < 0) {
    int r = -errno;
    LOG_ERROR("truncate staging failed: nodeid `, new_size `, r `",
              inode_->nodeid, new_size, r);
    return r;
  }

  // A chunk straddling new_size keeps its dirty mark; its valid data
  // [chunk_begin, new_size) still lives in staging.
  ctx->chunks.erase_above_chunk(new_size);

  // Cap remote_size so pwrite's GET-on-write and flush's upload_part_copy never
  // read remote bytes beyond the new size.
  ctx->remote_size = std::min(ctx->remote_size, new_size);

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  inode_->etag.clear();
  inode_->update_attr(new_size, now);

  mark_dirty();
  return 0;
}

// TODO: spawn N concurrent photon threads (one per chunk).
int OssRandomWriter::fetch_chunks(const std::vector<uint64_t> &cids) {
  if (cids.empty()) return 0;

  const uint64_t chunk_size = inode_->rw_ctx->chunks.chunk_size();
  int64_t old_bytes = inode_->rw_ctx->staging_disk_bytes;
  DEFER(fs_->staging_disk_usage_update(old_bytes, refresh_staging_bytes()));

  for (uint64_t cid : cids) {
    uint64_t chunk_begin = cid * chunk_size;
    RELEASE_ASSERT(chunk_begin < inode_->rw_ctx->remote_size);
    uint64_t want =
        std::min(chunk_size, inode_->rw_ctx->remote_size - chunk_begin);

    // TODO: maybe reserve cids.size() * chunk_size before the loop (maybe
    // over reserve some space for the last chunk smaller than chunk_size)?
    int space_r = reserve_randwrite_disk_budget(want);
    if (space_r < 0) return space_r;
    DEFER(release_randwrite_disk_budget(want));

    ssize_t r = PERFORM_BACKGROUND_OBJ_REQUEST(
        fs_, get_object_range_to_fd, inode_->rw_ctx->upload_path,
        inode_->rw_ctx->staging_fd, static_cast<off_t>(chunk_begin),
        static_cast<off_t>(chunk_begin), want);
    FAULT_INJECTION(FI_RandomWrite_Get_Chunk_Fail, [&]() { r = -EIO; });
    if (r < 0) {
      // clang-format off
      LOG_ERROR(
          "fetch_chunks: get_object_range_to_fd failed, nodeid: `, cid: `, r: `",
          inode_->nodeid, cid, r);
      // clang-format on
      return static_cast<int>(r);
    }

    // Marked dirty in pwrite's phase E only after the write succeeds.
  }

  return 0;
}

// TODO: mark downloaded clean chunks as loaded to avoid re-fetching.
int OssRandomWriter::refill_clean_chunk(uint64_t pos, uint64_t chunk_end,
                                        IObjStore *obj_store) {
  auto *ctx = inode_->rw_ctx;
  uint64_t remote_end = std::min(chunk_end, ctx->remote_size);
  if (pos >= remote_end) {
    // Hole part. pread from staging also reads zeros, so do nothing here.
    return 0;
  }

  size_t count = static_cast<size_t>(remote_end - pos);

  int space_r = reserve_randwrite_disk_budget(count);
  if (space_r < 0) return space_r;
  DEFER(release_randwrite_disk_budget(count));

  ssize_t got = obj_store->get_object_range_to_fd(
      ctx->upload_path, ctx->staging_fd, static_cast<off_t>(pos),
      static_cast<off_t>(pos), count);
  publish_refill_clean_chunk_growth();
  if (got < 0) {
    LOG_ERROR("refill: get_object_range failed, nodeid `, off `, r `",
              inode_->nodeid, pos, got);
    return static_cast<int>(got);
  }

  return 0;
}

int OssRandomWriter::refill_range(uint64_t range_begin, uint64_t range_end,
                                  IObjStore *obj_store) {
  auto *ctx = inode_->rw_ctx;
  const uint64_t chunk_size = ctx->chunks.chunk_size();
  uint64_t pos = range_begin;
  while (pos < range_end) {
    uint64_t cid = pos / chunk_size;
    uint64_t chunk_end = std::min((cid + 1) * chunk_size, range_end);
    if (!ctx->chunks.is_dirty(cid)) {
      int r = refill_clean_chunk(pos, chunk_end, obj_store);
      if (r < 0) return r;
    }
    pos = chunk_end;
  }
  return 0;
}

// Single-shot PUT for files that fit in one multipart part.
int OssRandomWriter::flush_no_multipart(uint64_t file_size) {
  auto bg_ctx = fs_->bg_vcpu_env_.bg_obj_store_env->get_obj_store_env_next();
  std::string new_etag;
  int r = bg_ctx.executor->perform([&]() {
    if (file_size > 0) {
      int rr = refill_range(0, file_size, bg_ctx.obj_store);
      if (rr < 0) return rr;
    }
    uint64_t crc64 = 0;
    ssize_t put_r = bg_ctx.obj_store->put_object_from_fd(
        inode_->rw_ctx->upload_path, inode_->rw_ctx->staging_fd, 0, file_size,
        fs_->options_.enable_crc64 ? &crc64 : nullptr, &new_etag);
    return static_cast<int>(put_r);
  });

  if (r < 0) {
    LOG_ERROR("flush_no_multipart: failed, nodeid `, size `, r `",
              inode_->nodeid, file_size, r);
  } else if (!new_etag.empty()) {
    inode_->etag = new_etag;
  }
  return r;
}

int OssRandomWriter::flush_multipart(uint64_t file_size) {
  const uint64_t chunk_size = inode_->rw_ctx->chunks.chunk_size();
  const uint64_t base_part_size = fs_->random_write_base_part_size();
  const uint64_t part_size =
      calc_random_write_part_size(file_size, base_part_size, chunk_size);
  if (part_size == 0) {
    LOG_ERROR("flush_multipart: file too large, nodeid `, size `",
              inode_->nodeid, file_size);
    return -EFBIG;
  }
  uint64_t num_parts = (file_size + part_size - 1) / part_size;
  if (part_size > base_part_size) {
    LOG_INFO(
        "flush_multipart: part size enlarged, nodeid `, size `, part_size `",
        inode_->nodeid, file_size, part_size);
  }

  void *upload_ctx = nullptr;
  int r = 0;
  FAULT_INJECTION(FI_RandomWrite_Init_Multipart_Fail, [&]() { r = -EIO; });
  if (r == 0) {
    r = PERFORM_BACKGROUND_OBJ_REQUEST(
        fs_, init_multipart_upload, inode_->rw_ctx->upload_path, &upload_ctx);
  }
  if (r < 0) {
    LOG_ERROR("flush_multipart: init failed, nodeid `, r `", inode_->nodeid, r);
    return r;
  }

  upload_ret_.store(0, std::memory_order_release);
  RELEASE_ASSERT(running_upload_tasks_.load() == 0);

  bool has_crc = fs_->options_.enable_crc64;
  std::vector<uint64_t> part_crcs(has_crc ? num_parts : 0, 0);
  uint64_t whole_crc = 0;
  crc_incomplete_.store(false, std::memory_order_relaxed);

  for (uint64_t i = 0; i < num_parts; ++i) {
    if (upload_ret_.load(std::memory_order_acquire) != 0) break;

    fs_->upload_sem_->wait(1);
    running_upload_tasks_.fetch_add(1);

    auto ctx = new MultipartContext;
    ctx->writer = this;
    ctx->upload_ctx = upload_ctx;
    ctx->part_begin = i * part_size;
    ctx->part_end = std::min(ctx->part_begin + part_size, file_size);
    ctx->part_number = static_cast<int>(i + 1);
    ctx->part_crcs = has_crc ? part_crcs.data() : nullptr;

    auto th = photon::thread_create(do_upload_or_copy_part, ctx);
    photon::thread_migrate(th,
                           fs_->bg_vcpu_env_.bg_obj_store_env->get_vcpu_next());
  }

  waiting_upload_tasks();
  r = upload_ret_.load(std::memory_order_acquire);
  std::string new_etag;
  if (r < 0) goto cleanup;

  if (has_crc && !crc_incomplete_.load(std::memory_order_acquire)) {
    whole_crc = part_crcs[0];
    for (uint64_t i = 1; i < num_parts; ++i) {
      uint64_t plen = std::min(part_size, file_size - i * part_size);
      whole_crc = crc64ecma_combine(whole_crc, part_crcs[i], plen);
    }
  }

  FAULT_INJECTION(FI_RandomWrite_Complete_Multipart_Fail, [&]() { r = -EIO; });
  if (r == 0) {
    r = PERFORM_BACKGROUND_OBJ_REQUEST(
        fs_, complete_multipart_upload, upload_ctx,
        has_crc && !crc_incomplete_.load(std::memory_order_acquire) ? &whole_crc
                                                                    : nullptr,
        &new_etag);
  }
  if (r < 0) {
    LOG_ERROR("complete failed, nodeid `, r `", inode_->nodeid, r);
    goto cleanup;
  }
  if (!new_etag.empty()) {
    inode_->etag = new_etag;
  }
  return 0;

cleanup:
  int abort_r =
      PERFORM_BACKGROUND_OBJ_REQUEST(fs_, abort_multipart_upload, upload_ctx);
  if (abort_r < 0) {
    LOG_ERROR("abort failed, nodeid `, abort_r `", inode_->nodeid, abort_r);
  }
  return r;
}

void OssRandomWriter::set_upload_error(int err) {
  int expected = 0;
  upload_ret_.compare_exchange_strong(expected, err, std::memory_order_acq_rel,
                                      std::memory_order_acquire);
}

void *OssRandomWriter::do_upload_or_copy_part(void *args) {
  auto multipart_ctx = static_cast<MultipartContext *>(args);
  auto writer = multipart_ctx->writer;
  thread_local auto obj_store =
      writer->fs_->bg_vcpu_env_.bg_obj_store_env->get_obj_store();

  int r = 0;
  size_t part_len =
      static_cast<size_t>(multipart_ctx->part_end - multipart_ctx->part_begin);
  auto *rw_ctx = writer->inode_->rw_ctx;

  if (multipart_ctx->part_end <= rw_ctx->remote_size &&
      !rw_ctx->chunks.is_range_dirty(multipart_ctx->part_begin,
                                     multipart_ctx->part_end)) {
    // TODO: ~0ULL sentinel could theoretically collide with a valid CRC64.
    // Consider adding a bool *crc_obtained output param to upload_part_copy.
    uint64_t copy_crc =
        ~0ULL;  // sentinel: photon leaves untouched if no header
    r = obj_store->upload_part_copy(
        multipart_ctx->upload_ctx,
        static_cast<off_t>(multipart_ctx->part_begin), part_len,
        multipart_ctx->part_number,
        multipart_ctx->part_crcs ? &copy_crc : nullptr);
    if (r < 0) {
      LOG_ERROR("parallel upload_part_copy failed, nodeid `, part `, r `",
                writer->inode_->nodeid, multipart_ctx->part_number, r);
      writer->set_upload_error(r);
    } else if (multipart_ctx->part_crcs) {
      if (copy_crc == ~0ULL) {
        LOG_WARN("copy part missing crc64 header, nodeid `, part `",
                 writer->inode_->nodeid, multipart_ctx->part_number);
        writer->crc_incomplete_.store(true, std::memory_order_release);
      } else {
        multipart_ctx->part_crcs[multipart_ctx->part_number - 1] = copy_crc;
      }
    }
  } else {
    r = writer->refill_range(multipart_ctx->part_begin, multipart_ctx->part_end,
                             obj_store);
    if (r < 0) {
      LOG_ERROR("fill sparse range failed, nodeid `, part `, r `",
                writer->inode_->nodeid, multipart_ctx->part_number, r);
      writer->set_upload_error(r);
    } else {
      uint64_t part_crc = 0;
      ssize_t up = obj_store->upload_part_from_fd(
          multipart_ctx->upload_ctx, rw_ctx->staging_fd,
          static_cast<off_t>(multipart_ctx->part_begin), part_len,
          multipart_ctx->part_number,
          multipart_ctx->part_crcs ? &part_crc : nullptr);
      if (up < 0) {
        LOG_ERROR("parallel upload_part(fd) failed, nodeid `, part `, r `",
                  writer->inode_->nodeid, multipart_ctx->part_number, up);
        writer->set_upload_error(static_cast<int>(up));
      } else if (multipart_ctx->part_crcs) {
        multipart_ctx->part_crcs[multipart_ctx->part_number - 1] = part_crc;
      }
    }
  }

  writer->fs_->upload_sem_->signal(1);
  writer->running_upload_tasks_.fetch_sub(1);
  delete multipart_ctx;

  return nullptr;
}

std::unique_ptr<IWriter> create_oss_writer(OssFs *fs, std::string_view path,
                                           FileInode *inode, int flags,
                                           bool is_dirty) {
  switch (fs->write_mode()) {
    case WriteMode::Random:
      return std::make_unique<OssRandomWriter>(fs, path, inode, flags);
    case WriteMode::Appendable:
      return std::make_unique<OssAppendableWriter>(fs, path, inode, flags);
    case WriteMode::Streaming:
      return std::make_unique<OssStreamingWriter>(fs, path, inode, flags);
  }
  return nullptr;
}

}  // namespace OssFileSystem
