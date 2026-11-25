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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include "common/logger.h"
#include "common/utils.h"

namespace {

// Foreign owner for attacker-state tests; chown requires root.
constexpr uid_t kNobodyUid = 65534;

std::string case_dir() {
  auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
  return std::filesystem::temp_directory_path() / "SecureLogDirTest" /
         info->name();
}

mode_t mode_of(const std::string &p) {
  struct stat st;
  EXPECT_EQ(lstat(p.c_str(), &st), 0) << p;
  return st.st_mode & 07777;
}

uid_t owner_of(const std::string &p) {
  struct stat st;
  EXPECT_EQ(lstat(p.c_str(), &st), 0) << p;
  return st.st_uid;
}

std::string content_of(const std::string &p) {
  std::ifstream f(p);
  return std::string((std::istreambuf_iterator<char>(f)),
                     std::istreambuf_iterator<char>());
}

void write_file(const std::string &p, const std::string &data) {
  std::ofstream f(p);
  f << data;
}

class SecureLogDirTest : public ::testing::Test {
 protected:
  std::string tmp_;
  mode_t saved_umask_ = 0;

  void SetUp() override {
    // Force exact mkdir modes so the assertions below are umask-independent.
    saved_umask_ = umask(0);
    tmp_ = case_dir();
    std::error_code ec;
    std::filesystem::remove_all(tmp_, ec);
    std::filesystem::create_directories(tmp_, ec);
    ASSERT_FALSE(ec) << ec.message();
    // Pin fixture dirs to 0755: ancestors refuses 0777 without sticky.
    ASSERT_EQ(chmod(std::filesystem::path(tmp_).parent_path().c_str(), 0755),
              0);
    ASSERT_EQ(chmod(tmp_.c_str(), 0755), 0);
  }

  void TearDown() override {
    umask(saved_umask_);
    // Never leave the global logger pointing at a test file.
    set_ossfs_log_to_stdout(ALOG_INFO);
    std::error_code ec;
    std::filesystem::remove_all(tmp_, ec);
  }

  bool is_root() const {
    return geteuid() == 0;
  }
};

// ---------------------------------------------------------------------------
// secure_shared_log_dir
// ---------------------------------------------------------------------------

TEST_F(SecureLogDirTest, SharedCreateMissing) {
  std::string def = tmp_ + "/shared_def";
  std::string custom = tmp_ + "/shared_custom";

  EXPECT_TRUE(secure_shared_log_dir(def, true));
  EXPECT_EQ(mode_of(def), 01777u);
  EXPECT_EQ(owner_of(def), geteuid());

  // A custom path keeps least privilege: created 0755, not world-writable.
  EXPECT_TRUE(secure_shared_log_dir(custom, false));
  EXPECT_EQ(mode_of(custom), 0755u);
}

TEST_F(SecureLogDirTest, SharedAddsStickyToLegacy0777) {
  std::string dir = tmp_ + "/legacy";
  ASSERT_EQ(mkdir(dir.c_str(), 0777), 0);
  // Simulate a pre-existing entry that must not be touched.
  write_file(dir + "/other_user_log", "data");
  ASSERT_EQ(chmod(dir.c_str(), 0777), 0);

  EXPECT_TRUE(secure_shared_log_dir(dir, true));
  EXPECT_EQ(mode_of(dir), 01777u);
  EXPECT_EQ(content_of(dir + "/other_user_log"), "data");
}

TEST_F(SecureLogDirTest, SharedCustomKeepsModeAddsStickyWhenWritable) {
  // A read-only custom dir keeps its least-privilege mode (no widening).
  std::string ro = tmp_ + "/custom_ro";
  ASSERT_EQ(mkdir(ro.c_str(), 0755), 0);
  EXPECT_TRUE(secure_shared_log_dir(ro, false));
  EXPECT_EQ(mode_of(ro), 0755u);

  // An already world-writable custom dir just gains the sticky bit.
  std::string wide = tmp_ + "/custom_wide";
  ASSERT_EQ(mkdir(wide.c_str(), 0777), 0);
  EXPECT_TRUE(secure_shared_log_dir(wide, false));
  EXPECT_EQ(mode_of(wide), 01777u);
}

TEST_F(SecureLogDirTest, SharedRejectsSymlinkAndFile) {
  std::string target = tmp_ + "/target";
  ASSERT_EQ(mkdir(target.c_str(), 0755), 0);
  std::string link = tmp_ + "/link";
  ASSERT_EQ(symlink(target.c_str(), link.c_str()), 0);
  EXPECT_FALSE(secure_shared_log_dir(link, true));
  // The symlink target must not be modified.
  EXPECT_EQ(mode_of(target), 0755u);

  std::string file = tmp_ + "/afile";
  write_file(file, "x");
  EXPECT_FALSE(secure_shared_log_dir(file, true));
}

TEST_F(SecureLogDirTest, SharedRejectsAncestorSymlinkBeforeMutating) {
  std::string target = tmp_ + "/target";
  ASSERT_EQ(mkdir(target.c_str(), 0755), 0);
  // <tmp>/link -> target; the shared dir would be <tmp>/link/ossfs2.
  std::string link = tmp_ + "/link";
  ASSERT_EQ(symlink(target.c_str(), link.c_str()), 0);
  EXPECT_FALSE(secure_shared_log_dir(link + "/ossfs2", true));
  // Ancestors are vetted first: nothing is created or chmod'ed in the target.
  EXPECT_FALSE(std::filesystem::exists(target + "/ossfs2"));
  EXPECT_EQ(mode_of(target), 0755u);
}

TEST_F(SecureLogDirTest, SharedRebuildsMissingAncestors) {
  // Runtime deletion of a multi-level custom tree must be recoverable.
  std::string dir = tmp_ + "/a/b/shared";
  EXPECT_TRUE(secure_shared_log_dir(dir, false));
  EXPECT_EQ(mode_of(tmp_ + "/a"), 0755u);
  EXPECT_EQ(mode_of(tmp_ + "/a/b"), 0755u);
  EXPECT_EQ(mode_of(dir), 0755u);
}

TEST_F(SecureLogDirTest, MountChainNeverPreCreatesThroughAncestorSymlink) {
  std::string target = tmp_ + "/target";
  ASSERT_EQ(mkdir(target.c_str(), 0755), 0);
  std::string link = tmp_ + "/link";
  ASSERT_EQ(symlink(target.c_str(), link.c_str()), 0);
  std::string log = link + "/ossfs2";
  // validate_dir_params only checks conflicts; creation is left to the helpers.
  EXPECT_TRUE(validate_dir_params(log, "", "", tmp_ + "/mp"));
  EXPECT_FALSE(std::filesystem::exists(target + "/ossfs2"));
  EXPECT_FALSE(secure_shared_log_dir(log, false));
  EXPECT_FALSE(std::filesystem::exists(target + "/ossfs2"));
  EXPECT_EQ(mode_of(target), 0755u);
}

TEST_F(SecureLogDirTest, SharedForeignOwner) {
  if (!is_root()) GTEST_SKIP() << "requires root to chown";

  std::string def = tmp_ + "/hijacked_def";
  ASSERT_EQ(mkdir(def.c_str(), 0755), 0);
  ASSERT_EQ(chown(def.c_str(), kNobodyUid, kNobodyUid), 0);
  // Default path: root takes the dir over in place.
  EXPECT_TRUE(secure_shared_log_dir(def, true));
  EXPECT_EQ(owner_of(def), 0u);
  EXPECT_EQ(mode_of(def), 01777u);

  std::string custom = tmp_ + "/hijacked_custom";
  ASSERT_EQ(mkdir(custom.c_str(), 0755), 0);
  ASSERT_EQ(chown(custom.c_str(), kNobodyUid, kNobodyUid), 0);
  // Custom path: refuse, never chown an admin-owned dir automatically.
  EXPECT_FALSE(secure_shared_log_dir(custom, false));
  EXPECT_EQ(owner_of(custom), kNobodyUid);
}

// ---------------------------------------------------------------------------
// secure_private_log_dir
// ---------------------------------------------------------------------------

TEST_F(SecureLogDirTest, PrivateCreateMissing0755) {
  std::string dir = tmp_ + "/user";
  EXPECT_TRUE(secure_private_log_dir(dir));
  EXPECT_EQ(mode_of(dir), 0755u);
  EXPECT_EQ(owner_of(dir), geteuid());
}

TEST_F(SecureLogDirTest, PrivateCreatesMissingAncestors) {
  // Missing ancestors are created 0755 and re-validated after EEXIST.
  std::string deep = tmp_ + "/a/b/c/user";
  EXPECT_TRUE(secure_private_log_dir(deep));
  EXPECT_EQ(mode_of(tmp_ + "/a"), 0755u);
  EXPECT_EQ(mode_of(tmp_ + "/a/b"), 0755u);
  EXPECT_EQ(mode_of(tmp_ + "/a/b/c"), 0755u);
  EXPECT_EQ(mode_of(deep), 0755u);
  EXPECT_EQ(owner_of(deep), geteuid());
}

TEST_F(SecureLogDirTest, PrivateStripsGroupOtherWrite) {
  std::string dir = tmp_ + "/user";
  ASSERT_EQ(mkdir(dir.c_str(), 0777), 0);
  EXPECT_TRUE(secure_private_log_dir(dir));
  EXPECT_EQ(mode_of(dir), 0755u);
}

TEST_F(SecureLogDirTest, PrivateKeeps0755ReadOnly) {
  std::string dir = tmp_ + "/user";
  ASSERT_EQ(mkdir(dir.c_str(), 0755), 0);
  EXPECT_TRUE(secure_private_log_dir(dir));
  EXPECT_EQ(mode_of(dir), 0755u);
}

TEST_F(SecureLogDirTest, PrivateRestoresOwnerWriteTraverse) {
  // 0500/0400 would pass an owner-only check yet block log file creation.
  std::string dir = tmp_ + "/user";
  ASSERT_EQ(mkdir(dir.c_str(), 0500), 0);
  EXPECT_TRUE(secure_private_log_dir(dir));
  EXPECT_EQ(mode_of(dir), 0755u);

  std::string dir2 = tmp_ + "/user2";
  ASSERT_EQ(mkdir(dir2.c_str(), 0400), 0);
  EXPECT_TRUE(secure_private_log_dir(dir2));
  EXPECT_EQ(mode_of(dir2), 0755u);
}

TEST_F(SecureLogDirTest, PrivateRejectsSymlinkAndFile) {
  std::string target = tmp_ + "/etc";
  ASSERT_EQ(mkdir(target.c_str(), 0755), 0);
  std::string link = tmp_ + "/user_link";
  ASSERT_EQ(symlink(target.c_str(), link.c_str()), 0);
  EXPECT_FALSE(secure_private_log_dir(link));

  std::string file = tmp_ + "/user_file";
  write_file(file, "x");
  EXPECT_FALSE(secure_private_log_dir(file));
}

TEST_F(SecureLogDirTest, PrivateRejectsHostileAncestorSymlink) {
  std::string target = tmp_ + "/etc";
  ASSERT_EQ(mkdir(target.c_str(), 0755), 0);
  // <tmp>/log/ossfs2 -> etc, private dir would be <tmp>/log/ossfs2/root.
  ASSERT_EQ(std::filesystem::create_directories(tmp_ + "/log"), true);
  // Keep the ancestor 0755 so the symlink is what gets rejected.
  ASSERT_EQ(chmod((tmp_ + "/log").c_str(), 0755), 0);
  ASSERT_EQ(symlink(target.c_str(), (tmp_ + "/log/ossfs2").c_str()), 0);
  EXPECT_FALSE(secure_private_log_dir(tmp_ + "/log/ossfs2/root"));
  // Nothing may be created inside the symlink target.
  EXPECT_FALSE(std::filesystem::exists(target + "/root"));
}

TEST_F(SecureLogDirTest, PrivateForeignOwner) {
  if (!is_root()) GTEST_SKIP() << "requires root to chown";

  std::string dir = tmp_ + "/root_hijack";
  ASSERT_EQ(mkdir(dir.c_str(), 0755), 0);
  ASSERT_EQ(chown(dir.c_str(), kNobodyUid, kNobodyUid), 0);
  // The root track rejects a pre-occupied private dir and preserves it.
  EXPECT_FALSE(secure_private_log_dir(dir));
  EXPECT_EQ(owner_of(dir), kNobodyUid);
  EXPECT_EQ(mode_of(dir), 0755u);
}

TEST_F(SecureLogDirTest, PrivateForeignAncestorWritableByOthers) {
  if (!is_root()) GTEST_SKIP() << "requires root to chown";

  // A foreign-owned writable ancestor can be swapped; root must refuse.
  ASSERT_EQ(std::filesystem::create_directories(tmp_ + "/shared"), true);
  ASSERT_EQ(chown((tmp_ + "/shared").c_str(), kNobodyUid, kNobodyUid), 0);
  ASSERT_EQ(chmod((tmp_ + "/shared").c_str(), 0777), 0);
  EXPECT_FALSE(secure_private_log_dir(tmp_ + "/shared/user"));
}

TEST_F(SecureLogDirTest, PrivateRejectsWorldWritableAncestorWithoutSticky) {
  if (!is_root()) GTEST_SKIP() << "the sticky-bit check is root-only";

  // Root refuses a 0777 ancestor without sticky and never chmods it.
  ASSERT_EQ(std::filesystem::create_directories(tmp_ + "/wide"), true);
  ASSERT_EQ(chmod((tmp_ + "/wide").c_str(), 0777), 0);
  EXPECT_FALSE(secure_private_log_dir(tmp_ + "/wide/user"));
  EXPECT_EQ(mode_of(tmp_ + "/wide"), 0777u);
}

TEST_F(SecureLogDirTest, PrivateAllowsStickyWorldWritableAncestor) {
  // /tmp model: sticky makes a world-writable ancestor safe.
  ASSERT_EQ(std::filesystem::create_directories(tmp_ + "/sticky"), true);
  ASSERT_EQ(chmod((tmp_ + "/sticky").c_str(), 01777), 0);
  EXPECT_TRUE(secure_private_log_dir(tmp_ + "/sticky/user"));
  EXPECT_EQ(mode_of(tmp_ + "/sticky/user"), 0755u);
}

// ---------------------------------------------------------------------------
// set_ossfs_log_setting: secure open (O_NOFOLLOW + fstat)
// ---------------------------------------------------------------------------

TEST_F(SecureLogDirTest, LogSettingRejectsSymlinkedLogFile) {
  // Equivalent of the reported PoC: <logdir>/root/ossfs2.log -> target.
  std::string target = tmp_ + "/ld.so.preload";
  write_file(target, "existing\n");
  std::string dir = tmp_ + "/root";
  ASSERT_EQ(mkdir(dir.c_str(), 0700), 0);
  std::string log = dir + "/ossfs2.log";
  ASSERT_EQ(symlink(target.c_str(), log.c_str()), 0);

  EXPECT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3), -1);
  // The symlink target must not have received a single byte.
  EXPECT_EQ(content_of(target), "existing\n");
}

TEST_F(SecureLogDirTest, LogSettingRejectsForeignOwnedLogFile) {
  if (!is_root()) GTEST_SKIP() << "requires root to chown";

  std::string dir = tmp_ + "/root";
  ASSERT_EQ(mkdir(dir.c_str(), 0700), 0);
  std::string log = dir + "/ossfs2.log";
  write_file(log, "");
  ASSERT_EQ(chown(log.c_str(), kNobodyUid, kNobodyUid), 0);

  EXPECT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3), -1);
  EXPECT_TRUE(content_of(log).empty());
}

TEST_F(SecureLogDirTest, LogSettingRejectsFifoWithoutBlocking) {
  std::string dir = tmp_ + "/root";
  ASSERT_EQ(mkdir(dir.c_str(), 0700), 0);
  std::string log = dir + "/ossfs2.log";
  ASSERT_EQ(mkfifo(log.c_str(), 0600), 0);
  // Without O_NONBLOCK this open() would hang on the reader-less FIFO.
  EXPECT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3), -1);
}

TEST_F(SecureLogDirTest, LogSettingAcceptsOwnRegularFile) {
  std::string dir = tmp_ + "/root";
  ASSERT_TRUE(secure_private_log_dir(dir));
  std::string log = dir + "/ossfs2.log";

  ASSERT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3), 0);
  LOG_INFO("secure log dir test marker");
  set_ossfs_log_to_stdout(ALOG_INFO);
  EXPECT_NE(content_of(log).find("secure log dir test marker"),
            std::string::npos);
}

TEST_F(SecureLogDirTest, LogSettingRuntimeSwapIsRejectedOnReopen) {
  if (!is_root()) GTEST_SKIP() << "requires root to chown";

  // A chowned file models a runtime swap (rename is dir-owner-only).
  std::string dir = tmp_ + "/root";
  ASSERT_TRUE(secure_private_log_dir(dir));
  std::string log = dir + "/ossfs2.log";
  ASSERT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3), 0);
  set_ossfs_log_to_stdout(ALOG_INFO);

  ASSERT_EQ(chown(log.c_str(), kNobodyUid, kNobodyUid), 0);
  EXPECT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3), -1);
}

TEST_F(SecureLogDirTest, LogSettingRecreatesDeletedDirTreeOnRotate) {
  // Multi-level custom tree: shared=<tmp>/a/b, private=<tmp>/a/b/user.
  std::string shared = tmp_ + "/a/b";
  std::string dir = shared + "/user";
  ASSERT_TRUE(secure_shared_log_dir(shared, false));
  ASSERT_TRUE(secure_private_log_dir(dir));
  std::string log = dir + "/ossfs2.log";
  ASSERT_EQ(set_ossfs_log_setting(log.c_str(), 1024 * 1024, 3, -1UL, ALOG_INFO,
                                  false),
            0);

  // The open fd survives deletion; recovery happens on rotate-triggered reopen.
  std::error_code ec;
  std::filesystem::remove_all(tmp_ + "/a", ec);
  ASSERT_FALSE(ec);

  // Exceed the 1 MiB rotate limit to force log_file_rotate + reopen.
  for (int i = 0; i < 9000; i++)
    LOG_INFO(
        "recreate marker padding ........................................");
  set_ossfs_log_to_stdout(ALOG_INFO);

  EXPECT_TRUE(std::filesystem::exists(log));
  EXPECT_EQ(mode_of(tmp_ + "/a"), 0755u);
  EXPECT_EQ(mode_of(shared), 0755u);
  EXPECT_EQ(mode_of(dir), 0755u);
  EXPECT_NE(content_of(log).find("recreate marker"), std::string::npos);
}

}  // namespace
