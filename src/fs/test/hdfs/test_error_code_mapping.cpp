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

#include <jdo_error.h>

#include "fs/test/test_suite.h"
#include "oss/oss_hdfs_store.h"

class Ossfs2HdfsErrorCodeMappingTest : public OssHdfsTestSuite {};

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_success) {
  ASSERT_EQ(jdo_error_code_to_posix(0), 0);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_EOF_ERROR), 0);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_client_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_ILLEGAL_CONF_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_ILLEGAL_REQUEST_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_CHECKSUM_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_JNI_OBJ_ALLOCATION_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CONFIG_OPTION_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_NO_ENOUGHT_RESERVE_MEM_ERROR),
            -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_NO_SPACE_RESOURCE_ERROR),
            -ENOSPC);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CLIENT_NO_MEM_RESOURCE_ERROR), -ENOMEM);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CACHESET_NOT_AVAILABLE_ERROR), -EIO);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_network_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SERVER_ERROR), -ECONNREFUSED);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SERVER_UNAVAILABLE_ERROR),
            -ECONNREFUSED);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SERVER_NO_IN_SERVICE_ERROR),
            -ECONNREFUSED);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SERVER_TIMEOUT_ERROR), -ETIMEDOUT);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_file_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_FILE_NOT_FOUND_ERROR), -ENOENT);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_FILE_SIZE_EXCEEDED_ERROR), -EFBIG);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_FILE_NAME_EXCEEDED_ERROR),
            -ENAMETOOLONG);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_QUOTA_EXCEEDED_ERROR), -EDQUOT);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_resource_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RESOURCE_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_IO_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CORRUPT_DATA_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SERVER_INTERNAL_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_NO_SERVER_ERROR), -EHOSTUNREACH);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SERVER_RESPONSE_TOO_LARGE_ERROR),
            -EFBIG);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_system_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SYSTEM_LOGIC_ERROR), -ENOTSUP);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_NOT_SUPPORTED_ERROR), -ENOTSUP);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_rename_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_DST_UNDER_SRC_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_DST_PARENT_NOT_FOUND_ERROR),
            -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_DST_PARENT_FILE_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_SRC_NOT_FOUND_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_DST_EXIST_AS_FILE_ERROR),
            -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_SRC_EQ_DST_AS_DIRECTORY_ERROR),
            -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_SRC_EQ_DST_AS_FILE_ERROR),
            -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_DST_EXIST_ERROR), -EINVAL);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_permission_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_DELETE_NOT_ALLOW_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_NO_PERMISSION_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_DELETE_DIRECTORY_NOT_EMPTY_ERROR),
            -ENOTEMPTY);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_PARENT_NOT_DIRECTORY_ERROR), -ENOTDIR);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_FILE_ALREADY_EXISTS_ERROR), -EEXIST);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_FILE_ALREADY_BEING_CREATED_ERROR),
            -EEXIST);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_INVALID_PATH_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_FILE_TYPE_IS_DIRECTORY_ERROR), -EISDIR);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_INVALID_ARGUMENT_ERROR), -EINVAL);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_lock_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_LEASE_EXPIRED_ERROR), -EAGAIN);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CONFLICT_LOCK_ERROR), -EAGAIN);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SAFE_MODE_ERROR), -EROFS);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_xattr_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_XATTR_ERROR), -ENODATA);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_XATTR_NOT_EXIST_ERROR), -ENODATA);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_XATTR_SET_ERROR), -ENODATA);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_atomic_rename_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_LOCKED_BY_OTHERS_ERROR),
            -EAGAIN);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_OTS_OP_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_OTS_CONDITIONAL_UPDATE_ERROR),
            -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_OTS_OBJECT_NOT_EXIST_ERROR),
            -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_OTS_AUTH_FAILED_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_OTS_SERVER_BUSY_ERROR), -EIO);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest,
       verify_error_code_security_and_http_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SECURITY_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_AUTHENTICATION_FAILED_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_ACCESS_CONTROL_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_ACL_DENY_ERROR), -EACCES);

  // Object HTTP errors.
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_400_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_403_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_404_ERROR), -ENOENT);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_405_ERROR), -ENOTSUP);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_409_ERROR), -EEXIST);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_411_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_412_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_416_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_424_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_500_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_502_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_REST_HTTP_503_ERROR), -EIO);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_symlink_errors) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_NOT_DLS_BUCKET_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_STANDBY_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_RENAME_ACROSS_STORES_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CONCAT_ACROSS_STORES_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_UNRESOLVED_LINK_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SYMLINK_LOOP_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SYMLINK_ACROSS_STORES_ERROR), -EINVAL);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_tiered_and_timeout) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_TIERED_OP_NOT_ALLOWED_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_LOCAL_WRITE_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_LOCAL_READ_ERROR), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_READ_TASK_TIMEOUT_ERROR), -ETIMEDOUT);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_WRITE_TASK_TIMEOUT_ERROR), -ETIMEDOUT);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_PATH_NOT_EMPTY_DIRECTORY_ERROR),
            -ENOTEMPTY);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SNAPSHOT_ERROR), -EIO);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest,
       verify_error_code_mount_and_access_policy) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CONFLICT_MOUNT_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_NO_SUCH_MOUNT_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_ROOT_POLICY_ALREADY_EXIST_ERROR),
            -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_ROOT_POLICY_NOT_EXIST_ERROR), -EINVAL);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_TOO_MANY_ACCESS_POLICIES_ERROR),
            -EINVAL);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_sasl_and_crypto) {
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SASL_CLIENT_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_SASL_SERVER_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_KERBEROS_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CRYPTO_POLICY_ALREADY_EXIST_ERROR),
            -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CRYPTO_POLICY_DIR_NOT_EMPTY_ERROR),
            -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CRYPTO_KEY_NOT_EXIST_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_ENCRYPTED_KEY_ERROR), -EACCES);
  ASSERT_EQ(jdo_error_code_to_posix(JDO_CRYPTO_POLICY_DISABLED_ERROR), -EACCES);
}

TEST_F(Ossfs2HdfsErrorCodeMappingTest, verify_error_code_unknown) {
  // Unknown error code -> -EIO (default branch).
  ASSERT_EQ(jdo_error_code_to_posix(99999), -EIO);
  ASSERT_EQ(jdo_error_code_to_posix(-1), -EIO);
}
