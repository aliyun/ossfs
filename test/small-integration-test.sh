#!/bin/bash
#
# ossfs - FUSE-based file system backed by Alibaba Cloud OSS
#
# Copyright 2007-2008 Randy Rizun <rrizun@gmail.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

#
# Test ossfs file system operations with
#

set -o errexit
set -o pipefail

source integration-test-common.sh

CACHE_DIR="/tmp/ossfs-cache"
LOGFILE="/root/ossfs.log"
CHECK_CACHE_FILE="/tmp/ossfs-cache-check.log"
rm -rf "${CACHE_DIR}"
mkdir "${CACHE_DIR}"

source test-utils.sh

#reserve 200MB for data cache
FAKE_FREE_DISK_SIZE=200
ENSURE_DISKFREE_SIZE=10
BACKWARD_CHUNKS=1
DIRECT_READ_LOCAL_FILE_CACHE_SIZE_MB=2048
AHBE_CONFIG="./sample_ahbe.conf"

export TEST_NUMBER=1
export CACHE_DIR
export LOGFILE
export CHECK_CACHE_FILE
export ENSURE_DISKFREE_SIZE 
export DIRECT_READ_LOCAL_FILE_CACHE_SIZE_MB
if [ -n "${ALL_TESTS}" ]; then
    FLAGS=(
        "use_cache=${CACHE_DIR} -o ensure_diskfree=${ENSURE_DISKFREE_SIZE} -o fake_diskfree=${FAKE_FREE_DISK_SIZE} -o del_cache"
        enable_content_md5
        disable_noobj_cache
        "max_stat_cache_size=100 -o stat_cache_expire=-1"
        nocopyapi
        nomultipart
        notsup_compat_dir
        sigv1
        "singlepart_copy_limit=10 -o noshallowcopyapi"  # limit size to exercise multipart code paths
        use_sse
        use_sse=kms
        listobjectsv2
        noshallowcopyapi
        symlink_in_meta
        "use_xattr=0 -o readdir_optimize"
        "use_xattr=0 -o readdir_optimize -o listobjectsv2 -ouse_sse=kms"
        "use_xattr=0 -o readdir_optimize -o readdir_check_size=48 -o symlink_in_meta"
        "use_cache=${CACHE_DIR} -o direct_read -o direct_read_backward_chunks=${BACKWARD_CHUNKS} -o direct_read_prefetch_thread=64
        -o direct_read_chunk_size=4 -o direct_read_prefetch_chunks=32 -o direct_read_prefetch_limit=1024 -o logfile=${LOGFILE}"
        "fake_diskfree=${FAKE_FREE_DISK_SIZE} -oparallel_count=10 -omultipart_size=10"
        "default_acl=private"
        "direct_read -o direct_read_local_file_cache_size_mb=${DIRECT_READ_LOCAL_FILE_CACHE_SIZE_MB}"
        "sigv4 -o region=${OSS_REGION}"
        ahbe_conf=${AHBE_CONFIG}
        "use_cache=${CACHE_DIR} -o del_cache -o set_check_cache_sigusr1=${CHECK_CACHE_FILE} -o logfile=${LOGFILE} -o check_cache_dir_exist"
        "max_dirty_data=50"
        "use_cache=${CACHE_DIR} -o free_space_ratio=1 -o del_cache"
        "public_bucket=0 -o no_check_certificate -o connect_timeout=300 -o readwrite_timeout=120 -o list_object_max_keys=1000"
        "use_wtf8 -o curldbg -o use_xattr -o ensure_diskfree=1"
        "allow_other -o uid=0 -o gid=0 -o mp_umask=000 -o umask=000"
        "auto_cache -o logfile=${LOGFILE} -o attr_timeout=0 -o entry_timeout=0 -o simulate_mtime_ns_with_crc64"
    )
else
    FLAGS=(
        sigv1
    )
fi

start_s3proxy
install_ossutil

if ! aws_cli s3api head-bucket --bucket "${TEST_BUCKET_1}" --region "${OSS_REGION}"; then
    aws_cli s3 mb "s3://${TEST_BUCKET_1}" --region "${OSS_REGION}"
fi

for flag in "${FLAGS[@]}"; do
    echo "testing ossfs flag: ${flag}"

    # shellcheck disable=SC2086
    start_ossfs -o ${flag}

    ./integration-test-main.sh

    TEST_NUMBER=$((TEST_NUMBER + 1))
    
    stop_ossfs
done

stop_s3proxy

echo "$0: tests complete."

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
