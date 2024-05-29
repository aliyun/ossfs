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
rm -rf "${CACHE_DIR}"
mkdir "${CACHE_DIR}"

source test-utils.sh

#reserve 200MB for data cache
FAKE_FREE_DISK_SIZE=200
ENSURE_DISKFREE_SIZE=10

export CACHE_DIR
export ENSURE_DISKFREE_SIZE 
if [ -n "${ALL_TESTS}" ]; then
    FLAGS=(
        "use_cache=${CACHE_DIR} -o ensure_diskfree=${ENSURE_DISKFREE_SIZE} -o fake_diskfree=${FAKE_FREE_DISK_SIZE}"
        enable_content_md5
        enable_noobj_cache
        "max_stat_cache_size=100"
        nocopyapi
        nomultipart
        notsup_compat_dir
        sigv1
        "singlepart_copy_limit=10"  # limit size to exercise multipart code paths
        use_sse
        use_sse=kms
        listobjectsv2
        noshallowcopyapi
        symlink_in_meta
        "use_xattr=0 -o readdir_optimize"
        "use_xattr=0 -o readdir_optimize -o listobjectsv2 -ouse_sse=kms"
        "use_xattr=0 -o readdir_optimize -o readdir_check_size=48 -o symlink_in_meta"
        "use_cache=${CACHE_DIR} -o direct_read"
        "fake_diskfree=${FAKE_FREE_DISK_SIZE} -oparallel_count=10 -omultipart_size=10"
    )
else
    FLAGS=(
        sigv1
    )
fi

start_s3proxy

if ! aws_cli s3api head-bucket --bucket "${TEST_BUCKET_1}" --region "${OSS_REGION}"; then
    aws_cli s3 mb "s3://${TEST_BUCKET_1}" --region "${OSS_REGION}"
fi

for flag in "${FLAGS[@]}"; do
    echo "testing ossfs flag: ${flag}"

    # shellcheck disable=SC2086
    start_ossfs -o ${flag}

    ./integration-test-main.sh

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
