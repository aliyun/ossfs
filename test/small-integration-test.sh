#!/bin/bash

#
# By default tests run against a local s3proxy instance.  To run against 
# Aliyun OSS, specify the following variables:
#
# OSSFS_CREDENTIALS_FILE=keyfile      ossfs format key file
# TEST_BUCKET_1=bucket               Name of bucket to use 
# OSSPROXY_BINARY=""                  Leave empty 
# OSS_URL=""   Specify OSS server
#
# Example: 
#
# OSSFS_CREDENTIALS_FILE=keyfile TEST_BUCKET_1=bucket OSSPROXY_BINARY="" OSS_URL="http://oss_url" ./small-integration-test.sh
#

set -o xtrace
set -o errexit

: ${OSS_URL:="http://127.0.0.1:8080"}

# Require root
REQUIRE_ROOT=require-root.sh
#source $REQUIRE_ROOT
source integration-test-common.sh

function retry {
    set +o errexit
    N=$1; shift;
    status=0
    for i in $(seq $N); do
        $@
        status=$?
        if [ $status == 0 ]; then
            break
        fi
        sleep 1
    done

    if [ $status != 0 ]; then
        echo "timeout waiting for $@"
    fi
    set -o errexit
    return $status
}

function exit_handler {
    if [ -n "${OSSPROXY_PID}" ]
    then
        kill $OSSPROXY_PID
    fi
    retry 30 fusermount -u $TEST_BUCKET_MOUNT_POINT_1
}
trap exit_handler EXIT

if [ -n "${OSSPROXY_BINARY}" ]
then
    stdbuf -oL -eL java -jar "$OSSPROXY_BINARY" --properties s3proxy.conf | stdbuf -oL -eL sed -u "s/^/s3proxy: /" &

    # wait for OSSProxy to start
    for i in $(seq 30);
    do
        if exec 3<>"/dev/tcp/127.0.0.1/8080";
        then
            exec 3<&-  # Close for read
            exec 3>&-  # Close for write
            break
        fi
        sleep 1
    done

    OSSPROXY_PID=$(netstat -lpnt | grep :8080 | awk '{ print $7 }' | sed -u 's|/java||')
fi

# Mount the bucket
if [ ! -d $TEST_BUCKET_MOUNT_POINT_1 ]
then
	mkdir -p $TEST_BUCKET_MOUNT_POINT_1
fi
stdbuf -oL -eL $OSSFS $TEST_BUCKET_1 $TEST_BUCKET_MOUNT_POINT_1 \
    -o createbucket \
    -o enable_content_md5 \
    -o passwd_file=$OSSFS_CREDENTIALS_FILE \
    -o sigv2 \
    -o singlepart_copy_limit=$((10 * 1024)) \
    -o url=${OSS_URL} \
    -o use_path_request_style \
    -o dbglevel=debug -f |& stdbuf -oL -eL sed -u "s/^/ossfs: /" &

retry 30 grep $TEST_BUCKET_MOUNT_POINT_1 /proc/mounts || exit 1

./integration-test-main.sh $TEST_BUCKET_MOUNT_POINT_1

echo "All tests complete."
