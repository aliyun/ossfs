#!/bin/bash -e
set -x
OSSFS=../src/ossfs

: ${OSSFS_CREDENTIALS_FILE:="passwd-ossfs"}

: ${TEST_BUCKET_1:="ossfs-integration-test"}
TEST_BUCKET_MOUNT_POINT_1=${TEST_BUCKET_1}

if [ ! -f "$OSSFS_CREDENTIALS_FILE" ]
then
	echo "Missing credentials file: $OSSFS_CREDENTIALS_FILE"
	exit 1
fi
chmod 600 "$OSSFS_CREDENTIALS_FILE"

S3PROXY_VERSION="1.4.0"
S3PROXY_BINARY=${S3PROXY_BINARY-"s3proxy-${S3PROXY_VERSION}"}
#if [ -n "${S3PROXY_BINARY}" ] && [ ! -e "${S3PROXY_BINARY}" ]; then
    #wget "https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-${S3PROXY_VERSION}/s3proxy" \
            #--quiet -O "${S3PROXY_BINARY}"
    #chmod +x "${S3PROXY_BINARY}"
#fi
