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
# Common code for starting an ossfs mountpoint and an S3Proxy instance 
# to run tests against S3Proxy locally.
#
# To run against an OSS, specify the following environment variables:
#
# OSSFS_CREDENTIALS_FILE=keyfile     ossfs format key file
# TEST_BUCKET_1=bucketname           Name of bucket to use 
# S3PROXY_BINARY=""                  Specify empty string to skip S3Proxy start
# OSS_URL="http://oss-cn-hangzhou.aliyuncs.com" Specify the oss provider
# OSS_REGION="cn-hangzhou"           Specify region
# TMPDIR="/var/tmp"                  Set to use a temporary directory different
#                                    from /var/tmp
#
# Example of running against OSS using a bucket named "bucket":
#
# OSSFS_CREDENTIALS_FILE=keyfile TEST_BUCKET_1=bucket S3PROXY_BINARY="" OSS_URL="http://oss-cn-hangzhou.aliyuncs.com" ./small-integration-test.sh
#
# To change the ossfs debug level:
#
#    DBGLEVEL=debug ./small-integration-test.sh
# 
# To stop and wait after the mount point is up for manual interaction. This allows you to
# explore the mounted file system exactly as it would have been started for the test case
#
#    INTERACT=1 DBGLEVEL=debug ./small-integration-test.sh
#
# Run all of the tests from the makefile
#
#    OSSFS_CREDENTIALS_FILE=keyfile TEST_BUCKET_1=bucket S3PROXY_BINARY="" OSS_URL="http://oss-cn-hangzhou.aliyuncs.com" make check
#
# Run the tests with request auth turned off in both S3Proxy and ossfs.  This can be
# useful for poking around with plain old curl
#
#    PUBLIC=1 INTERACT=1 ./small-integration-test.sh 
#
# A valgrind tool can be specified
# eg: VALGRIND="--tool=memcheck --leak-check=full" ./small-integration-test.sh

set -o errexit
set -o pipefail

OSSFS=../src/ossfs

# Allow these defaulted values to be overridden
: "${OSS_URL:="https://127.0.0.1:8080"}"
: "${OSS_REGION:="cn-zhangjiakou"}"
: "${OSSFS_CREDENTIALS_FILE:="passwd-ossfs"}"
: "${TEST_BUCKET_1:="ossfs-integration-test"}"
: "${OSS_SIGNATURE_VERSION:="sigv4"}"

export TEST_BUCKET_1
export OSS_URL
export OSS_REGION
export OSS_SIGNATURE_VERSION
TEST_SCRIPT_DIR=$(pwd)
export TEST_SCRIPT_DIR
export TEST_BUCKET_MOUNT_POINT_1=${TEST_BUCKET_1}

S3PROXY_VERSION="1.9.0"
S3PROXY_BINARY="${S3PROXY_BINARY-"s3proxy-${S3PROXY_VERSION}"}"

CHAOS_HTTP_PROXY_VERSION="1.1.0"
CHAOS_HTTP_PROXY_BINARY="chaos-http-proxy-${CHAOS_HTTP_PROXY_VERSION}"

if [ ! -f "$OSSFS_CREDENTIALS_FILE" ]
then
	echo "Missing credentials file: ${OSSFS_CREDENTIALS_FILE}"
	exit 1
fi
chmod 600 "${OSSFS_CREDENTIALS_FILE}"

#aws cli 
if [ -z "${AWS_PROFILE}" ]; then
    AWS_ACCESS_KEY_ID=$(cut -d: -f1 "${OSSFS_CREDENTIALS_FILE}")
    export AWS_ACCESS_KEY_ID
    
    AWS_SECRET_ACCESS_KEY=$(cut -d: -f2 "${OSSFS_CREDENTIALS_FILE}")
    export AWS_SECRET_ACCESS_KEY

    AWS_CONFIG_FILE=${TEST_SCRIPT_DIR}/aws_cli_config
    export AWS_CONFIG_FILE
fi

if [ ! -d "${TEST_BUCKET_MOUNT_POINT_1}" ]; then
	mkdir -p "${TEST_BUCKET_MOUNT_POINT_1}"
fi

# This function execute the function parameters $1 times
# before giving up, with 1 second delays.
function retry {
    local N="$1"
    shift
    rc=0
    for _ in $(seq "${N}"); do
        echo "Trying: $*"
        # shellcheck disable=SC2068,SC2294
        eval $@
        rc=$?
        if [ "${rc}" -eq 0 ]; then
            break
        fi
        sleep 1
        echo "Retrying: $*"
    done

    if [ "${rc}" -ne 0 ]; then
        echo "timeout waiting for $*"
    fi
    return "${rc}"
}

# Proxy is not started if S3PROXY_BINARY is an empty string
# PUBLIC unset: use s3proxy.conf
# PUBLIC=1:     use s3proxy-noauth.conf (no request signing)
# 
function start_s3proxy {
    if [ -n "${PUBLIC}" ]; then
        local S3PROXY_CONFIG="s3proxy-noauth.conf"
    else
        local S3PROXY_CONFIG="s3proxy.conf"
    fi

    if [ -n "${S3PROXY_BINARY}" ]
    then
        if [ ! -e "${S3PROXY_BINARY}" ]; then
            curl "https://github.com/gaul/s3proxy/releases/download/s3proxy-${S3PROXY_VERSION}/s3proxy" \
                --fail --location --silent --output "${S3PROXY_BINARY}"
            chmod +x "${S3PROXY_BINARY}"
        fi

        # generate self-signed SSL certificate
        rm -f /tmp/keystore.jks /tmp/keystore.pem
        echo -e 'password\npassword\n\n\n\n\n\n\nyes' | keytool -genkey -keystore /tmp/keystore.jks -keyalg RSA -keysize 2048 -validity 365 -ext SAN=IP:127.0.0.1
        echo password | keytool -exportcert -keystore /tmp/keystore.jks -rfc -file /tmp/keystore.pem

        "${STDBUF_BIN}" -oL -eL java -jar "${S3PROXY_BINARY}" --properties "${S3PROXY_CONFIG}" &
        S3PROXY_PID=$!

        # wait for S3Proxy to start
        wait_for_port 8080
    fi

    if [ -n "${CHAOS_HTTP_PROXY}" ]; then
        if [ ! -e "${CHAOS_HTTP_PROXY_BINARY}" ]; then
            curl "https://github.com/bouncestorage/chaos-http-proxy/releases/download/chaos-http-proxy-${CHAOS_HTTP_PROXY_VERSION}/chaos-http-proxy" \
                --fail --location --silent --output "${CHAOS_HTTP_PROXY_BINARY}"
            chmod +x "${CHAOS_HTTP_PROXY_BINARY}"
        fi

        "${STDBUF_BIN}" -oL -eL java -jar "${CHAOS_HTTP_PROXY_BINARY}" --properties chaos-http-proxy.conf &
        CHAOS_HTTP_PROXY_PID=$!

        # wait for Chaos HTTP Proxy to start
        wait_for_port 1080
    fi
}

function stop_s3proxy {
    if [ -n "${S3PROXY_PID}" ]
    then
        kill "${S3PROXY_PID}"
    fi

    if [ -n "${CHAOS_HTTP_PROXY_PID}" ]
    then
        kill "${CHAOS_HTTP_PROXY_PID}"
    fi
}

# Mount the bucket, function arguments passed to ossfs in addition to
# a set of common arguments.  
function start_ossfs {
    # Public bucket if PUBLIC is set
    if [ -n "${PUBLIC}" ]; then
        local AUTH_OPT="-o public_bucket=1"
    else
        local AUTH_OPT="-o passwd_file=${OSSFS_CREDENTIALS_FILE}"
    fi

    # If VALGRIND is set, pass it as options to valgrind.
    # start valgrind-listener in another shell. 
    # eg: VALGRIND="--tool=memcheck --leak-check=full" ./small-integration-test.sh
    # Start valgrind-listener (default port is 1500)
    if [ -n "${VALGRIND}" ]; then
        VALGRIND_EXEC="valgrind ${VALGRIND} --log-file=valgrind.%p.log"
    fi

    # On OSX only, we need to specify the direct_io and auto_cache flag.
    if [ "$(uname)" = "Darwin" ]; then
       local DIRECT_IO_OPT="-o direct_io -o auto_cache"
    else
       local DIRECT_IO_OPT=""
    fi

    if [ -n "${CHAOS_HTTP_PROXY}" ]; then
        export http_proxy="127.0.0.1:1080"
    fi

    # [NOTE]
    # On macos, running ossfs via stdbuf will result in no response.
    # Therefore, when it is macos, it is not executed via stdbuf.
    # This patch may be temporary, but no other method has been found at this time.
    #
    if [ "$(uname)" = "Darwin" ]; then
        local VIA_STDBUF_CMDLINE=""
    else
        local VIA_STDBUF_CMDLINE="${STDBUF_BIN} -oL -eL"
    fi

    # Common ossfs options:
    #
    # TODO: Allow all these options to be overridden with env variables
    #
    # use_path_request_style
    #     The test env doesn't have virtual hosts
    # $AUTH_OPT
    #     Will be either "-o public_bucket=1" 
    #                     or 
    #     "-o passwd_file=${OSSFS_CREDENTIALS_FILE}"
    # dbglevel
    #     error by default.  override with DBGLEVEL env variable
    # -f
    #     Keep ossfs in foreground instead of daemonizing
    #

    # subshell with set -x to log exact invocation of ossfs
    # shellcheck disable=SC2086
    (
        set -x 
        CURL_CA_BUNDLE=/tmp/keystore.pem \
        ${VIA_STDBUF_CMDLINE} \
            ${VALGRIND_EXEC} \
            ${OSSFS} \
            ${TEST_BUCKET_1} \
            ${TEST_BUCKET_MOUNT_POINT_1} \
            -o url="${OSS_URL}" \
            -o use_xattr=1 \
            -o enable_unsigned_payload \
            ${AUTH_OPT} \
            ${DIRECT_IO_OPT} \
            -o stat_cache_expire=1 \
            -o stat_cache_interval_expire=1 \
            -o dbglevel="${DBGLEVEL:=info}" \
            -o no_time_stamp_msg \
            -o retries=3 \
            -o region="${OSS_REGION}" \
            -o ${OSS_SIGNATURE_VERSION} \
            -f \
            "${@}" &
        echo $! >&3
    ) 3>pid | "${STDBUF_BIN}" -oL -eL "${SED_BIN}" "${SED_BUFFER_FLAG}" "s/^/ossfs: /" &
    sleep 1
    OSSFS_PID=$(<pid)
    export OSSFS_PID
    rm -f pid

    if [ "$(uname)" = "Darwin" ]; then
         local TRYCOUNT=0
         while [ "${TRYCOUNT}" -le "${RETRIES:=20}" ]; do
             df | grep -q "${TEST_BUCKET_MOUNT_POINT_1}"
             rc=$?
             if [ "${rc}" -eq 0 ]; then
                 break;
             fi
             sleep 1
             TRYCOUNT=$((TRYCOUNT + 1))
         done
         if [ "${rc}" -ne 0 ]; then
             exit 1
         fi
    else
        retry "${RETRIES:=20}" grep -q "${TEST_BUCKET_MOUNT_POINT_1}" /proc/mounts || exit 1
    fi

    # Quick way to start system up for manual testing with options under test
    if [[ -n "${INTERACT}" ]]; then
        echo "Mountpoint ${TEST_BUCKET_MOUNT_POINT_1} is ready"
        echo "control-C to quit"
        sleep infinity
        exit 0
    fi
}

function stop_ossfs {
    # Retry in case file system is in use
    if [ "$(uname)" = "Darwin" ]; then
        if df | grep -q "${TEST_BUCKET_MOUNT_POINT_1}"; then
            retry 10 df "|" grep -q "${TEST_BUCKET_MOUNT_POINT_1}" "&&" umount "${TEST_BUCKET_MOUNT_POINT_1}"
        fi
    else
        if grep -q "${TEST_BUCKET_MOUNT_POINT_1}" /proc/mounts; then 
            retry 10 grep -q "${TEST_BUCKET_MOUNT_POINT_1}" /proc/mounts "&&" fusermount -u "${TEST_BUCKET_MOUNT_POINT_1}"
        fi
    fi
}

# trap handlers do not stack.  If a test sets its own, the new handler should call common_exit_handler
function common_exit_handler {
    stop_ossfs
    stop_s3proxy
}
trap common_exit_handler EXIT

function install_ossutil {
    if ! [ -x "$(command -v ossutil)" ]; then
        curl https://gosspublic.alicdn.com/ossutil/install.sh > install_ossutil.sh
        bash install_ossutil.sh
        if ! [ -x "$(command -v ossutil)" ]; then
            echo "Failed to install ossutil"
            exit 1
        fi
    fi
}

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
