#!/bin/bash

# Usage:
#   step 1: echo "AK:SK" > /root/.passwd-ossfs
#   step 2: sh ossfs-coverage-centos7.sh bucket-name url

buildossfs() {
    cd $OSSFS_SOURCE_DIR
    make clean
    ./autogen.sh
    ./configure CXXFLAGS="-g -O2 --coverage -fprofile-arcs -ftest-coverage"
    make -j4  
}

BUCKET=$1
URL=$2
OSSFS_SOURCE_DIR=$PWD

echo $OSSFS_SOURCE_DIR

echo "Install dependencies..." 
yum install -y curl python-setuptools python-pip awscli mailcap attr
yum install -y lcov

echo "Build OSSFS..."
buildossfs

echo "execute the tests"
cd ${OSSFS_SOURCE_DIR}/test
chmod +x *.sh
chmod +x *.py

rm -rf ${OSSFS_SOURCE_DIR}/coverage_html && mkdir ${OSSFS_SOURCE_DIR}/coverage_html

DBGLEVEL=debug ALL_TESTS=1 OSSFS_CREDENTIALS_FILE=/root/.passwd-ossfs TEST_BUCKET_1=${BUCKET}  S3PROXY_BINARY="" OSS_URL=${URL} ./small-integration-test.sh

gcovr -r ${OSSFS_SOURCE_DIR}/src --html-details -o ${OSSFS_SOURCE_DIR}/coverage_html/coverage.html



