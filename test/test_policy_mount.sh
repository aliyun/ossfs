# This script is used to test whether the ossfs mount meets expectations under different bucket policy settings.
# To use this script, you need to know how to set the corresponding bucket policy.
# see https://help.aliyun.com/zh/oss/user-guide/use-bucket-policy-to-grant-permission-to-access-oss/?spm=a2c4g.11186623.0.0.1b814b76rHJgy5#concept-2071378
# You need to modify the policy.json file

# # This script runs all tests by default. You can run it in the following way
# Usage: ./test_policy_mount.sh [bucket] [mp] [ak] [sk] [endpoint] [subdir]
# example: ./test_policy_mount.sh your_bucket /mnt ak sk http://oss-cn-hangzhou.aliyuncs.com dir

# This script can also be used to run a single test. You can run it in the following way
# Usage: ./test_policy_mount.sh [bucket] [mp] [ak] [sk] [endpoint] [subdir] [case]
# example: ./test_policy_mount.sh your_bucket /mnt ak sk http://oss-cn-hangzhou.aliyuncs.com dir test_no_such_bucket

#!/bin/bash

bucket_name=$1
mp=$2
ak=$3
sk=$4
endpoint=$5
subdir_name=$6
case=$7

function drop_cache {
    sync
    (echo 3 | tee /proc/sys/vm/drop_caches) > /dev/null
}

function run_test {
    local test_case=$1
    shift
    drop_cache
    echo "testing $test_case "

    if [ $# != 0 ]; then
        $test_case $@ 
        test_status=$?
    else
        $test_case 
        test_status=$?
    fi

    if [ $test_status -eq 0 ]; then
        echo "test $test_case passed"
    else
        echo "test $test_case failed"
    fi
}

function mount {
    bucket=$1
    subdir=$2
    if [ -z "$subdir" ]; then
        ossfs $bucket $mp -ourl=$endpoint -odbglevel=dbg -ologfile=/root/ossfs_policy.log 
    else
        ossfs $bucket:/$subdir $mp -ourl=$endpoint -odbglevel=dbg -ologfile=/root/ossfs_policy.log 
    fi
}

function unmount {
    umount $mp 2> /dev/null
    rm -rf /root/ossfs_policy.log
}

function add_all_tests {
    run_test "test_no_such_bucket"
    run_test "test_bucket_no_policy_with_subdir_no_policy"
    run_test "test_bucket_no_policy_with_subdir_has_policy"
    run_test "test_exitcode"
    run_test "test_create_without_perm"
}

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

function ossutil_cmd {
    ossutil -i $ak -k $sk -e $endpoint $@
}

function test_no_such_bucket {
    if ps -ef | grep -v "grep" | grep -q "ossfs"; then
        unmount
    fi
    
    # mount temp bucket
    tmp_bucket="tmp_bucket_$(date +%s)"
    mount $tmp_bucket
    sleep 1
    if ps -ef | grep -v "grep" | grep -q "ossfs"; then
        echo "ossfs is running, not expected"
        return 1
    fi

    if ! grep -q "NoSuchBucket" /root/ossfs_policy.log; then
        echo "not found NoSuchBucket in log, not expected"
    fi

    unmount
    # mount temp subdir
    mount $tmp_bucket $subdir_name
    sleep 1
    if ps -ef | grep -v "grep" | grep -q "ossfs"; then
        echo "ossfs is running, not expected"
        return 1
    fi

    if ! grep -q "NoSuchBucket" /root/ossfs_policy.log; then
        echo "not found NoSuchBucket in log, not expected"
    fi

    unmount
}

function test_bucket_no_policy_with_subdir_no_policy {
    if ps -ef | grep -v "grep" | grep -q "ossfs"; then
        unmount
    fi
    
    # test mount bucket no policy
    mount $bucket_name
    sleep 1
    if ! grep -q "AccessDenied" /root/ossfs_policy.log; then
        echo "not found AccessDenied in log, not expected"
        return 1
    fi
    unmount
    
    # test subdir no policy
    subdir=tmp_subdir_$(date +%s)
    
    # test subdir does not exist
    mount $bucket_name $subdir
    sleep 1
    if ! grep -q "AccessDenied" /root/ossfs_policy.log; then
        echo "not found AccessDenied in log, not expected"
        return 1
    fi
    unmount

    # test subdir exist
    ossutil_cmd mkdir oss://$bucket_name/$subdir_name
    mount $bucket_name $subdir
    sleep 1
    if ! grep -q "AccessDenied" /root/ossfs_policy.log; then
        echo "not found AccessDenied in log, not expected"
        return 1
    fi
    unmount
    
}

function test_bucket_no_policy_with_subdir_has_policy {
    if ps -ef | grep -v "grep" | grep -q "ossfs"; then
        unmount
    fi

    # test mount bucket no policy but has subdir policy
    ossutil_cmd bucket-policy --method put oss://$bucket_name test_policies/policy.json

    # test subdir does not exist
    ossutil_cmd rm -r -f oss://$bucket_name/$subdir_name
    
    mount $bucket_name $subdir_name
    sleep 1
    if ! grep -q "NoSuchKey" /root/ossfs_policy.log; then
        echo "not found NoSuchKey in log, not expected"
        return 1
    fi

    if ! ps -ef | grep -v "grep" | grep -q "ossfs"; then
        echo "ossfs is not running, not expected"
        return 1
    fi
    unmount
    
    # test subdir exist
    ossutil_cmd mkdir oss://$bucket_name/$subdir_name
    
    mount $bucket_name $subdir_name
    sleep 1
    if ! ps -ef | grep -v "grep" | grep -q "ossfs"; then
        echo "ossfs is not running, not expected"
        return 1
    fi
    unmount
}

function test_exitcode {
  # set policy.json as no access to the bucket
  ossutil_cmd bucket-policy --method put oss://$bucket_name test_policies/policy.json

  mount $bucket_name $subdir_name
  if [ $? == 0 ]; then
    return 1
  fi

  unmount
}

function test_create_without_perm {
  # set policy_noaccess_prefix.json:
  # full access to the bucket
  # no access to the prefix/*
  ossutil_cmd bucket-policy --method put oss://$bucket_name test_policies/policy_noaccess_prefix.json

  mount $bucket_name
  sleep 1

  res=`ps -ef | grep ossfs | grep -v grep | grep "$mp"`
  if [ -z "$res" ]; then
    echo 1
        return 1
  fi

  res=`mkdir -p $mp/dir/whatever 2>/dev/stdout`
  if ! echo "$res" | grep  "not permitted"; then
    return 1
  fi

  unmount
}

install_ossutil
if [ -z "$case" ]; then
    add_all_tests
else
    run_test $case
fi