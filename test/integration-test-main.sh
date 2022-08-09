#!/bin/bash

set -o xtrace
set -o errexit

COMMON=integration-test-common.sh
source $COMMON

# Configuration
TEST_TEXT="HELLO WORLD"
TEST_TEXT_FILE=test-ossfs.txt
TEST_DIR=testdir
ALT_TEST_TEXT_FILE=test-ossfs-ALT.txt
TEST_TEXT_FILE_LENGTH=15
BIG_FILE=big-file-ossfs.txt
BIG_FILE_LENGTH=$((25 * 1024 * 1024))
CUR_DIR=`pwd`
TEST_BUCKET_MOUNT_POINT_1=$1

function get_size() {
    stat -c %s "$1"
}

function check_file_size() {
    FILE_NAME="$1"
    EXPECTED_SIZE="$2"

    # Verify file is zero length via metadata
    size=$(get_size ${FILE_NAME})
    if [ $size -ne $EXPECTED_SIZE ]
    then
        echo "error: expected ${FILE_NAME} to be zero length"
        return 1
    fi

    # Verify file is zero length via data
    size=$(cat ${FILE_NAME} | wc -c)
    if [ $size -ne $EXPECTED_SIZE ]
    then
        echo "error: expected ${FILE_NAME} to be $EXPECTED_SIZE length, got $size"
        return 1
    fi
}

function mk_test_file {
    if [ $# == 0 ]; then
        TEXT=$TEST_TEXT
    else
        TEXT=$1
    fi
    echo $TEXT > $TEST_TEXT_FILE
    if [ ! -e $TEST_TEXT_FILE ]
    then
        echo "Could not create file ${TEST_TEXT_FILE}, it does not exist"
        exit 1
    fi

    # wait & check
    BASE_TEXT_LENGTH=`echo $TEXT | wc -c | awk '{print $1}'`
    TRY_COUNT=10
    while true; do
        MK_TEXT_LENGTH=`wc -c $TEST_TEXT_FILE | awk '{print $1}'`
        if [ $BASE_TEXT_LENGTH -eq $MK_TEXT_LENGTH ]; then
            break
        fi
        TRY_COUNT=`expr $TRY_COUNT - 1`
        if [ $TRY_COUNT -le 0 ]; then
            echo "Could not create file ${TEST_TEXT_FILE}, that file size is something wrong"
        fi
        sleep 1
    done
}

function rm_test_file {
    if [ $# == 0 ]; then
        FILE=$TEST_TEXT_FILE
    else
        FILE=$1
    fi
    rm -f $FILE

    if [ -e $FILE ]
    then
        echo "Could not cleanup file ${TEST_TEXT_FILE}"
        exit 1
    fi
}

function mk_test_dir {
    if [ $# == 0 ]; then
        dir=${TEST_DIR}
    else
        dir=$1
    fi

    mkdir $dir

    if [ ! -d $dir ]; then
        echo "Directory $dir was not created"
        exit 1
    fi
}

function rm_test_dir {
    if [ $# == 0 ]; then
        dir=${TEST_DIR}
    else
        dir=$1
    fi

    rmdir $dir
    if [ -e $dir ]; then
        echo "Could not remove the test directory, it still exists: $dir"
        exit 1
    fi
}

function test_append_file {
    echo "Testing append to file ..."

    TEST_INPUT="echo ${TEST_TEXT} to ${TEST_TEXT_FILE}"

    # Write a small test file
    for x in `seq 1 $TEST_TEXT_FILE_LENGTH`
    do
        echo $TEST_INPUT
    done > ${TEST_TEXT_FILE}

    check_file_size "${TEST_TEXT_FILE}" $(($TEST_TEXT_FILE_LENGTH * $(echo $TEST_INPUT | wc -c)))

    rm_test_file
}

function test_truncate_file {
    echo "Testing truncate file ..."
    # Write a small test file
    echo "${TEST_TEXT}" > ${TEST_TEXT_FILE}

    # Truncate file to 0 length.  This should trigger open(path, O_RDWR | O_TRUNC...)
    : > ${TEST_TEXT_FILE}

    # Verify file is zero length
    check_file_size "${TEST_TEXT_FILE}" 0

    rm_test_file
}

function test_truncate_empty_file {
    echo "Testing truncate empty file ..."
    # Write an empty test file
    touch ${TEST_TEXT_FILE}

    # Truncate the file to 1024 length
    t_size=1024
    truncate ${TEST_TEXT_FILE} -s $t_size

    # Verify file is zero length
    check_file_size "${TEST_TEXT_FILE}" $t_size

    rm_test_file
}

function test_mv_file {
    echo "Testing mv file function ..."

    # if the rename file exists, delete it
    if [ -e $ALT_TEST_TEXT_FILE ]
    then
       rm $ALT_TEST_TEXT_FILE
    fi

    if [ -e $ALT_TEST_TEXT_FILE ]
    then
       echo "Could not delete file ${ALT_TEST_TEXT_FILE}, it still exists"
       exit 1
    fi

    # create the test file again
    mk_test_file

    #rename the test file
    mv $TEST_TEXT_FILE $ALT_TEST_TEXT_FILE
    if [ ! -e $ALT_TEST_TEXT_FILE ]
    then
       echo "Could not move file"
       exit 1
    fi

    # Check the contents of the alt file
    ALT_TEXT_LENGTH=`echo $TEST_TEXT | wc -c | awk '{print $1}'`
    ALT_FILE_LENGTH=`wc -c $ALT_TEST_TEXT_FILE | awk '{print $1}'`
    if [ "$ALT_FILE_LENGTH" -ne "$ALT_TEXT_LENGTH" ]
    then
       echo "moved file length is not as expected expected: $ALT_TEXT_LENGTH  got: $ALT_FILE_LENGTH"
       exit 1
    fi

    # clean up
    rm_test_file $ALT_TEST_TEXT_FILE
}

function test_mv_directory {
    echo "Testing mv directory function ..."
    if [ -e $TEST_DIR ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       exit 1
    fi

    mk_test_dir

    mv ${TEST_DIR} ${TEST_DIR}_rename

    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       exit 1
    fi

    rmdir ${TEST_DIR}_rename
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       exit 1
    fi
}

function test_redirects {
    echo "Testing redirects ..."

    mk_test_file ABCDEF

    CONTENT=`cat $TEST_TEXT_FILE`

    if [ ${CONTENT} != "ABCDEF" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected ABCDEF"
       exit 1
    fi

    echo XYZ > $TEST_TEXT_FILE

    CONTENT=`cat $TEST_TEXT_FILE`

    if [ ${CONTENT} != "XYZ" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected XYZ"
       exit 1
    fi

    echo 123456 >> $TEST_TEXT_FILE

    LINE1=`sed -n '1,1p' $TEST_TEXT_FILE`
    LINE2=`sed -n '2,2p' $TEST_TEXT_FILE`

    if [ ${LINE1} != "XYZ" ]; then
       echo "LINE1 was not as expected, got ${LINE1}, expected XYZ"
       exit 1
    fi

    if [ ${LINE2} != "123456" ]; then
       echo "LINE2 was not as expected, got ${LINE2}, expected 123456"
       exit 1
    fi

    # clean up
    rm_test_file
}

function test_mkdir_rmdir {
    echo "Testing creation/removal of a directory"

    if [ -e $TEST_DIR ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       exit 1
    fi

    mk_test_dir
    rm_test_dir
}

function test_chmod {
    echo "Testing chmod file function ..."

    # create the test file again
    mk_test_file

    ORIGINAL_PERMISSIONS=$(stat --format=%a $TEST_TEXT_FILE)

    chmod 777 $TEST_TEXT_FILE;

    # if they're the same, we have a problem.
    if [ $(stat --format=%a $TEST_TEXT_FILE) == $ORIGINAL_PERMISSIONS ]
    then
      echo "Could not modify $TEST_TEXT_FILE permissions"
      exit 1
    fi

    # clean up
    rm_test_file
}

function test_chown {
    echo "Testing chown file function ..."

    # show current user id
    id

    # create the test file again
    mk_test_file

    ORIGINAL_PERMISSIONS=$(stat --format=%u:%g $TEST_TEXT_FILE)

    chown 1234:1234 $TEST_TEXT_FILE;

    # if they're the same, we have a problem.
    if [ $(stat --format=%u:%g $TEST_TEXT_FILE) == $ORIGINAL_PERMISSIONS ]
    then
      echo "Could not modify $TEST_TEXT_FILE ownership"
      exit 1
    fi

    # clean up
    rm_test_file
}

function test_list {
    echo "Testing list"
    mk_test_file
    mk_test_dir

    file_cnt=$(ls -1 | wc -l)
    if [ $file_cnt != 2 ]; then
        echo "Expected 2 file but got $file_cnt"
        exit 1
    fi

    rm_test_file
    rm_test_dir
}

function test_list_many_files {
    echo "Testing list many files"

    dir="many_files"
    count=256
    mk_test_dir $dir

    for x in $(seq $count); do
        touch $dir/file-$x
    done

    file_cnt=$(ls -1 $dir | wc -l)
    if [ $file_cnt != $count ]; then
        echo "Expected $count files but got $file_cnt"
        exit 1
    fi

    rm -rf $dir
}

function test_remove_nonempty_directory {
    echo "Testing removing a non-empty directory"
    mk_test_dir
    touch "${TEST_DIR}/file"
    rmdir "${TEST_DIR}" 2>&1 | grep -q "Directory not empty"
    rm "${TEST_DIR}/file"
    rm_test_dir
}

function test_rename_before_close {
    echo "Testing rename before close ..."
    (
        echo foo
        mv $TEST_TEXT_FILE ${TEST_TEXT_FILE}.new
    ) > $TEST_TEXT_FILE

    if ! cmp <(echo foo) ${TEST_TEXT_FILE}.new; then
        echo "rename before close failed"
        exit 1
    fi

    rm_test_file ${TEST_TEXT_FILE}.new
    rm -f ${TEST_TEXT_FILE}
}

function test_multipart_upload {
    echo "Testing multi-part upload ..."
    dd if=/dev/urandom of="/tmp/${BIG_FILE}" bs=$BIG_FILE_LENGTH count=1
    dd if="/tmp/${BIG_FILE}" of="${BIG_FILE}" bs=$BIG_FILE_LENGTH count=1

    # Verify contents of file
    echo "Comparing test file"
    if ! cmp "/tmp/${BIG_FILE}" "${BIG_FILE}"
    then
       exit 1
    fi

    rm -f "/tmp/${BIG_FILE}"
    rm_test_file "${BIG_FILE}"
}

function test_multipart_copy {
    echo "Testing multi-part copy ..."
    dd if=/dev/urandom of="/tmp/${BIG_FILE}" bs=$BIG_FILE_LENGTH count=1
    dd if="/tmp/${BIG_FILE}" of="${BIG_FILE}" bs=$BIG_FILE_LENGTH count=1
    mv "${BIG_FILE}" "${BIG_FILE}-copy"

    # Verify contents of file
    echo "Comparing test file"
    if ! cmp "/tmp/${BIG_FILE}" "${BIG_FILE}-copy"
    then
       exit 1
    fi

    rm -f "/tmp/${BIG_FILE}"
    rm_test_file "${BIG_FILE}-copy"
}

function test_special_characters {
    echo "Testing special characters ..."

    ls 'special' 2>&1 | grep -q 'No such file or directory'
    ls 'special?' 2>&1 | grep -q 'No such file or directory'
    ls 'special*' 2>&1 | grep -q 'No such file or directory'
    ls 'special~' 2>&1 | grep -q 'No such file or directory'
    ls 'specialµ' 2>&1 | grep -q 'No such file or directory'
}

function test_symlink {
    echo "Testing symlinks ..."

    rm -f $TEST_TEXT_FILE
    rm -f $ALT_TEST_TEXT_FILE
    echo foo > $TEST_TEXT_FILE

    ln -s $TEST_TEXT_FILE $ALT_TEST_TEXT_FILE
    cmp $TEST_TEXT_FILE $ALT_TEST_TEXT_FILE

    rm -f $TEST_TEXT_FILE

    [ -L $ALT_TEST_TEXT_FILE ]
    [ ! -f $ALT_TEST_TEXT_FILE ]
}

function test_extended_attributes {
    command -v setfattr >/dev/null 2>&1 || \
        { echo "Skipping extended attribute tests" ; return; }

    echo "Testing extended attributes ..."

    rm -f $TEST_TEXT_FILE
    touch $TEST_TEXT_FILE

    # set value
    setfattr -n key1 -v value1 $TEST_TEXT_FILE
    getfattr -n key1 --only-values $TEST_TEXT_FILE | grep -q '^value1$'

    # append value
    setfattr -n key2 -v value2 $TEST_TEXT_FILE
    getfattr -n key1 --only-values $TEST_TEXT_FILE | grep -q '^value1$'
    getfattr -n key2 --only-values $TEST_TEXT_FILE | grep -q '^value2$'

    # remove value
    setfattr -x key1 $TEST_TEXT_FILE
    ! getfattr -n key1 --only-values $TEST_TEXT_FILE
    getfattr -n key2 --only-values $TEST_TEXT_FILE | grep -q '^value2$'
}

function test_mtime_file {
    echo "Testing mtime preservation function ..."

    # if the rename file exists, delete it
    if [ -e $ALT_TEST_TEXT_FILE -o -L $ALT_TEST_TEXT_FILE ]
    then
       rm $ALT_TEST_TEXT_FILE
    fi

    if [ -e $ALT_TEST_TEXT_FILE ]
    then
       echo "Could not delete file ${ALT_TEST_TEXT_FILE}, it still exists"
       exit 1
    fi

    # create the test file again
    mk_test_file
    sleep 2 # allow for some time to pass to compare the timestamps between test & alt

    #copy the test file with preserve mode
    cp -p $TEST_TEXT_FILE $ALT_TEST_TEXT_FILE
    testmtime=`stat -c %Y $TEST_TEXT_FILE`
    altmtime=`stat -c %Y $ALT_TEST_TEXT_FILE`
    if [ "$testmtime" -ne "$altmtime" ]
    then
       echo "File times do not match:  $testmtime != $altmtime"
       exit 1
    fi
}

function test_file_size_in_stat_cache {
    echo "Testing file size in stat cache..."
    python $CUR_DIR/stat_cache_test.py $TEST_BUCKET_MOUNT_POINT_1
}

function test_ut_ossfs {
    echo "Testing ossfs python ut..."
    python3 $CUR_DIR/ut_test.py
}

function test_mv_empty_directory {
    echo "Testing mv directory function ..."
    if [ -e $TEST_DIR ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir

    mv ${TEST_DIR} ${TEST_DIR}_rename
    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       return 1
    fi

    rmdir ${TEST_DIR}_rename
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       return 1
    fi
}

function test_mv_nonempty_directory {
    echo "Testing mv directory function ..."
    if [ -e $TEST_DIR ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir

    touch ${TEST_DIR}/file

    mv ${TEST_DIR} ${TEST_DIR}_rename
    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       return 1
    fi

    rm -r ${TEST_DIR}_rename
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       return 1
    fi
}

function test_rm_rf_dir {
   echo "Test that rm -rf will remove directory with contents"
   # Create a dir with some files and directories
   mkdir dir1
   mkdir dir1/dir2
   touch dir1/file1
   touch dir1/dir2/file2

   # Remove the dir with recursive rm
   rm -rf dir1

   if [ -e dir1 ]; then
       echo "rm -rf did not remove $PWD/dir1"
       return 1
   fi
}

function test_copy_file {
   echo "Test simple copy"

   dd if=/dev/urandom of=/tmp/simple_file bs=1024 count=1
   cp /tmp/simple_file copied_simple_file
   cmp /tmp/simple_file copied_simple_file

   rm_test_file /tmp/simple_file
   rm_test_file copied_simple_file
}

function test_write_after_seek_ahead {
   echo "Test writes succeed after a seek ahead"
   dd if=/dev/zero of=testfile seek=1 count=1 bs=1024
   rm_test_file testfile
}

function test_overwrite_existing_file_range {
    echo "Test overwrite range succeeds"
    dd if=<(seq 1000) of=${TEST_TEXT_FILE}
    dd if=/dev/zero of=${TEST_TEXT_FILE} seek=1 count=1 bs=1024 conv=notrunc
    cmp ${TEST_TEXT_FILE} <(
        seq 1000 | head -c 1024
        dd if=/dev/zero count=1 bs=1024
        seq 1000 | tail -c +2049
    )
    rm_test_file
}

function test_concurrency {
    echo "Test concurrent updates to a directory"
    for i in `seq 5`; do echo foo > $i; done
    for process in `seq 10`; do
        for i in `seq 5`; do
            file=$(ls `seq 5` | sed -n "$(($RANDOM % 5 + 1))p")
            cat $file >/dev/null || true
            rm -f $file
            echo foo > $file || true
        done &
    done
    wait
    rm -f `seq 5`
}

function test_concurrent_writes {
    echo "Test concurrent updates to a file"
    dd if=/dev/urandom of=${TEST_TEXT_FILE} bs=$BIG_FILE_LENGTH count=1
    for process in `seq 10`; do
        dd if=/dev/zero of=${TEST_TEXT_FILE} seek=$(($RANDOM % $BIG_FILE_LENGTH)) count=1 bs=1024 conv=notrunc &
    done
    wait
    rm_test_file
}

function test_open_second_fd {
    echo "read from an open fd"
    rm_test_file second_fd_file
    RESULT=$( (echo foo ; wc -c < second_fd_file >&2) 2>& 1>second_fd_file)
    if [ "$RESULT" -ne 4 ]; then
        echo "size mismatch, expected: 4, was: ${RESULT}"
        return 1
    fi
    rm_test_file second_fd_file
}

function test_write_multiple_offsets {
    echo "test writing to multiple offsets"
    python3 $CUR_DIR/write_multiple_offsets.py ${TEST_TEXT_FILE}
    rm_test_file ${TEST_TEXT_FILE}
}

function run_all_tests {
    test_append_file
    test_truncate_file
    test_truncate_empty_file
    test_mv_file
    test_mv_directory
    test_redirects
    test_mkdir_rmdir
    test_chmod
    test_chown
    test_list
    test_list_many_files
    # XXX: Haoran: Do not know why this case failed in script but passed by manually run.
    test_remove_nonempty_directory
    # TODO: broken: https://github.com/s3fs-fuse/s3fs-fuse/issues/145
    test_rename_before_close
    test_multipart_upload
    # TODO: test disabled until S3Proxy 1.5.0 is released
    test_multipart_copy
    test_special_characters
    test_symlink
    test_extended_attributes
    test_mtime_file
    test_file_size_in_stat_cache
    test_ut_ossfs
    test_mv_empty_directory
    test_mv_nonempty_directory
    test_rm_rf_dir
    test_copy_file
    test_write_after_seek_ahead
    test_overwrite_existing_file_range
    test_concurrency
    test_concurrent_writes
    test_open_second_fd
    test_write_multiple_offsets
}

# Mount the bucket
if [ "$TEST_BUCKET_MOUNT_POINT_1" == "" ]; then
    echo "Mountpoint missing"
    exit 1
fi
cd $TEST_BUCKET_MOUNT_POINT_1

if [ -e $TEST_TEXT_FILE ]
then
  rm -f $TEST_TEXT_FILE
fi

rm -rf *
run_all_tests

# Unmount the bucket
cd $CUR_DIR
echo "All tests complete."
