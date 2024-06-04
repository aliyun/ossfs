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

set -o errexit
set -o pipefail

source test-utils.sh

function test_create_empty_file {
    describe "Testing creating an empty file ..."

    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"

    touch "${TEST_TEXT_FILE}"

    check_file_size "${TEST_TEXT_FILE}" 0

    aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME}"

    rm_test_file
}

function test_append_file {
    describe "Testing append to file ..."
    local TEST_INPUT="echo ${TEST_TEXT} to ${TEST_TEXT_FILE}"

    # Write a small test file
    for x in $(seq 1 "${TEST_TEXT_FILE_LENGTH}"); do
        echo "${TEST_INPUT}"
    done > "${TEST_TEXT_FILE}"

    check_file_size "${TEST_TEXT_FILE}" $((TEST_TEXT_FILE_LENGTH * $((${#TEST_INPUT} + 1)) ))

    rm_test_file
}

function test_truncate_file {
    describe "Testing truncate file ..."
    # Write a small test file
    echo "${TEST_TEXT}" > "${TEST_TEXT_FILE}"

    # Truncate file to 0 length.  This should trigger open(path, O_RDWR | O_TRUNC...)
    : > "${TEST_TEXT_FILE}"

    check_file_size "${TEST_TEXT_FILE}" 0

    rm_test_file
}

function test_truncate_upload {
    describe "Testing truncate file for uploading ..."

    # This file size uses multipart, mix upload when uploading.
    # We will test these cases.

    rm_test_file "${BIG_FILE}"

    "${TRUNCATE_BIN}" "${BIG_FILE}" -s "${BIG_FILE_LENGTH}"

    rm_test_file "${BIG_FILE}"
}

function test_truncate_empty_file {
    describe "Testing truncate empty file ..."
    # Write an empty test file
    touch "${TEST_TEXT_FILE}"

    # Truncate the file to 1024 length
    local t_size=1024
    "${TRUNCATE_BIN}" "${TEST_TEXT_FILE}" -s "${t_size}"

    check_file_size "${TEST_TEXT_FILE}" "${t_size}"

    rm_test_file
}

function test_truncate_shrink_file {
    describe "Testing truncate shrinking large binary file ..."

    local BIG_TRUNCATE_TEST_FILE="big-truncate-test.bin"
    local t_size=$((1024 * 1024 * 32 + 64))

    dd if=/dev/urandom of="${TEMP_DIR}/${BIG_TRUNCATE_TEST_FILE}" bs=1024 count=$((1024 * 64))
    cp "${TEMP_DIR}/${BIG_TRUNCATE_TEST_FILE}" "${BIG_TRUNCATE_TEST_FILE}"

    "${TRUNCATE_BIN}" "${TEMP_DIR}/${BIG_TRUNCATE_TEST_FILE}" -s "${t_size}"
    "${TRUNCATE_BIN}" "${BIG_TRUNCATE_TEST_FILE}" -s "${t_size}"

    if ! cmp "${TEMP_DIR}/${BIG_TRUNCATE_TEST_FILE}" "${BIG_TRUNCATE_TEST_FILE}"; then
       return 1
    fi

    rm -f "${TEMP_DIR}/${BIG_TRUNCATE_TEST_FILE}"
    rm_test_file "${BIG_TRUNCATE_TEST_FILE}"
}

function test_mv_file {
    describe "Testing mv file function ..."
    # if the rename file exists, delete it
    if [ -e "${ALT_TEST_TEXT_FILE}" ]
    then
       rm "${ALT_TEST_TEXT_FILE}"
    fi

    if [ -e "${ALT_TEST_TEXT_FILE}" ]
    then
       echo "Could not delete file ${ALT_TEST_TEXT_FILE}, it still exists"
       return 1
    fi

    # create the test file again
    mk_test_file

    # save file length
    local ALT_TEXT_LENGTH; ALT_TEXT_LENGTH=$(wc -c "${TEST_TEXT_FILE}" | awk '{print $1}')

    #rename the test file
    mv "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"
    if [ ! -e "${ALT_TEST_TEXT_FILE}" ]
    then
       echo "Could not move file"
       return 1
    fi
    
    #check the renamed file content-type
    if [ -f "/etc/mime.types" ]
    then
      check_content_type "$1/${ALT_TEST_TEXT_FILE}" "text/plain"
    fi

    # Check the contents of the alt file
    local ALT_FILE_LENGTH; ALT_FILE_LENGTH=$(wc -c "${ALT_TEST_TEXT_FILE}" | awk '{print $1}')
    if [ "$ALT_FILE_LENGTH" -ne "$ALT_TEXT_LENGTH" ]
    then
       echo "moved file length is not as expected expected: $ALT_TEXT_LENGTH  got: $ALT_FILE_LENGTH"
       return 1
    fi

    # clean up
    rm_test_file "${ALT_TEST_TEXT_FILE}"
}

function test_mv_to_exist_file {
    describe "Testing mv file to exist file function ..."

    local BIG_MV_FILE_BLOCK_SIZE=$((BIG_FILE_BLOCK_SIZE + 1))

    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${BIG_FILE}"
    ../../junk_data $((BIG_MV_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${BIG_FILE}-mv"

    mv "${BIG_FILE}" "${BIG_FILE}-mv"

    rm_test_file "${BIG_FILE}-mv"
}

function test_mv_empty_directory {
    describe "Testing mv directory function ..."
    if [ -e "${TEST_DIR}" ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir

    mv "${TEST_DIR}" "${TEST_DIR}_rename"
    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       return 1
    fi

    rmdir "${TEST_DIR}_rename"
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       return 1
    fi
}

function test_mv_nonempty_directory {
    describe "Testing mv directory function ..."
    if [ -e "${TEST_DIR}" ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir

    touch "${TEST_DIR}"/file

    mv "${TEST_DIR}" "${TEST_DIR}_rename"
    if [ ! -d "${TEST_DIR}_rename" ]; then
       echo "Directory ${TEST_DIR} was not renamed"
       return 1
    fi

    rm -r "${TEST_DIR}_rename"
    if [ -e "${TEST_DIR}_rename" ]; then
       echo "Could not remove the test directory, it still exists: ${TEST_DIR}_rename"
       return 1
    fi
}

function test_redirects {
    describe "Testing redirects ..."

    mk_test_file "ABCDEF"

    local CONTENT; CONTENT=$(cat "${TEST_TEXT_FILE}")

    if [ "${CONTENT}" != "ABCDEF" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected ABCDEF"
       return 1
    fi

    echo "XYZ" > "${TEST_TEXT_FILE}"

    CONTENT=$(cat "${TEST_TEXT_FILE}")

    if [ "${CONTENT}" != "XYZ" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected XYZ"
       return 1
    fi

    echo "123456" >> "${TEST_TEXT_FILE}"

    local LINE1; LINE1=$("${SED_BIN}" -n '1,1p' "${TEST_TEXT_FILE}")
    local LINE2; LINE2=$("${SED_BIN}" -n '2,2p' "${TEST_TEXT_FILE}")

    if [ "${LINE1}" != "XYZ" ]; then
       echo "LINE1 was not as expected, got ${LINE1}, expected XYZ"
       return 1
    fi

    if [ "${LINE2}" != "123456" ]; then
       echo "LINE2 was not as expected, got ${LINE2}, expected 123456"
       return 1
    fi

    # clean up
    rm_test_file
}

function test_mkdir_rmdir {
    describe "Testing creation/removal of a directory ..."

    if [ -e "${TEST_DIR}" ]; then
       echo "Unexpected, this file/directory exists: ${TEST_DIR}"
       return 1
    fi

    mk_test_dir
    rm_test_dir
}

function test_chmod {
    describe "Testing chmod file function ..."

    # create the test file again
    mk_test_file

    local ORIGINAL_PERMISSIONS; ORIGINAL_PERMISSIONS=$(get_permissions "${TEST_TEXT_FILE}")

    chmod 777 "${TEST_TEXT_FILE}";

    # if they're the same, we have a problem.
    local CHANGED_PERMISSIONS; CHANGED_PERMISSIONS=$(get_permissions "${TEST_TEXT_FILE}")
    if [ "${CHANGED_PERMISSIONS}" = "${ORIGINAL_PERMISSIONS}" ]
    then
      echo "Could not modify ${TEST_TEXT_FILE} permissions"
      return 1
    fi

    # clean up
    rm_test_file
}

function test_chown {
    describe "Testing chown file function ..."

    # create the test file again
    mk_test_file

    local ORIGINAL_PERMISSIONS
    if [ "$(uname)" = "Darwin" ]; then
        ORIGINAL_PERMISSIONS=$(stat -f "%u:%g" "${TEST_TEXT_FILE}")
    else
        ORIGINAL_PERMISSIONS=$(stat --format=%u:%g "${TEST_TEXT_FILE}")
    fi

    # [NOTE]
    # Prevents test interruptions due to permission errors, etc.
    # If the chown command fails, an error will occur with the
    # following judgment statement. So skip the chown command error.
    # '|| true' was added due to a problem with Travis CI and MacOS
    # and ensure_diskfree option.
    #
    chown 1000:1000 "${TEST_TEXT_FILE}" || true

    # if they're the same, we have a problem.
    local CHANGED_PERMISSIONS
    if [ "$(uname)" = "Darwin" ]; then
        CHANGED_PERMISSIONS=$(stat -f "%u:%g" "${TEST_TEXT_FILE}")
    else
        CHANGED_PERMISSIONS=$(stat --format=%u:%g "${TEST_TEXT_FILE}")
    fi
    if [ "${CHANGED_PERMISSIONS}" = "${ORIGINAL_PERMISSIONS}" ]
    then
      if [ "${ORIGINAL_PERMISSIONS}" = "1000:1000" ]
      then
        echo "Could not be strict check because original file permission 1000:1000"
      else
        echo "Could not modify ${TEST_TEXT_FILE} ownership($ORIGINAL_PERMISSIONS to 1000:1000)"
        return 1
      fi
    fi

    # clean up
    rm_test_file
}

function test_list {
    describe "Testing list ..."
    mk_test_file
    mk_test_dir

    local file_list=(*)
    local file_cnt=${#file_list[@]}
    if [ "${file_cnt}" -ne 2 ]; then
        echo "Expected 2 file but got ${file_cnt}"
        echo "list results: " ${file_list[@]}
        return 1
    fi

    rm_test_file
    rm_test_dir
}

function test_remove_nonempty_directory {
    describe "Testing removing a non-empty directory ..."
    mk_test_dir
    touch "${TEST_DIR}/file"
    (
        set +o pipefail
        rmdir "${TEST_DIR}" 2>&1 | grep -q "Directory not empty"
    )
    rm "${TEST_DIR}/file"
    rm_test_dir
}

function test_external_directory_creation {
    describe "Test external directory creation ..."
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/directory/"${TEST_TEXT_FILE}"
    echo "data" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    ls directory >/dev/null 2>&1
    get_permissions directory | grep -q 750$
    ls directory
    cmp <(echo "data") directory/"${TEST_TEXT_FILE}"
    rm -f directory/"${TEST_TEXT_FILE}"
}

function test_external_modification {
    describe "Test external modification to an object ..."
    echo "old" > "${TEST_TEXT_FILE}"

    # [NOTE]
    # If the stat and file cache directory are enabled, an error will
    # occur if the unixtime(sec) value does not change.
    # If mtime(ctime/atime) when updating from the external program
    # (awscli) is the same unixtime value as immediately before, the
    # cache will be read out.
    # Therefore, we need to wait over 1 second here.
    #
    sleep 1

    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo "new new" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    cmp "${TEST_TEXT_FILE}" <(echo "new new")
    rm -f "${TEST_TEXT_FILE}"
}

function test_external_creation {
    describe "Test external creation of an object ..."
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo "data" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    # shellcheck disable=SC2009
    if ps u -p "${OSSFS_PID}" | grep -q noobj_cache; then
        [ ! -e "${TEST_TEXT_FILE}" ]
    fi
    sleep 1
    [ -e "${TEST_TEXT_FILE}" ]
    rm -f "${TEST_TEXT_FILE}"
}

function test_read_external_object() {
    describe "create objects via aws CLI and read via ossfs ..."
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo "test" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    cmp "${TEST_TEXT_FILE}" <(echo "test")
    rm -f "${TEST_TEXT_FILE}"
}

function test_read_external_dir_object() {
    describe "create directory objects via aws CLI and read via ossfs ..."
    local SUB_DIR_NAME;      SUB_DIR_NAME="subdir"
    local SUB_DIR_TEST_FILE; SUB_DIR_TEST_FILE="${SUB_DIR_NAME}/${TEST_TEXT_FILE}"
    local OBJECT_NAME;       OBJECT_NAME=$(basename "${PWD}")/"${SUB_DIR_TEST_FILE}"

    echo "test" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"

    if stat "${SUB_DIR_NAME}" | grep -q '1969-12-31[[:space:]]23:59:59[.]000000000'; then
        echo "sub directory a/c/m time is underflow(-1)."
        return 1
    fi
    rm -rf "${SUB_DIR_NAME}"
}

function test_update_metadata_external_small_object() {
    describe "update meta to small file after created file by aws cli"

    # [NOTE]
    # Use the only filename in the test to avoid being affected by noobjcache.
    #
    local TEST_FILE_EXT; TEST_FILE_EXT=$(make_random_string)
    local TEST_CHMOD_FILE="${TEST_TEXT_FILE}_chmod.${TEST_FILE_EXT}"
    local TEST_CHOWN_FILE="${TEST_TEXT_FILE}_chown.${TEST_FILE_EXT}"
    local TEST_UTIMENS_FILE="${TEST_TEXT_FILE}_utimens.${TEST_FILE_EXT}"
    local TEST_SETXATTR_FILE="${TEST_TEXT_FILE}_xattr.${TEST_FILE_EXT}"
    local TEST_RMXATTR_FILE="${TEST_TEXT_FILE}_xattr.${TEST_FILE_EXT}"

    local TEST_INPUT="TEST_STRING_IN_SMALL_FILE"

    #
    # chmod
    #
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_CHMOD_FILE}"
    echo "${TEST_INPUT}" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    chmod +x "${TEST_CHMOD_FILE}"
    cmp "${TEST_CHMOD_FILE}" <(echo "${TEST_INPUT}")

    #
    # chown
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_CHOWN_FILE}"
    echo "${TEST_INPUT}" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    chown "${UID}" "${TEST_CHOWN_FILE}"
    cmp "${TEST_CHOWN_FILE}" <(echo "${TEST_INPUT}")

    #
    # utimens
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_UTIMENS_FILE}"
    echo "${TEST_INPUT}" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    touch "${TEST_UTIMENS_FILE}"
    cmp "${TEST_UTIMENS_FILE}" <(echo "${TEST_INPUT}")

    #
    # set xattr
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_SETXATTR_FILE}"
    echo "${TEST_INPUT}" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    set_xattr key value "${TEST_SETXATTR_FILE}"
    cmp "${TEST_SETXATTR_FILE}" <(echo "${TEST_INPUT}")

    #
    # remove xattr
    #
    # "%7B%22key%22%3A%22dmFsdWU%3D%22%7D" = {"key":"value"}
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_RMXATTR_FILE}"
    echo "${TEST_INPUT}" | aws_cli s3 cp - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}" --metadata xattr=%7B%22key%22%3A%22dmFsdWU%3D%22%7D
    del_xattr key "${TEST_RMXATTR_FILE}"
    cmp "${TEST_RMXATTR_FILE}" <(echo "${TEST_INPUT}")

    rm -f "${TEST_CHMOD_FILE}"
    rm -f "${TEST_CHOWN_FILE}"
    rm -f "${TEST_UTIMENS_FILE}"
    rm -f "${TEST_SETXATTR_FILE}"
    rm -f "${TEST_RMXATTR_FILE}"
}

function test_update_metadata_external_large_object() {
    describe "update meta to large file after created file by aws cli"

    # [NOTE]
    # Use the only filename in the test to avoid being affected by noobjcache.
    #
    local TEST_FILE_EXT; TEST_FILE_EXT=$(make_random_string)
    local TEST_CHMOD_FILE="${TEST_TEXT_FILE}_chmod.${TEST_FILE_EXT}"
    local TEST_CHOWN_FILE="${TEST_TEXT_FILE}_chown.${TEST_FILE_EXT}"
    local TEST_UTIMENS_FILE="${TEST_TEXT_FILE}_utimens.${TEST_FILE_EXT}"
    local TEST_SETXATTR_FILE="${TEST_TEXT_FILE}_xattr.${TEST_FILE_EXT}"
    local TEST_RMXATTR_FILE="${TEST_TEXT_FILE}_xattr.${TEST_FILE_EXT}"

    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEMP_DIR}/${BIG_FILE}"

    #
    # chmod
    #
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_CHMOD_FILE}"
    aws_cli s3 cp "${TEMP_DIR}/${BIG_FILE}" "s3://${TEST_BUCKET_1}/${OBJECT_NAME}" --no-progress
    chmod +x "${TEST_CHMOD_FILE}"
    cmp "${TEST_CHMOD_FILE}" "${TEMP_DIR}/${BIG_FILE}"

    #
    # chown
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_CHOWN_FILE}"
    aws_cli s3 cp "${TEMP_DIR}/${BIG_FILE}" "s3://${TEST_BUCKET_1}/${OBJECT_NAME}" --no-progress
    chown "${UID}" "${TEST_CHOWN_FILE}"
    cmp "${TEST_CHOWN_FILE}" "${TEMP_DIR}/${BIG_FILE}"

    #
    # utimens
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_UTIMENS_FILE}"
    aws_cli s3 cp "${TEMP_DIR}/${BIG_FILE}" "s3://${TEST_BUCKET_1}/${OBJECT_NAME}" --no-progress
    touch "${TEST_UTIMENS_FILE}"
    cmp "${TEST_UTIMENS_FILE}" "${TEMP_DIR}/${BIG_FILE}"

    #
    # set xattr
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_SETXATTR_FILE}"
    aws_cli s3 cp "${TEMP_DIR}/${BIG_FILE}" "s3://${TEST_BUCKET_1}/${OBJECT_NAME}" --no-progress
    set_xattr key value "${TEST_SETXATTR_FILE}"
    cmp "${TEST_SETXATTR_FILE}" "${TEMP_DIR}/${BIG_FILE}"

    #
    # remove xattr
    #
    # "%7B%22key%22%3A%22dmFsdWU%3D%22%7D" = {"key":"value"}
    #
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_RMXATTR_FILE}"
    aws_cli s3 cp "${TEMP_DIR}/${BIG_FILE}" "s3://${TEST_BUCKET_1}/${OBJECT_NAME}" --no-progress --metadata xattr=%7B%22key%22%3A%22dmFsdWU%3D%22%7D
    del_xattr key "${TEST_RMXATTR_FILE}"
    cmp "${TEST_RMXATTR_FILE}" "${TEMP_DIR}/${BIG_FILE}"

    rm -f "${TEMP_DIR}/${BIG_FILE}"
    rm -f "${TEST_CHMOD_FILE}"
    rm -f "${TEST_CHOWN_FILE}"
    rm -f "${TEST_UTIMENS_FILE}"
    rm -f "${TEST_SETXATTR_FILE}"
    rm -f "${TEST_RMXATTR_FILE}"
}

function test_rename_before_close {
    describe "Testing rename before close ..."

    # shellcheck disable=SC2094
    (
        echo foo
        mv "${TEST_TEXT_FILE}" "${TEST_TEXT_FILE}.new"
    ) > "${TEST_TEXT_FILE}"

    if ! cmp <(echo "foo") "${TEST_TEXT_FILE}.new"; then
        echo "rename before close failed"
        return 1
    fi

    rm_test_file "${TEST_TEXT_FILE}.new"
    rm -f "${TEST_TEXT_FILE}"
}

function test_multipart_upload {
    describe "Testing multi-part upload ..."

    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEMP_DIR}/${BIG_FILE}"
    dd if="${TEMP_DIR}/${BIG_FILE}" of="${BIG_FILE}" bs="${BIG_FILE_BLOCK_SIZE}" count="${BIG_FILE_COUNT}"

    # Verify contents of file
    echo "Comparing test file"
    if ! cmp "${TEMP_DIR}/${BIG_FILE}" "${BIG_FILE}"
    then
       return 1
    fi

    rm -f "${TEMP_DIR}/${BIG_FILE}"
    rm_test_file "${BIG_FILE}"
}

function test_multipart_copy {
    describe "Testing multi-part copy ..."

    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEMP_DIR}/${BIG_FILE}"
    dd if="${TEMP_DIR}/${BIG_FILE}" of="${BIG_FILE}" bs="${BIG_FILE_BLOCK_SIZE}" count="${BIG_FILE_COUNT}"
    mv "${BIG_FILE}" "${BIG_FILE}-copy"

    # Verify contents of file
    echo "Comparing test file"
    if ! cmp "${TEMP_DIR}/${BIG_FILE}" "${BIG_FILE}-copy"
    then
       return 1
    fi

    #check the renamed file content-type
    check_content_type "$1/${BIG_FILE}-copy" "application/octet-stream"

    rm -f "${TEMP_DIR}/${BIG_FILE}"
    rm_test_file "${BIG_FILE}-copy"
}

function test_multipart_mix {
    describe "Testing multi-part mix ..."

    if [ "$(uname)" = "Darwin" ]; then
       cat /dev/null > "${BIG_FILE}"
    fi
    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEMP_DIR}/${BIG_FILE}"
    dd if="${TEMP_DIR}/${BIG_FILE}" of="${BIG_FILE}" bs="${BIG_FILE_BLOCK_SIZE}" count="${BIG_FILE_COUNT}"

    # (1) Edit the middle of an existing file
    #     modify directly(seek 7.5MB offset)
    #     In the case of nomultipart and nocopyapi,
    #     it makes no sense, but copying files is because it leaves no cache.
    #
    cp "${TEMP_DIR}/${BIG_FILE}" "${TEMP_DIR}/${BIG_FILE}-mix"
    cp "${BIG_FILE}" "${BIG_FILE}-mix"

    local MODIFY_START_BLOCK=$((15*1024*1024/2/4))
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}-mix" bs=4 count=4 seek="${MODIFY_START_BLOCK}" conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${TEMP_DIR}/${BIG_FILE}-mix" bs=4 count=4 seek="${MODIFY_START_BLOCK}" conv=notrunc

    # Verify contents of file
    echo "Comparing test file (1)"
    if ! cmp "${TEMP_DIR}/${BIG_FILE}-mix" "${BIG_FILE}-mix"
    then
       return 1
    fi

    # (2) Write to an area larger than the size of the existing file
    #     modify directly(over file end offset)
    #
    cp "${TEMP_DIR}/${BIG_FILE}" "${TEMP_DIR}/${BIG_FILE}-mix"
    cp "${BIG_FILE}" "${BIG_FILE}-mix"

    local OVER_FILE_BLOCK_POS=$((26*1024*1024/4))
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}-mix" bs=4 count=4 seek="${OVER_FILE_BLOCK_POS}" conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${TEMP_DIR}/${BIG_FILE}-mix" bs=4 count=4 seek="${OVER_FILE_BLOCK_POS}" conv=notrunc

    # Verify contents of file
    echo "Comparing test file (2)"
    if ! cmp "${TEMP_DIR}/${BIG_FILE}-mix" "${BIG_FILE}-mix"
    then
       return 1
    fi

    # (3) Writing from the 0th byte
    #
    cp "${TEMP_DIR}/${BIG_FILE}" "${TEMP_DIR}/${BIG_FILE}-mix"
    cp "${BIG_FILE}" "${BIG_FILE}-mix"

    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}-mix" bs=4 count=4 seek=0 conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${TEMP_DIR}/${BIG_FILE}-mix" bs=4 count=4 seek=0 conv=notrunc

    # Verify contents of file
    echo "Comparing test file (3)"
    if ! cmp "${TEMP_DIR}/${BIG_FILE}-mix" "${BIG_FILE}-mix"
    then
       return 1
    fi

    # (4) Write to the area within 5MB from the top
    #     modify directly(seek 1MB offset)
    #
    cp "${TEMP_DIR}/${BIG_FILE}" "${TEMP_DIR}/${BIG_FILE}-mix"
    cp "${BIG_FILE}" "${BIG_FILE}-mix"

    local MODIFY_START_BLOCK=$((1*1024*1024))
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}-mix" bs=4 count=4 seek="${MODIFY_START_BLOCK}" conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${TEMP_DIR}/${BIG_FILE}-mix" bs=4 count=4 seek="${MODIFY_START_BLOCK}" conv=notrunc

    # Verify contents of file
    echo "Comparing test file (4)"
    if ! cmp "${TEMP_DIR}/${BIG_FILE}-mix" "${BIG_FILE}-mix"
    then
       return 1
    fi

    rm -f "${TEMP_DIR}/${BIG_FILE}"
    rm -f "${TEMP_DIR}/${BIG_FILE}-mix"
    rm_test_file "${BIG_FILE}"
    rm_test_file "${BIG_FILE}-mix"
}

function test_utimens_during_multipart {
    describe "Testing utimens calling during multipart copy ..."

    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEMP_DIR}/${BIG_FILE}"

    cp "${TEMP_DIR}/${BIG_FILE}" "${BIG_FILE}"

    # The second copy of the "-p" option calls utimens during multipart upload.
    cp -p "${TEMP_DIR}/${BIG_FILE}" "${BIG_FILE}"

    rm -f "${TEMP_DIR}/${BIG_FILE}"
    rm_test_file "${BIG_FILE}"
}

function test_special_characters {
    describe "Testing special characters ..."

    (
        set +o pipefail
        # shellcheck disable=SC2010
        ls 'special' 2>&1 | grep -q 'No such file or directory'
        # shellcheck disable=SC2010
        ls 'special?' 2>&1 | grep -q 'No such file or directory'
        # shellcheck disable=SC2010
        ls 'special*' 2>&1 | grep -q 'No such file or directory'
        # shellcheck disable=SC2010
        ls 'special~' 2>&1 | grep -q 'No such file or directory'
        # shellcheck disable=SC2010
        ls 'specialµ' 2>&1 | grep -q 'No such file or directory'
    )

    mkdir "TOYOTA TRUCK 8.2.2"
    rm -rf "TOYOTA TRUCK 8.2.2"
}

function test_hardlink {
    describe "Testing hardlinks ..."

    rm -f "${TEST_TEXT_FILE}"
    rm -f "${ALT_TEST_TEXT_FILE}"
    echo foo > "${TEST_TEXT_FILE}"

    (
        set +o pipefail
        ln "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}" 2>&1 | grep -q 'Operation not supported'
    )

    rm_test_file
    rm_test_file "${ALT_TEST_TEXT_FILE}"
}

function test_symlink {
    describe "Testing symlinks ..."

    rm -f "${TEST_TEXT_FILE}"
    rm -f "${ALT_TEST_TEXT_FILE}"
    echo foo > "${TEST_TEXT_FILE}"

    ln -s "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"
    cmp "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"

    rm -f "${TEST_TEXT_FILE}"

    [ -L "${ALT_TEST_TEXT_FILE}" ]
    [ ! -f "${ALT_TEST_TEXT_FILE}" ]

    rm -f "${ALT_TEST_TEXT_FILE}"
}

function test_extended_attributes {
    describe "Testing extended attributes ..."

    rm -f "${TEST_TEXT_FILE}"
    touch "${TEST_TEXT_FILE}"

    # set value
    set_xattr key1 value1 "${TEST_TEXT_FILE}"
    get_xattr key1 "${TEST_TEXT_FILE}" | grep -q '^value1$'

    # append value
    set_xattr key2 value2 "${TEST_TEXT_FILE}"
    get_xattr key1 "${TEST_TEXT_FILE}" | grep -q '^value1$'
    get_xattr key2 "${TEST_TEXT_FILE}" | grep -q '^value2$'

    # remove value
    del_xattr key1 "${TEST_TEXT_FILE}"
    get_xattr key1 "${TEST_TEXT_FILE}" && exit 1
    get_xattr key2 "${TEST_TEXT_FILE}" | grep -q '^value2$'

    rm_test_file
}

function test_mtime_file {
    describe "Testing mtime preservation function ..."

    # if the rename file exists, delete it
    if [ -e "${ALT_TEST_TEXT_FILE}" ] || [ -L "${ALT_TEST_TEXT_FILE}" ]
    then
       rm "${ALT_TEST_TEXT_FILE}"
    fi

    if [ -e "${ALT_TEST_TEXT_FILE}" ]
    then
       echo "Could not delete file ${ALT_TEST_TEXT_FILE}, it still exists"
       return 1
    fi

    # create the test file again
    mk_test_file
    sleep 1 # allow for some time to pass to compare the timestamps between test & alt

    #copy the test file with preserve mode
    cp -p "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"
    local testmtime; testmtime=$(get_mtime "${TEST_TEXT_FILE}")
    local altmtime;  altmtime=$(get_mtime "${ALT_TEST_TEXT_FILE}")
    if [ "$testmtime" -ne "$altmtime" ]
    then
       echo "File times do not match:  $testmtime != $altmtime"
       return 1
    fi

    rm_test_file
    rm_test_file "${ALT_TEST_TEXT_FILE}"
}

# [NOTE]
# If it mounted with relatime or noatime options , the "touch -a"
# command may not update the atime.
# In ubuntu:xenial, atime was updated even if relatime was granted.
# However, it was not updated in bionic/focal.
# We can probably update atime by explicitly specifying the strictatime
# option and running the "touch -a" command. However, the strictatime
# option cannot be set.
# Therefore, if the relatime option is set, the test with the "touch -a"
# command is bypassed.
# We do not know why atime is not updated may or not be affected by
# these options.(can't say for sure)
# However, if atime has not been updated, the s3fs_utimens entry point
# will not be called from FUSE library. We added this bypass because
# the test became unstable.
#
function test_update_time_chmod() {
    describe "Testing update time function chmod..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # chmod -> update only ctime
    #
    chmod +x "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "chmod expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi
    rm_test_file
}

function test_update_time_chown() {
    describe "Testing update time function chown..."

    #
    # chown -> update only ctime
    #
    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    chown $UID "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "chown expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi
    rm_test_file
}

function test_update_time_xattr() {
    describe "Testing update time function set_xattr..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # set_xattr -> update only ctime
    #
    set_xattr key value "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "set_xattr expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi
    rm_test_file
}

function test_update_time_touch() {
    describe "Testing update time function touch..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # touch -> update ctime/atime/mtime
    #
    touch "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")
    if [ "${base_atime}" -eq "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -eq "${mtime}" ]; then
       echo "touch expected updated ctime: $base_ctime != $ctime, mtime: $base_mtime != $mtime, atime: $base_atime != $atime"
       return 1
    fi
    rm_test_file
}

function test_update_time_touch_a() {
    describe "Testing update time function touch -a..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # "touch -a" -> update ctime/atime, not update mtime
    #
    touch -a "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")
    if [ "${base_atime}" -eq "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
        echo "touch with -a option expected updated ctime: $base_ctime != $ctime, atime: $base_atime != $atime and same mtime: $base_mtime == $mtime"
        return 1
    fi
    rm_test_file
}

function test_update_time_append() {
    describe "Testing update time function append..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # append -> update ctime/mtime, not update atime
    #
    echo foo >> "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -eq "${mtime}" ]; then
        echo "append expected updated ctime: $base_ctime != $ctime, mtime: $base_mtime != $mtime and same atime: $base_atime == $atime"
        return 1
    fi
    rm_test_file
}

function test_update_time_cp_p() {
    describe "Testing update time function cp -p..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # cp -p -> update ctime, not update atime/mtime
    #
    local TIME_TEST_TEXT_FILE=test-ossfs-time.txt
    cp -p "${TEST_TEXT_FILE}" "${TIME_TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TIME_TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TIME_TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TIME_TEST_TEXT_FILE}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "cp with -p option expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi
}

function test_update_time_mv() {
    describe "Testing update time function mv..."

    local t0=1000000000  # 9 September 2001
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=${t0},ctime=${t0},mtime=${t0}" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    #
    # mv -> update ctime, not update atime/mtime
    #
    local TIME2_TEST_TEXT_FILE=test-ossfs-time2.txt
    mv "${TEST_TEXT_FILE}" "${TIME2_TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TIME2_TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TIME2_TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TIME2_TEST_TEXT_FILE}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "mv expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi

    rm_test_file "${TIME_TEST_TEXT_FILE}"
    rm_test_file "${TIME2_TEST_TEXT_FILE}"
}

# [NOTE]
# See the description of test_update_time () for notes about the
# "touch -a" command and atime.
#
function test_update_directory_time_chmod() {
    describe "Testing update time for directory mv..."

    #
    # create the directory and sub-directory and a file in directory
    #
    local t0=1000000000  # 9 September 2001
    local DIRECTORY_NAME; DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    aws_cli s3api put-object --content-type="application/x-directory" --metadata="atime=${t0},ctime=${t0},mtime=${t0}" --bucket "${TEST_BUCKET_1}" --key "$DIRECTORY_NAME/"

    local base_atime; base_atime=$(get_atime "${TEST_DIR}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_DIR}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_DIR}")

    #
    # chmod -> update only ctime
    #
    chmod 0777 "${TEST_DIR}"
    local atime; atime=$(get_atime "${TEST_DIR}")
    local ctime; ctime=$(get_ctime "${TEST_DIR}")
    local mtime; mtime=$(get_mtime "${TEST_DIR}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "chmod expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi

    rm -rf "${TEST_DIR}"
}

function test_update_directory_time_chown {
    describe "Testing update time for directory chown..."

    local t0=1000000000  # 9 September 2001
    local DIRECTORY_NAME; DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    aws_cli s3api put-object --content-type="application/x-directory" --metadata="atime=${t0},ctime=${t0},mtime=${t0}" --bucket "${TEST_BUCKET_1}" --key "$DIRECTORY_NAME/"

    local base_atime; base_atime=$(get_atime "${TEST_DIR}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_DIR}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_DIR}")
    #
    # chown -> update only ctime
    #
    chown $UID "${TEST_DIR}"
    local atime; atime=$(get_atime "${TEST_DIR}")
    local ctime; ctime=$(get_ctime "${TEST_DIR}")
    local mtime; mtime=$(get_mtime "${TEST_DIR}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "chown expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi

    rm -rf "${TEST_DIR}"
}

function test_update_directory_time_set_xattr {
    describe "Testing update time for directory set_xattr..."

    local t0=1000000000  # 9 September 2001
    local DIRECTORY_NAME; DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    aws_cli s3api put-object --content-type="application/x-directory" --metadata="atime=${t0},ctime=${t0},mtime=${t0}" --bucket "${TEST_BUCKET_1}" --key "$DIRECTORY_NAME/"

    local base_atime; base_atime=$(get_atime "${TEST_DIR}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_DIR}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_DIR}")
    #
    # set_xattr -> update only ctime
    #
    set_xattr key value "${TEST_DIR}"
    local atime; atime=$(get_atime "${TEST_DIR}")
    local ctime; ctime=$(get_ctime "${TEST_DIR}")
    local mtime; mtime=$(get_mtime "${TEST_DIR}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "set_xattr expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi

    rm -rf "${TEST_DIR}"
}

function test_update_directory_time_touch {
    describe "Testing update time for directory touch..."

    local t0=1000000000  # 9 September 2001
    local DIRECTORY_NAME; DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    aws_cli s3api put-object --content-type="application/x-directory" --metadata="atime=${t0},ctime=${t0},mtime=${t0}" --bucket "${TEST_BUCKET_1}" --key "$DIRECTORY_NAME/"

    local base_atime; base_atime=$(get_atime "${TEST_DIR}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_DIR}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_DIR}")
    #
    # touch -> update ctime/atime/mtime
    #
    touch "${TEST_DIR}"
    local atime; atime=$(get_atime "${TEST_DIR}")
    local ctime; ctime=$(get_ctime "${TEST_DIR}")
    local mtime; mtime=$(get_mtime "${TEST_DIR}")
    if [ "${base_atime}" -eq "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -eq "${mtime}" ]; then
       echo "touch expected updated ctime: $base_ctime != $ctime, mtime: $base_mtime != $mtime, atime: $base_atime != $atime"
       return 1
    fi

    rm -rf "${TEST_DIR}"
}

function test_update_directory_time_touch_a {
    describe "Testing update time for directory touch -a..."

    local t0=1000000000  # 9 September 2001
    local DIRECTORY_NAME; DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    aws_cli s3api put-object --content-type="application/x-directory" --metadata="atime=${t0},ctime=${t0},mtime=${t0}" --bucket "${TEST_BUCKET_1}" --key "$DIRECTORY_NAME/"

    local base_atime; base_atime=$(get_atime "${TEST_DIR}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_DIR}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_DIR}")
    #
    # "touch -a" -> update ctime/atime, not update mtime
    #
    touch -a "${TEST_DIR}"
    local atime; atime=$(get_atime "${TEST_DIR}")
    local ctime; ctime=$(get_ctime "${TEST_DIR}")
    local mtime; mtime=$(get_mtime "${TEST_DIR}")
    if [ "${base_atime}" -eq "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
        echo "touch with -a option expected updated ctime: $base_ctime != $ctime, atime: $base_atime != $atime and same mtime: $base_mtime == $mtime"
        return 1
    fi

    rm -rf "${TEST_DIR}"
}

function test_update_directory_time_subdir() {
    describe "Testing update time for directory subdirectory..."

    local TIME_TEST_SUBDIR="${TEST_DIR}/testsubdir"
    local TIME_TEST_FILE_INDIR="${TEST_DIR}/testfile"
    mk_test_dir
    mkdir "${TIME_TEST_SUBDIR}"
    touch "${TIME_TEST_FILE_INDIR}"
    # TODO: remove sleep after improving AWS CLI speed
    sleep 1

    local base_atime;    base_atime=$(get_atime "${TEST_DIR}")
    local base_ctime;    base_ctime=$(get_ctime "${TEST_DIR}")
    local base_mtime;    base_mtime=$(get_mtime "${TEST_DIR}")
    local subdir_atime;  subdir_atime=$(get_atime "${TIME_TEST_SUBDIR}")
    local subdir_ctime;  subdir_ctime=$(get_ctime "${TIME_TEST_SUBDIR}")
    local subdir_mtime;  subdir_mtime=$(get_mtime "${TIME_TEST_SUBDIR}")
    local subfile_atime; subfile_atime=$(get_atime "${TIME_TEST_FILE_INDIR}")
    local subfile_ctime; subfile_ctime=$(get_ctime "${TIME_TEST_FILE_INDIR}")
    local subfile_mtime; subfile_mtime=$(get_mtime "${TIME_TEST_FILE_INDIR}")
    #
    # mv -> update ctime, not update atime/mtime for target directory
    #       not update any for sub-directory and a file
    #
    local TIME_TEST_DIR=timetestdir
    local TIME2_TEST_SUBDIR="${TIME_TEST_DIR}/testsubdir"
    local TIME2_TEST_FILE_INDIR="${TIME_TEST_DIR}/testfile"
    mv "${TEST_DIR}" "${TIME_TEST_DIR}"
    local atime; atime=$(get_atime "${TIME_TEST_DIR}")
    local ctime; ctime=$(get_ctime "${TIME_TEST_DIR}")
    local mtime; mtime=$(get_mtime "${TIME_TEST_DIR}")
    if [ "${base_atime}" -ne "${atime}" ] || [ "${base_ctime}" -eq "${ctime}" ] || [ "${base_mtime}" -ne "${mtime}" ]; then
       echo "mv expected updated ctime: $base_ctime != $ctime and same mtime: $base_mtime == $mtime, atime: $base_atime == $atime"
       return 1
    fi
    atime=$(get_atime "${TIME2_TEST_SUBDIR}")
    ctime=$(get_ctime "${TIME2_TEST_SUBDIR}")
    mtime=$(get_mtime "${TIME2_TEST_SUBDIR}")
    if [ "${subdir_atime}" -ne "${atime}" ] || [ "${subdir_ctime}" -ne "${ctime}" ] || [ "${subdir_mtime}" -ne "${mtime}" ]; then
       echo "mv for sub-directory expected same ctime: $subdir_ctime == $ctime, mtime: $subdir_mtime == $mtime, atime: $subdir_atime == $atime"
       return 1
    fi
    atime=$(get_atime "${TIME2_TEST_FILE_INDIR}")
    ctime=$(get_ctime "${TIME2_TEST_FILE_INDIR}")
    mtime=$(get_mtime "${TIME2_TEST_FILE_INDIR}")
    if [ "${subfile_atime}" -ne "${atime}" ] || [ "${subfile_ctime}" -ne "${ctime}" ] || [ "${subfile_mtime}" -ne "${mtime}" ]; then
       echo "mv for a file in directory expected same ctime: $subfile_ctime == $ctime, mtime: $subfile_mtime == $mtime, atime: $subfile_atime == $atime"
       return 1
    fi

    rm -rf "${TIME_TEST_SUBDIR}"
    rm -rf "${TIME_TEST_DIR}"
    rm -rf "${TEST_DIR}"
}

# [NOTE]
# This test changes the file mode while creating/editing a new file,
# and finally closes it.
# Test with the sed command as it occurs when in place mode of the sed
# command. (If trying it with a standard C function(and shell script),
# it will be not the same result of sed, so sed is used.)
#
function test_update_chmod_opened_file() {
    describe "Testing create, modify the file by sed in place mode"

    # test file
    local BEFORE_STRING_DATA; BEFORE_STRING_DATA="sed in place test : BEFORE DATA"
    local AFTER_STRING_DATA;  AFTER_STRING_DATA="sed in place test : AFTER DATA"
    echo "${BEFORE_STRING_DATA}" > "${TEST_TEXT_FILE}"

    # sed in place
    sed -i -e 's/BEFORE DATA/AFTER DATA/g' "${TEST_TEXT_FILE}"

    # compare result
    local RESULT_STRING; RESULT_STRING=$(cat "${TEST_TEXT_FILE}")

    if [ -z "${RESULT_STRING}" ] || [ "${RESULT_STRING}" != "${AFTER_STRING_DATA}" ]; then
       echo "the file conversion by sed in place command failed."
       return 1
    fi

    # clean up
    rm_test_file "${ALT_TEST_TEXT_FILE}"
}

function test_rm_rf_dir {
   describe "Test that rm -rf will remove directory with contents ..."
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
   describe "Test simple copy ..."

   dd if=/dev/urandom of=/tmp/simple_file bs=1024 count=1
   cp /tmp/simple_file copied_simple_file
   cmp /tmp/simple_file copied_simple_file

   rm_test_file /tmp/simple_file
   rm_test_file copied_simple_file
}

function test_write_after_seek_ahead {
   describe "Test writes succeed after a seek ahead ..."
   dd if=/dev/zero of=testfile seek=1 count=1 bs=1024
   rm_test_file testfile
}

function test_overwrite_existing_file_range {
    describe "Test overwrite range succeeds ..."
    dd if=<(seq 1000) of="${TEST_TEXT_FILE}"
    dd if=/dev/zero of="${TEST_TEXT_FILE}" seek=1 count=1 bs=1024 conv=notrunc
    cmp "${TEST_TEXT_FILE}" <(
        seq 1000 | head -c 1024
        dd if=/dev/zero count=1 bs=1024
        seq 1000 | tail -c +2049
    )
    rm_test_file
}

function test_concurrent_directory_updates {
    describe "Test concurrent updates to a directory ..."
    for i in $(seq 5); do
        echo foo > "${i}"
    done
    for _ in $(seq 10); do
        for i in $(seq 5); do
            local file
            # shellcheck disable=SC2012,SC2046
            file=$(ls $(seq 5) | "${SED_BIN}" -n "$((RANDOM % 5 + 1))p")
            cat "${file}" >/dev/null || true
            rm -f "${file}"
            echo "foo" > "${file}" || true
        done &
    done
    wait
    # shellcheck disable=SC2046
    rm -f $(seq 5)
}

function test_concurrent_reads {
    describe "Test concurrent reads from a file ..."
    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEST_TEXT_FILE}"
    for _ in $(seq 10); do
        dd if="${TEST_TEXT_FILE}" of=/dev/null seek=$((RANDOM % BIG_FILE_LENGTH)) count=16 bs=1024 &
    done
    wait
    rm_test_file
}

function test_concurrent_writes {
    describe "Test concurrent writes to a file ..."
    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${TEST_TEXT_FILE}"
    for _ in $(seq 10); do
        dd if=/dev/zero of="${TEST_TEXT_FILE}" seek=$((RANDOM % BIG_FILE_LENGTH)) count=16 bs=1024 conv=notrunc &
    done
    wait
    rm_test_file
}

function test_open_second_fd {
    describe "read from an open fd ..."
    rm_test_file second_fd_file

    local RESULT
    # shellcheck disable=SC2094
    RESULT=$( (echo foo ; wc -c < second_fd_file >&2) 2>& 1>second_fd_file)
    if [ "${RESULT}" -ne 4 ]; then
        echo "size mismatch, expected: 4, was: ${RESULT}"
        return 1
    fi
    rm_test_file second_fd_file
}

function test_write_multiple_offsets {
    describe "test writing to multiple offsets ..."
    ../../write_multiblock -f "${TEST_TEXT_FILE}" -p "1024:1" -p "$((16 * 1024 * 1024)):1" -p "$((18 * 1024 * 1024)):1"
    rm_test_file "${TEST_TEXT_FILE}"
}

function test_write_multiple_offsets_backwards {
    describe "test writing to multiple offsets ..."
    ../../write_multiblock -f "${TEST_TEXT_FILE}" -p "$((20 * 1024 * 1024 + 1)):1" -p "$((10 * 1024 * 1024)):1"
    rm_test_file "${TEST_TEXT_FILE}"
}

function test_clean_up_cache() {
    describe "Test clean up cache ..."

    local dir="many_files"
    local count=25
    mkdir -p "${dir}"

    for x in $(seq "${count}"); do
        ../../junk_data 10485760 > "${dir}"/file-"${x}"
    done

    local file_list=("${dir}"/*);
    local file_cnt="${#file_list[@]}"
    if [ "${file_cnt}" != "${count}" ]; then
        echo "Expected $count files but got ${file_cnt}"
        rm -rf "${dir}"
        return 1
    fi
    local CACHE_DISK_AVAIL_SIZE; CACHE_DISK_AVAIL_SIZE=$(get_disk_avail_size "${CACHE_DIR}")
    if [ "${CACHE_DISK_AVAIL_SIZE}" -lt "${ENSURE_DISKFREE_SIZE}" ];then
        echo "Cache disk avail size:${CACHE_DISK_AVAIL_SIZE} less than ensure_diskfree size:${ENSURE_DISKFREE_SIZE}"
        rm -rf "${dir}"
        return 1
    fi
    rm -rf "${dir}"
}

function test_content_type() {
    describe "Test Content-Type detection ..."

    local DIR_NAME; DIR_NAME=$(basename "${PWD}")

    touch "test.txt"
    local CONTENT_TYPE; CONTENT_TYPE=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${DIR_NAME}/test.txt" | grep "ContentType")
    if ! echo "${CONTENT_TYPE}" | grep -q "text/plain"; then
        echo "Unexpected Content-Type: ${CONTENT_TYPE}"
        return 1;
    fi

    touch "test.jpg"
    CONTENT_TYPE=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${DIR_NAME}/test.jpg" | grep "ContentType")
    if ! echo "${CONTENT_TYPE}" | grep -q "image/jpeg"; then
        echo "Unexpected Content-Type: ${CONTENT_TYPE}"
        return 1;
    fi

    touch "test.bin"
    CONTENT_TYPE=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${DIR_NAME}/test.bin" | grep "ContentType")
    if ! echo "${CONTENT_TYPE}" | grep -q "application/octet-stream"; then
        echo "Unexpected Content-Type: ${CONTENT_TYPE}"
        return 1;
    fi

    mkdir "test.dir"
    CONTENT_TYPE=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${DIR_NAME}/test.dir/" | grep "ContentType")
    if ! echo "${CONTENT_TYPE}" | grep -q "application/x-directory"; then
        echo "Unexpected Content-Type: ${CONTENT_TYPE}"
        return 1;
    fi

    rm -f test.txt
    rm -f test.jpg
    rm -f test.bin
    rm -rf test.dir
}

# create more files than -o max_stat_cache_size
function test_truncate_cache() {
    describe "Test make cache files over max cache file size ..."

    for dir in $(seq 2); do
        mkdir "${dir}"
        for file in $(seq 75); do
            touch "${dir}/${file}"
        done
        ls "${dir}" > /dev/null
    done

    # shellcheck disable=SC2046
    rm -rf $(seq 2)
}

function test_cache_file_stat() {
    describe "Test cache file stat ..."

    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${BIG_FILE}"

    #
    # The first argument of the script is "testrun-<random>" the directory name.
    #
    local CACHE_TESTRUN_DIR=$1

    #
    # get cache file inode number
    #
    local CACHE_FILE_INODE
    # shellcheck disable=SC2012
    CACHE_FILE_INODE=$(ls -i "${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${BIG_FILE}" 2>/dev/null | awk '{print $1}')
    if [ -z "${CACHE_FILE_INODE}" ]; then
        echo "Not found cache file or failed to get inode: ${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${BIG_FILE}"
        return 1;
    fi

    #
    # get lines from cache stat file
    #
    local CACHE_FILE_STAT_LINE_1; CACHE_FILE_STAT_LINE_1=$("${SED_BIN}" -n 1p "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}")
    local CACHE_FILE_STAT_LINE_2; CACHE_FILE_STAT_LINE_2=$("${SED_BIN}" -n 2p "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}")
    if [ -z "${CACHE_FILE_STAT_LINE_1}" ] || [ -z "${CACHE_FILE_STAT_LINE_2}" ]; then
        echo "could not get first or second line from cache file stat: ${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}"
        return 1;
    fi

    #
    # compare
    #
    if [ "${CACHE_FILE_STAT_LINE_1}" != "${CACHE_FILE_INODE}:${BIG_FILE_LENGTH}" ]; then
        echo "first line(cache file stat) is different: \"${CACHE_FILE_STAT_LINE_1}\" != \"${CACHE_FILE_INODE}:${BIG_FILE_LENGTH}\""
        return 1;
    fi
    if [ "${CACHE_FILE_STAT_LINE_2}" != "0:${BIG_FILE_LENGTH}:1:0" ]; then
        echo "last line(cache file stat) is different: \"${CACHE_FILE_STAT_LINE_2}\" != \"0:${BIG_FILE_LENGTH}:1:0\""
        return 1;
    fi

    #
    # remove cache files directly
    #
    rm -f "${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${BIG_FILE}"
    rm -f "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}"

    #
    # write a byte into the middle(not the boundary) of the file
    #
    local CHECK_UPLOAD_OFFSET=$((10 * 1024 * 1024 + 17))
    dd if=/dev/urandom of="${BIG_FILE}" bs=1 count=1 seek="${CHECK_UPLOAD_OFFSET}" conv=notrunc

    #
    # get cache file inode number
    #
    # shellcheck disable=SC2012
    CACHE_FILE_INODE=$(ls -i "${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${BIG_FILE}" 2>/dev/null | awk '{print $1}')
    if [ -z "${CACHE_FILE_INODE}" ]; then
        echo "Not found cache file or failed to get inode: ${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${BIG_FILE}"
        return 1;
    fi

    #
    # get lines from cache stat file
    #
    CACHE_FILE_STAT_LINE_1=$("${SED_BIN}" -n 1p "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}")
    local CACHE_FILE_STAT_LINE_E; CACHE_FILE_STAT_LINE_E=$(tail -1 "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}" 2>/dev/null)
    if [ -z "${CACHE_FILE_STAT_LINE_1}" ] || [ -z "${CACHE_FILE_STAT_LINE_E}" ]; then
        echo "could not get first or end line from cache file stat: ${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${BIG_FILE}"
        return 1;
    fi

    #
    # check first and cache file length from last line
    #
    # we should check all stat lines, but there are cases where the value
    # differs depending on the processing system etc., then the cache file
    # size is calculated and compared.
    #
    local CACHE_LAST_OFFSET; CACHE_LAST_OFFSET=$(echo "${CACHE_FILE_STAT_LINE_E}" | cut -d ":" -f1)
    local CACHE_LAST_SIZE;   CACHE_LAST_SIZE=$(echo "${CACHE_FILE_STAT_LINE_E}" | cut -d ":" -f2)
    local CACHE_TOTAL_SIZE=$((CACHE_LAST_OFFSET + CACHE_LAST_SIZE))

    if [ "${CACHE_FILE_STAT_LINE_1}" != "${CACHE_FILE_INODE}:${BIG_FILE_LENGTH}" ]; then
        echo "first line(cache file stat) is different: \"${CACHE_FILE_STAT_LINE_1}\" != \"${CACHE_FILE_INODE}:${BIG_FILE_LENGTH}\""
        return 1;
    fi
    if [ "${BIG_FILE_LENGTH}" -ne "${CACHE_TOTAL_SIZE}" ]; then
        echo "the file size indicated by the cache stat file is different: \"${BIG_FILE_LENGTH}\" != \"${CACHE_TOTAL_SIZE}\""
        return 1;
    fi

    rm_test_file "${BIG_FILE}"
}

function test_zero_cache_file_stat() {
    describe "Test zero byte cache file stat ..."

    rm_test_file "${TEST_TEXT_FILE}"

    #
    # create empty file
    #
    touch "${TEST_TEXT_FILE}"

    #
    # The first argument of the script is "testrun-<random>" the directory name.
    #
    local CACHE_TESTRUN_DIR=$1

    # [NOTE]
    # The stat file is a one-line text file, expecting for "<inode>:0"(ex. "4543937: 0").
    #
    if ! head -1 "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${TEST_TEXT_FILE}" 2>/dev/null | grep -q ':0$' 2>/dev/null; then
        echo "The cache file stat after creating an empty file is incorrect : ${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${TEST_TEXT_FILE}"
        return 1;
    fi
    rm_test_file "${TEST_TEXT_FILE}"
}

function test_upload_sparsefile {
    describe "Testing upload sparse file ..."

    rm_test_file "${BIG_FILE}"
    rm -f "${TEMP_DIR}/${BIG_FILE}"

    #
    # Make all HOLE file
    #
    "${TRUNCATE_BIN}" "${BIG_FILE}" -s "${BIG_FILE_LENGTH}"

    #
    # Write some bytes to ABOUT middle in the file
    # (Dare to remove the block breaks)
    #
    local WRITE_POS=$((BIG_FILE_LENGTH / 2 - 128))
    echo -n "0123456789ABCDEF" | dd of="${TEMP_DIR}/${BIG_FILE}" bs=1 count=16 seek="${WRITE_POS}" conv=notrunc

    #
    # copy(upload) the file
    #
    cp "${TEMP_DIR}/${BIG_FILE}" "${BIG_FILE}"

    #
    # check
    #
    cmp "${TEMP_DIR}/${BIG_FILE}" "${BIG_FILE}"

    rm_test_file "${BIG_FILE}"
    rm -f "${TEMP_DIR}/${BIG_FILE}"
}

function test_mix_upload_entities() {
    describe "Testing upload sparse files ..."

    #
    # Make test file
    #
    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${BIG_FILE}"

    #
    # If the cache option is enabled, delete the cache of uploaded files.
    #
    if [ -f "${CACHE_DIR}/${TEST_BUCKET_1}/${BIG_FILE}" ]; then
        rm -f "${CACHE_DIR}/${TEST_BUCKET_1}/${BIG_FILE}"
    fi
    if [ -f "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${BIG_FILE}" ]; then
        rm -f "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${BIG_FILE}"
    fi

    #
    # Do a partial write to the file.
    #
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}" bs=1 count=16 seek=0 conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}" bs=1 count=16 seek=8192 conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}" bs=1 count=16 seek=1073152 conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}" bs=1 count=16 seek=26214400 conv=notrunc
    echo -n "0123456789ABCDEF" | dd of="${BIG_FILE}" bs=1 count=16 seek=26222592 conv=notrunc

    rm_test_file "${BIG_FILE}"
}

#
# [NOTE]
# This test runs last because it uses up disk space and may not recover.
# This may be a problem, especially on MacOS. (See the comment near the definition
# line for the ENSURE_DISKFREE_SIZE variable)
#
function test_ensurespace_move_file() {
    describe "Testing upload(mv) file when disk space is not enough ..."

    #
    # Make test file which is not under mountpoint
    #
    mkdir -p "${CACHE_DIR}/.ossfs_test_tmpdir"
    ../../junk_data $((BIG_FILE_BLOCK_SIZE * BIG_FILE_COUNT)) > "${CACHE_DIR}/.ossfs_test_tmpdir/${BIG_FILE}"

    #
    # Backup file stat
    #
    local ORIGINAL_PERMISSIONS
    if [ "$(uname)" = "Darwin" ]; then
        ORIGINAL_PERMISSIONS=$(stat -f "%u:%g" "${CACHE_DIR}/.ossfs_test_tmpdir/${BIG_FILE}")
    else
        ORIGINAL_PERMISSIONS=$(stat --format=%u:%g "${CACHE_DIR}/.ossfs_test_tmpdir/${BIG_FILE}")
    fi

    #
    # Fill the disk size
    #
    local NOW_CACHE_DISK_AVAIL_SIZE; NOW_CACHE_DISK_AVAIL_SIZE=$(get_disk_avail_size "${CACHE_DIR}")
    local TMP_FILE_NO=0
    while true; do
      local ALLOWED_USING_SIZE=$((NOW_CACHE_DISK_AVAIL_SIZE - ENSURE_DISKFREE_SIZE))
      if [ "${ALLOWED_USING_SIZE}" -gt "${BIG_FILE_LENGTH}" ]; then
          cp -p "${CACHE_DIR}/.ossfs_test_tmpdir/${BIG_FILE}" "${CACHE_DIR}/.ossfs_test_tmpdir/${BIG_FILE}_${TMP_FILE_NO}"
          local TMP_FILE_NO=$((TMP_FILE_NO + 1))
      else
          break;
      fi
    done

    #
    # move file
    #
    mv "${CACHE_DIR}/.ossfs_test_tmpdir/${BIG_FILE}" "${BIG_FILE}"

    #
    # file stat
    #
    local MOVED_PERMISSIONS
    if [ "$(uname)" = "Darwin" ]; then
        MOVED_PERMISSIONS=$(stat -f "%u:%g" "${BIG_FILE}")
    else
        MOVED_PERMISSIONS=$(stat --format=%u:%g "${BIG_FILE}")
    fi
    local MOVED_FILE_LENGTH
    # shellcheck disable=SC2012
    MOVED_FILE_LENGTH=$(ls -l "${BIG_FILE}" | awk '{print $5}')

    #
    # check
    #
    if [ "${MOVED_PERMISSIONS}" != "${ORIGINAL_PERMISSIONS}" ]; then
        echo "Failed to move file with permission"
        return 1
    fi
    if [ "${MOVED_FILE_LENGTH}" -ne "${BIG_FILE_LENGTH}" ]; then
        echo "Failed to move file with file length: ${MOVED_FILE_LENGTH} ${BIG_FILE_LENGTH}"
        return 1
    fi

    rm_test_file "${BIG_FILE}"
    rm -rf "${CACHE_DIR}/.ossfs_test_tmpdir"
}

function test_ut_ossfs {
    describe "Testing ossfs python ut..."

    # shellcheck disable=SC2153
    export TEST_BUCKET_MOUNT_POINT="${TEST_BUCKET_MOUNT_POINT_1}"
    ../../ut_test.py
}

#
# This test opens a file and writes multiple sets of data.
# The file is opened only once and multiple blocks of data are written
# to the file descriptor with a gap.
#
# That is, the data sets are written discontinuously.
# The data to be written uses multiple data that is less than or larger
# than the part size of the multi-part upload.
# The gap should be at least the part size of the multi-part upload.
# Write as shown below:
#  <SOF>....<write data>....<write data>....<write data><EOF>
#
# There are two types of tests: new files and existing files.
# For existing files, the file size must be larger than where this test
# writes last position.
#  <SOF>....<write data>....<write data>....<write data>...<EOF>
#
function test_write_data_with_skip() {
    describe "Testing write data block with skipping block..."

    #
    # The first argument of the script is "testrun-<random>" the directory name.
    #
    local CACHE_TESTRUN_DIR=$1

    local _SKIPWRITE_FILE="test_skipwrite"
    local _TMP_SKIPWRITE_FILE="/tmp/${_SKIPWRITE_FILE}"

    #------------------------------------------------------
    # (1) test new file 
    #------------------------------------------------------
    #
    # Clean files
    #
    rm_test_file "${_SKIPWRITE_FILE}"
    rm_test_file "${_TMP_SKIPWRITE_FILE}"

    #
    # Create new file in bucket and temporary directory(/tmp)
    #
    # Writing to the file is as follows:
    #    |<-- skip(12MB) --><-- write(1MB) --><-- skip(22MB)  --><-- write(20MB) --><-- skip(23MB)  --><-- write(1MB) -->| (79MB)
    #
    # As a result, areas that are not written to the file are mixed.
    # The part that is not written has a HOLE that is truncate and filled
    # with 0x00.
    # Assuming that multipart upload is performed on a part-by-part basis,
    # it will be as follows:
    #    part 1)       0x0.. 0x9FFFFF :                      <not write area(0x00)>
    #    part 2)  0xA00000..0x13FFFFF :  0xA00000..0xBFFFFF  <not write area(0x00)>
    #                                    0xC00000..0xCFFFFF  <write area>
    #                                    0xD00000..0x13FFFFF <not write area(0x00)>
    #    part 3) 0x1400000..0x1DFFFFF :                      <not write area(0x00)>
    #    part 4) 0x1E00000..0x27FFFFF : 0x1E00000..0x22FFFFF <not write area(0x00)>
    #                                   0x2300000..0x27FFFFF <write area>
    #    part 5) 0x2800000..0x31FFFFF :                      <write area>
    #    part 6) 0x3200000..0x3BFFFFF : 0x3200000..0x36FFFFF <write area>
    #                                   0x3700000..0x3BFFFFF <not write area(0x00)>
    #    part 7) 0x3C00000..0x45FFFFF :                      <not write area(0x00)>
    #    part 8) 0x4600000..0x4BFFFFF : 0x4600000..0x4AFFFFF <not write area(0x00)>
    #                                   0x4B00000..0x4BFFFFF <write area>
    # 
    ../../write_multiblock -f "${_SKIPWRITE_FILE}" -f "${_TMP_SKIPWRITE_FILE}" -p 12582912:65536 -p 36700160:20971520 -p 78643200:65536

    #
    # delete cache file if using cache
    #
    # shellcheck disable=SC2009
    if ps u -p "${OSSFS_PID}" | grep -q use_cache; then
        rm -f "${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${_SKIPWRITE_FILE}"
        rm -f "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${_SKIPWRITE_FILE}"
    fi

    #
    # Compare
    #
    cmp "${_SKIPWRITE_FILE}" "${_TMP_SKIPWRITE_FILE}"

    #------------------------------------------------------
    # (2) test existed file
    #------------------------------------------------------
    # [NOTE]
    # This test uses the file used in the previous test as an existing file.
    #
    # shellcheck disable=SC2009
    if ps u -p "${OSSFS_PID}" | grep -q use_cache; then
        rm -f "${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${_SKIPWRITE_FILE}"
        rm -f "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${_SKIPWRITE_FILE}"
    fi

    #
    # Over write data to existed file in bucket and temporary directory(/tmp)
    #
    # Writing to the file is as follows:
    #    |<----------------------------------------------- existed file ----------------------------------------------------------->| (79MB)
    #    |<-- skip(12MB) --><-- write(1MB) --><-- skip(22MB)  --><-- write(20MB) --><-- skip(22MB)  --><-- write(1MB) --><-- 1MB -->| (79MB)
    #
    # As a result, areas that are not written to the file are mixed.
    # The part that is not written has a HOLE that is truncate and filled
    # with 0x00.
    # Assuming that multipart upload is performed on a part-by-part basis,
    # it will be as follows:
    #    part 1)       0x0.. 0x9FFFFF :                      <not write area(0x00)>
    #    part 2)  0xA00000..0x13FFFFF :  0xA00000..0xBFFFFF  <not write area(0x00)>
    #                                    0xC00000..0xCFFFFF  <write area>
    #                                    0xD00000..0x13FFFFF <not write area(0x00)>
    #    part 3) 0x1400000..0x1DFFFFF :                      <not write area(0x00)>
    #    part 4) 0x1E00000..0x27FFFFF : 0x1E00000..0x22FFFFF <not write area(0x00)>
    #                                   0x2300000..0x27FFFFF <write area>
    #    part 5) 0x2800000..0x31FFFFF :                      <write area>
    #    part 6) 0x3200000..0x3BFFFFF : 0x3200000..0x36FFFFF <write area>
    #                                   0x3700000..0x3BFFFFF <not write area(0x00)>
    #    part 7) 0x3C00000..0x45FFFFF :                      <not write area(0x00)>
    #    part 8) 0x4600000..0x4BFFFFF : 0x4600000..0x49FFFFF <not write area(0x00)>
    #    part 8) 0x4600000..0x4BFFFFF : 0x4A00000..0x4AFFFFF <write area>
    #                                   0x4B00000..0x4BFFFFF <not write area(0x00)>
    # 
    ../../write_multiblock -f "${_SKIPWRITE_FILE}" -f "${_TMP_SKIPWRITE_FILE}" -p 12582912:65536 -p 36700160:20971520 -p 77594624:65536

    #
    # delete cache file if using cache
    #
    # shellcheck disable=SC2009
    if ps u -p "${OSSFS_PID}" | grep -q use_cache; then
        rm -f "${CACHE_DIR}/${TEST_BUCKET_1}/${CACHE_TESTRUN_DIR}/${_SKIPWRITE_FILE}"
        rm -f "${CACHE_DIR}/.${TEST_BUCKET_1}.stat/${CACHE_TESTRUN_DIR}/${_SKIPWRITE_FILE}"
    fi

    #
    # Compare
    #
    cmp "${_SKIPWRITE_FILE}" "${_TMP_SKIPWRITE_FILE}"

    #
    # Clean files
    #
    rm_test_file "${_SKIPWRITE_FILE}"
    rm_test_file "${_TMP_SKIPWRITE_FILE}"
}

function test_symlink_in_meta {
    describe "Testing symlinks in meta ..."

    rm -f "${TEST_TEXT_FILE}"
    rm -f "${ALT_TEST_TEXT_FILE}"
    echo foo > "${TEST_TEXT_FILE}"

    ln -s "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"
    cmp "${TEST_TEXT_FILE}" "${ALT_TEST_TEXT_FILE}"

    rm -f "${TEST_TEXT_FILE}"

    local ALT_OBJECT_NAME; ALT_OBJECT_NAME=$(basename "${PWD}")/"${ALT_TEST_TEXT_FILE}"
    local target_symlink; target_symlink=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${ALT_OBJECT_NAME}" | grep "symlink-target")
    if ! echo "${target_symlink}" | grep -q "header:${#TEST_TEXT_FILE}:${TEST_TEXT_FILE}"; then
        echo "Unexpected target_symlink: ${target_symlink}"
        return 1;
    fi

    [ -L "${ALT_TEST_TEXT_FILE}" ]
    [ ! -f "${ALT_TEST_TEXT_FILE}" ]

    rm -f "${ALT_TEST_TEXT_FILE}"

    # set target_symlink meta and get file stat
    local TEST_TEXT_FILE_RAW;TEST_TEXT_FILE_RAW="${TEST_TEXT_FILE}-raw.txt"
    local ALT_TEST_TEXT_FILE_RAW;ALT_TEST_TEXT_FILE_RAW="${TEST_TEXT_FILE_RAW}.link.txt"
    echo hello > "${TEST_TEXT_FILE_RAW}"
    local meta="header:${#TEST_TEXT_FILE_RAW}:${TEST_TEXT_FILE_RAW}"
    local SYMLINK_NAME; SYMLINK_NAME=$(basename "${PWD}")/"${ALT_TEST_TEXT_FILE_RAW}"
    aws_cli s3api put-object --content-type="text/plain" --metadata="symlink-target=${meta}" --bucket "${TEST_BUCKET_1}" --key "$SYMLINK_NAME"

    cmp "${TEST_TEXT_FILE_RAW}" "${ALT_TEST_TEXT_FILE_RAW}"

    rm -f "${TEST_TEXT_FILE_RAW}"

    [ -L "${ALT_TEST_TEXT_FILE_RAW}" ]
    [ ! -f "${ALT_TEST_TEXT_FILE_RAW}" ]

    rm -f "${ALT_TEST_TEXT_FILE_RAW}"

    #set target_symlink meta with urlencode
    local TEST_TEXT_FILE_ENC;TEST_TEXT_FILE_ENC="${TEST_TEXT_FILE}-raw-%.txt"
    local ALT_TEST_TEXT_FILE_ENC;ALT_TEST_TEXT_FILE_ENC="${TEST_TEXT_FILE_ENC}.link.txt"
    echo world > "${TEST_TEXT_FILE_ENC}"
    local meta_enc="header:${#TEST_TEXT_FILE_ENC}:${TEST_TEXT_FILE}-raw-%25.txt"
    local SYMLINK_NAME_ENC; SYMLINK_NAME_ENC=$(basename "${PWD}")/"${ALT_TEST_TEXT_FILE_ENC}"
    aws_cli s3api put-object --content-type="text/plain" --metadata="symlink-target=${meta_enc}" --bucket "${TEST_BUCKET_1}" --key "$SYMLINK_NAME_ENC"

    cmp "${TEST_TEXT_FILE_ENC}" "${ALT_TEST_TEXT_FILE_ENC}"

    local CONTENT; CONTENT=$(cat "${ALT_TEST_TEXT_FILE_ENC}")

    if [ "${CONTENT}" != "world" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected world"
       return 1
    fi

    rm -f "${TEST_TEXT_FILE_ENC}"

    [ -L "${ALT_TEST_TEXT_FILE_ENC}" ]
    [ ! -f "${ALT_TEST_TEXT_FILE_ENC}" ]

    rm -f "${ALT_TEST_TEXT_FILE_ENC}"

    #set target_symlink in body
    local TEST_TEXT_FILE_BODY;TEST_TEXT_FILE_BODY="${TEST_TEXT_FILE}-raw-body.txt"
    local ALT_TEST_TEXT_FILE_BODY;ALT_TEST_TEXT_FILE_BODY="${TEST_TEXT_FILE_BODY}.link.txt"
    echo "hello world" > "${TEST_TEXT_FILE_BODY}"
    local meta_body="body:0:"
    local SYMLINK_NAME_BODY; SYMLINK_NAME_BODY=$(basename "${PWD}")/"${ALT_TEST_TEXT_FILE_BODY}"
    local TMP_BODY_CONTENT_FILE="/tmp/${TEST_TEXT_FILE_BODY}.body"
    echo ${TEST_TEXT_FILE_BODY} > ${TMP_BODY_CONTENT_FILE}
    aws_cli s3api put-object --content-type="text/plain" --metadata="symlink-target=${meta_body}" --bucket "${TEST_BUCKET_1}" --key "${SYMLINK_NAME_BODY}" --body "${TMP_BODY_CONTENT_FILE}"

    local CONTENT_BODY; CONTENT_BODY=$(cat "${ALT_TEST_TEXT_FILE_BODY}")
    if [ "${CONTENT_BODY}" != "hello world" ]; then
       echo "CONTENT_BODY read is unexpected, got ${CONTENT_BODY}, expected hello world"
       return 1
    fi

    cmp "${TEST_TEXT_FILE_BODY}" "${ALT_TEST_TEXT_FILE_BODY}"
    rm -f "${TEST_TEXT_FILE_BODY}"

    [ -L "${ALT_TEST_TEXT_FILE_BODY}" ]
    [ ! -f "${ALT_TEST_TEXT_FILE_BODY}" ]

    rm -f "${ALT_TEST_TEXT_FILE_BODY}"
    rm -f "${TMP_BODY_CONTENT_FILE}"
}

function test_default_permissions_readdir_optimize {
    describe "Testing default permissions with readdir_optimize..."
    # 640 regular file
    # 750 dir
    # 777 symlink

    #1 get permissions by issuing head request
    #1.1 with mode value
    rm -rf "${TEST_DIR}"
    local DIRECTORY_NAME; DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_DIR}/normal.txt"
    local OBJECT_NAME_SYMLINK; OBJECT_NAME_SYMLINK=$(basename "${PWD}")/"${TEST_DIR}/symlink.txt"

    local TMP_BODY_CONTENT_FILE="/tmp/${TEST_TEXT_FILE_BODY}.body"
    echo "${TEST_DIR}/normal.txt" > ${TMP_BODY_CONTENT_FILE}

    #0755   -> 493
    #100644 ->33188
    #120640 ->41376
    aws_cli s3api put-object --content-type="application/x-directory" --metadata="mode=493" --bucket "${TEST_BUCKET_1}" --key "${DIRECTORY_NAME}/"
    aws_cli s3api put-object --content-type="text/plain" --metadata="mode=33188" --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME}"
    aws_cli s3api put-object --content-type="text/plain" --metadata="mode=41376" --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME_SYMLINK}"  --body "${TMP_BODY_CONTENT_FILE}"

    local DIRECTORY_PERMISSIONS; DIRECTORY_PERMISSIONS=$(get_permissions "${TEST_DIR}")
    local FILE_PERMISSIONS; FILE_PERMISSIONS=$(get_permissions "${TEST_DIR}/normal.txt")
    local SYMLINK_PERMISSIONS; SYMLINK_PERMISSIONS=$(get_permissions "${TEST_DIR}/symlink.txt")

    if [ "${DIRECTORY_PERMISSIONS}" != "750" ] ; then
      echo "dir permission is not correct."
      return 1
    fi
    if [ "${FILE_PERMISSIONS}" != "640" ] ; then
      echo "file permission is not correct."
      return 1
    fi
    if [ "${SYMLINK_PERMISSIONS}" != "777" ] ; then
      echo "symlink permission is not correct."
      return 1
    fi

    #1.2 without mode value
    rm -rf "${TEST_DIR}"
    DIRECTORY_PERMISSIONS=""
    FILE_PERMISSIONS=""
    DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}"
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_DIR}/normal-no-mode.txt"

    aws_cli s3api put-object --content-type="application/x-directory" --bucket "${TEST_BUCKET_1}" --key "${DIRECTORY_NAME}/"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME}"

    DIRECTORY_PERMISSIONS=$(get_permissions "${TEST_DIR}")
    FILE_PERMISSIONS=$(get_permissions "${TEST_DIR}/normal-no-mode.txt")

    if [ "${DIRECTORY_PERMISSIONS}" != "750" ] ; then
      echo "dir permission is not correct."
      return 1
    fi
    if [ "${FILE_PERMISSIONS}" != "640" ] ; then
      echo "file permission is not correct."
      return 1
    fi

    #1.3 new symlink format without mode
    SYMLINK_PERMISSIONS=""
    OBJECT_NAME_SYMLINK=$(basename "${PWD}")/"${TEST_DIR}/symlink-no-mode-new.txt"
    aws_cli s3api put-object --content-type="text/plain" --metadata="symlink-target=header:12:old-text.txt" --bucket "${TEST_BUCKET_1}" --key "$OBJECT_NAME_SYMLINK"
    SYMLINK_PERMISSIONS=$(get_permissions "${TEST_DIR}/symlink-no-mode-new.txt")
    if [ "${SYMLINK_PERMISSIONS}" != "777" ] ; then
      echo "symlink permission is not correct."
      return 1
    fi

    rm -rf "${TEST_DIR}"

    #get permissions by issuing list objects
    rm -rf "${TEST_DIR}-list"
    DIRECTORY_PERMISSIONS=""
    FILE_PERMISSIONS=""
    SYMLINK_PERMISSIONS=""
    DIRECTORY_NAME=$(basename "${PWD}")/"${TEST_DIR}-list"
    OBJECT_NAME=$(basename "${PWD}")/"${TEST_DIR}-list/normal.txt"
    local OBJECT_NAME1; OBJECT_NAME1=$(basename "${PWD}")/"${TEST_DIR}-list/sub-folder/normal.txt"
    OBJECT_NAME_SYMLINK=$(basename "${PWD}")/"${TEST_DIR}-list/symlink.txt"

    TMP_BODY_CONTENT_FILE="/tmp/${TEST_TEXT_FILE_BODY}.body"
    echo "${TEST_DIR}/normal.txt" > ${TMP_BODY_CONTENT_FILE}
    aws_cli s3api put-object --content-type="application/x-directory" --bucket "${TEST_BUCKET_1}" --key "${DIRECTORY_NAME}/"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME}" --body "${TMP_BODY_CONTENT_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME1}" --body "${TMP_BODY_CONTENT_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --metadata="symlink-target=header:12:old-text.txt" --bucket "${TEST_BUCKET_1}" --key "$OBJECT_NAME_SYMLINK"

    ls "${TEST_DIR}-list" -al > /dev/null
    DIRECTORY_PERMISSIONS=$(get_permissions "${TEST_DIR}-list/sub-folder")
    FILE_PERMISSIONS=$(get_permissions "${TEST_DIR}-list/normal.txt")
    SYMLINK_PERMISSIONS=$(get_permissions "${TEST_DIR}-list/symlink.txt")
    if [ "${DIRECTORY_PERMISSIONS}" != "750" ] ; then
      echo "dir permission is not correct."
      return 1
    fi
    if [ "${FILE_PERMISSIONS}" != "640" ] ; then
      echo "file permission is not correct."
      return 1
    fi
    if [ "${SYMLINK_PERMISSIONS}" != "777" ] ; then
      echo "symlink permission is not correct."
      return 1
    fi

    rm -rf "${TEST_DIR}-list"
    rm -rf "${TEST_DIR}"
}

function test_chmod_readdir_optimize {
    describe "Testing chmod file function with readdir_optimize..."
    # create the test file again
    mk_test_file

    local ORIGINAL_PERMISSIONS; ORIGINAL_PERMISSIONS=$(get_permissions "${TEST_TEXT_FILE}")

    chmod 777 "${TEST_TEXT_FILE}";

    # if they're not the same, we have a problem.
    local CHANGED_PERMISSIONS; CHANGED_PERMISSIONS=$(get_permissions "${TEST_TEXT_FILE}")
    if [ "${CHANGED_PERMISSIONS}" != "${ORIGINAL_PERMISSIONS}" ] ; then
      echo "chmod should work nothing with readdir_optimize."
      return 1
    fi

    # clean up
    rm_test_file
}

function test_chown_readdir_optimize {
    describe "Testing chown file function with readdir_optimize..."

    # create the test file again
    mk_test_file

    local ORIGINAL_PERMISSIONS
    if [ "$(uname)" = "Darwin" ]; then
        ORIGINAL_PERMISSIONS=$(stat -f "%u:%g" "${TEST_TEXT_FILE}")
    else
        ORIGINAL_PERMISSIONS=$(stat --format=%u:%g "${TEST_TEXT_FILE}")
    fi

    # [NOTE]
    # Prevents test interruptions due to permission errors, etc.
    # If the chown command fails, an error will occur with the
    # following judgment statement. So skip the chown command error.
    # '|| true' was added due to a problem with Travis CI and MacOS
    # and ensure_diskfree option.
    #
    chown 1000:1000 "${TEST_TEXT_FILE}" || true

    # if they're the same, we have a problem.
    local CHANGED_PERMISSIONS
    if [ "$(uname)" = "Darwin" ]; then
        CHANGED_PERMISSIONS=$(stat -f "%u:%g" "${TEST_TEXT_FILE}")
    else
        CHANGED_PERMISSIONS=$(stat --format=%u:%g "${TEST_TEXT_FILE}")
    fi
    if [ "${CHANGED_PERMISSIONS}" != "${ORIGINAL_PERMISSIONS}" ]; then
      echo "chmod should work nothing with readdir_optimize."
      return 1
    fi

    # clean up
    rm_test_file
}

function test_time_readdir_optimize {
    describe "Testing time function with readdir_optimize..."

    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    echo data | aws_cli s3 cp --metadata="atime=1688201714,ctime=1689218776,mtime=1688201714" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    local base_atime; base_atime=$(get_atime "${TEST_TEXT_FILE}")
    local base_ctime; base_ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local base_mtime; base_mtime=$(get_mtime "${TEST_TEXT_FILE}")

    local INFO_STR; INFO_STR=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME}" | grep "LastModified" | cut  -d'"' -f4 )
    local base_time; base_time=$(date "+%s" --date "${INFO_STR}")

    if [ "${base_time}" != "${base_atime}" ] || [ "${base_time}" != "${base_ctime}" ] || [ "${base_time}" != "${base_mtime}" ]; then
       echo "time should be the same"
       return 1
    fi

    sleep 2

    #
    # touch -> update ctime/atime/mtime
    #
    touch "${TEST_TEXT_FILE}"
    local atime; atime=$(get_atime "${TEST_TEXT_FILE}")
    local ctime; ctime=$(get_ctime "${TEST_TEXT_FILE}")
    local mtime; mtime=$(get_mtime "${TEST_TEXT_FILE}")

    INFO_STR=$(aws_cli s3api head-object --bucket "${TEST_BUCKET_1}" --key "${OBJECT_NAME}" | grep "LastModified" | cut  -d'"' -f4 )
    local touch_time; touch_time=$(date "+%s" --date "${INFO_STR}")

    if [ "${touch_time}" != "${atime}" ] || [ "${touch_time}" != "${ctime}" ] || [ "${touch_time}" != "${mtime}" ]; then
       echo "time should be the same"
       return 1
    fi

    if [ "${base_time}" = "${touch_time}"]; then
       echo "time is not the same"
       return 1
    fi

    rm_test_file
}
function test_readdir_check_size_48 {
    describe "Testing readdir_optimize with check size 48..."

    rm -rf "${TEST_TEXT_FILE}"
    rm -rf "${ALT_TEST_TEXT_FILE}"

    local OBJECT_NAME; OBJECT_NAME=$(basename "${PWD}")/"${TEST_TEXT_FILE}"
    local ALT_OBJECT_NAME; ALT_OBJECT_NAME=$(basename "${PWD}")/"${ALT_TEST_TEXT_FILE}"
    local content_48; content_48="1234567890/1234567890/1234567890/1234567890/1234567890.txt"
    echo "hello world" > text.txt
    echo "hello" > long-file-text.txt
    echo text.txt | aws_cli s3 cp --metadata="mode=41376" - "s3://${TEST_BUCKET_1}/${OBJECT_NAME}"
    echo ${content_48} | aws_cli s3 cp --metadata="mode=41376" - "s3://${TEST_BUCKET_1}/${ALT_OBJECT_NAME}"

    ls -al > /dev/null

    local CONTENT; CONTENT=$(cat "${TEST_TEXT_FILE}")
    if [ "${CONTENT}" != "hello world" ]; then
       echo "CONTENT read is unexpected, got ${CONTENT}, expected world"
       return 1
    fi

    local ALT_CONTENT; ALT_CONTENT=$(cat "${ALT_TEST_TEXT_FILE}")
    if [ "${ALT_CONTENT}" != ${content_48} ]; then
       echo "CONTENT read is unexpected, got ${ALT_CONTENT}, expected ${content_48}"
       return 1
    fi

    rm -rf "${TEST_TEXT_FILE}"
    rm -rf "${ALT_TEST_TEXT_FILE}"
}

function test_direct_read {
    describe "Testing direct read ..."

    local TEST_FILE="direct-read-test-file"
    local RANDOM_STRING; RANDOM_STRING=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w $((RANDOM % 1024 + 1)) | head -n 1)

    local BYTE_CNT; BYTE_CNT=$((RANDOM % 100 + 1))
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1 count=${BYTE_CNT}
    # avoid taking up local disk space bacase of cache file.
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"
    # compare file 
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}"; then
        return 1
    fi
    # cache file's size should be 0 because of direct read.
    if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'` -ne 0 ]; then
        return 1
    fi

    local KBYTE_CNT; KBYTE_CNT=$((RANDOM % 100 + 1))
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1K count=${KBYTE_CNT}
    echo ${RANDOM_STRING} >> "${TEMP_DIR}/${TEST_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}"; then
        return 1
    fi
    if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'` -ne 0 ]; then
        return 1
    fi

    local MBYTE_CNT; MBYTE_CNT=$((RANDOM % 3 + 1)) #less than a chunk
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=${MBYTE_CNT}
    echo ${RANDOM_STRING} >> "${TEMP_DIR}/${TEST_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}"; then
        return 1
    fi
    if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'` -ne 0 ]; then
        return 1
    fi

    MBYTE_CNT=$((RANDOM % 100 + 1))
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=${MBYTE_CNT}
    echo ${RANDOM_STRING} >> "${TEMP_DIR}/${TEST_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}"; then
        return 1
    fi
    if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'` -ne 0 ]; then
        return 1
    fi

    MBYTE_CNT=$((RANDOM % 100 + 128))
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=${MBYTE_CNT}
    echo ${RANDOM_STRING} >> "${TEMP_DIR}/${TEST_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}"; then
        return 1
    fi
    if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'` -ne 0 ]; then
        return 1
    fi

    # test reading file from the middle
    SKIP_CNT=$((RANDOM % 100))
    dd if="${TEST_FILE}" of="${TEMP_DIR}/${TEST_FILE}-read-from-middle" bs=1M skip=${SKIP_CNT}
    dd if="${TEMP_DIR}/${TEST_FILE}" of="${TEMP_DIR}/${TEST_FILE}-read-from-middle.cmp" bs=1M skip=${SKIP_CNT}
    if ! cmp "${TEMP_DIR}/${TEST_FILE}-read-from-middle" "${TEMP_DIR}/${TEST_FILE}-read-from-middle.cmp"; then
        return 1
    fi
    if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'` -ne 0 ]; then
        return 1
    fi

    # test writing
    local SEEK_COUNT=$((RANDOM % 100 + 1))
    dd if=/dev/zero of="${TEMP_DIR}/${TEST_FILE}" bs=1M seek=${SEEK_COUNT} count=1 conv=notrunc
    dd if=/dev/zero of="${TEST_FILE}" bs=1M seek=${SEEK_COUNT} count=1 conv=notrunc
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}"; then
        return 1
    fi

    rm -f "${TEMP_DIR}/${TEST_FILE}"
    rm_test_file "${TEST_FILE}"
}

function test_direct_read_with_out_of_order {
    describe "Testing direct read with out of order ..."

    local TEST_FILE="direct-read-test-file-with-out-of-order"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=256
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"
    
    # skip size less than a chunk will be considered as sequential reading.
    ../../direct_read_test -r "${TEST_FILE}" -o 100 -s 3 -w "${TEMP_DIR}/${TEST_FILE}-skip-less-than-chunksize"
    CACHE_SIZE=`du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'`
    if [ $CACHE_SIZE -ne 0 ]; then
        return 1
    fi

    ../../direct_read_test -r "${TEMP_DIR}/${TEST_FILE}" -o 100 -s 3 -w "${TEMP_DIR}/${TEST_FILE}-skip-less-than-chunksize.new"
    if ! cmp "${TEMP_DIR}/${TEST_FILE}-skip-less-than-chunksize.new" "${TEMP_DIR}/${TEST_FILE}-skip-less-than-chunksize"; then
        return 1
    fi
    rm -f "${TEMP_DIR}/${TEST_FILE}-skip-less-than-chunksize.new"
    rm -f "${TEMP_DIR}/${TEST_FILE}-skip-less-than-chunksize"
    
    ../../direct_read_test -r "${TEST_FILE}" -o 100 -s 20 -w "${TEMP_DIR}/${TEST_FILE}-skip-greater-than-chunksize"
    CACHE_SIZE=`du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'`
    if [ $CACHE_SIZE -ne 0 ]; then
        return 1
    fi

    ../../direct_read_test -r "${TEMP_DIR}/${TEST_FILE}" -o 100 -s 20 -w "${TEMP_DIR}/${TEST_FILE}-skip-greater-than-chunksize.new"
    if ! cmp "${TEMP_DIR}/${TEST_FILE}-skip-greater-than-chunksize.new" "${TEMP_DIR}/${TEST_FILE}-skip-greater-than-chunksize"; then
        return 1
    fi
    rm -f "${TEMP_DIR}/${TEST_FILE}-skip-greater-than-chunksize.new"
    rm -f "${TEMP_DIR}/${TEST_FILE}-skip-greater-than-chunksize"

    rm -f ${TEMP_DIR}/${TEST_FILE}
    rm_test_file "${TEST_FILE}"
}

function test_direct_read_with_write {
    describe "Testing direct read with another process write ..."

    local TEST_FILE="direct-read-test-file-with-write"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=512
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"

    dd if="${TEST_FILE}" of="${TEMP_DIR}/${TEST_FILE}.new" bs=1M &
    echo "" >> "${TEST_FILE}"
    wait
    
    local CACHE_FILE_SIZE=`du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'`
    if [ ${CACHE_FILE_SIZE} -eq 0 ]; then 
        return 1
    fi
    
    rm -f "${TEMP_DIR}/${TEST_FILE}"
    rm -f "${TEMP_DIR}/${TEST_FILE}.new"
    rm_test_file "${TEST_FILE}"
}

function test_direct_read_with_rename {
    describe "Testing direct read with another process rename ..."

    local TEST_FILE="direct-read-test-file-with-rename"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=512
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"

    local CACHE_DIR_SIZE=`du -s ${CACHE_DIR} | awk '{print $1}'`

    dd if="${TEST_FILE}" of="${TEMP_DIR}/${TEST_FILE}.new" bs=1M &
    mv "${TEST_FILE}" "${TEST_FILE}.cp"
    wait
    
    local CACHE_DIR_SIZE_NEW=`du -s ${CACHE_DIR} | awk '{print $1}'`

    if [ ${CACHE_DIR_SIZE} == ${CACHE_DIR_SIZE_NEW} ]; then
       return 1
    fi

    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEST_FILE}.cp"; then
       return 1
    fi 

    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEMP_DIR}/${TEST_FILE}.new"; then
       return 1
    fi 

    rm -f "${TEMP_DIR}/${TEST_FILE}"
    rm -f "${TEMP_DIR}/${TEST_FILE}.new"
    rm_test_file "${TEST_FILE}.cp"
}

function test_direct_read_with_unlink {
    describe "Testing direct read with another process unlink ..."

    local TEST_FILE="direct-read-test-file-with-unlink"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=512
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"

    dd if="${TEST_FILE}" of="${TEMP_DIR}/${TEST_FILE}.new" bs=1M &
    sleep 1
    rm -f ${TEST_FILE}
    wait
    
    if ! cmp "${TEMP_DIR}/${TEST_FILE}" "${TEMP_DIR}/${TEST_FILE}.new"; then
       return 1
    fi 

    rm -f "${TEMP_DIR}/${TEST_FILE}"
    rm -f "${TEMP_DIR}/${TEST_FILE}.new"
}

function test_direct_read_with_truncate {
    describe "Testing direct read with anothre process truncate ..."
    
    local TEST_FILE="direct-read-test-file-with-truncate"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=512
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}" --body "${TEMP_DIR}/${TEST_FILE}"

    dd if="${TEST_FILE}" of="${TEMP_DIR}/${TEST_FILE}" bs=1M &
    truncate -s 1024M "${TEST_FILE}"
    wait

    local CACHE_FILE_SIZE=`du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}" | awk '{print $1}'`
    if [ ${CACHE_FILE_SIZE} -eq 0 ]; then 
        return 1
    fi
    
    rm -f ${TEMP_DIR}/${TEST_FILE}
    rm_test_file "${TEST_FILE}"
}

function test_concurrent_direct_read {
    describe "Testing concurrent direct read ..."

    local TEST_FILE="concurrent_direct_read_test_file"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=254
    for i in $(seq 5); do
        aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}_${i}" --body "${TEMP_DIR}/${TEST_FILE}" &
    done
    wait

    #1. read different file
    for i in $(seq 5); do
        dd if="${TEST_FILE}_${i}" of="${TEMP_DIR}/${TEST_FILE}_${i}" bs=1M &
    done
    wait

    for i in $(seq 5); do
        if ! cmp "${TEMP_DIR}/${TEST_FILE}_${i}" "${TEMP_DIR}/${TEST_FILE}"; then
            return 1
        fi

        if [ `du -s "${CACHE_DIR}/${TEST_BUCKET_1}/$(basename "${PWD}")/${TEST_FILE}_${i}" | awk '{print $1}'` -ne 0 ]; then
            return 1
        fi
    done

    #2. clean up files
    rm -f "${TEMP_DIR}/${TEST_FILE}"
    for i in $(seq 5); do
        rm -f "${TEMP_DIR}/${TEST_FILE}_${i}"
    done

    rm_test_file
}

function test_free_cache_ahead {
    describe "Testing free cache ahead ..."

    # fake_diskfree = 200MB
    local TEST_FILE="test_reclaim_cache_ahead"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=100

    for i in $(seq 4); do
        aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "$(basename "${PWD}")/${TEST_FILE}_${i}" --body "${TEMP_DIR}/${TEST_FILE}" &
    done
    wait

    for i in $(seq 4); do
        dd if="${TEST_FILE}_${i}" of="${TEMP_DIR}/${TEST_FILE}_${i}" bs=1M &
    done
    wait

    for i in $(seq 4); do
        if ! cmp "${TEMP_DIR}/${TEST_FILE}_${i}" "${TEMP_DIR}/${TEST_FILE}"; then
            rm -f ${TEMP_DIR}/${TEST_FILE}_${i}
            return 1
        fi
        rm -f ${TEMP_DIR}/${TEST_FILE}_${i}
    done

    rm_test_file "${TEST_FILE}"
}

function test_default_acl {
    describe "Testing defacult acl..."
    
    local DIR_NAME; DIR_NAME=$(basename "${PWD}")
    touch "test.txt"

    local CONTENT_TYPE; CONTENT_TYPE=$(ossutil_cmd stat "oss://${TEST_BUCKET_1}/${DIR_NAME}/test.txt" | grep ACL)
    if ps u -p "${OSSFS_PID}" | grep -q default_acl; then
        if ! echo "${CONTENT_TYPE}" | grep -q "private"; then 
            return 1
        fi
    else    
        if ! echo "${CONTENT_TYPE}" | grep -q "default"; then 
            return 1
        fi
    fi

    rm -rf "test.txt"
}

# TEMP_DIR: set in test-utils.sh
function test_mix_direct_read {
    # write several files of random sizes, and calculate their md5s
    describe "Testing mixed direct read ..."

    local LOCAL_FILE_1="mix_direct_read_test_file_1"
    local LOCAL_FILE_2="mix_direct_read_test_file_2"

    for i in {0..4}; do
        # create a file of random size between [$i *1G, ($i+1) *1G)
        local DD_CNT=$((RANDOM % (256 * 1024) + $i * 256 * 1024))
        echo "round $i. Create a file sized of $DD_CNT * 4K"
        dd if=/dev/urandom of="${TEMP_DIR}/${LOCAL_FILE_1}_${i}" bs=4k count=$DD_CNT
       
        # calculate its md5
        local md5_1=`md5sum "${TEMP_DIR}/${LOCAL_FILE_1}_${i}" | awk '{print $1}'`
        echo "original md5 of ${TEMP_DIR}/${LOCAL_FILE_1}_${i} is $md5_1"

        # put it to oss
        local cloud_path="$(basename "${PWD}")/${LOCAL_FILE_1}_${i}"
        aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "${cloud_path}" --body "${TEMP_DIR}/${LOCAL_FILE_1}_${i}"

        # read this file from oss and write to a new file on the local disk in a random order
        ../../mix_direct_read_test -r "${LOCAL_FILE_1}_${i}" -w "${TEMP_DIR}/${LOCAL_FILE_2}_${i}" -s $(($DD_CNT * 4 * 1024))

        # calulate the md5 of the file on local disk
        local md5_2=`md5sum "${TEMP_DIR}/${LOCAL_FILE_2}_${i}" | awk '{print $1}'`
        echo "the md5 of the new file ${TEMP_DIR}/${LOCAL_FILE_2}_${i} is $md5_2"

        if [ "$md5_1" != "$md5_2" ]; then
            return 1
        fi
        
        # clear the file
        rm -f "${TEMP_DIR}/${LOCAL_FILE_1}_${i}" "${TEMP_DIR}/${LOCAL_FILE_2}_${i}"
    done
}

function test_mix_direct_read_with_skip {
    describe "Testing mixed direct read with skip ..."
    # write a file sized of 8GB, upload it to oss
    local TEST_FILE="raw_test_file"
    dd if=/dev/urandom of="${TEMP_DIR}/${TEST_FILE}" bs=1M count=8192

    local cloud_path="$(basename "${PWD}")/${TEST_FILE}"
    aws_cli s3api put-object --content-type="text/plain" --bucket "${TEST_BUCKET_1}" --key "${cloud_path}" --body "${TEMP_DIR}/${TEST_FILE}"

    local FILE_SKIPPED_READ_1="raw_test_file_skipped_read_1"
    local FILE_SKIPPED_READ_2="raw_test_file_skipped_read_2"

    for i in {1..5}; do
        # read part of the file out of order, and write to local disk
        local write_size_mb=$((RANDOM % (1024 * 4)))
        local write_offset=$((RANDOM % (1024 * 4)))

        echo "round $i. skip from ${write_offset} MB, skipped size ${write_size_mb} MB, and write to local disk"
        echo "read from local disk..."
        ../../direct_read_test -r "${TEMP_DIR}/${TEST_FILE}" -o ${write_offset} -s ${write_size_mb} -w "${TEMP_DIR}/${FILE_SKIPPED_READ_1}"
        
        echo "read from oss..."
        ../../direct_read_test -r "${TEST_FILE}" -o ${write_offset} -s ${write_size_mb} -w "${TEMP_DIR}/${FILE_SKIPPED_READ_2}"
        
        # compare these 2 files
        if ! cmp "${TEMP_DIR}/${FILE_SKIPPED_READ_1}" "${TEMP_DIR}/${FILE_SKIPPED_READ_2}"; then
            return 1
        fi

        rm -f "${TEMP_DIR}/${FILE_SKIPPED_READ_1}"
        rm -f "${TEMP_DIR}/${FILE_SKIPPED_READ_2}"
    done
    rm -f "${TEMP_DIR}/${TEST_FILE}"
}

function add_all_tests {
    # shellcheck disable=SC2009
    if ps u -p "${OSSFS_PID}" | grep -q use_cache; then
        add_tests test_cache_file_stat
        add_tests test_zero_cache_file_stat
    fi
    # shellcheck disable=SC2009
    if ! ps u -p "${OSSFS_PID}" | grep -q ensure_diskfree && ! uname | grep -q Darwin; then
        add_tests test_clean_up_cache
    fi
    add_tests test_create_empty_file
    add_tests test_append_file
    add_tests test_truncate_file
    add_tests test_truncate_upload
    add_tests test_truncate_empty_file
    add_tests test_truncate_shrink_file
    add_tests test_mv_file
    add_tests test_mv_to_exist_file
    add_tests test_mv_empty_directory
    add_tests test_mv_nonempty_directory
    add_tests test_redirects
    add_tests test_mkdir_rmdir
    add_tests test_list
    add_tests test_remove_nonempty_directory
    add_tests test_external_directory_creation
    add_tests test_external_modification
    add_tests test_external_creation
    add_tests test_read_external_object
    add_tests test_read_external_dir_object
    add_tests test_update_metadata_external_small_object
    add_tests test_update_metadata_external_large_object
    add_tests test_rename_before_close
    add_tests test_multipart_upload
    add_tests test_multipart_copy
    add_tests test_multipart_mix
    add_tests test_utimens_during_multipart
    add_tests test_special_characters
    add_tests test_hardlink
    add_tests test_symlink
    add_tests test_update_chmod_opened_file

    add_tests test_rm_rf_dir
    add_tests test_copy_file
    add_tests test_write_after_seek_ahead
    add_tests test_overwrite_existing_file_range
    add_tests test_concurrent_directory_updates
    add_tests test_concurrent_reads
    add_tests test_concurrent_writes
    add_tests test_open_second_fd
    add_tests test_write_multiple_offsets
    add_tests test_write_multiple_offsets_backwards
    add_tests test_content_type
    add_tests test_truncate_cache
    add_tests test_upload_sparsefile
    add_tests test_mix_upload_entities
    add_tests test_ut_ossfs
    # shellcheck disable=SC2009
    if ! ps u -p "${OSSFS_PID}" | grep -q ensure_diskfree && ! uname | grep -q Darwin; then
        add_tests test_ensurespace_move_file
    fi
    add_tests test_write_data_with_skip
    if ps u -p "${OSSFS_PID}" | grep -q symlink_in_meta; then
        add_tests test_symlink_in_meta
    fi

    # when readdir_optimize enable, ignore mtime/atime/ctime, uid/gid, and permissions
    if ps u -p "${OSSFS_PID}" | grep -q readdir_optimize; then
        add_tests test_default_permissions_readdir_optimize
        add_tests test_chmod_readdir_optimize
        add_tests test_chown_readdir_optimize
        add_tests test_time_readdir_optimize
        if ps u -p "${OSSFS_PID}" | grep -q readdir_check_size; then
            add_tests test_readdir_check_size_48
        fi
    else
        add_tests test_chmod
        add_tests test_chown
        add_tests test_extended_attributes
        add_tests test_mtime_file

        add_tests test_update_time_chmod
        add_tests test_update_time_chown
        add_tests test_update_time_xattr
        add_tests test_update_time_touch
        if ! mount -t fuse.ossfs | grep "$TEST_BUCKET_MOUNT_POINT_1 " | grep -q -e noatime -e relatime ; then
            add_tests test_update_time_touch_a
        fi
        add_tests test_update_time_append
        add_tests test_update_time_cp_p
        add_tests test_update_time_mv

        add_tests test_update_directory_time_chmod
        add_tests test_update_directory_time_chown
        add_tests test_update_directory_time_set_xattr
        add_tests test_update_directory_time_touch
        if ! mount -t fuse.ossfs | grep "$TEST_BUCKET_MOUNT_POINT_1 " | grep -q -e noatime -e relatime ; then
            add_tests test_update_directory_time_touch_a
        fi
        add_tests test_update_directory_time_subdir
    fi
    
    if ps u -p "${OSSFS_PID}" | grep direct_read | grep -v -q direct_read_local_file_cache_size_mb; then
        add_tests test_direct_read
        add_tests test_direct_read_with_out_of_order
        add_tests test_direct_read_with_write
        add_tests test_direct_read_with_rename
        add_tests test_direct_read_with_unlink
        add_tests test_direct_read_with_truncate
        add_tests test_concurrent_direct_read
        add_tests test_mix_direct_read_with_skip
    fi

    if ps u -p "${OSSFS_PID}" | grep -q parallel_count && ps u -p "${OSSFS_PID}" | grep -q fake_diskfree; then
        add_tests test_free_cache_ahead
    fi
    
    if ! uname | grep -q Darwin; then 
        add_tests test_default_acl
    fi

    if ps u -p "${OSSFS_PID}" | grep -q direct_read_local_file_cache_size_mb; then
        add_tests test_mix_direct_read
    fi
}

init_suite
add_all_tests
run_suite

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
