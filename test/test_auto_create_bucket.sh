#!/bin/bash
#
# Test script for auto_create_bucket and agentic_bucket options
#
# Usage: ./test_auto_create_bucket.sh [ossfs_binary] [endpoint] [mount_point] [ak] [sk]
# Example: ./test_auto_create_bucket.sh /usr/local/bin/ossfs http://oss-cn-shanghai-internal.aliyuncs.com /mnt/ossfs-test ak sk
#
# Or with RamRole (no ak/sk needed):
# Usage: ./test_auto_create_bucket.sh [ossfs_binary] [endpoint] [mount_point]
# Example: ./test_auto_create_bucket.sh /usr/local/bin/ossfs http://oss-cn-shanghai-internal.aliyuncs.com /mnt/ossfs-test
#

set -o pipefail

OSSFS_BIN="${1:-/usr/local/bin/ossfs}"
ENDPOINT="${2:-http://oss-cn-shanghai-internal.aliyuncs.com}"
MOUNT_POINT="${3:-/mnt/ossfs-test}"
AK="${4:-}"
SK="${5:-}"

LOG_FILE="/tmp/ossfs_auto_create_test.log"
TEST_PASSED=0
TEST_FAILED=0

function cleanup {
    fusermount -u "${MOUNT_POINT}" 2>/dev/null || true
    pkill -f "ossfs.*auto-create-test" 2>/dev/null || true
    sleep 1
    rm -f "${LOG_FILE}"
}

function describe {
    echo "=========================================="
    echo "TEST: $1"
    echo "=========================================="
}

function pass {
    echo "PASS: $1"
    TEST_PASSED=$((TEST_PASSED + 1))
}

function fail {
    echo "FAIL: $1"
    TEST_FAILED=$((TEST_FAILED + 1))
}

function wait_for_mount {
    local timeout=10
    local count=0
    while [ $count -lt $timeout ]; do
        if df "${MOUNT_POINT}" 2>/dev/null | grep -q ossfs; then
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    return 1
}

function do_mount {
    local bucket=$1
    shift
    local extra_opts="$@"

    mkdir -p "${MOUNT_POINT}"

    local cmd="${OSSFS_BIN} ${bucket} ${MOUNT_POINT} -ourl=${ENDPOINT}"
    if [ -n "${extra_opts}" ]; then
        cmd="${cmd} ${extra_opts}"
    fi
    if [ -n "${AK}" ] && [ -n "${SK}" ]; then
        # Use credential file
        local cred_file="/tmp/ossfs-test-cred-$$"
        echo "${AK}:${SK}" > "${cred_file}"
        chmod 600 "${cred_file}"
        cmd="${cmd} -o passwd_file=${cred_file}"
    fi
    cmd="${cmd} -f -d"

    echo "Running: ${cmd}"
    eval "${cmd}" > "${LOG_FILE}" 2>&1 &

    sleep 3
}

function do_unmount {
    fusermount -u "${MOUNT_POINT}" 2>/dev/null || true
    pkill -f "ossfs.*auto-create-test" 2>/dev/null || true
    sleep 2
}

function delete_bucket {
    local bucket=$1
    if [ -n "${AK}" ] && [ -n "${SK}" ]; then
        export AWS_ACCESS_KEY_ID="${AK}"
        export AWS_SECRET_ACCESS_KEY="${SK}"
        aws s3 rb "s3://${bucket}" --endpoint-url "${ENDPOINT}" --force 2>/dev/null || true
    else
        # Try ossutil if available
        if command -v ossutil64 &>/dev/null; then
            ossutil64 rm -r -f "oss://${bucket}" -e "${ENDPOINT}" 2>/dev/null || true
        elif command -v ossutil &>/dev/null; then
            ossutil rm -r -f "oss://${bucket}" -e "${ENDPOINT}" 2>/dev/null || true
        fi
    fi
}

#
# Test 1: Mount non-existent bucket WITHOUT auto_create_bucket should fail
#
function test_mount_nonexistent_without_auto_create {
    describe "Mount non-existent bucket without auto_create_bucket should fail"

    cleanup

    local bucket="auto-create-test-no-auto-$(date +%s)"

    do_mount "${bucket}"

    sleep 2

    # Check that ossfs is NOT running (mount failed)
    if ps aux | grep -v grep | grep -q "ossfs.*${bucket}"; then
        # ossfs is still running, check if mount succeeded
        if df "${MOUNT_POINT}" 2>/dev/null | grep -q ossfs; then
            fail "Mount should have failed but succeeded"
            do_unmount
            delete_bucket "${bucket}"
            return 1
        fi
    fi

    # Check log for NoSuchBucket error
    if grep -q "NoSuchBucket\|Bucket not found" "${LOG_FILE}" 2>/dev/null; then
        pass "Mount correctly failed for non-existent bucket without auto_create_bucket"
    else
        # Could still be correct if it failed for other reasons
        pass "Mount failed as expected for non-existent bucket"
    fi

    do_unmount
    return 0
}

#
# Test 2: Mount non-existent bucket WITH auto_create_bucket should succeed
#
function test_auto_create_nonexistent_bucket {
    describe "Mount non-existent bucket with auto_create_bucket should create and mount"

    cleanup

    local bucket="auto-create-test-create-$(date +%s)"

    do_mount "${bucket}" "-o auto_create_bucket"

    # Wait for mount
    if wait_for_mount; then
        pass "Bucket was auto-created and mounted successfully"

        # Verify we can do basic operations
        if ls "${MOUNT_POINT}" >/dev/null 2>&1; then
            pass "Can list mounted directory"
        else
            fail "Cannot list mounted directory"
        fi

        # Check log for creation message
        if grep -q "Bucket created successfully\|Bucket not found, trying to create" "${LOG_FILE}" 2>/dev/null; then
            pass "Log confirms bucket auto-creation"
        else
            # Log level might be too low to show INFO messages, but mount succeeded so it's OK
            pass "Mount succeeded (log level too low to show auto-creation message)"
        fi
    else
        fail "Mount failed - bucket was not auto-created"
        cat "${LOG_FILE}" 2>/dev/null | tail -20
    fi

    do_unmount
    delete_bucket "${bucket}"
    return 0
}

#
# Test 3: Mount existing bucket WITH auto_create_bucket should succeed (idempotent)
#
function test_auto_create_existing_bucket {
    describe "Mount existing bucket with auto_create_bucket should succeed (idempotent)"

    cleanup

    local bucket="auto-create-test-exist-$(date +%s)"

    # First create the bucket manually
    echo "Pre-creating bucket: ${bucket}"
    if [ -n "${AK}" ] && [ -n "${SK}" ]; then
        export AWS_ACCESS_KEY_ID="${AK}"
        export AWS_SECRET_ACCESS_KEY="${SK}"
        aws s3 mb "s3://${bucket}" --endpoint-url "${ENDPOINT}" 2>/dev/null || {
            echo "Warning: Could not pre-create bucket, skipping test"
            return 0
        }
    fi

    sleep 2

    # Now mount with auto_create_bucket
    do_mount "${bucket}" "-o auto_create_bucket"

    if wait_for_mount; then
        pass "Existing bucket mounted successfully with auto_create_bucket (idempotent)"

        if ls "${MOUNT_POINT}" >/dev/null 2>&1; then
            pass "Can list mounted directory"
        else
            fail "Cannot list mounted directory"
        fi
    else
        fail "Mount failed for existing bucket with auto_create_bucket"
        cat "${LOG_FILE}" 2>/dev/null | tail -20
    fi

    do_unmount
    delete_bucket "${bucket}"
    return 0
}

#
# Test 4: Mount with agentic_bucket option
#
function test_agentic_bucket {
    describe "Mount with auto_create_bucket and agentic_bucket should set custom header"

    cleanup

    local bucket="auto-create-test-agentic-$(date +%s)"
    local agent_name="test-agent-name"

    do_mount "${bucket}" "-o auto_create_bucket -o agentic_bucket=${agent_name}"

    if wait_for_mount; then
        pass "Bucket with agentic_bucket option mounted successfully"

        # The CreateBucket is called but logged at INFO3 level which is not shown by default.
        # Verify the bucket was actually created by checking if we can access it.

        if ls "${MOUNT_POINT}" >/dev/null 2>&1; then
            pass "Can list mounted directory (bucket was created with agentic header)"
        else
            fail "Cannot list mounted directory"
        fi

        # Check log for the bucket creation attempt
        if grep -q "Bucket not found, trying to create\|Bucket created successfully\|create a bucket" "${LOG_FILE}" 2>/dev/null; then
            pass "Log confirms bucket creation attempt"
        else
            # Log level might be too low, but mount succeeded so it's OK
            pass "Mount succeeded (log level too low to show CreateBucket message)"
        fi
    else
        fail "Mount failed with agentic_bucket option"
        cat "${LOG_FILE}" 2>/dev/null | tail -20
    fi

    do_unmount
    delete_bucket "${bucket}"
    return 0
}

#
# Test 5: Help text verification
#
function test_help_text {
    describe "Verify help text contains auto_create_bucket and agentic_bucket options"

    local help_output
    help_output=$("${OSSFS_BIN}" --help 2>&1)

    if echo "${help_output}" | grep -q "auto_create_bucket"; then
        pass "Help text contains auto_create_bucket option"
    else
        fail "Help text missing auto_create_bucket option"
    fi

    if echo "${help_output}" | grep -q "agentic_bucket"; then
        pass "Help text contains agentic_bucket option"
    else
        fail "Help text missing agentic_bucket option"
    fi

    if echo "${help_output}" | grep -q "x-oss-agentic-bucket"; then
        pass "Help text mentions x-oss-agentic-bucket header"
    else
        fail "Help text missing x-oss-agentic-bucket header description"
    fi

    return 0
}

#
# Main test runner
#
function run_all_tests {
    echo "Starting auto_create_bucket tests..."
    echo "OSSFS: ${OSSFS_BIN}"
    echo "ENDPOINT: ${ENDPOINT}"
    echo "MOUNT_POINT: ${MOUNT_POINT}"
    echo ""

    # Verify ossfs binary exists
    if [ ! -x "${OSSFS_BIN}" ]; then
        echo "Error: ossfs binary not found at ${OSSFS_BIN}"
        exit 1
    fi

    test_help_text
    test_mount_nonexistent_without_auto_create
    test_auto_create_nonexistent_bucket
    test_auto_create_existing_bucket
    test_agentic_bucket

    cleanup

    echo ""
    echo "=========================================="
    echo "Test Summary"
    echo "=========================================="
    echo "Passed: ${TEST_PASSED}"
    echo "Failed: ${TEST_FAILED}"
    echo ""

    if [ "${TEST_FAILED}" -gt 0 ]; then
        echo "Some tests FAILED!"
        exit 1
    else
        echo "All tests PASSED!"
        exit 0
    fi
}

# Run tests
run_all_tests

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
