#!/bin/bash
# Usage: ./credential-test.sh /mnt/mp /root/logfile test_ramrole


MP=$1
RAMROLE=$2
LOGFILE=$3

# 1. test credential process set invalidly
umount ${MP}
sleep 1s
/root/ossfs jenkins-wulanchabu /mnt -ourl=oss-cn-wulanchabu-internal.aliyuncs.com -osigv4 -oregion=cn-wulanchabu -ocredential_process="cat /root/token.txt"
if [ $? -eq 0 ]; then
  echo "ERROR: mount succeeded with invalid credential process 'cat /root/token.txt'"
  exit 1
fi
echo "${LINENO} passed"

curl http://100.100.100.200/latest/meta-data/ram/security-credentials/${RAMROLE} > /root/token.txt
/root/ossfs jenkins-wulanchabu /mnt -ourl=oss-cn-wulanchabu-internal.aliyuncs.com -osigv4 -oregion=cn-wulanchabu -ocredential_process="/usr/bin/cat /root/token.txt" 
df -h | grep ossfs | grep "/mnt"
if [ $? -ne 0 ]; then
  echo "ERROR: mount failed with valid credential process '/usr/bin/cat /root/token.txt'"
  exit 1
fi
echo "${LINENO} passed"

# 2. credential process outputs only ak and sk
echo "" > /root/token.txt
ak=$(cut -d':' -f1 /root/.passwd-ossfs)
sk=$(cut -d':' -f2 /root/.passwd-ossfs)
echo "{ \"AccessKeyId\": \"${ak}\", \"AccessKeySecret\": \"${sk}\" }" > /root/token.txt

umount ${MP}
sleep 1s
/root/ossfs jenkins-wulanchabu /mnt -ourl=oss-cn-wulanchabu-internal.aliyuncs.com -osigv4 -oregion=cn-wulanchabu -ocredential_process="/usr/bin/cat /root/token.txt" -ologfile=${LOGFILE} -odbglevel=dbg
df -h | grep ossfs | grep "/mnt"
if [ $? -ne 0 ]; then
  echo "ERROR: mount failed with valid credential process '/usr/bin/cat /root/token.txt'"
  echo "" > /root/token.txt
  exit 1
fi
echo "${LINENO} passed"


# 3. credential process outputs invalid expiration format
# insert an invalid expiration line at the 4th line.
curl http://100.100.100.200/latest/meta-data/ram/security-credentials/${RAMROLE} > /root/token.txt
sed -i '4d' /root/token.txt
sed -i '4i\"Expiration":"INVALID_EXPIRATION_TIME",' /root/token.txt
umount ${MP}
sleep 1s
/root/ossfs jenkins-wulanchabu /mnt -ourl=oss-cn-wulanchabu-internal.aliyuncs.com -osigv4 -oregion=cn-wulanchabu -ocredential_process="/usr/bin/cat /root/token.txt" -ologfile=${LOGFILE} -odbglevel=dbg
df -h | grep ossfs | grep "/mnt"
if [ $? -ne 0 ]; then
  echo "ERROR: mount failed with valid credential process '/usr/bin/cat /root/token.txt'"
  exit 1
fi
echo "${LINENO} passed"

echo "" > ${LOGFILE}
ls ${MP}
grep "SetRAMCredentials" ${LOGFILE}
if [ $? -ne 0 ]; then
  echo "ERROR: logfile doesn't contain 'SetRAMCredentials' with invalid expiration time"
  exit 1
fi

echo "SUCCESS: credential process test passed"