#!/bin/bash

ECS_KEY=$1
ECS_HOST=$2
LOGFILE=/root/log
REMOTE_RESULT_FILE=/root/ossfs-result

# step 1: build ossfs
./autogen.sh
./configure
make -j4

if [ ! -e ./src/ossfs ]; then
  echo "Build failed"
  exit 1
fi

# step 2: scp ossfs to a remote ecs
chmod 0600 ${ECS_KEY}
ssh -i ${ECS_KEY} root@${ECS_HOST} "
rm -rf /root/ossfs
rm -rf /root/longterm-stability-test
rm -f ${LOGFILE}
rm -f ${REMOTE_RESULT_FILE}
"

scp -i ${ECS_KEY} -r ./test/longterm-stability-test root@${ECS_HOST}:/root
scp -i ${ECS_KEY} ./src/ossfs root@${ECS_HOST}:/root

# step 3: mount ossfs and run test
ssh -i ${ECS_KEY} root@${ECS_HOST} "
chmod 777 /root/ossfs
umount /mnt
sleep 1s
/root/ossfs jenkins-wulanchabu /mnt -ourl=oss-cn-wulanchabu-internal.aliyuncs.com -odbglevel=dbg -osigv4 -oregion=cn-wulanchabu -ologfile=${LOGFILE}
"

LOCAL_RESULT_FILE="./ossfs-result"
echo "test result: " > ${LOCAL_RESULT_FILE}
ssh -i ${ECS_KEY} root@${ECS_HOST} "mount | grep ossfs" > ${LOCAL_RESULT_FILE}
grep "ossfs" ${LOCAL_RESULT_FILE}
if [ $? -ne 0 ]; then
  echo "Mount error";
  exit 1
fi

ssh -i ${ECS_KEY} root@${ECS_HOST} "
cd /root/longterm-stability-test
chmod 777 longterm-stability-test.sh
./longterm-stability-test.sh /mnt ${LOGFILE} ${REMOTE_RESULT_FILE}
"

ssh -i ${ECS_KEY} root@${ECS_HOST} "cat ${REMOTE_RESULT_FILE}" > ${LOCAL_RESULT_FILE}

