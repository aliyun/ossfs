#!/bin/bash
# Usage: ./longterm-stability-test.sh /mnt/mp/workdir /root/logfile /root/result_file

WORKDIR=$1
LOGFILE=$2
RESULTFILE=$3

# zipfile.zip after being inflatted:
# zipdir/file_1, ..., zipdir/file_1000
ZIPFILE="zipfile.zip"

for i in {1..1000}; do
  echo "ROUND $i"
  date

  cp "./$ZIPFILE" $WORKDIR
  unzip "$WORKDIR/$ZIPFILE" -d $WORKDIR

  echo "start to rename..."
  mv $WORKDIR/zipdir $WORKDIR/zipdir-rename

  echo "start to read..."
  for j in {0..9}; do
  {
    for k in {0..99}; do
      index=$(($j * 100 + $k + 1))
      cat $WORKDIR/zipdir-rename/file_$index > /dev/null
    done
  } &
  done
  wait

  echo "start to remove..."
  rm -rf $WORKDIR/zipdir-rename
  rm -f "$WORKDIR/$ZIPFILE"

  if [ `expr $i % 10` == 0 ]; then
    echo "ROUND $i done"
    date

    grep "\[ERR\]" ${LOGFILE}
    if [ $? -eq 0 ]; then
      echo "err in logfile"
      echo "ERROR" > ${RESULTFILE}
      exit 1
    fi
  fi

done

echo "SUCCESS" > ${RESULTFILE}
