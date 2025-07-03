#!/bin/bash

# Prerequisites:
#   - ossfs is suggested to be mounted with internal endpoint
#   - ossutil is installed and configured (internal endpoint)
#   - disk cache has at least 10GB free space

# Usage:
#   ./benchmark.sh single
#   ./benchmark.sh concur

MOUNTPATH=/mnt/oss
CONCUR=10

function benchark_single_read {
  echo "=============== Benchmark Test (Read Single File 10GB) ==============="

  # create a 10GB file on the cloud
  local file_name="localfile"
  dd if=/dev/zero of=$MOUNTPATH/$file_name bs=1M count=10240 status=progress

  pid=`pgrep ossfs`
  mount_cmd=`ps -p $pid -o args=`
  umount $MOUNTPATH
  $mount_cmd

  # read the file
  tp1=$(($(date +%s%N) / 1000000))
  dd if=$MOUNTPATH/$file_name of=/dev/null bs=1M
  tp2=$(($(date +%s%N) / 1000000))

  time_cost_ms=$(($tp2 - $tp1))
  echo "Time Cost: $time_cost_ms ms"
  echo "Throughput: $((10240 * 1000 / $time_cost_ms)) MB/s"
}

function benchmark_concurrent_read {
  echo "=============== Benchmark Test (Read Muliple Files 5GB) ==============="

  local concurrency=10
  if [ $# -eq 1 ]; then
    concurrency=$1
  fi

  # create a 5GB file on the cloud
  echo "uploading $concurrency files of 5GB..."
  local file_name="localfile"
  for i in `seq 1 $concurrency`; do
    dd if=/dev/zero of=$MOUNTPATH/"$file_name"_"$i" bs=1M count=5120 status=progress &
  done
  wait
  echo "finished uploading"

  pid=`pgrep ossfs`
  mount_cmd=`ps -p $pid -o args=`
  umount $MOUNTPATH
  $mount_cmd

  tp1=$(($(date +%s%N) / 1000000))
  for i in `seq 1 $concurrency`; do
    dd if=$MOUNTPATH/"$file_name"_"$i" of=/dev/null bs=1M &
  done
  wait
  tp2=$(($(date +%s%N) / 1000000))

  time_cost_ms=$(($tp2 - $tp1))
  echo "Time Cost: $time_cost_ms ms"
  echo "Throughput: $((5120 * $concurrency * 1000 / $time_cost_ms)) MB/s"
}

if [ $# -eq 0 ]; then
  benchark_single_read
  benchmark_concurrent_read
else
  mode=$1
  if [ $mode = "single" ]; then
    benchark_single_read
  elif [ $mode = "concur" ]; then
    benchmark_concurrent_read $CONCUR
  else
    echo "Invalid mode: $mode"
  fi
fi
