#!/bin/bash

set -e

PROG1=ossfs
PROG2=s3fs
PROG3=goofys
PROG4=ossfs+readdir-optimize

python3 ./bench_format.py <(paste bench.$PROG1 bench.$PROG2 bench.$PROG3 bench.$PROG4) > bench_all.data

OUT=bench_all.png

gnuplot -c bench_graph_all.gnuplot bench_all.data "$OUT" "$PROG1" "$PROG2" "$PROG3" "$PROG4"

convert -rotate 90 "$OUT" "$OUT"
