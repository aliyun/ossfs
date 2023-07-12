#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

: ${BUCKET:="oss-bucket-ossfs-bench"}
: ${GOOFYS_BUCKET:="$BUCKET"}
: ${FAST:="false"}
: ${CACHE:="false"}
: ${ENDPOINT:="http://oss-cn-shenzhen-internal.aliyuncs.com/"}
: ${OSS_ACCESS_KEY_ID:=""}
: ${OSS_SECRET_ACCESS_KEY:=""}
: ${PROG:="ossfs"}

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

rm -rf bench-mnt
mkdir -p bench-mnt

OSSFS_CACHE="-ouse_cache=/tmp/cache"
GOOFYS_CACHE="--cache /tmp/cache -o allow_other"

if [ "$CACHE" == "false" ]; then
    OSSFS_CACHE=""
    GOOFYS_CACHE=""
fi

OSSFS_ENDPOINT="-ourl=$ENDPOINT"
GOOFYS_ENDPOINT="--endpoint $ENDPOINT --subdomain"

#credential
if test "${OSS_ACCESS_KEY_ID}" != ""; then
    echo "${OSS_ACCESS_KEY_ID}:${OSS_ACCESS_KEY_SECRET}" > /etc/passwd-ossfs
    chmod 0400 /etc/passwd-ossfs
    echo "${OSS_ACCESS_KEY_ID}:${OSS_ACCESS_KEY_SECRET}" > /etc/passwd-s3fs
    chmod 0400 /etc/passwd-s3fs
    AWS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID}"
    AWS_SECRET_ACCESS_KEY="${OSS_ACCESS_KEY_SECRET}"
    export AWS_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY
fi

export BUCKET
export ENDPOINT

S3FS="s3fs -f -ostat_cache_expire=1 ${OSSFS_CACHE} ${OSSFS_ENDPOINT} $BUCKET bench-mnt"
OSSFS="ossfs -f -ostat_cache_expire=1 ${OSSFS_CACHE} ${OSSFS_ENDPOINT} $BUCKET bench-mnt"
GOOFYS="goofys -f --stat-cache-ttl 1s --type-cache-ttl 1s ${GOOFYS_CACHE} ${GOOFYS_ENDPOINT} ${GOOFYS_BUCKET} bench-mnt"

iter=10
if [ "$FAST" != "false" ]; then
    iter=1
fi

function cleanup {
    $GOOFYS >/dev/null &
    PID=$!

    sleep 5

    for f in $dir/bench.goofys $dir/bench.s3fs $dir/bench.ossfs $dir/bench.ossfs-ls $dir/bench.data $dir/bench.png; do
	if [ -e $f ]; then
	    cp $f bench-mnt/
	fi
    done

    kill $PID
    fusermount -u bench-mnt || true
    sleep 1
    rmdir bench-mnt
}
trap cleanup EXIT

for fs in s3fs ossfs ossfs-ls goofys; do
    if [ "$fs" == "-" ]; then
	continue
    fi

    if mountpoint -q bench-mnt; then
	echo "bench-mnt is still mounted"
	exit 1
    fi

    case $fs in
        s3fs)
            FS=$S3FS
            CREATE_FS=$FS
            ;;
        goofys)
            FS=$GOOFYS
            CREATE_FS=$FS
            ;;
        ossfs)
            FS=$OSSFS
            CREATE_FS=$FS
            ;;
        ossfs-ls)
            FS="${OSSFS} -oreaddir_optimize"
            CREATE_FS=$FS
            ;;
    esac

    if [ -e $dir/bench.$fs ]; then
	rm $dir/bench.$fs
    fi

    if [ "$t" = "" ]; then
        for tt in create create_parallel io; do
            $dir/bench.sh "$FS" bench-mnt $tt |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        done

        $dir/bench.sh "$CREATE_FS"  bench-mnt ls_create

        for i in $(seq 1 $iter); do
            $dir/bench.sh "$FS" bench-mnt ls_ls |& tee -a $dir/bench.$fs
        done

        $dir/bench.sh "$FS" bench-mnt ls_rm

        $dir/bench.sh "$CREATE_FS" bench-mnt find_create |& tee -a $dir/bench.$fs
        $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
        $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
    else
        if [ "$t" = "find" ]; then
            $dir/bench.sh "$CREATE_FS" bench-mnt find_create |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
	elif [ "$t" = "cleanup" ]; then
            $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        else
            $dir/bench.sh "$FS" bench-mnt $t |& tee $dir/bench.$fs
        fi
    fi
done

$dir/bench_format.py <(paste $dir/bench.ossfs $dir/bench.s3fs) > $dir/bench.data
gnuplot -c $dir/bench_graph.gnuplot $dir/bench.data $dir/bench.png ossfs s3fs && convert -rotate 90 $dir/bench.png $dir/bench.png


