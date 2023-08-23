# Benchmark

## Goofys tests

These tests are run with `./run_bench.sh` in this directory. They're rather simple but the results are also included for completeness:

![benchmark results](bench.png?raw=true "Benchmark Results")

The test was run on an ECS ecs.s6-c1m2.xlarge 4CPU 8GiB in cn-shenzhen connected to a bucket in oss-cn-shenzhen. Units are seconds.
Using `-ostat_cache_expire=1` for s3fs（version 1.91）and ossfs.
Using `--stat-cache-ttl 1s --type-cache-ttl 1s` for goofys.
Using `-ostat_cache_expire=1 -oreaddir_optimize` for ossfs+readdir-optimize.
