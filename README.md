# OSSFS2

[OSSFS2 中文文档](https://help.aliyun.com/zh/oss/developer-reference/ossfs-2-0)

## Overview

OSSFS2 is a high-performance file client for mounting an [Alibaba Cloud OSS (Object Storage Service)](https://www.alibabacloud.com/en/product/object-storage-service) bucket as a local filesystem. It has excellent sequential read and write throughput that fully leverage the high bandwidth advantages of OSS.

It is optimized for applications with high storage performance requirements, such as AI training, inference, big data processing, autonomous driving, and other new compute-intensive workloads. These workloads primarily involve sequential and random reads, sequential (append-only) write operations, and do not require full POSIX semantics.

OSSFS2 delivers **significant performance gains with very low CPU and memory overhead**, via the following key optimizations:
* **Redesigned filesystem engine based on the [libfuse3](https://github.com/libfuse/libfuse) Low-Level API**, avoiding the additional performance and memory overhead of the High-Level API.
* **High-performance, coroutine-based HTTP client built on [PhotonLibOS](https://github.com/alibaba/PhotonLibOS)** for efficient communication with OSS.
* **Fine-grained resource control through coroutines, memory pools, and elimination of unnecessary memory copies**, significantly lowering client-side CPU and memory consumption.
* **Efficient cross-platform CRC64 checksumming on writes**, ensuring data integrity with very low overhead.
* **Pipelined prefetching with sliding windows**, maximizing throughput and minimizing latency for sequential reads.

> [!NOTE] 
> The [`main`](https://github.com/aliyun/ossfs/tree/main) branch now defaults to OSSFS2. For OSSFS1, please refer to [`main-v1`](https://github.com/aliyun/ossfs/tree/main-v1) branch, which will continue to receive updates and maintenance.

## Performance Improvements

OSSFS2 delivers significant performance improvements over OSSFS1, particularly in sequential read/write operations and high-concurrency small-file reads. For detailed benchmark results, please refer to the [Performance Benchmarks](https://www.alibabacloud.com/help/en/oss/developer-reference/performance-test-of-ossfs-2-0).

All tests use a **per-thread-per-file** concurrency model (each thread reads/writes its own independent file).

Compared to OSSFS1, OSSFS2 delivers:
* **1800% higher throughput** in single-threaded sequential large-file (100 GiB) writes.
* **Over 300% higher throughput** in sequential large-file (100 GiB) reads with either 1 or 4 threads.
* **More than 2000% higher throughput** in concurrent small-file (128 KiB) reads under 128-thread workloads.

## Limitations

OSSFS2 does not support full POSIX semantics. See limitations on [OSSFS2 Overview](https://www.alibabacloud.com/help/en/oss/developer-reference/ossfs-2-0).

## Getting Started

### Installing Pre-compiled Packages

We provide packages for common Linux distributions:

* Alibaba Cloud Linux 2 (x86_64)
* Alibaba Cloud Linux 3 (x86_64 and aarch64)
* CentOS 7 and 8 (x86_64)
* Ubuntu 14.04 or later (x86_64)
* Debian 11 or later (x86_64)

Please select the corresponding package for download and installation from the [release page](https://github.com/aliyun/ossfs/releases).

On Ubuntu, execute the following command to install:

```bash
sudo dpkg -i <your_ossfs2_package>.deb
```

On Alibaba Cloud Linux/CentOS, execute the following command to install:

```bash
sudo yum install <your_ossfs2_package>.rpm -y
```

For other Linux distributions not listed above, you can compile and install OSSFS2 from source code. Please refer to the [Building from Source](#building-from-source) section below for detailed instructions.

After installation, you can check the ossfs2 version information with the `ossfs2 --version` command.

### Mounting Your Bucket

You'll need valid OSS access credentials to mount your bucket. OSSFS2 supports the following access credential configurations:

* `OSS_ACCESS_KEY_ID` and `OSS_ACCESS_KEY_SECRET` environment variables:

```bash
export OSS_ACCESS_KEY_ID=<your_access_key_id>
export OSS_ACCESS_KEY_SECRET=<your_access_key_secret>
ossfs2 mount /path/to/mount --oss_endpoint=<your_endpoint> --oss_bucket=<your_bucket>
```

* ECSRAMRole:

```bash
ossfs2 mount /path/to/mount --oss_endpoint=<your_endpoint> --oss_bucket=<your_bucket> --ram_role=<your_ecs_ram_role>
```

* `--oss_access_key_id` and `--oss_access_key_secret` mount options:

```bash
ossfs2 mount /path/to/mount --oss_endpoint=<your_endpoint> --oss_bucket=<your_bucket> \
  --oss_access_key_id=<your_access_key_id> --oss_access_key_secret=<your_access_key_secret>
```

> [!NOTE] 
> It is strongly recommended to use ECSRAMRole or environment variables for mounting.

Now you can access OSS just like a local file system:

```bash
ls /path/to/mount
echo "123" > /path/to/mount/test.txt
cat /path/to/mount/test.txt
```

Once you have finished working with the mount point, you can unmount it with the following command:

```bash
umount /path/to/mount
```

In addition to setting parameters directly in the startup command, OSSFS2 also supports mounting using a [configuration file](https://www.alibabacloud.com/help/en/oss/developer-reference/configure-ossfs-2-0). For more details on mount options, run `ossfs2 mount --help` or visit [Mount Options Description](https://www.alibabacloud.com/help/en/oss/developer-reference/description-of-mount-options).

## Troubleshooting

For debugging purposes, you can enable debug logs by setting the `--log_level=debug` option. The logs will be written to `/tmp/ossfs2` directory by default, and this location can be changed with the `--log_dir` option.

If you are running multiple OSSFS2 processes, we strongly recommend using a dedicated `log_dir` for each one.

## Building from Source

OSSFS2 is tested and supported only on Linux with GCC 9 to 13 (inclusive). The build requires the static C++ standard library (libstdc++.a) and CMake 3.8 or higher. The compilation and installation commands are as follows:

```bash
git clone https://github.com/aliyun/ossfs.git
cd ossfs
mkdir build && cd build
cmake ..
make -j4
make install
```

> [!NOTE] 
> For both pre-compiled packages and source compilation, aarch64 support is currently only available for Alibaba Cloud Linux 3. Other ARM-based systems are not supported.

## Contributing
Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

Please ensure your code follows the existing style and includes appropriate tests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For troubleshooting and common questions, consult the [OSSFS2 FAQ](https://www.alibabacloud.com/help/en/oss/developer-reference/ossfs-2-0-faq). If the issue persists, please file a report in the GitHub repository or contact the project maintainers.
