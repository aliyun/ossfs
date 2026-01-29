# Pre-built libfuse 3.16.2

This directory contains pre-built libfuse 3.16.2 binaries for convenient and consistent deployment across different operating systems.

## Contents

- `libfuse-3.16.2-linux-x86_64.tar.gz`: Pre-built binaries for x86_64 Linux systems
- `libfuse-3.16.2-linux-aarch64.tar.gz`: Pre-built binaries for ARM64 (aarch64) Linux systems

## Purpose

We have compiled libfuse following the official build instructions to provide a consistent version that can be quickly deployed across different operating systems without the need to compile from source on each target system.

## Building Process

Build enverionment: CentOS 7 with gcc 6 and libc 2.17.

The binaries were built following the official libfuse build procedure using Meson and Ninja:

1. Downloaded libfuse 3.16.2 source from [the official repository](https://github.com/libfuse/libfuse/releases/tag/fuse-3.16.2)
2. Configured the build using Meson with appropriate options
```bash
$ tar xzf fuse-X.Y.Z.tar.gz; cd fuse-X.Y.Z
$ mkdir build; cd build
$ meson setup ..
```
3. Compiled the library using Ninja
```bash
$ ninja
$ sudo python3 -m pytest test/
$ sudo ninja install
```
4. Packaged the resulting binaries and headers
