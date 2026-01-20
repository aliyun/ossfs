# OSSFS

[![Version](https://badge.fury.io/gh/aliyun%2Fossfs.svg)][releases]
[![Build Status](https://travis-ci.org/aliyun/ossfs.svg?branch=master)](https://travis-ci.org/aliyun/ossfs?branch=master)

### [README of Chinese](https://github.com/aliyun/ossfs/blob/master/README-CN.md)

## Introduction

The ossfs enables you to mount Alibaba Cloud OSS buckets to a local file in Linux, macOS, and FreeBSD systems. 
In the system, you can conveniently operate on objects in OSS while using the local file system to maintain data sharing. 

## Features

The ossfs is built based on s3fs and has all the features of s3fs. Main features:

* large subset of POSIX including reading/writing files, directories, symlinks, mode, uid/gid, and extended attributes
* allows random writes and appends
* large files via multi-part upload
* renames via server-side copy
* optional server-side encryption
* data integrity via MD5 hashes
* in-memory metadata caching
* local disk data caching

In addition to the above features, ossfs also has its own features.
* renames via server-side single-part copy to improve large files renaming performance
* optional saves the symbolic link target in object user metadata
* optional improve readdir perfermance by ignoring metadata-atime/ctime, uid/gid, and permissions
* optional improve sequential-read performance by downloading file data to and reading from the memory instead of the disk

### Precompiled installer

We have prepared an installer package for common Linux releases: 

- Ubuntu-14.04 or later
- CentOS-7.0 or later
- Anolis-7 or later

Please select the corresponding installer on the [Version Releases Page][Releases] to download and install the tool. The latest version is recommended. 

- For Ubuntu systems, the installation command is: 

```
sudo apt-get update
sudo apt-get install gdebi-core
sudo gdebi your_ossfs_package
```

- For CentOS, the installation command is: 

```
sudo yum localinstall your_ossfs_package
```

- For Anolis, the installation command is: 

```
sudo yum localinstall your_ossfs_package
```

### Install by source code

If you fail to find the corresponding installer package, you can also install the tool by compiling the code on your own. First install the following dependency libraries before compilation: 

Ubuntu 14.04: 

```
sudo apt-get install automake autotools-dev g++ git libcurl4-gnutls-dev \
                     libfuse-dev libssl-dev libxml2-dev make pkg-config
```

CentOS 7.0:

```
sudo yum install automake gcc-c++ git libcurl-devel libxml2-devel \
                 fuse-devel make openssl-devel
```

Then you can download the source code from GitHub and compile the code for installing the tool: 

```
git clone -b main-v1 https://github.com/aliyun/ossfs.git
cd ossfs
./autogen.sh
./configure
make
sudo make install
```

Otherwise consult the [compilation instructions](COMPILATION.md).

## Run OSSFS

The default location for the ossfs password file can be created:

* using a `.passwd-ossfs` file in the users home directory (i.e. `${HOME}/.passwd-ossfs`)
* using the system-wide `/etc/passwd-ossfs` file

Enter your credentials in a file `${HOME}/.passwd-ossfs` and set
owner-only permissions:

```
echo my-access-key-id:my-access-key-secret > ${HOME}/.passwd-ossfs
chmod 600 ${HOME}/.passwd-ossfs
```

Run ossfs with an existing bucket `my-bucket` and directory `/path/to/mountpoint`:

```
ossfs my-bucket /path/to/mountpoint -ourl=my-oss-endpoint
```

If you encounter any errors, enable debug output:

```
ossfs my-bucket /path/to/mountpoint -ourl=my-oss-endpoint -o dbglevel=info -f -o curldbg
```

You can also mount on boot by entering the following line to `/etc/fstab`:

```
my-bucket /path/to/mountpoint fuse.ossfs _netdev,allow_other,url=my-oss-endpoint 0 0
```

Note: You may also want to create the global credential file first

```
echo my-access-key-id:my-access-key-secret > /etc/passwd-ossfs
chmod 600 /etc/passwd-ossfs
```

Note2: You may also need to make sure `netfs` service is start on boot

### Example

Mount the 'my-bucket' bucket to the '/tmp/ossfs' directory and the AccessKeyId is 'faint', 
the AccessKeySecret is '123', and the OSS endpoint is 'http://oss-cn-hangzhou.aliyuncs.com'.

```
echo faint:123 > /etc/passwd-ossfs
chmod 600 /etc/passwd-ossfs
mkdir /tmp/ossfs
ossfs my-bucket /tmp/ossfs -ourl=http://oss-cn-hangzhou.aliyuncs.com
```

Unmount the bucket:

```bash
umount /tmp/ossfs # root user
fusermount -u /tmp/ossfs # non-root user
```

### Common settings

- You can use 'ossfs --version' to view the current version and 'ossfs -h' to view available parameters. 
- If you are using ossfs on an Alibaba Cloud ECS instance, you can use the intranet domain name to **save traffic charges** and 
  **improve speed**: 

        ossfs my-bucket /tmp/ossfs -ourl=http://oss-cn-hangzhou-internal.aliyuncs.com

- In a Linux system, [updatedb][updatedb] will scan the file system on a regular basis. If you do not want the 
  ossfs-mounted directory to be scanned, refer to [FAQ][FAQ-updatedb] to configure skipping the mounted directory. 
- The ossfs allows you to specify multiple sets of bucket/access_key_id/access_key_secret information. When 
  multiple sets of information are in place, the format of the information written to passwd-ossfs is: 

        bucket1:access_key_id1:access_key_secret1
        bucket2:access_key_id2:access_key_secret2

- The [Supervisor][Supervisor] is recommended in a production environment to start and monitor the ossfs process. For usage 
  see [FAQ][faq-supervisor]. 

### Advanced settings

- You can add the '-f -d' parameters to run the ossfs in the foreground and output the debug log. 
- You can use the '-o kernel_cache' parameter to enable the ossfs to use the page cache of the file system. 
  If you have multiple servers mounted to the same bucket and require strong consistency, **do not** use this 
  option. 

## Errors

Do not panic in case of errors. Troubleshoot the problem following the steps below: 

1. If a printing error occurs, read and understand the error message. 
2. View '/var/log/syslog' or '/var/log/messages' to check for any related information. 

        grep 's3fs' /var/log/syslog
        grep 'ossfs' /var/log/syslog

3. Retry ossfs mounting and open the debug log: 

        ossfs ... -o dbglevel=debug -f -d > /tmp/fs.log 2>&1

    Repeat the operation and save the '/tmp/fs.log' to check or send the file to me. 

## Limitations

Generally OSS cannot offer the same performance or semantics as a local file system.  More specifically:

* random writes or appends to files require rewriting the entire object, optimized with multi-part upload copy
* metadata operations such as listing directories have poor performance due to network latency
* no atomic renames of files or directories
* no coordination between multiple clients mounting the same bucket
* no hard links
* inotify detects only local modifications, not external ones by other clients or tools
* not suitable for scenarios with highly concurrent reads/writes as it will increase the system load


## Frequently Asked Questions

* [FAQ](https://github.com/aliyun/ossfs/wiki/FAQ-EN)

## Related

* [ossfs Wiki](https://github.com/aliyun/ossfs/wiki)
* [s3fs](https://github.com/s3fs-fuse/s3fs-fuse)- Mount the s3 bucket to the local file system through the fuse interface. 

## Contact us

* [Alibaba Cloud OSS official website](http://oss.aliyun.com/)
* [Alibaba Cloud OSS official forum](http://bbs.aliyun.com/thread/211.html)
* [Alibaba Cloud OSS official documentation center](http://www.aliyun.com/product/oss#Docs)
* Alibaba Cloud official technical support: [Submit a ticket](https://workorder.console.aliyun.com/#/ticket/createIndex)

## License

Copyright (C) 2010 Randy Rizun <rrizun@gmail.com>

Licensed under the GNU GPL version 2

