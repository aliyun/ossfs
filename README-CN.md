# OSSFS

[![Version](https://badge.fury.io/gh/aliyun%2Fossfs.svg)][releases]
[![Build Status](https://travis-ci.org/aliyun/ossfs.svg?branch=master)](https://travis-ci.org/aliyun/ossfs?branch=master)

### [README of English](https://github.com/aliyun/ossfs/blob/master/README.md)

### 简介

ossfs 能让您在Linux/Mac OS X 系统中把Aliyun OSS bucket 挂载到本地文件
系统中，您能够便捷的通过本地文件系统操作OSS 上的对象，实现数据的共享。

### 功能

ossfs 基于s3fs 构建，具有s3fs 的全部功能。主要功能包括：

* 支持POSIX 文件系统的大部分功能，包括文件读写，目录，链接操作，权限，
  uid/gid，以及扩展属性（extended attributes）
* 通过OSS 的multipart 功能上传大文件。
* MD5 校验保证数据完整性。

### 安装

#### 预编译的安装包

我们为常见的linux发行版制作了安装包：

- Ubuntu-14.04
- CentOS-7.0/6.5/5.11

请从[版本发布页面][releases]选择对应的安装包下载安装，建议选择最新版本。

- 对于Ubuntu，安装命令为：

```
sudo apt-get update
sudo apt-get install gdebi-core
sudo gdebi your_ossfs_package
```

- 对于CentOS6.5及以上，安装命令为：

```
sudo yum localinstall your_ossfs_package
```

- 对于CentOS5，安装命令为：

```
sudo yum localinstall your_ossfs_package --nogpgcheck
```

#### 源码安装

如果没有找到对应的安装包，您也可以自行编译安装。编译前请先安装下列依赖库：

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

然后您可以在github上下载源码并编译安装：

```
git clone https://github.com/aliyun/ossfs.git
cd ossfs
./autogen.sh
./configure
make
sudo make install
```

### 运行

设置bucket name, access key/id信息，将其存放在/etc/passwd-ossfs 文件中，
注意这个文件的权限必须正确设置，建议设为640。如果不使用默认的文件路径，文件权限建议设置为600。

```
echo my-bucket:my-access-key-id:my-access-key-secret > /etc/passwd-ossfs
chmod 640 /etc/passwd-ossfs
```

将oss bucket mount到指定目录

```
ossfs my-bucket my-mount-point -ourl=my-oss-endpoint
```
#### 示例

将`my-bucket`这个bucket挂载到`/tmp/ossfs`目录下，AccessKeyId是`faint`，
AccessKeySecret是`123`，oss endpoint是`http://oss-cn-hangzhou.aliyuncs.com`

```
echo my-bucket:faint:123 > /etc/passwd-ossfs
chmod 640 /etc/passwd-ossfs
mkdir /tmp/ossfs
ossfs my-bucket /tmp/ossfs -ourl=http://oss-cn-hangzhou.aliyuncs.com
```

卸载bucket:

```bash
umount /tmp/ossfs # root user
fusermount -u /tmp/ossfs # non-root user
```

#### 常用设置

- 使用`ossfs --version`来查看当前版本，使用`ossfs -h`来查看可用的参数
- 如果使用ossfs的机器是阿里云ECS，可以使用内网域名来**避免流量收费**和
  **提高速度**：

        ossfs my-bucket /tmp/ossfs -ourl=http://oss-cn-hangzhou-internal.aliyuncs.com

- 在linux系统中，[updatedb][updatedb]会定期地扫描文件系统，如果不想
  ossfs的挂载目录被扫描，可参考[FAQ][FAQ-updatedb]设置跳过挂载目录
- 如果你没有使用[eCryptFs][ecryptfs]等需要[XATTR][xattr]的文件系统，可
  以通过添加`-o noxattr`参数来提升性能
- ossfs允许用户指定多组bucket/access_key_id/access_key_secret信息。当
  有多组信息，写入passwd-ossfs的信息格式为：

        bucket1:access_key_id1:access_key_secret1
        bucket2:access_key_id2:access_key_secret2

- 生产环境中推荐使用[supervisor][supervisor]来启动并监控ossfs进程，使
  用方法见[FAQ][faq-supervisor]

#### 高级设置

- 可以添加`-f -d`参数来让ossfs运行在前台并输出debug日志
- 可以使用`-o kernel_cache`参数让ossfs能够利用文件系统的page cache，如
  果你有多台机器挂载到同一个bucket，并且要求强一致性，请**不要**使用此
  选项

### 遇到错误

遇到错误不要慌:) 按如下步骤进行排查：

1. 如果有打印错误信息，尝试阅读并理解它
2. 查看`/var/log/syslog`或者`/var/log/messages`中有无相关信息

        grep 's3fs' /var/log/syslog
        grep 'ossfs' /var/log/syslog

3. 重新挂载ossfs，打开debug log：

        ossfs ... -o dbglevel=debug -f -d > /tmp/fs.log 2>&1

    然后重复你出错的操作，出错后将`/tmp/fs.log`保留，自己查看或者发给我

### 局限性

ossfs提供的功能和性能和本地文件系统相比，具有一些局限性。具体包括：

* 随机或者追加写文件会导致整个文件的重写。
* 元数据操作，例如list directory，性能较差，因为需要远程访问oss服务器。
* 文件/文件夹的rename操作不是原子的。
* 多个客户端挂载同一个oss bucket时，依赖用户自行协调各个客户端的行为。例如避免多个客户端写同一个文件等等。
* 不支持hard link。
* 不适合用在高并发读/写的场景，这样会让系统的load升高

### 参与开发

0. 开发流程参考：https://github.com/rockuw/oss-sdk-status#development-oss-members-only
1. 提交代码后，确保travis CI是PASS的
2. 每发布一个新的版本：
  - 运行`scripts/build-pkg.py`生成相应的安装包
  - 在[Release页面][releases]发布一个版本
  - 将生成的安装包上传到相应的Release下面

### 常见问题

[FAQ](https://github.com/aliyun/ossfs/wiki/FAQ)

### 相关链接

* [ossfs wiki](https://github.com/aliyun/ossfs/wiki)
* [s3fs](https://github.com/s3fs-fuse/s3fs-fuse) - 通过fuse接口，mount s3 bucket到本地文件系统。

### 联系我们

* [阿里云OSS官方网站](http://oss.aliyun.com/)
* [阿里云OSS官方论坛](http://bbs.aliyun.com/thread/211.html)
* [阿里云OSS官方文档中心](http://www.aliyun.com/product/oss#Docs)
* 阿里云官方技术支持：[提交工单](https://workorder.console.aliyun.com/#/ticket/createIndex)

### License

Copyright (C) 2010 Randy Rizun <rrizun@gmail.com>

Copyright (C) 2015 Haoran Yang <yangzhuodog1982@gmail.com>

Licensed under the GNU GPL version 2


[releases]: https://github.com/aliyun/ossfs/releases
[updatedb]: http://linux.die.net/man/8/updatedb
[faq-updatedb]: https://github.com/aliyun/ossfs/wiki/FAQ
[ecryptfs]: http://ecryptfs.org/
[xattr]: http://man7.org/linux/man-pages/man7/xattr.7.html
[supervisor]: http://supervisord.org/
[faq-supervisor]: https://github.com/aliyun/ossfs/wiki/FAQ#18
