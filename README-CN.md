# OSSFS

[![Version](https://badge.fury.io/gh/aliyun%2Fossfs.svg)][releases]
[![Build Status](https://travis-ci.org/aliyun/ossfs.svg?branch=master)](https://travis-ci.org/aliyun/ossfs?branch=master)

### [README of English](https://github.com/aliyun/ossfs/blob/master/README.md)

### 简介

ossfs 能让您在Linux/Mac OS X 系统中把Aliyun OSS bucket 挂载到本地文件
系统中，您能够便捷的通过本地文件系统操作OSS 上的对象，实现数据的共享。

### 功能

ossfs 基于s3fs 构建，具有s3fs 的全部功能。主要功能包括：

* 支持POSIX 文件系统的大部分功能，包括文件读写，目录，链接操作，权限，uid/gid，以及扩展属性（extended attributes）
* 支持随机写和追加写
* 大文件通过分片方式(multi-part api)上传
* 重名名通过拷贝(single-part copy/multi-part copy)接口
* 可选择开启服务端加密
* 支持MD5校验保证数据完整性
* 使用内存缓存元数据
* 依赖本地文件作为缓存

除了以上功能, ossfs 还具备自己的特性.
* 默认使用single-part拷贝以提升大文件的重名性能
* 可选符号链接信息保存在对象的元数据里
* 可选开启目录读取优化模式，该模式下会忽略如下文件信息，atime/ctime, uid/gid 和 permissions
* 可选开启直读模式，该模式下，读取文件时，数据下载到内存而非磁盘，再从内存中读取，从而提升顺序读性能

### 安装

#### 预编译的安装包

我们为常见的linux发行版制作了安装包：

- Ubuntu-14.04 or later
- CentOS-7.0 or later
- Anolis-7 or later

请从[版本发布页面][releases]选择对应的安装包下载安装，建议选择最新版本。

- 对于Ubuntu，安装命令为：

```
sudo apt-get update
sudo apt-get install gdebi-core
sudo gdebi your_ossfs_package
```

- 对于CentOS，安装命令为：

```
sudo yum localinstall your_ossfs_package
```

- 对于Anolis，安装命令为：

```
sudo yum localinstall your_ossfs_package
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

其它平台，请参阅[编译说明](COMPILATION.md)

### 运行

ossfs的密钥文件的默认路径如下:
* 用户主目录中 `.passwd-ossfs` 文件 ( 例如 `${HOME}/.passwd-ossfs`)
* 系统路径文件 `/etc/passwd-ossfs`

设置bucket name, access key/id信息，将其存放在`${HOME}/.passwd-ossfs` 文件中，并设置成仅限所有者的权限，即600。
如果密钥文件路径为`/etc/passwd-ossfs`, 可以设置成640


```
echo my-access-key-id:my-access-key-secret > ${HOME}/.passwd-ossfs
chmod 600 ${HOME}/.passwd-ossfs
```

将 `my-bucket` 挂载到指定目录`/path/to/mountpoint`

```
ossfs my-bucket /path/to/mountpoint -ourl=my-oss-endpoint
```

如果您在使用ossfs的过程中遇到错误，可以开启调试日志:

```
ossfs my-bucket /path/to/mountpoint -ourl=my-oss-endpoint -o dbglevel=info -f -o curldbg
```

您可以在`/etc/fstab`加入以下命令，在开机自动挂载目录:

```
my-bucket /path/to/mountpoint fuse.ossfs _netdev,allow_other,url=my-oss-endpoint 0 0
```

注意一: 您需要将密钥等信息写入`/etc/passwd-ossfs`文件里, 并将文件权限修改为640

```
echo my-access-key-id:my-access-key-secret > /etc/passwd-ossfs
chmod 600 /etc/passwd-ossfs
```

注意二:您可能还需要确保`netfs`服务已经启动

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

- 在linux系统中，[updatedb][updatedb]会定期地扫描文件系统，
如果不想ossfs的挂载目录被扫描，可参考[FAQ][FAQ-updatedb]设置跳过挂载目录
- ossfs允许用户指定多组bucket/access_key_id/access_key_secret信息。
当有多组信息，写入passwd-ossfs的信息格式为：

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

* 随机或者追加写文件会导致整个文件的重写
* 元数据操作，例如list directory，性能较差，因为需要远程访问oss服务器
* 文件/文件夹的rename操作不是原子的
* 多个客户端挂载同一个oss bucket时，依赖用户自行协调各个客户端的行为。例如避免多个客户端写同一个文件等等
* 不支持hard link
* 仅检测本地修改，而不检测其他客户端或工具的外部修改
* 不适合用在高并发读/写的场景，这样会让系统的load升高

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
