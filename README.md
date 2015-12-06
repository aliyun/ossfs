# ossfs

### 简介

ossfs 能让您在Linux/Mac OS X 系统中把Aliyun OSS bucket 挂载到本地文件系统中，您能够便捷的通过本地文件系统操作OSS 上的对象，实现数据的共享。

### 功能

ossfs 基于s3fs 构建，具有s3fs 的全部功能。主要功能包括：

* 支持POSIX 文件系统的大部分功能，包括文件读写，目录，链接操作，权限，uid/gid，以及扩展属性（extended attributes）
* 通过OSS 的multipart 功能上传大文件。
* MD5 校验保证数据完整性。

### 安装

编译前请先安装下列依赖库：

Ubuntu 14.04:

```
sudo apt-get install automake autotools-dev g++ git libcurl4-gnutls-dev libfuse-dev libssl-dev libxml2-dev make pkg-config
```

CentOS 7.0:

```
sudo yum install automake fuse-devel gcc-c++ git libcurl-devel libxml2-devel make openssl-devel
```

然后您可以在github上下载源码并编译安装：

```
git clone https://github.com/aliyun/ossfs-fuse.git
cd ossfs-fuse
./autogen.sh
./configure
make
sudo make install
```

### 运行

设置bucket name, access key/id信息，将其存放在~/.passwd-ossfs 文件中，注意这个文件的权限必须正确设置，建议设为600。

```
echo your_bucket_name:your_key_id:your_key_secret > ~/.passwd-ossfs
chmod 600 ~/.passwd-ossfs
```

将oss bucket mount到指定目录

```
ossfs your_oss_bucket your_mount_dir -ourl=your_oss_service_url
```

示例

将ossfs-fuse这个bucket mount到/tmp/ossfs目录下，access key id是faint，access key secret是123，oss service url是http://oss-cn-hangzhou.aliyuncs.com

```
echo ossfs-fuse:faint:123 > ~/.passwd-ossfs
chmod 600 ~/.passwd-ossfs
mkdir /tmp/ossfs
ossfs ossfs-fuse /tmp/ossfs -ourl=http://oss-cn-hangzhou.aliyuncs.com
```

> 注1：ossfs的命令参数和s3fs相同，用户可以在启动ossfs时指定其他参数控制ossfs的行为，具体参见[s3fs文档](https://github.com/s3fs-fuse/s3fs-fuse/wiki/Fuse-Over-Amazon)。
> 
> 注2：ossfs允许用户指定多组bucket/access_key_id/access_key_secret信息。当有多组信息，写入.passwd-ossfs的信息格式为：
> 
> your_bucket_name1:your_access_key_id1:your_access_key_secret1
> 
> your_bucket_name2:your_access_key_id2:your_access_key_secret2
> 
> ......

### 局限性

ossfs提供的功能和性能和本地文件系统相比，具有一些局限性。具体包括：

* 随机或者追加写文件会导致整个文件的重写。
* 元数据操作，例如list directory，性能较差，因为需要远程访问oss服务器。
* 文件/文件夹的rename操作不是原子的。
* 多个客户端挂载同一个oss bucket时，依赖用户自行协调各个客户端的行为。例如避免多个客户端写同一个文件等等。
* 不支持hard link。

### 相关链接

* [s3fs](https://github.com/s3fs-fuse/s3fs-fuse) - 通过fuse接口，mount s3 bucket到本地文件系统。

### 联系我们

* [阿里云OSS官方网站](http://oss.aliyun.com/)
* [阿里云OSS官方论坛](http://bbs.aliyun.com/thread/211.html)
* [阿里云OSS官方文档中心](http://www.aliyun.com/product/oss#Docs)
* 阿里云官方技术支持：[提交工单](https://workorder.console.aliyun.com/#/ticket/createIndex)

### License

Copyright (C) 2010 Randy Rizun <rrizun@gmail.com>

Copyright (C) 2015 Haoran Yang <haoran.yanghr@alibaba-inc.com>

Licensed under the GNU GPL version 2
