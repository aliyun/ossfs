FROM centos:centos8

RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN yum update  -y \
    && yum install -y gcc gcc-c++ make cmake curl rpm-build\
    && yum install -y automake gcc-c++ git libcurl-devel libxml2-devel fuse-devel openssl-devel \
    && yum install -y ruby \
    && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
    && gem install fpm
