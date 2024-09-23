FROM centos:centos7

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
RUN yum update upgrade -y \
    && yum install -y gcc gcc-c++ make cmake curl rpm-build \
    && yum install -y automake gcc-c++ git libcurl-devel libxml2-devel fuse-devel openssl-devel \
    && curl -SL "https://cache.ruby-china.com/pub/ruby/ruby-2.6.5.tar.gz" -o /opt/ruby-2.6.5.tar.gz\
    && cd /opt/ && tar zxvf ruby-2.6.5.tar.gz && cd ruby-2.6.5 \
    && ./configure && make -j10 && make install \
    && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
    && gem install dotenv -v 2.8.1 \
    && gem install fpm
