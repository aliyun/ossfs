FROM alibaba-cloud-linux-4-registry.cn-hangzhou.cr.aliyuncs.com/alinux4/alinux4:latest

RUN yum update -y 
RUN yum install -y gcc gcc-c++ make cmake curl rpm-build
RUN yum install -y automake gcc-c++ git libcurl-devel libxml2-devel fuse-devel openssl-devel ruby \
  && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
  && gem install dotenv -v 2.8.1 \
  && gem install fpm
