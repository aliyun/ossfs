FROM rockylinux:9

RUN dnf install -y epel-release \
  && dnf config-manager --set-enabled crb \
  && dnf install -y automake gcc-c++ git libcurl-devel libxml2-devel fuse-devel make openssl-devel \
  && dnf install -y ruby \
  && dnf install -y rpm-build \
  && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
  && gem install dotenv -v 2.8.1 \
  && gem install fpm