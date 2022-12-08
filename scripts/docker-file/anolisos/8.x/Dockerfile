FROM openanolis/anolisos:8.6


RUN yum update -y \
    && yum install -y gcc gcc-c++ make cmake curl rpm-build\
    && yum install -y automake gcc-c++ git libcurl-devel libxml2-devel fuse-devel openssl-devel \
    && yum install -y ruby\
    && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
    && gem install fpm
