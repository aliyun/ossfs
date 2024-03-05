FROM ubuntu:16.04

RUN    apt-get update \
    && apt-get install -y software-properties-common

RUN    add-apt-repository ppa:brightbox/ruby-ng \
    && apt-get update \
    && apt-get install -y automake autotools-dev g++ git libcurl4-gnutls-dev curl\
    && apt-get install -y libfuse-dev libssl-dev libxml2-dev make pkg-config \
    && apt-get install -y ruby2.6 ruby2.6-dev \
    && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
    && gem install dotenv -v 2.8.1 \
    && gem install fpm