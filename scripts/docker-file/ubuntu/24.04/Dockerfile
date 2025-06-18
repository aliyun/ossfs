FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN    apt-get update \
    && apt-get install -y automake autotools-dev g++ git libcurl4-gnutls-dev curl\
    && apt-get install -y libfuse-dev libssl-dev libxml2-dev make pkg-config \
    && apt-get install -y ruby \
    && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
    && gem install fpm
