FROM ubuntu:14.04

RUN    apt-get update \
    && apt-get install -y automake autotools-dev g++ git libcurl4-gnutls-dev curl\
    && apt-get install -y libfuse-dev libssl-dev libxml2-dev make pkg-config \
    && curl -SL -k "https://cache.ruby-china.com/pub/ruby/ruby-2.6.5.tar.gz" -o /opt/ruby-2.6.5.tar.gz\
    && cd /opt/ && tar zxvf ruby-2.6.5.tar.gz && cd ruby-2.6.5 \
    && ./configure && make -j10 && make install \
    && gem sources -a http://mirrors.aliyun.com/rubygems/ -r https://rubygems.org/ \
    && gem install fpm