FROM ubuntu:18.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get -y install --no-install-recommends \
            # build s3fs,ossfs dependencies \
            automake autotools-dev fuse g++ git libcurl4-gnutls-dev libfuse-dev \
            libssl-dev libxml2-dev make pkg-config \
            # for running benchmark \
            curl python-setuptools python-pip gnuplot-nox imagemagick awscli \
            # finally, clean up to make image smaller \
            && apt-get clean
# bench graph generation
RUN pip install numpy

# bench mdtest


WORKDIR /tmp

# build s3fs
RUN git clone --depth 1 https://github.com/s3fs-fuse/s3fs-fuse.git && \
    cd s3fs-fuse && ./autogen.sh && ./configure && make -j8 > /dev/null && make install && \
    cd .. && rm -Rf s3fs-fuse

# get prebuild bin
RUN curl -SL https://github.com/kahing/goofys/releases/latest/download/goofys -o /usr/local/bin/goofys && \
    chmod +x /usr/local/bin/goofys

#get ossfs
RUN git clone --depth 1 https://github.com/aliyun/ossfs.git && \
    cd ossfs && ./autogen.sh && ./configure && make -j8 > /dev/null && make install && \
    cp -rf ./bench /root/bench && \
    cd .. && rm -Rf ossfs

# build mdtest
# Install MPI & HDF5
#./mdtest -d ~/ya/mdtest -b 6 -I 8 -z 4
RUN apt-get install -y openmpi-bin libopenmpi-dev && \
    apt-get install -y hdf5-tools libhdf5-openmpi-dev
RUN git clone --depth 1 https://github.com/hpc/ior.git  && \
    cd ior && ./bootstrap && ./configure && make > /dev/null && make install && \
    cd .. && rm -Rf ior

ENTRYPOINT ["/root/bench/run_bench.sh"]
