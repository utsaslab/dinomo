FROM ubuntu:18.04

MAINTAINER Sekwon Lee <sekwonlee90@gmail.com> version: 0.1

USER root

# Install all apt-packages that are shared across the whole project.
RUN apt-get update
RUN apt-get install -y build-essential autoconf automake libtool curl make \
      unzip pkg-config wget curl git vim jq software-properties-common \
      libzmq3-dev git gcc libpq-dev libssl-dev cmake \
      openssl libffi-dev zlib1g-dev net-tools

# Install python packages
RUN apt-get install -y python3-distutils
RUN apt-get install -y python3-pip
RUN pip3 install awscli cloudpickle zmq protobuf==3.19.4 boto3 kubernetes six

# Clone the repo of DINOMO project.
RUN git clone https://github.com/utsaslab/dinomo.git DINOMO

# Create and populate DINOMO project context
ENV DINOMO_HOME /DINOMO

# Install common dependencies (cmake, lcov, protobuf)
WORKDIR /DINOMO/common/scripts
RUN bash install-dependencies.sh g++

RUN apt-get install -y autoconf binutils-dev bison cmake flex g++ gcc \
    gdb git libboost1.65-all-dev libbz2-dev libdouble-conversion-dev \
    libevent-dev libgflags-dev libgoogle-glog-dev libjemalloc-dev \
    liblz4-dev liblzma-dev liblzma5 libsnappy-dev libsodium-dev \
    libssl-dev libtool libunwind8-dev make pkg-config python-dev \
    ragel default-jdk libmemcached-tools libmemcached-dev libtbb-dev \
    libjemalloc-dev libpmem-dev librpmem-dev libpmemblk-dev \
    libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev \
    libhugetlbfs-dev hugepages libibverbs-dev librdmacm-dev libpapi-dev

WORKDIR /
