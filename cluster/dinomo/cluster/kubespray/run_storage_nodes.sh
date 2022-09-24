#!/bin/bash

StorageNodeIPs=($(<./storageNodes))

numStorage=${#StorageNodeIPs[@]}

PREPARE_STORAGE_CONFIG="sudo apt-get install -y autoconf binutils-dev bison cmake \
    flex g++ gcc git libboost1.65-all-dev libbz2-dev libdouble-conversion-dev \
    libevent-dev libgflags-dev libgoogle-glog-dev libjemalloc-dev \
    liblz4-dev liblzma-dev liblzma5 libsnappy-dev libsodium-dev \
    libssl-dev libtool libunwind8-dev make pkg-config python-dev \
    ragel default-jdk libmemcached-tools libmemcached-dev libtbb-dev \
    libjemalloc-dev libpmem-dev librpmem-dev libpmemblk-dev \
    libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev \
    libhugetlbfs-dev hugepages libibverbs-dev librdmacm-dev \
    build-essential autoconf automake libtool curl make \
    unzip pkg-config wget curl git vim jq software-properties-common \
    libzmq3-dev git gcc libpq-dev libssl-dev cmake \
    openssl libffi-dev zlib1g-dev net-tools && \
    cd projects && \
    git clone https://github.com/utsaslab/dinomo.git DINOMO && \
    cp /home/${REMOTE_USER_NAME}/dinomo-config.yml /home/${REMOTE_USER_NAME}/projects/DINOMO/conf/ && \
    cd DINOMO/common/scripts && sudo ./install-dependencies.sh g++ && \
    cd ../../ && bash scripts/build.sh -bRelease -j8 -g;"

for ((i = 0; i < ${numStorage}; i++)); do
    if [[ -z "${REMOTE_USER_NAME}" ]]; then
        scp /home/${REMOTE_USER_NAME}/projects/DINOMO/conf/dinomo-base.yml ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]}:/home/${REMOTE_USER_NAME}/dinomo-config.yml
        ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} ${PREPARE_STORAGE_CONFIG} 
        scp /home/${REMOTE_USER_NAME}/projects/DINOMO/conf/dinomo-base.yml ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]}:/home/${REMOTE_USER_NAME}/projects/DINOMO/conf/dinomo-config.yml
    else
        scp /home/${REMOTE_USER_NAME}/projects/DINOMO/conf/dinomo-base.yml ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]}:/home/${REMOTE_USER_NAME}/dinomo-config.yml
        ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} ${PREPARE_STORAGE_CONFIG} 
        scp /home/${REMOTE_USER_NAME}/projects/DINOMO/conf/dinomo-base.yml ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]}:/home/${REMOTE_USER_NAME}/projects/DINOMO/conf/dinomo-config.yml
    fi
done

#for ((i = 0; i < ${numStorage}; i++)); do
#    if [[ -z "${REMOTE_USER_NAME}" ]]; then
#        ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} "cd projects/DINOMO && ./build/target/kvs/dinomo-storage;" &
#    else
#        ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} "cd projects/DINOMO && sudo ./build/target/kvs/dinomo-storage;" &
#    fi
#done
