#!/bin/bash

MasterNodeIPs=($(<./masterNode))
RoutingNodeIPs=($(<./routingNodes))
BenchmarkNodeIPs=($(<./benchmarkNodes))
MemoryNodeIPs=($(<./memoryNodes))
StorageNodeIPs=($(<./storageNodes))

numMaster=${#MasterNodeIPs[@]}
numRouting=${#RoutingNodeIPs[@]}
numBenchmark=${#BenchmarkNodeIPs[@]}
numMemory=${#MemoryNodeIPs[@]}
numStorage=${#StorageNodeIPs[@]}

PREPARE_CLUSTER_CONFIG="sudo sysctl -w net.ipv4.ip_forward=1 && sudo ufw disable && \
    sudo ln -Tfs /usr/bin/python3 /usr/bin/python"

retVal=""

PREPARE_IB_CONFIG() {
    retVal="mkdir -p projects/downloads && cd projects/downloads && \
        wget http://content.mellanox.com/ofed/MLNX_OFED-4.9-0.1.7.0/MLNX_OFED_LINUX-4.9-0.1.7.0-ubuntu18.04-x86_64.tgz && \
        tar xvf MLNX_OFED_LINUX-4.9-0.1.7.0-ubuntu18.04-x86_64.tgz && \
        cd MLNX_OFED_LINUX-4.9-0.1.7.0-ubuntu18.04-x86_64 && \
        sudo ./mlnxofedinstall --without-neohost-backend --without-neohost-sdk --force && \
        sudo bash -c 'echo \"auto ib0\" >> /etc/network/interfaces' && \
        sudo bash -c 'echo \"iface ib0 inet static\" >> /etc/network/interfaces' && \
        sudo bash -c 'echo \"address 10.0.0.$1\" >> /etc/network/interfaces' && \
        sudo bash -c 'echo \"netmask 255.255.255.0\" >> /etc/network/interfaces' && \
        sudo bash -c 'echo \"broadcast 10.0.0.255\" >> /etc/network/interfaces' && \
        sudo /etc/init.d/openibd restart && \
        sudo modprobe -a mlx4_core mlx4_ib mlx4_en mlx5_core mlx5_ib \
        mlx5_fpga_tools ib_umad ib_uverbs ib_ipoib rdma_cm ib_ucm rdma_ucm;"
}

#PREPARE_IB_CONFIG() {
#    retVal="mkdir -p projects/downloads && cd projects/downloads && \
#        wget https://content.mellanox.com/ofed/MLNX_OFED-5.5-1.0.3.2/MLNX_OFED_LINUX-5.5-1.0.3.2-ubuntu18.04-x86_64.tgz && \
#        tar xvf MLNX_OFED_LINUX-5.5-1.0.3.2-ubuntu18.04-x86_64.tgz && \
#        cd MLNX_OFED_LINUX-5.5-1.0.3.2-ubuntu18.04-x86_64 && \
#        sudo ./mlnxofedinstall --without-dkms --add-kernel-support --kernel $(uname -r) --without-neohost-backend --without-neohost-sdk --without-fw-update --force && \
#        sudo bash -c 'echo \"auto ib0\" >> /etc/network/interfaces' && \
#        sudo bash -c 'echo \"iface ib0 inet static\" >> /etc/network/interfaces' && \
#        sudo bash -c 'echo \"address 10.0.0.$1\" >> /etc/network/interfaces' && \
#        sudo bash -c 'echo \"netmask 255.255.255.0\" >> /etc/network/interfaces' && \
#        sudo bash -c 'echo \"broadcast 10.0.0.255\" >> /etc/network/interfaces' && \
#        sudo /etc/init.d/openibd restart && \
#        sudo modprobe -a mlx4_core mlx4_ib mlx4_en mlx5_core mlx5_ib \
#        mlx5_fpga_tools ib_umad ib_uverbs ib_ipoib rdma_cm ib_ucm rdma_ucm;"
#}

for ((i = 0; i < ${numMaster}; i++)); do
    ssh ${REMOTE_USER_NAME}@${MasterNodeIPs[$i]} ${PREPARE_CLUSTER_CONFIG}
done

for ((i = 0; i < ${numRouting}; i++)); do
    ssh ${REMOTE_USER_NAME}@${RoutingNodeIPs[$i]} ${PREPARE_CLUSTER_CONFIG}
done

for ((i = 0; i < ${numBenchmark}; i++)); do
    ssh ${REMOTE_USER_NAME}@${BenchmarkNodeIPs[$i]} ${PREPARE_CLUSTER_CONFIG}
done

pidList=[]
for ((i = 0; i < ${numMemory}; i++)); do
    ssh ${REMOTE_USER_NAME}@${MemoryNodeIPs[$i]} ${PREPARE_CLUSTER_CONFIG}
    if ssh ${REMOTE_USER_NAME}@${MemoryNodeIPs[$i]} "ifconfig | grep ib0"
    then
        :
    else
        PREPARE_IB_CONFIG $((i + 1))
        ssh ${REMOTE_USER_NAME}@${MemoryNodeIPs[$i]} ${retVal} &
        pidList[$((i))]=$!
    fi
done

for ((i = 0; i < ${numStorage}; i++)); do
    ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} ${PREPARE_CLUSTER_CONFIG}
    if ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} "ifconfig | grep ib0"
    then
        :
    else
        PREPARE_IB_CONFIG $((i + numMemory + 1))
        ssh ${REMOTE_USER_NAME}@${StorageNodeIPs[$i]} ${retVal} &
        pidList[$((i + numMemory))]=$!
    fi
done

for pid in ${pidList[*]}; do
    wait $pid
done
