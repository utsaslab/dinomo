#!/bin/bash

USERNAME="cc"

DestinationIPs=($(<./cluster/dinomo/cluster/kubespray/benchmarkNodes))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/masterNode))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/memoryNodes))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/routingNodes))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/storageNodes))

numDestination=${#DestinationIPs[@]}

for ((i = 0; i < ${numDestination}; i++)); do
    ssh ${USERNAME}@${DestinationIPs[$i]}
done
