#!/bin/bash

USERNAME="cc"

DestinationIPs=($(<./cluster/dinomo/cluster/kubespray/benchmarkNodes))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/masterNode))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/memoryNodes))
DestinationIPs+=($(<./cluster/dinomo/cluster/kubespray/routingNodes))

numDestination=${#DestinationIPs[@]}

pidList=[]
for ((i = 0; i < ${numDestination}; i++)); do
    ssh ${USERNAME}@${DestinationIPs[$i]} 'sudo docker image rm $(sudo docker image ls --format "{{.Repository}} {{.ID}}" | grep "sekwonlee" | awk "{print $2}")' &
    pidList[i]=$!
done

while :
do
    finishCnt="0"
    for ((i = 0; i < ${numDestination}; i++)); do
        if [ -n ${pidList[i]} -a -e /proc/${pidList[i]} ]; then
            :
        else
            finishCnt=$(($finishCnt + 1))
        fi
    done

    if [ $finishCnt -eq $numDestination ]; then
        break
    fi
done
