#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./add_servers.sh node-type instance-count"
    echo "Valid node types are memory and routing."
    echo "The number of requested instance count should be specified."
    exit 1
fi

node_type=""
if [[ "$1" == "memory" ]]; then
    node_type="worker"
elif [[ "$1" == "routing" ]]; then
    node_type="router"
else
    node_type="bench"
fi

instance_count=$2

nodeList=""
#nodeList=($(kubectl get nodes --show-labels | grep Ready | grep ${node_type} | grep -v $1 | awk '{print $1}'))
if [[ "$1" == "benchmark" ]]; then
    nodeList=($(kubectl get nodes --show-labels | grep Ready | grep ${node_type} | grep -v $1 | awk '{print $1}' | sort -n -k1.6))
else
    nodeList=($(kubectl get nodes --show-labels | grep Ready | grep ${node_type} | grep -v $1 | awk '{print $1}' | sort -n -k1.7))
fi

numNodes=${#nodeList[@]}

echo "numNodes = ${numNodes} and instance_count = ${instance_count}"
if [ "$numNodes" -ge "$instance_count" ]; then
    for ((i = 0; i < ${instance_count}; i++)); do
        kubectl label nodes ${nodeList[$i]} role=$1 > /dev/null 2>&1
        kubectl uncordon ${nodeList[$i]} > /dev/null 2>&1
    done
else
    echo "# of requested nodes is greater than currently available nodes"
    echo "We just provision the number of nodes as much as currently available"

    for ((i = 0; i < ${numNodes}; i++)); do
        kubectl label nodes ${nodeList[$i]} role=$1 > /dev/null 2>&1
        kubectl uncordon ${nodeList[$i]} > /dev/null 2>&1
    done
fi
