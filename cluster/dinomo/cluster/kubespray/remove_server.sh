#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./remove_server.sh hostname (node name) node-type"
    echo "If the hostname is not specified, we just ignore this request."
    echo "Valid node types are memory and routing."
    exit 1
fi

# Safely evict the pods from the node that we are trying to delete
kubectl drain $1 --ignore-daemonsets --delete-local-data > /dev/null 2>&1

kubectl label node $1 role- > /dev/null 2>&1
