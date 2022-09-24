#!/bin/bash

#IP=`ifconfig eno1 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`
IP=`hostname -I | awk '{print $1}'`

cd $DINOMO_HOME

git pull https://github.com/utsaslab/dinomo.git

cd client/python && python3.6 setup.py install --prefix=$HOME/.local

cd $DINOMO_HOME/cluster

# Generate compiled Python protobuf libraries from other Hydro project
# repositories. This is really a hack, but it shouldn't matter too much because
# this code should only ever be run in cluster mode, where this will be
# isolated from the user.
./scripts/compile-proto.sh

# Start the management servers. Add the current directory to the PYTHONPATH to
# be able to run its scripts.
cd $DINOMO_HOME/cluster
export PYTHONPATH=$PYTHONPATH:$(pwd)
python3.6 dinomo/management/k8s_server.py &
python3.6 dinomo/management/management_server.py $IP
