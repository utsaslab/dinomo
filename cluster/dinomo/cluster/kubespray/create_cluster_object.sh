#!/bin/bash

if [[ -z "$1" ]]; then
    echo "Usage: ./create_cluster_object.sh path-to-ssh-key"
    echo ""
    echo "If no SSH key is specified, the default SSH key (~/.ssh/id_rsa) will be used."

    exit 1
fi

SSH_KEY=$1

PATH_NODE_LIST="$DINOMO_HOME/cluster/dinomo/cluster/kubespray"

MasterNodeIPs=($(<${PATH_NODE_LIST}/masterNode))
RoutingNodeIPs=($(<${PATH_NODE_LIST}/routingNodes))
BenchmarkNodeIPs=($(<${PATH_NODE_LIST}/benchmarkNodes))
MemoryNodeIPs=($(<${PATH_NODE_LIST}/memoryNodes))

numMaster=${#MasterNodeIPs[@]}
numRouting=${#RoutingNodeIPs[@]}
numBenchmark=${#BenchmarkNodeIPs[@]}
numMemory=${#MemoryNodeIPs[@]}

for ((i = 0; i < ${numMaster}; i++)); do
    sudo bash -c 'echo "${MasterNodeIPs[$i]} master$i" >> /etc/hosts'
done

for ((i = 0; i < ${numRouting}; i++)); do
    sudo bash -c 'echo "${RoutingNodeIPs[$i]} router$i" >> /etc/hosts'
done

for ((i = 0; i < ${numBenchmark}; i++)); do
    sudo bash -c 'echo "${BenchmarkNodeIPs[$i]} bench$i" >> /etc/hosts'
done

for ((i = 0; i < ${numMemory}; i++)); do
    sudo bash -c 'echo "${MemoryNodeIPs[$i]} worker$i" >> /etc/hosts'
done

cd $DINOMO_HOME/kubespray

sudo pip3 install -r requirements.txt

echo "Creating cluster object..."
ansible-playbook -i inventory/dinomo_cluster/inventory.ini --become \
    --user=${REMOTE_USER_NAME} --become-user=root cluster.yml \
    --private-key=${SSH_KEY}

echo "Finish creating cluster objects..."

mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

#sudo docker login

#sudo kubectl create secret docker-registry regcred --docker-server=<your-registry-server> --docker-username=<your-name> --docker-password=<your-pword> --docker-email=<your-email>
sudo kubectl create secret generic regcred --from-file=.dockerconfigjson=/home/${REMOTE_USER_NAME}/.docker/config.json --type=kubernetes.io/dockerconfigjson
sudo kubectl taint nodes master0 node-role.kubernetes.io/master:NoSchedule-

cd $DINOMO_HOME/cluster
