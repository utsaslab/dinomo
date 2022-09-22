## DINOMO: An Elastic, Scalable, High-Performance Key-Value Store for Disaggregated Persistent Memory (VLDB 2023, PVLDB 2022 Vol. 15 No. 13)
Dinomo is a novel key-value store for disaggregated persistent memory (DPM). 
Dinomo is the first key-value store for DPM that simultaneously achieves 
high common-case performance, scalability, and lightweight online reconfiguration.
This repository includes the implementations of our key-value store and relevant
tools to run experiments.

Please cite the following paper if you use Dinomo or compare it with your system:

```
@article{dinomo-vldb,
author = {Lee, Sekwon and Ponnapalli, Soujanya and Singhal, Sharad and Aguilera, Marcos K. and Keeton, Kimberly and Chidambaram, Vijay},
title = {DINOMO: An Elastic, Scalable, High-Performance Key-Value Store for Disaggregated Persistent Memory},
year = {2022},
publisher = {VLDB Endowment},
volume = {15},
number = {13},
issn = {2150-8097},
journal = {Proc. VLDB Endow.},
}
```


## Contents

1. `include/dinomo_compute.hpp`: Core implementation of `KN`.
2. `src/kvs/adaptive-cache.h`: Core implementation of `DAC`.
3. `src/kvs/dinomo_storage.cpp`: Core implementation of `DPM`.
4. `src/monitor/monitoring.cpp`: Core implementation of `M-node`.
5. `conf/dinomo-base.yml`: Configuration file for KVS


## Configure cluster
Dinomo uses [Kubernetes](https://kubernetes.io/) as a cluster orchestration tool.
We will build container images for each instance pod and make Kubernetes
automatically manage them. Dinomo cluster consists of a Monitoring/management 
node (`M-node`), Routing Nodes (`RNs`), KVS Nodes (`KNs`), `DPM`, and 
clients (running inside applications). To run Dinomo, the current implementation 
needs at least 5 separate physical machines to assign each pod for the node
instances exclusively to a separate physical machine. Among those physical machines,
at least 2 machines must be Infiniband-enabled for a `KN` and a `DPM`.

We use [Kubespray](https://github.com/kubernetes-sigs/kubespray) to generate 
Dinomo cluster orchestrated by Kubernetes over baremetal servers.
In the following instructions, we assume that a setting where 1 `M-node`, 1 `RN`, 1 `KN`,
1 `DPM`, and 1 application client exist. However, you can also increase the number 
of `RNs` and `KNs` according to your own setting. We assume the instructions 
specified in the following sections are excuted in one of the baremetal servers. 
This server will act as a master node where a Kubernetes master as well as a 
`M-node` pod are spawned and as a domain to control the overall cluster executions 
with a command line interface.

### Tested Environment
- Ubuntu18.04
- Connectx-3 RNIC

### Download source codes

```
$ cd ~/; mkdir projects; cd projects; git clone https://github.com/utsaslab/dinomo.git DINOMO; cd dinomo
```

### Install dependency

```
$ bash scripts/dependencies.sh
```

### Configure os environment variables
There are a few environment variables that should be configured according
to your own environment.

1. `$DINOMO_HOME`: Path to Dinomo home directory in the master node
2. `$REMOTE_USER_NAME`: Other machines's user name, user name should be same
across all the machines since the python script to generate cluster uses
the user name to access other machines over ssh and install required 
packages remotely.

```
$ export DINOMO_HOME=/home/cc/projects/dinomo
$ export REMOTE_USER_NAME=cc
```

### Configure the number of open file descriptors to be maximum (optional)
```
$ ulimit -n $(ulimit -Hn)
```

### Update configuration files
Update configuration files by specifying the ip addresses of nodes depending on their type.

```
$ vi kubespray/inventory/dinomo_cluster/inventory.ini
$ vi cluster/dinomo/cluster/kubespray/benchmarkNodes
$ vi cluster/dinomo/cluster/kubespray/masterNode
$ vi cluster/dinomo/cluster/kubespray/memoryNodes
$ vi cluster/dinomo/cluster/kubespray/routingNodes
$ vi cluster/dinomo/cluster/kubespray/storageNodes
```

### Create Dinomo Cluster

- Run the following python script to build up cluster configurations over baremetal servers. The script automatically accesses each server over ssh to apply the required configures.
```
Usage: python3 -m dinomo.cluster.create_cluster -m <# of KNs> -r <# of RNs> -b <# of ClientNodes>
$ cd cluster
$ python3 -m dinomo.cluster.create_cluster -m 1 -r 1 -b 1
```

- Check if the cluster configurations are completed properly. The following command should show the list of the server nodes currently managed by Kubernetes.
```
$ kubectl get nodes
```

### Build KVS source codes
```
$ cd ~/projects/dinomo
$ bash scripts/build.sh -bRelease -j8 -g
```

### Build Docker images for cluster instance pods and upload them to Dockerhub (optional)
We provide the pre-built container images for Dinomo cluster through [our public repository](https://hub.docker.com/repository/docker/sekwonlee/dinomo).
If you want to use your own images, please refer to the following script.

```
$ bash scripts/clean_build_docker.sh
```


## Run cluster
We execute Dinomo cluster on the master node and one of the Infiniband-enabled servers.

### Run DPM (on one of the Infiniband-enabled servers)
Note that the python script we used will only set up environments (installing 
required packages) and run nodes as Kubernetes pods except for DPM node. 
So, you should run DPM instance manually in the DPM machine you will use. 
Please download this source code to DPM side as well and follow the instructions 
below to run.

```
$ cd ~/projects/dinomo
$ bash scripts/dependencies.sh
$ bash scripts/build.sh -bRelease -j8 -g
$ ./build/target/kvs/dinomo-storage
```

### Run Dinomo cluster (on the master node)
```
Usage: python3 -m dinomo.cluster.run_cluster -m <# of KNs> -r <# of RNs> -b <# of ClientNodes>
$ cd cluster
$ python3 -m dinomo.cluster.run_cluster -m 1 -r 1 -b 1
```

#### Event trigger
After generating pods, the script will prompt an event trigger. The trigger 
supports seven types of commands to allow us to manually control the cluster. 
We used this trigger to manually control the load variance in our evaluation section.

1. `LOAD`: load the given number of key-value pairs to KVS (This command must be
first run before running workloads).

2. `RUN`: run a workload with particular read-write ratio and key distribution.

3. `WARM`: warm up KVS with the given number of requests as well as 
the specified workload pattern.

- `WARM-RUN`: run the workload again used in warm-up period for a specified time. This command should be used coupled with `WARM`.

4. `ADD`: spawn more application nodes (clients).

5. `REMOVE`: remove the specified number of application nodes (clients).

6. `FAIL`: simulate failures to the specified number of KNs.


## Contact
Please contact us at `sklee@cs.utexas.edu` or `vijayc@utexas.edu` with any questions.
