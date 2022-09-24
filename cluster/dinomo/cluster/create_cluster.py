import argparse
import os
import subprocess

from dinomo.cluster.add_nodes import batch_add_nodes
from dinomo.shared import util

BATCH_SIZE = 100

def create_cluster(mem_count, route_count, bench_count, cfile, ssh_key, kvs_type, cluster_name):
    if 'DINOMO_HOME' not in os.environ:
        raise ValueError('DINOMO_HOME environment variable must be set to be '
                         + 'the directory where all Dinomo project repos are '
                         + 'located.')

    prefix = os.path.join(os.environ['DINOMO_HOME'], 'cluster/dinomo/cluster')

    # Prepare to configure a cluster by installing dependencies
    util.run_process(['./prepare_host_configurations.sh', ssh_key], 'dinomo/cluster/kubespray')

    # Run disaggregated storage node
    util.run_process(['./run_storage_nodes.sh'], 'dinomo/cluster/kubespray')

    # Organize an initial cluster through kubespray
    util.run_process(['./create_cluster_object.sh', ssh_key], 'dinomo/cluster/kubespray')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Creates a Dinomo cluster
                                    using Kubernetes and Kubespray. If no SSH key
                                    is specified, we use the default SSH key
                                    (~/.ssh/id_rsa), and we expect that the corresponding
                                    public key has the same path and ends in .pub.

                                    If no configuration file base is specified, we use the
                                    default ($DINOMO_HOME/conf/dinomo-base.yml).''')

    if 'DINOMO_HOME' not in os.environ:
        os.environ['DINOMO_HOME'] = "/home/cc/projects/DINOMO/"
    if 'DINOMO_CLUSTER_NAME' not in os.environ:
        os.environ['DINOMO_CLUSTER_NAME'] = "DINOMO_k8s_CLUSTER"
    if 'REMOTE_USER_NAME' not in os.environ:
        os.environ['REMOTE_USER_NAME'] = "cc"

    parser.add_argument('-m', '--memory', nargs=1, type=int, metavar='M',
                        help='The number of memory nodes to start with ' +
                        '(required)', dest='memory', required=True)
    parser.add_argument('-r', '--routing', nargs=1, type=int, metavar='R',
                        help='The number of routing nodes in the cluster ' +
                        '(required)', dest='routing', required=True)
    parser.add_argument('-b', '--benchmark', nargs='?', type=int, metavar='B',
                        help='The number of benchmark nodes in the cluster ' +
                        '(optional)', dest='benchmark', default=0)
    parser.add_argument('--conf', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='conf',
                        default=os.path.join(os.getenv('DINOMO_HOME', '..'),
                                             'conf/dinomo-base.yml'))
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'], '.ssh/id_rsa'))
    parser.add_argument('--kvs-type', nargs='?', type=str,
                        help='The type of kvs systems used to choose backend datastores',
                        dest='kvstype', default='memory')

    cluster_name = util.check_or_get_env_arg('DINOMO_CLUSTER_NAME')

    args = parser.parse_args()

    create_cluster(args.memory[0], args.routing[0], args.benchmark,
                   args.conf, args.sshkey, args.kvstype, cluster_name)
