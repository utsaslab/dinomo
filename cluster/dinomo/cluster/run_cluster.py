import argparse
import os
import subprocess

from dinomo.cluster.add_nodes import batch_add_nodes
from dinomo.shared import util

BATCH_SIZE = 100

def run_cluster(mem_count, route_count, bench_count, cfile, ssh_key, kvs_type, cluster_name):
    if 'DINOMO_HOME' not in os.environ:
        raise ValueError('DINOMO_HOME environment variable must be set to be '
                         + 'the directory where all Dinomo project repos are '
                         + 'located.')

    prefix = os.path.join(os.environ['DINOMO_HOME'], 'cluster/dinomo/cluster')

    client, apps_client = util.init_k8s()

    print('Creating management pods...')
    management_spec = util.load_yaml('yaml/pods/management-pod.yml', prefix)
    env = management_spec['spec']['containers'][0]['env']

    util.replace_yaml_val(env, 'DINOMO_CLUSTER_NAME', cluster_name)

    client.create_namespaced_pod(namespace=util.NAMESPACE, body=management_spec)

    # Waits until the management pod starts to move forward -- we need to do
    # this because other pods depend on knowing the management pod's IP address.
    management_ip = util.get_pod_ips(client, 'role=management', is_running=True)[0]

    # Copy kube config file to management pod, so it can execute kubectl
    # commands, in addition to SSH keys and KVS config.
    management_podname = management_spec['metadata']['name']
    kcname = management_spec['spec']['containers'][0]['name']

    os.system('cp %s dinomo-config.yml' % cfile)
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
    master_ip = subprocess.getoutput("hostname -I | awk '{print $1}'")
    print(master_ip)
    os.system('sed -i "s|127.0.0.1|%s|g" %s' % (master_ip, kubecfg))
    util.copy_file_to_pod(client, kubecfg, management_podname, '/root/.kube/', kcname)
    util.copy_file_to_pod(client, ssh_key, management_podname, '/root/.ssh/', kcname)
    util.copy_file_to_pod(client, ssh_key + '.pub', management_podname, '/root/.ssh/', kcname)
    util.copy_file_to_pod(client, 'dinomo-config.yml', management_podname, '/DINOMO/conf/', kcname)

    # Start the monitoring pod.
    mon_spec = util.load_yaml('yaml/pods/monitoring-pod.yml', prefix)
    util.replace_yaml_val(mon_spec['spec']['containers'][0]['env'], 'MGMT_IP', management_ip)
    client.create_namespaced_pod(namespace=util.NAMESPACE, body=mon_spec)

    # Wait until the monitoring pod is finished creating to get its IP address
    # and then copy KVS config into the monitoring pod.
    util.get_pod_ips(client, 'role=monitoring')
    util.copy_file_to_pod(client, 'dinomo-config.yml', mon_spec['metadata']['name'],
                          '/DINOMO/conf/', mon_spec['spec']['containers'][0]['name'])
    os.system('rm dinomo-config.yml')

    print('Creating %d routing nodes...' % (route_count))
    batch_add_nodes(client, apps_client, cfile, ['routing'], [route_count], BATCH_SIZE, prefix)
    util.get_pod_ips(client, 'role=routing')

    print('Creating %d memory node(s)...' % (mem_count))
    if kvs_type == 'clover':
        batch_add_nodes(client, apps_client, cfile, ['clover'], [mem_count], BATCH_SIZE, prefix)
    elif kvs_type == 'asymnvm':
        batch_add_nodes(client, apps_client, cfile, ['asymnvm'], [mem_count], BATCH_SIZE, prefix)
    else:
        batch_add_nodes(client, apps_client, cfile, ['memory'], [mem_count], BATCH_SIZE, prefix)

    print('Creating %d benchmark node(s)...' % (bench_count))
    if kvs_type == 'clover':
        batch_add_nodes(client, apps_client, cfile, ['clover-bench'], [bench_count], BATCH_SIZE, prefix)
    elif kvs_type == 'asymnvm':
        batch_add_nodes(client, apps_client, cfile, ['asymnvm-bench'], [bench_count], BATCH_SIZE, prefix)
    else:
        batch_add_nodes(client, apps_client, cfile, ['benchmark'], [bench_count], BATCH_SIZE, prefix)

    print('Finished creating all pods...')
    os.system('touch setup_complete')
    util.copy_file_to_pod(client, 'setup_complete', management_podname, '/DINOMO/', kcname)
    os.system('rm setup_complete')

    print('Start benchmark trigger...')
    benchmark_ips = ' '.join(util.get_pod_ips(client, 'role=benchmark'))
    os.environ['BENCH_IPS'] = benchmark_ips
    management_ips = ' '.join(util.get_pod_ips(client, 'role=management'))
    os.environ['MGMT_IPS'] = management_ips
    os.system('cp ../conf/dinomo-base.yml ../conf/dinomo-config.yml')
    util.run_process(['./start-dinomo.sh', 'bt'], '../dockerfiles/')

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

    run_cluster(args.memory[0], args.routing[0], args.benchmark,
                   args.conf, args.sshkey, args.kvstype, cluster_name)
