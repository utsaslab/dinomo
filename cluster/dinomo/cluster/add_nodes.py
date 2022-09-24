import random
import os

from dinomo.shared import util

# Generate list of all recently created pods.
def get_current_pod_container_pairs(pods):
    pod_container_pairs = set()
    for pod in pods:
        pname = pod.metadata.name
        hname = pod.spec.node_name
        for container in pod.spec.containers:
            cname = container.name
            pod_container_pairs.add((pname, cname, hname))
    return pod_container_pairs

def add_nodes(client, apps_client, cfile, kinds, counts, create=False, prefix=None):
    previously_created_pods_list = []
    expected_counts = []
    for i in range(len(kinds)):
        print('Adding %d %s server node(s) to cluster...' % (counts[i], kinds[i]))

        kind = kinds[i]
        if kinds[i] == 'clover' or kinds[i] == 'asymnvm':
            kind = 'memory'

        if kinds[i] == 'clover-bench' or kinds[i] == 'asymnvm-bench':
            kind = 'benchmark'

        pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                          label_selector='role=' + kind).items

        previously_created_pods_list.append(get_current_pod_container_pairs(pods))

        prev_count = util.get_previous_count(client, kind)
        util.run_process(['./add_servers.sh', kind, str(counts[i])], 'dinomo/cluster/kubespray')

        expected_counts.append(counts[i] + prev_count)

    management_ip = util.get_pod_ips(client, 'role=management')[0]
    route_ips = util.get_pod_ips(client, 'role=routing')

    if len(route_ips) > 0:
        seed_ip = random.choice(route_ips)
    else:
        seed_ip = ''

    mon_str = ' '.join(util.get_pod_ips(client, 'role=monitoring'))
    route_str = ' '.join(route_ips)

    for i in range(len(kinds)):
        kind = kinds[i]

        # Create should only be true when the DaemonSet is being created for the
        # first time -- i.e., when this is called from create_cluster. After that,
        # we can basically ignore this because the DaemonSet will take care of
        # adding pods to created nodes.
        if create:
            fname = 'yaml/ds/%s-ds.yml' % kind
            yml = util.load_yaml(fname, prefix)

            for container in yml['spec']['template']['spec']['containers']:
                env = container['env']

                util.replace_yaml_val(env, 'ROUTING_IPS', route_str)
                util.replace_yaml_val(env, 'MON_IPS', mon_str)
                util.replace_yaml_val(env, 'MGMT_IP', management_ip)
                util.replace_yaml_val(env, 'SEED_IP', seed_ip)

            apps_client.create_namespaced_daemon_set(namespace=util.NAMESPACE, body=yml)

        if kind == 'clover' or kind == 'asymnvm':
            kind = 'memory'

        if kind == 'clover-bench' or kind == 'asymnvm-bench':
            kind = 'benchmark'

        # Wait until all pods of this kind are running
        res = []
        while len(res) != expected_counts[i]:
            res = util.get_pod_ips(client, 'role=' + kind, is_running=True)

        pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                          label_selector='role=' + kind).items

        created_pods = get_current_pod_container_pairs(pods)

        new_pods = created_pods.difference(previously_created_pods_list[i])

        # Copy the KVS config into all recently created pods.
        for pname, cname, hname in new_pods:
            os.system('cp %s ./dinomo-config.yml' % cfile)
            if kind == 'memory':
                os.system('sed -i "s|NODE_UID|%s|g" ./dinomo-config.yml' % hname[6:])
            util.copy_file_to_pod(client, 'dinomo-config.yml', pname, '/DINOMO/conf/', cname)
            os.system('rm ./dinomo-config.yml')

def batch_add_nodes(client, apps_client, cfile, node_types, node_counts, batch_size, prefix):
    if sum(node_counts) <= batch_size:
        add_nodes(client, apps_client, cfile, node_types, node_counts, True, prefix)
    else:
        for i in range(len(node_types)):
            if node_counts[i] <= batch_size:
                batch_add_nodes(client, apps_client, cfile, [node_types[i]], [node_counts[i]], batch_size, prefix)
            else:
                batch_count = 1
                print('Batch %d: adding %d nodes...' % (batch_count, batch_size))
                add_nodes(client, apps_client, cfile, [node_types[i]], [batch_size], True, prefix)
                remaining_count = node_counts[i] - batch_size
                batch_count += 1
                while remaining_count > 0:
                    if remaining_count <= batch_size:
                        print('Batch %d: adding %d nodes...' % (batch_count, remaining_count))
                        add_nodes(client, apps_client, cfile, [node_types[i]],
                                  [remaining_count], False, prefix)
                        remaining_count = 0
                    else:
                        print('Batch %d: adding %d nodes...' % (batch_count, batch_size))
                        add_nodes(client, apps_client, cfile, [node_types[i]],
                                  [batch_size], False, prefix)
                        remaining_count = remaining_count - batch_size
                    batch_count += 1
