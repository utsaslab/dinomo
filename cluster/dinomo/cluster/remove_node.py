from dinomo.shared import util

def remove_node(ip, ntype):
    client, _ = util.init_k8s()

    hostname = util.get_hostname_from_ip(client, ip)

    util.run_process(['./remove_server.sh', hostname, ntype], 'dinomo/cluster/kubespray')
