import logging
import os
import random
import time
import sys

import zmq

from anna.zmq_util import SocketCache

from dinomo.management.scaler.default_scaler import DefaultScaler
from dinomo.management.util import (
    get_monitoring_depart_address,
    get_routing_depart_address,
    get_routing_seed_address,
    get_storage_depart_address,
    get_storage_failover_address,
    send_message
)

from dinomo.shared import util
#from dinomo.shared.proto.internal_pb2 import ThreadStatus, ExecutorStatistics
#from dinomo.shared.proto.shared_pb2 import StringSet
from dinomo.shared.proto.metadata_pb2 import ClusterMembership, MEMORY

REPORT_PERIOD = 5

PIN_ACCEPT_PORT = '5010'

logging.basicConfig(filename = 'log_management.txt', level = logging.INFO,
                    format = '%(asctime)s %(message)s')

def run(self_ip):
    context = zmq.Context(1)

    pusher_cache = SocketCache(context, zmq.PUSH)

    restart_pull_socket = context.socket(zmq.REP)
    restart_pull_socket.bind('tcp://*:8100')
    #restart_pull_socket.bind('tcp://*:7000')

    churn_pull_socket = context.socket(zmq.PULL)
    churn_pull_socket.bind('tcp://*:8101')
    #churn_pull_socket.bind('tcp://*:7001')

    list_executors_socket = context.socket(zmq.PULL)
    list_executors_socket.bind('tcp://*:8102')
    #list_executors_socket.bind('tcp://*:7002')

    pin_accept_socket = context.socket(zmq.PULL)
    pin_accept_socket.setsockopt(zmq.RCVTIMEO, 10000) # 10 seconds.
    pin_accept_socket.bind('tcp://*:' + PIN_ACCEPT_PORT)

    poller = zmq.Poller()
    poller.register(restart_pull_socket, zmq.POLLIN)
    poller.register(churn_pull_socket, zmq.POLLIN)
    poller.register(list_executors_socket, zmq.POLLIN)

    add_push_socket = context.socket(zmq.PUSH)
    add_push_socket.connect('ipc:///tmp/node_add')

    remove_push_socket = context.socket(zmq.PUSH)
    remove_push_socket.connect('ipc:///tmp/node_remove')

    client, _ = util.init_k8s()

    scaler = DefaultScaler(self_ip, context, add_push_socket, remove_push_socket)

    kvs_spec = util.load_yaml('/DINOMO/conf/dinomo-config.yml')
    num_memory_threads = kvs_spec['threads']['memory']
    num_routing_threads = kvs_spec['threads']['routing']

    start = time.time()
    while True:
        socks = dict(poller.poll(timeout=1000))

        # 7001 --> 8101
        if (churn_pull_socket in socks and socks[churn_pull_socket] == zmq.POLLIN):
            msg = churn_pull_socket.recv_string()
            args = msg.split(':')

            if args[0] == 'add':
                scaler.add_vms(args[2], args[1])
            elif args[0] == 'remove':
                scaler.remove_vms(args[2], args[1])

        # 7000 --> 8100
        if (restart_pull_socket in socks and socks[restart_pull_socket] == zmq.POLLIN):
            msg = restart_pull_socket.recv_string()
            args = msg.split(':')

            pod = util.get_pod_from_ip(client, args[1])
            count = str(pod.status.container_statuses[0].restart_count)

            restart_pull_socket.send_string(count)

        '''
        # 7002
        # This context is assumed not to be used for Dinomo (even if it is called for reporting function status)
        if (list_executors_socket in socks and socks[list_executors_socket] == zmq.POLLIN):
            # We can safely ignore this message's contents, and the response
            # does not depend on it.
            response_ip = list_executors_socket.recv_string()

            ips = StringSet()
            for ip in util.get_pod_ips(client, 'role=function'):
                ips.keys.append(ip)
            for ip in util.get_pod_ips(client, 'role=gpu'):
                ips.keys.append(ip)

            sckt = pusher_cache.get(response_ip)
            sckt.send(ips.SerializeToString())
        '''

        end = time.time()
        if end - start > REPORT_PERIOD: # Check every 5 seconds
            logging.info('Checking hash ring...')
            check_hash_ring(client, context, num_memory_threads, num_routing_threads)

            # Restart the timer for the next reporting epoch.
            start = time.time()


def check_hash_ring(client, context, num_memory_threads, num_routing_threads):
    route_ips = util.get_pod_ips(client, 'role=routing')

    # If there are no routing nodes in the system currently, the system is
    # still starting, so we do nothing.
    if not route_ips:
        return

    ip = random.choice(route_ips)

    # Retrieve a list of all current members of the cluster.
    socket = context.socket(zmq.REQ)
    socket.connect(get_routing_seed_address(ip, 0))
    socket.send_string('')
    resp = socket.recv()

    cluster = ClusterMembership()
    cluster.ParseFromString(resp)
    tiers = cluster.tiers

    # If there are no tiers, then we don't need to evaluate anything.
    if len(tiers) == 0:
        return
    elif len(tiers) == 1:
        # If there is one tier, it will be the memory tier.
        mem_tier, ebs_tier = tiers[0], None
    else:
        # If there are two tiers, we need to make sure that we assign the
        # correct tiers as the memory and EBS tiers, respectively.
        if tiers[0].tier_id == MEMORY:
            mem_tier = tiers[0]
            ebs_tier = tiers[1]
        else:
            mem_tier = tiers[1]
            ebs_tier = tiers[0]

    # Queries the Kubernetes master for the list of memory nodes its aware of
    # -- if any of the nodes in the hash ring aren't currently running, we add
    # those the departed list.
    mem_ips = util.get_pod_ips(client, 'role=memory')
    departed = []
    for node in mem_tier.servers:
        if node.private_ip not in mem_ips:
            departed.append(('MEMORY', node))

    # Performs the same process for the EBS tier if it exists.
    ebs_ips = []
    if ebs_tier:
        ebs_ips = util.get_pod_ips(client, 'role=ebs')
        for node in ebs_tier.servers:
            if node.private_ip not in ebs_ips:
                departed.append(('STORAGE', node))

    logging.info('Found %d departed nodes.' % (len(departed)))
    mon_ips = util.get_pod_ips(client, 'role=monitoring')
    storage_ips = mem_ips + ebs_ips

    failover_start = time.time()

    # Before broadcasting failure notifications, make sure the log segments
    # associated with the failed nodes are merged to the main DPM indexes
    for pair in departed:
        hostname = util.get_hostname_from_ip(client, pair[1].private_ip)
        logging.info('Randomly pick a node to delegate merge for the failed node %s %s/%s.' %
                     (hostname, pair[1].public_ip, pair[1].private_ip))
        ip = random.choice(storage_ips)
        socket = context.socket(zmq.REQ)
        socket.connect(get_storage_failover_address(ip, 0))
        socket.send_string(pair[0] + ':' + hostname[6:])
        resp = socket.recv()

    # For each departed node the cluster is unaware of, we inform all storage
    # nodes, all monitoring nodes, and all routing nodes that it has departed.
    for pair in departed:
        logging.info('Informing cluster that node %s/%s has departed.' %
                     (pair[1].public_ip, pair[1].private_ip))

        msg = pair[0] + ':' + pair[1].public_ip + ':' + pair[1].private_ip

        # NOTE: In this code, we are presuming there are 4 threads per
        # storage/routing node. If there are more, this will be buggy; if there
        # are fewer, this is fine as the messages will go into the void.
        for ip in storage_ips:
            for t in range(num_memory_threads):
                send_message(context, msg, get_storage_depart_address(ip, t))

        msg = 'depart:' + msg
        for ip in route_ips:
            for t in range(num_routing_threads):
                send_message(context, msg, get_routing_depart_address(ip, t))

        for ip in mon_ips:
            send_message(context, msg, get_monitoring_depart_address(ip))

    failover_end = time.time()
    logging.info('Elapsed time for failover in microseconds = %d ' % ((failover_end - failover_start) * 1000000))


if __name__ == '__main__':
    # We wait for this file to appear before starting the management server,
    # so we don't make policy decisions before the cluster has finished
    # spinning up.
    while not os.path.isfile(os.path.join(os.environ['DINOMO_HOME'], 'setup_complete')):
        pass

    # Waits until the kubecfg file is copied into the pod because we cannot
    # perform any Kubernetes operations without it
    while not os.path.isfile(os.path.join(os.environ['HOME'], '.kube/config')):
        pass

    run(sys.argv[1])
