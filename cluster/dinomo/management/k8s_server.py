import logging
import os

import zmq

from dinomo.cluster.add_nodes import add_nodes
from dinomo.cluster.remove_node import remove_node
from dinomo.shared import util

logging.basicConfig(filename='log_k8s.txt', level=logging.INFO)

def run():
    context = zmq.Context(1)
    client, apps_client = util.init_k8s()

    prefix = os.path.join(os.environ['DINOMO_HOME'], 'cluster/dinomo/cluster')

    node_add_socket = context.socket(zmq.PULL)
    node_add_socket.bind('ipc:///tmp/node_add')

    node_remove_socket = context.socket(zmq.PULL)
    node_remove_socket.bind('ipc:///tmp/node_remove')

    poller = zmq.Poller()
    poller.register(node_add_socket, zmq.POLLIN)
    poller.register(node_remove_socket, zmq.POLLIN)

    cfile = os.path.join(os.environ['DINOMO_HOME'], 'conf/dinomo-config.yml')

    while True:
        socks = dict(poller.poll(timeout=1000))

        if node_add_socket in socks and socks[node_add_socket] == zmq.POLLIN:
            msg = node_add_socket.recv_string()
            args = msg.split(':')

            ntype = args[0]
            num = int(args[1])
            logging.info('Adding %d new %s node(s)...' % (num, ntype))

            add_nodes(client, apps_client, cfile, [ntype], [num], prefix=prefix)
            logging.info('Successfully added %d %s node(s).' % (num, ntype))

        if node_remove_socket in socks and socks[node_remove_socket] == zmq.POLLIN:
            msg = node_remove_socket.recv_string()
            args = msg.split(':')

            ntype = args[0]
            ip = args[1]

            remove_node(ip, ntype)
            logging.info('Successfully removed node %s.' % (ip))


if __name__ == '__main__':
    # Wait for this file to be copied into the pod before starting.
    while not os.path.isfile(os.path.join(os.environ['DINOMO_HOME'], 'setup_complete')):
        pass

    run()
