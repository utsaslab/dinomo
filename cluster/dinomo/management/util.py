import zmq

NUM_EXEC_THREADS = 3

TCP_BASE = 'tcp://%s:%d'

EXECUTOR_DEPART_PORT = 4050
EXECUTOR_PIN_PORT = 4000
EXECUTOR_UNPIN_PORT = 4010

#KVS_NODE_DEPART_PORT = 6050
KVS_NODE_DEPART_PORT = 6100
#KVS_NODE_FAILOVER_PORT = 6251
KVS_NODE_FAILOVER_PORT = 6600
#ROUTING_SEED_PORT = 6350
ROUTING_SEED_PORT = 7000
#ROUTING_NOTIFY_PORT = 6400
ROUTING_NOTIFY_PORT = 7100
#MONITORING_NOTIFY_PORT = 6600
MONITORING_NOTIFY_PORT = 7400

def send_message(context, message, address):
    socket = context.socket(zmq.PUSH)
    socket.connect(address)

    if type(message) == str:
        socket.send_string(message)
    else:
        socket.send(message)

def get_executor_depart_address(ip, tid):
    return TCP_BASE % (ip, tid + EXECUTOR_DEPART_PORT)

def get_executor_pin_address(ip, tid):
    return TCP_BASE % (ip, tid + EXECUTOR_PIN_PORT)

def get_executor_unpin_address(ip, tid):
    return TCP_BASE % (ip, tid + EXECUTOR_UNPIN_PORT)

def get_routing_seed_address(ip, tid):
    return TCP_BASE % (ip, tid + ROUTING_SEED_PORT)

def get_storage_depart_address(ip, tid):
    return TCP_BASE % (ip, tid + KVS_NODE_DEPART_PORT)

def get_storage_failover_address(ip, tid):
    return TCP_BASE % (ip, tid + KVS_NODE_FAILOVER_PORT)

def get_routing_depart_address(ip, tid):
    return TCP_BASE % (ip, tid + ROUTING_NOTIFY_PORT)

def get_monitoring_depart_address(ip):
    return TCP_BASE % (ip, MONITORING_NOTIFY_PORT)
