import logging
import random
import zmq

from dinomo.management.util import (
    send_message
)
from dinomo.management.scaler.base_scaler import BaseScaler


class DefaultScaler(BaseScaler):
    def __init__(self, ip, ctx, add_socket, remove_socket):
        self.ip = ip
        self.context = ctx
        self.add_socket = add_socket
        self.remove_socket = remove_socket

    def add_vms(self, kind, count):
        msg = kind + ':' + str(count)
        self.add_socket.send_string(msg)

    def remove_vms(self, kind, ip):
        msg = kind + ':' + ip
        self.remove_socket.send_string(msg)
