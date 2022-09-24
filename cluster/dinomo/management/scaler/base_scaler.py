class BaseScaler():
    def __init__(self):
        raise NotImplementedError

    def add_vms(self, kind, count):
        '''
        Add a number (count) of VMs of a certain kind (currently support: memory)
        '''
        raise NotImplementedError

    def remove_vms(self, kind, ip):
        '''
        Remove a particular node (denoted by the IP address ip) from the system;
        the support kinds are memory
        '''
        raise NotImplementedError
