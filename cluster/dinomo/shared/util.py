import os
import subprocess
import sys
import tarfile
from tempfile import TemporaryFile
import yaml

import kubernetes as k8s
from kubernetes.stream import stream

NAMESPACE = 'default'

def replace_yaml_val(yaml_dict, name, val):
    for pair in yaml_dict:
        if pair['name'] == name:
            pair['value'] = val
            return

def init_k8s():
    cfg = k8s.config
    cfg.load_kube_config()
    client = k8s.client.CoreV1Api()
    apps_client = k8s.client.AppsV1Api()

    return client, apps_client

def load_yaml(filename, prefix=None):
    if prefix:
        filename = os.path.join(prefix, filename)

    try:
        with open(filename, 'r') as f:
            return yaml.safe_load(f.read())
    except yaml.YAMLError as e:
        print(f'''Unexpected error while loading YAML file:')
        {e.stderr}

        Make sure to clean up the cluster object and state store before
        recreating the cluster.
        ''')
        sys.exit(1)

def run_process(command, current_workdir):
    try:
        subprocess.run(command, cwd=current_workdir, check=True)
    except subprocess.CalledProcessError as e:
        print(f'''Unexpected error while running command {e.cmd}
        {e.stderr}

        Make sure to clean up the cluster object and state store before
        recreating the cluster.''')
        sys.exit(1)

def run_background_process(command, current_workdir):
    subprocess.Popen(command, cwd=current_workdir)

def check_or_get_env_arg(arg_name):
    if arg_name not in os.environ:
        raise ValueError(f'''Required argument {arg_name} not found as an
        environment variable. Please specify before re-running.''')

    return os.environ[arg_name]

def get_pod_ips(client, selector, is_running=False):
    pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
                                          label_selector=selector).items

    pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))
    running = False
    while None in pod_ips or not running:
        pod_list = client.list_namespaced_pod(namespace=NAMESPACE,
                                              label_selector=selector).items

        pod_ips = list(map(lambda pod: pod.status.pod_ip, pod_list))

        if is_running:
            pod_statuses = list(filter(lambda pod: pod.status.phase != 'Running', pod_list))
            running = len(pod_statuses) == 0
        else:
            running = True

    return pod_ips

def get_previous_count(client, kind):
    selector = 'role=%s' % (kind)
    items = client.list_namespaced_pod(namespace=NAMESPACE,
                                       label_selector=selector).items
    return len(items)

def get_hostname_from_ip(client, ip):
    nodes = client.list_node()
    for i in range(len(nodes.items)):
        if (nodes.items[i].status.addresses[0].address == ip):
            return nodes.items[i].status.addresses[1].address

def get_pod_from_ip(client, ip):
    pods = client.list_namespaced_pod(namespace=NAMESPACE).items
    pod = list(filter(lambda pod: pod.status.pod_ip == ip, pods))[0]
    return pod

def copy_file_to_pod(client, file_path, pod_name, pod_path, container):
    exec_command = ['tar', 'xmvf', '-', '-C', pod_path]
    resp = stream(client.connect_get_namespaced_pod_exec, pod_name, NAMESPACE,
                  command=exec_command, stderr=True, stdin=True,
                  stdout=True, tty=False, _preload_content=False,
                  container=container)

    filename = file_path.split('/')[-1]
    with TemporaryFile() as tar_buffer:
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            tar.add(file_path, arcname=filename)

        tar_buffer.seek(0)
        commands = [str(tar_buffer.read(), 'utf-8')]

        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                pass
            if resp.peek_stderr():
                print("Unexpected error while copying files: %s" %
                      (resp.read_stderr()))
                sys.exit(1)
            if commands:
                c = commands.pop(0)
                resp.write_stdin(c)
            else:
                break
        resp.close()
