#!/usr/bin/python3
import argparse
import subprocess
import socket
from multiprocessing import cpu_count
from psutil import virtual_memory
from collections import namedtuple
import re
import os
import time

placeholder = re.compile(r'\<(.+?)\>')

Conf = namedtuple(
    'Conf',
    ['text', 'settings']
)

def get_docker_ip(hostname, network):
    ip = subprocess.check_output(
        'docker inspect --format "{{{{.NetworkSettings.Networks.{network}.IPAddress}}}}" {hostname}'.format(
            hostname=hostname,
            network=network
        ),
        shell=True,
        executable='/bin/bash'
    ).decode().strip()
    if len(ip):
        return ip
    print("No ip for", hostname)

def boot_node(network, cpus=None, mem=None):
    x = subprocess.check_output(
        # Docker wrapper takes care of remapping /mnt/nfs and /root/.config/gcloud to the appropriate host path
        'docker run --rm -it -d '
        '-v /mnt/nfs:/mnt/nfs '
        '-v /var/run/docker.sock:/var/run/docker.sock '
        '-v /root/.config/gcloud:/root/.config/gcloud '
        '{cpus} {mem} '
        '--network {network} {image} /worker.sh'.format(
            network=network,
            image=subprocess.check_output(
                'docker inspect --format "{{{{.Image}}}}" {}'.format(socket.gethostname()),
                shell=True
            ).decode().strip(),
            cpus='' if cpus is None else '--cpus {}'.format(cpus),
            mem='' if mem is None else '--memory {}'.format(mem*(1024**3))
        ),
        shell=True
    ).decode().strip()[:12]
    print("Booted", x)
    time.sleep(1)
    return x

def read_conf(path):
    """
    Returns a 2-tuple: (text, dict)
    Dict contains a mapping of placeholder key names -> None
    Update the dict with desired values.
    call write_conf to rewrite conf
    """
    with open(path) as r:
        text = r.read()
    return Conf(
        text,
        {
            match.group(1): None
            for match in placeholder.finditer(text)
        }
    )

def write_conf(conf, path):
    """
    Dumps a configuration. If any values are not set (ie: settings that map to NONE)
    an exception will be raised
    """
    text = '' + conf.text # Make a copy; Don't edit original text
    for key, val in conf.settings.items():
        if val is None:
            raise ValueError("Setting '{}' left blank".format(key))
        text = text.replace('<{}>'.format(key), val)
    with open(path, 'w') as w:
        w.write(text)

def format_node(name, network, cpus, mem):
    return 'NodeName={} NodeAddr={} CPUs={:d} RealMemory={:d} State=UNKNOWN'.format(
        name,
        get_docker_ip(name, network),
        cpus if cpus is not None else cpu_count(),
        int(mem*1024) if mem is not None else int(virtual_memory().total/1101004) # bytes->mb with small safety margin
    )

def main(network, nodes, cpus, mem):
    if os.path.exists("/mnt/nfs/controller.ready"):
        os.remove("/mnt/nfs/controller.ready")
    subprocess.check_call('service ssh start', shell=True)
    if not os.path.isdir('/mnt/nfs/clust_conf/slurm/'):
        os.makedirs('/mnt/nfs/clust_conf/slurm/')
    slurm_conf = read_conf('/conf_templates/slurm.conf')
    slurm_conf.settings['CONTROLLER HOSTNAME'] = socket.gethostname()
    slurm_conf.settings['CONTROLLER ADDRESS'] = get_docker_ip(
        slurm_conf.settings['CONTROLLER HOSTNAME'],
        network
    )
    nodenames = [
        boot_node(network, cpus, mem)
        for i in range(nodes)
    ]
    # Allow containers to start
    time.sleep(5)
    slurm_conf.settings['NODE DEFS'] = '\n'.join(
        format_node(name, network, cpus, mem)
        for name in nodenames
    )
    slurm_conf.settings['NODE NAMES'] = ','.join(nodenames)
    write_conf(slurm_conf, '/mnt/nfs/clust_conf/slurm/slurm.conf')

    slurmdbd_conf = read_conf('/conf_templates/slurmdbd.conf')
    slurmdbd_conf.settings['CONTROLLER HOSTNAME'] = slurm_conf.settings['CONTROLLER HOSTNAME']
    write_conf(slurmdbd_conf, '/mnt/nfs/clust_conf/slurm/slurmdbd.conf')

    subprocess.check_call('service munge start', shell=True)
    subprocess.check_call('service mysql start', shell=True)
    subprocess.check_call('slurmdbd -vvvvv', shell=True)
    time.sleep(3)
    subprocess.check_call('sacctmgr --immediate add cluster local_slurm', shell=True)
    subprocess.check_call('slurmctld -vvvvv', shell=True)
    subprocess.check_call('touch /mnt/nfs/controller.ready', shell=True)

    # Now become bash
    subprocess.run('/bin/bash', shell=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        'SlurmConfiguration'
    )
    parser.add_argument(
        'network',
        help="Name of docker network"
    )
    parser.add_argument(
        'nodes',
        type=int,
        help="Number of nodes to start"
    )
    parser.add_argument(
        '-c', '--cpus',
        type=int,
        help='Number of CPUS on each worker node. If not provided, CPUS will not be limited',
        default=None
    )
    parser.add_argument(
        '-m', '--memory',
        type=int,
        help='Number of Memory (bytes) on each worker node. If not provided, memory will not be limited',
        default=None
    )

    args = parser.parse_args()
    main(args.network, args.nodes, args.cpus, args.memory)
