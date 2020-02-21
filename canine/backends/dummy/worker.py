#!/usr/bin/python3
import time
import os
import socket
import subprocess

if __name__ == '__main__':
    subprocess.check_call('service munge start', shell=True)
    hostname = socket.gethostname()
    while not os.path.exists('/mnt/nfs/controller.ready'):
        print(hostname, "waiting for controller to start")
        time.sleep(10)
    proc = subprocess.Popen('slurmd -D', shell=True)
    try:
        proc.wait(0.1)
        proc.terminate()
    except subprocess.TimeoutExpired:
        subprocess.check_call(
            'scontrol show node={}'.format(hostname),
            shell=True
        )
    proc.wait()
    # subprocess.run('sleep 300', shell=True)
