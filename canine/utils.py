import typing
import os
import sys
import select
import io
import warnings
from collections import namedtuple
import functools
import shlex
import subprocess
import google.auth
import paramiko
import shutil
import time
import pandas as pd
import requests

def isatty(*streams: typing.IO) -> bool:
    """
    Returns true if all of the provided streams are ttys
    """
    for stream in streams:
        try:
            if not (hasattr(stream, 'fileno') and os.isatty(stream.fileno())):
                return False
        except io.UnsupportedOperation:
            return False
    return True

class ArgumentHelper(dict):
    """
    Helper class for setting arguments to slurm commands
    Used only to handle keyword arguments to console commands
    Should not be responsible for positionals
    """

    def __init__(self, *flags: str, **params: typing.Any):
        """
        Creates a new ArgumentHelper
        Flags can be passed as positional arguments
        Parameters can be passed as keyword arguments
        """
        object.__setattr__(self, 'defaults', {})
        object.__setattr__(self, 'flags', [item for item in flags])
        object.__setattr__(self, 'params', {k:v for k,v in params.items()})
        for key, val in [*self.params.items()]:
            if val is True:
                self.flags.append(key)
                del self.params[key]

    def __repr__(self) -> str:
        return '<ArgumentHelper{}>'.format(
            self.commandline
        )

    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]

    def __getitem__(self, name):
        if name in self.params:
            return self.params[name]
        if name in self.flags:
            return True
        if name in self.defaults:
            return self.defaults[name]

    def __setitem__(self, name, value):
        if value is True:
            self.flags.append(name)
        elif value is False:
            object.__setattr__(self, 'flags', [flag for flag in self.flags if flag != name])
        else:
            self.params[name] = value

    def __delitem__(self, name):
        if name in self.params:
            del self.params[name]
        elif name in self.flags:
            object.__setattr__(self, 'flags', [flag for flag in self.flags if flag != name])
        else:
            raise KeyError("No such argument {}".format(name))

    @staticmethod
    def translate(flag) -> str:
        """Converts acceptable python strings to command line args"""
        return flag.replace('_', '-')

    @property
    def commandline(self) -> str:
        """Expands the arguments to command line form"""
        return '{short_prespace}{short_flags}{short_params}{long_flags}{params}'.format(
            short_prespace=' -' if len([f for f in self.flags if len(f) == 1]) else '',
            short_flags=''.join(flag for flag in self.flags if len(flag)==1),
            short_params=''.join(
                ' -{}={}'.format(self.translate(key), shlex.quote(value))
                for key, value in self.params.items()
                if len(key) == 1
            ),
            long_flags=''.join(' --{}'.format(self.translate(flag)) for flag in self.flags if len(flag) > 1),
            params=''.join(
                ' --{}={}'.format(self.translate(key), shlex.quote(value))
                for key, value in self.params.items()
                if len(key) > 1
            )
        )

    def setdefaults(self, **kwargs: typing.Any):
        self.defaults.update(kwargs)

def make_interactive(channel: paramiko.Channel) -> typing.Tuple[int, typing.BinaryIO, typing.BinaryIO]:
    """
    Manages an interactive command
    Takes in a paramiko.Channel shared by stdin, stdout, stderr of a currently running command
    The current interpreter stdin is duplicated and written to the command's stdin
    The command's stdout and stderr are written to the interpreter's stdout and stderr
    and also buffered in a ByteStream for later reading
    Returns (exit status, Stdout buffer, Stderr bufer)
    """
    infd = sys.stdin.fileno()
    channelfd = channel.fileno()
    poll = select.poll()
    poll.register(infd, select.POLLIN+select.POLLPRI+select.POLLERR+select.POLLHUP)
    poll.register(channelfd, select.POLLIN+select.POLLPRI+select.POLLERR+select.POLLHUP)
    stdout = io.BytesIO()
    stderr = io.BytesIO()
    while not channel.exit_status_ready():
        for fd, event in poll.poll(0.5):
            if fd == infd and event & (select.POLLIN + select.POLLPRI):
                # Text available on python stdin
                channel.send(os.read(infd, 4096))
        if channel.recv_ready():
            content = channel.recv(4096)
            sys.stdout.write(content.decode())
            sys.stdout.flush()
            stdout.write(content)
        if channel.recv_stderr_ready():
            content = channel.recv_stderr(4096)
            sys.stderr.write(content.decode())
            sys.stderr.flush()
            stderr.write(content)
    if channel.recv_ready():
        content = channel.recv(4096)
        sys.stdout.write(content.decode())
        sys.stdout.flush()
        stdout.write(content)
    if channel.recv_stderr_ready():
        content = channel.recv_stderr(4096)
        sys.stderr.write(content.decode())
        sys.stderr.flush()
        stderr.write(content)
    stdout.seek(0,0)
    stderr.seek(0,0)
    return channel.recv_exit_status(), stdout, stderr

def get_default_gcp_zone():
    try:
        response = requests.get(
            'http://metadata.google.internal/computeMetadata/v1/instance/zone',
            headers={
                'Metadata-Flavor': 'Google'
            }
        )
        if response.status_code == 200:
            return os.path.basename(response.text)
    except requests.exceptions.ConnectionError:
        pass
    # not on GCE instance, check env
    try:
        response = subprocess.run('gcloud config get-value compute/zone', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if response.returncode == 0 and b'(unset)' not in response.stdout.strip():
            return response.stdout.strip().decode()
    except subprocess.CalledProcessError:
        pass
    # gcloud config not happy, just return default
    return 'us-central1-a'

__DEFAULT_GCP_PROJECT__ = None

def get_default_gcp_project():
    """
    Returns the currently configured default project
    """
    global __DEFAULT_GCP_PROJECT__
    try:
        if __DEFAULT_GCP_PROJECT__ is None:
            __DEFAULT_GCP_PROJECT__ = google.auth.default()[1]
    except google.auth.exceptions.GoogleAuthError:
        warnings.warn(
            "Unable to load gcloud credentials. Some features may not function properly",
            stacklevel=1
        )
    return __DEFAULT_GCP_PROJECT__

def check_call(cmd:str, rc: int, stdout: typing.Optional[typing.BinaryIO] = None, stderr: typing.Optional[typing.BinaryIO] = None):
    """
    Checks that the rc is 0
    If not, flush stdout and stderr streams and raise a CalledProcessError
    """
    if rc != 0:
        if stdout is not None:
            sys.stdout.write(stdout.read().decode())
            sys.stdout.flush()
        if stderr is not None:
            sys.stderr.write(stderr.read().decode())
            sys.stderr.flush()
        raise subprocess.CalledProcessError(rc, cmd)

predefined_mtypes = {
    # cost / CPU in each of the predefined tracks
    'n1-standard': (0.0475, 0.01),
    'n1-highmem': (0.0592, 0.0125),
    'n1-highcpu': (0.03545, 0.0075),
    'n2-standard': (0.4855, 0.01175),
    'n2-highmem': (0.0655, 0.01585),
    'n2-highcpu': (0.03585, 0.00865),
    'm1-ultramem': (0.1575975, 0.0332775),
    'm2-ultramem': (0.2028173077, 0), # no preemptible version
    'c2-standard': (0.0522, 0.012625)
}

CustomPricing = namedtuple('CustomPricing', [
    'cpu_cost',
    'mem_cost',
    'ext_cost',
    'preempt_cpu_cost',
    'preempt_mem_cost',
    'preempt_ext_cost'
])

custom_mtypes = {
    # cost / CPU, extended memory cost, preemptible cpu, preemptible extended memory
    'n1-custom': CustomPricing(
        0.033174, 0.004446, 0.00955,
        0.00698, 0.00094, 0.002014 #extends > 6.5gb/core
    ),
    'n2-custom': CustomPricing(
        0.033174, 0.004446, 0.00955,
        0.00802, 0.00108, 0.002310 # extends > 7gb/core
    )
}

fixed_cost = {
    # For fixed machine types, direct mapping of cost
    'm1-megamem-96': (10.6740, 2.26),
    'f1-micro': (0.0076, 0.0035),
    'g1-small': (0.0257, 0.007)
}

gpu_pricing = {
    'nvidia-tesla-t4': (0.95, 0.29),
    'nvidia-tesla-p4': (0.60, 0.216),
    'nvidia-tesla-v100': (2.48, 0.74),
    'nvidia-tesla-p100': (1.46, 0.43),
    'nvidia-tesla-k80': (0.45, 0.135)
}

@functools.lru_cache()
def _get_mtype_cost(mtype: str) -> typing.Tuple[float, float]:
    """
    Returns the hourly cost of a google VM based on machine type.
    Returns a tuple of (non-preemptible cost, preemptible cost)
    """
    if mtype in fixed_cost:
        return fixed_cost[mtype]
    components = mtype.split('-')
    if len(components) < 3:
        raise ValueError("mtype {} not in expected format".format(mtype))
    track = '{}-{}'.format(components[0], components[1])
    if 'custom' in mtype:
        # (n1-|n2-)?custom-(\d+)-(\d+)(-ext)?
        if components[0] == 'custom':
            components = ['n1'] + components
        if len(components) not in {4, 5}:
            raise ValueError("Custom mtype {} not in expected format".format(mtype))
        cores = int(components[2])
        mem = int(components[3]) / 1024
        if track == 'n1-custom':
            reg_mem = min(mem, cores * 6.5)
        else:
            reg_mem = min(mem, cores * 8)
        ext_mem = mem - reg_mem
        price_model = custom_mtypes[track]
        return (
            (price_model.cpu_cost * cores) + (price_model.mem_cost * reg_mem) + (price_model.ext_cost * ext_mem),
            (price_model.preempt_cpu_cost * cores) + (price_model.preempt_mem_cost * reg_mem) + (price_model.preempt_ext_cost * ext_mem)
        )
    if track not in predefined_mtypes:
        raise ValueError("mtype family {} not defined".format(track))
    cores = int(components[2])
    return (
        predefined_mtypes[track][0] * cores,
        predefined_mtypes[track][1] * cores
    )

def gcp_hourly_cost(mtype: str, preemptible: bool = False, ssd_size: int = 0, hdd_size: int = 0, gpu_type: typing.Optional[str] = None, gpu_count: int = 0) -> float:
    """
    Gets the hourly cost of a GCP VM based on its machine type and disk size.
    Does not include any sustained usage discounts. Actual pricing may vary based
    on compute region
    """
    mtype_cost, preemptible_cost = _get_mtype_cost(mtype)
    return (
        (preemptible_cost if preemptible else mtype_cost) +
        (0.00023287671232876715 * ssd_size) +
        (0.00005479452055 * hdd_size) +
        (
            0 if gpu_type is None or gpu_count < 1
            else (gpu_pricing[gpu_type][1 if preemptible else 0] * gpu_count)
        )
    )

# rmtree_retry removed in favor of AbstractTransport.rmtree

from threading import Lock
write_lock = Lock()
read_lock = Lock()

def pandas_write_hdf5_buffered(df: pd.DataFrame, key: str, buf: io.BufferedWriter):
    """
	Write a Pandas dataframe in HDF5 format to a buffer.
    """

    ## I am getting
    ##   HDF5ExtError("Unable to open/create file '/dev/null'")
    ##   unable to truncate a file which is already open
    with write_lock:
        with pd.HDFStore(
          "/dev/null",
          mode = "w",
          driver = "H5FD_CORE",
          driver_core_backing_store = 0
        ) as store:
            store["results"] = df
            buf.write(store._handle.get_file_image())

def pandas_read_hdf5_buffered(key: str, buf: io.BufferedReader) -> pd.DataFrame:
    """
	Read a Pandas dataframe in HDF5 format from a buffer.
    """
    ## Without this lock, job avoidance breaks when starting two jobs simultaneously!!
    with read_lock:
        with pd.HDFStore(
          "dummy_hdf5",
          mode = "r",
          driver = "H5FD_CORE",
          driver_core_backing_store = 0,
          driver_core_image = buf.read()
        ) as store:
            return store[key]
