"""
For argument parsing and CLI interface
"""
import argparse
import sys
from . import Orchestrator
from .backends import TransientGCPSlurmBackend
import yaml

def ConfType(nfields, nMax=None):
    """
    Returns an argument handler which expects the given number of fields
    Less than the specified number results in an error
    More than the specified number results in the extra fields being combined together
    """
    if nMax is None:
        nMax=nfields

    def parse_arg(argument):
        args = argument.split(':')
        if len(args) < nfields:
            raise argparse.ArgumentError(argument, "Not enough fields in argument (expected {}, got {})".format(nfields, len(args)))
        elif len(args) > nMax:
            args = args[:nMax-1] + ':'.join(args[nMax-1:])
        return args

    return parse_arg

def boot_transient():
    parser = argparse.ArgumentParser(
        'canine-transient',
        description="Boots a transient slurm cluster"
    )
    parser.add_argument(
        'name',
        help="Name of the cluster"
    )
    parser.add_argument(
        '-n', '--node-count',
        type=int,
        help="Max number of compute nodes in the cluster",
        default=10
    )
    parser.add_argument(
        '-z', '--zone',
        help="GCP Compute zone in which to create the cluster",
        default='us-central1-a'
    )
    parser.add_argument(
        '-c', '--controller-type',
        help="Instance type for controller node",
        default="n1-standard-16"
    )
    parser.add_argument(
        '-w', '--worker-type',
        help="Instance type for compute node",
        default="n1-highcpu-2"
    )
    parser.add_argument(
        '-s', '--controller-disk-size',
        type=int,
        help="Size of the controller's disk in GB",
        default=200
    )
    parser.add_argument(
        '-k', '--secondary-disk-size',
        type=int,
        help="Size of the secondary disk",
        default=0
    )
    parser.add_argument(
        '-d', '--worker-disk-size',
        type=int,
        help="Size of the compute node disk in GB",
        default=20
    )
    parser.add_argument(
        '-t', '--gpu-type',
        help="Type of GPU to attach to compute nodes",
        default=None
    )
    parser.add_argument(
        '-g', '--gpu-count',
        type=int,
        help="Number of GPUs to attach to compute nodes",
        default=0
    )
    args = parser.parse_args()
    with TransientGCPSlurmBackend(
        name=args.name,
        max_node_count=args.node_count,
        compute_zone=args.zone,
        controller_type=args.controller_type,
        worker_type=args.worker_type,
        compute_disk_size=args.worker_disk_size,
        controller_disk_size=args.controller_disk_size,
        secondary_disk_size=args.secondary_disk_size,
        gpu_type=args.gpu_type,
        gpu_count=args.gpu_count
    ) as slurm:
        print("=====================")
        print("Slurm cluster started")
        print("SSH using", slurm._RemoteSlurmBackend__hostname)
        print("=====================")
        try:
            while True:
                input("Press Ctrl+C to kill the slurm cluster")
        except KeyboardInterrupt:
            pass # silence keyboard interrupt

def main():
    parser = argparse.ArgumentParser(
        'canine',
        description="A dalmatian-based job manager to schedule tasks using SLURM"
    )
    parser.add_argument(
        'pipeline',
        nargs='?',
        type=argparse.FileType('r'),
        help="Path to a pipeline file. Command line arguments will merge with,"
        "and override options in the file",
        default=None
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="If provided, the job will not actually run. Canine will parse job"
        " inputs and walk through localization, but will not ever schedule the job."
        " All inputs and job scripts will be prepared and localized in the staging"
        " directory"
    )
    parser.add_argument(
        '--export',
        type=argparse.FileType('w'),
        help="If provided, Canine will write the final merged pipeline object to"
        " the provided filepath",
        default=None
    )
    parser.add_argument(
        '--output-dir',
        help="Output directory for canine pipeline. Defaults to 'canine_output'",
        default=None
    )
    parser.add_argument(
        '-n', '--name',
        help="Name of the job",
        default=None
    )
    parser.add_argument(
        '-s', '--script',
        help="Path to the script to run",
        default=None,
        type=argparse.FileType('r')
    ),
    parser.add_argument(
        '-i', '--input',
        help="Script inputs. Must specify in the form inputName:inputValue. --input"
        " may be specified as many times as necessary, and inputNames may also be repeated",
        type=ConfType(2),
        action='append',
        default=[]
    )
    parser.add_argument(
        '-r', '--resources',
        help="SLURM arguments. Must specify in the form argName:argValue. --resources"
        " may be specified as many times as necessary, and a specific argName may also"
        " be repeated. Specify slurm arguments without leading dashes, but otherwise"
        " exactly as they'd appear on the command line. For slurm options"
        " which take no arguments, set --resources argName:true",
        type=ConfType(2),
        action='append',
        default=[]
    )
    parser.add_argument(
        '-a', '--adapter',
        help="Adapter configuration. Must specify in the form optionName:optionValue."
        " --adapter may be specified as many times as necessary",
        type=ConfType(2),
        action='append',
        default=[]
    )
    parser.add_argument(
        '-b', '--backend',
        help="Backend configuration. Must specify in the form optionName:optionValue."
        " --backend may be provided as many times as necessary",
        type=ConfType(2),
        action='append',
        default=[]
    )
    parser.add_argument(
        '-l', '--localization',
        help="Localization configuration. Must specify in the form optionName:optionValue."
        " --localization may be provided as many times as necessary. localization"
        " overrides should be specified using --localization overrides:outputName:overrideValue",
        type=ConfType(2,3),
        action='append',
        default=[]
    )
    parser.add_argument(
        '-o', '--output',
        help="Output patterns. Must specify in the form outputName:globPattern."
        " --output may be provided as many times as necessary.",
        type=ConfType(2),
        action='append',
        default=[]
    )
    args = parser.parse_args()
    conf = {}
    if args.pipeline:
        conf = yaml.load(args.pipeline, Loader=yaml.loader.SafeLoader)
    if args.name is not None:
        conf['name'] = args.name
    if args.script is not None:
        conf['script'] = args.script.name
    if len(args.resources) > 0:
        if 'resources' not in conf:
            conf['resources'] = {}
        conf['resources'] = {
            **conf['resources'],
            **{key: val for key, val in args.resources}
        }
    if len(args.adapter) > 0:
        if 'adapter' not in conf:
            conf['adapter'] = {}
        conf['adapter'] = {
            **conf['adapter'],
            **{key: val for key, val in args.adapter}
        }
    if len(args.backend) > 0:
        if 'backend' not in conf:
            conf['backend'] = {}
        conf['backend'] = {
            **conf['backend'],
            **{key: val for key, val in args.backend}
        }
    if len(args.output) > 0:
        if 'outputs' not in conf:
            conf['outputs'] = {}
            conf['outputs'] = {
                **conf['outputs'],
                **{key: val for key, val in args.output}
            }
    if len(args.input) > 0:
        inputs = {}
        for name, val in args.input:
            if name in inputs:
                if isinstance(inputs[name], list):
                    inputs[name].append(val)
                else:
                    inputs[name] = [inputs[name], val]
            else:
                inputs[name] = val
        if 'inputs' not in conf:
            conf['inputs'] = {}
        conf['inputs'] = {
            **conf['inputs'],
            **inputs
        }
    if len(args.localization) > 0:
        overrides = {}
        localization = {}
        for entry in args.localization:
            if entry[0] == 'overrides':
                overrides[entry[1]] = entry[2]
            else:
                localization[entry[0]] = entry[1]
        if 'localization' not in conf:
            conf['localization'] = {'overrides':{}}
        conf['localization'] = {
            **conf['localization'],
            **localization
        }
        if len(overrides):
            if 'overrides' not in conf['localization']:
                conf['localization']['overrides'] = {}
            conf['localization']['overrides'] = {
                **conf['localization']['overrides'],
                **overrides
            }
    if args.export is not None:
        yaml.dump(conf, args.export)
    if not len(conf):
        sys.exit("Empty pipeline config")
    kwargs = {'dry_run': args.dry_run}
    if args.output_dir is not None:
        kwargs['output_dir'] = args.output_dir
    Orchestrator(conf).run_pipeline(**kwargs)

if __name__ == '__main__':
    main()
