# Canine

A Dalmatian-based job manager to schedule tasks using SLURM

---

## Backends

Canine is designed to work with a variety of SLURM setups. The currently supported
backends are:

* `canine.backends.LocalSlurmBackend`: If Canine is currently running on a SLURM
controller or login node
* `canine.backends.RemoteSlurmBackend`: If Canine needs to SSH into an existing SLURM
controller or login node
* `canine.backends.TransientGCPSlurmBackend`: If Canine needs to create a new SLURM
cluster in Google, then delete it afterwards

The backends provide the lowest level API. All backends implement at least the
interface provided by `canine.backends.AbstractSlurmBackend`. You can use the backends
to interact with a Slurm cluster. Each backend can also produce a Transport object,
which implements `canine.backends.AbstractTransport`. Transports provide access
to the Slurm cluster's filesystem

## Usage

Canine operates by running jobs on a SLURM cluster. It is designed to take a bash
or WDL script and schedule jobs using data from a Firecloud workspace or with manually
provided inputs. API usage documented at the bottom of this section

### Configuration

Canine uses a YAML file to specify the job configuration.
See `pipeline_options.md` for a detailed description of pipeline configuration

```yaml
name: (str, optional) The name for this job {--name}
script: (str) The script to run {--script}
inputs: # Inputs to the script
  varname: value {--input varname:value}
  varname: # {--input varname:value1 --input varname:value2}
    - value1
    - value2
resources: # slurm resources
  varname: value {--resources varname:value}
adapter: # Job input adapter configuration
  type: [One of: Manual (default), Firecloud] The adapter to map inputs into actual job inputs {--adapter type:value}
  # Other Keyword arguments to provide to adapter
  # Manual Args:
  product: (bool, default false) Whether adapter should take the product of all inputs rather than iterating {--adapter product:value}
  # FireCloud Args:
  workspace: (namespace)/(workspace) {--adapter workspace:ws/ns}
  entityType: [One of: sample, pair, participant, *_set] The entity type to use {--adapter entityType:type}
  entityName: (str) The entity to use {--adapter entityName:name}
  entityExpression: (str, optional) The expression to map the input entity to multiple sub-entities {--adapter entityExpression:expr}
  write_to_workspace: (bool, default True) If outputs should be written back to the workspace {--adapter write-to-workspace:value}
backend: # SLURM backend configuration
  type: [One of: Local (default), Remote, TransientGCP] The backend to use when interfacing with SLURM {--backend type:value}
  # Other keyword arguments to provide to the backend for initialization
  argname: argvalue {--backend argname:argvalue}
localization: # Localization options
  localizeGS: (bool, default True) Enables automatic localization of string inputs which start with "gs://" {--localization localizeGS:value}
  common: (bool, default True) Files (gs:// or otherwise) which are shared by multiple tasks will be downloaded only once {--localization common:value}
  staging_dir: (str, default tempdir) Directory in which files for this job should be staged. For Remote backends, this should be set within the NFS share. If no NFS share exists, set this to "SBCAST" {--localization staging-dir:path}
  mount_path: (str, default null) Path within compute nodes where the staging dir can be found {--localization mount-path:path}
  overrides: # Override localization handling for specific inputs
    varname: [One of: Stream, Localize, Common, Delayed, null] Localization handling {--localization overrides:varname:mode}
    # Stream: The input variable will be streamed to a FIFO pipe which is passed to the job
    # Localize: The input variable will be downloaded for the job. If it's a local path, it will be copied to the job's staging directory
    # Common: The input variable will be downloaded to the common directory. If it's a local path, it will be copied to the common directory
    #   If the input variable resolves to multiple values during the setup phase, each unique value will be copied to the common directory
    # Delayed: Same as Localize, except the file is downloaded during job setup, on the compute node. Delayed handling is ignored for local filepaths
    # null: The input variable will be treated as a regular string and no localization will take place
outputs: # Required output files
  outputName: pattern {--output outputName:pattern}
```

#### Manual Jobs

In manual mode, the provided inputs are taken at face value. If any input is an
array, the job will become a SLURM array job. In default iterating mode, all array
inputs must be of equal length, and each task in the array will pop one set of arguments
from the lists (the nth task runs using the nth value for each array input). In
product mode, each task will run using a unique combination of arguments. In either
mode, single-valued inputs are held constant. The SLURM batch job will run the script and all input "varnames"
will be exported as environment variables to the job. All environment variables
will have a single value, reflecting the input for that task in the array

##### Delocalization

In manual mode, after each job completes, any files which match any of the provided
output patterns will be moved to an output directory on your local system

#### Firecloud Jobs

In Firecloud mode, the provided inputs are taken as expressions which need to be
evaluated as inputs to the bash script. In this mode, the adapter arguments for
entities are used to select one or mode entities to use for the job. Each entity
will correspond to one task in the SLURM batch job array. For each entity, all
of the inputs will be evaluated as firecloud expressions in the context of the entity

##### Delocalization

In Firecloud mode, after each job completes, any files which match any of the provided
output patterns will be copied into firecloud, if `write-to-workspace` is enabled

---

# Overview

* The user's chosen adapter maps script and raw inputs to actual task inputs
* The Localizer uses the chosen transport backend to stage inputs on the SLURM controller
* The adapter then uses the chosen SLURM backend to dispatch jobs
* After jobs complete, Localizer identifies outputs and moves them out of the staging directory into the output directory
* If using the Firecloud adapter, outputs are then written back to Firecloud

## Job Environment Variables

In addition to all variables present during [SLURM batch jobs](https://slurm.schedmd.com/sbatch.html#lbAI)
and all variables provided based on config inputs, Canine also exports the following
variables to all jobs:
* `CANINE`: The current canine version
* `CANINE_BACKEND`: The name of the current backend type
* `CANINE_ADAPTER`: The name of the current adapter type
* `CANINE_ROOT`: The path to the staging directory
* `CANINE_COMMON`: The path to the directory where common files are localized
* `CANINE_OUTPUT`: The path to the directory where job outputs will be staged during delocalization
* `CANINE_JOBS`: The path to the directory which contains subdirectories for each job's inputs and workspace
* `CANINE_JOB_VARS`: A colon separated list of the names of all variables generated by job inputs
* `CANINE_JOB_INPUTS`: The path to the directory where job inputs are localized
* `CANINE_JOB_ROOT`: The path to the working directory for the job. Equal to CWD at the start of the job. Output files should be written here
* `CANINE_JOB_SETUP`: The path to the setup script which ran during job start
* `CANINE_JOB_TEARDOWN`: The path the the teardown script which will run after the job
