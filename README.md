# Canine

A Dalmatian-based job manager to schedule tasks using SLURM

---

## Usage

Canine operates by running jobs on a SLURM cluster. It is designed to take a bash
or WDL script and schedule jobs using data from a Firecloud workspace or with manually
provided inputs. API usage documented at the bottom of this section.

Canine may be used in any of the following ways:
* Running a pipeline yaml file (ie: `$ canine examples/example_pipeline.yaml`)
* Running a pipeline defined on the commandline (ie: `$ canine --backend type:TransientGCP --backend name:my-cluster (etc...)`)
* Building and running a pipeline in python (ie: `>>> canine.Orchestrator(pipeline_dict).run_pipeline()`)
* Using the [Canine API](https://broadinstitute.github.io/canine/) to execute custom
workflows in Slurm, which could not be configured as a pipeline object

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
  common: (bool, default True) Files (gs:// or otherwise) which are shared by multiple tasks will be downloaded only once {--localization common:value}
  staging_dir: (str, default tempdir) Directory in which files for this job should be staged. For Remote backends, this should be set within the NFS share. If no NFS share exists, set this to "SBCAST" {--localization staging-dir:path}
  mount_path: (str, default null) Path within compute nodes where the staging dir can be found {--localization mount-path:path}
  strategy: [One of: Batched, Local, Remote] Strategy for staging inputs {--localization strategy:mode}
  transfer_bucket: (str, default null) Transfer directories via the given bucket instead of directly over SFTP. Bucket transfer generally faster
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

Canine is designed to support a large variety of Slurm setups, including creating
a temporary Slurm cluster exclusively for itself. The options supported in pipelines
should allow users to run Slurm jobs in most common setups. For more complicated
workflows, you may need to use Canine's [Python API](https://broadinstitute.github.io/canine/)
to gain more granular control.

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
