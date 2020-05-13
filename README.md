# Canine

A modular, high-performance computing solution to run jobs using SLURM

---

## Usage

Canine operates by running jobs on a SLURM cluster. It is designed to take a bash
or WDL script and schedule jobs using data from a Firecloud workspace or with manually
provided inputs. API usage documented at the bottom of this section.

Canine may be used in any of the following ways:
* Running a pipeline yaml file (ie: `$ canine examples/example_pipeline.yaml`)
* Running a pipeline defined on the commandline (ie: `$ canine --backend type:TransientGCP --backend name:my-cluster (etc...)`)
* Building and running a pipeline in python (ie: `>>> canine.Orchestrator(pipeline_dict).run_pipeline()`)
* Using the [Canine API](https://getzlab.github.io/canine/) to execute custom
workflows in Slurm, which could not be configured as a pipeline object

## Anatomy of a pipeline

Canine can be natively configured to suit a vast range of setups.
Canine is modularized into three main components which can be mixed and matched as needed: Adapters, Backends, and Localizers.
A pipeline specifies which Adapter, Backend, and Localizer to use, along with any configuration options for each.

### Adapters

The pipeline adapter is responsible for converting the provided list of inputs into an input specification for each job.

#### Choosing an Adapter

This is a list of available adapters. For more details, see [pipeline_options.md](https://github.com/getzlab/canine/blob/master/pipeline_options.md)

* `Manual`: (Default) This is the primary input adapter responsible for determining the number of jobs and the inputs for each job, based on the raw inputs provided by the user.
    * Inputs which have a single constant value will have the same value for all jobs
    * Inputs which have a 1D list of values will have one of those values in each job. By Default, all list inputs must have the same length, and there will be one job per element. The nth job will have the nth value of each input
    * Inputs which have a 2D (or deeper) list of values will flatten to 2D, then pass one of the nested lists to each job as if each were a regular value.
    Other than that, the same rules apply, so the list must have the same length as all other lists along the first dimension
    * There are extra configuration options which can change how inputs are combined or how lists are interpreted
* `Firecloud`/`Terra`: Choose this adapter if you are using data hosted in a FireCloud or Terra workspace.
Your inputs will be interpreted as entity expressions, similar to how FireCloud and Terra workflows interpret inputs. This adapter can also be configured to post results back to your workspace, if you choose. **Warning:** Reading from Workspace buckets is convenient, but you may encounter issues if your Slurm cluster is not logged in using your credentials

### Backends

The pipeline backend is responsible for interfacing with the Slurm controller.
There are many different backends available depending on where SLURM is running (or for creating a Slurm cluster for you).

#### Choosing a Backend

This is a list of available backends. For more details, see [pipeline_options.md](https://github.com/getzlab/canine/blob/master/pipeline_options.md)

* `Local`: (Default) Choose this backend if you will be running Canine from the Slurm controller and your cluster is fully configured.
This backend will run Slurm commands through the local shell
* `Remote`: Choose this backend if you have a fully configured SLURM cluster, but you will be running Canine elsewhere.
This backend uses SSH and SFTP to interact with the Slurm controller
* `GCPTransient`: Choose this backend if you do not have a Slurm cluster.
This backend will create a cluster to your specifications in Google Cloud and then use SSH and SFTP to interact with the controller. The cluster will be deleted after Canine has finished
* `ImageTransient`: Choose this backend if you do not have a Slurm cluster, but want more control over its startup than `GCPTransient`.
This backend assumes that the current system has Slurm installed and has an NFS mount set up.
It then creates worker nodes from a Google Compute Image that you have setup and configured.
* `DockerTransient`: Choose this backend if you want the same control as `ImageTransient` but do not want to set up a Google Compute Image.
The Slurm daemons run inside docker containers on the worker nodes.
The Slurm controller daemon runs inside a docker container on the local filesystem
* `Dummy`: Choose this backend for developing or testing pipelines.
This backend simulates a Slurm cluster by running the controller and workers as docker containers on the local system. **This backend does not provision any cloud resources.**
It runs entirely through the local docker daemon.

### Localizers

The pipeline localizer is responsible for staging the pipeline on the SLURM controller and for transferring inputs/outputs as needed.
There are four different localizers to accommodate different needs.

#### Choosing a Localizer

This is a list of available localizers. For more details, see [pipeline_options.md](https://github.com/getzlab/canine/blob/master/pipeline_options.md)

* `Batched`: (Default) This localizer is suitable for most situations.
It stages the canine pipeline workspace locally in a temporary directory, copying or symlinking local files into it before broadcasting the workspace directory structure over to the Slurm controller.
Files stored in Google Cloud Storage are downloaded at the end, directly onto the Slurm Controller (using credentials stored on the controller).
* `Local`: Choose this localizer if you have files in Google Cloud Storage which need to be localized but you are unable to save suitable credentials to the Slurm controller.
This is very similar to the `Batched` localizer, except that Google Cloud Storage files are staged locally and broadcast to the Slurm Controller along with the rest of the pipeline files
* `Remote`: Choose this localizer for small pipelines with few local files.
This localizer stages the pipeline directory directly on the Slurm controller using SFTP. It is often less efficient than the bulk directory copy used by the `Batched` and `Local` localizers (especially if you provide a `transfer_bucket` to them) but can outperform other localizers for small pipelines which consist entirely of files from Google Cloud Storage.
* `NFS`: Choose this backend if the current system has an active NFS mount to the Slurm controller.
The canine pipeline will be staged locally, within the NFS mount point, allowing NFS to take care of transferring the pipeline directory to the controller.


### Examples

There are a few examples in the `examples/` directory which can be run out-of-the box.
To run one of these pipelines, follow any of the following instructions:

##### Command Line

```
$ canine examples/example_pipeline.yaml
```

#### Python (using filepath)
```python
import canine
orchestrator = canine.Orchestrator('examples/example_pipeline.yaml')
results = orchestrator.run_pipeline()
```

#### Python (using dictionary)
```python
import canine
import yaml

with open('examples/example_pipeline.yaml') as r:
  config = yaml.load(r)

orchestrator = canine.Orchestrator(config)
results = orchestrator.run_pipeline()
```

### Other pipeline components

Hopefully you've run an example or two and have a better understanding of what a pipeline looks like.
This section will describe the other parts of a pipeline configuration not covered already

#### inputs

Inputs describe both the number of jobs and the inputs to each job.
The `inputs` section of the pipeline should be a dictionary.
Each key is a string, mapping the name of the input to either a string or list of strings.
As described above, the adapter is responsible for parsing the raw, user-provided inputs into the set of inputs for each job that will be run.

* Raw inputs which were lists of 2 or more dimensions are interpreted by the adapter as if the user wished to provide one of the nested lists to each job. The array is flattened to 2 dimensions, and interpreted as if it were a regular list input (with one element passed to each job). The contents of these arrays are handled using the above localization rules
* Raw inputs which were lists of any dimensions, but marked as `common` in the overrides are flattened to 1 dimension, and the whole list is provided as an input to each job. The contents of the array are handled as `common` files (see below)

#### script

The pipeline script is the heart of the pipeline. This is the actual bash script which will be run. The `script` key can either be a filepath to a bash script to run, or a list of strings, each of which is a command to run.
Either way, the script gets executed by each job of the pipeline.

**NOTE:** During setup, every job will configure a `$CANINE_DOCKER_ARGS` environment variable. We recommend that you expand this variable inside the argument list to `docker run` commands to enable the container to properly interact with the canine environment

### overrides

Localization overrides, defined in `localization.overrides` allow the user to change the localizer's default handling for a specific input.
The overrides section should be a dictionary mapping input names, to a string describing the desired handling, as follows:

* Default rules (no override):
    * Strings which exist as a local filepath are treated as files and will be localized to the Slurm controller
    * Strings which start with `gs://` are interpreted to be files/directories within Google Cloud Storage and will be localized to the Slurm controller
    * Any file or Google Storage object which appears as an input to multiple jobs is considered `common` and will be localized once to a common directory, visible to all jobs
    * If any input to any arbitrary job is a list, the contents of the list are interpreted using the same rules
* `Common`: Inputs marked as common will be considered common to all jobs and localized once, to a directory which is visible to all jobs. Inputs marked as common which cannot be interpreted as a filepath or a Google Cloud Storage object are ignored and treated as strings.
    * Note: You can also use `Common` to force an input list to be handed to all jobs. If an input which was a list is marked as `Common` the list will be flattened to 1D, and all elements of the list will be passed to all jobs, as if the list were a single element
* `Stream`: Inputs marked as `Stream` will be streamed into a FIFO pipe, and the path to the pipe will be exported to the job. The `Stream` override is ignored for inputs which are not Google Cloud Storage objects, causing those inputs to be localized under default rules. Jobs which are requeued due to node failure will always restart the stream. Streams are created in a temporary directory on the local disk of the compute node
* `Delayed`: Inputs marked as `Delayed` will be downloaded by the job once it starts, instead of upfront during localization. The `Delayed` override is ignored for inputs which are not Google Cloud Storage objects, causing those inputs to be localized under default rules. Jobs which are requeued due to node failures will only re-download delayed inputs if the job failed before the download completed
* `Local`: Similar to `Delayed`. Inputs marked as `Local` will be downloaded by the job once it starts. The difference between `Delayed` and `Local` is that for `Local` files, a new disk is provisioned and mounted to the worker node and `Local` downloads are saved there. The disk is automatically sized
to fit all files marked as `Local` plus a small safety margin. **Warning:** Do not create or unzip files in the local download directory. The local download disks are sized automatically to fit the size of the downloaded files and will likely run out of space if additional files are created or unpacked
* `Localize`: Inputs marked as `Localize` will be treated as files and localized to job-specific input directories. This can be used to force files which would be handled as common, to be localized for each job. The `Localize` override is ignored for inputs which are not valid filepaths or Google Cloud Storage objects, causing those inputs to be treated as strings
* `Null` or `None`: Inputs marked this way are treated as strings, and no localization will be applied.

#### outputs

The outputs section defines a mapping of output names to file patterns which should be grabbed for output. File patterns may be raw filenames or globs, and may include shell variables (including job inputs).
These patterns are always relative to each job's initial cwd (`$CANINE_JOB_ROOT`). Patterns _may_ match files above the workspace directory, but this is not recommended.
By default, `stdout` and `stderr` are included in the outputs, which will grab the job's stdout/err streams.
You may override this behavior by providing your own pattern for `stdout` or `stderr`.
**Warning:** the outputs `stdout` and `stderr` have special handling, which expects their patterns to match exactly one file.
If you provide a custom pattern for `stdout` or `stderr` and matches more than one file, the output dataframe will only show the first filename matched

All files which match a provided output pattern will be delocalized from the Slurm controller back to the current system in the following directory structure:
```
output_dir/
  {job id}/
    stdout
    stderr
    {other output names}/
      {matched files/directories}
```

#### resources

The `resources` section allows you to define additional arguments to `sbatch` to control the resource allocation or other scheduling parameters. The `resource` dictionary is converted to commandline arguments as follows:

* Single-letter keys are converted to short (`-x`) options.
* Multi-letter keys are converted to long (`--xx`) options.
* Keys with a value of `True` are converted to flags (no value)
* keys with any other value are converted to paramters (`--key=val`)
* Underscores in keys are converted to hyphens (`foo_bar` becomes `--foo-bar`)
