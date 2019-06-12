import typing
import os
import yaml
version = '0.0.1'

class Orchestrator(object):
    """
    Main class
    Parses a configuration object, initializes, runs, and cleans up a Canine Pipeline
    """

    @staticmethod
    def fill_config(cfg: typing.Union[str, typing.Dict[str, typing.Any]]) -> typing.Dict[str, typing.Any]:
        """
        Loads the given config object (or reads from the given filepath)
        Applies Canine defaults, then returns the final config dictionary
        """
        if isinstance(cfg, str):
            with open(cfg) as r:
                cfg = yaml.load(r)
        DEFAULTS = {
            'name': 'canine',
            'adapter': {
                'type': 'Manual',
            },
            'backend': {
                'type': 'Local'
            },
        }
        for key, value in DEFAULTS.items():
            if key not in cfg:
                cfg[key] = value
            elif isinstance(value, dict):
                cfg[key] = {**value, **cfg[key]}


# Pipeline will look something like this
# config = parse_config()
# with SlurmBackend(**args) as backend:
#     with Localizer(backend, **args) as localizer:
#         localizer.localize(config.inputs(), config.overrides())
#         for job in config.jobs():
#             job.startup_script = CanineCoreStartup(job.id) + localizer.startup_hook(job.id)
#             job.main_script = wrap_script(config.script(), localizer.environment(job.id))
#         backend.sbatch(wrapped_script(), task_array=config.jobs(), **args)
#         for job in config.jobs():
#             wait_for_job_complete(backend, job)
#         outputs = localizer.delocalize()
#     adapter.handle_outputs(outputs)
