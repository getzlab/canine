
class Orchestrator(object):
    """
    Main class
    Parses a configuration object, initializes, runs, and cleans up a Canine Pipeline
    """
    pass


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
