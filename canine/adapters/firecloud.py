import typing
import json
import os
import warnings
import pandas as pd
import numpy as np
from .base import AbstractAdapter
import dalmatian

class FirecloudAdapter(AbstractAdapter):
    """
    Job input adapter
    Parses inputs as firecloud expression bois
    if enabled, job outputs will be written back to workspace
    """
    def __init__(self, workspace: str, entityType: str, entityName: str, entityExpression: typing.Optional[str] = None, write_to_workspace: bool = True):
        """
        Initializes the adapter
        Must provide workspace and entity information
        If no expression is provided:
        * Assume a single job, and resolve all input expressions in the context of the one entity
        If an expression is provided:
        * Assume multiple entities (entity type will be auto-detected)
        * Launch one job per entity, resolving input expressions for each one
        """
        self.workspace = dalmatian.WorkspaceManager(workspace)
        self.workspace.populate_cache()
        self.workspace.go_offline()
        if entityName not in self.workspace._get_entities_internal(entityType).index:
            raise NameError('No such {} "{}" in workspace {}'.format(
                entityType,
                entityName,
                workspace
            ))
        self._entityType = entityType
        self._entityName = entityName
        self._entityExpression = entityExpression
        self.evaluator = dalmatian.Evaluator(self.workspace.entity_types)
        for etype in self.workspace.entity_types:
            self.evaluator.add_entities(etype, self.workspace._get_entities_internal(etype))
        self.evaluator.add_attributes(self.workspace.attributes)
        if entityExpression is not None:
            self.entities = self.evaluator(entityType, entityName, entityExpression)
            self.etype = self.evaluator.determine_reference_type(entityType, self.entities, '')
        else:
            self.entities = [entityName]
            self.etype = entityType
        self.write_to_workspace = write_to_workspace
        self.__spec = None
        print("initialized fc adapter in workspace", self.workspace)
        print(len(self.entities), "of type", self.etype)

    def evaluate(self, etype: str, entity: str, expr: str) -> str:
        """
        Evaluates and unpacks the results of a firecloud expression to a single value
        Raises appropriate exceptions otherwise
        """
        results = [
            item for item in self.evaluator(etype, entity, expr)
            if isinstance(item, str) or not np.isnan(item)
        ]

        if len(results) == 1:
            return results[0]
        elif len(results) == 0:
            return None
        else:
            raise ValueError("Expression '{}' on {} {} returned more than one result".format(
                expr,
                etype,
                entity
            ))

    def parse_inputs(self, inputs: typing.Dict[str, typing.Union[typing.Any, typing.List[typing.Any]]]) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Takes raw user inputs and parses out actual inputs for each job
        Returns a job input specification useable for Localization
        Also sets self.spec to the same dictionary
        """
        # If constant input:
        #   this. or workspace. -> evaluate
        #   gs:// -> raw
        #   else: warn, but assume raw
        # if array input: fail (can't map array expr to entity expr)
        self.__spec = {str(i):{} for i in range(len(self.entities))}
        for name, expr in inputs.items():
            if isinstance(expr, list):
                raise TypeError("Firecloud adapter cannot handle array-type inputs")
            elif expr.startswith('this.') or expr.startswith('workspace.'):
                for i, entity in enumerate(self.entities):
                    self.__spec[str(i)][name] = self.evaluate(self.etype, entity, expr)
            else:
                if isinstance(expr, str) and not expr.startswith('gs://'):
                    warnings.warn("Assuming expression is not a Firecloud expression", stacklevel=2)
                for i, entity in enumerate(self.entities):
                    self.__spec[str(i)][name] = expr
        self.workspace.hound.write_log_entry(
            'job',
            'Canine launching new job with input configuration: {}; Results will{} be written back to workspace'.format(
                json.dumps({
                    entity:cfg for entity, (i, cfg) in zip(self.entities, self.__spec.items())
                }),
                '' if self.write_to_workspace else ' not'
            ),
            entities=[
                '{}/{}'.format(self.etype, entity)
                for entity in self.entities
            ]
        )
        return self.spec

    def parse_outputs(self, outputs: typing.Dict[str, typing.Dict[str, typing.List[str]]]):
        """
        Takes a dictionary of job outputs
        {jobId: {outputName: [output paths]}}
        If write_to_workspace is enabled, output files will be written back to firecloud
        """
        if self.write_to_workspace:
            with self.workspace.hound.with_reason('Uploading results from Canine job'):
                outputDf = pd.DataFrame.from_dict(
                    {
                        jobId: {
                            outputName: [outputFile for outputFile in outputFiles if os.path.exists(outputFile)]
                            for outputName, outputFiles in jobOutput.items()
                            if outputName not in {'stdout', 'stderr'}
                        }
                        for jobId, jobOutput in outputs.items()
                    },
                    orient='index'
                ).set_index(
                    pd.Index(
                        [self.entities[int(i)] for i in outputs],
                        name='{}_id'.format(self.etype)
                    )
                ).applymap(
                    lambda cell: cell if not isinstance(cell, list) else (
                        cell[0] if len(cell) > 0 else np.nan
                    )
                )
                self.workspace.sync()
                self.workspace.update_entity_attributes(
                    self.etype,
                    outputDf
                )

    @property
    def spec(self) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        The most recent job specification
        """
        return {
            jobId: {**spec}
            for jobId, spec in self.__spec.items()
        }
