import typing
from .base import AbstractAdapter
import dalmatian
#
# adapter: # Job input adapter configuration
#   type: [One of: Manual (default), Firecloud] The adapter to map inputs into actual job inputs {--adapter type:value}
#   # Other Keyword arguments to provide to adapter
#   # Manual Args:
#   product: (bool, default false) Whether adapter should take the product of all inputs rather than iterating {--adapter product:value}
#   # FireCloud Args:
#   workspace: (namespace)/(workspace) {--adapter workspace:ws/ns}
#   entityType: [One of: sample, pair, participant, *_set] The entity type to use {--adapter entityType:type}
#   entityName: (str) The entity to use {--adapter entityName:name}
#   entityExpression: (str, optional) The expression to map the input entity to multiple sub-entities {--adapter entityExpression:expr}
#   write-to-workspace: (bool, default True) If outputs should be written back to the workspace {--adapter write-to-workspace:value}

class FirecloudAdapter(AbstractAdapter):
    """
    Job input adapter
    Parses inputs as firecloud expression bois
    if enabled, job outputs will be written back to workspace
    """
    def __init__(self, workspace: str, entityType: str, entityName: str, entityExpression: typing.Optional[str] = None):
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
        self.evaluator = dalmatian.Evaluator(ws.entity_types)
        for etype in ws.entity_types:
            self.evaluator.add_entities(etype, self.workspace._get_entities_internal(etype))
        self.evaluator.add_attributes(self.workspace.attributes)
        if entityExpression is not None:
            self.entities = self.evaluator(entityType, entityName, entityExpression)
            self.etype = self.evaluator.determine_reference_type(entityType, self.entities, '')
        else:
            self.entities = [entityName]
            self.etype = entityType

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

    def parse_outputs(self, outputs: typing.Dict[str, typing.Dict[str, typing.List[str]]]):
        """
        Takes a dictionary of job outputs
        {jobId: {outputName: [output paths]}}
        And handles the post-processing
        """
        pass

    @property
    def spec(self) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        The most recent job specification
        """
        pass
