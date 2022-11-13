import abc
import typing
from itertools import product, repeat
from functools import reduce

class AbstractAdapter(abc.ABC):
    """
    Base class for pipeline input adapters
    """

    def __init__(self, alias: typing.Union[None, str, typing.List[str]] = None):
        """
        Initializes the adapter.
        If alias is provided, it is used to specify custom job aliases.
        alias may be a list of strings (an alias for each job) or a single string
        (the input variable to use as the alias)
        """
        self.alias = alias


    @abc.abstractmethod
    def parse_inputs(self, inputs: typing.Dict[str, typing.Union[typing.Any, typing.List[typing.Any]]]) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Takes raw user inputs and parses out actual inputs for each job
        Returns a job input specification useable for Localization
        Also sets self.spec to the same dictionary
        """
        pass

    @abc.abstractmethod
    def parse_outputs(self, outputs: typing.Dict[str, typing.Dict[str, typing.List[str]]]):
        """
        Takes a dictionary of job outputs
        {jobId: {outputName: [output paths]}}
        And handles the post-processing
        """
        pass

    @property
    @abc.abstractmethod
    def spec(self) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        The most recent job specification
        """
        pass

def maxdepth(li):
    """
    Gets maximum nested depth of an array.
    """
    if isinstance(li, list):
        return max([maxdepth(j) for j in li]) + 1
    else:
        return 0

class ManualAdapter(AbstractAdapter):
    """
    Handles manual argument formatting
    Does pretty much nothing, except maybe combining arguments
    """

    def __init__(self, alias: typing.Union[None, str, typing.List[str]] = None, product: bool = False):
        """
        Initializes the adapter
        If product is True, array arguments will be combined, instead of co-iterated.
        If alias is provided, it is used to specify custom job aliases.
        alias may be a list of strings (an alias for each job) or a single string
        (the input variable to use as the alias)
        """
        super().__init__(alias=alias)
        self.product = product
        self.__spec = None
        self._job_length = 0

    def parse_inputs(self, inputs: typing.Dict[str, typing.Union[typing.Any, typing.List[typing.Any]]]) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Takes raw user inputs and parses out actual inputs for each job
        Returns a job input specification useable for Localization
        Also sets self.spec to the same dictionary
        """

        from ..orchestrator import stringify

        # sort inputs by job variable
        inputs = dict(sorted(inputs.items()))

        input_lengths = {
            key: len(val) if isinstance(val, list) else 1
            for key, val in inputs.items()
        }

        # make sure input arrays are not >2D
        for key, val in inputs.items():
            if maxdepth(val) > 2:
                raise ValueError("Input {} is an array with nesting >2".format(key))

        #
        # HACK: deal with lists of length 1
        for key, val in inputs.items():
            if isinstance(val, list) and len(val) == 1 and not isinstance(val[0], list):
                inputs[key] = val[0]

        #
        # convert input dict to job shards, in two possible ways:

        # 1. define jobs shards by Cartesian product of all input arrays
        if self.product:
            self._job_length = reduce(lambda x,y: x*y, input_lengths.values(), 1)
            self.__spec = { str(i) : dict() for i in range(self._job_length) }

            prod = product(
                *[input if isinstance(input, list) else (input,)
                for job_var, input in inputs.items()]
            )

            for shard_idx, input in enumerate(prod):
                self.__spec[str(shard_idx)] = { k : stringify(v) for k, v in zip(inputs.keys(), input) }

        # 2. define job shards normally, i.e. one shard per array element
        else:
            # make sure all input arrays have the same length. job length
            # will be length of each array input
            for key, l in input_lengths.items():
                if l > self._job_length:
                    if self._job_length <= 1:
                        self._job_length = l
                    else:
                        raise ValueError("Array inputs to scatter jobs must all have equal length (input \"{}\" is discrepant)".format(key))
                elif 1 != l != self._job_length:
                    raise ValueError("Array inputs to scatter jobs must all have equal length (input \"{}\" is discrepant)".format(key))

            # tranpose input dict, repeating 1D inputs as necessary
            # ex:
            # a = [1, 2, 3], b = 1 ->
            #     { 0 : { a : 1, b : 1 },
            #       1 : { a : 2, b : 1 },
            #       2 : { a : 3, b : 1 } }
            # a = [1, 2, 3], b = [4, 5, 6] ->
            #     { 0 : { a : 1, b : 4 },
            #       1 : { a : 2, b : 5 },
            #       2 : { a : 3, b : 6 } }
            # a = [1, 2, 3], b = [[4, 5, 6]] ->
            #     { 0 : { a : 1, b : [4, 5, 6] },
            #       1 : { a : 2, b : [4, 5, 6] },
            #       2 : { a : 3, b : [4, 5, 6] } }
            # a = [1, 2], b = [[4, 5, 6], [7, 8, 9]] ->
            #     { 0 : { a : 1, b : [4, 5, 6] },
            #       1 : { a : 2, b : [7, 8, 9] } }
            # a = [[1, 2, 3], [4, 5, 6]], b = [[7, 8, 9], [10, 11, 12]] ->
            #     { 0 : { a : [1, 2, 3], b : [7, 8, 9] },
            #       1 : { a : [4, 5, 6], b : [10, 11, 12] } }
            self.__spec = { str(i) : dict() for i in range(self._job_length) }
            for job_var, input in inputs.items():
                for shard_idx in range(self._job_length):
                    if isinstance(input, list):
                        # scatter input, e.g. for job_length = 2,
                        # * [1, 2] (regular scatter)
                        # * [[1, 2, 3], [4, 5, 6]] (scatter of gathers)
                        if input_lengths[job_var] == self._job_length:
                            self.__spec[str(shard_idx)][job_var] = stringify(input[shard_idx])

                        # gather input (e.g. [[1, 2, 3]])
                        elif input_lengths[job_var] == 1:
                            self.__spec[str(shard_idx)][job_var] = stringify(input[0])

                        # should not happen!
                        else:
                            raise ValueError(f"Malformed job input \"{job_var}\": {input}")
                    else:
                        self.__spec[str(shard_idx)][job_var] = stringify(input)

        assert len(self.__spec) == self._job_length, "Failed to predict input length"
        if self.alias is not None:
            if isinstance(self.alias, list):
                assert len(self.alias) == self._job_length, "Number of job aliases does not match number of jobs"
                for i, alias in enumerate(self.alias):
                    self.__spec[str(i)]['CANINE_JOB_ALIAS'] = alias
            elif isinstance(self.alias, str):
                assert self.alias in inputs, "User provided alias variable not provided in inputs"
                self.__spec[str(i)]['CANINE_JOB_ALIAS'] = self.__spec[str(i)][self.alias]
            else:
                raise TypeError("alias must be a string of list of strings")
            if len({job['CANINE_JOB_ALIAS'] for job in self.__spec.values()}) != len(self.__spec):
                raise ValueError("Job aliases are not unique")
        return self.spec

    @property
    def spec(self) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        The most recent job specification
        """
        return {
            jobId: {**spec}
            for jobId, spec in self.__spec.items()
        }

    def parse_outputs(self, outputs: typing.Dict[str, typing.Dict[str, typing.List[str]]]):
        """
        Takes a dictionary of job outputs
        {jobId: {outputName: [output paths]}}
        Does nothing. Manual Adapter has no output handling
        """
        pass
