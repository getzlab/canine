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
        keys = sorted(inputs)
        input_lengths = {
            key: len(val) if isinstance(val, list) else 1
            for key, val in inputs.items()
        }

        #
        # HACK: deal with lists of length 1
        for key, val in inputs.items():
            if isinstance(val, list) and len(val) == 1:
                inputs[key] = val[0]

        if self.product:
            self._job_length = reduce(lambda x,y: x*y, input_lengths.values(), 1)
            generator = product(
                *[inputs[key] if isinstance(inputs[key], list) else (inputs[key],)
                for key in keys]
            )
        else:
            for key, l in input_lengths.items():
                if l > self._job_length:
                    if self._job_length <= 1:
                        self._job_length = l
                    else:
                        raise ValueError("Manual Adapter cannot resolve job with uneven input {}".format(key))
                elif 1 != l != self._job_length:
                    raise ValueError("Manual Adapter cannot resolve job with uneven input {}".format(key))
            #
            # XXX: simplify this with itertools.zip_longest() ?
            generator = zip(*[
                inputs[key] if isinstance(inputs[key], list) else repeat(inputs[key], self._job_length)
                for key in keys
            ])
        self.__spec = {
            str(i): {
                key: str(val)
                for key, val in zip(keys, job)
            }
            for i, job in enumerate(generator)
        }
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
