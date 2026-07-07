"""
Pure unit tests for canine.orchestrator — no SLURM cluster, no Docker required.
The legacy test_orchestrator.py covers integration tests; this file covers logic
that can be exercised in a plain Python process.
"""
import os
import pytest
import yaml
import pandas as pd
import numpy as np

from canine.orchestrator import stringify, _job_indices_to_array_str, Orchestrator
from canine.localization.file_handlers import StringLiteral


# ---------------------------------------------------------------------------
# stringify
# ---------------------------------------------------------------------------

class TestStringify:

    def test_plain_string_passthrough(self):
        assert stringify("hello") == "hello"

    def test_int_to_string(self):
        assert stringify(42) == "42"

    def test_float_to_string(self):
        assert stringify(3.14) == "3.14"

    def test_none_to_string(self):
        assert stringify(None) == "None"

    def test_list_of_ints(self):
        assert stringify([1, 2, 3]) == ["1", "2", "3"]

    def test_nested_list(self):
        assert stringify([[1, 2], [3, 4]]) == [["1", "2"], ["3", "4"]]

    def test_dict_values_stringified(self):
        assert stringify({"a": 1, "b": "foo"}) == {"a": "1", "b": "foo"}

    def test_nested_dict(self):
        assert stringify({"outer": {"inner": 99}}) == {"outer": {"inner": "99"}}

    def test_pandas_series(self):
        assert stringify(pd.Series([1, 2, 3])) == ["1", "2", "3"]

    def test_pandas_index(self):
        assert stringify(pd.Index(["a", "b", "c"])) == ["a", "b", "c"]

    def test_numpy_array(self):
        assert stringify(np.array([10, 20, 30])) == ["10", "20", "30"]

    def test_pandas_dataframe(self):
        df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        assert stringify(df) == {"x": ["1", "2"], "y": ["3", "4"]}

    def test_filetype_passthrough(self):
        ft = StringLiteral("gs://bucket/file.txt")
        assert stringify(ft) is ft

    def test_string_with_newline_raises_by_default(self):
        with pytest.raises(TypeError):
            stringify("line1\nline2")

    def test_string_with_newline_allowed_when_safe_false(self):
        assert stringify("line1\nline2", safe=False) == "line1\nline2"

    def test_empty_list(self):
        assert stringify([]) == []

    def test_empty_dict(self):
        assert stringify({}) == {}


# ---------------------------------------------------------------------------
# _job_indices_to_array_str
# ---------------------------------------------------------------------------

class TestJobIndicesToArrayStr:

    def test_single_index(self):
        assert _job_indices_to_array_str([5]) == "5"

    def test_all_contiguous(self):
        assert _job_indices_to_array_str([0, 1, 2, 3]) == "0-3"

    def test_all_singletons(self):
        assert _job_indices_to_array_str([0, 2, 4]) == "0,2,4"

    def test_mixed_ranges_and_singletons(self):
        assert _job_indices_to_array_str([0, 1, 2, 5, 6, 9]) == "0-2,5-6,9"

    def test_starts_at_nonzero(self):
        assert _job_indices_to_array_str([3, 4, 5]) == "3-5"

    def test_single_two_element_range(self):
        assert _job_indices_to_array_str([4, 5]) == "4-5"

    def test_contiguous_then_gap_then_single(self):
        assert _job_indices_to_array_str([0, 1, 2, 3, 10]) == "0-3,10"

    def test_empty_list(self):
        assert _job_indices_to_array_str([]) == ""

    def test_large_non_contiguous(self):
        assert _job_indices_to_array_str([0, 100, 200]) == "0,100,200"


# ---------------------------------------------------------------------------
# Orchestrator.fill_config
# ---------------------------------------------------------------------------

class TestFillConfig:

    def test_empty_config_gets_all_defaults(self):
        cfg = Orchestrator.fill_config({})
        assert cfg["name"] == "canine"
        assert cfg["adapter"] == {"type": "Manual"}
        assert cfg["backend"] == {"type": "Local"}
        assert cfg["localization"] == {"strategy": "Batched"}
        assert cfg["outputs"] == {}

    def test_user_name_preserved(self):
        cfg = Orchestrator.fill_config({"name": "my_pipeline"})
        assert cfg["name"] == "my_pipeline"

    def test_user_backend_type_preserved(self):
        cfg = Orchestrator.fill_config({"backend": {"type": "Remote", "host": "slurm.example.com"}})
        assert cfg["backend"]["type"] == "Remote"
        assert cfg["backend"]["host"] == "slurm.example.com"

    def test_user_backend_merges_with_defaults(self):
        cfg = Orchestrator.fill_config({"backend": {"type": "Remote"}})
        assert cfg["backend"]["type"] == "Remote"

    def test_user_adapter_merged(self):
        cfg = Orchestrator.fill_config({"adapter": {"type": "Firecloud", "workspace": "broad/test"}})
        assert cfg["adapter"]["type"] == "Firecloud"
        assert cfg["adapter"]["workspace"] == "broad/test"

    def test_existing_outputs_preserved(self):
        cfg = Orchestrator.fill_config({"outputs": {"result": "*.txt"}})
        assert cfg["outputs"] == {"result": "*.txt"}

    def test_loads_from_yaml_file(self, tmp_path):
        config = {"name": "from_file", "inputs": {"sample": "NA12878"}}
        cfg_path = tmp_path / "pipeline.yaml"
        cfg_path.write_text(yaml.dump(config))
        cfg = Orchestrator.fill_config(str(cfg_path))
        assert cfg["name"] == "from_file"
        assert cfg["inputs"] == {"sample": "NA12878"}
        assert cfg["backend"] == {"type": "Local"}

    def test_user_localization_merged_with_defaults(self):
        cfg = Orchestrator.fill_config({"localization": {"strategy": "NFS", "staging_dir": "/mnt/nfs"}})
        assert cfg["localization"]["strategy"] == "NFS"
        assert cfg["localization"]["staging_dir"] == "/mnt/nfs"
