import pytest
from canine.adapters.base import ManualAdapter, maxdepth


# ---------------------------------------------------------------------------
# maxdepth
# ---------------------------------------------------------------------------

class TestMaxDepth:
    def test_scalar_int(self):
        assert maxdepth(42) == 0

    def test_scalar_string(self):
        assert maxdepth("hello") == 0

    def test_flat_list(self):
        assert maxdepth([1, 2, 3]) == 1

    def test_single_element_list(self):
        assert maxdepth([1]) == 1

    def test_nested_2d(self):
        assert maxdepth([[1, 2], [3, 4]]) == 2

    def test_nested_3d(self):
        assert maxdepth([[[1, 2]]]) == 3

    def test_ragged_uses_deepest_branch(self):
        # [1, [2, [3]]] -> max depth is 3 at the deepest branch
        assert maxdepth([1, [2, [3]]]) == 3

    def test_mixed_depth(self):
        assert maxdepth([[1], [2, 3]]) == 2


# ---------------------------------------------------------------------------
# ManualAdapter.parse_inputs
# ---------------------------------------------------------------------------

class TestManualAdapterParseInputs:

    # -- basic scatter cases --

    def test_all_scalars_produces_one_shard(self):
        a = ManualAdapter()
        spec = a.parse_inputs({"a": "foo", "b": "bar"})
        assert list(spec.keys()) == ["0"]
        assert spec["0"] == {"a": "foo", "b": "bar"}

    def test_single_list_produces_n_shards(self):
        a = ManualAdapter()
        spec = a.parse_inputs({"x": [10, 20, 30]})
        assert set(spec.keys()) == {"0", "1", "2"}
        assert spec["0"]["x"] == "10"
        assert spec["1"]["x"] == "20"
        assert spec["2"]["x"] == "30"

    def test_length_one_list_promoted_to_scalar(self):
        # HACK in parse_inputs: length-1 lists are unwrapped before scattering
        a = ManualAdapter()
        spec = a.parse_inputs({"a": [42], "b": "const"})
        assert list(spec.keys()) == ["0"]
        assert spec["0"] == {"a": "42", "b": "const"}

    def test_scalar_broadcast_across_list(self):
        a = ManualAdapter()
        spec = a.parse_inputs({"a": [1, 2, 3], "b": "const"})
        assert len(spec) == 3
        for i in range(3):
            assert spec[str(i)]["b"] == "const"
        assert spec["0"]["a"] == "1"
        assert spec["2"]["a"] == "3"

    def test_two_equal_length_lists_are_zipped(self):
        a = ManualAdapter()
        spec = a.parse_inputs({"a": [1, 2], "b": ["x", "y"]})
        assert len(spec) == 2
        assert spec["0"] == {"a": "1", "b": "x"}
        assert spec["1"] == {"a": "2", "b": "y"}

    def test_mismatched_list_lengths_raise(self):
        a = ManualAdapter()
        with pytest.raises(ValueError, match="equal length"):
            a.parse_inputs({"a": [1, 2, 3], "b": [1, 2]})

    # -- gather cases --

    def test_single_gather_broadcasts_inner_list(self):
        # [[4, 5, 6]] with a=[1,2] -> both shards get [4, 5, 6]
        a = ManualAdapter()
        spec = a.parse_inputs({"a": [1, 2], "b": [[4, 5, 6]]})
        assert spec["0"]["b"] == ["4", "5", "6"]
        assert spec["1"]["b"] == ["4", "5", "6"]

    def test_scatter_of_gathers(self):
        # a=[1,2], b=[[4,5,6],[7,8,9]] -> shard 0 gets [4,5,6], shard 1 gets [7,8,9]
        a = ManualAdapter()
        spec = a.parse_inputs({"a": [1, 2], "b": [[4, 5, 6], [7, 8, 9]]})
        assert spec["0"]["b"] == ["4", "5", "6"]
        assert spec["1"]["b"] == ["7", "8", "9"]

    # -- nesting depth --

    def test_nesting_deeper_than_2_raises(self):
        a = ManualAdapter()
        with pytest.raises(ValueError):
            a.parse_inputs({"a": [[[1, 2]]]})

    # -- product mode --

    def test_product_mode_cartesian(self):
        a = ManualAdapter(product=True)
        spec = a.parse_inputs({"a": [1, 2], "b": ["x", "y"]})
        assert len(spec) == 4
        pairs = {(v["a"], v["b"]) for v in spec.values()}
        assert pairs == {("1", "x"), ("1", "y"), ("2", "x"), ("2", "y")}

    def test_product_mode_with_scalar_and_list(self):
        a = ManualAdapter(product=True)
        spec = a.parse_inputs({"a": [1, 2, 3], "b": "const"})
        assert len(spec) == 3

    # -- alias --

    def test_alias_list_assigns_per_shard(self):
        a = ManualAdapter(alias=["alpha", "beta"])
        spec = a.parse_inputs({"x": [10, 20]})
        assert spec["0"]["CANINE_JOB_ALIAS"] == "alpha"
        assert spec["1"]["CANINE_JOB_ALIAS"] == "beta"

    def test_alias_list_length_mismatch_raises(self):
        a = ManualAdapter(alias=["only_one"])
        with pytest.raises(AssertionError):
            a.parse_inputs({"x": [1, 2, 3]})

    def test_duplicate_aliases_raise(self):
        a = ManualAdapter(alias=["same", "same"])
        with pytest.raises(ValueError):
            a.parse_inputs({"x": [1, 2]})

    def test_alias_as_string_uses_input_value_as_alias(self):
        a = ManualAdapter(alias="x")
        spec = a.parse_inputs({"x": ["alpha", "beta"]})
        assert spec["0"]["CANINE_JOB_ALIAS"] == "alpha"
        assert spec["1"]["CANINE_JOB_ALIAS"] == "beta"

    def test_alias_as_string_duplicate_values_raise(self):
        a = ManualAdapter(alias="x")
        with pytest.raises(ValueError):
            a.parse_inputs({"x": ["dup", "dup"]})

    # -- output type --

    def test_stringify_converts_ints_to_strings(self):
        a = ManualAdapter()
        spec = a.parse_inputs({"n": 42})
        assert spec["0"]["n"] == "42"

    def test_output_is_independent_copy(self):
        # spec property should return a fresh copy each time
        a = ManualAdapter()
        a.parse_inputs({"x": [1, 2]})
        s1 = a.spec
        s2 = a.spec
        assert s1 is not s2
