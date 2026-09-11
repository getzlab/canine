"""
Pure unit tests for results-bucket naming and provisioning.

The results bucket holds one namespace's task outputs. Workers write to it at
job teardown and downstream tasks read it back over a read-only gcsfuse mount,
so unlike the per-localization buckets it is long-lived and reused across runs.

Two things here are easy to get wrong and expensive to discover late:

  * The name shares its shape with a per-localization bucket
    (wolf-<project_number>-<region>-*), and localization buckets are swept by a
    lifecycle rule with no prefix filter. The distinguishing marker is a label,
    not the name.
  * The namespace is user-chosen, so the 63-char bucket-name ceiling is reachable.
    It must fail loudly rather than truncate: two namespaces differing only past
    the cutoff would otherwise share a bucket and interleave their results.
"""
from unittest.mock import MagicMock, patch

import pytest

from canine.utils import (
    RESULTS_BUCKET_LABEL,
    RESULTS_BUCKET_LABEL_VALUE,
    RESULTS_BUCKET_NAMESPACE_MAX_LEN,
    get_or_create_results_bucket,
    results_bucket_name,
)

PROJNUM = "123456789012"
LONGEST_REGION = "northamerica-northeast1"


class TestNaming:

    def test_basic_shape(self):
        assert results_bucket_name(PROJNUM, "us-central1", "wolf-test-slw") == \
            "wolf-123456789012-us-central1-wolf-test-slw"

    def test_namespace_is_sanitized(self):
        """wolF's default namespace is mixed-case; GCS names are not."""
        assert results_bucket_name(PROJNUM, "us-central1", "wolF-workspace") == \
            "wolf-123456789012-us-central1-wolf-workspace"

    def test_max_length_namespace_fits_in_longest_region(self):
        """
        The 21-char limit is derived from the worst case, so the worst case must
        actually fit inside the 63-char ceiling.
        """
        name = results_bucket_name(
          PROJNUM, LONGEST_REGION, "a" * RESULTS_BUCKET_NAMESPACE_MAX_LEN
        )
        assert len(name) <= 63

    def test_over_length_namespace_raises(self):
        """Hard failure, not truncation."""
        too_long = "a" * (RESULTS_BUCKET_NAMESPACE_MAX_LEN + 1)
        with pytest.raises(ValueError, match="too long"):
            results_bucket_name(PROJNUM, "us-central1", too_long)

    def test_limit_is_fixed_not_region_dependent(self):
        """
        A namespace must not be accepted in a short-named region and rejected in
        a long-named one -- that turns a naming limit into a deploy-time surprise
        the first time a workflow runs somewhere else.
        """
        too_long = "a" * (RESULTS_BUCKET_NAMESPACE_MAX_LEN + 1)
        for region in ("us-central1", LONGEST_REGION):
            with pytest.raises(ValueError, match="too long"):
                results_bucket_name(PROJNUM, region, too_long)

    def test_length_check_applies_to_sanitized_form(self):
        """
        Sanitization strips and collapses characters, so the check must run on
        the result, not the raw input. This raw namespace is over the limit but
        sanitizes to exactly the limit, so it must be accepted.
        """
        raw = "A" * RESULTS_BUCKET_NAMESPACE_MAX_LEN + "___"
        assert len(raw) > RESULTS_BUCKET_NAMESPACE_MAX_LEN  # would fail a naive check
        assert results_bucket_name(PROJNUM, "us-central1", raw) == \
            "wolf-123456789012-us-central1-" + "a" * RESULTS_BUCKET_NAMESPACE_MAX_LEN


def _fake_bucket(exists=True, labels=None, lifecycle_rules=()):
    bucket = MagicMock()
    bucket.exists.return_value = exists
    bucket.labels = dict(labels or {})
    bucket.lifecycle_rules = list(lifecycle_rules)
    return bucket


def _delete_rule(days):
    return {"action": {"type": "Delete"}, "condition": {"daysSinceCustomTime": days}}


class TestProvisioning:

    def _run(self, bucket, expiry_days=30):
        client = MagicMock()
        client.bucket.return_value = bucket
        client.create_bucket.return_value = bucket
        with patch("canine.utils.gcloud_storage_client", return_value=client), \
             patch("canine.utils.get_project_number", return_value=PROJNUM):
            name = get_or_create_results_bucket(
              "us-central1-a", "my-project", "ns", expiry_days=expiry_days
            )
        return name, client

    def test_returns_deterministic_name(self):
        name, _ = self._run(_fake_bucket())
        assert name == "wolf-123456789012-us-central1-ns"

    def test_existing_bucket_is_not_recreated(self):
        _, client = self._run(_fake_bucket(exists=True))
        client.create_bucket.assert_not_called()

    def test_missing_bucket_is_created_in_the_zones_region(self):
        _, client = self._run(_fake_bucket(exists=False))
        assert client.create_bucket.call_args.kwargs["location"] == "us-central1"

    def test_labelled_as_results(self):
        """Tooling keys off this label, since the name shape is shared."""
        bucket = _fake_bucket()
        self._run(bucket)
        assert bucket.labels[RESULTS_BUCKET_LABEL] == RESULTS_BUCKET_LABEL_VALUE

    def test_expiry_rule_uses_custom_time_not_age(self):
        bucket = _fake_bucket()
        self._run(bucket, expiry_days=30)
        assert _delete_rule(30) in bucket.lifecycle_rules

    def test_expiry_change_is_reconciled_on_reuse(self):
        """
        The bucket outlives any single run. A later run with a different
        expiry must actually replace the rule, not leave the old one in place.
        """
        bucket = _fake_bucket(lifecycle_rules=[_delete_rule(30)])
        self._run(bucket, expiry_days=7)
        assert bucket.lifecycle_rules == [_delete_rule(7)]

    @pytest.mark.parametrize("never", [0, None])
    def test_zero_or_none_means_never_expire(self, never):
        bucket = _fake_bucket(lifecycle_rules=[_delete_rule(30)])
        self._run(bucket, expiry_days=never)
        assert bucket.lifecycle_rules == []

    def test_unrelated_lifecycle_rules_are_preserved(self):
        """Only the customTime delete rule is ours to manage."""
        other = {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
                 "condition": {"age": 90}}
        bucket = _fake_bucket(lifecycle_rules=[other])
        self._run(bucket, expiry_days=30)
        assert other in bucket.lifecycle_rules

    def test_handles_generator_valued_lifecycle_rules(self):
        """
        google-cloud-storage's Bucket.lifecycle_rules is a property yielding a
        generator, not a list, so the reconcile logic must not index it or call
        len() on it. The mocks above hand back plain lists and would hide that;
        this models the real property shape.
        """
        class GeneratorRulesBucket:
            def __init__(self, rules):
                self._rules = rules
                self.labels = {RESULTS_BUCKET_LABEL: RESULTS_BUCKET_LABEL_VALUE}
                self.patched = 0

            @property
            def lifecycle_rules(self):
                return (r for r in self._rules)

            @lifecycle_rules.setter
            def lifecycle_rules(self, value):
                self._rules = list(value)

            def exists(self):
                return True

            def patch(self):
                self.patched += 1

        bucket = GeneratorRulesBucket([_delete_rule(30)])
        self._run(bucket, expiry_days=30)
        # already correct -> must be recognised as such, not rewritten
        assert bucket.patched == 0
        assert list(bucket.lifecycle_rules) == [_delete_rule(30)]

    def test_no_patch_when_already_correct(self):
        """Avoid a pointless write on every single cluster start."""
        bucket = _fake_bucket(
          labels={RESULTS_BUCKET_LABEL: RESULTS_BUCKET_LABEL_VALUE},
          lifecycle_rules=[_delete_rule(30)],
        )
        self._run(bucket, expiry_days=30)
        bucket.patch.assert_not_called()
