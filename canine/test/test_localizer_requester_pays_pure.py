"""
Pure unit tests for AbstractLocalizer.get_requester_pays()'s describe-vs-ls
fallback and the allow_requester_pays download gate -- no SLURM cluster, no
Docker required (uses a plain MagicMock backend instead of DummySlurmBackend,
since test_localizer_batched.py's module-level Docker setup is unavailable in
some environments).

Regression coverage for a bug found via live testing against a real bucket
(gs://gtex-resources): `gcloud storage buckets describe` requires
storage.buckets.get, which many requester-pays buckets don't grant even when
object-level read access works fine. get_requester_pays() must fall back to
`gcloud storage ls` (object-level) whenever `describe` fails for *any* reason,
not just when its error text happens to contain "404".
"""
import io
from unittest.mock import MagicMock

import pytest

from canine.localization.local import BatchedLocalizer


def _invoke_result(rc, stdout=b"", stderr=b""):
    return (rc, io.BytesIO(stdout), io.BytesIO(stderr))


def make_localizer(**kwargs):
    return BatchedLocalizer(MagicMock(), **kwargs)


class TestGetRequesterPaysFallback:

    def test_describe_succeeds_used_directly(self):
        loc = make_localizer()
        loc.backend.invoke = MagicMock(return_value=_invoke_result(0, stdout=b"True"))
        assert loc.get_requester_pays("gs://bucket/file.txt") is True
        loc.backend.invoke.assert_called_once()  # never falls through to ls

    def test_permission_denied_on_describe_falls_back_to_ls_and_detects_requester_pays(self):
        loc = make_localizer()
        loc.backend.invoke = MagicMock(side_effect=[
            _invoke_result(1, stderr=b"403 ... does not have storage.buckets.get access to the Google Cloud Storage bucket ..."),
            _invoke_result(1, stderr=b"HTTPError 400: Bucket is a requester pays bucket but no user project provided."),
        ])
        assert loc.get_requester_pays("gs://bucket/file.txt") is True
        assert loc.backend.invoke.call_count == 2

    def test_permission_denied_on_describe_and_ls_succeeds_non_requester_pays(self):
        loc = make_localizer()
        loc.backend.invoke = MagicMock(side_effect=[
            _invoke_result(1, stderr=b"403 ... does not have storage.buckets.get access to the Google Cloud Storage bucket ..."),
            _invoke_result(0),
        ])
        assert loc.get_requester_pays("gs://bucket/file.txt") is False

    def test_object_truly_missing_raises(self):
        loc = make_localizer()
        loc.backend.invoke = MagicMock(side_effect=[
            _invoke_result(1, stderr=b"404: gs://bucket not found"),
            _invoke_result(1, stderr=b"404: gs://bucket/file.txt not found"),
        ])
        with pytest.raises(Exception):
            loc.get_requester_pays("gs://bucket/file.txt")

    def test_result_is_cached_per_bucket(self):
        loc = make_localizer()
        loc.backend.invoke = MagicMock(return_value=_invoke_result(0, stdout=b"True"))
        assert loc.get_requester_pays("gs://bucket/a.txt") is True
        assert loc.get_requester_pays("gs://bucket/b.txt") is True
        loc.backend.invoke.assert_called_once()  # second call hits the cache


class TestDownloadGate:
    """gs_dircp/gs_copy's allow_requester_pays gate on the download leg."""

    def test_denied_by_default_raises_before_any_command(self):
        loc = make_localizer()
        loc.get_requester_pays = MagicMock(return_value=True)
        loc.backend.invoke = MagicMock()
        with pytest.raises(ValueError, match="disabled"):
            loc.gs_copy("gs://bucket/file.txt", "/local/dest", "remote")
        loc.backend.invoke.assert_not_called()

    def test_allowed_adds_billing_project(self):
        loc = make_localizer(allow_requester_pays=True, project="my-project")
        loc.get_requester_pays = MagicMock(return_value=True)
        loc.backend.invoke = MagicMock(return_value=_invoke_result(0))
        loc.gs_copy("gs://bucket/file.txt", "/local/dest", "remote")
        command = loc.backend.invoke.call_args[0][0]
        assert "--billing-project=my-project" in command

    def test_non_requester_pays_bucket_unaffected_by_flag(self):
        loc = make_localizer()
        loc.get_requester_pays = MagicMock(return_value=False)
        loc.backend.invoke = MagicMock(return_value=_invoke_result(0))
        loc.gs_copy("gs://bucket/file.txt", "/local/dest", "remote")
        loc.backend.invoke.assert_called_once()
