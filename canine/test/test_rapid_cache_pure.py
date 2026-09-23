"""
Pure unit tests for Rapid Cache provisioning: opt-in, and TTL aligned with the
localization bucket's own object expiry.

Rapid Cache is not free and not always a win. Cache storage bills per GiB-hour
at roughly 4x standard regional storage, while the transfer it would save is
$0/GiB within North America -- so a workload whose bucket and workers share a
region pays purely for read latency. It must therefore default to off, and its
TTL must not outlive the objects it caches.
"""
import inspect
from unittest.mock import MagicMock, patch

import pytest

from canine.utils import get_or_create_rapid_cache
from canine.backends.imageTransient import TransientImageSlurmBackend
from canine.backends.gcpTransient import TransientGCPSlurmBackend


class TestDefaultsOff:

    @pytest.mark.parametrize("cls", [TransientImageSlurmBackend, TransientGCPSlurmBackend])
    def test_rapid_cache_defaults_to_off(self, cls):
        """Opt-in only: nobody should get billed for a cache they did not ask for."""
        assert inspect.signature(cls.__init__).parameters["rapid_cache"].default is False

    @pytest.mark.parametrize("cls", [TransientImageSlurmBackend, TransientGCPSlurmBackend])
    def test_ttl_defaults_to_one_day(self, cls):
        """
        Must match AbstractLocalizer.localization_expiry_days (1). A longer cache
        TTL pays to cache objects that have already been deleted -- the previous
        7d default against a 1d expiry was ~7x the cache bill for no benefit.
        """
        assert inspect.signature(cls.__init__).parameters["rapid_cache_ttl"].default == "1d"

    def test_helper_ttl_default_matches(self):
        assert inspect.signature(get_or_create_rapid_cache).parameters["ttl"].default == "1d"


class TestGating:
    """
    The provisioning call must sit behind the flag. Asserting on source structure
    because reaching __enter__ needs a live GCE/Slurm cluster.
    """

    def test_image_transient_gates_on_the_flag(self):
        src = inspect.getsource(TransientImageSlurmBackend.__enter__)
        gate = src.index('if self.config["rapid_cache"]')
        call = src.index("get_or_create_rapid_cache(")
        assert gate < call, "provisioning must be inside the flag check"

    def test_failure_stays_non_fatal(self):
        """Cache affects read speed, not correctness; it must never fail startup."""
        src = inspect.getsource(TransientImageSlurmBackend.__enter__)
        seg = src[src.index("get_or_create_rapid_cache("):]
        assert "except Exception" in seg


class TestCachesTheResultsBucket:
    """
    The cache used to be provisioned against a per-workflow bucket that nothing
    ever read (`grep storage_bucket canine/localization/` returned nothing), so
    rapid_cache=True billed ~4x standard storage to accelerate an empty bucket.
    It now caches the bucket that task outputs actually land in.
    """

    def test_provisions_the_results_bucket(self):
        src = inspect.getsource(TransientImageSlurmBackend.__enter__)
        call = src[src.index("get_or_create_rapid_cache("):]
        assert 'self.config["results_bucket"]' in call.split(")")[0]

    def test_does_not_reference_the_deleted_workflow_bucket(self):
        src = inspect.getsource(TransientImageSlurmBackend.__enter__)
        assert "storage_bucket" not in src
        assert "get_or_create_workflow_bucket" not in src

    def test_workflow_bucket_helper_is_gone(self):
        """Its only caller was the dead cache path; leaving it invites re-use."""
        import canine.utils
        assert not hasattr(canine.utils, "get_or_create_workflow_bucket")

    def test_ingest_on_write_is_explicit(self):
        """
        This is what makes the cache useful at all here: it populates as a
        producer writes, so the consumer's read-after-write hits cache. It does
        not speed up the write -- object metadata is never cached.
        """
        src = inspect.getsource(TransientImageSlurmBackend.__enter__)
        call = src[src.index("get_or_create_rapid_cache("):]
        assert "ingest_on_write = True" in call.split(")")[0]

    def test_skipped_when_there_is_no_results_bucket(self):
        """
        In shared mode outputs stay on the NFS mount, so results_bucket is None.
        Passing None through would provision a cache for the literal string
        "None" or raise inside startup.
        """
        src = inspect.getsource(TransientImageSlurmBackend.__enter__)
        gate = src.index('if self.config["rapid_cache"]')
        call = src.index("get_or_create_rapid_cache(")
        assert 'self.config["results_bucket"] is not None' in src[gate:call]

    def test_gcp_transient_no_longer_provisions_a_cache(self):
        """
        That backend has no workdir_mode and so no results bucket, and the
        per-localization input buckets are created on demand by the localizer --
        there is no single bucket for a cache to be pointed at. It warns instead
        of silently billing for one.
        """
        src = inspect.getsource(TransientGCPSlurmBackend.__enter__)
        assert "get_or_create_rapid_cache(" not in src
        assert "if self.rapid_cache:" in src
        assert "warning" in src[src.index("if self.rapid_cache:"):]


class TestHelperCommand:

    def _create_cmd(self, **kw):
        """Capture the argv get_or_create_rapid_cache would run."""
        seen = []

        def fake_run(cmd, **_):
            seen.append(cmd)
            r = MagicMock()
            r.returncode = 0
            r.stdout = b""        # no existing cache -> proceed to create
            r.stderr = b""
            return r

        with patch("canine.utils.subprocess.run", side_effect=fake_run):
            get_or_create_rapid_cache("bkt", "us-central1-c", **kw)
        return [c for c in seen if "create" in c]

    def test_ttl_is_passed_through(self):
        cmd = self._create_cmd(ttl="1d")[0]
        assert "--ttl=1d" in cmd

    def test_ingest_on_write_enabled_by_default(self):
        """Without it a first read is always a miss, so a read-once cache is pointless."""
        assert "--enable-ingest-on-write" in self._create_cmd()[0]

    def test_ingest_on_write_can_be_disabled(self):
        assert "--enable-ingest-on-write" not in self._create_cmd(ingest_on_write=False)[0]

    def test_existing_cache_in_zone_is_not_recreated(self):
        """GCS allows only one cache instance per zone per bucket."""
        def fake_run(cmd, **_):
            r = MagicMock()
            r.returncode = 0
            r.stdout = b"us-central1-c\n"   # already present
            r.stderr = b""
            return r
        with patch("canine.utils.subprocess.run", side_effect=fake_run) as m:
            get_or_create_rapid_cache("bkt", "us-central1-c")
        assert all("create" not in c for c in [a.args[0] for a in m.call_args_list])
