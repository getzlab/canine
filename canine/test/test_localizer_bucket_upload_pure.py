"""
Pure unit tests for AbstractLocalizer.bucket_upload_script() -- the worker-side
commands that place inputs into a per-localization bucket. No SLURM cluster, no
GCP credentials, no Docker.

The central assertion here is that the generated script never mentions
self.staging_dir. Routing every localized byte through the shared NFS mount --
written on download, read back on re-upload -- is precisely the regression this
code path exists to remove, and it is invisible to any test that only checks
that localization "worked".
"""
import io
import json
import shutil
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.base import UploadItem
from canine.localization.local import BatchedLocalizer


def make_localizer(**kwargs):
    kwargs.pop("direct_bucket_upload", None)  # flag removed in stage 5; single path now
    return BatchedLocalizer(MagicMock(), **kwargs)


def gs_item(path="gs://src-bucket/reads.bam", dest="gs://wolf-1-us-central1-abc/inp/reads.bam",
            rp_string="", is_dir=False):
    """An UploadItem whose handler looks enough like HandleGSURL for the emitter."""
    fh = MagicMock()
    fh.path = path
    fh.rp_string = rp_string
    fh.is_dir = is_dir
    return UploadItem(fh=fh, dest=dest, kind="server_side")


def script_for(items, wait_tries=60, region="us-central1"):
    loc = make_localizer(bucket_upload_wait_tries=wait_tries)
    return "\n".join(loc.bucket_upload_script(items, "gs://wolf-1-us-central1-abc", region))


class TestNoNFSInvolvement:

    def test_staging_dir_never_appears(self):
        """The whole point: no byte, and no path, goes through the shared mount."""
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace/somejob"  # backend is mocked; pin a real NFS path
        script = "\n".join(
            loc.bucket_upload_script([gs_item()], "gs://wolf-1-us-central1-abc", "us-central1")
        )
        assert loc.staging_dir not in script
        assert "/mnt/nfs" not in script

    def test_copy_is_gs_to_gs(self):
        """Both endpoints are GCS, so the copy is a server-side rewrite."""
        script = script_for([gs_item()])
        cp = [l for l in script.splitlines() if "storage cp" in l]
        assert len(cp) == 1
        assert "gs://src-bucket/reads.bam" in cp[0]
        assert "gs://wolf-1-us-central1-abc/inp/reads.bam" in cp[0]

    def test_one_copy_per_input(self):
        items = [
            gs_item(path="gs://s/a.bam", dest="gs://b/i/a.bam"),
            gs_item(path="gs://s/b.bam", dest="gs://b/i/b.bam"),
            gs_item(path="gs://s/c.bam", dest="gs://b/i/c.bam"),
        ]
        script = script_for(items)
        assert len([l for l in script.splitlines() if "storage cp" in l]) == 3


class TestCopyFlags:

    def test_no_clobber_so_a_requeued_shard_does_not_recopy(self):
        assert "-n" in [l for l in script_for([gs_item()]).splitlines() if "storage cp" in l][0]

    def test_custom_time_set_on_upload(self):
        """
        Without --custom-time the daysSinceCustomTime lifecycle rule can never
        match the object, so nothing ever expires and the bucket grows forever.
        """
        cp = [l for l in script_for([gs_item()]).splitlines() if "storage cp" in l][0]
        assert "--custom-time=" in cp

    def test_requester_pays_source_carries_billing_project(self):
        cp = [l for l in script_for([gs_item(rp_string=" --billing-project=proj")]).splitlines()
              if "storage cp" in l][0]
        assert "--billing-project=proj" in cp

    def test_non_requester_pays_has_no_billing_project(self):
        assert "--billing-project" not in script_for([gs_item()])

    def test_directory_source_targets_parent(self):
        """`cp -r gs://a/d gs://B/k/d` would otherwise yield gs://B/k/d/d/..."""
        cp = [l for l in script_for([
            gs_item(path="gs://src/dir", dest="gs://b/inp/dir", is_dir=True)
        ]).splitlines() if "storage cp" in l][0]
        assert cp.rstrip().endswith("gs://b/inp/")

    def test_file_source_targets_the_object_itself(self):
        cp = [l for l in script_for([gs_item()]).splitlines() if "storage cp" in l][0]
        assert cp.rstrip().endswith("gs://wolf-1-us-central1-abc/inp/reads.bam")


class TestBucketLifecycle:

    def test_create_applies_lifecycle_rule_inline(self):
        """
        The rule must be applied at creation -- a follow-up call to set it might
        never happen if the job dies in between.
        """
        script = script_for([gs_item()])
        assert "buckets create" in script
        assert "--lifecycle-file=" in script
        assert "daysSinceCustomTime" in script

    def test_create_pins_the_region(self):
        assert "--location=europe-west1" in script_for([gs_item()], region="europe-west1")

    def test_soft_delete_is_disabled_at_creation(self):
        """
        New GCS buckets get 7-day soft delete by default, which retains and
        *bills* for every expired object for a further week -- silently undoing
        a 1-day expiry on multi-TB localizations. This is a cache; nothing here
        is worth undeleting.
        """
        create = [l for l in script_for([gs_item()]).splitlines() if "buckets create" in l][0]
        assert "--soft-delete-duration=0" in create

    def test_expiry_defaults_to_one_day(self):
        line = self._lifecycle_line(script_for([gs_item()]))
        assert json.loads(line)["rule"][0]["condition"]["daysSinceCustomTime"] == 1

    def test_expiry_is_configurable(self):
        loc = make_localizer(localization_expiry_days=7)
        script = "\n".join(loc.bucket_upload_script([gs_item()], "gs://b", "us-central1"))
        assert json.loads(self._lifecycle_line(script))["rule"][0]["condition"]["daysSinceCustomTime"] == 7

    def test_lifecycle_body_is_valid_json(self):
        """It is emitted through str.format, so a brace slip would corrupt it."""
        for days in (1, 30):
            loc = make_localizer(localization_expiry_days=days)
            script = "\n".join(loc.bucket_upload_script([gs_item()], "gs://b", "us-central1"))
            json.loads(self._lifecycle_line(script))

    @staticmethod
    def _lifecycle_line(script):
        return [l for l in script.splitlines() if "daysSinceCustomTime" in l][0]


class TestStateMachine:

    def test_owner_labels_working_then_success(self):
        script = script_for([gs_item()])
        assert script.index("wolf=working") < script.index("wolf=success")

    def test_upload_happens_between_the_two_labels(self):
        script = script_for([gs_item()])
        assert script.index("wolf=working") < script.index("storage cp") < script.index("wolf=success")

    def test_waits_for_bucket_to_resolve_before_reading_label(self):
        """
        Under contention the dominant 409 is the transient "a conflicting
        operation is currently in progress", which means the create is still in
        flight -- not that the bucket can be read yet.
        """
        script = script_for([gs_item()])
        assert "buckets describe" in script
        assert script.index("buckets describe") < script.index("labels.wolf")

    def test_absent_label_is_not_treated_as_success(self):
        """
        `buckets create` cannot set labels, so there is a window where the bucket
        exists unlabelled. Reading that as "not success" and uploading would
        duplicate a sibling's work; only an explicit "success" may short-circuit.
        """
        script = script_for([gs_item()])
        assert '"$CANINE_BUCKET_STATE" == "success"' in script

    def test_expired_content_is_repopulated(self):
        script = script_for([gs_item()])
        assert "storage ls" in script
        assert "expired" in script

    def test_present_content_gets_its_expiry_refreshed(self):
        """Content still in use must not age out from under a running workflow."""
        assert "objects update" in script_for([gs_item()])

    def test_timeout_requeues_on_another_node(self):
        assert "exit 5" in script_for([gs_item()])

    def test_stale_claim_is_taken_over_not_waited_on(self):
        """
        Regression: the timeout path marks the bucket "stale" and exits 5. If the
        job that retries elsewhere treated "stale" like "working" it would wait
        again -- nobody would ever finish the upload, and the shard would just
        ping-pong between nodes until its retry budget ran out.
        """
        script = script_for([gs_item()])
        assert '"$CANINE_BUCKET_STATE" == "stale"' in script
        stale_branch = script.index('== "stale"')
        wait_branch = script.index("CANINE_BUCKET_WAITS=$((")
        assert stale_branch < wait_branch, "stale must be handled before the wait branch"

    def test_wait_tries_is_configurable(self):
        assert "-ge 7 " in script_for([gs_item()], wait_tries=7)


class TestGeneratedShellIsValid:
    """
    The emitter builds bash by string concatenation, so a quoting or `if/fi`
    slip produces a script that only fails at runtime on a worker. Parse it.
    """

    def _check(self, script):
        import shutil
        import subprocess
        bash = shutil.which("bash")
        if bash is None:
            pytest.skip("bash not available")
        proc = subprocess.run([bash, "-n"], input=script.encode(), capture_output=True)
        assert proc.returncode == 0, proc.stderr.decode()

    def test_single_input(self):
        self._check("set -e\n" + script_for([gs_item()]))

    def test_multiple_inputs_incl_directory_and_requester_pays(self):
        self._check("set -e\n" + script_for([
            gs_item(path="gs://s/reads.bam", dest="gs://b/inp/reads.bam"),
            gs_item(path="gs://rp/dir", dest="gs://b/ref/dir",
                    rp_string=" --billing-project=proj", is_dir=True),
        ]))

    def test_paths_with_spaces_are_quoted(self):
        script = script_for([
            gs_item(path="gs://s/a file.bam", dest="gs://b/inp/a file.bam")
        ])
        self._check("set -e\n" + script)
        assert "'gs://s/a file.bam'" in script


class TestCreateBucketMountLayouts:
    """
    Characterization of create_bucket_mount's two object layouts. The legacy one
    had no test coverage at all before the direct_bucket_upload refactor pulled
    its URL construction apart, so these pin it down: with the flag off, nothing
    about the emitted bucketmount:// URLs may change.

    dry_run=True is used throughout: it returns before the _SUCCESS marker check,
    so no GCS client is needed, while still exercising the URL construction.
    """

    def _inputs(self):
        fh = MagicMock()
        fh.path = "gs://src/reads.bam"
        fh.hash = "deadbeef"
        fh.size = 1234
        fh.localization_mode = "url"
        return {"filename": [fh]}

    def test_direct_layout_is_a_dedicated_bucket_with_no_prefix(self):
        loc = make_localizer()
        loc.backend.config = {"storage_bucket": "unused", "zone": "us-central1-c"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, paths, _ = loc.create_bucket_mount(self._inputs(), dry_run=True)
        url = paths["filename"][0]
        assert url.startswith("bucketmount://wolf-406002258908-us-central1-")
        assert url.endswith("/filename/reads.bam")
        # no "canine-<hash>/" prefix segment: the bucket *is* the content address
        assert "/canine-" not in url

    def test_compute_zone_config_key_is_honoured(self):
        """
        Regression: only gcpTransient records the zone as "zone". imageTransient
        -- and therefore DockerTransient, the backend wolF actually runs --
        records it as "compute_zone" and has no "zone" key at all, so reading
        only "zone" meant the configured zone was never seen and auto-detection
        ran every time. get_default_gcp_zone is patched to explode: reaching it
        at all is the failure.
        """
        loc = make_localizer()
        loc.backend.config = {"compute_zone": "europe-west4-a"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"), \
             patch("canine.localization.base.get_default_gcp_zone",
                   side_effect=AssertionError("must not auto-detect a configured zone")):
            _, _, paths, _ = loc.create_bucket_mount(self._inputs(), dry_run=True)
        assert "-europe-west4-" in paths["filename"][0]

    def test_explicit_zone_key_wins_over_compute_zone(self):
        loc = make_localizer()
        loc.backend.config = {"zone": "us-central1-c", "compute_zone": "europe-west4-a"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, paths, _ = loc.create_bucket_mount(self._inputs(), dry_run=True)
        assert "-us-central1-" in paths["filename"][0]

    def test_identical_inputs_converge_on_one_bucket(self):
        """Content addressing: the same input set must reuse the same bucket."""
        names = []
        for _ in range(2):
            loc = make_localizer()
            loc.backend.config = {"zone": "us-central1-c"}
            loc.project = "proj"
            with patch("canine.localization.base.get_project_number", return_value="406002258908"):
                _, _, paths, _ = loc.create_bucket_mount(self._inputs(), dry_run=True)
            names.append(paths["filename"][0].split("/")[2])
        assert names[0] == names[1]

    def test_local_mode_inputs_are_uploaded_not_silently_dropped(self):
        """
        Regression: a "local" input (typically an upstream task's output) was
        given a bucketmount:// URL but left out of the upload plan. With an
        all-local localization the plan came back empty, so bucket_upload_script
        was skipped entirely, the bucket was never created, and the consumer
        tried to gcsfuse-mount a bucket that did not exist. This is the ordinary
        `LocalizeToDisk(files=upstream["out"])` pattern.
        """
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace"
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, paths, plan = loc.create_bucket_mount(
                {"reference": [self._local_fh()]}, dry_run=False)

        assert len(plan) == 1, "an all-local localization must still produce a plan"
        assert plan[0].kind == "copy"
        assert "reference" in paths

    def test_local_input_is_uploaded_from_its_existing_path(self):
        """No re-staging: it is already on the shared mount, so upload from there."""
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace"
        item = UploadItem(fh=self._local_fh(), dest="gs://b/reference/ref.fa", kind="copy")
        script = "\n".join(loc.bucket_upload_script([item], "gs://b", "us-central1"))
        cp = [l for l in script.splitlines() if "storage cp" in l]
        assert len(cp) == 1
        assert "/mnt/nfs/workspace/ref.fa" in cp[0]
        assert "gs://b/reference/ref.fa" in cp[0]
        assert "--custom-time=" in cp[0]

    def test_upstream_output_in_a_sibling_task_dir_is_accepted(self):
        """
        Regression: the reachability guard originally required the path to sit
        under self.staging_dir -- the *current task's* directory. Upstream
        outputs live in sibling task dirs on the same shared mount, so the
        ordinary LocalizeToDisk(files=upstream["out"]) pattern was rejected
        outright on a live cluster.
        """
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/ws/run/BatchLocalDisk__2026-09-06--02-05-09_x"
        fh = self._local_fh()
        fh.path = "/mnt/nfs/ws/run/produce__2026-09-06--02-03-37_y/outputs/0/big/big.txt"
        item = UploadItem(fh=fh, dest="gs://b/big_file/big.txt", kind="copy")
        script = "\n".join(loc.bucket_upload_script([item], "gs://b", "us-central1"))
        assert fh.path in script

    def test_never_deletes_an_upstream_output_it_did_not_copy(self):
        """
        Data-loss regression. FileType.__init__ seeds localized_path with the
        *original* path; localize_file() is what repoints it at a copy. Local
        inputs become bucket mounts before localize_file ever runs, so an
        unguarded `rm -f localized_path` deletes the upstream task's output.
        A previous run did exactly that, leaving only .crc32c sidecars.
        """
        fh = self._local_fh()
        fh.localized_path = fh.path          # never copied: the original
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace"
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        loc.inputs = {"0": {"reference": [fh]}}
        loc.rodisk_paths = {}

        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, paths, plan = loc.create_bucket_mount({"reference": [fh]}, dry_run=False)

        # the file we are about to upload from must not be scheduled for deletion
        assert plan[0].kind == "copy"
        assert plan[0].fh.path == "/mnt/nfs/workspace/ref.fa"

    @staticmethod
    def _local_fh():
        fh = MagicMock()
        fh.path = "/mnt/nfs/workspace/ref.fa"
        fh.hash = "cafebabe"
        fh.size = 99
        fh.localization_mode = "local"
        return fh

    def test_dry_run_emits_no_upload_plan(self):
        loc = make_localizer()
        loc.backend.config = {"storage_bucket": "b", "zone": "us-central1-c"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, skip, _, plan = loc.create_bucket_mount(self._inputs(), dry_run=True)
        assert skip is True
        assert plan == []


def s3_item(path="s3://src/reads.bam", dest="gs://wolf-1-us-central1-abc/inp/reads.bam",
            command="aws s3api get-object --bucket src --key reads.bam >(cat >> DEST)"):
    """An UploadItem for a source with no server-side copy."""
    fh = MagicMock()
    fh.path = path
    fh.rp_string = ""
    fh.is_dir = False
    fh.localization_command = MagicMock(return_value=command)
    return UploadItem(fh=fh, dest=dest, kind="mount")


class TestNonGsSourcesUseAReadWriteMount:
    """
    Sources with no server-side copy must still keep the shared NFS mount out of
    the data path: the bucket is mounted read-write and the download is written
    straight into it, so gcsfuse streams the bytes up as they arrive.
    """

    def test_still_never_touches_nfs(self):
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace/somejob"
        script = "\n".join(loc.bucket_upload_script([s3_item()], "gs://wolf-1-us-central1-abc", "us-central1"))
        assert loc.staging_dir not in script
        assert "/mnt/nfs" not in script

    def test_mount_is_writable(self):
        """The consumer mount uses `-o ro`; this one must not."""
        script = script_for([s3_item()])
        mount = [l for l in script.splitlines() if "gcsfuse" in l]
        assert len(mount) == 1
        assert "-o ro" not in mount[0]
        assert "--implicit-dirs" in mount[0]

    def test_uses_a_separate_mountpoint_from_the_consumer_mounts(self):
        """
        /mnt/bucketmounts is reused across jobs with a "mount if not already
        mounted" shortcut that is only safe because those mounts are read-only.
        """
        script = script_for([s3_item()])
        assert "/mnt/localize/" in script
        assert "/mnt/bucketmounts" not in script

    def test_download_writes_into_the_mount(self):
        item = s3_item(dest="gs://wolf-1-us-central1-abc/inp/reads.bam")
        script = script_for([item])
        called_with = item.fh.localization_command.call_args[0][0]
        assert called_with == "/mnt/localize/wolf-1-us-central1-abc/inp/reads.bam"
        assert "aws s3api get-object" in script

    def test_unmounts_before_anything_else_runs(self):
        """Unmount finalizes the objects and leaves nothing writable exposed."""
        script = script_for([s3_item()])
        assert "fusermount -u" in script
        assert script.index("aws s3api") < script.index("fusermount -u")

    def test_verifies_objects_landed_before_labelling_success(self):
        # "storage ls <obj>" also appears in the state machine's existing-content
        # check, so anchor on the post-upload error text instead
        script = script_for([s3_item()])
        assert script.index("fusermount -u") < script.index("missing after upload")
        assert script.index("missing after upload") < script.index("wolf=success")

    def test_custom_time_is_set_after_the_mount_write(self):
        """
        gcsfuse cannot set customTime on write, and an object without it is
        invisible to the daysSinceCustomTime rule -- it would never expire.
        The gs:// path gets this from `cp --custom-time`; this path must not
        silently skip it.
        """
        script = script_for([s3_item()])
        update = [l for l in script.splitlines()
                  if "objects update" in l and "inp/reads.bam" in l and "$CANINE_BUCKET_CT" in l]
        assert update, "non-gs uploads must have customTime applied after unmount"
        assert script.index("fusermount -u") < script.index(update[0])

    def test_mixed_plan_uses_the_right_mechanism_for_each(self):
        script = script_for([
            gs_item(path="gs://s/a.bam", dest="gs://wolf-1-us-central1-abc/inp/a.bam"),
            s3_item(path="s3://s/b.bam", dest="gs://wolf-1-us-central1-abc/inp/b.bam"),
        ])
        assert "storage cp" in script          # server-side for the gs:// one
        assert "gcsfuse" in script             # rw mount for the s3:// one
        assert "gs://s/a.bam" in script

    def test_gs_only_plan_needs_no_mount_at_all(self):
        assert "gcsfuse" not in script_for([gs_item()])

    def test_generated_shell_is_valid(self):
        import subprocess
        bash = shutil.which("bash")
        if bash is None:
            pytest.skip("bash not available")
        script = "set -e\n" + script_for([
            gs_item(path="gs://s/a.bam", dest="gs://wolf-1-us-central1-abc/inp/a.bam"),
            s3_item(path="s3://s/b.bam", dest="gs://wolf-1-us-central1-abc/inp/b.bam"),
        ])
        proc = subprocess.run([bash, "-n"], input=script.encode(), capture_output=True)
        assert proc.returncode == 0, proc.stderr.decode()
