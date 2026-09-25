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
import re
import shlex
import shutil
from unittest.mock import MagicMock, patch

import pytest

from canine.localization.base import UploadItem
from canine.localization.file_handlers import HandleGSURL
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


def copy_command(line):
    """A copy line without the `&& break` of the retry loop it sits in."""
    line = line.strip()
    return line[:-len("&& break")].rstrip() if line.endswith("&& break") else line


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
        """
        Both endpoints are GCS, so the copy is a server-side rewrite, identified
        by referencing the real source path.
        """
        script = script_for([gs_item()])
        cp = [l for l in script.splitlines() if "storage cp" in l and "gs://src-bucket/reads.bam" in l]
        assert len(cp) == 1
        assert "gs://wolf-1-us-central1-abc/inp/reads.bam" in cp[0]

    def test_one_copy_per_input(self):
        items = [
            gs_item(path="gs://s/a.bam", dest="gs://b/i/a.bam"),
            gs_item(path="gs://s/b.bam", dest="gs://b/i/b.bam"),
            gs_item(path="gs://s/c.bam", dest="gs://b/i/c.bam"),
        ]
        script = script_for(items)
        # one plain-copy line reading from each real source path
        assert len([l for l in script.splitlines() if "storage cp" in l and "gs://s/" in l]) == 3


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
        """The source may be requester-pays even though the localization bucket is not."""
        script = script_for([gs_item(rp_string=" --billing-project=proj")])
        assert any("--billing-project=proj" in l and "gs://src-bucket/reads.bam" in l for l in script.splitlines())

    def test_non_requester_pays_has_no_billing_project(self):
        assert "--billing-project" not in script_for([gs_item()])

    def test_directory_source_targets_parent(self):
        """`cp -r gs://a/d gs://B/k/d` would otherwise yield gs://B/k/d/d/..."""
        cp = [l for l in script_for([
            gs_item(path="gs://src/dir", dest="gs://b/inp/dir", is_dir=True)
        ]).splitlines() if "storage cp" in l][0]
        assert copy_command(cp).endswith("gs://b/inp/")

    def test_file_source_targets_the_object_itself(self):
        cp = [l for l in script_for([gs_item()]).splitlines() if "storage cp" in l][0]
        assert copy_command(cp).endswith("gs://wolf-1-us-central1-abc/inp/reads.bam")

class TestContentEncoding:
    """
    A gs:// source object can carry Content-Encoding: gzip as transport metadata. A
    server-side rewrite preserves that metadata -- and the still-compressed bytes --
    verbatim, so every reader of the bucket-mount gets the gzip stream. A consumer with
    no gzip-awareness of its own has no way to know: GATK opening a plain-text reference
    .dict reported "Failed to load reference dictionary", with nothing pointing at
    Content-Encoding. Clearing the metadata afterward does not help either; the stored
    bytes stay compressed regardless of the tag.

    Such an object is classified at plan time from metadata already fetched and routed
    to the "mount" kind, where the downloader streams the stored bytes through a
    verified decode onto the read-write mount. It replaced a runtime `describe` +
    `gcloud storage cat | gunzip > $(mktemp)` + re-upload in the server-side branch,
    which staged the whole decompressed object on the boot disk, decoded mislabelled
    .vcf.gz files, and fell through to the plain copy whenever `describe` failed.
    """

    def test_the_server_side_branch_is_a_plain_copy(self):
        script = script_for([gs_item()])
        for absent in ("objects describe", "gunzip", "storage cat", "CANINE_DECOMP_TMP"):
            assert absent not in script, absent
        cp = [l for l in script.splitlines() if "storage cp" in l]
        assert len(cp) == 1
        assert "gs://src-bucket/reads.bam" in cp[0] and "-r -n" in cp[0]

    def test_directory_sources_take_the_plain_copy(self):
        """
        No gzip-encoded directory tree has been seen, and classifying one means
        inspecting every object under the prefix.
        """
        script = script_for([gs_item(path="gs://src/dir", dest="gs://b/inp/dir", is_dir=True)])
        assert "objects describe" not in script
        assert "gunzip" not in script

    def test_local_copy_has_no_content_encoding_handling(self):
        """
        A "copy"-kind upload writes fresh bytes from a local/shared-mount file
        -- there's no pre-existing GCS object metadata for `cp` to inherit, so
        there's nothing here for this feature to guard against.
        """
        fh = MagicMock()
        fh.path = "/mnt/nfs/workspace/ref.fa"
        fh.localization_mode = "local"
        item = UploadItem(fh=fh, dest="gs://b/reference/ref.fa", kind="copy")
        script = "\n".join(make_localizer().bucket_upload_script([item], "gs://b", "us-central1"))
        assert "objects describe" not in script
        assert "gunzip" not in script

    @staticmethod
    def _gs_fh(transport_gzip):
        fh = MagicMock(spec=HandleGSURL)
        fh.path = "gs://src-bucket/ref.dict"
        fh.rp_string = ""
        fh.is_dir = False
        fh.transport_gzip = transport_gzip
        fh.hash = "deadbeef"
        fh.size = 1234
        fh.localization_mode = "url"
        fh.downloader_command = MagicMock(return_value="K9_DECODE_MARKER --gs-source x --gunzip")
        return fh

    def _plan(self, fh):
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace"
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, _, plan = loc.create_bucket_mount({"reference": [fh]}, dry_run=False)
        return plan

    def test_an_encoded_object_is_planned_onto_the_mount(self):
        plan = self._plan(self._gs_fh(transport_gzip=True))
        assert [item.kind for item in plan] == ["mount"]

    def test_an_unencoded_object_keeps_the_server_side_copy(self):
        plan = self._plan(self._gs_fh(transport_gzip=False))
        assert [item.kind for item in plan] == ["server_side"]

    def test_the_mount_loop_uses_the_decoding_downloader(self):
        """
        Not localization_command: for a gs:// source that is a `gcloud storage cp`,
        whose sliced download writes out of order and stages on local disk.
        """
        fh = self._gs_fh(transport_gzip=True)
        item = UploadItem(fh=fh, dest="gs://wolf-1-us-central1-abc/reference/ref.dict", kind="mount")
        script = script_for([item])
        fh.downloader_command.assert_called_once_with(
            "/mnt/localize/wolf-1-us-central1-abc/reference/ref.dict")
        fh.localization_command.assert_not_called()
        assert "K9_DECODE_MARKER" in script
        assert "gcsfuse" in script
        assert script.index("K9_DECODE_MARKER") < script.index("fusermount -u")

    def test_a_non_gs_mount_source_still_uses_its_own_command(self):
        item = s3_item()
        script = script_for([item])
        item.fh.localization_command.assert_called_once()
        assert "aws s3api" in script


DIR = "gs://src-bucket/funcotator"
DIR_DEST = "gs://wolf-1-us-central1-abc/data_source_folder/funcotator"
ENCODED = ("MANIFEST.txt", "gencode/hg38/gencode.v43.pc_transcripts.dict", "odd name+(1).txt")


class TestAGzipEncodedObjectInADirectory:
    """
    funcotator_run's data sources directory has 35 of 46 objects gzip-encoded. Its
    server-side copy leaves those out and each one takes the decode, at its own path
    within the copy -- otherwise every reader of the mount gets the gzip stream.
    """

    @staticmethod
    def _member(name):
        m = TestContentEncoding._gs_fh(transport_gzip=True)
        m.path = DIR + "/" + name
        m.downloader_command = MagicMock(return_value="K9_DECODE " + shlex.quote(name))
        return m

    def _dir_fh(self, encoded=ENCODED):
        fh = TestContentEncoding._gs_fh(transport_gzip=False)
        fh.path = DIR
        fh.is_dir = True
        fh.gzip_members = [self._member(n) for n in encoded]
        fh.relative_name = lambda m: m.path[len(DIR) + 1:]
        return fh

    def _plan(self, fh):
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace"
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, _, plan = loc.create_bucket_mount({"data_source_folder": [fh]}, dry_run=False)
        return plan

    def test_the_encoded_objects_are_excluded_from_the_copy(self):
        plan = self._plan(self._dir_fh())
        assert plan[0].kind == "server_side"
        assert plan[0].exclude == ENCODED

    def test_each_encoded_object_is_planned_onto_the_mount(self):
        plan = self._plan(self._dir_fh())
        assert [(i.kind, i.dest) for i in plan[1:]] == [
            ("mount", plan[0].dest + "/" + name) for name in ENCODED]

    def test_an_unencoded_directory_is_one_plain_copy(self):
        plan = self._plan(self._dir_fh(encoded=()))
        assert [(i.kind, i.exclude) for i in plan] == [("server_side", ())]

    def _script(self, fh=None):
        fh = fh or self._dir_fh()
        items = [UploadItem(fh=fh, dest=DIR_DEST, kind="server_side", exclude=ENCODED)]
        items += [UploadItem(fh=m, dest=DIR_DEST + "/" + n, kind="mount")
                  for m, n in zip(fh.gzip_members, ENCODED)]
        return script_for(items)

    def _rsync(self):
        [line] = [l for l in self._script().split("\n") if "gcloud storage rsync" in l]
        return shlex.split(copy_command(line))

    def test_the_copy_is_an_rsync_into_the_directory_itself(self):
        """rsync copies a directory's contents, so the destination is the directory."""
        argv = self._rsync()
        assert argv[-2:] == [DIR, DIR_DEST]
        assert "-r" in argv and "-n" in argv
        assert '--custom-time=$CANINE_BUCKET_CT' in argv
        assert "gcloud storage cp -r -n --custom-time=\"$CANINE_BUCKET_CT\" " + DIR not in self._script()

    def test_the_exclude_matches_exactly_the_encoded_names(self):
        """gcloud applies it as a Python regex to names relative to the source."""
        [pattern] = [a[len("--exclude="):] for a in self._rsync() if a.startswith("--exclude=")]
        for name in ENCODED:
            assert re.search(pattern, name)
        for name in ("MANIFEST.txt.bak", "gencode/hg38/MANIFEST.txt", "odd name+(1)xtxt",
                     "gnomAD_exome/hg38/gnomAD_exome.tar.gz"):
            assert not re.search(pattern, name), name

    def test_each_encoded_object_decodes_to_its_place_in_the_copy(self):
        fh = self._dir_fh()
        script = self._script(fh)
        mount = "/mnt/localize/wolf-1-us-central1-abc/data_source_folder/funcotator/"
        for member, name in zip(fh.gzip_members, ENCODED):
            member.downloader_command.assert_called_once_with(mount + name)
            assert "K9_DECODE " + shlex.quote(name) in script
        assert script.index("gcloud storage rsync") < script.index("fusermount -u")

    def test_requester_pays_bills_the_copy(self):
        fh = self._dir_fh()
        fh.rp_string = " --billing-project=proj"
        script = script_for([UploadItem(fh=fh, dest=DIR_DEST, kind="server_side", exclude=ENCODED)])
        assert "rsync -r -n --billing-project=proj " in script

    def test_is_valid_bash(self):
        TestGeneratedShellIsValid()._check("set -e\n" + self._script())


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
        `LocalizeToBucket(files=upstream["out"])` pattern.
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
        ordinary LocalizeToBucket(files=upstream["out"]) pattern was rejected
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


class TestTheUploadWaitCeiling:
    """
    `bucket_upload_wait_tries` x 60s is how long a worker waits for a sibling's upload
    before declaring the claim stale and requeueing. It has to clear the longest
    legitimate localization -- below that, healthy uploads are taken over mid-flight and
    a duplicate transfer is charged on every large input, reported only as a warning.

    It is also the recovery time when an uploader genuinely dies, which on preemptible
    workers is routine. So it is bounded on BOTH sides, and a test that only checks the
    floor would wave through a value that stalls every preemption for hours.

    Now derived rather than guessed. 60 was arbitrary; 180 was a placeholder from risk
    asymmetry while this route's throughput was unknown (§13.49). The bucket route
    measures 0.57 h for the largest real input -- twice, agreeing to 0.7% -- so
    0.57 h + 60 s bucket-create ceiling, doubled, is 71 polls.
    """

    # BENCHMARK_RUNBOOK.md §6.6: 2031.4 s and 2045.8 s for 278.91 GiB. A literal, not
    # derived from the default, so the test cannot agree with whatever the default is.
    MEASURED_LOCALIZATION_SECONDS = 2045.8
    BUCKET_CREATE_CEILING_SECONDS = 60
    SAFETY = 2.0

    @staticmethod
    def _default():
        import inspect
        from canine.localization.base import AbstractLocalizer
        return inspect.signature(
            AbstractLocalizer.__init__).parameters["bucket_upload_wait_tries"].default

    def _needed(self):
        claim = self.MEASURED_LOCALIZATION_SECONDS + self.BUCKET_CREATE_CEILING_SECONDS
        return claim * self.SAFETY / 60.0

    def test_it_clears_the_measured_localization_with_the_safety_factor(self):
        assert self._default() >= self._needed(), (
            "{} polls ({:.2f} h) does not cover a measured {:.2f} h localization at "
            "{}x safety -- healthy uploads would be taken over".format(
                self._default(), self._default() / 60.0,
                self.MEASURED_LOCALIZATION_SECONDS / 3600.0, self.SAFETY))

    def test_it_is_not_so_large_that_a_preemption_stalls_for_hours(self):
        """
        The other side, which the 180 placeholder failed. The ceiling is also how long
        every sibling waits after an uploader dies, and on preemptible workers that is
        not an edge case.
        """
        assert self._default() <= 2.5 * self._needed(), (
            "{} polls ({:.2f} h) is {:.1f}x what the measurement needs; that is the "
            "stall after every preemption".format(
                self._default(), self._default() / 60.0,
                self._default() / self._needed()))

    def test_the_emitted_loop_uses_the_configured_value(self):
        """The constant is only worth pinning if it reaches the generated script."""
        assert "-ge 90" in script_for([gs_item()], wait_tries=90)
        assert "-ge 7" in script_for([gs_item()], wait_tries=7)


def emitted_block(script, first, last):
    """The lines of `script` from the first containing `first` to the next containing `last`."""
    lines = script.split("\n")
    start = next(i for i, l in enumerate(lines) if first in l)
    end = next(i for i in range(start, len(lines)) if last in lines[i])
    return "\n".join(lines[start:end + 1])


def run_with_fake_gcloud(block, tmp_path, gcloud_body):
    """
    Run `block` under `set -e`, as localization.sh runs it, with `gcloud` and `sleep`
    replaced by stubs on PATH. The fake gcloud logs each call to calls.log.
    """
    import os
    import subprocess
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "gcloud").write_text("#!/bin/bash\necho \"$*\" >> {}\n{}\n".format(
        tmp_path / "calls.log", gcloud_body))
    (bin_dir / "sleep").write_text("#!/bin/bash\n:\n")
    for stub in ("gcloud", "sleep"):
        os.chmod(str(bin_dir / stub), 0o755)
    env = dict(os.environ, PATH="{}:{}".format(bin_dir, os.environ["PATH"]),
               CANINE_BUCKET_CT="2026-09-25T22:00:00Z")
    proc = subprocess.run(["bash", "-c", "set -e\n" + block + "\necho CANINE_BLOCK_DONE"],
                          capture_output=True, text=True, env=env, timeout=60)
    log = tmp_path / "calls.log"
    calls = log.read_text().splitlines() if log.exists() else []
    return proc, calls


class TestTwoJobsPopulatingOneBucket:
    """
    Production, 2026-09-25: two jobs with the same inputs found their shared bucket
    expired and both repopulated it. The second `cp -n` of dbsnp_138.hg19.vcf.gz failed
    with GcsPreconditionFailedError, because its sibling wrote the object between the
    no-clobber check and the copy, and localization failed. Reproduced against real GCS:
    the losing copy exits 1 with HTTP 412, and a rerun skips the object with exit 0.
    """

    FIRST_COPY_LOSES = (
        'if [ "$1 $2" = "storage cp" ] || [ "$1 $2" = "storage rsync" ]; then\n'
        '  n=$(grep -c "^storage " {log} || true)\n'
        '  if [ "$n" -le 1 ]; then echo "ERROR: HTTPError 412: At least one of the '
        'pre-conditions you specified did not hold." >&2; exit 1; fi\n'
        'fi\nexit 0')

    def _copy_block(self, item):
        return emitted_block(script_for([item]), "for CANINE_COPY_TRY", "done")

    @pytest.mark.parametrize("item", [
        gs_item(),
        gs_item(path="gs://src/dir", dest="gs://b/inp/dir", is_dir=True),
        UploadItem(fh=MagicMock(path="/mnt/nfs/out/ref.fa", localization_mode="local"),
                   dest="gs://b/reference/ref.fa", kind="copy"),
    ], ids=["object", "directory", "shared-mount copy"])
    def test_a_lost_race_is_retried_and_succeeds(self, tmp_path, item):
        body = self.FIRST_COPY_LOSES.replace("{log}", str(tmp_path / "calls.log"))
        proc, calls = run_with_fake_gcloud(self._copy_block(item), tmp_path, body)
        assert "CANINE_BLOCK_DONE" in proc.stdout, proc.stderr
        assert len(calls) == 2, calls
        assert "another job may be populating this bucket" in proc.stderr

    def test_the_directory_rsync_is_retried_too(self, tmp_path):
        item = UploadItem(fh=TestAGzipEncodedObjectInADirectory()._dir_fh(), dest=DIR_DEST,
                          kind="server_side", exclude=ENCODED)
        body = self.FIRST_COPY_LOSES.replace("{log}", str(tmp_path / "calls.log"))
        proc, calls = run_with_fake_gcloud(self._copy_block(item), tmp_path, body)
        assert "CANINE_BLOCK_DONE" in proc.stdout, proc.stderr
        assert [c.split()[1] for c in calls] == ["rsync", "rsync"]

    def test_a_copy_that_keeps_failing_fails_after_three_tries(self, tmp_path):
        proc, calls = run_with_fake_gcloud(self._copy_block(gs_item()), tmp_path, "exit 1")
        assert proc.returncode == 1
        assert "CANINE_BLOCK_DONE" not in proc.stdout
        assert len(calls) == 3
        assert "copy failed 3 times" in proc.stderr

    def test_a_copy_that_succeeds_runs_once(self, tmp_path):
        proc, calls = run_with_fake_gcloud(self._copy_block(gs_item()), tmp_path, "exit 0")
        assert "CANINE_BLOCK_DONE" in proc.stdout, proc.stderr
        assert len(calls) == 1
        assert "WARNING" not in proc.stderr


class TestTheDownloadersOwnCustomTimeIsAccepted:
    """
    Production, 2026-09-25: every object the parallel downloader composed onto the rw
    mount failed the post-unmount customTime patch with GcsApiError(''). The downloader
    stamps customTime at compose time (f09504a), later than $CANINE_BUCKET_CT, and GCS
    refuses to decrease one: "HTTPError 400: Custom time cannot be decreased", confirmed
    against real GCS. The patch only has to ensure each object HAS a customTime.
    """

    def _block(self):
        return emitted_block(script_for([s3_item(), s3_item(dest="gs://wolf-1-us-central1-abc/inp/b.bam")]),
                             "if ! gcloud storage objects update", "    fi")

    @staticmethod
    def _gcloud(update_rc, described):
        return ('if [ "$1 $2 $3" = "storage objects update" ]; then\n'
                '  [ {rc} -eq 0 ] || echo "ERROR: HTTPError 400: Custom time cannot be decreased." >&2\n'
                '  exit {rc}\nfi\n'
                'if [ "$1 $2 $3" = "storage objects describe" ]; then\n'
                '  case "$4" in {cases} esac\nfi\nexit 0').format(
                    rc=update_rc,
                    cases="".join('*{}) echo "{}";; '.format(k, v) for k, v in described.items()))

    def test_a_later_customtime_from_the_downloader_is_accepted(self, tmp_path):
        body = self._gcloud(1, {"reads.bam": "2026-09-25T22:17:20+0000",
                                "b.bam": "2026-09-25T22:17:21+0000"})
        proc, calls = run_with_fake_gcloud(self._block(), tmp_path, body)
        assert "CANINE_BLOCK_DONE" in proc.stdout, proc.stderr
        assert sum("objects describe" in c for c in calls) == 2

    def test_an_object_with_no_customtime_still_fails(self, tmp_path):
        """The invariant the patch exists for: nothing may be left that never expires."""
        body = self._gcloud(1, {"reads.bam": "2026-09-25T22:17:20+0000", "b.bam": ""})
        proc, _ = run_with_fake_gcloud(self._block(), tmp_path, body)
        assert proc.returncode == 1
        assert "b.bam has no customTime" in proc.stderr

    def test_a_successful_update_checks_nothing(self, tmp_path):
        proc, calls = run_with_fake_gcloud(self._block(), tmp_path, self._gcloud(0, {}))
        assert "CANINE_BLOCK_DONE" in proc.stdout, proc.stderr
        assert not any("objects describe" in c for c in calls)
