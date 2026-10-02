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


def input_copies(script):
    """The lines that copy inputs, not the claim write (see bucket_upload_script)."""
    return [l for l in script.splitlines()
            if "storage cp" in l and "CANINE_BUCKET_CLAIM" not in l]


def describes_a_source(script):
    """Whether any line describes an object other than the bucket's own claim."""
    return any("objects describe" in l and "CANINE_BUCKET_CLAIM" not in l
               for l in script.splitlines())


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
        assert "-n" in input_copies(script_for([gs_item()]))[0]

    def test_custom_time_set_on_upload(self):
        """
        Without --custom-time the daysSinceCustomTime lifecycle rule can never
        match the object, so nothing ever expires and the bucket grows forever.
        """
        cp = input_copies(script_for([gs_item()]))[0]
        assert "--custom-time=" in cp

    def test_requester_pays_source_carries_billing_project(self):
        """The source may be requester-pays even though the localization bucket is not."""
        script = script_for([gs_item(rp_string=" --billing-project=proj")])
        assert any("--billing-project=proj" in l and "gs://src-bucket/reads.bam" in l for l in script.splitlines())

    def test_non_requester_pays_has_no_billing_project(self):
        assert "--billing-project" not in script_for([gs_item()])

    def test_directory_source_targets_parent(self):
        """`cp -r gs://a/d gs://B/k/d` would otherwise yield gs://B/k/d/d/..."""
        cp = input_copies(script_for([
            gs_item(path="gs://src/dir", dest="gs://b/inp/dir", is_dir=True)
        ]))[0]
        assert copy_command(cp).endswith("gs://b/inp/")

    def test_file_source_targets_the_object_itself(self):
        cp = input_copies(script_for([gs_item()]))[0]
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
        assert not describes_a_source(script)
        for absent in ("gunzip", "storage cat", "CANINE_DECOMP_TMP"):
            assert absent not in script, absent
        cp = input_copies(script)
        assert len(cp) == 1
        assert "gs://src-bucket/reads.bam" in cp[0] and "-r -n" in cp[0]

    def test_directory_sources_take_the_plain_copy(self):
        """
        No gzip-encoded directory tree has been seen, and classifying one means
        inspecting every object under the prefix.
        """
        script = script_for([gs_item(path="gs://src/dir", dest="gs://b/inp/dir", is_dir=True)])
        assert not describes_a_source(script)
        assert "gunzip" not in script

    def test_a_local_path_has_no_content_encoding_handling(self):
        """
        A "local" upload writes fresh bytes from a file on the controller -- there's
        no pre-existing GCS object metadata for `cp` to inherit, so there's nothing
        here for this feature to guard against.
        """
        fh = MagicMock()
        fh.path = "/home/user/ref.fa"
        fh.localization_mode = "local"
        item = UploadItem(fh=fh, dest="gs://b/reference/ref.fa", kind="local")
        script = "\n".join(make_localizer().bucket_upload_script([item], "gs://b", "us-central1"))
        assert not describes_a_source(script)
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
        copy = input_copies(script)[0]
        assert script.index("wolf=working") < script.index(copy) < script.index("wolf=success")

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
        Regression: a job treating "stale" like "working" would wait again, nobody
        would ever finish the upload, and the shard would ping-pong between nodes
        until its retry budget ran out. A stale bucket has no live claim, so the
        claim is taken; TestTheClaimMakesTakeoverExclusive runs it.
        """
        script = script_for([gs_item()])
        assert 'stale) echo "INFO: taking over stale localization bucket' in script

    def test_a_timed_out_waiter_does_not_mark_a_live_upload_stale(self):
        """It waited on a live claim, so its uploader is alive: exit 5, nothing more."""
        script = script_for([gs_item()])
        timeout = script[script.index("timed out waiting"):]
        timeout = timeout[:timeout.index("exit 5")]
        assert "wolf=stale" not in timeout

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

    def test_a_local_path_is_uploaded_not_silently_dropped(self):
        """
        Regression: a "local" input was given a bucketmount:// URL but left out of
        the upload plan. With an all-local localization the plan came back empty,
        so bucket_upload_script was skipped entirely, the bucket was never created,
        and the consumer tried to gcsfuse-mount a bucket that did not exist.
        """
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/workspace"
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        fh = self._local_fh()
        fh.path = "/home/user/ref.fa"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            _, _, paths, plan = loc.create_bucket_mount({"reference": [fh]}, dry_run=False)

        assert len(plan) == 1, "an all-local localization must still produce a plan"
        assert plan[0].kind == "local"
        assert paths["reference"][0].startswith("bucketmount://")

    def test_an_upstream_output_on_the_share_passes_through(self):
        """
        The ordinary LocalizeToBucket(files=upstream["out"]) pattern. An upstream
        output lives in a sibling task dir on the NFS share, where every worker can
        read it, so it is not copied into the bucket: its path is the result.
        """
        loc = make_localizer()
        loc.staging_dir = "/mnt/nfs/ws/run/BatchLocalDisk__2026-09-06--02-05-09_x"
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        fh = self._local_fh()
        fh.path = "/mnt/nfs/ws/run/produce__2026-09-06--02-03-37_y/outputs/0/big/big.txt"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            prefix, _, paths, plan = loc.create_bucket_mount({"big_file": [fh]}, dry_run=False)
        assert plan == []
        assert prefix is None, "nothing to localize, so no bucket"
        assert paths == {"big_file": [fh.path]}

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

        # passed through, so not uploaded, and its path is what consumers get
        assert plan == []
        assert paths == {"reference": ["/mnt/nfs/workspace/ref.fa"]}

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
    ], ids=["object", "directory"])
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


FAKE_GCS_GCLOUD = r'''#!/usr/bin/env python3
# A stand-in gcloud that keeps one bucket's existence, labels and claim object in files,
# enough to run the whole emitted claim-and-upload script. Every call holds a file lock,
# so concurrent jobs race the way they do against GCS, and the claim's compare-and-swap
# is atomic. FAKE_CP selects how input copies behave: "ok", "fail", or "hang".
# FAKE_SUCCESS_RC makes every wolf=success label update fail.
import fcntl, json, os, sys, time
state = os.environ["FAKE_STATE"]
args = sys.argv[1:]
lock = open(os.path.join(state, "lock"), "a")

def path(name):
    return os.path.join(state, name)

def labels():
    try:
        return json.load(open(path("labels.json")))
    except (OSError, ValueError):
        return {}

def claim():
    try:
        return json.load(open(path("claim.json")))
    except (OSError, ValueError):
        return None

def write(name, value):
    with open(path(name) + ".tmp", "w") as f:
        json.dump(value, f)
    os.replace(path(name) + ".tmp", path(name))

def flag(name):
    for a in args:
        if a.startswith(name + "="):
            return a.split("=", 1)[1]

def is_claim(url):
    return url.endswith("/.wolf_claim")

with open(path("calls.log"), "a") as log:
    log.write(" ".join(args) + "\n")

if args[:2] in (["storage", "cp"], ["storage", "rsync"]) and not is_claim(args[-1]):
    mode = os.environ.get("FAKE_CP", "ok")            # outside the lock: copies run long
    if mode == "hang":
        time.sleep(float(os.environ.get("FAKE_CP_SECONDS", "60")))
    if mode == "fail":
        sys.exit(1)
    open(path("populated"), "w").close()
    sys.exit(0)

fcntl.flock(lock, fcntl.LOCK_EX)
if args[:3] == ["storage", "buckets", "create"]:
    if os.path.exists(path("exists")):
        sys.exit(1)                                   # 409: someone else created it
    open(path("exists"), "w").close()
    sys.exit(0)
if args[:3] == ["storage", "buckets", "describe"]:
    if not os.path.exists(path("exists")):
        sys.exit(1)
    for a in args:
        if a.startswith("--format=value(labels."):
            print(labels().get(a[len("--format=value(labels."):-1], ""))
    sys.exit(0)
if args[:3] == ["storage", "buckets", "update"]:
    d = labels()
    for a in args:
        if a.startswith("--update-labels="):
            pairs = dict(p.split("=", 1) for p in a[len("--update-labels="):].split(","))
            if pairs.get("wolf") == "success" and os.environ.get("FAKE_SUCCESS_RC"):
                sys.exit(int(os.environ["FAKE_SUCCESS_RC"]))
            d.update(pairs)
    write("labels.json", d)
    sys.exit(0)
if args[:3] == ["storage", "objects", "describe"] and is_claim(args[3]):
    c = claim()
    if c is None:
        sys.exit(1)                                   # 404
    fmt = flag("--format")
    if "custom_fields.canine_owner" in fmt:
        print("{}\t{}.0\t{}".format(c["gen"], c["updated"], c.get("owner", "")))
    elif "update_time" in fmt:
        print("{}\t{}.0".format(c["gen"], c["updated"]))
    else:
        print(c["gen"])
    sys.exit(0)
if args[:2] == ["storage", "cp"]:                     # the claim write: compare-and-swap
    c = claim()
    if int(flag("--if-generation-match")) != (c["gen"] if c else 0):
        print("ERROR: HTTPError 412: At least one of the pre-conditions you specified did not hold.",
              file=sys.stderr)
        sys.exit(1)
    meta = flag("--custom-metadata") or ""
    owner = meta.split("=", 1)[1] if meta.startswith("canine_owner=") else ""
    write("claim.json", {"gen": time.time_ns(), "updated": int(time.time()),
                         "body": open(args[-2]).read(), "owner": owner})
    sys.exit(0)
if args[:3] == ["storage", "objects", "update"] and is_claim(args[3]):
    c = claim()                                       # the heartbeat
    if c is None or int(flag("--if-generation-match")) != c["gen"]:
        sys.exit(1)
    c["updated"] = int(time.time())
    write("claim.json", c)
    sys.exit(0)
if args[:2] == ["storage", "rm"] and is_claim(args[-1]):
    c = claim()                                       # the release
    if c is None or int(flag("--if-generation-match")) != c["gen"]:
        sys.exit(1)
    os.remove(path("claim.json"))
    sys.exit(0)
if args[:2] == ["storage", "ls"] and os.path.exists(path("objects.json")):
    # real listing semantics: an exact URL must name an object, and <prefix>/** lists
    # every object under it; exit 1 if any URL matched nothing
    objects = json.load(open(path("objects.json")))
    missing = False
    for url in (a for a in args[2:] if a.startswith("gs://")):
        if url.endswith("/**"):
            matched = [o for o in objects if o.startswith(url[:-2])]
            print("\n".join(matched)) if matched else None
            missing = missing or not matched
        elif url in objects:
            print(url)
        else:
            missing = True
    sys.exit(1 if missing else 0)
if args[:2] == ["storage", "ls"]:
    sys.exit(0 if os.path.exists(path("populated")) else 1)
sys.exit(0)                                           # the inputs' objects update
'''


def install_fake_gcs(tmp_path, labels=None, exists=False, populated=False, claim=None):
    '''
    Put FAKE_GCS_GCLOUD first on a PATH, with its bucket in the given state. `sleep` is
    shortened, not removed, so a heartbeat really runs; gcsfuse and fusermount are
    no-ops. Returns (the PATH to use, the state directory).
    '''
    import json
    import os
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "gcloud").write_text(FAKE_GCS_GCLOUD)
    (bin_dir / "sleep").write_text("#!/bin/bash\n/bin/sleep 0.05\n")
    for noop in ("gcsfuse", "fusermount"):
        (bin_dir / noop).write_text("#!/bin/bash\nexit 0\n")
    # The mount setup is `sudo mkdir -p /mnt/localize/...`, which cannot run here.
    (bin_dir / "sudo").write_text("#!/bin/bash\nexit 0\n")
    # `timeout -k 60 60 gcsfuse ...`; macOS has no GNU timeout.
    (bin_dir / "timeout").write_text(
        '#!/bin/bash\nwhile [ "${1#-}" != "$1" ] || [ -z "${1//[0-9]/}" ]; do shift; done\nexec "$@"\n')
    for stub in ("gcloud", "sleep", "gcsfuse", "fusermount", "sudo", "timeout"):
        os.chmod(str(bin_dir / stub), 0o755)
    state = tmp_path / "state"
    state.mkdir()
    if exists:
        (state / "exists").write_text("")
    if populated:
        (state / "populated").write_text("")
    if labels:
        (state / "labels.json").write_text(json.dumps(labels))
    if claim:
        (state / "claim.json").write_text(json.dumps(claim))
    return "{}:{}".format(bin_dir, os.environ["PATH"]), state


def run_claim_script(tmp_path, items=None, labels=None, exists=False, populated=False,
                     claim=None, wait_tries=60, background=False, jobs=1, **env):
    '''
    Run the whole emitted bucket script, `jobs` copies at once, under `set -e`, against
    FAKE_GCS_GCLOUD.
    '''
    import os
    import subprocess
    path, state = install_fake_gcs(tmp_path, labels=labels, exists=exists,
                                   populated=populated, claim=claim)
    script = script_for(items or [gs_item()], wait_tries=wait_tries)
    environ = dict(os.environ, PATH=path, FAKE_STATE=str(state), **env)
    argv = ["bash", "-c", "set -e\n" + script + "\necho CANINE_SCRIPT_DONE"]
    if background:
        return subprocess.Popen(argv, env=environ, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, start_new_session=True), state
    if jobs > 1:
        procs = [subprocess.Popen(argv, env=environ, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True) for _ in range(jobs)]
        return [(p.wait(timeout=300), p.stdout.read(), p.stderr.read()) for p in procs], state
    return subprocess.run(argv, env=environ, capture_output=True, text=True, timeout=120), state


def fake_labels(state):
    import json
    try:
        return json.loads((state / "labels.json").read_text())
    except OSError:
        return {}


def fake_claim(state):
    import json
    try:
        return json.loads((state / "claim.json").read_text())
    except OSError:
        return None


def fake_calls(state):
    path = state / "calls.log"
    return path.read_text().splitlines() if path.exists() else []


def input_copy_calls(state):
    return [c for c in fake_calls(state)
            if c.startswith(("storage cp", "storage rsync")) and ".wolf_claim" not in c]


def aged(seconds):
    import time
    return int(time.time()) - seconds


class TestAnUnfinishedUploadReleasesItsClaim:
    '''
    Production, 2026-09: a workflow was cancelled and restarted, and the new job waited up
    to bucket_upload_wait_tries minutes (90 by default) on a bucket its cancelled
    predecessor had left labelled wolf=working. An upload that does not finish now
    releases its claim and marks the bucket stale on the way out, so the next job takes
    over at once.
    '''

    def test_a_failed_upload_releases_the_claim(self, tmp_path):
        proc, state = run_claim_script(tmp_path, FAKE_CP="fail")
        assert proc.returncode == 1, proc.stderr
        assert fake_labels(state).get("wolf") == "stale"
        assert fake_claim(state) is None
        assert "releasing it so the next job takes over at once" in proc.stderr

    def test_the_exit_code_is_kept(self, tmp_path):
        '''canine reads 5 as requeue and 1 as a failure; the trap must not rewrite it.'''
        proc, state = run_claim_script(tmp_path, items=[s3_item(command="exit 5")])
        assert proc.returncode == 5, proc.stderr
        assert fake_labels(state).get("wolf") == "stale"
        assert fake_claim(state) is None

    def test_a_cancelled_upload_releases_the_claim(self, tmp_path):
        '''scancel sends SIGTERM to every process in the job, and waits before SIGKILL.'''
        import os
        import signal
        import time
        proc, state = run_claim_script(tmp_path, background=True, FAKE_CP="hang")
        deadline = time.monotonic() + 30
        while not input_copy_calls(state):
            assert time.monotonic() < deadline, "the copy never started"
            time.sleep(0.05)
        os.killpg(proc.pid, signal.SIGTERM)
        _, err = proc.communicate(timeout=30)
        assert proc.returncode == 143, err
        assert fake_labels(state).get("wolf") == "stale"
        assert fake_claim(state) is None

    def test_a_finished_upload_is_success_and_releases_the_claim(self, tmp_path):
        proc, state = run_claim_script(tmp_path)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert fake_labels(state) == {"wolf": "success"}
        assert fake_claim(state) is None
        assert "releasing it" not in proc.stderr

    def test_a_rate_limited_success_label_is_retried(self, tmp_path):
        '''At 100 jobs this update returned HTTP 429 and failed a tenth of them.'''
        proc, state = run_claim_script(tmp_path, FAKE_SUCCESS_RC="1")
        attempts = [c for c in fake_calls(state) if "--update-labels=wolf=success" in c]
        assert len(attempts) == 8
        assert proc.returncode == 1          # still fails if the limit never lifts

    def test_no_heartbeat_outlives_the_upload(self, tmp_path):
        import time
        proc, state = run_claim_script(tmp_path)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        settled = len(fake_calls(state))
        time.sleep(0.5)                              # ten shortened heartbeat intervals
        assert len(fake_calls(state)) == settled

    def test_the_heartbeat_refreshes_the_claim_while_uploading(self, tmp_path):
        proc, state = run_claim_script(tmp_path, FAKE_CP="hang", FAKE_CP_SECONDS="1")
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        beats = [c for c in fake_calls(state)
                 if c.startswith("storage objects update") and ".wolf_claim" in c]
        assert len(beats) >= 3, fake_calls(state)
        assert all("--if-generation-match=" in b for b in beats)


class TestTheClaimMakesTakeoverExclusive:
    '''
    Only bucket creation used to be a mutex. At 100 jobs sharing a bucket, an expired,
    stale or dead-heartbeat bucket was taken over by 40-50 of them at once, 33-39 made
    their own copy of a 1.48 GB input, and up to 14 failed on label rate limits. Taking
    an upload over now means winning a compare-and-swap on the claim object.
    '''

    def test_a_new_bucket_is_claimed_by_its_creator(self, tmp_path):
        proc, state = run_claim_script(tmp_path)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert any(c.startswith("storage cp --if-generation-match=0") and ".wolf_claim" in c
                   for c in fake_calls(state))

    def test_a_populated_bucket_needs_no_claim(self, tmp_path):
        proc, state = run_claim_script(tmp_path, exists=True, populated=True,
                                       labels={"wolf": "success"})
        assert "already populated" in proc.stderr
        assert not any(".wolf_claim" in c for c in fake_calls(state))
        assert input_copy_calls(state) == []

    @pytest.mark.parametrize("scenario", ["expired", "stale", "dead", "unclaimed"])
    def test_one_of_twenty_racing_jobs_takes_it_over(self, tmp_path, scenario):
        from canine.localization.base import BUCKET_HEARTBEAT_STALE
        setup = {
            "expired": dict(labels={"wolf": "success"}),
            "stale": dict(labels={"wolf": "stale"}),
            "dead": dict(labels={"wolf": "working"},
                         claim={"gen": 7, "updated": aged(BUCKET_HEARTBEAT_STALE + 60)}),
            "unclaimed": dict(labels={"wolf": "working"}),
        }[scenario]
        results, state = run_claim_script(tmp_path, exists=True, jobs=20,
                                          FAKE_CP="hang", FAKE_CP_SECONDS="0.5", **setup)
        assert [rc for rc, _, _ in results] == [0] * 20, [e for rc, _, e in results if rc]
        uploaders = [e for _, _, e in results
                     if "repopulating" in e or "taking over stale" in e or "taking it over" in e]
        assert len(uploaders) == 1, uploaders
        assert len(input_copy_calls(state)) == 1
        assert fake_labels(state) == {"wolf": "success"}

    def test_a_live_claim_is_waited_on(self, tmp_path):
        proc, state = run_claim_script(tmp_path, exists=True, wait_tries=0,
                                       labels={"wolf": "working"},
                                       claim={"gen": 7, "updated": aged(5)})
        assert proc.returncode == 5                   # the existing wait-then-requeue
        assert input_copy_calls(state) == []
        assert fake_claim(state)["gen"] == 7           # untouched
        assert fake_labels(state).get("wolf") == "working"

    def test_a_live_claim_is_waited_on_even_when_the_label_says_stale(self, tmp_path):
        '''The claim decides who uploads; the labels are informational.'''
        proc, state = run_claim_script(tmp_path, exists=True, wait_tries=0,
                                       labels={"wolf": "stale"},
                                       claim={"gen": 7, "updated": aged(5)})
        assert proc.returncode == 5
        assert input_copy_calls(state) == []


class TestSettlingALocalizationOnTheController:
    '''
    Every LocalizeToBucket used to take an exclusive n1-standard-8, even to find its
    bucket already populated or to copy gs:// objects server-side. With 50-100
    workflows sharing reference files, 99 nodes booted to do nothing.
    resolve_on_controller settles a job on the controller when no node is needed,
    running the same claim-and-upload a node would.
    '''

    PREFIX = "gs://wolf-1-us-central1-abc"

    def _localizer(self, monkeypatch, tmp_path, items, localizer_kwargs=None, **fake):
        path, state = install_fake_gcs(tmp_path, **fake)
        monkeypatch.setenv("PATH", path)
        monkeypatch.setenv("FAKE_STATE", str(state))
        monkeypatch.delenv("CANINE_DISABLE_CONTROLLER_LOCALIZATION", raising=False)
        loc = make_localizer(**(localizer_kwargs or {}))
        loc.bucket_plans["0"] = (self.PREFIX, "us-central1", items)
        return loc, state

    @staticmethod
    def _delocalized(loc):
        import os
        out = os.path.join(loc.environment("local")["CANINE_OUTPUT"], "0")
        return {name: os.path.isfile(os.path.join(out, name)) for name in ("stdout", "stderr")}

    def test_a_populated_bucket_is_settled_without_copying(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()], exists=True,
                                     populated=True, labels={"wolf": "success"})
        assert loc.resolve_on_controller("0") is True
        assert input_copy_calls(state) == []
        assert self._delocalized(loc) == {"stdout": True, "stderr": True}

    def test_its_expiry_clock_is_refreshed(self, monkeypatch, tmp_path):
        '''The node job this replaces renewed customTime; without it, content in use ages out.'''
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()], exists=True,
                                     populated=True, labels={"wolf": "success"})
        loc.resolve_on_controller("0")
        assert any(c.startswith("storage objects update") and "--custom-time" in c
                   and ".wolf_claim" not in c for c in fake_calls(state))

    def test_a_server_side_only_bucket_is_claimed_and_copied_here(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()], exists=True,
                                     labels={"wolf": "success"})           # expired
        assert loc.resolve_on_controller("0") is True
        assert len(input_copy_calls(state)) == 1
        assert fake_labels(state) == {"wolf": "success"}
        assert fake_claim(state) is None
        assert self._delocalized(loc) == {"stdout": True, "stderr": True}

    def test_a_new_bucket_is_created_and_filled_here(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()])
        assert loc.resolve_on_controller("0") is True
        assert any(c.startswith("storage buckets create") for c in fake_calls(state))
        assert len(input_copy_calls(state)) == 1

    def _local_item(self):
        fh = MagicMock(path="/home/user/ref.fa", localization_mode="local")
        return UploadItem(fh=fh, dest=self.PREFIX + "/reference/ref.fa", kind="local")

    def test_a_local_path_settles_without_a_node(self, monkeypatch, tmp_path):
        '''
        The controller claims and labels the bucket like any uploader, uploading the
        local path under the claim (-n: a no-op once localization has uploaded it).
        '''
        loc, state = self._localizer(monkeypatch, tmp_path, [self._local_item()],
                                     exists=True, populated=True)
        assert loc.resolve_on_controller("0") is True
        copies = input_copy_calls(state)
        assert len(copies) == 1 and "/home/user/ref.fa" in copies[0] and "-n" in copies[0].split()
        assert fake_labels(state) == {"wolf": "success"}

    def test_a_missing_local_path_is_uploaded_again_not_sent_to_a_node(self, monkeypatch, tmp_path):
        '''A node cannot read it, so it could only fail there.'''
        loc, state = self._localizer(monkeypatch, tmp_path, [self._local_item()], exists=True)
        assert loc.resolve_on_controller("0") is True
        assert len(input_copy_calls(state)) == 1
        assert fake_labels(state) == {"wolf": "success"}

    def test_another_jobs_live_claim_is_waited_on_here(self, monkeypatch, tmp_path):
        '''Waiting holds a controller thread instead of an exclusive node.'''
        import json
        import threading
        import time
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()], exists=True,
                                     labels={"wolf": "working"},
                                     claim={"gen": 7, "updated": aged(5)})

        def owner_finishes():
            time.sleep(0.5)
            (state / "populated").write_text("")
            (state / "labels.json").write_text(json.dumps({"wolf": "success"}))
            (state / "claim.json").unlink()
        threading.Thread(target=owner_finishes).start()
        assert loc.resolve_on_controller("0") is True
        assert input_copy_calls(state) == []

    def test_download_inputs_are_settled_only_if_already_populated(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [s3_item(), gs_item()],
                                     exists=True, populated=True, labels={"wolf": "success"})
        assert loc.resolve_on_controller("0") is True

    def test_download_inputs_on_an_unpopulated_bucket_go_to_a_node(self, monkeypatch, tmp_path):
        '''Read-only: the node job does the claiming.'''
        loc, state = self._localizer(monkeypatch, tmp_path, [s3_item()], exists=True,
                                     labels={"wolf": "success"})           # expired
        assert loc.resolve_on_controller("0") is False
        assert not any(".wolf_claim" in c for c in fake_calls(state))
        assert input_copy_calls(state) == []
        assert self._delocalized(loc) == {"stdout": False, "stderr": False}

    def test_a_failed_controller_run_goes_to_a_node(self, monkeypatch, tmp_path):
        import os
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()])
        monkeypatch.setenv("FAKE_CP", "fail")
        assert loc.resolve_on_controller("0") is False
        log = os.path.join(loc.environment("local")["CANINE_JOBS"], "0", "controller_localization.log")
        assert "copy failed 3 times" in open(log).read()
        assert self._delocalized(loc) == {"stdout": False, "stderr": False}
        assert fake_claim(state) is None                   # the trap released it

    def test_the_environment_variable_turns_it_off(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()], exists=True,
                                     populated=True, labels={"wolf": "success"})
        monkeypatch.setenv("CANINE_DISABLE_CONTROLLER_LOCALIZATION", "1")
        assert loc.resolve_on_controller("0") is False
        assert fake_calls(state) == []

    def test_the_option_turns_it_off(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()], exists=True,
                                     populated=True, labels={"wolf": "success"},
                                     localizer_kwargs={"resolve_on_controller": False})
        assert loc.resolve_on_controller("0") is False
        assert fake_calls(state) == []

    def test_a_job_with_no_bucket_upload_goes_to_a_node(self, monkeypatch, tmp_path):
        loc, state = self._localizer(monkeypatch, tmp_path, [gs_item()])
        assert loc.resolve_on_controller("1") is False

    def test_job_setup_teardown_keeps_the_plan(self):
        '''resolve_on_controller settles from what localization already planned.'''
        loc = make_localizer(localize_to_persistent_disk=True)
        loc.inputs = {"0": {}}
        loc.clean_on_exit = False
        item = gs_item()
        with patch.object(loc, "create_bucket_mount",
                          return_value=(self.PREFIX, False, {"inp": ["bucketmount://x"]}, [item])), \
             patch.object(loc, "backend_zone", return_value="us-central1-c"):
            try:
                loc.job_setup_teardown("0", {"stdout": "../stdout", "stderr": "../stderr"},
                                       MagicMock())
            except Exception:
                pass                                      # only the bucket branch matters here
        assert loc.bucket_plans.get("0") == (self.PREFIX, "us-central1", [item])


class TestTheOrchestratorDropsSettledJobs:

    def test_settled_jobs_are_dropped_like_avoided_ones(self):
        import types
        from canine.orchestrator import Orchestrator
        orch = types.SimpleNamespace(job_spec={"0": {"a": 1}, "1": {"a": 2}, "2": None})
        loc = MagicMock()
        loc.resolve_on_controller.side_effect = lambda job: job == "0"
        assert Orchestrator.resolve_localizations_on_controller(orch, loc) == 1
        assert orch.job_spec == {"0": None, "1": {"a": 2}, "2": None}
        # an already-avoided job is not asked
        assert [c.args[0] for c in loc.resolve_on_controller.call_args_list] == ["0", "1"]


class TestTheLogSaysWhatASettledJobDid:
    '''
    Most localization jobs settled on the controller transfer nothing: another
    workflow's job, earlier or concurrent, already filled the shared bucket. The log
    must not call that "localized on the controller".
    '''

    def _settle(self, monkeypatch, tmp_path, items, setup=None, **fake):
        logged = []
        monkeypatch.setattr("canine.localization.base.canine_logging.info1", logged.append)
        loc, state = TestSettlingALocalizationOnTheController()._localizer(
            monkeypatch, tmp_path, items, **fake)
        if setup:
            setup(state)
        assert loc.resolve_on_controller("0") is True
        return [m for m in logged if m.startswith("localization job 0:")][-1]

    def test_a_bucket_found_complete(self, monkeypatch, tmp_path):
        msg = self._settle(monkeypatch, tmp_path, [gs_item()], exists=True, populated=True,
                           labels={"wolf": "success"})
        assert "already localized in gs://wolf-1-us-central1-abc, found complete" in msg
        assert "transferred" not in msg

    def test_a_download_bucket_found_complete(self, monkeypatch, tmp_path):
        msg = self._settle(monkeypatch, tmp_path, [s3_item()], exists=True, populated=True,
                           labels={"wolf": "success"})
        assert "found complete" in msg

    def test_a_bucket_completed_by_another_job_while_waiting(self, monkeypatch, tmp_path):
        import json
        import threading
        import time

        def owner_finishes(state):
            def finish():
                time.sleep(0.5)
                (state / "populated").write_text("")
                (state / "labels.json").write_text(json.dumps({"wolf": "success"}))
                (state / "claim.json").unlink()
            threading.Thread(target=finish).start()
        msg = self._settle(monkeypatch, tmp_path, [gs_item()], setup=owner_finishes, exists=True,
                           labels={"wolf": "working"}, claim={"gen": 7, "updated": aged(5)})
        assert "localized in gs://wolf-1-us-central1-abc by another job, waited for its upload" in msg

    def test_a_transfer_this_job_made(self, monkeypatch, tmp_path):
        msg = self._settle(monkeypatch, tmp_path, [gs_item()], exists=True,
                           labels={"wolf": "success"})              # expired
        assert "transferred on the controller into gs://wolf-1-us-central1-abc" in msg
        assert "already localized" not in msg

    def test_a_bucket_this_job_created_and_filled(self, monkeypatch, tmp_path):
        msg = self._settle(monkeypatch, tmp_path, [gs_item()])
        assert "transferred on the controller" in msg



class TestARequeuedJobTakesBackItsOwnClaim:
    '''
    An uploader whose node was preempted before its trap could run leaves a claim whose
    heartbeat looks live for up to BUCKET_HEARTBEAT_STALE. SLURM requeues the job with
    the same SLURM_JOB_ID, and used to wait out that time on its own dead predecessor.
    The claim now records its owner (the submitting controller's host and the job ID),
    and a requeued attempt that finds its own claim takes it back at once.
    '''

    JOB = {"SLURM_JOB_ID": "4242", "SLURM_SUBMIT_HOST": "wolf-ctl"}

    def test_its_own_live_claim_is_taken_back_at_once(self, tmp_path):
        proc, state = run_claim_script(tmp_path, exists=True, wait_tries=0,
                                       labels={"wolf": "working"},
                                       claim={"gen": 7, "updated": aged(5), "owner": "wolf-ctl:4242"},
                                       **self.JOB)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert "is this job's own" in proc.stderr
        assert len(input_copy_calls(state)) == 1
        assert fake_labels(state) == {"wolf": "success"}

    @pytest.mark.parametrize("owner", ["wolf-ctl:9999", "other-ctl:4242", ""])
    def test_anyone_elses_live_claim_is_waited_on(self, tmp_path, owner):
        '''Another job, the same job ID on another cluster, or an owner not recorded.'''
        proc, state = run_claim_script(tmp_path, exists=True, wait_tries=0,
                                       labels={"wolf": "working"},
                                       claim={"gen": 7, "updated": aged(5), "owner": owner},
                                       **self.JOB)
        assert proc.returncode == 5
        assert input_copy_calls(state) == []

    def test_the_claim_records_its_owner(self, tmp_path):
        proc, state = run_claim_script(tmp_path, **self.JOB)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert any(".wolf_claim" in c and "--custom-metadata=canine_owner=wolf-ctl:4242" in c
                   for c in fake_calls(state))

    def test_a_controller_side_owner_never_matches_a_job(self, tmp_path):
        '''A controller run is never requeued, so it must never take back a live claim.'''
        proc, state = run_claim_script(tmp_path, exists=True, wait_tries=0,
                                       labels={"wolf": "working"},
                                       claim={"gen": 7, "updated": aged(5), "owner": ""})
        assert proc.returncode == 5
        claims = [c for c in fake_calls(state) if c.startswith("storage cp") and ".wolf_claim" in c]
        assert claims == []


class TestLocalPathsAndTheShare:
    '''
    A local path under the NFS share (/mnt/nfs) is the same file on every worker, so it
    passes through as its path and is never copied into a bucket. Any other local path
    is on the controller alone: no worker can read it, so the controller uploads it when
    the job is localized, and the node only checks that it arrived.
    '''
    PREFIX = "gs://wolf-1-us-central1-abc"

    @staticmethod
    def _fh(path, mode="local", hash="cafebabe"):
        fh = MagicMock()
        fh.path = path
        fh.hash = hash
        fh.size = 99
        fh.localization_mode = mode
        return fh

    @staticmethod
    def _plan(inputs):
        loc = make_localizer()
        loc.backend.config = {"zone": "us-central1-c"}
        loc.project = "proj"
        with patch("canine.localization.base.get_project_number", return_value="406002258908"):
            return loc.create_bucket_mount(inputs, dry_run=False)

    @pytest.fixture
    def share(self, monkeypatch, tmp_path):
        '''A real directory standing in for /mnt/nfs, so symlinks can resolve into it.'''
        root = (tmp_path / "nfs").resolve()
        root.mkdir()
        monkeypatch.setattr("canine.localization.base.SHARED_MOUNT", str(root))
        return root

    def test_the_share_is_decided_by_prefix(self):
        from canine.localization.base import on_shared_mount
        assert on_shared_mount("/mnt/nfs/ws/ref.fa")
        assert not on_shared_mount("/mnt/nfsother/ref.fa")
        assert not on_shared_mount("/mnt/nfs")
        assert not on_shared_mount("/home/user/ref.fa")

    def test_a_string_literal_on_the_share_passes_through(self):
        '''NFSLocalizer turns share paths that are not canine outputs into these.'''
        _, _, paths, plan = self._plan({"ref": [self._fh("/mnt/nfs/ref/hg38.fa", mode="string")]})
        assert plan == []
        assert paths == {"ref": ["/mnt/nfs/ref/hg38.fa"]}

    def test_a_symlink_into_the_share_passes_the_share_path(self, share, tmp_path):
        '''The link itself is on the controller, so it would dangle on a worker.'''
        (share / "hg38.fa").write_text(">chr1\n")
        link = tmp_path / "home_link.fa"
        link.symlink_to(share / "hg38.fa")
        _, _, paths, plan = self._plan({"ref": [self._fh(str(link))]})
        assert plan == []
        assert paths == {"ref": [str(share / "hg38.fa")]}

    def test_a_symlink_out_of_the_share_is_uploaded(self, share, tmp_path):
        '''It names a place on the share, but its bytes are on the controller alone.'''
        (tmp_path / "hg38.fa").write_text(">chr1\n")
        (share / "link.fa").symlink_to(tmp_path / "hg38.fa")
        _, _, _, plan = self._plan({"ref": [self._fh(str(share / "link.fa"))]})
        assert [item.kind for item in plan] == ["local"]

    def test_an_array_mixing_both_keeps_every_element_in_order(self):
        _, _, paths, plan = self._plan({"refs": [
            self._fh("/mnt/nfs/ref/a.fa"), self._fh("/home/user/b.fa"), self._fh("/mnt/nfs/ref/c.fa"),
        ]})
        assert [item.kind for item in plan] == ["local"]
        assert paths["refs"][0] == "/mnt/nfs/ref/a.fa"
        assert paths["refs"][1].startswith("bucketmount://") and paths["refs"][1].endswith("/refs/b.fa")
        assert paths["refs"][2] == "/mnt/nfs/ref/c.fa"

    def test_a_share_path_does_not_change_the_bucket(self):
        '''It is not in the bucket, so it is not in the content hash either.'''
        alone, _, _, _ = self._plan({"b": [self._fh("/home/user/b.fa")]})
        mixed, _, _, _ = self._plan({"b": [self._fh("/home/user/b.fa")],
                                     "a": [self._fh("/mnt/nfs/a.fa", hash="0ddba11")]})
        assert alone == mixed

    def test_the_controller_run_uploads_a_local_path_itself(self):
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        script = "\n".join(make_localizer().bucket_upload_script(
            [item], self.PREFIX, "us-central1", upload_local=True))
        assert len(input_copies(script)) == 1
        assert "is a local path on the controller" not in script

    def test_the_node_only_checks_that_a_local_path_arrived(self):
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        script = script_for([item])
        assert input_copies(script) == []
        assert "/home/user/ref.fa" not in script
        assert "gcloud storage ls " + self.PREFIX + "/ref/ref.fa" in script

    def _local_upload(self, tmp_path, items, **fake):
        import os
        import subprocess
        path, state = install_fake_gcs(tmp_path, **fake)
        lines = make_localizer().local_upload_script(items, self.PREFIX, "us-central1")
        proc = subprocess.run(["bash", "-c", "set -e\n" + "\n".join(lines) + "\necho CANINE_SCRIPT_DONE"],
                              env=dict(os.environ, PATH=path, FAKE_STATE=str(state)),
                              capture_output=True, text=True, timeout=60)
        return proc, state

    def test_the_controller_creates_the_bucket_and_uploads(self, tmp_path):
        items = [UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local"),
                 gs_item(dest=self.PREFIX + "/inp/reads.bam")]
        proc, state = self._local_upload(tmp_path, items)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        calls = fake_calls(state)
        create = [c for c in calls if c.startswith("storage buckets create")]
        assert len(create) == 1 and "--lifecycle-file=" in create[0] and "--soft-delete-duration=0" in create[0]
        copies = input_copy_calls(state)
        assert len(copies) == 1, "only the local path; the gs:// input is the node's"
        assert "/home/user/ref.fa" in copies[0] and "-n" in copies[0].split()
        assert "--custom-time=" in copies[0]
        assert fake_labels(state) == {}, "labels and the claim stay the node's"
        assert fake_claim(state) is None

    def test_an_existing_bucket_is_not_created_again(self, tmp_path):
        items = [UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")]
        proc, state = self._local_upload(tmp_path, items, exists=True)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert not any(c.startswith("storage buckets create") for c in fake_calls(state))

    def test_then_the_node_script_finds_it_and_labels_the_bucket(self, tmp_path):
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        proc, state = run_claim_script(tmp_path, items=[item], exists=True, populated=True)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert input_copy_calls(state) == []
        assert fake_labels(state) == {"wolf": "success"}
        assert fake_claim(state) is None

    def test_a_node_missing_it_fails_and_releases_the_bucket(self, tmp_path):
        '''Exit 1, not a requeue: only a rerun of the task localizes, and so uploads, again.'''
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        proc, state = run_claim_script(tmp_path, items=[item], exists=True)
        assert proc.returncode == 1
        assert "is a local path on the controller" in proc.stderr
        assert fake_labels(state).get("wolf") == "stale"
        assert fake_claim(state) is None

    def test_a_failed_upload_fails_localization(self, monkeypatch, tmp_path):
        path, state = install_fake_gcs(tmp_path, exists=True)
        monkeypatch.setenv("PATH", path)
        monkeypatch.setenv("FAKE_STATE", str(state))
        monkeypatch.setenv("FAKE_CP", "fail")
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        with pytest.raises(RuntimeError, match="/home/user/ref.fa"):
            make_localizer().upload_local_paths([item], self.PREFIX, "us-central1")

    def test_a_plan_with_no_local_path_runs_nothing(self, monkeypatch):
        run = MagicMock()
        monkeypatch.setattr("canine.localization.base.subprocess.run", run)
        make_localizer().upload_local_paths([gs_item()], self.PREFIX, "us-central1")
        run.assert_not_called()

    def test_job_setup_teardown_uploads_before_emitting_the_node_script(self):
        loc = make_localizer(localize_to_persistent_disk=True)
        loc.inputs = {"0": {}}
        loc.clean_on_exit = False
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        order = []
        with patch.object(loc, "create_bucket_mount",
                          return_value=(self.PREFIX, False, {"ref": ["bucketmount://x"]}, [item])), \
             patch.object(loc, "backend_zone", return_value="us-central1-c"), \
             patch.object(loc, "upload_local_paths", side_effect=lambda *a: order.append("upload")), \
             patch.object(loc, "bucket_upload_script", side_effect=lambda *a: order.append("script") or []):
            try:
                loc.job_setup_teardown("0", {"stdout": "../stdout", "stderr": "../stderr"}, MagicMock())
            except Exception:
                pass                                      # only the bucket branch matters here
        assert order == ["upload", "script"]

    def test_a_share_path_reaches_the_task_as_a_string(self):
        from canine.localization.file_handlers import HandleBucketMountURL, StringLiteral
        loc = make_localizer(localize_to_persistent_disk=True)
        loc.inputs = {"0": {"ref": [self._fh("/mnt/nfs/ref/a.fa"), self._fh("/home/user/b.fa")]}}
        loc.clean_on_exit = False
        with patch.object(loc, "create_bucket_mount", return_value=(
                 self.PREFIX, False, {"ref": ["/mnt/nfs/ref/a.fa", "bucketmount://wolf-1-us-central1-abc/ref/b.fa"]},
                 [UploadItem(fh=self._fh("/home/user/b.fa"), dest=self.PREFIX + "/ref/b.fa", kind="local")])), \
             patch.object(loc, "backend_zone", return_value="us-central1-c"), \
             patch.object(loc, "upload_local_paths"):
            try:
                loc.job_setup_teardown("0", {"stdout": "../stdout", "stderr": "../stderr"}, MagicMock())
            except Exception:
                pass
        a, b = loc.inputs["0"]["ref"]
        assert isinstance(a, StringLiteral) and a.path == "/mnt/nfs/ref/a.fa"
        assert isinstance(b, HandleBucketMountURL)

    def test_the_log_says_local_paths_were_uploaded(self, monkeypatch, tmp_path):
        item = UploadItem(fh=self._fh("/home/user/ref.fa"), dest=self.PREFIX + "/ref/ref.fa", kind="local")
        msg = TestTheLogSaysWhatASettledJobDid()._settle(monkeypatch, tmp_path, [item],
                                                         exists=True, populated=True)
        assert "(local paths uploaded from the controller)" in msg
        assert "server-side" not in msg


class TestNFSLocalizerAndTheShare:
    '''NFSLocalizer symlinks a file workers can follow, and copies one they cannot.'''

    @staticmethod
    def _localize(src):
        from canine.localization.base import PathType
        from canine.localization.nfs import NFSLocalizer
        loc = NFSLocalizer.__new__(NFSLocalizer)
        fh = MagicMock(path=str(src), localization_mode="local")
        dest = src.parent.parent / "inputs" / src.name
        loc.localize_file(fh, PathType(str(dest), str(dest)))
        return dest

    def test_a_file_on_the_share_is_symlinked(self, monkeypatch, tmp_path):
        root = (tmp_path / "nfs").resolve()
        (root / "ws").mkdir(parents=True)
        (root / "ws" / "ref.fa").write_text(">chr1\n")
        monkeypatch.setattr("canine.localization.base.SHARED_MOUNT", str(root))
        assert self._localize(root / "ws" / "ref.fa").is_symlink()

    def test_a_local_path_is_copied(self, monkeypatch, tmp_path):
        '''Even on the same device as the share, which is the usual controller layout.'''
        (tmp_path / "home").mkdir()
        (tmp_path / "home" / "ref.fa").write_text(">chr1\n")
        monkeypatch.setattr("canine.localization.base.SHARED_MOUNT", str((tmp_path / "nfs").resolve()))
        dest = self._localize(tmp_path / "home" / "ref.fa")
        assert not dest.is_symlink() and dest.read_text() == ">chr1\n"


class TestLocalizationBucketsArePrivate:
    '''Localized inputs include protected data such as BAMs.'''
    PRIVATE = ("--public-access-prevention", "--uniform-bucket-level-access")

    def test_a_node_creates_a_private_bucket(self):
        create = [l for l in script_for([gs_item()]).splitlines() if "buckets create" in l]
        assert len(create) == 1
        assert all(flag in create[0].split() for flag in self.PRIVATE)

    def test_the_controller_creates_a_private_bucket(self):
        fh = MagicMock(path="/home/user/ref.fa", localization_mode="local")
        item = UploadItem(fh=fh, dest="gs://wolf-1-us-central1-abc/ref/ref.fa", kind="local")
        lines = make_localizer().local_upload_script([item], "gs://wolf-1-us-central1-abc", "us-central1")
        create = [l for l in lines if "buckets create" in l]
        assert len(create) == 1
        assert all(flag in create[0].split() for flag in self.PRIVATE)

    def test_an_existing_bucket_gets_public_access_prevention_when_uploaded_into(self, tmp_path):
        proc, state = run_claim_script(tmp_path, exists=True)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        updates = [c for c in fake_calls(state) if "wolf=working" in c]
        assert len(updates) == 1 and "--public-access-prevention" in updates[0].split()

    def test_but_not_uniform_access_which_would_drop_its_acls(self):
        '''Enabling it on an existing bucket cut project editors off from its objects.'''
        script = script_for([gs_item()])
        assert not any("buckets update" in l and "uniform-bucket-level-access" in l
                       for l in script.splitlines())
        assert "canine_bucket_label --update-labels=wolf=working --public-access-prevention" in script


class TestEveryObjectMustBeThere:
    '''
    A bucket labeled success counts as populated only if every object of its plan is
    there: a file by name, a directory file by file. Checked against real GCS on a
    scratch bucket (2026-10-02): `ls` of several URLs fails if any one matches nothing,
    but `ls` of a directory's prefix passes while anything at all is left under it,
    and `objects update` of a bare prefix matches no object.
    '''
    B = "gs://wolf-1-us-central1-abc"

    @staticmethod
    def _dir_fh(names):
        fh = MagicMock(path="gs://src/funcotator", rp_string="", is_dir=True, localization_mode="url")
        fh.member_names = names
        return fh

    def _dir_item(self, names=("a.txt", "gencode/x.dict", "odd name+(1).txt")):
        return UploadItem(fh=self._dir_fh(list(names)), dest=self.B + "/data/funcotator", kind="server_side")

    def _complete(self, tmp_path, items, objects):
        import json
        import os
        import subprocess
        from canine.localization.base import bucket_complete_function
        path, state = install_fake_gcs(tmp_path)
        (state / "objects.json").write_text(json.dumps(sorted(objects)))
        script = "\n".join(bucket_complete_function(items)) + "\ncanine_bucket_complete"
        return subprocess.run(["bash", "-c", script], env=dict(os.environ, PATH=path, FAKE_STATE=str(state)),
                              capture_output=True, text=True, timeout=60).returncode

    def _under(self, item, names):
        return [item.dest + "/" + n for n in names]

    def test_every_file_present(self, tmp_path):
        items = [gs_item(dest=self.B + "/a/1.bam"), gs_item(dest=self.B + "/b/2.bai")]
        assert self._complete(tmp_path, items, [self.B + "/a/1.bam", self.B + "/b/2.bai"]) == 0

    def test_one_file_missing(self, tmp_path):
        items = [gs_item(dest=self.B + "/a/1.bam"), gs_item(dest=self.B + "/b/2.bai"), gs_item(dest=self.B + "/c/3.fa")]
        assert self._complete(tmp_path, items, [self.B + "/a/1.bam", self.B + "/c/3.fa"]) == 1

    def test_a_whole_directory(self, tmp_path):
        item = self._dir_item()
        assert self._complete(tmp_path, [item], self._under(item, ["a.txt", "gencode/x.dict", "odd name+(1).txt"])) == 0

    def test_a_directory_missing_one_file_is_incomplete(self, tmp_path):
        '''The bug: `ls` of the prefix passed, so this read as populated.'''
        item = self._dir_item()
        assert self._complete(tmp_path, [item], self._under(item, ["a.txt", "odd name+(1).txt"])) == 1

    def test_extra_objects_under_a_directory_are_harmless(self, tmp_path):
        item = self._dir_item(("a.txt",))
        assert self._complete(tmp_path, [item], self._under(item, ["a.txt", "b.txt"])) == 0

    def test_an_empty_directory_prefix_is_incomplete(self, tmp_path):
        assert self._complete(tmp_path, [self._dir_item()], [self.B + "/other/x"]) == 1

    def test_a_sibling_with_a_longer_name_does_not_count(self, tmp_path):
        '''gs://B/data/funcotator2/a.txt is not under gs://B/data/funcotator/.'''
        item = self._dir_item(("a.txt",))
        assert self._complete(tmp_path, [item], [self.B + "/data/funcotator2/a.txt"]) == 1

    def test_a_directory_with_unknown_contents_falls_back_to_the_prefix(self, tmp_path):
        item = UploadItem(fh=MagicMock(path="gs://src/d", is_dir=True), dest=self.B + "/d/d", kind="server_side")
        (tmp_path / "x").mkdir()
        (tmp_path / "y").mkdir()
        assert self._complete(tmp_path / "x", [item], [self.B + "/d/d/anything"]) == 0
        assert self._complete(tmp_path / "y", [item], []) == 1

    def test_a_local_directory_is_checked_file_by_file(self, tmp_path):
        root = tmp_path / "ref"
        (root / "sub").mkdir(parents=True)
        (root / "a.fa").write_text("x")
        (root / "sub" / "b.fai").write_text("x")
        item = UploadItem(fh=MagicMock(path=str(root)), dest=self.B + "/ref/ref", kind="local")
        (tmp_path / "x").mkdir()
        (tmp_path / "y").mkdir()
        assert self._complete(tmp_path / "x", [item], self._under(item, ["a.fa", "sub/b.fai"])) == 0
        assert self._complete(tmp_path / "y", [item], self._under(item, ["a.fa"])) == 1

    def test_the_refresh_reaches_every_object_of_a_directory(self):
        from canine.localization.base import bucket_object_patterns
        pats = shlex.split(bucket_object_patterns([self._dir_item(), gs_item(dest=self.B + "/a/1.bam")]))
        assert pats == [self.B + "/data/funcotator/**", self.B + "/a/1.bam"]

    def test_every_refresh_uses_the_patterns(self):
        loc = make_localizer()
        item = self._dir_item()
        for script in ("\n".join(loc.bucket_upload_script([item], self.B, "us-central1")),
                       "\n".join(loc.bucket_populated_script([item], self.B))):
            refreshes = [l for l in script.splitlines() if "objects update" in l and ".wolf_claim" not in l
                         and "CANINE_BUCKET_CLAIM" not in l]
            assert refreshes and all("/data/funcotator/**" in l for l in refreshes)

    def test_a_partly_expired_directory_is_repopulated(self, tmp_path):
        import json
        item = self._dir_item(("a.txt", "b.txt"))
        path, state = install_fake_gcs(tmp_path, exists=True, labels={"wolf": "success"})
        (state / "objects.json").write_text(json.dumps([item.dest + "/a.txt"]))
        import os
        import subprocess
        script = script_for([item])
        proc = subprocess.run(["bash", "-c", "set -e\n" + script + "\necho CANINE_SCRIPT_DONE"],
                              env=dict(os.environ, PATH=path, FAKE_STATE=str(state)),
                              capture_output=True, text=True, timeout=120)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert "expired; repopulating" in proc.stderr
        assert len(input_copy_calls(state)) == 1

    def test_a_complete_directory_is_not_copied_again(self, tmp_path):
        import json
        import os
        import subprocess
        item = self._dir_item(("a.txt", "b.txt"))
        path, state = install_fake_gcs(tmp_path, exists=True, labels={"wolf": "success"})
        (state / "objects.json").write_text(json.dumps(self._under(item, ["a.txt", "b.txt"])))
        proc = subprocess.run(["bash", "-c", "set -e\n" + script_for([item]) + "\necho CANINE_SCRIPT_DONE"],
                              env=dict(os.environ, PATH=path, FAKE_STATE=str(state)),
                              capture_output=True, text=True, timeout=120)
        assert "CANINE_SCRIPT_DONE" in proc.stdout, proc.stderr
        assert "already populated" in proc.stderr
        assert input_copy_calls(state) == []


class TestADirectoryListsItsMembers:
    @staticmethod
    def _fh(names):
        fh = object.__new__(HandleGSURL)
        fh.path = "gs://src/funcotator"
        fh.is_dir = True
        fh._size = 1
        fh._dir_object_names = names
        return fh

    def test_names_are_relative_to_the_directory(self):
        assert self._fh(["funcotator/a.txt", "funcotator/sub/b.dict"]).member_names == ["a.txt", "sub/b.dict"]

    def test_placeholder_objects_are_left_out(self):
        assert self._fh(["funcotator/", "funcotator/sub/", "funcotator/a.txt"]).member_names == ["a.txt"]

    def test_a_single_object_has_none(self):
        fh = self._fh([])
        fh.is_dir = False
        assert fh.member_names == []


class TestAJobWithNothingToUpload:
    '''
    Every input of a LocalizeToBucket can pass through as is: upstream outputs on the
    NFS share, or existing bucketmount:// URLs. Then there is no bucket, and it was
    still submitted to a node that would only run the no-op script -- seen on
    wolf2-east-slw 2026-10-02, where Localize_T_bam_char asked for a node for two BAMs
    already on the share.
    '''
    def _localizer(self, monkeypatch, tmp_path, rodisk_paths, **kwargs):
        monkeypatch.delenv("CANINE_DISABLE_CONTROLLER_LOCALIZATION", raising=False)
        loc = make_localizer(localize_to_persistent_disk=True, **kwargs)
        loc.rodisk_paths = rodisk_paths
        return loc

    @staticmethod
    def _outputs(loc):
        import os
        out = os.path.join(loc.environment("local")["CANINE_OUTPUT"], "0")
        return {n: os.path.isfile(os.path.join(out, n)) for n in ("stdout", "stderr")}

    def test_it_settles_on_the_controller(self, monkeypatch, tmp_path):
        loc = self._localizer(monkeypatch, tmp_path, {"0": {"t_bam": ["/mnt/nfs/ws/t.bam"]}})
        assert loc.resolve_on_controller("0") is True
        assert self._outputs(loc) == {"stdout": True, "stderr": True}

    def test_the_log_says_why(self, monkeypatch, tmp_path):
        logged = []
        monkeypatch.setattr("canine.localization.base.canine_logging.info1", logged.append)
        loc = self._localizer(monkeypatch, tmp_path, {"0": {"bam": ["bucketmount://b/bam/x.bam"]}})
        loc.resolve_on_controller("0")
        assert any("every input passes through" in m and "no node needed" in m for m in logged)

    def test_not_on_a_dry_run(self, monkeypatch, tmp_path):
        loc = self._localizer(monkeypatch, tmp_path, {"0": {"t_bam": ["/mnt/nfs/ws/t.bam"]}},
                              persistent_disk_dry_run=True)
        assert loc.resolve_on_controller("0") is False

    def test_not_for_a_job_bucket_localization_never_planned(self, monkeypatch, tmp_path):
        loc = self._localizer(monkeypatch, tmp_path, {})
        assert loc.resolve_on_controller("0") is False

    def test_not_when_turned_off(self, monkeypatch, tmp_path):
        loc = self._localizer(monkeypatch, tmp_path, {"0": {"t_bam": ["/mnt/nfs/ws/t.bam"]}},
                              resolve_on_controller=False)
        assert loc.resolve_on_controller("0") is False
        loc = self._localizer(monkeypatch, tmp_path, {"0": {"t_bam": ["/mnt/nfs/ws/t.bam"]}})
        monkeypatch.setenv("CANINE_DISABLE_CONTROLLER_LOCALIZATION", "1")   # after _localizer clears it
        assert loc.resolve_on_controller("0") is False
