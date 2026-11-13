"""
move_to_bucket (MOVE_TO_BUCKET.md): a scratch-disk job's outputs moved to a
bucket when it succeeds, its disk deleted, and the outputs returned as
bucketmount:// URLs. No cluster and no GCS: a fake gcloud copies into a local
directory standing in for GCS.
"""
import os
import shutil
import subprocess
import sys
from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from canine.localization import base
from canine.localization import delocalization
from canine.localization.nfs import NFSLocalizer

sys.path.insert(0, os.path.dirname(__file__))
from test_orchestrator_avoid_pure import OUTPUTS, Localizer, finished_job

BASH = shutil.which("bash")

FAKE_GCLOUD = r"""#!/bin/bash
# gcloud storage cp [-r] [-n] [--custom-time=...] <sources...> <gs:// destination>
echo "$@" >> "$FAKE_GCLOUD_LOG"
[ -n "$FAKE_GCLOUD_FAIL" ] && exit 1
shift 2
args=(); for a in "$@"; do [[ $a == -* ]] || args+=("$a"); done
last=$(( ${#args[@]} - 1 )); dest=${args[$last]}; unset "args[$last]"
dest=$FAKE_GCS/${dest#gs://}
for src in "${args[@]}"; do
  if [[ $dest == */ ]]; then mkdir -p "$dest"; cp -r "$src" "$dest"; else mkdir -p "$(dirname "$dest")"; cp -r "$src" "$dest"; fi
done
"""


@pytest.fixture
def fake_gcloud(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "gcloud").write_text(FAKE_GCLOUD)
    (bin_dir / "gcloud").chmod(0o755)
    monkeypatch.setenv("PATH", "{}:{}".format(bin_dir, os.environ["PATH"]))
    monkeypatch.setenv("FAKE_GCS", str(tmp_path / "gcs"))
    monkeypatch.setenv("FAKE_GCLOUD_LOG", str(tmp_path / "gcloud.log"))
    monkeypatch.setattr(delocalization.time, "sleep", lambda s: None)
    return tmp_path


def job(tmp_path):
    """A finished job's workspace on its scratch disk, and its output directory."""
    workspace = tmp_path / "rwdisks" / "canine-scratch-abc-3"
    workspace.mkdir(parents = True)
    (workspace / "S.bam").write_text("bam")
    (workspace / "S.bai").write_text("bai")
    (workspace / "qc").mkdir()
    (workspace / "qc" / "a.txt").write_text("a")
    jobdir = tmp_path / "outputs" / "3"
    outputs = []
    for name, rel in (("bam", "S.bam"), ("bai", "S.bai"), ("qc", "qc")):
        (jobdir / name).mkdir(parents = True)
        (jobdir / name / rel).symlink_to(workspace / rel)
        outputs.append([name, str(jobdir / name / rel), rel])
    return workspace, jobdir, outputs


@pytest.mark.skipif(BASH is None, reason = "bash needed")
class TestMovingOutputs:

    def move(self, tmp_path):
        workspace, jobdir, outputs = job(tmp_path)
        os.chdir(workspace)
        ok = delocalization.move_to_bucket(str(jobdir), "3", outputs, "wolf-1-r-h", "/mnt/bucketmounts/wolf-1-r-h")
        return ok, jobdir

    def test_copies_every_output_in_one_call(self, fake_gcloud):
        ok, _ = self.move(fake_gcloud)
        calls = (fake_gcloud / "gcloud.log").read_text().splitlines()
        assert ok
        assert len(calls) == 2, "the outputs together, then the record"
        assert calls[0].startswith("storage cp -r -n --custom-time=")
        assert calls[0].endswith(" S.bai S.bam qc gs://wolf-1-r-h/3/")
        gcs = fake_gcloud / "gcs" / "wolf-1-r-h" / "3"
        assert (gcs / "S.bam").read_text() == "bam" and (gcs / "qc" / "a.txt").read_text() == "a"

    def test_points_the_outputs_at_the_bucket_mount(self, fake_gcloud):
        _, jobdir = self.move(fake_gcloud)
        assert os.readlink(jobdir / "bam" / "S.bam") == "/mnt/bucketmounts/wolf-1-r-h/3/S.bam"
        assert os.readlink(jobdir / "qc" / "qc") == "/mnt/bucketmounts/wolf-1-r-h/3/qc"

    def test_records_every_object(self, fake_gcloud):
        _, jobdir = self.move(fake_gcloud)
        expected = ["gs://wolf-1-r-h/3/S.bai", "gs://wolf-1-r-h/3/S.bam", "gs://wolf-1-r-h/3/qc/a.txt"]
        assert (jobdir / base.MOVED_MARKER).read_text().splitlines() == expected
        assert (fake_gcloud / "gcs" / "wolf-1-r-h" / "3" / ".wolf_moved").read_text().splitlines() == expected

    def test_logs_the_uploads_size_and_speed(self, fake_gcloud, monkeypatch, capsys):
        workspace, jobdir, outputs = job(fake_gcloud)
        os.truncate(workspace / "S.bam", 8 * 10**6)
        clock = iter([100.0, 104.0])
        monkeypatch.setattr(delocalization.time, "monotonic", lambda: next(clock))
        os.chdir(workspace)
        assert delocalization.move_to_bucket(str(jobdir), "3", outputs, "b", "/mnt/bucketmounts/b")
        assert "INFO: moved 3 output file(s), 8 MB in 4 s (2.0 MB/s), to gs://b/3" in capsys.readouterr().err

    def test_a_failed_copy_leaves_the_outputs_on_the_disk(self, fake_gcloud, monkeypatch):
        monkeypatch.setenv("FAKE_GCLOUD_FAIL", "1")
        ok, jobdir = self.move(fake_gcloud)
        assert not ok
        assert len((fake_gcloud / "gcloud.log").read_text().splitlines()) == 3, "tried three times"
        assert "rwdisks" in os.readlink(jobdir / "bam" / "S.bam")
        assert not (jobdir / base.MOVED_MARKER).exists()

    def test_an_output_two_patterns_match_is_copied_once(self, fake_gcloud):
        workspace, jobdir, outputs = job(fake_gcloud)
        (jobdir / "all").mkdir()
        (jobdir / "all" / "S.bam").symlink_to(workspace / "S.bam")
        outputs.append(["all", str(jobdir / "all" / "S.bam"), "S.bam"])
        os.chdir(workspace)
        assert delocalization.move_to_bucket(str(jobdir), "3", outputs, "b", "/mnt/bucketmounts/b")
        assert (fake_gcloud / "gcloud.log").read_text().splitlines()[0].endswith(" S.bai S.bam qc gs://b/3/")
        assert os.readlink(jobdir / "all" / "S.bam") == "/mnt/bucketmounts/b/3/S.bam"

    def test_outputs_below_the_workspace_keep_their_directory(self, fake_gcloud):
        workspace, jobdir, outputs = job(fake_gcloud)
        (workspace / "sub").mkdir()
        (workspace / "sub" / "x.vcf").write_text("v")
        (jobdir / "vcf").mkdir()
        (jobdir / "vcf" / "x.vcf").symlink_to(workspace / "sub" / "x.vcf")
        outputs.append(["vcf", str(jobdir / "vcf" / "x.vcf"), "sub/x.vcf"])
        os.chdir(workspace)
        assert delocalization.move_to_bucket(str(jobdir), "3", outputs, "b", "/mnt/bucketmounts/b")
        assert (fake_gcloud / "gcs" / "b" / "3" / "sub" / "x.vcf").read_text() == "v"
        assert os.readlink(jobdir / "vcf" / "x.vcf") == "/mnt/bucketmounts/b/3/sub/x.vcf"


class TestOutputUrls:

    def test_a_moved_output_is_a_bucket_mount(self):
        assert base.scratch_output_url("/mnt/bucketmounts/wolf-1-r-h/3/S.bam") == "bucketmount://wolf-1-r-h/3/S.bam"

    def test_one_left_on_the_disk_is_a_rodisk(self):
        assert base.scratch_output_url("/mnt/rwdisks/canine-scratch-abc-3/S.bam") == "rodisk://canine-scratch-abc-3/S.bam"


def scratch_teardown(moved_marker):
    localizer = object.__new__(NFSLocalizer)
    localizer.protect_disk = False
    disk_client = MagicMock()
    disk_client.get.side_effect = google.api_core.exceptions.NotFound("no disk")
    with patch.object(base, "get_default_gcp_zone", return_value = "us-central1-a"), \
         patch.object(base, "gcloud_disk_client", return_value = disk_client):
        _, _, teardown, _ = localizer.create_persistent_disk(disk_name = "abc-3", moved_marker = moved_marker)
    return teardown


@pytest.mark.skipif(BASH is None, reason = "bash needed")
class TestTheDisk:

    def end(self, tmp_path, rc, moved):
        """The disk's fate: the last lines of the teardown, run with a fake gcloud."""
        marker = tmp_path / base.MOVED_MARKER
        if moved:
            marker.write_text("gs://b/3/S.bam\n")
        lines = scratch_teardown(str(marker))
        lines = lines[lines.index('if [[ ! -z $CANINE_JOB_RC && $CANINE_JOB_RC -eq 0 ]]; then'):]
        (tmp_path / "gcloud").write_text('#!/bin/bash\necho "$@"\n')
        (tmp_path / "gcloud").chmod(0o755)
        script = "\n".join(["set -e", 'export PATH="{}:$PATH"'.format(tmp_path),
                            "CANINE_JOB_RC={} CANINE_NODE_ZONE=z".format(rc)] + lines)
        return subprocess.run([BASH], input = script, capture_output = True, text = True, check = True).stdout

    def test_is_deleted_once_the_outputs_are_moved(self, tmp_path):
        assert self.end(tmp_path, 0, True).strip() == "compute disks delete canine-scratch-abc-3 --zone z --quiet"

    def test_is_kept_and_labelled_when_they_were_not(self, tmp_path):
        assert self.end(tmp_path, 0, False).strip() == "compute disks add-labels canine-scratch-abc-3 --zone z --labels finished=yes"

    def test_is_kept_when_the_job_failed(self, tmp_path):
        assert self.end(tmp_path, 1, True).strip() == ""

    def test_a_delete_that_fails_only_warns(self, tmp_path):
        """GCE refuses to delete a disk another node still has attached."""
        (tmp_path / base.MOVED_MARKER).write_text("gs://b/3/S.bam\n")
        lines = base.AbstractLocalizer.moved_disk_teardown(None, "canine-scratch-abc-3", str(tmp_path / base.MOVED_MARKER))
        (tmp_path / "gcloud").write_text("#!/bin/bash\nexit 1\n")
        (tmp_path / "gcloud").chmod(0o755)
        script = "\n".join(["set -e", 'export PATH="{}:$PATH"'.format(tmp_path), "CANINE_JOB_RC=0"] + lines + ["echo continued"])
        proc = subprocess.run([BASH], input = script, capture_output = True, text = True)
        assert proc.returncode == 0 and proc.stdout.strip() == "continued"
        assert "WARNING: could not delete scratch disk canine-scratch-abc-3" in proc.stderr

    def test_without_move_to_bucket_is_labelled_as_before(self):
        assert scratch_teardown(None)[-1] == \
          'if [[ ! -z $CANINE_JOB_RC && $CANINE_JOB_RC -eq 0 ]]; then gcloud compute disks add-labels "canine-scratch-abc-3" --zone "$CANINE_NODE_ZONE" --labels finished=yes; fi'


def job_teardown(finished, move_to_bucket = True):
    """The teardown job_setup_teardown writes for a job whose scratch disk is finished, or new."""
    from canine.localization.local import BatchedLocalizer
    loc = BatchedLocalizer(MagicMock(), use_scratch_disk = True, scratch_disk_name = "canine-scratch-abc",
                           move_to_bucket = move_to_bucket)
    loc.clean_on_exit = False
    loc.staging_dir = "/mnt/nfs/staging"
    loc.backend.config = {"compute_zone": "us-central1-a"}
    loc.inputs = {"0": {}}
    loc.input_array_flag = {"0": {}}
    disk = (("/mnt/rwdisks/canine-scratch-abc-0", [], [], "rodisk://canine-scratch-abc-0") if finished
            else ("/mnt/rwdisks/canine-scratch-abc-0", ["echo create"], ["echo scratch teardown"], None))
    with patch.object(loc, "create_persistent_disk", return_value = disk), \
         patch.object(type(loc), "move_bucket_name", return_value = "wolf-123-us-central1-x"):
        _, _, teardown, _ = loc.job_setup_teardown("0", {"stdout": "../stdout", "stderr": "../stderr"}, MagicMock())
    return teardown


class TestAJobSkippedOnItsFinishedDisk:
    """
    A job whose move failed kept its finished disk; on a rerun it's skipped
    (exit 15) with the disk mounted read-only, and its delocalization moves the
    outputs off it. Then nothing needs the disk.
    """

    def test_deletes_it_once_it_is_detached(self):
        teardown = job_teardown(finished = True)
        delete = teardown.index('gcloud compute disks delete "canine-scratch-abc-0"')
        assert teardown.index("Detach calls done") < delete
        # only if this job's delocalization moved its outputs
        check = teardown[teardown.rindex("if [ -f ", 0, delete):delete].splitlines()[0]
        assert "/outputs/0/{}".format(base.MOVED_MARKER) in check and check.endswith(" ]; then")

    def test_a_new_disk_has_only_its_own_teardown(self):
        teardown = job_teardown(finished = False)
        assert "echo scratch teardown" in teardown
        assert "disks delete" not in teardown

    def test_not_without_move_to_bucket(self):
        assert "disks delete" not in job_teardown(finished = True, move_to_bucket = False)


class TestTheTeardownScript:
    """The whole teardown parses: a heredoc's terminator indented by a block ends the script early."""

    @pytest.mark.parametrize("finished", [True, False])
    def test_bash_parses_it(self, finished):
        r = subprocess.run(["bash", "-n"], input = job_teardown(finished), text = True, capture_output = True)
        assert r.returncode == 0, r.stderr

    def test_creates_the_bucket_with_its_lifecycle_rule(self, tmp_path):
        # the bucket-creation block alone, run with a gcloud that records its calls
        lines = job_teardown(finished = False).splitlines()
        start = lines.index('if [[ -z $CANINE_DEBUG_MODE && $CANINE_JOB_RC == 0 ]]; then')
        block = lines[start:lines.index("fi", lines.index('rm -f "$CANINE_BUCKET_LC"', start)) + 1]
        script = "\n".join([
          'gcloud() {',
          '  if [ "$3" != create ]; then [ -e made ]; return; fi',
          '  for a; do case $a in --lifecycle-file=*) cat "${a#*=}" > lifecycle;; esac; done; touch made',
          '}',
          "CANINE_JOB_RC=0",
        ] + block)
        r = subprocess.run(["bash", "-c", script], cwd = tmp_path, text = True, capture_output = True)
        assert r.returncode == 0, r.stderr
        assert '"daysSinceCustomTime":7' in (tmp_path / "lifecycle").read_text()


class MovingLocalizer(Localizer):
    def __init__(self, root, present = True, **kwargs):
        super().__init__(root, use_scratch_disk = True, **kwargs)
        self.move_to_bucket = True
        self.present = present
        self.checked = []

    def moved_outputs_present(self, marker_path, transport):
        self.checked.append(marker_path)
        return self.present


class TestAvoidingAJobWhoseOutputsWereMoved:
    """Its disk is gone; its outputs are in the bucket, or it reruns."""

    def avoid(self, tmp_path, **kwargs):
        from canine.orchestrator import Orchestrator
        orch = object.__new__(Orchestrator)
        orch.job_spec = {"0": {"individual": "S"}}
        orch.raw_outputs = dict(OUTPUTS)
        localizer = MovingLocalizer(tmp_path, **kwargs)
        n, _ = orch.job_avoid(localizer)
        return n, orch.job_spec, localizer

    def test_is_avoided_while_they_are_in_the_bucket(self, tmp_path):
        finished_job(tmp_path, copied = ())
        n, spec, localizer = self.avoid(tmp_path)
        assert n == 1 and spec["0"] is None
        assert localizer.checked == [str(tmp_path / "outputs" / "0" / base.MOVED_MARKER)]

    def test_is_rerun_when_one_is_gone(self, tmp_path):
        finished_job(tmp_path, copied = ())
        n, spec, _ = self.avoid(tmp_path, present = False)
        assert n == 0 and spec["0"] is not None

    def test_checks_the_copies_too(self, tmp_path):
        finished_job(tmp_path, copied = ())
        n, _, _ = self.avoid(tmp_path, files_to_copy_to_outputs = {"mat_file"})
        assert n == 0, "mat_file was to be copied to NFS, and isn't there"


class TestCheckingTheBucket:

    def present(self, tmp_path, urls, existing):
        marker = tmp_path / base.MOVED_MARKER
        if urls is not None:
            marker.write_text("".join(u + "\n" for u in urls))
        client = MagicMock()
        client.bucket.side_effect = lambda b: MagicMock(blob = lambda p: MagicMock(exists = lambda: "gs://{}/{}".format(b, p) in existing))
        localizer = object.__new__(NFSLocalizer)
        transport = MagicMock(isfile = os.path.isfile, open = open)
        with patch.object(base, "gcloud_storage_client", return_value = client):
            return localizer.moved_outputs_present(str(marker), transport)

    def test_every_object_there(self, tmp_path):
        assert self.present(tmp_path, ["gs://b/0/S.bam", "gs://b/0/S.bai"], {"gs://b/0/S.bam", "gs://b/0/S.bai"})

    def test_one_expired(self, tmp_path):
        assert not self.present(tmp_path, ["gs://b/0/S.bam", "gs://b/0/S.bai"], {"gs://b/0/S.bam"})

    def test_no_marker(self, tmp_path):
        assert not self.present(tmp_path, None, set())


class TestTheBucket:

    def localizer(self, name):
        localizer = object.__new__(NFSLocalizer)
        localizer.project = "p"
        localizer.scratch_disk_name = name
        localizer.backend = MagicMock(config = {"compute_zone": "us-central1-a"})
        return localizer

    def test_is_one_per_task(self):
        with patch.object(base, "get_project_number", return_value = "123"):
            a = self.localizer("canine-scratch-abc").move_bucket_name()
            b = self.localizer("canine-scratch-abd").move_bucket_name()
        assert a.startswith("wolf-123-us-central1-") and a != b

    def test_is_created_private_with_the_moved_outputs_expiry(self):
        localizer = self.localizer("canine-scratch-abc")
        localizer.move_to_bucket_expiry_days = 7
        script = "\n".join(localizer.move_bucket_script("wolf-123-us-central1-x"))
        assert '"daysSinceCustomTime":7' in script
        assert "buckets create gs://wolf-123-us-central1-x --location=us-central1 " + base.BUCKET_CREATE_FLAGS in script
