"""
Route C tests: stage on a real block device, verify there, then publish.

The generic fallback for a non-POSIX destination that is not a resolvable bucket.

Forcing this route needs care: only the *destination* may look non-POSIX, because the
staging directory has to be a real filesystem or the route makes no sense. So
select_route is patched per-path rather than wholesale, which also exercises the fact
that Route C consults it a second time for its own staging candidate.
"""

import hashlib
import os

import pytest

from canine.localization import parallel_download as pdl
from pdl_server import Server

MIB = 1024 * 1024


@pytest.fixture(scope="module")
def payload():
    return os.urandom(5 * MIB + 999)


@pytest.fixture(scope="module")
def payload_md5(payload):
    return hashlib.md5(payload).hexdigest()


def force_staged_for(monkeypatch, dest):
    """
    Make just `dest` look like it lives on a non-POSIX filesystem, leaving every other
    path (notably staging candidates) to the real gate.
    """
    real = pdl.select_route
    dest_dir = os.path.dirname(os.path.abspath(dest))

    def patched(path, **kwargs):
        if os.path.dirname(os.path.abspath(path)) == dest_dir:
            return pdl.RouteDecision(pdl.ROUTE_STAGED, "test: forced staged route")
        return real(path, **kwargs)

    monkeypatch.setattr(pdl, "select_route", patched)


def options_for(dest, url, size, **overrides):
    argv = [
        "--url", url, "--dest", dest, "--size", str(size),
        "--connections", str(overrides.pop("connections", 4)),
        "--min-chunk", str(overrides.pop("min_chunk", MIB)),
    ]
    for key, value in overrides.items():
        if key == "work_dir":
            for item in (value if isinstance(value, list) else [value]):
                argv += ["--work-dir", str(item)]
        else:
            argv += ["--" + key.replace("_", "-"), str(value)]
    return pdl.build_parser().parse_args(argv)


# ---------------------------------------------------------------------------
# work directory selection
# ---------------------------------------------------------------------------

class TestWorkDirectorySelection:

    def test_explicit_work_dirs_come_first(self, tmp_path, monkeypatch):
        """
        The handler passes the localization persistent disk explicitly, and it must win:
        it is the only candidate whose staged file survives a preemption.
        """
        monkeypatch.setenv("CANINE_LOCAL_DISK_DIR", str(tmp_path / "ephemeral"))
        preferred = tmp_path / "pd"
        preferred.mkdir()
        options = options_for("/mnt/x/o.bin", "http://h/o", 1024,
                              work_dir=[str(preferred)])
        candidates = pdl.work_directory_candidates(options)
        assert candidates[0] == str(preferred)

    def test_env_candidates_are_included_in_order(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CANINE_LOCAL_DISK_DIR", "/local/disk")
        monkeypatch.setenv("TMPDIR", "/some/tmp")
        options = options_for("/mnt/x/o.bin", "http://h/o", 1024)
        candidates = pdl.work_directory_candidates(options)
        assert candidates.index("/local/disk") < candidates.index("/some/tmp")

    def test_duplicates_are_collapsed(self, monkeypatch):
        monkeypatch.setenv("CANINE_LOCAL_DISK_DIR", "/dup")
        monkeypatch.setenv("TMPDIR", "/dup")
        options = options_for("/mnt/x/o.bin", "http://h/o", 1024, work_dir="/dup")
        assert pdl.work_directory_candidates(options).count("/dup") == 1

    def test_a_directory_without_room_is_rejected(self, tmp_path, monkeypatch):
        """
        Proceeding anyway would run out of space mid-publish and leave a truncated
        object at the destination, which is worse than declining the route.
        """
        monkeypatch.setattr(pdl, "free_bytes", lambda directory: 10)
        options = options_for("/mnt/x/o.bin", "http://h/o", 1024,
                              work_dir=str(tmp_path))
        staging, _ = pdl.select_work_directory(options, 1024)
        assert staging is None

    def test_a_non_posix_candidate_is_rejected(self, tmp_path, monkeypatch):
        """A staging area on another FUSE mount would defeat the whole point."""
        monkeypatch.setattr(
            pdl, "select_route",
            lambda path, **kw: pdl.RouteDecision(pdl.ROUTE_STAGED, "not posix"))
        options = options_for("/mnt/x/o.bin", "http://h/o", 1024,
                              work_dir=str(tmp_path))
        staging, _ = pdl.select_work_directory(options, 1024)
        assert staging is None

    def test_a_usable_candidate_is_accepted(self, tmp_path):
        options = options_for("/mnt/x/o.bin", "http://h/o", 1024,
                              work_dir=str(tmp_path))
        staging, decision = pdl.select_work_directory(options, 1024)
        assert staging == os.path.join(str(tmp_path), pdl.STAGING_SUBDIR)
        assert decision.route == pdl.ROUTE_POSIX

    def test_headroom_is_required_not_just_the_bare_size(self, tmp_path, monkeypatch):
        size = 1000
        monkeypatch.setattr(pdl, "free_bytes", lambda d: size + 10)
        options = options_for("/mnt/x/o.bin", "http://h/o", size,
                              work_dir=str(tmp_path))
        assert pdl.select_work_directory(options, size)[0] is None


# ---------------------------------------------------------------------------
# end to end
# ---------------------------------------------------------------------------

class TestStagedRoute:

    def test_publishes_a_correct_object(self, tmp_path, monkeypatch, payload,
                                       payload_md5):
        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        force_staged_for(monkeypatch, dest)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     work_dir=str(tmp_path / "work")))
        assert rc == pdl.EXIT_OK
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5

    def test_staged_copy_is_cleaned_up(self, tmp_path, monkeypatch, payload, payload_md5):
        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        work = tmp_path / "work"
        force_staged_for(monkeypatch, dest)
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5, work_dir=str(work)))
        staging = work / pdl.STAGING_SUBDIR
        leftover = [p.name for p in staging.iterdir()] if staging.exists() else []
        assert leftover == [], "staging not cleaned: {}".format(leftover)

    def test_marker_is_written_beside_the_destination(self, tmp_path, monkeypatch,
                                                     payload, payload_md5):
        """
        The marker has to be next to dest, not next to the staged file: it is what the
        emitted bash consults on the next attempt.
        """
        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        force_staged_for(monkeypatch, dest)
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5,
                                work_dir=str(tmp_path / "work")))
        _, marker = pdl.sidecar_paths(dest)
        assert os.path.exists(marker)

    def test_degrades_to_a_single_stream_when_nothing_has_room(
        self, tmp_path, monkeypatch, payload
    ):
        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        force_staged_for(monkeypatch, dest)
        monkeypatch.setattr(pdl, "free_bytes", lambda directory: 1)
        sentinel = str(tmp_path / "legacy-ran")
        with Server(payload) as source:
            rc = pdl.run(options_for(
                dest, source.url(), len(payload),
                legacy_cmd="touch {}".format(sentinel),
            ))
        assert rc == 0
        assert os.path.exists(sentinel), "should have degraded to the legacy command"

    def test_no_temp_name_and_rename_is_used(self, tmp_path, monkeypatch, payload,
                                            payload_md5):
        """
        rename is not atomic on a flat-namespace bucket, so a publish-then-rename would
        leave a window where the destination is neither the old nor the new object.
        """
        renames = []
        real_rename = pdl.os.rename

        def tracking_rename(src, dst):
            renames.append((src, dst))
            return real_rename(src, dst)

        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        force_staged_for(monkeypatch, dest)
        monkeypatch.setattr(pdl.os, "rename", tracking_rename)

        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5,
                                work_dir=str(tmp_path / "work")))

        into_dest = [pair for pair in renames if pair[1] == dest]
        assert into_dest == [], "published via rename: {}".format(into_dest)


# ---------------------------------------------------------------------------
# verify-before-publish, and publish resumability (§8.4p)
# ---------------------------------------------------------------------------

class TestVerifyBeforePublish:

    def test_a_corrupt_download_never_reaches_the_destination(
        self, tmp_path, monkeypatch, payload
    ):
        """
        Verification runs on the staged copy first, so a bad download costs no upload
        and never puts a wrong object where a consumer could read it.
        """
        published = []
        monkeypatch.setattr(
            pdl, "publish_staged_file",
            lambda staged, dest, size: published.append(dest))

        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        force_staged_for(monkeypatch, dest)

        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5="0" * 32,
                                     work_dir=str(tmp_path / "work")))

        assert rc == pdl.EXIT_FAIL
        assert published == [], "published a file that failed verification"
        assert not os.path.exists(dest)
        _, marker = pdl.sidecar_paths(dest)
        assert not os.path.exists(marker)


class TestPublishResumability:

    def _setup(self, tmp_path, monkeypatch):
        dest = str(tmp_path / "mount" / "obj.bin")
        os.makedirs(os.path.dirname(dest))
        force_staged_for(monkeypatch, dest)
        return dest, str(tmp_path / "work")

    def test_failure_during_publish_requeues_and_keeps_the_staged_copy(
        self, tmp_path, monkeypatch, payload, payload_md5
    ):
        dest, work = self._setup(tmp_path, monkeypatch)

        def boom(staged, dest_path, size):
            raise IOError("publish interrupted")

        monkeypatch.setattr(pdl, "publish_staged_file", boom)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, work_dir=work))

        assert rc == pdl.EXIT_REQUEUE
        staged = os.path.join(work, pdl.STAGING_SUBDIR, "obj.bin")
        assert os.path.exists(staged), "staged copy must survive for the next attempt"
        _, staged_marker = pdl.sidecar_paths(staged)
        assert os.path.exists(staged_marker), "verification result must survive too"

    def test_republish_after_an_interrupted_publish_redownloads_nothing(
        self, tmp_path, monkeypatch, payload, payload_md5
    ):
        """
        The headline Route C property: a preemption during the publish costs only the
        publish. The staged copy is already verified, so the re-run must transfer no
        source bytes at all.
        """
        dest, work = self._setup(tmp_path, monkeypatch)

        def boom(staged, dest_path, size):
            raise IOError("publish interrupted")

        monkeypatch.setattr(pdl, "publish_staged_file", boom)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, work_dir=work))
            assert rc == pdl.EXIT_REQUEUE
            before = source.state.snapshot()["sent"]

            monkeypatch.undo()
            force_staged_for(monkeypatch, dest)
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, work_dir=work))
            transferred = source.state.snapshot()["sent"] - before

        assert rc == pdl.EXIT_OK, "re-publish did not complete"
        assert hashlib.md5(open(dest, "rb").read()).hexdigest() == payload_md5
        # only the mandatory one-byte range probe may cross the wire
        assert transferred <= 1, (
            "re-publish refetched {} bytes; the staged copy was already "
            "verified".format(transferred)
        )

    def test_kill_after_publish_before_marker_does_not_redownload(
        self, tmp_path, monkeypatch, payload, payload_md5
    ):
        dest, work = self._setup(tmp_path, monkeypatch)
        marker_writes = []
        real_marker = pdl.write_done_marker

        def failing_marker(path, size, plan_id, digest):
            marker_writes.append(path)
            if path == pdl.sidecar_paths(dest)[1]:
                raise IOError("died before the marker landed")
            return real_marker(path, size, plan_id, digest)

        monkeypatch.setattr(pdl, "write_done_marker", failing_marker)
        with Server(payload) as source:
            with pytest.raises(IOError):
                pdl.run(options_for(dest, source.url(), len(payload),
                                    check_md5=payload_md5, work_dir=work))
            assert os.path.exists(dest), "publish should have completed"
            before = source.state.snapshot()["sent"]

            monkeypatch.undo()
            force_staged_for(monkeypatch, dest)
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, work_dir=work))
            transferred = source.state.snapshot()["sent"] - before

        assert rc == pdl.EXIT_OK
        assert transferred <= 1, "refetched {} bytes".format(transferred)
        _, marker = pdl.sidecar_paths(dest)
        assert os.path.exists(marker)

    def test_marker_is_written_only_after_the_post_close_size_check(
        self, tmp_path, monkeypatch, payload, payload_md5
    ):
        """
        With gcsfuse the object exists only once the handle closes, so the size can only
        be confirmed afterwards -- and the marker must come after that confirmation.
        """
        dest, work = self._setup(tmp_path, monkeypatch)

        def short_publish(staged, dest_path, size):
            with open(dest_path, "wb") as fh:
                fh.write(b"truncated")
            return 9

        monkeypatch.setattr(pdl, "publish_staged_file", short_publish)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, work_dir=work))

        assert rc == pdl.EXIT_REQUEUE
        _, marker = pdl.sidecar_paths(dest)
        assert not os.path.exists(marker), \
            "marker written despite the published object being the wrong size"


class TestPublishStagedFile:

    def test_copies_bytes_exactly(self, tmp_path):
        payload = os.urandom(3 * MIB + 17)
        staged = tmp_path / "staged.bin"
        staged.write_bytes(payload)
        dest = str(tmp_path / "out" / "dest.bin")
        assert pdl.publish_staged_file(str(staged), dest, len(payload)) == len(payload)
        assert open(dest, "rb").read() == payload

    def test_creates_the_destination_directory(self, tmp_path):
        staged = tmp_path / "staged.bin"
        staged.write_bytes(b"x" * 10)
        dest = str(tmp_path / "a" / "b" / "c.bin")
        pdl.publish_staged_file(str(staged), dest, 10)
        assert os.path.exists(dest)

    def test_size_mismatch_raises(self, tmp_path):
        staged = tmp_path / "staged.bin"
        staged.write_bytes(b"x" * 10)
        with pytest.raises(pdl.TransientError):
            pdl.publish_staged_file(str(staged), str(tmp_path / "out.bin"), 999)

    def test_overwrites_an_existing_destination(self, tmp_path):
        """A re-publish after an interrupted attempt must not append or fail."""
        staged = tmp_path / "staged.bin"
        staged.write_bytes(b"new-content")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(b"stale-and-longer-content")
        pdl.publish_staged_file(str(staged), str(dest), len(b"new-content"))
        assert dest.read_bytes() == b"new-content"
