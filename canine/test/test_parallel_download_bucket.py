"""
The bucket-compose route tests: parts uploaded through resumable sessions, composed server-side.

The bucket-compose route runs in-process here rather than as a subprocess, because it has to be pointed
at a fake GCS endpoint. That means SIGKILL is not the instrument it is for the in-place route;
instead interruption is simulated at the storage layer, which is arguably closer to what
actually happens -- the resumability claim is about what GCS has durably persisted, and
the fake service models that at the real 256 KiB granularity.
"""

import hashlib
import json
import inspect
import os
import subprocess
import threading
import time

import pytest

from canine.localization import parallel_download as pdl
from pdl_gcs import GcsServer, patch_client_endpoints
from pdl_server import Server

MIB = 1024 * 1024
BUCKET = "test-bucket"
OBJECT = "inputs/sample.bam"


@pytest.fixture(scope="module")
def payload():
    return os.urandom(6 * MIB + 1234)


@pytest.fixture(scope="module")
def payload_md5(payload):
    return hashlib.md5(payload).hexdigest()


@pytest.fixture
def gcs(monkeypatch):
    with GcsServer() as server:
        patch_client_endpoints(monkeypatch, pdl, server)
        yield server


def force_bucket_route(monkeypatch, gs_url="gs://{}/{}".format(BUCKET, OBJECT)):
    monkeypatch.setattr(
        pdl, "select_route",
        lambda dest, **kw: pdl.RouteDecision(
            pdl.ROUTE_BUCKET, "test: forced bucket route", gs_url=gs_url),
    )


MANIFEST_OBJECT = OBJECT + ".k9pdl.json"


def read_manifest(gcs):
    """
    The manifest is a GCS object on this route, not a file. It must not be on the mount:
    a flat-namespace bucket has no atomic rename, which is what the filesystem manifest's
    commit depends on -- and this route exists because that filesystem misbehaves.
    """
    body = gcs.state.objects.get(MANIFEST_OBJECT)
    return json.loads(body.decode("utf-8")) if body else None


def options_for(dest, url, size, **overrides):
    argv = [
        "--url", url, "--dest", dest, "--size", str(size),
        "--connections", str(overrides.pop("connections", 4)),
        "--min-chunk", str(overrides.pop("min_chunk", MIB)),
        "--retries", str(overrides.pop("retries", 3)),
        # No linger in tests: payloads are tiny, so a 2 s window per commit round would
        # add minutes across the suite for no coverage. The linger itself is tested
        # directly in TestTheManifestWriterBatches.
        "--commit-linger", str(overrides.pop("commit_linger", 0)),
    ]
    for key, value in overrides.items():
        argv += ["--" + key.replace("_", "-"), str(value)]
    return pdl.build_parser().parse_args(argv)


# ---------------------------------------------------------------------------
# gs:// URL handling
# ---------------------------------------------------------------------------

class TestSplitGsUrl:

    def test_splits_bucket_and_object(self):
        assert pdl.split_gs_url("gs://b/path/to/o.bam") == ("b", "path/to/o.bam")

    @pytest.mark.parametrize("url", ["https://b/o", "gs://b", "gs://b/", "gs:///o"])
    def test_rejects_malformed(self, url):
        with pytest.raises(ValueError):
            pdl.split_gs_url(url)


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

class TestBucketRoute:

    def test_composes_a_correct_object(self, tmp_path, monkeypatch, gcs, payload,
                                      payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
        assert rc == pdl.EXIT_OK
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5

    def test_no_local_file_is_created(self, tmp_path, monkeypatch, gcs, payload,
                                     payload_md5):
        """The VM is a pure relay on this route: bytes never land on a local disk."""
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
        assert rc == pdl.EXIT_OK
        assert not os.path.exists(dest)

    def test_parts_are_composed_in_order(self, tmp_path, monkeypatch, gcs, payload,
                                        payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5))
        destination, sources = gcs.state.compose_calls[-1]
        assert destination == OBJECT
        assert sources == sorted(sources), "parts must be composed in order"
        assert all(s.startswith(OBJECT + ".k9pdl.parts/") for s in sources)

    def test_parts_are_deleted_after_compose(self, tmp_path, monkeypatch, gcs, payload,
                                            payload_md5):
        """
        The JSON API's objects.compose has no deleteSourceObjects parameter, so parts are
        removed explicitly; leaving them would be a silent, permanent storage charge.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5))
        leftover = [n for n in gcs.state.object_names() if ".k9pdl.parts/" in n]
        assert leftover == [], "parts left behind: {}".format(leftover)

    def test_marker_written_and_manifest_removed(self, tmp_path, monkeypatch, gcs,
                                                payload, payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5))
        manifest_path, marker_path = pdl.sidecar_paths(dest)
        assert os.path.exists(marker_path)
        assert not os.path.exists(manifest_path)

    def test_rerun_short_circuits_on_the_marker(self, tmp_path, monkeypatch, gcs,
                                               payload, payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5))
            before = source.state.snapshot()["sent"]
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
            after = source.state.snapshot()["sent"]
        assert rc == pdl.EXIT_OK
        assert after == before


# ---------------------------------------------------------------------------
# session handling and resumability (§8.4o)
# ---------------------------------------------------------------------------

class TestSessionResumability:

    def _interrupted_run(self, dest, source, payload, payload_md5, gcs,
                         succeed_uploads, connections=4):
        """
        Let `succeed_uploads` uploads through, then refuse everything, which abandons the
        attempt with several parts genuinely partially persisted -- the state a
        preemption leaves behind. Returns the exit code.

        Pins the upload block to GCS's commit granularity, because that granularity is
        what this class is about: a part left partially persisted, resumable from the
        durable offset GCS reports. At the default 8 MiB block a 6 MiB payload is one
        PUT per chunk, so "fail after N uploads" never fires and every test here would
        pass while interrupting nothing -- which is exactly what happened when the
        default moved.
        """
        gcs.state.uploads_seen = 0
        gcs.state.fail_uploads_after = succeed_uploads
        try:
            return pdl.run(options_for(dest, source.url(), len(payload),
                                       check_md5=payload_md5, connections=connections,
                                       upload_block=pdl.GCS_UPLOAD_GRANULARITY,
                                       retries=0))
        finally:
            gcs.state.fail_uploads_after = None

    @staticmethod
    def _persisted_bytes(gcs):
        """How much every still-open session has durably committed."""
        with gcs.state.lock:
            return sum(s["committed"] for s in gcs.state.sessions.values())

    def test_session_uris_are_persisted_before_any_bytes_are_sent(
        self, tmp_path, monkeypatch, gcs, payload, payload_md5
    ):
        """
        The session URI is the only handle to partially-uploaded data. Recording it after
        sending bytes would mean paying for bytes nothing can ever reach.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        manifest_path, _ = pdl.sidecar_paths(dest)

        with Server(payload) as source:
            self._interrupted_run(dest, source, payload, payload_md5, gcs,
                                  succeed_uploads=0)

        state = read_manifest(gcs)
        assert state is not None, "manifest should survive the failure"
        assert not os.path.exists(manifest_path), \
            "the manifest must live in the bucket, not on the mount"
        sessions = [r.get("session") for r in state["chunks"].values()]
        assert sessions and all(sessions), \
            "expected a persisted session per attempted part, got {}".format(sessions)

    def test_resume_refetches_only_past_the_committed_offset(
        self, tmp_path, monkeypatch, gcs, payload, payload_md5
    ):
        """
        The invariant: on resume the downloader asks each session how far it persisted
        and refetches only from there. GCS commits at 256 KiB granularity, so the waste
        is bounded by that per in-flight part -- not by a whole part.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        connections = 4

        with Server(payload) as source:
            # throttled so the server's byte counter tracks what the client actually
            # pulled; unthrottled it writes a whole range into the socket at once and
            # counts bytes the interrupted client never consumed
            source.state.throttle_bytes = 64 * 1024

            rc = self._interrupted_run(dest, source, payload, payload_md5, gcs,
                                       succeed_uploads=8, connections=connections)
            assert rc in (pdl.EXIT_REQUEUE, pdl.EXIT_FAIL), rc

            persisted = self._persisted_bytes(gcs)
            assert persisted > 0, "nothing was persisted; test is not exercising resume"
            before = source.state.snapshot()["sent"]

            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, connections=connections))
            resumed = source.state.snapshot()["sent"] - before

        assert rc == pdl.EXIT_OK, "resume did not complete"
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5

        # The invariant: the resume refetches only what GCS had NOT persisted. The
        # allowance covers the one-byte range probe plus the sub-granule tail each
        # in-flight part may legitimately re-read.
        expected = len(payload) - persisted
        budget = expected + connections * pdl.GCS_UPLOAD_GRANULARITY + 1024
        assert resumed <= budget, (
            "resume refetched {} bytes; {} were already persisted so at most ~{} "
            "should have been needed. Sessions are being restarted, not resumed."
            .format(resumed, persisted, budget)
        )

    def test_an_expired_session_restarts_only_that_part(
        self, tmp_path, monkeypatch, gcs, payload, payload_md5
    ):
        """A lost or expired session (410) costs one part, never the whole object."""
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")

        with Server(payload) as source:
            self._interrupted_run(dest, source, payload, payload_md5, gcs,
                                  succeed_uploads=6)

            # kill one of the recorded sessions
            state = read_manifest(gcs)
            assert state is not None
            for record in state["chunks"].values():
                session_uri = record.get("session")
                if session_uri:
                    gcs.state.expire_session(session_uri.rsplit("/", 1)[-1])
                    break

            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))

        assert rc == pdl.EXIT_OK, "an expired session should not be fatal"
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5

    def test_incomplete_part_is_never_visible_as_an_object(
        self, tmp_path, monkeypatch, gcs, payload, payload_md5
    ):
        """
        An incomplete resumable upload leaves no object at all, so there is never an
        ambiguous half-written part to reason about.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")

        with Server(payload) as source:
            self._interrupted_run(dest, source, payload, payload_md5, gcs,
                                  succeed_uploads=5)

        open_sessions = {
            s["name"]: s["committed"] for s in gcs.state.sessions.values()
        }
        assert any(v > 0 for v in open_sessions.values()), \
            "test did not actually leave a partially-uploaded session"

        # A still-open session with committed bytes must have no object. (Objects for
        # *other* parts may well exist -- a short tail part completes in a single final
        # PUT, which is not subject to the commit granularity.)
        visible = set(gcs.state.object_names())
        leaked = [name for name, committed in open_sessions.items()
                  if committed > 0 and name in visible]
        assert leaked == [], \
            "a partial upload became a visible object: {}".format(leaked)

    def test_plan_id_mismatch_discards_recorded_sessions(
        self, tmp_path, monkeypatch, gcs, payload, payload_md5
    ):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")

        with Server(payload) as source:
            self._interrupted_run(dest, source, payload, payload_md5, gcs,
                                  succeed_uploads=6)

            replacement = os.urandom(len(payload) + 4096)
            source.state.payload = replacement
            rc = pdl.run(options_for(
                dest, source.url(), len(replacement),
                check_md5=hashlib.md5(replacement).hexdigest()))

        assert rc == pdl.EXIT_OK
        assert gcs.state.objects[OBJECT] == replacement


# ---------------------------------------------------------------------------
# compose
# ---------------------------------------------------------------------------

class TestComposeTree:

    def test_single_call_below_the_source_limit(self, gcs, monkeypatch):
        client = pdl.GcsClient()
        for index in range(5):
            gcs.state.objects["p{}".format(index)] = bytes([index]) * 10
            gcs.state.composite["p{}".format(index)] = 1
        pdl.compose_tree(client, BUCKET, "out",
                         ["p{}".format(i) for i in range(5)])
        assert len(gcs.state.compose_calls) == 1
        assert gcs.state.objects["out"] == b"".join(
            bytes([i]) * 10 for i in range(5))

    def test_tree_composes_beyond_the_source_limit(self, gcs, monkeypatch):
        """
        Sources may themselves be composite, so more than 32 parts fold in levels. This
        is what lets a 50 GB object with 800 parts compose at all.
        """
        client = pdl.GcsClient()
        count = 70
        expected = b""
        names = []
        for index in range(count):
            name = "p{:03d}".format(index)
            data = bytes([index % 251]) * 8
            gcs.state.objects[name] = data
            gcs.state.composite[name] = 1
            names.append(name)
            expected += data

        pdl.compose_tree(client, BUCKET, "out", names)

        assert len(gcs.state.compose_calls) > 1, "should have needed multiple levels"
        assert gcs.state.objects["out"] == expected
        for _, sources in gcs.state.compose_calls:
            assert len(sources) <= pdl.GCS_COMPOSE_MAX_SOURCES

    def test_intermediates_are_cleaned_up(self, gcs, monkeypatch):
        client = pdl.GcsClient()
        names = []
        for index in range(70):
            name = "p{:03d}".format(index)
            gcs.state.objects[name] = b"x" * 4
            gcs.state.composite[name] = 1
            names.append(name)
        pdl.compose_tree(client, BUCKET, "out", names)
        leftover = [n for n in gcs.state.object_names() if ".k9pdl.compose/" in n]
        assert leftover == [], "compose intermediates left behind: {}".format(leftover)

    def test_ordering_is_preserved_across_levels(self, gcs, monkeypatch):
        """Tree composition must not reorder bytes."""
        client = pdl.GcsClient()
        names = []
        expected = b""
        for index in range(100):
            name = "p{:03d}".format(index)
            data = "{:03d}".format(index).encode()
            gcs.state.objects[name] = data
            gcs.state.composite[name] = 1
            names.append(name)
            expected += data
        pdl.compose_tree(client, BUCKET, "out", names)
        assert gcs.state.objects["out"] == expected


class TestComposeTreeAtTheDepthTheRealObjectNeeds:
    """
    Every test above stops at TWO levels. 100 parts folds 100 -> 4 -> 1, and 70 folds
    70 -> 3 -> 1; neither ever builds an intermediate out of intermediates, because
    `generation` never reaches 1.

    The real object does. 279 GiB in 87 MiB chunks is **3283 parts**, which folds
    3283 -> 103 -> 4 -> 1: three compose levels, and the middle one composes objects
    that are themselves composites. Everything that can only break there -- a name
    collision between generations, a carried-forward single that never gets composed,
    an ordering slip between levels, an intermediate from generation 0 deleted before
    generation 1 consumes it -- is invisible to the existing tests and would surface
    an hour into a full-size run, after the upload has already been paid for.

    Three levels begin above `32 * 32 = 1024` sources, so 1025 is the real boundary
    and none of the counts tested so far come near it.
    """

    # 279 GiB / 87 MiB, the §6.3 object. Kept as the literal rather than derived, so
    # that a change to chunk sizing does not silently move this test off the case it
    # exists for.
    FULL_SIZE_PARTS = 3283

    def seed(self, gcs, count):
        """`count` distinct single-component sources; returns (names, expected bytes)."""
        names, expected = [], b""
        for index in range(count):
            name = "p{:05d}".format(index)
            # Four digits so all 3283 payloads differ -- with a narrower field the
            # values wrap and a reordering between levels could reassemble to the
            # same bytes and pass.
            data = "{:04d}".format(index).encode()
            gcs.state.objects[name] = data
            gcs.state.composite[name] = 1
            names.append(name)
            expected += data
        return names, expected

    @staticmethod
    def generations(gcs, destination):
        """Which intermediate generations were actually built."""
        marker = destination + ".k9pdl.compose/"
        return {
            int(dest[len(marker):].split("-", 1)[0])
            for dest, _ in gcs.state.compose_calls if dest.startswith(marker)
        }

    def test_three_levels_at_the_full_size_part_count(self, gcs, monkeypatch):
        client = pdl.GcsClient()
        names, expected = self.seed(gcs, self.FULL_SIZE_PARTS)

        pdl.compose_tree(client, BUCKET, "out", names)

        assert self.generations(gcs, "out") == {0, 1}, (
            "3283 parts must fold through two intermediate generations before the "
            "final compose; only {} were built".format(self.generations(gcs, "out")))
        assert gcs.state.objects["out"] == expected

    def test_no_compose_call_exceeds_the_source_limit_at_depth(self, gcs, monkeypatch):
        """The limit is per call, so it has to hold on the intermediate levels too."""
        client = pdl.GcsClient()
        names, _ = self.seed(gcs, self.FULL_SIZE_PARTS)

        pdl.compose_tree(client, BUCKET, "out", names)

        oversized = [(dest, len(sources)) for dest, sources in gcs.state.compose_calls
                     if len(sources) > pdl.GCS_COMPOSE_MAX_SOURCES]
        assert oversized == [], "compose calls over the source limit: {}".format(oversized)
        assert len(gcs.state.compose_calls) > 32, "expected a tree, not a single level"

    def test_every_part_is_present_exactly_once_at_depth(self, gcs, monkeypatch):
        """
        Component count is the invariant that catches a part being dropped or counted
        twice without depending on the payload bytes. A tree that loses one part of
        3283 changes the object by four bytes in thirteen kilobytes.
        """
        client = pdl.GcsClient()
        names, _ = self.seed(gcs, self.FULL_SIZE_PARTS)

        pdl.compose_tree(client, BUCKET, "out", names)

        assert gcs.state.composite["out"] == self.FULL_SIZE_PARTS

    def test_intermediates_from_both_generations_are_cleaned_up(self, gcs, monkeypatch):
        """
        Cleanup is deferred to the end precisely because generation 1 reads generation
        0's output. Deleting eagerly would be correct-looking and wrong, and only at
        three levels is there a generation whose inputs are themselves intermediates.
        """
        client = pdl.GcsClient()
        names, _ = self.seed(gcs, self.FULL_SIZE_PARTS)

        pdl.compose_tree(client, BUCKET, "out", names)

        leftover = [n for n in gcs.state.object_names() if ".k9pdl.compose/" in n]
        assert leftover == [], "{} intermediates left behind, first: {}".format(
            len(leftover), leftover[0] if leftover else None)

    @pytest.mark.parametrize("count", [
        1,      # single source: compose still has to name the destination
        2,
        32,     # exactly one call
        33,     # first fold
        1024,   # exactly 32 groups of 32 -- still two levels
        1025,   # one more, and the third level appears
    ])
    def test_boundaries_around_the_source_limit(self, gcs, monkeypatch, count):
        client = pdl.GcsClient()
        names, expected = self.seed(gcs, count)

        pdl.compose_tree(client, BUCKET, "out", names)

        assert gcs.state.objects["out"] == expected
        assert gcs.state.composite["out"] == count
        assert all(len(s) <= pdl.GCS_COMPOSE_MAX_SOURCES
                   for _, s in gcs.state.compose_calls)
        assert [n for n in gcs.state.object_names() if ".k9pdl.compose/" in n] == []

    @pytest.mark.parametrize("count,levels", [
        (32, set()),        # no intermediates at all
        (33, {0}),
        (1024, {0}),        # 1024 -> 32 -> final: still one generation
        (1025, {0, 1}),     # 1025 -> 33 -> 2 -> final
    ])
    def test_the_third_level_starts_exactly_above_1024(self, gcs, monkeypatch,
                                                       count, levels):
        """
        Pins where the depth changes. 1024 and 1025 differ by one source and by a
        whole generation, and that is the boundary the existing tests sit 900 parts
        below.
        """
        client = pdl.GcsClient()
        names, _ = self.seed(gcs, count)

        pdl.compose_tree(client, BUCKET, "out", names)

        assert self.generations(gcs, "out") == levels

    def test_a_carried_single_survives_being_bubbled_through_levels(self, gcs,
                                                                    monkeypatch):
        """
        A group of one is carried forward uncomposed rather than wrapped in a pointless
        intermediate. At 1025 sources that carried part is the last one, and it is
        carried TWICE -- through generation 0 and again through generation 1 -- before
        reaching the final call. If the bubbling drops it, the object is short by
        exactly its own length and everything else still lines up.
        """
        client = pdl.GcsClient()
        names, expected = self.seed(gcs, 1025)

        pdl.compose_tree(client, BUCKET, "out", names)

        assert gcs.state.objects["out"].endswith(b"1024")
        assert gcs.state.objects["out"] == expected
        assert gcs.state.composite["out"] == 1025

    @pytest.mark.parametrize("count", [1, 5, 1025, FULL_SIZE_PARTS])
    def test_the_destination_is_composed_exactly_once(self, gcs, monkeypatch, count):
        """
        The final call is unconditional -- a single remaining source still needs
        composing, since that is what gives the destination its name. It used to be
        written as a `len(level) > 1` test falling through to the same call when the
        result came back None, which read as two cases but was one, and would have
        composed twice for any client whose compose returned None. Pinned at every
        depth because the fall-through was only reachable on the single-source path.
        """
        client = pdl.GcsClient()
        names, _ = self.seed(gcs, count)

        pdl.compose_tree(client, BUCKET, "out", names)

        final = [sources for dest, sources in gcs.state.compose_calls if dest == "out"]
        assert len(final) == 1, "destination composed {} times".format(len(final))

    def test_a_max_sources_that_cannot_make_progress_is_rejected(self, gcs,
                                                                 monkeypatch):
        """
        `max_sources=1` makes every group a single, every single is carried forward
        unchanged, and the next level is identical to the last -- an infinite loop with
        no requests in flight, so it presents as a hang rather than a failure. The
        parameter exists to be varied, and a suite that cannot safely probe it is not
        testing it.
        """
        client = pdl.GcsClient()
        names, _ = self.seed(gcs, 40)

        with pytest.raises(ValueError):
            pdl.compose_tree(client, BUCKET, "out", names, max_sources=1)


# ---------------------------------------------------------------------------
# verification (§11: check_hash is absolute)
# ---------------------------------------------------------------------------

class TestBucketVerification:

    def test_md5_mismatch_fails_and_writes_no_marker(self, tmp_path, monkeypatch, gcs,
                                                    payload):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5="0" * 32))
        assert rc == pdl.EXIT_FAIL
        _, marker_path = pdl.sidecar_paths(dest)
        assert not os.path.exists(marker_path)

    def test_md5_mismatch_removes_the_composed_object_and_parts(
        self, tmp_path, monkeypatch, gcs, payload
    ):
        """A destination that failed verification must not be left for a consumer."""
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5="0" * 32))
        assert OBJECT not in gcs.state.objects
        assert [n for n in gcs.state.object_names() if ".k9pdl.parts/" in n] == []

    def test_verification_reads_the_composed_object_back(
        self, tmp_path, monkeypatch, gcs, payload, payload_md5
    ):
        """
        A composite object has no md5Hash of its own, so the stored metadata cannot be
        used. The read-back is the accepted cost of keeping check_hash absolute.
        """
        client = pdl.GcsClient()
        gcs.state.objects["obj"] = payload
        gcs.state.composite["obj"] = 4
        metadata = client.get_object(BUCKET, "obj")
        assert "md5Hash" not in metadata, "fake should model a composite object"

        options = pdl.build_parser().parse_args(
            ["--dest", "/x", "--url", "http://x/", "--check-md5", payload_md5])
        assert pdl.verify_bucket_object(
            client, BUCKET, "obj", len(payload), options) == payload_md5

    def test_read_back_detects_corruption(self, gcs, payload, payload_md5):
        client = pdl.GcsClient()
        gcs.state.objects["obj"] = payload[:-1] + bytes([payload[-1] ^ 0xFF])
        gcs.state.composite["obj"] = 4
        options = pdl.build_parser().parse_args(
            ["--dest", "/x", "--url", "http://x/", "--check-md5", payload_md5])
        with pytest.raises(pdl.PermanentError, match="md5 mismatch"):
            pdl.verify_bucket_object(client, BUCKET, "obj", len(payload), options)

    def test_etag_verification_is_refused_rather_than_skipped(
        self, tmp_path, monkeypatch, gcs, payload
    ):
        """
        A multipart ETag cannot be checked against a composed object. Per the invariant
        that an inability to verify is a hard failure, this must not silently pass.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_etag="abc-3", part_length=MIB))
        assert rc == pdl.EXIT_FAIL
        _, marker_path = pdl.sidecar_paths(dest)
        assert not os.path.exists(marker_path)


class TestGcsObjectMd5:

    def test_converts_base64_to_hex(self):
        assert pdl.gcs_object_md5({"md5Hash": "1B2M2Y8AsgTpgAmY7PhCfg=="}) == \
            "d41d8cd98f00b204e9800998ecf8427e"

    def test_composite_object_has_none(self):
        assert pdl.gcs_object_md5({"componentCount": 4}) is None

    def test_malformed_yields_none(self):
        assert pdl.gcs_object_md5({"md5Hash": "!!!not base64!!!"}) is None


class TestManifestLivesInTheBucket:
    """
    the bucket-compose route exists for a bucket destination, so its manifest must not be committed through
    the mount. The filesystem manifest writes a temp file and renames, but on a
    flat-namespace bucket rename is a server-side copy followed by a delete -- not atomic.
    A single media upload gives the property directly: the object appears whole or not at
    all.

    The consequence of getting this wrong is bounded rather than corrupting -- a torn
    manifest fails to parse, loads as None, and forces a clean restart -- but that costs
    re-uploading every part, and this is the one route where the filesystem cannot be
    trusted to avoid it.
    """

    def test_the_manifest_is_a_bucket_object(self, tmp_path, monkeypatch, gcs, payload,
                                             payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            # interrupt so the manifest is still present at the end
            gcs.state.uploads_seen = 0
            gcs.state.fail_uploads_after = 2
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5, retries=0))
            gcs.state.fail_uploads_after = None

        assert read_manifest(gcs) is not None
        assert MANIFEST_OBJECT in gcs.state.objects

    def test_nothing_is_written_to_the_mount(self, tmp_path, monkeypatch, gcs, payload,
                                             payload_md5):
        """The VM is a pure relay on this route; the manifest is not an exception."""
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            gcs.state.uploads_seen = 0
            gcs.state.fail_uploads_after = 2
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5, retries=0))
            gcs.state.fail_uploads_after = None

        assert list(tmp_path.iterdir()) == [], \
            "wrote to the destination filesystem: {}".format(
                [p.name for p in tmp_path.iterdir()])

    def test_it_is_committed_without_a_temp_name_or_rename(self, gcs):
        """
        Staging then renaming is exactly what does not work here, so the GCS manifest has
        no staging path at all -- asking for one is a programming error rather than a
        silent fallback to the unsafe pattern.
        """
        manifest = pdl.GcsManifest.create(
            pdl.GcsClient(), BUCKET, "m.json", "pid", 100, 50, [(0, 50), (50, 100)]
        )
        with pytest.raises(AssertionError):
            manifest.tmp_path

    def test_a_torn_manifest_is_impossible_but_unparseable_still_restarts(self, gcs):
        """
        A partial object cannot be observed here, unlike the filesystem case. Unparseable
        content is nonetheless treated identically -- no usable resume state -- so the
        behavior does not depend on that guarantee holding.
        """
        client = pdl.GcsClient()
        gcs.state.objects["m.json"] = b'{"schema_version": 1, "plan'
        assert pdl.GcsManifest.load(client, BUCKET, "m.json") is None

    def test_a_wrong_schema_version_restarts(self, gcs):
        client = pdl.GcsClient()
        gcs.state.objects["m.json"] = b'{"schema_version": 999}'
        assert pdl.GcsManifest.load(client, BUCKET, "m.json") is None

    def test_an_absent_manifest_loads_as_none(self, gcs):
        assert pdl.GcsManifest.load(pdl.GcsClient(), BUCKET, "absent.json") is None

    def test_records_survive_a_round_trip(self, gcs):
        """
        Only persistence is overridden; the record-keeping is inherited, so the two
        manifests cannot drift in what they record.
        """
        client = pdl.GcsClient()
        manifest = pdl.GcsManifest.create(
            client, BUCKET, "m.json", "pid", 100, 50, [(0, 50), (50, 100)]
        )
        manifest.record_session(0, "https://upload/session/7")
        manifest.record_part_digest(0, "d41d8cd98f00b204e9800998ecf8427e")
        manifest.record_chunk_done(0, None)

        reloaded = pdl.GcsManifest.load(client, BUCKET, "m.json")
        assert reloaded.session_uri(0) == "https://upload/session/7"
        assert reloaded.chunk_record(0)["md5"] == "d41d8cd98f00b204e9800998ecf8427e"
        assert reloaded.is_complete(0)
        assert not reloaded.is_complete(1)

    def test_a_failed_manifest_write_does_not_fail_the_transfer(self, gcs, monkeypatch):
        """
        Losing an update costs re-uploading parts, never correctness -- the same rule as
        the filesystem manifest, but the errors here are the client's, not OSError.
        """
        client = pdl.GcsClient()
        manifest = pdl.GcsManifest.create(
            client, BUCKET, "m.json", "pid", 100, 50, [(0, 50), (50, 100)]
        )

        def boom(*args, **kwargs):
            raise pdl.TransientError("bucket unavailable")

        monkeypatch.setattr(client, "put_object", boom)
        manifest.record_chunk_done(0, None)   # must not raise

    def test_plan_id_identity_is_what_invalidates_it(self, gcs):
        """
        There is no local file on this route, so inode and size checks are meaningless;
        the plan identity is the whole test.
        """
        client = pdl.GcsClient()
        manifest = pdl.GcsManifest.create(
            client, BUCKET, "m.json", "pid", 100, 50, [(0, 50), (50, 100)]
        )
        assert manifest.matches("pid", None, 100)
        assert not manifest.matches("other", None, 100)
        assert not manifest.matches("pid", None, 200)

    def test_it_is_removed_on_success(self, tmp_path, monkeypatch, gcs, payload,
                                     payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            assert pdl.run(options_for(dest, source.url(), len(payload),
                                       check_md5=payload_md5)) == pdl.EXIT_OK
        assert MANIFEST_OBJECT not in gcs.state.objects

    def test_the_marker_still_lands_beside_the_destination(self, tmp_path, monkeypatch,
                                                           gcs, payload, payload_md5):
        """
        The manifest moves to the bucket but the completion marker must not: the emitted
        bash consults it on the next attempt, and that runs against the mount.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            pdl.run(options_for(dest, source.url(), len(payload),
                                check_md5=payload_md5))
        _, marker_path = pdl.sidecar_paths(dest)
        assert os.path.exists(marker_path)


class TestPerPartDigestComparison:
    """
    §4.7's cheap verification for a bucket destination: each uploaded part is a plain
    single-stream upload, so GCS reports its md5Hash, and comparing that against the md5
    computed in flight checks every byte against the source's own digest with no read-back.

    It was dead code. record_chunk_done replaced the chunk's record rather than merging
    into it, so the md5 written by record_part_digest was destroyed immediately afterwards
    and the comparison read None every time. These tests pin both halves: that the digest
    survives, and that a mismatch is actually caught.
    """

    def test_the_digest_survives_being_marked_done(self, gcs):
        """
        The specific regression. record_part_digest then record_chunk_done is the exact
        order BucketChunkSink.chunk_done uses.
        """
        client = pdl.GcsClient()
        manifest = pdl.GcsManifest.create(
            client, BUCKET, "m.json", "pid", 100, 50, [(0, 50), (50, 100)]
        )
        manifest.record_session(0, "https://upload/session/7")
        manifest.record_part_digest(0, "d41d8cd98f00b204e9800998ecf8427e")
        manifest.record_chunk_done(0, None)

        assert manifest.chunk_record(0)["md5"] == "d41d8cd98f00b204e9800998ecf8427e"
        assert manifest.session_uri(0) == "https://upload/session/7"
        assert manifest.is_complete(0)

    def test_the_same_holds_for_the_filesystem_manifest(self, tmp_path):
        """The bug was in the shared base class, so both routes were affected."""
        destination = tmp_path / "obj.bin"
        destination.write_bytes(b"\0" * 100)
        manifest = pdl.Manifest.create(
            str(tmp_path / "m.json"), "pid", 100, 50, [(0, 50), (50, 100)],
            os.stat(str(destination)), True, 0,
        )
        manifest.record_part_digest(0, "abc")
        manifest.record_chunk_done(0, None)
        assert manifest.chunk_record(0)["md5"] == "abc"
        assert manifest.is_complete(0)

    def test_a_recorded_digest_is_compared_before_compose(self, tmp_path, monkeypatch,
                                                          gcs, payload, payload_md5):
        """
        End to end: the digests are recorded during the transfer and the comparison runs.
        A successful download implies every part matched what GCS stored.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        recorded = {}
        real = pdl.gcs_object_md5

        def spy(metadata):
            value = real(metadata)
            recorded[metadata.get("name")] = value
            return value

        monkeypatch.setattr(pdl, "gcs_object_md5", spy)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
        assert rc == pdl.EXIT_OK
        assert recorded, "the per-part comparison never ran"
        assert all(v is not None for v in recorded.values()), \
            "GCS reported no md5 for a part, so nothing was compared"

    def test_a_part_whose_md5_disagrees_fails_before_compose(self, tmp_path, monkeypatch,
                                                             gcs, payload, payload_md5):
        """
        The point of the comparison. If it were still dead, this would compose and pass.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        monkeypatch.setattr(pdl, "gcs_object_md5", lambda metadata: "0" * 32)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
        assert rc == pdl.EXIT_FAIL
        assert OBJECT not in gcs.state.objects, "composed despite a part mismatch"


# ---------------------------------------------------------------------------
# nothing is written in place
# ---------------------------------------------------------------------------

class TestNothingIsWrittenInPlace:
    """
    the bucket-compose route must never touch a local file at the destination.

    This is the property the route exists for: a full-size ftruncate or an out-of-order
    pwrite is exactly what makes a FUSE object store materialize gigabytes of zeros and
    re-upload the whole object per write. Asserted here on a run that SUCCEEDS -- an
    earlier version of this test forced the route with no GCS behind it and checked the
    property on a failed attempt, which proves much less: a run that dies early has not
    had the chance to write in place yet.
    """

    def test_no_ftruncate_or_pwrite_anywhere_on_a_successful_run(
            self, tmp_path, monkeypatch, gcs, payload, payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")

        calls = []
        real_ftruncate, real_pwrite = pdl.os.ftruncate, pdl.os.pwrite

        # Recorded and then genuinely performed, so an unexpected call fails on the
        # assertion below rather than as some confusing downstream symptom.
        def spy_ftruncate(fd, length):
            calls.append(("ftruncate", length))
            return real_ftruncate(fd, length)

        def spy_pwrite(fd, data, offset):
            calls.append(("pwrite", offset, len(data)))
            return real_pwrite(fd, data, offset)

        monkeypatch.setattr(pdl.os, "ftruncate", spy_ftruncate)
        monkeypatch.setattr(pdl.os, "pwrite", spy_pwrite)

        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))

        assert rc == pdl.EXIT_OK
        assert calls == [], "wrote in place on the bucket route: {}".format(calls)
        assert gcs.state.objects.get(OBJECT) == payload

    def test_the_destination_path_is_never_created_on_the_mount(
            self, tmp_path, monkeypatch, gcs, payload, payload_md5):
        """
        Not even an empty file. The bytes go to the bucket, and the only thing allowed
        beside the destination is the done marker.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
        assert rc == pdl.EXIT_OK
        assert not os.path.exists(dest), "created the destination file on the mount"
        leftover = [n for n in os.listdir(str(tmp_path)) if "k9pdl" not in n]
        assert leftover == [], "left files on the mount: {}".format(leftover)


class TestAnUnreadableManifestIsNotFatal:
    """
    Failing to READ the resume state costs re-uploading parts, never correctness -- the
    same rule flush() follows for failing to write it.

    This asymmetry was a real bug: load() happens before the downloader's own error
    handling, so a raised TransientError escaped to main()'s catch-all and exited
    do-not-retry, turning a transient GCS blip into a permanently failed job.
    """

    def test_a_transient_read_failure_starts_fresh_and_succeeds(
            self, tmp_path, monkeypatch, gcs, payload, payload_md5):
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")

        def unreadable(self, bucket, name):
            raise pdl.TransientError("GET {}: 503".format(name))

        monkeypatch.setattr(pdl.GcsClient, "read_object", unreadable)
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5,
                                     upload_block=pdl.GCS_UPLOAD_GRANULARITY))
        assert rc == pdl.EXIT_OK, "an unreadable manifest should not fail the transfer"
        assert gcs.state.objects.get(OBJECT) == payload

    def test_it_does_not_exit_do_not_retry(self, tmp_path, monkeypatch, gcs, payload):
        """
        The specific regression: exit 1 is canine's do-not-retry, so this must never be
        the way a manifest read failure surfaces.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")

        def unreadable(self, bucket, name):
            raise pdl.TransientError("GET {}: 503".format(name))

        monkeypatch.setattr(pdl.GcsClient, "read_object", unreadable)
        with Server(payload) as source:
            rc = pdl.main([
                "--url", source.url(), "--dest", dest, "--size", str(len(payload)),
                "--connections", "4", "--min-chunk", str(MIB), "--retries", "2",
            ])
        assert rc != pdl.EXIT_FAIL


class TestTheMarkerRecordsWhichRouteWroteIt:
    """
    The marker is read before the route is chosen, so it has to say whether a local file
    should exist. Adding a presence check to the short-circuit without this broke the
    bucket route: it leaves no local file, so the check failed, the marker was ignored
    and the whole object was re-uploaded -- the regression showed up as exactly 2x the
    bytes.

    An absent `route` in an older marker is read as ROUTE_POSIX deliberately. A
    mis-applied presence check costs a redundant transfer; a skipped one reports success
    for a file that is not there.
    """

    def test_the_bucket_route_tags_its_marker(self, tmp_path):
        payload = json.loads(json.dumps({}))  # keep the import used consistently
        assert pdl.ROUTE_BUCKET == "bucket-compose"
        path = str(tmp_path / ".obj.k9pdl.done")
        pdl.write_done_marker(path, 1024, "plan", None, route=pdl.ROUTE_BUCKET)
        assert pdl.read_done_marker(path)["route"] == pdl.ROUTE_BUCKET

    def test_the_default_route_is_the_one_that_expects_a_file(self, tmp_path):
        path = str(tmp_path / ".obj.k9pdl.done")
        pdl.write_done_marker(path, 1024, "plan", None)
        assert pdl.read_done_marker(path)["route"] == pdl.ROUTE_POSIX

    def test_an_old_marker_without_a_route_is_treated_as_posix(self):
        """
        The safe direction: a pre-existing marker gets the presence check, so a missing
        file causes a re-download rather than a false success.
        """
        source = inspect.getsource(pdl.run)
        assert 'marker.get("route", ROUTE_POSIX)' in source, source[:0] or "default changed"


# ---------------------------------------------------------------------------
# two writers on one object (§13.48 step 3)
# ---------------------------------------------------------------------------

class TestTwoWritersOnOneObject:
    """
    Production can put two workers on the same bucket object at the same time, and
    nothing here has ever tested it.

    `LOCALIZATION.md` §3: a worker that loses the bucket-creation race waits
    `bucket_upload_wait_tries` x 60s for the winner -- a **one hour** ceiling by default
    -- then declares the claim `stale` and exits 5. A later worker reads `stale`,
    concludes the uploader died, and takes over. The measured full-size localization is
    **1.62 h**, so the winner is still uploading when that happens. For the `server_side`
    and `copy` upload kinds `-n` makes the overlap harmless; `s3://`/GDC inputs are
    `kind == "mount"`, which is this route, and here the only thing between two writers
    and a corrupt object is the shared state in the bucket:

      * one manifest object, `<object>.k9pdl.json`, holding per-chunk completion AND the
        resumable session URIs;
      * deterministic part names, `<object>.k9pdl.parts/NNNNN`.

    Identical part content makes last-writer-wins on a part harmless. The manifest is the
    hazard: it is read-modify-write with no generation precondition, so one writer can
    drop the other's completion records and -- worse -- hand back a session URI the other
    writer is actively uploading to.

    A node dying mid-upload produces the same overlap without any timeout involved, so
    this is worth knowing regardless of what `bucket_upload_wait_tries` is set to.

    The two writers get different local `dest` paths on purpose: in production they are
    different VMs with no shared filesystem, so only the bucket is common.
    """

    def _race(self, tmp_path, monkeypatch, gcs, payload, payload_md5, writers=2):
        force_bucket_route(monkeypatch)
        results, errors = [], []

        with Server(payload) as source:
            def worker(index):
                try:
                    results.append(pdl.run(options_for(
                        str(tmp_path / "w{}.bam".format(index)),
                        source.url(), len(payload),
                        check_md5=payload_md5, connections=4)))
                except BaseException as exc:          # noqa: BLE001 - reported, not swallowed
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(writers)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(180)
            assert not any(t.is_alive() for t in threads), "a writer hung"
            sent = source.state.snapshot()["sent"]

        # Without this the whole class could pass while never racing anything: if one
        # writer short-circuited on a marker, or finished before the other started, the
        # assertions below would hold trivially. Both writers fetching a full payload is
        # the evidence that two of them were really in the object at once -- measured at
        # 2.00 payloads every run.
        assert sent >= 1.5 * len(payload), (
            "writers did not overlap: source served {:.2f} payloads, so this measured "
            "a sequence, not a race".format(sent / len(payload)))
        assert len(results) == writers, "a writer produced no result"

        return results, errors

    def test_the_object_is_never_silently_wrong(self, tmp_path, monkeypatch, gcs,
                                                payload, payload_md5):
        """
        The one property that must hold however the race resolves. Either writer may
        fail -- requeue is a correct answer to losing a race -- but a writer that
        reports EXIT_OK is asserting the object is complete and correct, and any object
        present at the end must match the source.
        """
        results, errors = self._race(tmp_path, monkeypatch, gcs, payload, payload_md5)

        assert not errors, "writer raised: {!r}".format(errors[0])
        stored = gcs.state.objects.get(OBJECT)
        if pdl.EXIT_OK in results:
            assert stored is not None, "a writer returned EXIT_OK but there is no object"
        if stored is not None:
            assert hashlib.md5(stored).hexdigest() == payload_md5, (
                "composed object does not match the source")

    def test_no_part_is_left_at_the_wrong_length(self, tmp_path, monkeypatch, gcs,
                                                 payload, payload_md5):
        """
        Crossed resumable sessions show up here first: two writers appending into one
        session produce a part longer than its chunk. The pre-compose check is supposed
        to catch that, but it only runs for a writer that gets that far, so assert it
        directly against the bucket.
        """
        self._race(tmp_path, monkeypatch, gcs, payload, payload_md5)

        prefix = OBJECT + ".k9pdl.parts/"
        parts = {n: gcs.state.objects[n] for n in gcs.state.object_names()
                 if n.startswith(prefix)}
        oversized = {n: len(b) for n, b in parts.items() if len(b) > MIB}
        assert not oversized, "parts longer than one chunk: {}".format(oversized)

    def test_the_manifest_survives_as_valid_json(self, tmp_path, monkeypatch, gcs,
                                                 payload, payload_md5):
        """
        Two interleaved media uploads of the manifest must not leave a torn document --
        a half-written manifest is unreadable by the next resume, which turns a
        recoverable state into a full re-download.
        """
        self._race(tmp_path, monkeypatch, gcs, payload, payload_md5)

        body = gcs.state.objects.get(MANIFEST_OBJECT)
        if body is not None:
            json.loads(body.decode("utf-8"))

    def test_a_writer_that_loses_the_race_requeues_rather_than_failing(
            self, tmp_path, monkeypatch, gcs, payload, payload_md5):
        """
        Which writer wins is genuinely nondeterministic -- observed both [0, 0] and
        [5, 0] across runs, depending on whether the loser reaches its pre-compose
        check before the winner deletes the parts. Both are correct outcomes, and the
        object verified in every case.

        What must NOT vary is the *kind* of failure. canine reads these codes: 5 means
        requeue this shard, 15 means skip it, and **anything else nonzero is
        do-not-retry**. A loser is not a broken job -- it lost a race to a sibling that
        succeeded, which is the most retryable situation there is. If this ever returns
        EXIT_FAIL, a workflow dies where it should have requeued, and the trigger is a
        timing window nobody will reproduce on demand.
        """
        results, _ = self._race(tmp_path, monkeypatch, gcs, payload, payload_md5)

        allowed = {pdl.EXIT_OK, pdl.EXIT_REQUEUE}
        assert set(results) <= allowed, (
            "a writer returned a do-not-retry code: {} (allowed {})".format(
                results, sorted(allowed)))
        assert pdl.EXIT_OK in results, "nobody completed the object"

    # NOTE: the assertion above cannot be relied on to cover the loser branch. The
    # common outcome is [0, 0] -- both writers compose, because the loser reaches its
    # pre-compose check before the winner deletes the parts -- so the requeue path is
    # only sometimes taken. Making the loser return EXIT_FAIL was mutation-tested here
    # and SURVIVED six consecutive runs. The deterministic cover is the next test; this
    # one is kept because it exercises real concurrency, which that one does not.

    def test_parts_vanishing_before_compose_requeues(self, tmp_path, monkeypatch, gcs,
                                                     payload, payload_md5):
        """
        The loser's branch, driven directly instead of hoped for.

        This is what a take-over worker finds when it arrives after the winner has
        already composed and swept the parts: every upload succeeded, and then the
        parts are gone. canine reads the exit code -- 5 requeue, 15 skip, **anything
        else nonzero do-not-retry** -- and losing a race to a sibling that succeeded is
        the most retryable situation there is. EXIT_FAIL here kills a workflow that
        should simply have run again, on a timing window nobody can reproduce on demand.
        """
        force_bucket_route(monkeypatch)
        real_get_object = pdl.GcsClient.get_object

        def vanishing(self, bucket, name):
            if ".k9pdl.parts/" in name:
                raise pdl.PermanentError(
                    "404 no such object: {} (another writer composed first)".format(name))
            return real_get_object(self, bucket, name)

        monkeypatch.setattr(pdl.GcsClient, "get_object", vanishing)

        with Server(payload) as source:
            rc = pdl.run(options_for(str(tmp_path / "loser.bam"), source.url(),
                                     len(payload), check_md5=payload_md5))

        assert rc == pdl.EXIT_REQUEUE, (
            "a writer whose parts were swept must requeue (5), got {}".format(rc))

    def test_exactly_one_object_results_and_the_parts_are_cleaned_up(
            self, tmp_path, monkeypatch, gcs, payload, payload_md5):
        """
        Two writers must not leave two objects' worth of storage behind. Parts are
        charged for until deleted, and a 279 GiB localization's parts are another
        279 GiB -- so a race that leaks them doubles the bill silently.
        """
        self._race(tmp_path, monkeypatch, gcs, payload, payload_md5)

        leftover = [n for n in gcs.state.object_names() if ".k9pdl.parts/" in n]
        assert leftover == [], "{} parts left behind after the race".format(len(leftover))


# ---------------------------------------------------------------------------
# which identity writes
# ---------------------------------------------------------------------------

class TestTheClientAuthenticatesAsADC:
    """
    The credential order decides *who* every bucket write is attributed to, and it had
    no test at all.

    It used to try the metadata server first, reasoning that workers are GCE VMs and it
    saves a subprocess. The metadata server always answers on GCE, so it always won, and
    every write went out as the **compute service account** while the rest of the same
    job ran as the user credentials `docker_copy_gcloud_credentials.sh` stages.

    canine pins ADC explicitly in two other places for precisely this reason
    (`base.py`'s bucket-mount block, `dockerTransient.py`'s rclone path), and the comment
    there names the failure: the identity "silently works in one project and fails in
    another". Both outcomes are quiet -- a 403 that reads as a bucket problem, or success
    with an audit trail that does not match the workflow.
    """

    def _runner(self, ok=(), token="tok"):
        """Fake subprocess.run where only `ok` command prefixes succeed."""
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            joined = " ".join(command)
            if any(joined.startswith(prefix) for prefix in ok):
                return subprocess.CompletedProcess(
                    command, 0, (token + "\n").encode(), b"")
            return subprocess.CompletedProcess(command, 1, b"", b"denied")

        return run, calls

    def test_adc_is_preferred_over_everything(self, monkeypatch):
        run, calls = self._runner(ok=("gcloud auth application-default",), token="adc")
        watch = _MetadataWatch()
        monkeypatch.setattr(pdl.subprocess, "run", run)
        monkeypatch.setattr(pdl.urllib.request, "urlopen", watch)

        token, _ = pdl.GcsClient()._fetch_token()

        assert token == "adc"
        assert not watch.reached, "the metadata server was consulted anyway"
        assert calls[0] == ["gcloud", "auth", "application-default",
                            "print-access-token"]

    def test_the_metadata_server_is_not_consulted_when_adc_works(self, monkeypatch):
        """
        The actual regression. On GCE the metadata server always answers, so merely
        *listing* it first was enough to make it always win.
        """
        run, _ = self._runner(ok=("gcloud auth application-default",))
        watch = _MetadataWatch()
        monkeypatch.setattr(pdl.subprocess, "run", run)
        monkeypatch.setattr(pdl.urllib.request, "urlopen", watch)

        pdl.GcsClient()._fetch_token()

        assert not watch.reached, (
            "ADC succeeded but the metadata server was still consulted -- listing it "
            "first is all it takes, because on GCE it always answers")

    def test_it_falls_back_to_the_active_gcloud_account(self, monkeypatch):
        run, calls = self._runner(ok=("gcloud auth print-access-token",), token="acct")
        watch = _MetadataWatch()
        monkeypatch.setattr(pdl.subprocess, "run", run)
        monkeypatch.setattr(pdl.urllib.request, "urlopen", watch)

        token, _ = pdl.GcsClient()._fetch_token()

        assert token == "acct"
        assert not watch.reached
        assert calls[0][:4] == ["gcloud", "auth", "application-default",
                                "print-access-token"], "ADC must still be tried first"

    def test_the_metadata_server_is_the_last_resort_and_says_so(self, monkeypatch,
                                                                capsys):
        """
        Kept rather than removed -- a worker with no user credentials is a real
        configuration, and failing to authenticate is worse than authenticating as the
        SA. But it is announced, because it means the writes are not the user's.
        """
        run, _ = self._runner(ok=())            # neither gcloud path works
        monkeypatch.setattr(pdl.subprocess, "run", run)

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def read(self):
                return json.dumps({"access_token": "sa", "expires_in": 3600}).encode()

        monkeypatch.setattr(pdl.urllib.request, "urlopen",
                            lambda *a, **k: Response())

        token, _ = pdl.GcsClient()._fetch_token()

        assert token == "sa"
        assert "compute service account" in capsys.readouterr().err

    def test_no_source_at_all_is_a_permanent_error(self, monkeypatch):
        run, _ = self._runner(ok=())
        watch = _MetadataWatch()
        monkeypatch.setattr(pdl.subprocess, "run", run)
        monkeypatch.setattr(pdl.urllib.request, "urlopen", watch)

        with pytest.raises(pdl.PermanentError):
            pdl.GcsClient()._fetch_token()
        assert watch.reached, "the last resort must at least have been tried"


class _MetadataWatch:
    """
    Records whether the metadata server was reached, rather than raising.

    Raising does not work here: `_fetch_token`'s metadata branch is wrapped in
    `except Exception`, which swallows AssertionError along with everything else -- so a
    sentinel that raises is silently neutralised by the code it is watching. Caught by
    mutation-testing the original defect back in: four of five tests still passed.
    """

    def __init__(self):
        self.reached = False

    def __call__(self, *args, **kwargs):
        self.reached = True
        raise OSError("metadata server unavailable")


class TestTheADCLabelIsHonest:
    """
    `gcloud auth application-default print-access-token` **succeeds with no ADC file at
    all** -- resolution falls through to the GCE metadata server and gcloud returns a
    *service account* token, exit 0. So "the command worked" is not evidence the
    identity is yours.

    Observed on a real node: `CLOUDSDK_CONFIG=/user_gcloud_config` while the mounted user
    credentials sat in `/root/.config/gcloud/`. gcloud never saw them, the upload 403'd
    naming `...-compute@developer.gserviceaccount.com`, and every check run beforehand
    had passed -- including one written specifically to catch this, which only proved
    that *a* token could be minted.

    So the ADC branch is skipped unless a credentials file actually exists. The log line
    is the only way an operator learns which identity wrote their objects; it has to be
    true.
    """

    def _runner(self, ok=(), token="tok"):
        def run(command, **kwargs):
            joined = " ".join(command)
            if any(joined.startswith(prefix) for prefix in ok):
                return subprocess.CompletedProcess(
                    command, 0, (token + "\n").encode(), b"")
            return subprocess.CompletedProcess(command, 1, b"", b"denied")
        return run

    def test_adc_is_skipped_when_no_credentials_file_exists(self, monkeypatch, capsys):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        monkeypatch.setenv("CLOUDSDK_CONFIG", "/nonexistent-config")
        monkeypatch.setattr(pdl.os.path, "expanduser", lambda p: "/nonexistent-home")
        # ADC would "succeed" here, via the metadata server, and be reported as ADC.
        monkeypatch.setattr(pdl.subprocess, "run",
                            self._runner(ok=("gcloud auth application-default",
                                             "gcloud auth print-access-token"),
                                         token="sa-in-disguise"))
        watch = _MetadataWatch()
        monkeypatch.setattr(pdl.urllib.request, "urlopen", watch)

        pdl.GcsClient()._fetch_token()
        err = capsys.readouterr().err

        assert "no ADC credentials file found" in err
        assert "using ADC" not in err, (
            "reported ADC with no credentials file -- the label would be a lie")
        assert "using gcloud account" in err

    def test_adc_is_used_and_names_the_file_when_one_exists(self, tmp_path,
                                                            monkeypatch, capsys):
        creds = tmp_path / "application_default_credentials.json"
        creds.write_text("{}")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds))
        monkeypatch.setattr(pdl.subprocess, "run",
                            self._runner(ok=("gcloud auth application-default",)))
        monkeypatch.setattr(pdl.urllib.request, "urlopen", _MetadataWatch())

        pdl.GcsClient()._fetch_token()

        err = capsys.readouterr().err
        assert "using ADC" in err
        assert str(creds) in err, "name the file, so the identity is checkable"

    def test_cloudsdk_config_is_honoured(self, tmp_path, monkeypatch):
        """The real node's layout: ADC found via $CLOUDSDK_CONFIG, not the home dir."""
        config = tmp_path / "user_gcloud_config"
        config.mkdir()
        (config / "application_default_credentials.json").write_text("{}")
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        monkeypatch.setenv("CLOUDSDK_CONFIG", str(config))

        assert pdl.GcsClient.adc_file() == str(
            config / "application_default_credentials.json")

    def test_a_stale_GOOGLE_APPLICATION_CREDENTIALS_path_does_not_count(
            self, tmp_path, monkeypatch):
        """Set but pointing at nothing is the same as unset, and must not claim ADC."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "gone.json"))
        monkeypatch.setenv("CLOUDSDK_CONFIG", str(tmp_path / "also-gone"))
        monkeypatch.setattr(pdl.os.path, "expanduser", lambda p: "/nonexistent-home")

        assert pdl.GcsClient.adc_file() is None


# ---------------------------------------------------------------------------
# multipart ETag verification on the bucket route
# ---------------------------------------------------------------------------

class TestBucketRouteVerifiesAMultipartETag:
    """
    This route used to refuse ETag sources outright -- `EXIT_FAIL`, "ETag verification
    is not available for a composed object" -- which meant the real workload could not
    be localized to a bucket at all. A 279 GiB S3 object has a multipart ETag and no
    whole-file md5, so it would transfer for hours and then fail at compose.

    `verify()` could not be reused: it takes a local file path and re-reads it, and the
    whole point of this route is that no bytes touch local disk. So the digests are
    accumulated in flight instead, the same way #19 does on the in-place route, and
    assembled afterwards.

    Note what the ETag alone does and does not prove. It establishes that the bytes
    *relayed* match the source; that GCS stored those bytes is a separate check, run
    before compose, comparing each part's recorded md5 to what GCS reports. The pair is
    what removes the read-back -- which is why these tests also pin that a corrupted
    upload is still caught.
    """

    PART = MIB          # pretend S3 part length
    CHUNK = 2 * MIB     # a chunk spans two parts, as it does in production

    @staticmethod
    def expected_etag(payload, part_length):
        parts = [payload[i:i + part_length]
                 for i in range(0, len(payload), part_length)]
        joined = b"".join(hashlib.md5(p).digest() for p in parts)
        return "{}-{}".format(hashlib.md5(joined).hexdigest(), len(parts))

    def run(self, tmp_path, monkeypatch, gcs, payload, etag=None, **overrides):
        force_bucket_route(monkeypatch)
        with Server(payload) as source:
            return pdl.run(options_for(
                str(tmp_path / "sample.bam"), source.url(), len(payload),
                check_etag=etag or self.expected_etag(payload, self.PART),
                part_length=self.PART, min_chunk=self.CHUNK, **overrides))

    def test_a_multipart_etag_source_now_succeeds(self, tmp_path, monkeypatch, gcs,
                                                  payload, payload_md5):
        rc = self.run(tmp_path, monkeypatch, gcs, payload)
        assert rc == pdl.EXIT_OK
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5

    def test_it_reads_nothing_back_on_the_happy_path(self, tmp_path, monkeypatch, gcs,
                                                     payload, capsys):
        """
        The entire reason for hashing in flight. A read-back of the composed object
        would be a second full transfer of it -- measured at 62% of the wall clock on
        the md5 path (§6.6a), which is what this avoids.
        """
        self.run(tmp_path, monkeypatch, gcs, payload)
        err = capsys.readouterr().err
        assert "0 re-read" in err, err.splitlines()[-1] if err else "(no output)"

    def test_a_wrong_etag_fails_rather_than_passing(self, tmp_path, monkeypatch, gcs,
                                                    payload):
        bogus = "{}-{}".format("0" * 32, 7)
        rc = self.run(tmp_path, monkeypatch, gcs, payload, etag=bogus)
        assert rc == pdl.EXIT_FAIL

    def test_the_short_final_part_is_handled(self, tmp_path, monkeypatch, gcs):
        """The payload is deliberately not a multiple of the part length."""
        odd = os.urandom(3 * MIB + 7919)
        rc = self.run(tmp_path, monkeypatch, gcs, odd)
        assert rc == pdl.EXIT_OK
        assert gcs.state.objects[OBJECT] == odd

    def test_the_digests_are_per_s3_part_not_per_chunk(self, tmp_path, monkeypatch,
                                                       gcs, payload):
        """
        The keyspace trap, asserted directly. A chunk is one uploaded GCS part but
        TWO S3 parts here, so an implementation that recorded one digest per chunk
        would assemble an ETag with half the part count -- and would still look like a
        working ETag. Verifying against a chunk-length ETag must therefore FAIL.
        """
        chunk_etag = self.expected_etag(payload, self.CHUNK)
        part_etag = self.expected_etag(payload, self.PART)
        assert chunk_etag != part_etag, "the fixture must distinguish the two"

        assert self.run(tmp_path, monkeypatch, gcs, payload,
                        etag=chunk_etag) == pdl.EXIT_FAIL
        assert self.run(tmp_path, monkeypatch, gcs, payload,
                        etag=part_etag) == pdl.EXIT_OK

    def test_an_etag_without_a_part_length_is_refused_not_assumed(
            self, tmp_path, monkeypatch, gcs, payload):
        """An opaque ETag is not an md5-of-md5s; there is nothing to reproduce."""
        force_bucket_route(monkeypatch)
        with Server(payload) as source:
            rc = pdl.run(options_for(
                str(tmp_path / "sample.bam"), source.url(), len(payload),
                check_etag="opaque-not-a-digest"))
        assert rc == pdl.EXIT_FAIL

    def test_digests_survive_a_preemption_and_only_the_gap_is_re_read(
            self, tmp_path, monkeypatch, gcs, payload):
        """
        Resume is the case the per-part fallback exists for. Digests recorded before
        the interruption must persist in the manifest, the parts straddling the resume
        point were never seen from byte zero so they are deliberately unrecorded, and
        those -- and only those -- are read back.

        All-or-nothing fallback would turn one preemption into a full re-download of a
        279 GiB object, which is the cost this whole mechanism exists to avoid.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        etag = self.expected_etag(payload, self.PART)

        with Server(payload) as source:
            gcs.state.uploads_seen = 0
            gcs.state.fail_uploads_after = 4
            try:
                first = pdl.run(options_for(
                    dest, source.url(), len(payload), check_etag=etag,
                    part_length=self.PART, min_chunk=self.CHUNK,
                    upload_block=pdl.GCS_UPLOAD_GRANULARITY, retries=0))
            finally:
                gcs.state.fail_uploads_after = None
            assert first != pdl.EXIT_OK, "the run was supposed to be interrupted"

            recorded = (read_manifest(gcs) or {}).get("part_md5", {})
            assert recorded, "no part digests survived the interruption"

            rc = pdl.run(options_for(
                dest, source.url(), len(payload), check_etag=etag,
                part_length=self.PART, min_chunk=self.CHUNK,
                upload_block=pdl.GCS_UPLOAD_GRANULARITY))

        assert rc == pdl.EXIT_OK
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == \
            hashlib.md5(payload).hexdigest()


class TestTheUploadBlockIsTunableAndBounded:
    """
    The bucket route sent one PUT per 256 KiB, which measured at 4.71 and 4.76 MiB/s per
    stream from two unrelated sources -- the GDC S3 endpoint and a GCS object. Agreeing
    to within 1% across sources is what identified the block, rather than the network,
    as the cap: a resumable PUT costs a round-trip whatever it carries, so the rate was
    simply 256 KiB per 53 ms.

    Raising it trades a real guarantee for throughput, so the guarantee is pinned here
    rather than left in a comment: what is bounded is bytes read from the source but not
    yet acknowledged by GCS, per in-flight chunk, and that bound IS the block size.
    """

    def test_the_default_is_large_enough_to_not_be_latency_bound(self):
        """
        At ~53 ms per PUT, 256 KiB gives 4.7 MiB/s per stream and ~72 MiB/s at 16
        connections -- below the 43.9 MiB/s pd-standard it is supposed to beat by
        enough to matter. The default has to be well clear of that regime.
        """
        assert pdl.DEFAULT_UPLOAD_BLOCK >= 4 * 1024 * 1024
        assert pdl.DEFAULT_UPLOAD_BLOCK % pdl.GCS_UPLOAD_GRANULARITY == 0

    def test_a_non_multiple_of_the_granularity_is_refused(self, gcs):
        """
        GCS rejects a non-final PUT that is not a multiple of 256 KiB. Failing at
        construction beats failing partway through a 279 GiB transfer.
        """
        with pytest.raises(pdl.PermanentError):
            pdl.BucketChunkSink(pdl.GcsClient(), BUCKET, "p", None, [],
                                upload_block=pdl.GCS_UPLOAD_GRANULARITY + 1)

    def test_a_bigger_block_issues_proportionally_fewer_puts(
            self, tmp_path, monkeypatch, gcs, payload, payload_md5):
        """
        The whole change, measured at the service rather than asserted from config: a
        PUT costs a round-trip whatever it carries, so throughput is request count, and
        request count is what has to fall.

        Counted by the fake, so this reflects what was actually sent. The first version
        of this test asserted `uploads_seen > 0`, which is true of every run that
        uploads anything at all -- it would have passed with the option ignored
        entirely.
        """
        force_bucket_route(monkeypatch)

        def puts_for(block, name):
            gcs.state.uploads_seen = 0
            with Server(payload) as source:
                rc = pdl.run(options_for(
                    str(tmp_path / name), source.url(), len(payload),
                    check_md5=payload_md5, upload_block=block, min_chunk=4 * MIB))
            assert rc == pdl.EXIT_OK, "block {} did not complete".format(block)
            assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5
            return gcs.state.uploads_seen

        small = puts_for(pdl.GCS_UPLOAD_GRANULARITY, "small.bam")   # 256 KiB
        large = puts_for(4 * MIB, "large.bam")                      # 16x bigger

        assert large < small / 4, (
            "16x the block should mean far fewer PUTs, got {} vs {}".format(
                large, small))

    def test_a_larger_block_still_resumes_correctly(self, tmp_path, monkeypatch, gcs,
                                                    payload, payload_md5):
        """
        The guarantee being traded. A preemption discards at most one block per
        in-flight chunk; whatever the block, the resumed attempt must still converge on
        the right bytes rather than leaving a gap.
        """
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            gcs.state.uploads_seen = 0
            gcs.state.fail_uploads_after = 3
            try:
                first = pdl.run(options_for(
                    dest, source.url(), len(payload), check_md5=payload_md5,
                    upload_block=MIB, retries=0))
            finally:
                gcs.state.fail_uploads_after = None
            assert first != pdl.EXIT_OK

            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5, upload_block=MIB))

        assert rc == pdl.EXIT_OK
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5


class TestTheManifestWriterBatches:
    """
    `commit` grew from 8% of wall at 12 GiB to 43% at full size -- 2.14x the chunks
    between the 96 GiB and 279 GiB runs but 7.2x the time, because each commit rewrites
    a manifest that now carries 9849 part digests as well as 3283 chunk records, and
    `mean batch 1.0` said the batching meant to amortise that never engaged.

    The writer took the whole queue the instant it woke, so batching only happened when
    chunks arrived *during* a commit. That is true on the in-place route, where an fsync
    is slow enough that 3283 chunks committed in 2 batches. On the bucket route a commit
    is ~0.23 s and completions are ~0.55 s apart, so it never was.

    The `NOT BATCHED` guard reported this on all three bucket runs and was twice
    explained away -- "nothing to batch at 4.7 MiB/s", then "it will fix itself when the
    upload block goes up". It was measuring a real property throughout, which is why
    these tests assert batch *size* rather than that a commit happened.
    """

    def _committer(self, linger, arrivals, gap, settle=0.0):
        """
        Drive the writer directly: `arrivals` completions `gap` apart, then wait
        `settle` before stopping.

        `settle` exists because without it the stop flag can be set before the writer
        has even entered the linger, so the outer guard skips the window and a test
        aimed at the stop path never reaches it -- which is how the first version of
        test_stopping_does_not_wait_out_the_linger passed with the stop check removed.
        """
        batches = []

        class Sink:
            def commit(self, batch):
                batches.append(len(batch))

        class Options:
            commit_linger = linger

        d = pdl.Downloader.__new__(pdl.Downloader)
        d.sink = Sink()
        d._writer = None
        d._writer_cv = threading.Condition()
        d._commit_seconds = 0.0
        d._commit_batches = 0
        d._commit_chunks = 0
        d._commit_linger = linger
        d._start_writer()
        try:
            for i in range(arrivals):
                d._enqueue_done(i, None)
                time.sleep(gap)
            time.sleep(settle)
        finally:
            d._stop_writer()
        return batches

    def test_without_linger_every_commit_is_one_chunk(self):
        """The observed behaviour, reproduced: arrivals slower than the commit."""
        batches = self._committer(linger=0, arrivals=6, gap=0.05)
        assert batches, "nothing was committed"
        assert max(batches) == 1, (
            "expected unbatched commits with no linger, got {}".format(batches))

    def test_with_linger_the_same_arrivals_coalesce(self):
        batches = self._committer(linger=0.5, arrivals=6, gap=0.05)
        assert max(batches) > 1, (
            "linger did not coalesce anything: {}".format(batches))
        assert sum(batches) == 6, "every completion must still be committed exactly once"

    def test_stopping_does_not_wait_out_the_linger(self):
        """
        Teardown must not pay the window. A long linger with a single arrival should
        still drain promptly, because _stop_writer sets the flag the wait checks.
        """
        started = time.monotonic()
        # settle: make sure the writer is genuinely inside the window before stopping.
        batches = self._committer(linger=30.0, arrivals=1, gap=0, settle=0.3)
        elapsed = time.monotonic() - started
        assert sum(batches) == 1
        assert elapsed < 10, "teardown waited on the linger: {:.1f}s".format(elapsed)

    def test_nothing_is_lost_or_duplicated_under_linger(self):
        batches = self._committer(linger=0.2, arrivals=25, gap=0.01)
        assert sum(batches) == 25
