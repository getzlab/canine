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
                                     check_md5=payload_md5))
        assert rc == pdl.EXIT_OK
        assert hashlib.md5(gcs.state.objects[OBJECT]).hexdigest() == payload_md5

    def test_no_local_file_is_created(self, tmp_path, monkeypatch, gcs, payload,
                                     payload_md5):
        """The VM is a pure relay on this route: bytes never land on a local disk."""
        force_bucket_route(monkeypatch)
        dest = str(tmp_path / "sample.bam")
        with Server(payload) as source:
            rc = pdl.run(options_for(dest, source.url(), len(payload),
                                     check_md5=payload_md5))
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
                                     check_md5=payload_md5))
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
        """
        gcs.state.uploads_seen = 0
        gcs.state.fail_uploads_after = succeed_uploads
        try:
            return pdl.run(options_for(dest, source.url(), len(payload),
                                       check_md5=payload_md5, connections=connections,
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
                                     check_md5=payload_md5))

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
                                     check_md5=payload_md5))
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
                                     check_md5=payload_md5))
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
                                     check_md5=payload_md5))

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
                                     check_md5=payload_md5))
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
                                     check_md5=payload_md5))
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
