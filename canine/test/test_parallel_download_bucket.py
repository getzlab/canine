"""
Route B tests: parts uploaded through resumable sessions, composed server-side.

Route B runs in-process here rather than as a subprocess, because it has to be pointed
at a fake GCS endpoint. That means SIGKILL is not the instrument it is for Route A;
instead interruption is simulated at the storage layer, which is arguably closer to what
actually happens -- the resumability claim is about what GCS has durably persisted, and
the fake service models that at the real 256 KiB granularity.
"""

import hashlib
import json
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

        assert os.path.exists(manifest_path), "manifest should survive the failure"
        with open(manifest_path) as fh:
            state = json.load(fh)
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
            manifest_path, _ = pdl.sidecar_paths(dest)
            with open(manifest_path) as fh:
                state = json.load(fh)
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
