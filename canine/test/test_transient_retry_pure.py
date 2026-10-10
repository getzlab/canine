"""
Retrying transient source failures for a time budget, not a handful of tries.

Seen 2026-10-10: seven controllers started ~350 localizations from one GDC endpoint
together; it reset connections for minutes, and 302 of them failed. The downloader's
range probe gave up after six tries in ~45 s, and wolF's retries never reached a node:
HandleAWSURL's head-object, run when the task is built, failed on the first dropped
connection, three times a minute apart.
"""
import json
import subprocess
import time

import pytest

import canine.localization.file_handlers as fh
import canine.localization.parallel_download as pdl

MIB = 1024 * 1024


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(pdl.time, "monotonic", c.monotonic)
    monkeypatch.setattr(pdl.time, "sleep", c.sleep)
    return c


# ---------------------------------------------------------------------------
# the downloader's range probe
# ---------------------------------------------------------------------------

class _FlakySource:
    def __init__(self, failures, error=pdl.TransientError):
        self.failures = failures
        self.error = error
        self.probes = 0

    def probe_range(self, size):
        self.probes += 1
        if self.failures is None or self.probes <= self.failures:
            raise self.error("probe failed: [Errno 104] Connection reset by peer")

    def open_range(self, start, end):
        raise AssertionError("not reached: the route is forced to the legacy command")

    def refresh_url(self):
        return False


def _run(tmp_path, monkeypatch, source):
    monkeypatch.setattr(pdl, "build_source", lambda opts: source)
    # past the probe, take the single-stream command, so the test needs no server
    monkeypatch.setattr(pdl, "select_route",
                        lambda dest, **kw: pdl.RouteDecision(pdl.ROUTE_STAGED, "test"))
    marker = tmp_path / "ran"
    options = pdl.build_parser().parse_args([
        "--url", "http://127.0.0.1:1/obj", "--dest", str(tmp_path / "obj.bin"),
        "--size", str(8 * MIB), "--connections", "4", "--min-chunk", str(MIB),
        "--legacy-cmd", "touch {}".format(marker),
    ])
    return pdl.run(options), marker


class TestTheProbe:

    def test_rides_out_a_several_minute_outage(self, tmp_path, monkeypatch, clock):
        """Ten failures used to be well past the six tries it allowed."""
        source = _FlakySource(failures=10)
        rc, marker = _run(tmp_path, monkeypatch, source)
        assert rc == pdl.EXIT_OK and marker.exists()
        assert source.probes == 11
        assert clock.now > 60, "the outage it rode out should outlast the old ~45 s"

    def test_gives_up_within_its_budget(self, tmp_path, monkeypatch, clock):
        source = _FlakySource(failures=None)
        rc, marker = _run(tmp_path, monkeypatch, source)
        assert rc == pdl.EXIT_FAIL and not marker.exists()
        assert clock.now <= pdl.PROBE_RETRY_BUDGET
        assert clock.now > pdl.PROBE_RETRY_BUDGET / 2, "should use most of its budget"
        assert max(clock.sleeps) <= pdl.PROBE_MAX_DELAY * 1.5

    def test_a_permanent_failure_is_not_retried(self, tmp_path, monkeypatch, clock):
        source = _FlakySource(failures=None, error=pdl.PermanentError)
        rc, _ = _run(tmp_path, monkeypatch, source)
        assert rc == pdl.EXIT_FAIL
        assert source.probes == 1 and clock.sleeps == []


# ---------------------------------------------------------------------------
# HandleAWSURL's head-object, on the controller
# ---------------------------------------------------------------------------

HEAD = {"ContentLength": 1234, "ETag": '"0123456789abcdef0123456789abcdef"'}
SECRET = "SECRETKEYdoNotLog123"
FAKE_KEYS = {"aws_access_key_id": "AKIDEXAMPLE", "aws_secret_access_key": SECRET}


def _proc(rc, stdout=b"", stderr=b""):
    return subprocess.CompletedProcess(args="", returncode=rc, stdout=stdout, stderr=stderr)

OK = _proc(0, json.dumps(HEAD).encode())
DROPPED = _proc(255, stderr=b'\nConnection was closed before we received a valid response '
                            b'from endpoint URL: "https://objstore.example/bucket/key".\n')
SLOW_DOWN = _proc(254, stderr=b"An error occurred (503) when calling the HeadObject operation: "
                              b"Service Unavailable")
FORBIDDEN = _proc(254, stderr=b"An error occurred (403) when calling the HeadObject operation: "
                              b"Forbidden")


@pytest.fixture
def aws(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(time, "monotonic", c.monotonic)
    monkeypatch.setattr(time, "sleep", c.sleep)
    warned = []
    monkeypatch.setattr(fh.canine_logging, "warning", warned.append)

    def build(*responses):
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            return responses[min(len(calls), len(responses)) - 1]
        monkeypatch.setattr(fh.subprocess, "run", run)
        handler = fh.HandleAWSURL("s3://bucket/key", **FAKE_KEYS)
        return handler, calls
    build.clock, build.warned = c, warned
    return build


class TestHeadObject:

    def test_a_dropped_connection_is_retried(self, aws):
        handler, calls = aws(DROPPED, DROPPED, OK)
        assert handler.headers["ContentLength"] == 1234
        assert len(calls) == 3

    def test_a_503_is_retried(self, aws):
        handler, calls = aws(SLOW_DOWN, OK)
        assert handler.headers["ContentLength"] == 1234 and len(calls) == 2

    def test_a_403_fails_at_once(self, aws):
        with pytest.raises(ValueError, match="permission"):
            aws(FORBIDDEN)
        assert aws.clock.sleeps == []

    def test_gives_up_within_its_budget(self, aws):
        with pytest.raises(ValueError, match="Connection was closed"):
            aws(DROPPED)
        assert fh.S3API_RETRY_BUDGET / 2 < aws.clock.now <= fh.S3API_RETRY_BUDGET

    def test_the_keys_are_never_logged(self, aws):
        aws(DROPPED, OK)
        assert aws.warned, "a retry should be logged"
        assert not any(SECRET in w for w in aws.warned)
