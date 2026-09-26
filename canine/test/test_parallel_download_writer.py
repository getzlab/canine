"""
The deferred manifest writer, and the one ordering rule everything else rests on.

Per-chunk completion used to cost three fsyncs and a rename, serialised on
Manifest._lock, and that was 683 of 982 worker-seconds on the 3283-chunk run -- the only
cost that grows with the CHUNK COUNT rather than the byte count. The fix batches those
commits onto a single writer thread. It does not make a commit cheaper; it makes the
NUMBER of commits independent of the number of chunks.

What it must not do is weaken the guarantee the per-chunk version provided:

    a chunk's `done` marker may only become durable after an fsync that covers that
    chunk's bytes.

Violating it is silent. A done marker that outruns its data means a resumed run skips a
chunk whose bytes were never committed, and the failure surfaces as a wrong hash at the
end of a several-hour download -- or not at all, if the caller trusts the marker. The
existing suite has no guard for it, because before the writer existed the ordering was a
straight-line consequence of Manifest.flush() fsyncing first. Now it is a property of the
snapshot-then-fsync protocol, so it is asserted directly.

Every assertion here is mutation-checked in both directions: it fires when the ordering is
broken, AND it stays quiet on the correct case. The second half is the one that was being
skipped, and it is how thirteen instrumentation defects in this effort got as far as they
did -- a guard that can only ever pass measures nothing.
"""

import json
import os
import threading
import time

import pytest

from canine.localization import parallel_download as pdl


MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# a Downloader wired to instrumented, in-process fakes
# ---------------------------------------------------------------------------

class FakeStream:
    def __init__(self, payload, offset, end, delay=0.0):
        self.buf = payload[offset:end]
        self.pos = 0
        self.delay = delay

    def read(self, n):
        if self.delay:
            time.sleep(self.delay)
        out = self.buf[self.pos:self.pos + n]
        self.pos += len(out)
        return out

    def close(self):
        pass


class FakeSource:
    def __init__(self, payload, delay=0.0):
        self.payload = payload
        self.delay = delay

    def open_range(self, offset, end):
        return FakeStream(self.payload, offset, end, self.delay)


class Options:
    def __init__(self, dest, size, connections, commit_linger=0):
        self.dest = dest
        self.size = size
        self.connections = connections
        self.retries = 2
        # No linger by default. This file tests the ORDERING rule -- that a chunk
        # finishing during an fsync waits for the next round -- which needs commits to
        # start promptly so there is an in-flight commit to finish during. With the
        # production 2 s window every arrival coalesced into one batch and
        # test_a_chunk_finishing_during_an_fsync_waits_for_the_next_round stopped
        # exercising its own case. Its guard caught that; the linger is tested for its
        # own sake in TestTheManifestWriterBatches.
        self.commit_linger = commit_linger


class Recorder:
    """
    Orders three kinds of event on one monotonic counter: pwrite, fsync, and the manifest
    being published.

    A single counter is the point. The question is not "did an fsync happen" -- one always
    does -- but whether, for each chunk, an fsync of the data fd sits BETWEEN that chunk's
    last write and the moment its done marker became durable. That is only answerable if
    all three are on the same timeline.

    The third event is anchored to the manifest's own bytes being written -- the single
    os.write of its JSON body inside _flush -- not to commit() or _flush() returning.
    Anchoring it to the return accepts a build whose data fsync runs at the END of _flush,
    after the marker is already on its way to disk, because that fsync still falls before
    the tick. That inversion was tried against this suite and passed, which is why the
    anchor moved.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.clock = 0
        self.writes = {}        # fd -> [(offset, length, tick), ...]
        self.fsyncs = {}        # fd -> [tick, ...]
        self.publishes = []     # (tick, {chunk index, ...} marked done as of that publish)

    def tick(self):
        with self.lock:
            self.clock += 1
            return self.clock

    def note_write(self, fd, offset, length):
        t = self.tick()
        with self.lock:
            self.writes.setdefault(fd, []).append((offset, length, t))

    def note_fsync(self, fd):
        t = self.tick()
        with self.lock:
            self.fsyncs.setdefault(fd, []).append(t)

    def note_publish(self, done_indices):
        t = self.tick()
        with self.lock:
            self.publishes.append((t, set(done_indices)))

    def first_done_tick(self, index):
        """The tick of the first published manifest that carried this chunk's done marker."""
        with self.lock:
            for t, done in self.publishes:
                if index in done:
                    return t
        return None

    def all_done(self):
        with self.lock:
            return set().union(*[d for _, d in self.publishes]) if self.publishes else set()

    def last_write_tick(self, fd, start, end):
        """The latest tick at which any byte in [start, end) was written to `fd`."""
        with self.lock:
            ticks = [t for offset, length, t in self.writes.get(fd, [])
                     if offset < end and offset + length > start]
        return max(ticks) if ticks else None

    def fsync_between(self, fd, after, before):
        with self.lock:
            return any(after < t < before for t in self.fsyncs.get(fd, []))


@pytest.fixture
def instrument(monkeypatch):
    rec = Recorder()

    real_pwrite = os.pwrite
    real_fsync = os.fsync
    real_write = os.write

    def pwrite(fd, buf, offset):
        n = real_pwrite(fd, buf, offset)
        rec.note_write(fd, offset, n)
        return n

    def fsync(fd):
        real_fsync(fd)
        rec.note_fsync(fd)

    def write(fd, buf):
        # The manifest is serialised with a single os.write of its whole JSON body; chunk
        # data goes through os.pwrite, so the two cannot be confused. Ticking HERE rather
        # than around _flush or commit() is the whole point -- see Recorder's docstring.
        # An earlier version of this fixture wrapped _flush and ticked after it returned,
        # which happily accepted a build with the data fsync moved to the END of _flush:
        # the fsync was still inside the wrapped call, so it still fell before the tick.
        n = real_write(fd, buf)
        try:
            state = json.loads(bytes(buf).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return n
        if isinstance(state, dict) and "chunks" in state:
            rec.note_publish(int(i) for i, record in state["chunks"].items()
                             if record.get("done"))
        return n

    monkeypatch.setattr(pdl.os, "pwrite", pwrite)
    monkeypatch.setattr(pdl.os, "fsync", fsync)
    monkeypatch.setattr(pdl.os, "write", write)
    return rec


def build(tmp_path, payload, chunk_size, connections=4, rec=None, delay=0.0,
          part_length=None, seek_hole=None):
    """A Downloader over an in-process source, writing to a real sparse file."""
    dest = str(tmp_path / "out.bin")
    size = len(payload)
    chunks = [(s, min(s + chunk_size, size)) for s in range(0, size, chunk_size)]
    if seek_hole is None:
        # Probed, never asserted. Claiming SEEK_HOLE on a filesystem that does not
        # really support it is precisely the silent-corruption case the probe exists for
        # -- every chunk reads as complete -- and hardcoding True here reproduced it on
        # APFS while the test was being written.
        seek_hole = pdl.probe_seek_hole(str(tmp_path))

    fd = os.open(dest, os.O_RDWR | os.O_CREAT, 0o644)
    os.ftruncate(fd, size)
    manifest = pdl.Manifest.create(
        str(tmp_path / "out.bin.k9pdl.json"), "plan", size, chunk_size, chunks,
        os.fstat(fd), seek_hole, 0)
    sink = pdl.PosixChunkSink(fd, manifest, chunks, dest,
                              part_length=part_length, size=size)

    downloader = pdl.Downloader(
        FakeSource(payload, delay), sink, manifest, chunks,
        Options(dest, size, connections), pdl.Progress(size))
    return downloader, sink, manifest, fd, dest, chunks


# ---------------------------------------------------------------------------
# the ordering rule
# ---------------------------------------------------------------------------

class TestDoneNeverOutrunsItsData:
    def test_every_done_marker_is_preceded_by_an_fsync_covering_its_chunk(
            self, tmp_path, instrument):
        """
        The rule, stated directly: for each chunk, an fsync of the data fd falls between
        that chunk's last write and the publication of the manifest that marks it done.
        """
        payload = os.urandom(8 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4, rec=instrument)
        try:
            downloader.run()
        finally:
            os.close(fd)

        assert instrument.publishes, "the manifest was never published"
        for index in range(len(chunks)):
            start, end = chunks[index]
            done_at = instrument.first_done_tick(index)
            assert done_at is not None, "chunk {} was never marked done".format(index)
            wrote = instrument.last_write_tick(fd, start, end)
            assert wrote is not None, "chunk {} was marked done but never written".format(index)
            assert instrument.fsync_between(fd, wrote, done_at), (
                "chunk {} was marked done at tick {} with no fsync of the data fd since "
                "its last write at tick {}".format(index, done_at, wrote))

    def test_the_guard_fires_when_the_fsync_is_removed(self, tmp_path, instrument):
        """
        Mutation check, direction one. Drop the data fsync from the commit and the
        assertion above must fail -- otherwise it is checking nothing.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4, rec=instrument)

        # commit without ever fsyncing the data fd
        def broken(batch):
            manifest.record_chunks_done([i for i, _ in batch], None)

        sink.commit = broken
        try:
            downloader.run()
        finally:
            os.close(fd)

        violations = 0
        for index in range(len(chunks)):
            start, end = chunks[index]
            done_at = instrument.first_done_tick(index)
            wrote = instrument.last_write_tick(fd, start, end)
            if done_at is None or wrote is None or \
                    not instrument.fsync_between(fd, wrote, done_at):
                violations += 1
        assert violations > 0, (
            "the ordering guard stayed quiet with the data fsync removed, so it is not "
            "measuring the ordering")

    def test_the_guard_fires_when_the_fsync_follows_the_publish(self, tmp_path,
                                                                instrument):
        """
        Mutation check, direction one again, for the subtler inversion: the fsync still
        happens, just after the marker is already durable.

        This is the failure a naive probe misses. Anchoring the marker's timestamp to
        commit() RETURNING would see the fsync inside the call, before the tick, and pass
        -- which is why the tick is taken at the manifest's rename instead.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4, rec=instrument)

        def inverted(batch):
            manifest.record_chunks_done([i for i, _ in batch], None)  # publish first
            os.fsync(sink.fd)                                          # ...then fsync

        sink.commit = inverted
        try:
            downloader.run()
        finally:
            os.close(fd)

        violations = 0
        for index in range(len(chunks)):
            start, end = chunks[index]
            done_at = instrument.first_done_tick(index)
            wrote = instrument.last_write_tick(fd, start, end)
            if done_at is None or wrote is None or \
                    not instrument.fsync_between(fd, wrote, done_at):
                violations += 1
        assert violations > 0, (
            "the guard accepted an fsync that happened AFTER the done marker was "
            "published, so it is measuring the call rather than the ordering")

    def test_the_guard_stays_quiet_on_a_slow_correct_run(self, tmp_path, instrument):
        """
        Mutation check, direction two -- the one that keeps getting skipped.

        A guard that fires on the broken case but also on correct ones is worse than no
        guard: it gets tuned until it stops firing, which is how it ends up measuring
        nothing. Reading slowly spreads writes across many commit rounds, which is exactly
        the interleaving most likely to produce a false positive.
        """
        payload = os.urandom(2 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, 128 * 1024, connections=8, rec=instrument,
            delay=0.002)
        try:
            downloader.run()
        finally:
            os.close(fd)

        for index in range(len(chunks)):
            start, end = chunks[index]
            done_at = instrument.first_done_tick(index)
            assert done_at is not None, index
            wrote = instrument.last_write_tick(fd, start, end)
            assert wrote is not None, index
            assert instrument.fsync_between(fd, wrote, done_at), (
                "false positive: chunk {} on a correct run".format(index))

    def test_a_chunk_finishing_during_an_fsync_waits_for_the_next_round(self, tmp_path):
        """
        The snapshot rule. An fsync covers the bytes written before it STARTED, so a chunk
        that completes while it is in flight is not covered by it and must not be marked
        done by the commit that fsync belongs to.

        Forced deterministically: the writer is held inside its first commit until a second
        chunk has been enqueued, then released. That second chunk must appear in a LATER
        batch, never retroactively in the one already in progress.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=1)

        entered = threading.Event()
        release = threading.Event()
        batches = []
        real_commit = sink.commit
        first = [True]

        def commit(batch):
            indices = [index for index, _ in batch]
            if first[0]:
                first[0] = False
                entered.set()
                release.wait(10)
            batches.append(indices)
            real_commit(batch)

        sink.commit = commit

        runner = threading.Thread(target=downloader.run)
        runner.start()
        assert entered.wait(10), "the writer never reached a commit"
        # give the single worker time to finish and enqueue more chunks while the
        # writer is parked inside the first commit
        time.sleep(0.5)
        release.set()
        runner.join(60)
        assert not runner.is_alive()
        os.close(fd)

        assert len(batches) >= 2, (
            "everything landed in one batch, so the in-flight case was not exercised: "
            "{}".format(batches))
        # nothing was retroactively added to the batch that was already committing
        assert batches[0] == [0], batches
        flat = [index for batch in batches for index in batch]
        assert sorted(flat) == list(range(len(chunks))), flat
        assert len(flat) == len(set(flat)), "a chunk was committed twice: {}".format(flat)


# ---------------------------------------------------------------------------
# batching actually happens
# ---------------------------------------------------------------------------

class TestTheDeferralAmortises:
    def test_a_slow_commit_produces_batches_larger_than_one(self, tmp_path):
        """
        The mechanism has to be observed, not assumed. If the writer is drained as fast as
        it is filled, every batch is one chunk, the commit count still scales with the
        chunk count, and nothing was bought -- a state indistinguishable from the fix
        working if you look only at elapsed time.
        """
        payload = os.urandom(8 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=8)

        real_commit = sink.commit

        def commit(batch):
            time.sleep(0.15)
            real_commit(batch)

        sink.commit = commit
        try:
            downloader.run()
        finally:
            os.close(fd)

        assert downloader._commit_batches < len(chunks), (
            "one batch per chunk: the commits were never amortised")
        mean = downloader._commit_chunks / float(downloader._commit_batches)
        assert mean > 1.0, mean
        assert downloader._commit_chunks == len(chunks)

    def test_the_worker_side_cost_excludes_the_commit(self, tmp_path):
        """
        k9pdl-bookkeeping is read as "share of the worker pool spent not moving bytes", and
        the whole claim of this change is that the number collapses. It only means that if
        the commit is genuinely off the workers.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4)

        real_commit = sink.commit

        def commit(batch):
            time.sleep(0.20)
            real_commit(batch)

        sink.commit = commit
        try:
            downloader.run()
        finally:
            os.close(fd)

        assert downloader._commit_seconds >= 0.20
        assert downloader._bookkeeping_calls == len(chunks)
        assert downloader._bookkeeping_seconds < 0.20, (
            "the commit leaked back onto the worker threads: {:.3f}s".format(
                downloader._bookkeeping_seconds))


# ---------------------------------------------------------------------------
# failure modes
# ---------------------------------------------------------------------------

class TestTheWriterCannotHangTheRun:
    def test_a_writer_that_dies_surfaces_as_an_error(self, tmp_path):
        """
        Must surface, not hang. The workers never block on the writer -- the queue is
        unbounded by design, so a dead writer cannot deadlock them -- which means the only
        way its death is noticed is at the join.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4)

        def exploding(batch):
            raise RuntimeError("writer died")

        sink.commit = exploding
        try:
            with pytest.raises(RuntimeError, match="writer died"):
                downloader.run()
        finally:
            os.close(fd)

    def test_a_dead_writer_leaves_no_false_done_markers(self, tmp_path):
        """
        The failure has to be safe as well as visible. Chunks whose commit never happened
        must be absent from the manifest, so the next attempt re-fetches them -- expensive,
        and the correct direction. Recording them would be silent corruption.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4)

        def exploding(batch):
            raise RuntimeError("writer died")

        sink.commit = exploding
        try:
            with pytest.raises(RuntimeError):
                downloader.run()
            for index in range(len(chunks)):
                assert not manifest.is_complete(index), index
        finally:
            os.close(fd)

    def test_a_worker_failure_still_joins_the_writer(self, tmp_path):
        """
        The join is in a `finally` for this case. A live writer outlasting run() would be
        rewriting the manifest while verify() and the done marker read it.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4)

        def boom(offset, end):
            raise pdl.PermanentError("source is gone")

        downloader.source.open_range = boom
        try:
            with pytest.raises(pdl.PermanentError):
                downloader.run()
        finally:
            os.close(fd)
        assert downloader._writer is None
        assert not any(t.name == "k9pdl-manifest" and t.is_alive()
                       for t in threading.enumerate())

    def test_the_writer_is_joined_before_run_returns(self, tmp_path):
        """
        verify(), finalize() and the done marker all run after run() returns and all read
        state the writer writes. None of them may race it.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4)

        real_commit = sink.commit

        def commit(batch):
            time.sleep(0.10)
            real_commit(batch)

        sink.commit = commit
        try:
            downloader.run()
            assert downloader._writer is None
            # every completed chunk is durable in the manifest by the time run() returns
            reloaded = pdl.Manifest.load(manifest.path)
            for index in range(len(chunks)):
                assert reloaded.is_complete(index), index
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# resume, where done markers are missing but the bytes are there
# ---------------------------------------------------------------------------

class TestResumeWithMissingDoneMarkers:
    def test_physically_complete_chunks_are_recognised_and_recorded(self, tmp_path):
        """
        The happy path that was never tested: a run interrupted between an fsync and its
        commit leaves chunks whose bytes are fully on disk and whose done markers are not.

        Batching widens that window -- it is now up to one commit round rather than one
        chunk -- so the frontier has to recognise those chunks from the file's own extents
        and the second run has to record them. Re-fetching them would be merely wasteful;
        the failure to guard against is the frontier mistaking a complete chunk for a
        partial one and writing over it.
        """
        payload = os.urandom(4 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(
            tmp_path, payload, MIB, connections=4)
        seek_hole = bool(manifest.state.get("seek_hole"))
        try:
            downloader.run()
        finally:
            os.close(fd)
        assert open(dest, "rb").read() == payload

        # forget every done marker, keep the bytes -- the post-fsync, pre-commit state
        manifest.state["chunks"] = {}
        manifest.flush(None)

        fd2 = os.open(dest, os.O_RDWR)
        sink2 = pdl.PosixChunkSink(fd2, manifest, chunks, dest, size=len(payload))
        progress = pdl.Progress(len(payload))
        second = pdl.Downloader(FakeSource(payload), sink2, manifest, chunks,
                                Options(dest, len(payload), 4), progress)
        try:
            second.run()
        finally:
            os.close(fd2)

        # Whatever the frontier decides, the file must be right. Re-fetching is merely
        # wasteful; the failure to guard against is a complete chunk being mistaken for a
        # partial one and half-overwritten.
        assert open(dest, "rb").read() == payload
        for index in range(len(chunks)):
            assert manifest.is_complete(index), index

        if seek_hole:
            # The strong property, available only where the extents can be read back:
            # nothing was transferred at all, because every chunk was recognised as
            # already on disk.
            assert progress.transferred == 0, (
                "{} bytes were re-fetched despite an intact file and a working "
                "SEEK_HOLE".format(progress.transferred))
        else:
            # On the checkpoint fallback the markers ARE the only record, so losing them
            # legitimately costs a re-download. Asserted rather than skipped so the
            # fallback's behaviour is pinned down too.
            assert progress.transferred == len(payload)


class TestDurationsSurviveAClockStep:
    """
    Every duration the downloader logs is measured on time.monotonic(). They were on
    time.time(), which NTP can step: a step back mid-transfer made every interval spanning
    it about -3600 s, and the logged concurrency, io split and commit share meaningless.
    """

    def test_a_wall_clock_step_back_does_not_corrupt_the_accounting(self, tmp_path, monkeypatch):
        real = pdl.time.time
        calls = [0]

        def stepped():
            calls[0] += 1
            return real() - (3600 if calls[0] > 3 else 0)   # steps back an hour, early on

        monkeypatch.setattr(pdl.time, "time", stepped)
        payload = os.urandom(8 * MIB)
        downloader, sink, manifest, fd, dest, chunks = build(tmp_path, payload, MIB,
                                                             connections=4)
        try:
            downloader.run()
        finally:
            os.close(fd)
        for name in ("_stream_seconds", "_read_seconds", "_write_seconds",
                     "_commit_seconds", "_bookkeeping_seconds"):
            value = getattr(downloader, name)
            assert 0 <= value < 600, "{} = {} after a clock step".format(name, value)

    def test_the_only_wall_clock_read_is_the_attempt_timestamp(self):
        """landed_during() compares against GCS's timeCreated, so that one must stay."""
        import inspect
        source = inspect.getsource(pdl)
        uses = [l.strip() for l in source.splitlines() if "time.time()" in l
                and not l.strip().startswith("#")]
        assert uses == ["attempt_started = time.time()"], uses
