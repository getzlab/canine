"""
Is the inflate slowdown the GIL, or contention multiprocessing cannot fix?

The thread split measured inflate rising 11.3 -> 16.2s once uploads genuinely overlapped
it, and the write-up blamed the GIL. That was inference: the upload side is only ~50 HTTP
requests for 1.64 GB, and sendall and OpenSSL's AES both release the GIL, so it is not
obvious there is 5 seconds of held-GIL work to find.

Three arms, same inflate, same upload load:
  solo      inflate alone                     -> baseline
  threads   upload load in 4 THREADS          -> should reproduce the ~43% slowdown
  procs     identical load in 4 PROCESSES     -> the discriminator

procs close to solo  => the GIL is the mechanism, multiprocessing would pay.
procs close to threads => it is memory/CPU/NIC contention, which multiprocessing shares
just as much, and the idea is dead before it is built.
"""
import multiprocessing, os, sys, time, zlib
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/tmp/pdl")
import parallel_download as pdl

BUCKET = os.environ["FUSE_BUCKET"]
GZ = "/tmp/gz-big.gz"
SLICE = 32 * 1024 * 1024
TOTAL = 1644444450
WIDTH = 4


def upload_one(index):
    client = pdl.GcsClient(timeout=600)
    body = os.urandom(SLICE)
    # A UNIQUE name per slice. Rewriting one name in a loop hits GCS's ~1 write/sec
    # per-object limit and returns 429 -- which the real uploader never does, because
    # every slice already gets its own name.
    per = TOTAL // WIDTH
    sent = 0
    seq = 0
    names = []
    while sent < per:
        n = min(SLICE, per - sent)
        name = "gilprobe/{}-{}-{}".format(os.getpid(), index, seq)
        session = client.start_resumable_upload(BUCKET, name)
        client.upload_range(session, body[:n], 0, n)
        names.append(name)
        sent += n
        seq += 1
    for name in names:
        client.delete_object(BUCKET, name)


def inflate_once():
    """Decompress the whole object, discarding output. Pure CPU plus local reads."""
    decoder = pdl.GzipStreamDecoder()
    produced = 0
    mark = time.monotonic()
    with open(GZ, "rb") as fh:
        while True:
            block = fh.read(pdl.READ_BUFFER)
            if not block:
                break
            produced += len(decoder.feed(block))
    produced += len(decoder.flush())
    return time.monotonic() - mark, produced


def measure(label, spawn):
    workers = spawn() if spawn else []
    seconds, produced = inflate_once()
    for w in workers:
        w.join()
    print("{:<9} inflate {:6.2f}s  ({} bytes, {:.1f} MB/s)".format(
        label, seconds, produced, produced / seconds / 1e6), flush=True)
    return seconds


def threads():
    pool = ThreadPoolExecutor(max_workers=WIDTH)
    futures = [pool.submit(upload_one, i) for i in range(WIDTH)]

    class Waiter:
        def join(self):
            for f in futures:
                f.result()
            pool.shutdown()
    return [Waiter()]


def procs():
    started = []
    for i in range(WIDTH):
        p = multiprocessing.Process(target=upload_one, args=(i,))
        p.start()
        started.append(p)
    return started


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    results = {}
    for rep in range(2):
        for label, spawn in (("solo", None), ("threads", threads), ("procs", procs)):
            results.setdefault(label, []).append(measure(label, spawn))
    print("\n=== medians ===")
    solo = sorted(results["solo"])[0]
    for label in ("solo", "threads", "procs"):
        best = sorted(results[label])[0]
        print("{:<9} {:6.2f}s   vs solo {:.2f}x".format(label, best, best / solo))
    print("\nprocs ~= solo    -> GIL; multiprocessing would pay")
    print("procs ~= threads -> shared-resource contention; it would not")
