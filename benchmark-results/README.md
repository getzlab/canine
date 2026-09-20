# §6.6 raw results — bucket-compose localization

The `--json` output behind every figure in `canine/test/BENCHMARK_RUNBOOK.md` §6.6 and
`update_localization.md` §13.50–§13.53. Kept because most of those conclusions rest on
comparing two runs, and a comparison is only checkable if both sides survive.

All from one `pdl-bench` (`n1-standard-8`, `us-east1-c`), source
`s3://dlbcl-misc-bucket/…RP-1329.bam` via the GDC jamboree endpoint unless noted,
destination a gcsfuse-mounted regional bucket. Node, disk and bucket have been deleted.

| file | run | headline |
|---|---|---|
| `routeb.json` | route-correctness gate, 12 GiB | `rc=0`, 192 parts composed — `bucket-compose` reached on a real RW gcsfuse mount |
| `fuse-96g.json` | 96 GiB, **GCS source**, 256 KiB block | 1574.6 s — relay 70.1 MiB/s |
| `fuse-s3-96g.json` | 96 GiB, GDC source, 256 KiB block | 1704.9 s — relay 64.4 MiB/s |
| `fuse-s3-96g-8m.json` | 96 GiB, GDC source, **8 MiB block** | 783.8 s — relay 146.4 MiB/s, the 2.27× |
| `fuse-300g.json` | **full 278.91 GiB**, verified | 2031.4 s — `hash ok`, `0 re-read` |
| `fuse-300g-batched.json` | full size, with commit batching | 2045.8 s — `commit` 43% → 8% of wall |
| `fuse-300g-prefetch.json` | full size, read/write prefetch | 1911.6 s — +6.2%, **reverted** (§13.53) |

Two pairs carry most of the argument:

* `fuse-s3-96g` vs `fuse-s3-96g-8m` — same source, same size, one constant apart. Per-worker
  upload 4.71 → 39.2 MiB/s, which is what identified the 256 KiB block rather than the
  network as the cap.
* `fuse-300g` and `fuse-300g-batched` — the n=2 baseline, 0.7% apart. That spread is what
  makes the prefetch run's −6.2% a result rather than noise.

Read `read_seconds` / `write_seconds` / `overlap_ceiling` together: §13.53 records that
the ceiling is an upper bound only when the two halves are independent, and on this route
they contend, so it overstated the achievable gain by 4×.
