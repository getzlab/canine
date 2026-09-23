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

---

## §6.6b raw results — `--gunzip` on the bucket route

From a second `pdl-bench` (`n1-standard-8`, **`us-east1-b`**, 2026-09-23), gcsfuse 3.11.2
on `slurm_gcp_docker:v0.18.3`. Source is a 170401724-byte gzip uploaded with
`contentEncoding: gzip`, decoding to 328888890 bytes (1.93:1). Node and bucket deleted.

| file | run | headline |
|---|---|---|
| `gunzip-decode.json` | the pass itself | relay 2.3 s, compose 0.1 s, **gunzip 11.6 s**, 19.78 s total. Destination md5 equals the original plaintext; `cmp` byte-identical |
| `gunzip-rerun.json` | same command again | **1.25 s**, `already complete per ...`, no transfer — `stored_size` works on real GCS |
| `gunzip-keep-as-is.json` | `.gz` name, singly compressed | 0.3 s decode phase, bytes kept, `gzip -t` valid, `cmp` byte-identical to the upload |
| `gunzip-nofallbackheader.json` | **the trap** — `Accept-Encoding: gzip` omitted | `fell_back: true`, **rc=0 in 8.76 s**, verified nothing, decoded nothing |

The two that carry the argument:

* `gunzip-decode` vs `gunzip-nofallbackheader` — same object, one header apart. Without
  `Accept-Encoding: gzip`, GCS returns **200 and ignores the Range**, the downloader falls
  back to a bare `curl` (the benchmark supplies no `--legacy-cmd`, so nothing verifies or
  decodes), and it **exits 0**. The output was correct only because GCS transcoded it on
  the way out. On this route a fallback is never a pass.
* `gunzip-decode`'s own phase split — the decode is **5.0× the relay** (11.6 s vs 2.3 s),
  because the relay is 16-way at 74.1 MB/s and the decode is one sequential stream at
  28.4 MB/s against what it writes. That ratio grows with the compression ratio, and it
  contradicts the "cheap second pass" framing the design note (§13.55) used before anyone
  measured it.

`phases` does not sum to `seconds` here: the md5 read-back is not wrapped in a `phase()`
on this route, which is the missing ~5.8 s.
