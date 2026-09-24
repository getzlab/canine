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
| `gunzip-split-170m.json` | decode stage split, 1 member | read 2.420 / inflate 2.047 / write 6.145 s, ceiling 1.73×, pipe2 1.73× |
| `gunzip-split-852m-5member.json` | decode stage split, **5 members**, 852012410 → 1644444450 | read 26.080 / inflate 10.135 / write 29.077 s, ceiling 2.25×, pipe2 1.80× |

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

### The stage split (`gunzip-split-*.json`)

Added after the first §6.6b run, which measured the decode at 5× the relay but could not
say which stage to attack. Two sizes, because the answer turned out to depend on size:

| | read | inflate | write | ceiling | pipe2 | decode/relay |
|---|---|---|---|---|---|---|
| 170 MB, 1 member | 2.420 s (23%) | 2.047 s (19%) | 6.145 s (58%) | 1.73× | 1.73× | 4.2× |
| 852 MB, 5 members | 26.080 s (40%) | 10.135 s (16%) | 29.077 s (45%) | 2.25× | 1.80× | 7.9× |

**Not the relay's shape.** The relay was 95% write-bound with a 1.05× ceiling, which is
why its prefetch was reverted at −6.2%. This loop is balanced, so pipelining has real
headroom — and `pipe2` (one thread reading and inflating, another uploading) captures 80%
of it at the larger size and *all* of it at the smaller, where `read + inflate` is still
under `write`.

**The two ceilings differ because read halved, not because the file grew.** Per byte,
inflate went 160.7 → 162.3 MB/s (×1.01) and write 53.5 → 56.6 (×1.06); read went
70.4 → 32.7 (×0.46). Unresolved at n=1 per size — variance, a real size effect, or the
component count (3 parts vs 13). Do not quote the 2.25× as a size trend.

Both ceilings are upper bounds on non-independent stages: read and write share one NIC.

The decode/relay ratio **widens with size** — 4.2× to 7.9× — because the relay is 16-way
and scales while the decode is one sequential stream. §13.57's "5.0×" was one point on a
rising curve, not a constant.

### The read discriminator (`gunzip-readbench-grid.json`, `gunzip-warm-*.json`)

`d278e39` showed the decode's read stage halving between 170 MB and 852 MB, which made a
third pipeline thread look size-dependent. Three candidates were on the table: run
variance, a real size effect, and composite component count. **It is none of them.**

`gunzip-readbench-grid.json` — sequential 8 MiB `download_range` reads over a
size x component-count grid, 3 interleaved reps, via the real `GcsClient`:

| object | components | first read | steady (best of 3) |
|---|---|---|---|
| `src/big.gz` 852 MB | 1 (uploaded) | 87.1 | 87.1 MB/s |
| `big-c4` / `c13` / `c51` | 4 / 13 / 51 | 33.9 / 37.0 / 38.1 | 91.4 / 91.6 / 88.4 |
| `src/small.gz` 170 MB | 1 (uploaded) | 83.9 | 90.1 |
| `small-c1` / `c3` / `c11` | 1 / 3 / 11 | 38.5 / 40.5 / 40.4 | 95.5 / 89.6 / 93.2 |

Component count is flat, both cold and steady. Size is flat. What is **not** flat is the
first read of a freshly-written object: 33-40 MB/s against 88-96 steady, a ~2.4x penalty
that applies to every composed object and to none of the gcloud-uploaded ones. **The
decode always reads a sidecar it has just composed, so it always pays this.**

`gunzip-warm-*.json` — the same decode with and without `--md5`, i.e. with and without a
full verify read-back immediately before the decode's own read:

| run | read MB/s | inflate MB/s | write MB/s |
|---|---|---|---|
| small, md5 | 51.5 | 158.3 | 54.9 |
| small, no md5 | 51.6 | 159.7 | 48.3 |
| big, md5 | 28.8 | 156.2 | 55.4 |
| big, no md5 | 32.0 | 159.8 | 54.8 |

3.306 s vs 3.302 s on the small read: the verify pass does **not** warm the sidecar.
Inflate (156-160) and write (48-55) are flat across a 5x size range, now n=4.

Two corrections to earlier write-ups fall out. The 70.4 MB/s small-object read in
`gunzip-split-170m.json` was the outlier -- repeats give 51.5 and 51.6. And read/write
interference, which `d278e39` asserted as the reason to distrust the ceiling, is **not
established**: against the cold isolated baseline the decode's interleaved read is 19%
slower at 852 MB and 30% *faster* at 170 MB. Inconsistent in sign, so not a finding.

### Read-ahead (`gunzip-readahead-depth-sweep.json`, `gunzip-e2e-depth*.json`)

Depth sweep on never-read objects, five reps with the depth order **rotated** (an earlier
attempt ran depth 1 first in every rep, which charged it with the run-first penalty;
by-slot medians here are flat, 99.4-107.7, so order is not the effect):

| depth | median MB/s | range |
|---|---|---|
| 1 | 59.3 | 42.9-72.2 |
| **4** | **140.3** | **129.3-147.7** |
| 8 / 16 | 105.2 / 110.6 | both worse than 4 |

End to end at 852013000 -> 1644444450, depths alternated, all outputs byte-identical:

| depth | total | decode | blocked on read |
|---|---|---|---|
| 1 | 102.94 / 101.17 s | 67.8 / 68.8 s | 26.80 / 28.95 s |
| 4 | 72.12 / 72.37 s | 39.8 / 40.7 s | 0.273 / 0.241 s |

1.41x whole-run, 1.70x on the decode, 108x less time blocked on reads. Afterwards the
write is 72% of the decode and the 3-stage ceiling drops 2.23-2.35x -> 1.36-1.39x, so
this supersedes the pipelining case rather than adding to it.

### Sliced upload (`gunzip-upload-width-sweep.json`, `gunzip-e2e-w*.json`)

Standalone, 1.64 GB through K resumable sessions + compose, widths rotated:
one session **81.3 MB/s**, width 2 159.4, width 4 301.6, width 8 541.6.

End to end, widths alternated, all outputs the same length and the width-1 runs matching
the known md5:

| width | total | write stage | output |
|---|---|---|---|
| 1 | 73.63 / 74.38 s | 31.08 / 30.60 s | plain, md5Hash present |
| 4 | 56.10 / 54.35 s | 9.99 / 9.11 s | 50 components, no md5Hash |
| 8 | 53.85 s | 9.48 s | 50 components, no md5Hash |

Default is 4: at width 4 the upload is already faster than the inflater (155 MB/s), which
cannot be parallelised, so width 8 measures 1.5% better for twice the memory.

Note the in-situ upload runs ~170 MB/s against 301 standalone -- interleaving with the
inflater costs ~40%, the same gap isolated-vs-in-loop that the read grid showed.

### Verify read-back (`gunzip-e2e-verify-*.json`)

The same read-ahead applied to the md5 read-back, which was the largest single cost left
and had never been inside a `phase()`.

| run | readahead | total | relay | verify | gunzip |
|---|---|---|---|---|---|
| ra1 | 1 | 76.38 s | 6.5 | 22.0 | 44.4 |
| ra4 | 4 | 35.81 s | 5.4 | 6.1 | 21.4 |
| ra4b | 4 | 36.31 s | 5.6 | 6.7 | 21.2 |
| ra1b | 1 | 81.90 s | 6.7 | 26.6 | 45.4 |

Verify 3.8x (~35 -> ~133 MB/s, against a measured depth-4 ceiling of 140.3). Whole run
55.2 -> 36.1 s; cumulative with the read-ahead and sliced upload, **102.9 -> 36.1 s, 2.85x**.
