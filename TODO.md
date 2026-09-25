# TODO

- Skip VM dispatch for localization when the data transfer is bucket-to-bucket (`gs://` source copied server-side) — no worker node should be needed for that case.
  - Caveat: a `gs://` object stored with `Content-Encoding: gzip` is *not* bucket-to-bucket. A server-side copy cannot decode it, so every reader of the bucket-mount would get the gzip stream (the GATK `.dict` failure). The planner routes such inputs to the node on purpose (`HandleGSURL.transport_gzip`, `update_localization.md` §13.71). Skipping dispatch is only safe when no input has `transport_gzip`.
- A localization upload that fails (or is cancelled) leaves its bucket labelled `wolf=working`, so the next job waits up to `bucket_upload_wait_tries` (default 60 × 60s = 1h) before taking over. Mark the bucket `wolf=stale` on upload failure so the next job takes over immediately.
