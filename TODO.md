# TODO

- Skip VM dispatch for localization when the data transfer is bucket-to-bucket (`gs://` source copied server-side) — no worker node should be needed for that case.
- A localization upload that fails (or is cancelled) leaves its bucket labelled `wolf=working`, so the next job waits up to `bucket_upload_wait_tries` (default 60 × 60s = 1h) before taking over. Mark the bucket `wolf=stale` on upload failure so the next job takes over immediately.
