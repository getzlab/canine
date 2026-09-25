# canine tests

Test files fall into two groups by what they need to run.

## Tests that run locally

Everything in this directory except the seven files listed under the next heading. They
need no SLURM cluster and no Docker. Non-installable packages (`slurm_gcp_docker`,
`dalmatian`, etc.) are stubbed out automatically by `conftest.py` at the repo root.

### One-time setup

From the `canine/` repo root:

```bash
uv venv --python 3.14 .venv-test
uv pip install --python .venv-test/bin/python -r canine/test/requirements.txt
```

`requirements.txt` mirrors the dependencies in `pyproject.toml`, except for the two that
`conftest.py` stubs.

### Running

```bash
.venv-test/bin/python -m pytest canine/test -q \
  --ignore=canine/test/test_orchestrator.py \
  --ignore=canine/test/test_backend_dummy.py \
  --ignore=canine/test/test_backend_remote.py \
  --ignore=canine/test/test_localizer_batched.py \
  --ignore=canine/test/test_localizer_local.py \
  --ignore=canine/test/test_localizer_nfs.py \
  --ignore=canine/test/test_localizer_remote.py
```

Expected: **1622 passed, 1 skipped** (Python 3.14, pandas 3.0).

## Tests that need the Docker SLURM cluster

These seven files start a `DummySlurmBackend` (or run `canine --backend type:Dummy`),
a Docker SLURM cluster built from the `gcr.io/broad-cga-aarong-gtex/slurmind` image, and
fail without it:

* `test_orchestrator.py`
* `test_backend_dummy.py`, `test_backend_remote.py`
* `test_localizer_batched.py`, `test_localizer_local.py`, `test_localizer_nfs.py`,
  `test_localizer_remote.py`

The localizer files' module setup starts the cluster for every test in them, so a test
that does not need it belongs in a file that runs locally instead. For example, the
requester-pays gate tests are in `test_localizer_requester_pays_pure.py`, and each
localizer's script staging is checked in `test_script_staging.py`.

Run them where Docker can pull that image, with the full canine environment installed:

```bash
python -m unittest discover canine/test
```
