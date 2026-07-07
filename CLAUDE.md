# canine

HPC job runner that submits batches of jobs to SLURM clusters, handles GCS/NFS file localization, and manages compute backends (local, GCP transient Docker, etc.). Used by wolF as its execution layer; can also be used standalone via CLI or Python API.

GitHub: https://github.com/getzlab/canine

## Architecture

```
canine.Orchestrator
  ├── Backends  (how/where jobs run)
  │     ├── Local              — runs jobs on the current machine via SLURM
  │     ├── Remote             — SSH into existing SLURM controller
  │     ├── GCPTransient       — spins up ephemeral GCP SLURM cluster via slurm_gcp_docker
  │     ├── ImageTransient     — uses a prebuilt GCE image for the cluster
  │     └── DockerTransient    — local Docker-based SLURM (dev/testing)
  └── Adapters  (how job input is defined)
        ├── Manual             — direct dict of inputs
        └── FireCloud          — pulls inputs from Terra/FireCloud workspace

Localization (canine/localization/)
  ├── BatchedLocalizer  — default; batches GCS transfers
  ├── LocalLocalizer    — copy files locally
  └── NFSLocalizer      — uses NFS mount
```

**Key files:**
- `canine/orchestrator.py` — `Orchestrator` class; version string lives here (`version = 'x.y.z'`)
- `canine/backends/gcpTransient.py` — GCP ephemeral cluster backend (most commonly used)
- `canine/localization/base.py` — core localization logic, **heavy pandas use** — highest pandas migration risk
- `canine/localization/remote.py` — GCS ↔ cluster transfer logic

## Setup & Dev Installation

```bash
pip install -e .
pip install -r canine/test/requirements.txt  # adds: timeout-decorator
```

Install canine inside Docker skips `slurm_gcp_docker` (detected via `/.dockerenv`).

## Running Tests

### Pure unit tests (no cluster, no GCP credentials) — run locally

Regression baseline against the current production environment.

```bash
# One-time venv setup:
uv venv --python 3.9 .venv-test
uv pip install --python .venv-test/bin/python -r canine/test/requirements.txt

# Run (from the canine/ repo root):
.venv-test/bin/python -m pytest \
  canine/test/test_adapters.py \
  canine/test/test_file_handlers.py \
  canine/test/test_orchestrator_pure.py \
  canine/test/test_utils_pure.py \
  -v
```

Baseline: **138 passed** (Python 3.9, pandas 1.4.2).

### Legacy integration tests (requires Docker/SLURM)

```bash
# Full test suite with coverage
coverage run --source=canine \
  --omit='canine/backends/dummy/controller.py,*canine/test/*.py,canine/localization/delocalization.py,canine/__main__.py,canine/xargs.py' \
  -m unittest discover canine/test

# Without coverage
python -m unittest discover canine/test
```

Most tests in `canine/test/` can run without GCP credentials. The remote/NFS tests require a live cluster. `test_backend_dummy.py` uses the local Docker-based SLURM backend.

## Dependencies (Current → Target)

| Package | Current | Target | Upgrade Risk |
|---------|---------|--------|-------------|
| `pandas` | `==1.4.2` | `>=2.2` | **HIGH** — see pandas notes below |
| `tables` (PyTables/HDF5) | `>=3.6.1` | latest compatible | **MEDIUM** — verify Python 3.14 + NumPy 2.x support first |
| `numpy` | `>=1.18.0` | `>=2.0` | Medium — NumPy 2.x has C-API changes |
| `paramiko` | `>=2.5.0` | `>=2.5.0` | Low risk |
| `google-cloud-compute` | `>=1.6.1` | `>=1.6.1` | Low risk |
| `firecloud-dalmatian` | git commit `d39177e` | latest or new commit | Medium — pinned to commit |
| `slurm_gcp_docker` | `git+...@v0.17.0` | new tag post-upgrade | Tracks slurm_gcp_docker releases |
| `docker` | `>=4.1.0` | `>=4.1.0` | Low risk |
| `agutil` | `>=4.1.0` | `>=4.1.0` | Low risk |
| `hound` | `>=0.2.0` | `>=0.2.0` | Low risk |
| `port-for` | `>=0.4` | `>=0.4` | Low risk |

## CI

`.github/workflows/CI.yml` — runs on push/PR to `master`, matrix over Python **3.7** and **3.8** (both EOL). Update to Python **3.14** and modern action versions (`actions/checkout@v4`, `actions/setup-python@v5`).

## Python Version Constraint

`python_requires = ">3.7"` — update to `>=3.14` to match wolF.

## pandas 2.x Migration Notes

The most pandas-intensive code is in `canine/localization/base.py` and `canine/orchestrator.py`. Key breaking changes to audit:

- `DataFrame.append()` — **removed** in pandas 2.0. Replace with `pd.concat([df, new_row_df])`.
- `Index.is_monotonic` — **renamed** to `Index.is_monotonic_increasing`.
- Inplace operations on slices — pandas 2.0 enforces copy-on-write. Code that modifies a slice of a DataFrame expecting to modify the original will silently stop working.
- Integer NA — pandas 2.0 changed default integer dtype handling. `Int64` (nullable) vs `int64` behavior differs.
- `DataFrame.swaplevel()` and MultiIndex operations — minor behavior changes.

## tables / HDF5 Notes

`tables` is used for job state serialization in canine. Before starting the upgrade:
1. Confirm `tables >= 3.9` installs cleanly on Python 3.14 with NumPy 2.x.
2. If `tables` does not support Python 3.14 + NumPy 2.x, evaluate replacing HDF5 serialization with `joblib` or `pickle` + compression.
3. Do not upgrade Python/NumPy versions until this is resolved, as it can block the entire upgrade.

## Post-Upgrade Versioning

After completing the upgrade:
1. Bump the version in `canine/orchestrator.py` (e.g., `0.18.0`).
2. Create a new git tag (e.g., `v0.18.0`).
3. Update `wolF/setup.py` to pin to the new tag.
