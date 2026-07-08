# canine tests

## Pure unit tests (no cluster required)

These tests cover pure Python logic and can be run locally without a SLURM cluster,
GCP credentials, or Docker. Non-installable packages (`slurm_gcp_docker`, `dalmatian`,
etc.) are stubbed out automatically by `conftest.py` at the repo root.

### One-time setup

From the `canine/` repo root:

```bash
uv venv --python 3.9 .venv-test
uv pip install --python .venv-test/bin/python -r canine/test/requirements.txt
```

### Running

```bash
.venv-test/bin/python -m pytest \
  canine/test/test_adapters.py \
  canine/test/test_file_handlers.py \
  canine/test/test_orchestrator_pure.py \
  canine/test/test_utils_pure.py \
  -v
```

Expected baseline: **138 passed** (Python 3.9, pandas 1.4.2).

## Legacy integration tests

`test_orchestrator.py`, `test_utils.py`, and the other files in this directory are
integration tests that require a live SLURM cluster or Docker. Run them on a VM
with the full canine environment installed:

```bash
python -m unittest discover canine/test
```
