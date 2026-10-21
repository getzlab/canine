"""
Tests for the disk-autoresize daemon emitted into the localization script by
`AbstractLocalizer._disk_resize_daemon_lines`.

These are "emitted-script contract" tests: the daemon is a bash script generated on
the controller, written out on the compute node by a heredoc, and run there. Unit
testing the Python alone would miss the parts that actually break — heredoc escaping
(which values get baked in at file-write time vs. evaluated at run time) and the
resize arithmetic. So the tests below really do run bash, with `df`/`gcloud`/`sudo`
stubbed out.

Background: the localization disk is sized from the sum of input sizes with only a 5%
margin, and for a gzip-transcoded GCS object that estimate is the *compressed* byte
count while the object lands decompressed. Genomics text compresses 4-10x, so the
disk must be able to grow on demand. Before this daemon was hoisted out of the
scratch-disk-only branch, the localization disk got no daemon at all.
"""
import os
import shutil
import subprocess
import tempfile

import pytest

from canine.localization.base import AbstractLocalizer

SCRATCH_KW = {}  # defaults reproduce the long-standing scratch-disk tuning
LOCALIZATION_KW = dict(
    poll_interval_sec=5,
    min_free_pct=25,
    headroom_sec=60,
    grow_pct=125,
    min_grow_gb=20,
    max_grow_gb=100,
)

BAKED_ENV = {
    "GCP_TSNT_DISKS_DIR": "/mnt/rwdisks",
    "GCP_DISK_NAME": "canine-abc123",
    "CANINE_NODE_ZONE": "us-central1-a",
}


def emit(kind, **kw):
    """The lines as they appear inside localization.sh."""
    return AbstractLocalizer._disk_resize_daemon_lines(kind, **kw)


def write_daemon(tmpdir, kind, **kw):
    """Run just the heredoc, returning the daemon script as written on the node."""
    lines = emit(kind, **kw)
    heredoc = "\n".join(lines[: lines.index("EOF") + 1])
    env = dict(os.environ, CANINE_JOB_ROOT=str(tmpdir), **BAKED_ENV)
    subprocess.run(["bash", "-c", heredoc], env=env, check=True)
    with open(os.path.join(str(tmpdir), ".diskresizedaemon.sh")) as f:
        return f.read()


def run_daemon(daemon_src, df_values):
    """
    Run the daemon against a stubbed environment.

    `df_values` is a list of "total_mb:free_mb" strings, one per loop iteration. The
    daemon calls df twice per iteration (total, then free), so the stub advances every
    two calls. When the values run out the stub signals completion and the daemon
    exits, so this terminates without needing `timeout` (absent on macOS).

    Returns (resize_sizes_gb, stderr_lines).
    """
    work = tempfile.mkdtemp()
    try:
        binv = os.path.join(work, "bin")
        os.makedirs(binv)
        with open(os.path.join(work, "values"), "w") as f:
            f.write("\n".join(df_values) + "\n")
        with open(os.path.join(work, "calls"), "w") as f:
            f.write("0\n")

        stubs = {
            "df": (
                '#!/bin/bash\n'
                'W="$(dirname "$0")/.."\n'
                'n=$(cat "$W/calls"); echo $((n+1)) > "$W/calls"\n'
                'line=$(sed -n "$(( n/2 + 1 ))p" "$W/values")\n'
                '[ -z "$line" ] && { touch "$W/done"; exit 1; }\n'
                'total=${line%:*}; free=${line#*:}\n'
                'echo "Filesystem 1M-blocks Used Available Use% Mounted"\n'
                'echo "/dev/fake $total $((total-free)) $free 0% /mnt"\n'
            ),
            "mountpoint": "#!/bin/bash\nexit 0\n",
            "gcloud_exp_backoff": (
                '#!/bin/bash\n'
                'W="$(dirname "$0")/.."\n'
                'while [ $# -gt 0 ]; do\n'
                '  [ "$1" = "--size" ] && echo "$2" >> "$W/resizes"\n'
                '  shift\n'
                'done\n'
                'exit 0\n'
            ),
            "sudo": "#!/bin/bash\nexit 0\n",
        }
        for name, body in stubs.items():
            p = os.path.join(binv, name)
            with open(p, "w") as f:
                f.write(body)
            os.chmod(p, 0o755)

        resizes_path = os.path.join(work, "resizes")
        open(resizes_path, "a").close()

        # Neutralise the sleep so the test is instant, and exit once df is exhausted.
        done = os.path.join(work, "done")
        src = daemon_src.replace(
            "  sleep $POLL", '  [ -f "{}" ] && exit 0'.format(done)
        )
        assert "sleep $POLL" not in src, "sleep substitution failed"
        script = os.path.join(work, "daemon.sh")
        with open(script, "w") as f:
            f.write(src)

        env = dict(os.environ, PATH=binv + os.pathsep + os.environ["PATH"])
        proc = subprocess.run(
            ["bash", script], env=env, capture_output=True, text=True
        )
        with open(resizes_path) as f:
            resizes = [int(x) for x in f.read().split()]
        return resizes, proc.stderr.splitlines()
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# emitted-script contract
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kind,kw", [("scratch", SCRATCH_KW), ("localization", LOCALIZATION_KW)])
def test_wrapper_is_valid_bash(kind, kw):
    wrapper = "\n".join(emit(kind, **kw))
    r = subprocess.run(["bash", "-n"], input=wrapper, text=True, capture_output=True)
    assert r.returncode == 0, r.stderr


@pytest.mark.parametrize("kind,kw", [("scratch", SCRATCH_KW), ("localization", LOCALIZATION_KW)])
def test_survives_debug_sh_transform(kind, kw):
    """debug.sh regenerates a runnable script with `grep -v '#DEBUG_OMIT'`."""
    lines = emit(kind, **kw)
    assert not any("#DEBUG_OMIT" in l for l in lines)
    stripped = "\n".join(l for l in lines if "#DEBUG_OMIT" not in l)
    r = subprocess.run(["bash", "-n"], input=stripped, text=True, capture_output=True)
    assert r.returncode == 0, r.stderr


@pytest.mark.parametrize("kind,kw", [("scratch", SCRATCH_KW), ("localization", LOCALIZATION_KW)])
def test_pid_file_written_to_absolute_path(kind, kw):
    """
    The daemon PID must be recorded under $CANINE_JOB_ROOT, and the teardown script's
    kill must read it back from the same absolute path. A bare relative
    `.diskresizedaemon_pid` only resolves if teardown happens to run with
    $CANINE_JOB_ROOT as its cwd, which the localization-disk path does not arrange.
    """
    lines = emit(kind, **kw)
    assert "echo $! > $CANINE_JOB_ROOT/.diskresizedaemon_pid" in lines


def test_teardown_kill_uses_absolute_pid_path():
    """
    Guards the fix directly. Asserted against the module source rather than a built
    teardown script because reaching `create_persistent_disk` needs live GCP disk
    clients, which a pure unit test cannot have.
    """
    base_py = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "localization", "base.py",
    )
    with open(base_py) as f:
        src = f.read()
    assert "kill $(cat $CANINE_JOB_ROOT/.diskresizedaemon_pid)" in src
    assert "kill $(cat .diskresizedaemon_pid)" not in src


@pytest.mark.parametrize("kind,kw", [("scratch", SCRATCH_KW), ("localization", LOCALIZATION_KW)])
def test_heredoc_bakes_disk_identity_and_defers_runtime_vars(kind, kw, tmp_path):
    """
    The heredoc is deliberately unquoted so the disk's identity is baked in at
    file-write time: GCP_DISK_NAME / GCP_TSNT_DISKS_DIR are plain shell assignments,
    not exports, so a backgrounded child bash would not inherit them.
    """
    daemon = write_daemon(tmp_path, kind, **kw)

    r = subprocess.run(["bash", "-n"], input=daemon, text=True, capture_output=True)
    assert r.returncode == 0, r.stderr

    # baked in
    assert "/mnt/rwdisks/canine-abc123" in daemon
    assert "us-central1-a" in daemon
    assert "google-canine-abc123" in daemon
    assert "$GCP_DISK_NAME" not in daemon
    assert "$GCP_TSNT_DISKS_DIR" not in daemon
    assert "$CANINE_NODE_ZONE" not in daemon

    # still evaluated at run time
    for var in ("$DISK_DIR", "$TOTAL_MB", "$FREE_MB", "$NEW_GB", "$POLL"):
        assert var in daemon, var


# ---------------------------------------------------------------------------
# resize arithmetic
# ---------------------------------------------------------------------------

def test_localization_disk_grows_on_free_space_floor(tmp_path):
    daemon = write_daemon(tmp_path, "localization", **LOCALIZATION_KW)
    # 100 GiB total, 20 GiB free => 20% < min_free_pct 25 => grow 125%
    resizes, log = run_daemon(daemon, ["102400:20480"])
    assert resizes == [125]
    assert any("free space below 25%" in l for l in log)


def test_localization_disk_does_not_grow_when_healthy(tmp_path):
    daemon = write_daemon(tmp_path, "localization", **LOCALIZATION_KW)
    # 50% free and no write activity between polls => neither trigger fires
    resizes, _ = run_daemon(daemon, ["102400:51200", "102400:51200"])
    assert resizes == []


def test_localization_disk_grows_on_projected_time_to_full(tmp_path):
    """
    The rate trigger is the reason this daemon differs from the scratch-disk one.
    Parallel download streams can consume the disk faster than one poll interval plus
    a control-plane resize, so a free-percentage floor alone loses the race.

    1000 GiB disk, free drops 500 GiB -> 100 GiB across one 5s poll: ~82 GB/s, so the
    projected time to full is ~1s. The rate-derived target is far larger than
    grow_pct would give, so max_grow_gb caps the step at +100 GiB.
    """
    daemon = write_daemon(tmp_path, "localization", **LOCALIZATION_KW)
    resizes, log = run_daemon(daemon, ["1024000:512000", "1024000:102400"])
    assert resizes == [1100]
    assert any("projected full in" in l for l in log)


def test_per_step_growth_is_capped(tmp_path):
    """
    The localization disk persists as a RODISK, so overshoot is a permanent storage
    cost: cap each step and resize repeatedly instead of over-provisioning once.
    """
    daemon = write_daemon(tmp_path, "localization", **LOCALIZATION_KW)
    resizes, _ = run_daemon(daemon, ["1024000:512000", "1024000:1024"])
    assert resizes, "expected a resize"
    assert all(sz - 1000 <= LOCALIZATION_KW["max_grow_gb"] for sz in resizes)


def test_minimum_growth_step_applies_to_small_disks(tmp_path):
    """A 10 GiB disk grown by 125% would only gain 2 GiB; min_grow_gb floors it."""
    daemon = write_daemon(tmp_path, "localization", **LOCALIZATION_KW)
    resizes, _ = run_daemon(daemon, ["10240:1024"])
    assert resizes == [10 + LOCALIZATION_KW["min_grow_gb"]]


def test_scratch_disk_tuning_is_unchanged(tmp_path):
    """
    Scratch disks kept their long-standing rule: grow to 160% when under 30% free,
    with the rate trigger disabled. This is a regression guard on hoisting the daemon
    out of the scratch-only branch — the scratch path must not have changed.
    """
    daemon = write_daemon(tmp_path, "scratch", **SCRATCH_KW)

    resizes, _ = run_daemon(daemon, ["102400:25600"])   # 25% free => grow
    assert resizes == [160]

    resizes, _ = run_daemon(daemon, ["102400:35840"])   # 35% free => no grow
    assert resizes == []

    # a large write burst must NOT trigger a resize for scratch disks: 40% free
    # remains above the floor, and headroom_sec = 0 disables the rate trigger
    resizes, _ = run_daemon(daemon, ["1024000:512000", "1024000:409600"])
    assert resizes == []


def test_daemon_tolerates_unmounted_disk(tmp_path):
    """A resize failure or a missing mount must not kill the daemon."""
    daemon = write_daemon(tmp_path, "localization", **LOCALIZATION_KW)
    # df reporting a zero-size filesystem must not divide by zero or resize
    resizes, _ = run_daemon(daemon, ["0:0", "102400:51200"])
    assert resizes == []
