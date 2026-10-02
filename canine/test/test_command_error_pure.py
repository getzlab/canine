"""
A failed command's error says why it failed, not only its exit status: check_call's
CommandError carries the command's stderr in its message.
"""
import io
import subprocess
import pytest

from canine.utils import CommandError, check_call


def _stream(text):
    return io.BytesIO(text.encode())


def test_the_message_includes_stderr():
    with pytest.raises(CommandError) as e:
        check_call("sbatch x", 1, _stream(""), _stream("sbatch: error: invalid partition specified: n1-standard-8\n"))
    assert "returned non-zero exit status 1" in str(e.value)
    assert "invalid partition specified: n1-standard-8" in str(e.value)


def test_it_is_still_a_called_process_error():
    with pytest.raises(subprocess.CalledProcessError) as e:
        check_call("false", 2, None, _stream("boom"))
    assert e.value.returncode == 2 and e.value.stderr == "boom"


def test_no_stderr_leaves_the_plain_message():
    with pytest.raises(CommandError) as e:
        check_call("false", 1)
    assert str(e.value) == str(subprocess.CalledProcessError(1, "false"))


def test_success_raises_nothing():
    check_call("true", 0, _stream("out"), _stream("err"))


def test_a_failed_sbatch_says_why():
    """The wolf2-east-slw failure: the partition did not exist."""
    from canine.backends.base import AbstractSlurmBackend
    err = "sbatch: error: invalid partition specified: n1-standard-8\nallocation failure: Invalid partition name specified\n"

    class Backend(AbstractSlurmBackend):
        def invoke(self, command, interactive=False, **kwargs):
            return 1, _stream(""), _stream(err)
        transport = __enter__ = __exit__ = lambda self, *a: None

    with pytest.raises(CommandError) as e:
        object.__new__(Backend).sbatch("entrypoint.sh", partition="n1-standard-8")
    assert "invalid partition specified: n1-standard-8" in str(e.value)


def test_stderr_goes_through_the_logger_not_raw_stderr(monkeypatch, capsys):
    """
    Under wolF the logger canine_logging hooks feeds the console and the run log. A
    raw write to sys.stderr reached only the console, unformatted: seen on
    wolf2-east-slw 2026-10-02, where the run log never had the sbatch error.
    """
    import logging
    import canine.utils as cu
    records = []

    class Logger:
        def error(self, msg):
            records.append((logging.ERROR, msg))
        def log(self, level, msg, *a, **k):
            records.append((level, msg))

    monkeypatch.setattr(cu, "CANINE_GET_LOGGER_HOOK", lambda: Logger())
    with pytest.raises(CommandError):
        check_call("sbatch --partition=n1-standard-8 -- x", 1, _stream("some stdout"),
                   _stream("sbatch: error: invalid partition specified: n1-standard-8\n"))
    assert (logging.ERROR, "sbatch exited 1; its stderr:\nsbatch: error: invalid partition specified: n1-standard-8") in records
    assert any("its stdout:\nsome stdout" in m for _, m in records)
    captured = capsys.readouterr()
    assert "invalid partition" not in captured.err and "some stdout" not in captured.out


def test_without_a_logger_it_still_prints(monkeypatch, capsys):
    """Standalone canine, with no hook installed, still shows the stderr."""
    import canine.utils as cu
    monkeypatch.setattr(cu, "CANINE_GET_LOGGER_HOOK", None)
    with pytest.raises(CommandError):
        check_call("sbatch x", 1, None, _stream("sbatch: error: invalid partition\n"))
    assert "sbatch: error: invalid partition" in capsys.readouterr().err
