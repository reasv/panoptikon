"""`legs.py`'s `FdRecorder`: it must be joinable.

`FdRecorder` is a `threading.Thread` subclass, and `stop()` joins it so no
descriptor sample lands in `fds.jsonl` after the gateway is gone. A
`threading.Thread` subclass may not use `_stop` for its own state: the base
class owns that name (`Thread._stop`, called by `Thread.join` through
`_wait_for_tstate_lock` once the thread has finished), and shadowing it with
an `Event` turned every teardown into `TypeError: 'Event' object is not
callable` -- which aborted the leg before `panoptikon.log` and `legs.json`
were written and left the gateway and the recorders running. That was the
first failure of the MPS pass, and it is platform-independent.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import threading
from pathlib import Path

LEGS = Path(__file__).resolve().parents[1] / "legs.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs", LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()


def _recorder(tmp_path: Path):
    """A recorder sampling this very process, so it has a real reader."""
    return legs.FdRecorder(os.getpid(), tmp_path / "fds.jsonl", interval=0.01)


def test_no_thread_internals_are_shadowed(tmp_path):
    """No attribute of the recorder may collide with a `Thread` internal."""
    recorder = _recorder(tmp_path)
    # Only the attributes the subclass adds on top of what `Thread.__init__`
    # already set, against the names `Thread` defines on the class itself
    # (`_stop` is one of those, a method).
    added = set(vars(recorder)) - set(vars(threading.Thread(daemon=True)))
    collide = added & set(vars(threading.Thread))
    assert not collide, sorted(collide)
    assert "path" in added, "the recorder's own attributes were not found"


def test_start_stop_joins_without_raising(tmp_path):
    """`stop()` is the teardown path: it must return, not raise."""
    recorder = _recorder(tmp_path)
    recorder.start()
    # The recorder writes its first sample before waiting, so one line is
    # enough to know the loop ran; then stop() sets the event and joins.
    deadline = threading.Event()
    deadline.wait(0.2)
    recorder.stop()
    assert not recorder.is_alive()


def test_stop_after_run_finished_still_joins(tmp_path):
    """The regression exactly: join a thread that has already exited.

    `_wait_for_tstate_lock` -- and so the shadowed name -- is only reached
    once the thread's tstate lock is released, i.e. after `run` returned, so
    a recorder that has already stopped is the case that raised.
    """
    recorder = _recorder(tmp_path)
    recorder.start()
    recorder._stopped.set()
    recorder.join(timeout=5)
    assert not recorder.is_alive()
    recorder.stop()  # the double stop the leg teardown can do; must not raise


def test_samples_are_the_jsonl_analyze_reads(tmp_path):
    """A sample carries the keys `analyze.py::read_fds` asks for."""
    path = tmp_path / "fds.jsonl"
    recorder = legs.FdRecorder(os.getpid(), path, interval=0.01)
    recorder.start()
    threading.Event().wait(0.2)
    recorder.stop()
    lines = [json.loads(line) for line in
             path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert lines, "the recorder wrote no sample"
    assert {"iso", "fds", "sockets", "limit"} <= set(lines[0])
    assert lines[0]["fds"] > 0


def test_no_sample_lands_after_stop(tmp_path):
    """The reason `stop()` joins at all."""
    path = tmp_path / "fds.jsonl"
    recorder = legs.FdRecorder(os.getpid(), path, interval=0.01)
    recorder.start()
    threading.Event().wait(0.1)
    recorder.stop()
    after = path.read_text(encoding="utf-8")
    threading.Event().wait(0.1)
    assert path.read_text(encoding="utf-8") == after
