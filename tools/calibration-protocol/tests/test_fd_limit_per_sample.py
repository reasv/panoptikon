"""The descriptor limit belongs to the sample, not to the recorder's start.

The gateway raises its soft `nofile` limit to the hard one a few
milliseconds after it starts (`rlimit.rs`). `FdRecorder` read the limit once,
before that, so every row in `fds.jsonl` said `limit: 1024` against a real
1 048 576 and `peak_fds` printed a percentage ~1024x too large
(run4-deploy, T3). The recorder now re-reads per sample and `analyze.py`
prices the peak against the limit recorded with it.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load("_calib_legs_fdlimit", HERE / "legs.py")
analyze = _load("_calib_analyze_fdlimit", HERE / "analyze.py")


def test_the_recorder_re_reads_the_limit_on_every_sample(tmp_path, monkeypatch):
    """The raise happens after the first sample; the rows must follow it."""
    limits = iter([1024, 1024, 1048576])

    def fd_limit(_pid):
        return next(limits, 1048576)

    monkeypatch.setattr(legs, "fd_limit", fd_limit)
    path = tmp_path / "fds.jsonl"
    recorder = legs.FdRecorder(os.getpid(), path, interval=0.01)
    recorder.start()
    threading.Event().wait(0.3)
    recorder.stop()
    rows = [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) >= 3
    assert rows[0]["limit"] == 1024
    assert rows[-1]["limit"] == 1048576


def _verdict(rows):
    args = argparse.Namespace(worker_pattern="inferio", join_tolerance=1.0,
                              probe=[])
    ctx = analyze.Context(args=args, vramrec=[], healthrec=[], hog=[], log=[],
                          before=None, after=None, jobs=None, probes=[],
                          fds=rows)
    return analyze.check_peak_fds(ctx)


def test_the_peak_is_priced_against_its_own_limit():
    """The pre-raise rows must not set the denominator for the whole run."""
    rows = [{"iso": "t0", "t_wall": 0.0, "fds": 40, "sockets": 2, "limit": 1024},
            {"iso": "t1", "t_wall": 1.0, "fds": 900, "sockets": 800,
             "limit": 1048576}]
    verdict = _verdict(rows)
    assert verdict.numbers["soft_limit"] == 1048576
    assert "0%" in verdict.detail
    assert "AT THE LIMIT" not in verdict.detail


def test_a_sample_at_its_own_limit_still_says_so():
    rows = [{"iso": "t0", "t_wall": 0.0, "fds": 1024, "sockets": 983,
             "limit": 1024},
            {"iso": "t1", "t_wall": 1.0, "fds": 60, "sockets": 4,
             "limit": 1048576}]
    verdict = _verdict(rows)
    assert "AT THE LIMIT" in verdict.detail
    assert verdict.numbers["peak_fds"] == 1024


def test_rows_without_a_limit_fall_back_to_the_largest_seen():
    rows = [{"iso": "t0", "t_wall": 0.0, "fds": 10, "sockets": 1, "limit": 1024},
            {"iso": "t1", "t_wall": 1.0, "fds": 99, "sockets": 4, "limit": None}]
    assert _verdict(rows).numbers["soft_limit"] == 1024
