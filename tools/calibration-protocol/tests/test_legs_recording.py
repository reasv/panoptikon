"""What `legs.py` records around the gateway: the recorders' first samples
before it starts, and monotonic marks.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
import types
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(f"_calib_recording_{name}",
                                                  HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load("legs")

SAMPLE = '{"schema": "healthrec/1", "kind": "sample", "seq": 0}\n'


def test_the_gateway_waits_for_a_late_recorder_and_names_a_silent_one(tmp_path):
    late, silent = tmp_path / "vramrec.jsonl", tmp_path / "healthrec.jsonl"
    late.write_text('{"kind": "header"}\n')
    writer = threading.Timer(0.3, lambda: late.write_text(SAMPLE))
    writer.start()
    assert legs.unsampled([late], 5.0) == []
    writer.join()
    assert legs.unsampled([late, silent], 0.3) == ["healthrec.jsonl"]


def test_throughput_times_the_job_on_the_monotonic_clock(tmp_path, monkeypatch):
    analyze = _load("analyze")
    leg = types.SimpleNamespace(events=[])
    for name, mono in (("job_posted", 1000.0), ("job_end", 1010.0)):
        monkeypatch.setattr(legs.time, "monotonic", lambda: mono)
        legs.Leg.mark(leg, name)
    (tmp_path / "legs.json").write_text(json.dumps({"events": leg.events}))
    # The wall clock stepped back 1.8 s during the 10 s job.
    (tmp_path / "jobs.json").write_text(json.dumps({"history": [
        {"total_segments": 100, "start_time": "2026-10-03T10:00:00",
         "end_time": "2026-10-03T10:00:08.2"}]}))
    out = tmp_path / "verdicts.json"
    monkeypatch.undo()
    analyze.main(["--scenario", str(tmp_path), "--checks", "throughput",
                  "--json", str(out), "--quiet"])
    (verdict,) = json.loads(out.read_text())["verdicts"]
    assert verdict["numbers"]["items_per_s"] == 10.0
