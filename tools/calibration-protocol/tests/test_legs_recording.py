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

HEADER = json.dumps({"kind": "header"}) + "\n"
SAMPLE = json.dumps({"kind": "sample", "seq": 0, "t_wall": 100.0}) + "\n"


def test_the_leg_waits_for_both_recorders_and_marks_a_silent_one(tmp_path):
    sampled, late = tmp_path / "vramrec.jsonl", tmp_path / "healthrec.jsonl"
    sampled.write_text(HEADER + SAMPLE)
    late.write_text(HEADER)
    writer = threading.Timer(0.3, lambda: late.write_text(HEADER + SAMPLE))
    writer.start()
    assert legs.unsampled([sampled, late], 5.0) == []
    writer.join()

    # A partly written sample line is not a sample.
    late.write_text(HEADER + SAMPLE[:20])
    leg = types.SimpleNamespace(path=lambda name: tmp_path / name, events=[])
    leg.mark = lambda name, **detail: legs.Leg.mark(leg, name, **detail)
    legs.Leg.wait_for_recorders(leg, timeout=0.3)
    (event,) = leg.events
    assert (event["event"], event["files"], event["waited_s"]) == (
        "recorder_sample_timeout", ["healthrec.jsonl"], 0.3)
    assert isinstance(event["t_mono"], float)
