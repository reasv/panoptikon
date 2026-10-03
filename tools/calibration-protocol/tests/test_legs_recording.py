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

