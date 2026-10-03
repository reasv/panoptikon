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


def test_a_hog_event_that_asks_for_nothing_is_marked_and_skips_hog_tracking(
        monkeypatch, tmp_path):
    """The spike's leave-free target is at or under what the hog held: void.
    A release asks for nothing on purpose, and a state from before the
    change took effect is not the answer."""
    replies = iter([{"seq": 5, "held_mb": 0}, {"seq": 7, "held_mb": 0},
                    {"seq": 9, "held_mb": 0}])
    states = iter([{"seq": 5, "target_mb": 0, "held_mb": 0},
                   {"seq": 6, "target_mb": 0, "held_mb": 0},
                   {"seq": 8, "target_mb": 0, "held_mb": 0},
                   {"seq": 9, "target_mb": 0, "held_mb": 0},
                   {"seq": 10, "target_mb": 4096, "held_mb": 4096}])
    monkeypatch.setattr(legs, "request", lambda *_a, **_k: (
        200, json.dumps(next(replies)).encode()))
    monkeypatch.setattr(legs, "get_json", lambda *_a, **_k: next(states))
    monkeypatch.setattr(legs.time, "sleep", lambda _s: None)
    leg = types.SimpleNamespace(events=[], hog_url=lambda path: path)
    leg.mark = lambda name, **detail: legs.Leg.mark(leg, name, **detail)
    legs.Leg.drive_hog(leg, [
        {"at_s": 0, "label": "spike", "leave_free_mb": 2048},
        {"at_s": 0, "label": "release", "mb": 0},
        {"at_s": 0, "label": "step up", "mb": 4096}], legs.time.monotonic())
    assert [event["label"] for event in leg.events
            if event["event"] == "hog_event_void"] == ["spike"]

    analyze = _load("analyze")
    (tmp_path / "hog.jsonl").write_text(
        json.dumps({"kind": "header", "target": "gpu", "gpu_uuid": "G"}) + "\n"
        + json.dumps({"kind": "state", "t_wall": 100.0, "held_mb": 0}) + "\n")
    (tmp_path / "healthrec.jsonl").write_text(json.dumps(
        {"kind": "sample", "t_wall": 100.0, "iso": "x", "health": {
            "ok": True, "vram": [{"gpu_uuid": "G", "external_known": True,
                                  "external_mb": 0}]}}) + "\n")
    for events, verdict in ((leg.events, "SKIP"), ([], "INFO")):
        (tmp_path / "legs.json").write_text(json.dumps({"events": events}))
        analyze.main(["--scenario", str(tmp_path), "--checks", "hog_tracking",
                      "--json", str(tmp_path / "v.json"), "--quiet"])
        (result,) = json.loads((tmp_path / "v.json").read_text())["verdicts"]
        assert result["verdict"] == verdict
