"""What `legs.py` records around the gateway: the recorders' first samples
before it starts, and monotonic marks.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import threading
import tomllib
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

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
    """The spike's leave-free target is at or under what the hog held, and a
    hold leaves the target where it was: void. A release or a step-down asks
    for nothing on purpose. The state one tick after the reply can still
    carry the old target, so it is not the answer."""
    replies = iter([{"seq": 5, "held_mb": 0, "target_mb": 0},
                    {"seq": 7, "held_mb": 0, "target_mb": 0},
                    {"seq": 9, "held_mb": 0, "target_mb": 0},
                    {"seq": 11, "held_mb": 8192, "target_mb": 8192},
                    {"seq": 13, "held_mb": 4096, "target_mb": 4096}])
    states = iter([{"seq": 5, "target_mb": 0, "held_mb": 0},
                   {"seq": 7, "target_mb": 0, "held_mb": 0},
                   {"seq": 9, "target_mb": 0, "held_mb": 0},
                   {"seq": 10, "target_mb": 0, "held_mb": 0},
                   {"seq": 11, "target_mb": 8192, "held_mb": 8192},
                   {"seq": 13, "target_mb": 4096, "held_mb": 4096},
                   {"seq": 15, "target_mb": 4096, "held_mb": 4096}])
    monkeypatch.setattr(legs, "request", lambda *_a, **_k: (
        200, json.dumps(next(replies)).encode()))
    monkeypatch.setattr(legs, "get_json", lambda *_a, **_k: next(states))
    monkeypatch.setattr(legs.time, "sleep", lambda _s: None)
    leg = types.SimpleNamespace(events=[], hog_url=lambda path: path)
    leg.mark = lambda name, **detail: legs.Leg.mark(leg, name, **detail)
    legs.Leg.drive_hog(leg, [
        {"at_s": 0, "label": "spike", "leave_free_mb": 2048},
        {"at_s": 0, "label": "release", "mb": 0},
        {"at_s": 0, "label": "step up", "mb": 8192},
        {"at_s": 0, "label": "step down", "mb": 4096},
        {"at_s": 0, "label": "hold", "mb": 4096}], legs.time.monotonic())
    assert [event["label"] for event in leg.events
            if event["event"] == "hog_event_void"] == ["spike", "hold"]

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


REMOTE = "http://10.0.0.5:7777"


def test_a_split_gateway_forwards_inference_and_both_sides_are_polled(
        tmp_path):
    """`--inference-url` replaces the config's inference servers and turns
    local inference off, in the leg's copy and in `--write-config`'s; the
    server's own /health is polled beside the gateway's."""
    config = tmp_path / "server-X.toml"
    config.write_text('[server]\nport = 16342\n\n[inference_local]\n'
                      'enabled = true\n\n[[upstreams.inference]]\n'
                      'base_url = "http://old:1"\nweight = 2.0\n\n'
                      '[search]\ncache_size_mb = 1\n', encoding="utf-8")
    document = tomllib.loads(legs.leg_config(config.read_text(), None, None,
                                             REMOTE))
    assert document["inference_local"]["enabled"] is False
    assert document["upstreams"]["inference"] == [{"base_url": REMOTE}]
    assert document["search"] == {"cache_size_mb": 1}

    out = tmp_path / "out"
    assert legs.main(["--config", "C1", "--repo", str(HERE.parents[1]),
                      "--no-dotenv", "--python", "/opt/venv/bin/python",
                      "--inference-url", REMOTE, "--write-config",
                      str(out)]) == 0
    written = tomllib.loads((out / "server-C1.toml").read_text())
    assert written["upstreams"]["inference"] == [{"base_url": REMOTE}]
    assert written["inference_local"]["enabled"] is False

    plan = json.loads(subprocess.run(
        [sys.executable, str(HERE / "legs.py"), "--scenario", "S14",
         "--config", str(config), "--inference-url", REMOTE + "/",
         "--no-dotenv", "--dry-run"],
        capture_output=True, text=True, check=True).stdout)
    assert plan["health_urls"] == {"healthrec": "http://127.0.0.1:16342",
                                   "healthrec-remote": REMOTE}


def test_healthrec_keeps_the_clients_of_a_gateway_that_answered_504():
    """The gateway's 504 for a frozen inference server names its clients and
    when it declared the server frozen; the sample keeps both."""
    healthrec = _load("healthrec")
    clients = [{"base_url": REMOTE, "transport": "h2c",
                "frozen_since": "2026-10-04T10:00:00Z"}]
    body = json.dumps({"detail": "frozen", "inference_clients": clients})

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(504)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/api/inference/health"
        result = healthrec.fetch(url, 5.0)
    finally:
        server.shutdown()
    health = healthrec.flatten_health(result, full=False)
    assert (health["ok"], health["status_code"]) == (False, 504)
    assert health["inference_clients"] == clients
    assert health["detail"] == "frozen"
    assert "running" not in healthrec.flatten_queue(result)


# Every child legs starts, replaced by one that exits once its parent is gone.
DRIVER = """
import os, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import legs
start = legs.Supervisor.start
CHILD = ("import os, time\\nparent = os.getppid()\\n"
         "while os.getppid() == parent: time.sleep(0.1)")
legs.Supervisor.start = lambda self, name, argv, **kw: start(
    self, name, [sys.executable, "-c", CHILD], **kw)
legs.unsampled = lambda paths, timeout: []
legs.board_total_mb = lambda device: None
legs.rocm_sysfs.inventory = lambda *roots: []
directory = Path(sys.argv[2]) / "run" / "S14"
directory.mkdir(parents=True)
legs.subprocess.run = lambda argv, **kw: subprocess.CompletedProcess(
    argv, 0, stdout=f"{directory}\\n", stderr="")
sys.exit(legs.main(sys.argv[3:]))
"""


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal")
def test_sigterm_tears_the_leg_down_and_records_it(tmp_path):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "manifest.json").write_text(json.dumps(
        {"tier": "smoke", "generator": legs.CORPUS_GENERATOR}))
    config = tmp_path / "server-X.toml"
    config.write_text("[server]\nport = 1\n")
    driver = tmp_path / "driver.py"
    driver.write_text(DRIVER)
    log = tmp_path / "out.log"
    with log.open("wb") as sink:
        leg = subprocess.Popen(
            [sys.executable, str(driver), str(HERE), str(tmp_path),
             "--scenario", "S14", "--config", str(config), "--bin",
             str(config), "--corpus", str(corpus), "--results",
             str(tmp_path), "--no-dotenv", "--stop-grace", "5"],
            stdout=sink, stderr=subprocess.STDOUT)
        try:
            assert legs.wait_for(lambda: b"gateway_started" in log.read_bytes(),
                                 30.0, interval=0.1), log.read_text()
            leg.send_signal(signal.SIGTERM)
            assert leg.wait(timeout=30) == 1
        finally:
            leg.kill()
    recorded = json.loads((tmp_path / "run" / "S14" / "legs.json").read_text())
    assert recorded["outcome"] == "interrupted"
    assert {"event": "interrupted", "signal": "SIGTERM"}.items() <= next(
        event for event in recorded["events"]
        if event["event"] == "interrupted").items()
    # Each child was stopped and reaped by the leg, not left to its parent's
    # death.
    assert set(recorded["processes"]) == {"vramrec", "healthrec", "gateway"}
    assert all(row["returncode"] == -signal.SIGTERM
               for row in recorded["processes"].values())
