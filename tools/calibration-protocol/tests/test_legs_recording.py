"""What `legs.py` records around the gateway: the recorders' first samples
before it starts, and monotonic marks.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import signal
import subprocess
import sys
import threading
import tomllib
import types
import urllib.error
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


def test_the_leg_waits_for_every_recorder_and_marks_a_silent_one(
        monkeypatch, tmp_path):
    sampled, late = tmp_path / "vramrec.jsonl", tmp_path / "healthrec.jsonl"
    sampled.write_text(HEADER + SAMPLE)
    late.write_text(HEADER)
    writer = threading.Timer(0.3, lambda: late.write_text(HEADER + SAMPLE))
    writer.start()
    leg = types.SimpleNamespace(path=lambda name: tmp_path / name, events=[])
    leg.mark = lambda name, **detail: legs.Leg.mark(leg, name, **detail)
    legs.Leg.wait_for_recorders(leg, ["healthrec"], timeout=5.0)
    writer.join()
    assert leg.events == []

    # A missing file, and a partly written sample line, hold no sample; only
    # the silent recordings are listed.
    sampled.unlink()
    late.write_text(HEADER + SAMPLE[:20])
    (tmp_path / "healthrec-remote.jsonl").write_text(HEADER + SAMPLE)
    monkeypatch.setattr(legs.time, "monotonic", lambda: 1234.56789)
    legs.Leg.wait_for_recorders(leg, ["healthrec", "healthrec-remote"],
                                timeout=0)
    (event,) = leg.events
    assert (event["event"], event["files"], event["waited_s"]) == (
        "recorder_sample_timeout", ["vramrec.jsonl", "healthrec.jsonl"], 0)
    assert event["t_mono"] == 1234.568


REMOTE = "http://10.0.0.5:7777"


def test_a_split_gateway_forwards_inference_and_both_sides_are_polled(
        capsys, tmp_path):
    """`--inference-url` replaces the config's inference servers and turns
    local inference off; the server's own /health is polled beside the
    gateway's. A hog is refused: it would pressure this host."""
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

    argv = ["--config", str(config), "--no-dotenv", "--dry-run",
            "--gpu-total-mb", "24564"]
    assert legs.main(["--scenario", "S14", *argv, "--inference-url",
                      REMOTE + "/api/inference/"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["health_urls"] == {"healthrec": "http://127.0.0.1:16342",
                                   "healthrec-remote": REMOTE}
    for hog in (["--scenario", "S4a"],
                ["--scenario", "S14", "--hog-event", "at=5,release"]):
        assert legs.main([*hog, *argv]) == 0
        with pytest.raises(SystemExit):
            legs.main([*hog, *argv, "--inference-url", REMOTE])


def test_healthrec_keeps_the_clients_of_a_gateway_that_answered_504(
        monkeypatch):
    """The gateway's 504 for a frozen inference server names its clients and
    when it declared the server frozen; the sample keeps both."""
    healthrec = _load("healthrec")
    clients = [{"base_url": REMOTE, "transport": "h2c",
                "frozen_since": "2026-10-04T10:00:00Z"}]
    body = json.dumps({"detail": "frozen", "inference_clients": clients})

    def answer_504(url, timeout):
        raise urllib.error.HTTPError(url, 504, "", {},
                                     io.BytesIO(body.encode()))

    monkeypatch.setattr(healthrec.urllib.request, "urlopen", answer_504)
    result = healthrec.fetch("http://gateway/api/inference/health", 5.0)
    health = healthrec.flatten_health(result, full=False)
    assert (health["ok"], health["status_code"]) == (False, 504)
    assert health["inference_clients"] == clients
    assert health["detail"] == "frozen"
    assert "raw" not in health
    assert healthrec.flatten_health(
        {**result, "payload": {"detail": "x"}}, full=False)[
            "inference_clients"] is None
    assert healthrec.flatten_health(result, full=True)["raw"] == json.loads(body)
    assert "running" not in healthrec.flatten_queue(result)


# Every child legs starts is logged and replaced by one that exits once the
# driver is gone, or after 60 s. The teardown's stop_all starts 1 s late, so a
# signal can land inside it. A second Ctrl-C lands while the interrupt is
# being handled.
DRIVER = """
import json, os, signal, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import legs
tmp = Path(sys.argv[2])
CHILD = ("import os, sys, time\\ndeadline = time.monotonic() + 60\\n"
         "while time.monotonic() < deadline:\\n"
         "    try: os.kill(int(sys.argv[1]), 0)\\n"
         "    except OSError: break\\n"
         "    time.sleep(0.1)")
start, stop_all = legs.Supervisor.start, legs.Supervisor.stop_all
def logged_start(self, name, argv, **kw):
    with (tmp / "started.jsonl").open("a") as out:
        out.write(json.dumps([name, list(argv)]) + "\\n")
    return start(self, name, [sys.executable, "-c", CHILD, str(os.getpid())],
                 **kw)
def late_stop_all(self):
    (tmp / "teardown").touch()
    time.sleep(1.0)
    return stop_all(self)
legs.Supervisor.start, legs.Supervisor.stop_all = logged_start, late_stop_all
mark = legs.Leg.mark
def mark_then_sigint(self, name, **detail):
    mark(self, name, **detail)
    if name == "interrupted":
        signal.raise_signal(signal.SIGINT)
legs.Leg.mark = mark_then_sigint
legs.Leg.wait_for_recorders = lambda self, health: None
legs.nvml_total_mb = lambda device: None
legs.rocm_sysfs.inventory = lambda *roots: []
directory = tmp / "run" / "S14"
directory.mkdir(parents=True)
legs.subprocess.run = lambda argv, **kw: subprocess.CompletedProcess(
    argv, 0, stdout=f"{directory}\\n", stderr="")
sys.exit(legs.main(sys.argv[3:]))
"""


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal")
@pytest.mark.parametrize("first, split", [("SIGTERM", False),
                                          ("SIGHUP", True),
                                          ("SIGINT", False)])
def test_a_stop_signal_tears_the_leg_down_and_records_it(tmp_path, first,
                                                         split):
    """With stdout gone (a hung-up terminal), and a second signal during the
    teardown, every child is still stopped and legs.json still written."""
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "manifest.json").write_text(json.dumps(
        {"tier": "smoke", "generator": legs.CORPUS_GENERATOR}))
    config = tmp_path / "server-X.toml"
    config.write_text("[server]\nport = 1\n")
    driver = tmp_path / "driver.py"
    driver.write_text(DRIVER)
    argv = [sys.executable, str(driver), str(HERE), str(tmp_path),
            "--scenario", "S14", "--config", str(config), "--bin",
            str(config), "--corpus", str(corpus), "--results", str(tmp_path),
            "--no-dotenv", "--stop-grace", "5"]
    if split:
        argv += ["--inference-url", REMOTE]
    with (tmp_path / "err.log").open("wb") as err:
        # A background job or nohup inherits these ignored.
        leg = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=err,
            preexec_fn=lambda: [signal.signal(s, signal.SIG_DFL)
                                for s in (signal.SIGINT, signal.SIGHUP)])
    try:
        for line in leg.stdout:
            if b"gateway_started" in line:
                break
        else:
            pytest.fail((tmp_path / "err.log").read_text())
        leg.stdout.close()
        leg.send_signal(getattr(signal, first))
        assert legs.wait_for((tmp_path / "teardown").exists, 30.0,
                             interval=0.1)
        leg.send_signal(signal.SIGINT)
        leg.wait(timeout=30)
    finally:
        leg.kill()
        leg.wait(timeout=5)
    recorded = json.loads((tmp_path / "run" / "S14" / "legs.json").read_text())
    assert recorded["outcome"] == "interrupted"
    assert {"event": "interrupted", "signal": first}.items() <= next(
        event for event in recorded["events"]
        if event["event"] == "interrupted").items()
    # Each child was stopped and reaped by the leg, not left to its parent's
    # death.
    extra = {"healthrec-remote"} if split else set()
    assert set(recorded["processes"]) == {"vramrec", "healthrec", "gateway",
                                          *extra}
    assert all(row["returncode"] == -signal.SIGTERM
               for row in recorded["processes"].values())
    if split:
        started = dict(json.loads(line) for line in
                       (tmp_path / "started.jsonl").read_text().splitlines())
        remote = started["healthrec-remote"]
        assert remote[remote.index("--base") + 1] == REMOTE
        assert "--no-queue" in remote
        gateway = started["healthrec"]
        assert float(gateway[gateway.index("--timeout") + 1]) > 10
        (written,) = (tmp_path / "run" / "S14").glob("server-*.toml")
        document = tomllib.loads(written.read_text())
        assert document["inference_local"]["enabled"] is False
        assert document["upstreams"]["inference"] == [{"base_url": REMOTE}]


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal")
def test_an_ignored_sighup_stays_ignored():
    """nohup: a leg started with SIGHUP ignored is not ended by a hang-up."""
    saved = {sig: signal.getsignal(sig) for sig in legs.STOP_SIGNALS}
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    try:
        legs.stop_on_signals()
        assert signal.getsignal(signal.SIGHUP) is signal.SIG_IGN
        assert signal.getsignal(signal.SIGTERM) is not saved[signal.SIGTERM]
    finally:
        for sig, handler in saved.items():
            signal.signal(sig, handler)


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal")
@pytest.mark.parametrize("sig", ["SIGINT", "SIGTERM"])
def test_a_signal_while_a_child_starts_is_raised_once_it_is_registered(
        monkeypatch, sig):
    """The teardown stops a child whose start a stop signal interrupted."""
    saved = {each: signal.getsignal(each)
             for each in (signal.SIGINT, *legs.STOP_SIGNALS)}
    popen = subprocess.Popen

    def signalled(*args, **kwargs):
        signal.raise_signal(getattr(signal, sig))
        return popen(*args, **kwargs)

    monkeypatch.setattr(legs.subprocess, "Popen", signalled)
    supervisor = legs.Supervisor(grace=5.0)
    try:
        legs.stop_on_signals()
        with pytest.raises(KeyboardInterrupt):
            supervisor.start("child", [sys.executable, "-c",
                                       "import time; time.sleep(60)"])
    finally:
        for each, handler in saved.items():
            signal.signal(each, handler)
    (child,) = supervisor.children
    supervisor.stop_all()
    assert child.popen.returncode == -signal.SIGTERM


def test_job_start_is_marked_before_the_post(monkeypatch, tmp_path):
    leg = types.SimpleNamespace(
        db="cal", base="http://gw", events=[], path=lambda name: tmp_path / name,
        args=types.SimpleNamespace(job_cap=None),
        wait_for_queue=lambda cap: "drained", job_items=lambda model, tag: 1)
    leg.mark = lambda name, **detail: legs.Leg.mark(leg, name, **detail)
    seen = []
    monkeypatch.setattr(legs, "save", lambda url, path, method="GET": seen.append(
        (method, [event["event"] for event in leg.events])) or 200)
    legs.Leg.run_job(leg, "m", "")
    assert seen[0] == ("POST", ["job_start"])
