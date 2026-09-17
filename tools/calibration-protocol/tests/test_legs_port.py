"""`--port` has to move the gateway, not only the probe.

Its help says "gateway port (default: read from the config)", but it only
changed the URL `legs.py` polled: a leg run with `--port 17912` against
`server-C1.toml` bound 6342/6343/6339 and aborted `gateway_never_answered`
four minutes later (final-deploy O4). The port now moves every listener the
config declares, in the same per-leg copy `--python` is written into, and the
plan says which ports will be bound.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
C1 = HERE / "config" / "server-C1.toml"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_port",
                                                  HERE / "legs.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()

CONFIG = """\
[server]
host = "127.0.0.1"
port = 6342  # CALIB: per-configuration port
trust_forwarded_headers = true

[[server.endpoints]]
name = "test"
port = 6343

[[server.endpoints]]
name = "legacy_ui"
port = 6339

[upstreams.ui]
base_url = "http://127.0.0.1:6340"

[inference_local]
port = 9999
"""


def test_every_listener_moves_by_the_same_offset():
    moved = tomllib.loads(legs.repin_ports(CONFIG, 17912 - 6342))
    assert moved["server"]["port"] == 17912
    assert [entry["port"] for entry in moved["server"]["endpoints"]] == [
        17913, 17909]


def test_a_port_outside_the_server_tables_is_left_alone():
    moved = tomllib.loads(legs.repin_ports(CONFIG, 11570))
    assert moved["inference_local"]["port"] == 9999
    assert moved["upstreams"]["ui"]["base_url"].endswith(":6340")
    assert moved["server"]["host"] == "127.0.0.1"


def test_the_comment_on_the_line_survives_the_move():
    assert "17912  # CALIB" in legs.repin_ports(CONFIG, 11570)


def test_the_shipped_c1_config_moves_as_a_whole():
    text = legs.repin_ports(C1.read_text(encoding="utf-8"), 17912 - 6342)
    assert tomllib.loads(text)["server"]["port"] == 17912
    assert {(row["name"], row["port"]) for row in legs.endpoints_in(text)} == {
        ("test", 17913), ("legacy_ui", 17909)}


def test_the_python_pin_and_the_port_move_compose():
    text = legs.repin_ports(
        legs.repin_inference_python(CONFIG, "/cpu/venv/bin/python"), 11570)
    document = tomllib.loads(text)
    assert document["inference_local"]["python"] == "/cpu/venv/bin/python"
    assert document["server"]["port"] == 17912


# --- what the leg plans ----------------------------------------------------


def _plan(*extra: str) -> dict:
    result = subprocess.run(
        [sys.executable, str(HERE / "legs.py"), "--scenario", "S2",
         "--dry-run", *extra],
        capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def test_the_plan_shows_the_ports_the_gateway_will_bind():
    plan = _plan("--port", "17912")
    assert plan["bound_ports"] == {"gateway": 17912, "test": 17913,
                                   "legacy_ui": 17909}
    assert plan["base_url"] == "http://127.0.0.1:17912"
    assert plan["endpoints"] == [{"name": "test", "port": 17913},
                                 {"name": "legacy_ui", "port": 17909}]


def test_without_the_flag_the_configs_own_ports_are_bound():
    plan = _plan()
    assert plan["bound_ports"] == {"gateway": 6342, "test": 6343,
                                   "legacy_ui": 6339}


def test_a_config_named_by_path_moves_the_same_way(tmp_path):
    """Its listeners are the ones S14's endpoint assertions are read against."""
    config = tmp_path / "server-X.toml"
    config.write_text(CONFIG, encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(HERE / "legs.py"), "--scenario", "S14",
         "--config", str(config), "--port", "17912", "--dry-run"],
        capture_output=True, text=True, check=True)
    plan = json.loads(result.stdout)
    assert plan["bound_ports"]["gateway"] == 17912
    assert plan["endpoints"] == [{"name": "test", "port": 17913},
                                 {"name": "legacy_ui", "port": 17909}]
