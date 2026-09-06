"""S14 asserts every extra listener, instead of hoping someone checks by hand.

A config can declare more listeners than the gateway port: the same routes on
another port, matched to a different policy by name -- `test` on 6343 (pinned
to the stdtest DBs) and `legacy_ui` on 6339 (old bookmarks). On these configs
both take the localhost policy, so **200 is the pass**; the 403 belongs to
Docker alone, where the second port is the public endpoint with
`restricted_demo`.

Until now `legs.py` probed one port, only when it was passed by hand, and
recorded the status with no expectation -- so the Windows and MPS passes both
checked `6339 -> 200` by hand and the leg itself would not have noticed a
listener that never came up (T7). The ports now come from the config being
run, and a failure is its own event.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
LEGS = HERE / "legs.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_endpoints",
                                                  LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()


# --- the ports come from the config, not from a constant -------------------


def test_the_shipped_c1_config_declares_both_extra_listeners():
    got = legs.config_endpoints(HERE / "config" / "server-C1.toml")
    assert {(row["name"], row["port"]) for row in got} == {
        ("test", 6343), ("legacy_ui", 6339)}


def test_a_config_with_no_extra_listeners_yields_no_rows(tmp_path):
    toml = tmp_path / "server-X.toml"
    toml.write_text("[server]\nhost = \"127.0.0.1\"\nport = 6342\n",
                    encoding="utf-8")
    assert legs.config_endpoints(toml) == []


def test_an_unreadable_config_is_not_an_exception(tmp_path):
    toml = tmp_path / "broken.toml"
    toml.write_text("this is not toml = = =\n", encoding="utf-8")
    assert legs.config_endpoints(toml) == []
    assert legs.config_endpoints(tmp_path / "absent.toml") == []


def test_an_endpoint_without_a_usable_port_is_skipped(tmp_path):
    toml = tmp_path / "server-Y.toml"
    toml.write_text(
        "[server]\nport = 6342\n"
        "[[server.endpoints]]\nname = \"good\"\nport = 6339\n"
        "[[server.endpoints]]\nname = \"portless\"\n",
        encoding="utf-8")
    assert legs.config_endpoints(toml) == [{"name": "good", "port": 6339}]


# --- the probe -------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    status = 200

    def log_message(self, *_args):
        return

    def do_GET(self):  # noqa: N802
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"queue": []}')


def _serve(status=200):
    handler = type("_H", (_Handler,), {"status": status})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def _leg(endpoints, legacy_port=None):
    args = argparse.Namespace(legacy_port=legacy_port)
    return legs.Leg(args=args, scenario=legs.SCENARIOS["S14"],
                    directory=Path("."), python=sys.executable,
                    config_toml=Path("."), env={},
                    base="http://127.0.0.1:1", total_mb=1,
                    supervisor=legs.Supervisor(1.0), endpoints=endpoints)


def test_a_listener_answering_200_passes():
    server, port = _serve(200)
    try:
        rows = _leg([{"name": "legacy_ui", "port": port}]).endpoint_assertions()
    finally:
        server.shutdown()
    assert len(rows) == 1
    assert rows[0]["status"] == 200 and rows[0]["ok"] is True
    assert rows[0]["expected"] == 200


def test_a_403_is_not_a_pass_off_docker():
    """It is the *Docker* expectation; here it means the wrong policy."""
    server, port = _serve(403)
    try:
        rows = _leg([{"name": "legacy_ui", "port": port}]).endpoint_assertions()
    finally:
        server.shutdown()
    assert rows[0]["status"] == 403 and rows[0]["ok"] is False
    assert rows[0]["expected_under_docker"] == 403


def test_a_listener_that_never_came_up_is_a_failure_not_a_blank():
    rows = _leg([{"name": "legacy_ui", "port": 1}]).endpoint_assertions()
    assert rows[0]["ok"] is False
    assert "error" in rows[0] or rows[0].get("status") != 200


def test_every_declared_listener_is_probed():
    first, port_a = _serve(200)
    second, port_b = _serve(200)
    try:
        rows = _leg([{"name": "test", "port": port_a},
                     {"name": "legacy_ui", "port": port_b}]).endpoint_assertions()
    finally:
        first.shutdown()
        second.shutdown()
    assert [row["name"] for row in rows] == ["test", "legacy_ui"]
    assert all(row["ok"] for row in rows)


def test_an_explicit_legacy_port_is_added_once_and_not_twice():
    server, port = _serve(200)
    try:
        leg = _leg([{"name": "legacy_ui", "port": port}], legacy_port=port)
        rows = leg.endpoint_assertions()
        extra = _leg([], legacy_port=port).endpoint_assertions()
    finally:
        server.shutdown()
    assert len(rows) == 1
    assert len(extra) == 1 and extra[0]["name"] == "legacy_ui"
