"""`--python` must reach the worker, not only the recorders.

`config/server-C1.toml` pins `[inference_local] python` to a CUDA venv. A
leg run with `--python <cpu venv>` still handed the gateway that config, so
the worker loaded torch with CUDA and one `tags` job ran ~90 s on GPU 0
(run4-deploy, "One leg ran on a GPU before I caught it"). The override now
wins, through a per-leg copy of the config, and the leg says which
interpreter the gateway will use.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

LEGS = Path(__file__).resolve().parents[1] / "legs.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_python", LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()

CONFIG = """\
[server]
port = 16342

[inference_local]
enabled = true
python      = "/opt/cuda-venv/bin/python"
impl_dirs   = ["/tree/impl"]

[inference_local.python_env]
auto_setup = "${PANOPTIKON_AUTO_SETUP:-true}"
python = "not this one"
"""


def _table(text: str, name: str) -> dict:
    import tomllib

    return tomllib.loads(text).get(name, {})


def test_the_pinned_interpreter_is_replaced():
    out = legs.repin_inference_python(CONFIG, "/cpu/venv/bin/python")
    assert _table(out, "inference_local")["python"] == "/cpu/venv/bin/python"
    assert "/opt/cuda-venv/bin/python" not in out


def test_only_the_inference_local_table_is_touched():
    """`[inference_local.python_env]` has its own keys; it is a sub-table."""
    out = legs.repin_inference_python(CONFIG, "/cpu/venv/bin/python")
    sub = _table(out, "inference_local")["python_env"]
    assert sub["python"] == "not this one"
    assert _table(out, "server")["port"] == 16342


def test_a_config_without_the_key_gets_one():
    text = "[inference_local]\nenabled = true\n\n[server]\nport = 1\n"
    out = legs.repin_inference_python(text, "/cpu/venv/bin/python")
    assert _table(out, "inference_local")["python"] == "/cpu/venv/bin/python"
    assert _table(out, "server")["port"] == 1


def test_a_config_without_the_table_gets_one():
    out = legs.repin_inference_python("[server]\nport = 1\n", "/py")
    assert _table(out, "inference_local")["python"] == "/py"


def test_a_windows_path_survives_the_rewrite():
    """A TOML basic string escapes backslashes; a raw path would not parse."""
    out = legs.repin_inference_python(CONFIG, r"C:\venv\Scripts\python.exe")
    assert (_table(out, "inference_local")["python"]
            == r"C:\venv\Scripts\python.exe")


def test_the_config_python_is_read_when_no_override_is_given(tmp_path):
    config = tmp_path / "server-CT.toml"
    config.write_text(CONFIG, encoding="utf-8")
    assert legs.config_inference_python(config) == "/opt/cuda-venv/bin/python"


def _plan(capsys, *argv) -> dict:
    assert legs.main(list(argv)) == 0
    return json.loads(capsys.readouterr().out)


def test_the_plan_names_the_interpreter_the_gateway_will_use(capsys, tmp_path):
    config = tmp_path / "server-CT.toml"
    config.write_text(CONFIG, encoding="utf-8")
    plan = _plan(capsys, "--scenario", "S2", "--config", str(config),
                 "--dry-run")
    assert plan["inference_python"] == "/opt/cuda-venv/bin/python"
    assert plan["inference_python_source"] == "config"

    plan = _plan(capsys, "--scenario", "S2", "--config", str(config),
                 "--python", "/cpu/venv/bin/python", "--dry-run")
    assert plan["inference_python"] == "/cpu/venv/bin/python"
    assert plan["inference_python_source"] == "--python"
