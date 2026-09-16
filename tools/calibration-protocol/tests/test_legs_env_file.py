"""`KEY=` in an env file is an assignment of the empty string.

A leg masks every GPU with `CUDA_VISIBLE_DEVICES=`; if that line is read as
"no value" the gateway inherits the host's devices and a CPU-only leg runs on
a GPU (run4-deploy). The value also has to be visible afterwards, so the plan
carries the device-visibility variables the gateway was started with.

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
    spec = importlib.util.spec_from_file_location("_calib_legs_env", LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()


def _read(tmp_path: Path, text: str, base=None) -> dict:
    path = tmp_path / "env.CT"
    path.write_text(text, encoding="utf-8")
    return legs.read_env_file(path, {} if base is None else base)


def test_a_bare_empty_assignment_is_kept(tmp_path):
    out = _read(tmp_path, "CUDA_VISIBLE_DEVICES=\nRUST_LOG=info\n")
    assert out["CUDA_VISIBLE_DEVICES"] == ""
    assert out["RUST_LOG"] == "info"


def test_the_quoted_and_exported_forms_are_kept_too(tmp_path):
    out = _read(tmp_path, 'export CUDA_VISIBLE_DEVICES=""\nHIP_VISIBLE_DEVICES=\n')
    assert out == {"CUDA_VISIBLE_DEVICES": "", "HIP_VISIBLE_DEVICES": ""}


def test_an_empty_value_overrides_the_inherited_one(tmp_path):
    """The mask is the point: the file's empty value must win over the host's."""
    base = {"CUDA_VISIBLE_DEVICES": "0,1"}
    out = _read(tmp_path, "CUDA_VISIBLE_DEVICES=\n", base)
    merged = {**base, **out}
    assert merged["CUDA_VISIBLE_DEVICES"] == ""


def test_an_empty_substitution_default_is_a_value(tmp_path):
    out = _read(tmp_path, "CUDA_VISIBLE_DEVICES=${NOT_SET:-}\n")
    assert out["CUDA_VISIBLE_DEVICES"] == ""


def test_the_plan_records_the_mask(capsys, monkeypatch, tmp_path):
    """An empty mask must show up in `legs.json`, not vanish from it."""
    config = tmp_path / "server-CT.toml"
    config.write_text("[server]\nport = 16342\n", encoding="utf-8")
    (tmp_path / "env.CT").write_text("CUDA_VISIBLE_DEVICES=\n", encoding="utf-8")
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    monkeypatch.setenv("HIP_VISIBLE_DEVICES", "0")
    assert legs.main(["--scenario", "S2", "--config", str(config),
                      "--no-dotenv", "--dry-run"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["device_env"] == {"CUDA_VISIBLE_DEVICES": "",
                                  "HIP_VISIBLE_DEVICES": "0"}
