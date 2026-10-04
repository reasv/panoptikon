"""`--python` must reach the worker, not only the recorders.

The generated configs pin `[inference_local] python` to a CUDA venv, so a
`--python <cpu venv>` that reached only the recorders would leave the worker
on the GPU. The override wins, through a per-leg copy of the config, and the
leg says which interpreter the gateway will use.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
from pathlib import Path

LEGS = Path(__file__).resolve().parents[1] / "legs.py"
REPO = LEGS.parents[2]


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


def test_the_config_python_is_read_when_no_override_is_given():
    assert legs.config_inference_python(CONFIG) == "/opt/cuda-venv/bin/python"


def _plan(capsys, *argv) -> dict:
    assert legs.main(list(argv)) == 0
    return json.loads(capsys.readouterr().out)


def test_the_plan_names_the_interpreter_the_gateway_will_use(
        capsys, monkeypatch, tmp_path):
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

    # The gateway runs in `--root`: a relative path is made absolute, without
    # following the venv's symlink; a command name is left to PATH.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "venv" / "bin").mkdir(parents=True)
    (tmp_path / "venv" / "bin" / "python").symlink_to(sys.executable)
    for given, expected in ((str(Path("venv", "bin", "python")),
                             str(tmp_path / "venv" / "bin" / "python")),
                            ("python3", "python3")):
        plan = _plan(capsys, "--scenario", "S2", "--config", str(config),
                     "--python", given, "--dry-run")
        assert plan["inference_python"] == expected


def test_the_venv_interpreter_follows_the_os(monkeypatch, tmp_path):
    import tomllib

    monkeypatch.setattr(legs, "IS_WINDOWS", True)
    assert legs.venv_python(Path("v")) == Path("v", "Scripts", "python.exe")
    rendered = tomllib.loads(legs.render_config("C1", REPO))
    assert Path(rendered["inference_local"]["python"]) == (
        REPO / "python" / ".venv" / "Scripts" / "python.exe")
    # A Windows tree's venv is found where it is.
    (tmp_path / "config" / "server").mkdir(parents=True)
    shutil.copy(REPO / "config" / "server" / "default.toml",
                tmp_path / "config" / "server")
    (tmp_path / "python" / ".venv" / "Scripts").mkdir(parents=True)
    (tmp_path / "python" / ".venv" / "Scripts" / "python.exe").touch()
    legs.resolve_config(argparse.Namespace(config="C1", repo=str(tmp_path),
                                           dry_run=False), {})
    monkeypatch.setattr(legs, "IS_WINDOWS", False)
    assert legs.venv_python(Path("v")) == Path("v", "bin", "python")


def test_the_cudnn_path_follows_the_worker_venv(monkeypatch, tmp_path):
    """`LD_LIBRARY_PATH` names the `--python` venv's cuDNN, else the tree's,
    in the run's environment and in the env file `--write-config` writes, and
    is left out when that venv has none."""
    venv = tmp_path / "tree" / "python" / ".venv"
    tree_cudnn = venv / "lib" / "python3.11" / "site-packages" / "nvidia" / "cudnn" / "lib"
    tree_cudnn.mkdir(parents=True)
    assert legs.config_env("C1", tmp_path / "tree", {})["LD_LIBRARY_PATH"] == (
        str(tree_cudnn))

    gpu = tmp_path / "gpu"
    cudnn = gpu / "lib" / "python3.11" / "site-packages" / "nvidia" / "cudnn" / "lib"
    cudnn.mkdir(parents=True)
    python = str(gpu / "bin" / "python")
    assert legs.config_env("C1", REPO, {}, python)["LD_LIBRARY_PATH"] == str(cudnn)
    # A bare name is the interpreter it names on PATH.
    found = legs.venv_python(gpu)
    found.parent.mkdir()
    found.touch(mode=0o755)
    monkeypatch.setenv("PATH", str(found.parent))
    assert legs.config_env("C1", REPO, {}, found.name)["LD_LIBRARY_PATH"] == (
        str(cudnn))
    cpu = str(tmp_path / "cpu" / "bin" / "python")
    assert "LD_LIBRARY_PATH" not in legs.config_env("C1", REPO, {}, cpu)

    for given, expected in ((python, f"LD_LIBRARY_PATH={cudnn}\n"), (cpu, None)):
        out = tmp_path / "out"
        assert legs.main(["--config", "C1", "--repo", str(REPO), "--no-dotenv",
                          "--python", given, "--write-config", str(out)]) == 0
        written = (out / "env.C1").read_text(encoding="utf-8")
        assert (expected in written) if expected else (
            "LD_LIBRARY_PATH" not in written)
        other = cpu if given == python else python
        _, _, env, _ = legs.resolve_config(
            argparse.Namespace(config=str(out / "server-C1.toml")), {}, other)
        assert env.get("LD_LIBRARY_PATH") == (
            str(cudnn) if other == python else None)
