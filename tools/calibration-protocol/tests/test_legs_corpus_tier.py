"""A corpus directory is not a tier, and `--corpus` means what it says.

`corpus_complaint` compared the manifest's stamped TIER against
`Scenario.corpus`, a DIRECTORY name. S4b/S4c/S4d declare `ramp8` -- the
README's `corpus.py --tier ramp --scale 8`, stamped `ramp` -- so all three
legs were unstartable and the remedy printed (`--tier ramp8`) is rejected by
argparse. The check also ignored `--corpus`, so `S5-oomimpl` on `poison` and
`S5-oomtimed-long` on `ramp8` were refused for using the documented flag
(ampere final T2, sm_120 final T2).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load("_calib_legs_tier", HERE / "legs.py")
corpus_py = _load("_calib_corpus_tier", HERE / "corpus.py")


def _stamped(directory: Path, tier: str, generator=None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text(
        json.dumps({"tier": tier,
                    "generator": legs.CORPUS_GENERATOR
                    if generator is None else generator}),
        encoding="utf-8")
    return directory


def _remedy(complaint: str) -> list:
    command = re.search(r"`([^`]+)`", complaint)
    assert command, complaint
    return shlex.split(command.group(1))[1:]


# --- a directory name maps to a recipe -------------------------------------


def test_a_scale_suffix_is_the_scale_not_part_of_the_tier():
    assert legs.corpus_recipe("ramp8") == ("ramp", "8")
    assert legs.corpus_recipe("ramp4") == ("ramp", "4")
    assert legs.corpus_recipe("smoke") == ("smoke", None)
    assert legs.corpus_recipe("8") == ("8", None)


def test_every_scenarios_corpus_maps_to_a_tier_corpus_py_knows():
    for scenario in legs.SCENARIOS.values():
        tier, _ = legs.corpus_recipe(scenario.corpus)
        assert tier in corpus_py.TIERS, scenario.key


# --- S4b/S4c/S4d: the README's own ramp8 recipe ----------------------------


def test_the_readme_ramp8_recipe_is_accepted_for_the_legs_that_declare_it(
        tmp_path):
    """`corpus.py --tier ramp --scale N --out .../ramp8`, stamped `ramp`."""
    out = tmp_path / "ramp8"
    assert corpus_py.main(["--tier", "ramp", "--scale", "0.005", "--out",
                           str(out), "--jobs", "1"]) == 0
    assert json.loads((out / "manifest.json").read_text())["tier"] == "ramp"
    for key in ("S4b", "S4c", "S4d"):
        assert legs.corpus_complaint(out, legs.SCENARIOS[key].corpus) is None


def test_a_ramp8_corpus_of_the_wrong_tier_is_still_refused(tmp_path):
    out = _stamped(tmp_path / "ramp8", "smoke")
    complaint = legs.corpus_complaint(out, "ramp8")
    assert complaint and "'smoke'" in complaint and "'ramp'" in complaint


def test_the_remedy_for_a_ramp8_corpus_is_a_command_corpus_py_accepts(
        tmp_path):
    complaint = legs.corpus_complaint(tmp_path / "ramp8", "ramp8")
    assert complaint and "--tier ramp --scale 8" in complaint
    argv = _remedy(complaint)
    assert corpus_py.main(argv + ["--dry-run"]) == 0


# --- --corpus names the corpus ---------------------------------------------


def test_an_explicit_corpus_is_judged_by_its_own_tier(tmp_path):
    """S5 declares `smoke`; `--corpus .../poison` is the documented flag."""
    poison = _stamped(tmp_path / "poison", "poison")
    assert legs.corpus_complaint(poison, legs.SCENARIOS["S5"].corpus)
    assert legs.corpus_complaint(poison, None) is None


def test_an_explicit_ramp8_corpus_is_accepted_for_a_smoke_scenario(tmp_path):
    """S5-oomtimed-long runs the S5 fixture over the ramp corpus."""
    assert legs.corpus_complaint(_stamped(tmp_path / "ramp8", "ramp"),
                                 None) is None


def test_an_explicit_corpus_is_still_refused_when_it_is_stale(tmp_path):
    out = _stamped(tmp_path / "ramp8", "ramp", generator=1)
    complaint = legs.corpus_complaint(out, None)
    assert complaint and "version 1" in complaint
    assert "--tier ramp --scale 8" in complaint
    assert corpus_py.main(_remedy(complaint) + ["--dry-run"]) == 0


def test_a_stale_scenario_corpus_is_refused_with_a_runnable_remedy(tmp_path):
    out = _stamped(tmp_path / "smoke", "smoke", generator=1)
    complaint = legs.corpus_complaint(out, "smoke")
    assert complaint and "version 1" in complaint
    assert corpus_py.main(_remedy(complaint) + ["--dry-run"]) == 0


# --- the leg reads --corpus the same way -----------------------------------


def test_the_leg_does_not_hold_an_explicit_corpus_to_the_scenarios_tier(
        tmp_path):
    poison = _stamped(tmp_path / "poison", "poison")
    plan = subprocess.run(
        [sys.executable, str(HERE / "legs.py"), "--scenario", "S5",
         "--corpus", str(poison), "--dry-run"],
        capture_output=True, text=True, check=True)
    assert json.loads(plan.stdout)["corpus"] == str(poison)
    assert legs.corpus_complaint(poison, None) is None
