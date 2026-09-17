"""Each S5 fixture is read against what it was written to inject.

`legs.py` printed one `analyze.py` command for the whole S5 table --
`--expect-ooms 1`, the `oom_second_batch` fixture's figure -- so every other
fixture FAILed for working as designed, and each pass re-invented the
thresholds by hand (run4-sm120, ampere final §6, sm_120 final). They now come
from the table, per fixture, and `dies_on_load` carries the one declaration
the zero-item rule needs: its setter records no items by construction.

The thresholds are checked against the recordings run4-sm120 left in the
repository, which are the runs they were taken from.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
RUN4 = HERE / "results" / "run4-sm120"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load("_calib_legs_s5", HERE / "legs.py")
analyze = _load("_calib_analyze_s5", HERE / "analyze.py")


def _recorded_legs():
    """The S5 legs run4-sm120 left behind, as `(directory, model)`."""
    for directory in sorted(RUN4.glob("S5-*")):
        plan = directory / "legs.json"
        if plan.is_file():
            yield directory, json.loads(plan.read_text())["model"]


def _job_outcome(directory: Path, expect, tmp_path: Path) -> dict:
    out = tmp_path / f"{directory.name}.json"
    analyze.main(["--scenario", str(directory), "--checks", "job_outcome",
                  *expect, "--json", str(out), "--quiet"])
    return json.loads(out.read_text())["verdicts"][0]


# --- the table -------------------------------------------------------------


def test_every_fixture_the_legs_have_run_is_in_the_table():
    for _, model in _recorded_legs():
        assert legs.fixture_for(model) is not None, model


def test_the_cpu_twin_of_a_fixture_reads_the_same_row():
    """`_cuda` / `_cpu` differ in whether the ledger prices them."""
    assert (legs.fixture_for("calibfixture/oom_cpu")
            == legs.fixture_for("calibfixture/oom_cuda"))
    assert legs.fixture_for("tags/wd-vit-tagger-v3") is None


def test_only_dies_on_load_declares_that_it_extracts_nothing():
    declared = {name for name, fixture in legs.S5_FIXTURES.items()
                if fixture.no_items}
    assert declared == {"dies_on_load"}


# --- what legs.py prints ---------------------------------------------------


def _leg(model: str) -> "legs.Leg":
    return legs.Leg(args=None, scenario=legs.SCENARIOS["S5"],
                    directory=Path("."), python=sys.executable,
                    config_toml=Path("."), env={}, base="", total_mb=1,
                    supervisor=legs.Supervisor(1.0), models=(model,))


def test_each_fixture_gets_its_own_expectations_not_the_tables():
    flat = legs.SCENARIOS["S5"].expect
    assert _leg("calibfixture/dying_cuda").expectations() != flat
    assert _leg("calibfixture/oom_second_batch_cuda").expectations() == flat
    # A real model on S5 (MobileCLIP over `poison`) keeps the scenario's.
    assert _leg("tags/wd-vit-tagger-v3").expectations() == flat


def test_the_dies_on_load_leg_declares_its_empty_setter_both_ways():
    leg = _leg("calibfixture/dies_on_load_cuda")
    assert leg.expects_no_items() is True
    assert "--expect-empty-setters" in leg.expectations()
    assert not _leg("calibfixture/dying_cuda").expects_no_items()


# --- against the run4-sm120 recordings -------------------------------------


def test_every_recorded_leg_passes_on_its_own_fixtures_expectations(tmp_path):
    """The stock command, on the run it was calibrated against."""
    for directory, model in _recorded_legs():
        if directory.name == "S5-oomtimed-long":
            continue  # 2 000 items, not the table's 180-item smoke tier
        verdict = _job_outcome(directory,
                               legs.fixture_for(model).expect, tmp_path)
        assert verdict["verdict"] == "PASS", (directory.name, verdict["detail"])


def test_a_leg_on_a_bigger_corpus_raises_the_threshold_by_hand(tmp_path):
    directory = RUN4 / "S5-oomtimed-long"
    stock = list(legs.fixture_for("calibfixture/oom_timed_cuda").expect)
    assert _job_outcome(directory, stock, tmp_path)["verdict"] == "FAIL"
    raised = list(stock)
    raised[raised.index("--expect-failures") + 1] = "2000"
    assert _job_outcome(directory, raised, tmp_path)["verdict"] == "PASS"


def test_the_dies_on_load_recording_is_a_pass_and_was_a_fail(tmp_path):
    directory = RUN4 / "S5-dieonload"
    fixture = legs.fixture_for("calibfixture/dies_on_load_cuda")
    passed = _job_outcome(directory, fixture.expect, tmp_path)
    assert passed["verdict"] == "PASS"
    assert passed["numbers"]["empty_jobs"] == ["calibfixture/dies_on_load_cuda"]
    without = [flag for flag in fixture.expect
               if flag != "--expect-empty-setters"]
    assert _job_outcome(directory, without, tmp_path)["verdict"] == "FAIL"
