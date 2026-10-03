"""Each S5 fault-injection fixture is read against what it was written to inject.

One `analyze.py` command for the whole S5 table -- `--expect-ooms 1`, the
`oom_second_batch` fixture's figure -- would FAIL every other fixture for
working as designed. The thresholds come from the table, per fixture, and
`dies_on_load` carries the one declaration the zero-item rule needs: its
setter records no items by construction.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load("_calib_legs_s5", HERE / "legs.py")
analyze = _load("_calib_analyze_s5", HERE / "analyze.py")


# --- the table -------------------------------------------------------------


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


def test_s5_samples_health_often_enough_to_see_a_fixture_job():
    """A fixture's job can last 0.3 s; the default 0.5 s poll misses it."""
    assert legs.SCENARIOS["S5"].health_interval <= 0.1


# --- what analyze.py makes of them ------------------------------------------

#: Each count `--expect-*` flag, and the check that judges it.
COUNTS = {"--expect-ooms": "failures", "--expect-deaths": "failures",
          "--expect-failures": "job_outcome",
          "--expect-failed-jobs": "job_outcome"}


def _verdicts(directory: Path, model: str, expect, counts):
    """`failures` and `job_outcome` over a recording holding `counts`."""
    directory.mkdir()
    log = ["2026-10-03T00:00:00.000000Z  INFO panoptikon: started"]
    log += [f"2026-10-03T00:00:00.{index:06d}Z  WARN panoptikon::inferio::"
           f"ledger: settled a granted window model={model} outcome=negative "
           f"reason=oom" for index in range(counts["--expect-ooms"])]
    log += [f"2026-10-03T00:00:01.000000Z ERROR panoptikon::inferio: worker "
            f"died fatally model={model}"] * counts["--expect-deaths"]
    (directory / "panoptikon.log").write_text("\n".join(log) + "\n")
    items = 0 if "--expect-empty-setters" in expect else 1
    (directory / "jobs.json").write_text(json.dumps({
        "history": [{"setter": model, "total_segments": items,
                     "failed_items": counts["--expect-failures"]}],
        "outcomes": [{"status": "failed"}] * counts["--expect-failed-jobs"]}))
    out = directory / "verdicts.json"
    analyze.main(["--scenario", str(directory), "--checks",
                  "failures,job_outcome", "--quiet", "--json", str(out),
                  *expect])
    return {row["name"]: row["verdict"]
            for row in json.loads(out.read_text())["verdicts"]}


@pytest.mark.parametrize("name", sorted(legs.S5_FIXTURES))
def test_each_fixture_passes_at_its_thresholds_and_fails_one_past(tmp_path,
                                                                 name):
    model = f"calibfixture/{name}_cuda"
    expect = list(_leg(model).expectations())
    at = {flag: int(expect[expect.index(flag) + 1]) if flag in expect else 0
          for flag in COUNTS}
    # One OOM negative per item: every one of the smoke tier's 180 images.
    if name in ("oom", "oom_timed"):
        assert at["--expect-ooms"] == 180
    verdicts = _verdicts(tmp_path / "at", model, expect, at)
    assert verdicts["failures"] in ("PASS", "WARN")
    assert verdicts["job_outcome"] == "PASS"
    for flag, check in COUNTS.items():
        over = _verdicts(tmp_path / flag, model, expect,
                         {**at, flag: at[flag] + 1})
        assert over[check] == "FAIL", flag
