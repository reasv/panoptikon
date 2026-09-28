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


legs = _load("_calib_legs_s5", HERE / "legs.py")


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
