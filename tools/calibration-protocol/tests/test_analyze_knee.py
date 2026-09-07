"""`calibration_learned`: a plateau knee is something learned, not nothing.

The check reads "peak `unit_budget` no higher than the first recorded" as
"nothing was learned". That is right for a ramp that never started and wrong
for a model that ends *below* its seed on purpose: rule 4 stops the ramp where
throughput stops improving, so a knee under the seed is the brake working, and
the worker then holds there, re-testing the plateau every so many clean
windows.

The MPS pass's S4a is the case, with its numbers used below: seed 64, knee
first learned at 3 and widened up to 15, budget running as low as 2 — reported
as `NOTHING WAS LEARNED: peak unit_budget never left the seed (seed 64, peak
64)` while the ledger was doing exactly the right thing (T6).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

ANALYZE = Path(__file__).resolve().parents[1] / "analyze.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_analyze_knee",
                                                  ANALYZE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze = _load()

MODEL = "tags/wd-vit-tagger-v3"
STORE = {"profile": [{"inference_id": MODEL}]}


def _context(series, after=STORE, learning=True, held=None,
             certified=False):
    """`series` is a list of `(unit_budget, knee_units)` health samples."""
    samples = [
        {"kind": "sample", "t_wall": 100.0 + index,
         "health": {"ok": True, "workers": [
             {"inference_id": MODEL, "unit_budget": budget,
              "knee_units": knee, "fit_samples": 10,
              "ramp_held": held is not None,
              "held_units": held,
              "held_certified": held is not None and certified}]}}
        for index, (budget, knee) in enumerate(series)
    ]
    args = argparse.Namespace(worker_pattern="inferio", join_tolerance=1.0,
                              probe=[], learning=learning,
                              explicit_checks=set())
    return analyze.Context(args=args, vramrec=[], healthrec=samples, hog=[],
                           log=[], before=None, after=after, jobs=None,
                           probes=[])


# The S4a-mps shape: the seed, then the brake, then the widening probes.
BRAKED = ([(64, None), (32, None), (16, None), (8, None)]
          + [(3, 3)] * 12 + [(7, 7)] * 4 + [(3, 3)] * 12 + [(15, 15)] * 3)


def test_a_budget_held_at_its_knee_is_learning():
    verdict = analyze.check_calibration_learned(_context(BRAKED))
    assert verdict.verdict == "PASS"
    assert "NOTHING WAS LEARNED" not in verdict.detail
    assert "held at a learned plateau knee" in verdict.detail


def test_the_detail_says_what_it_was_holding_at():
    verdict = analyze.check_calibration_learned(_context(BRAKED))
    assert "seed 64" in verdict.detail
    assert "knee first learned at 3" in verdict.detail
    assert "widened 2 time(s) up to 15" in verdict.detail
    assert "ran as low as 3" in verdict.detail
    row = verdict.numbers["models"][MODEL]
    assert (row["first"], row["peak"], row["low"]) == (64, 64, 3)
    assert (row["knee_first"], row["knee"], row["knee_widenings"]) == (3, 15, 2)


def test_a_ramp_that_never_started_still_fails():
    """No knee anywhere: the budget really did sit on the seed."""
    verdict = analyze.check_calibration_learned(
        _context([(64, None)] * 20))
    assert verdict.verdict == "FAIL"
    assert "peak unit_budget never left the seed" in verdict.detail


def test_a_knee_does_not_excuse_an_empty_store():
    verdict = analyze.check_calibration_learned(_context(BRAKED, after={}))
    assert verdict.verdict == "FAIL"
    assert "no [[profile]]" in verdict.detail


def test_a_knee_does_not_excuse_zero_fit_samples():
    ctx = _context(BRAKED)
    for sample in ctx.healthrec:
        sample["health"]["workers"][0]["fit_samples"] = 0
    verdict = analyze.check_calibration_learned(ctx)
    assert verdict.verdict == "FAIL"
    assert "fit samples == 0" in verdict.detail


def test_a_ramp_that_did_climb_is_unaffected():
    climbed = [(4, None), (8, None), (16, None), (32, None), (96, None)]
    verdict = analyze.check_calibration_learned(_context(climbed))
    assert verdict.verdict == "PASS"
    assert "held at a learned plateau knee" not in verdict.detail


def test_ramp_progress_does_not_blame_b16_for_a_knee_at_the_seed():
    """A model braked at 64 is not the REQUEST_UNIT_BUDGET symptom."""
    braked_at_64 = [(64, 64)] * 10
    verdict = analyze.check_ramp_progress(_context(braked_at_64))
    assert "finding B16" not in verdict.detail
    assert "knee 64" in verdict.detail
    # …while a peak of exactly 64 with no knee still earns the note.
    plain = analyze.check_ramp_progress(_context([(64, None)] * 10))
    assert "finding B16" in plain.detail


# --- the throughput brake, with no knee anywhere (round 3, D7) --------------
#
# `ramp_held` is a knee's statement made before any knee fits -- but only when
# `held_certified` says the ring measured that rung. A hold it cannot certify
# is the opposite statement: nothing was measured there. S2-text-loadgen is the
# shape (peak `unit_budget` never left the seed, the brake engaged, no knee),
# and it went green the moment the brake engaged for anything at all.


def test_a_budget_held_at_a_certified_rung_is_learning():
    verdict = analyze.check_calibration_learned(
        _context([(64, None)] * 20, held=64, certified=True))
    assert verdict.verdict == "PASS"
    assert "NOTHING WAS LEARNED" not in verdict.detail
    assert "held by the throughput brake at a rung the ring certified" \
        in verdict.detail
    assert "held at 64 for 20 sample(s)" in verdict.detail
    row = verdict.numbers["models"][MODEL]
    assert (row["held"], row["held_units"], row["held_certified"]) \
        == (20, 64, 20)


def test_a_budget_held_at_an_uncertified_rung_learned_nothing():
    """The text-loadgen shape: the brake engaged, and the ring never
    certified the rung it engaged at, so the leg measured nothing."""
    verdict = analyze.check_calibration_learned(
        _context([(64, None)] * 20, held=64))
    assert verdict.verdict == "FAIL"
    assert "peak unit_budget never left the seed" in verdict.detail
    assert "a rung the ring never certified" in verdict.detail
    row = verdict.numbers["models"][MODEL]
    assert (row["held"], row["held_certified"]) == (20, 0)


def test_an_unheld_ramp_that_never_started_still_fails():
    """The same series with the brake off: nothing was holding it back."""
    verdict = analyze.check_calibration_learned(_context([(64, None)] * 20))
    assert verdict.verdict == "FAIL"
    assert "peak unit_budget never left the seed" in verdict.detail
