"""`calibration_learned`: a working size a trial left in place is something
learned, not nothing.

The check reads "peak `unit_budget` no higher than the first recorded" as
"nothing was learned". That is right for a batch size that never left the
seed and wrong for a model that ends *below* its seed on purpose: the batch
size is the smallest whose rate is within 10 % of the best a trial measured,
and the worker stays there, trying the sizes next to it every so often.

The case below: seed 64, a size first left in place at 3 and later at 7 and
15, budget running as low as 3 — which would read `NOTHING WAS LEARNED: peak
unit_budget never left the seed (seed 64, peak 64)` while the ledger was
doing exactly the right thing.

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


def _context(series, after=STORE, learning=True, local=True):
    """`series` is a list of `(unit_budget, knee_units)` health samples;
    `local` is what `/health` says of every `knee_units` among them."""
    samples = [
        {"kind": "sample", "t_wall": 100.0 + index,
         "health": {"ok": True, "workers": [
             {"inference_id": MODEL, "unit_budget": budget,
              "knee_units": knee, "fit_samples": 10,
              "knee_is_local": local and knee is not None}]}}
        for index, (budget, knee) in enumerate(series)
    ]
    args = argparse.Namespace(worker_pattern="inferio", join_tolerance=1.0,
                              probe=[], learning=learning,
                              explicit_checks=set())
    return analyze.Context(args=args, vramrec=[], healthrec=samples, hog=[],
                           log=[], before=None, after=after, jobs=None,
                           probes=[])


# The seed, then the size trials left in place.
BRAKED = ([(64, None), (32, None), (16, None), (8, None)]
          + [(3, 3)] * 12 + [(7, 7)] * 4 + [(3, 3)] * 12 + [(15, 15)] * 3)


def test_a_budget_at_a_size_a_trial_left_in_place_is_learning():
    verdict = analyze.check_calibration_learned(_context(BRAKED))
    assert verdict.verdict == "PASS"
    assert "NOTHING WAS LEARNED" not in verdict.detail
    assert "at a working size a trial left in place" in verdict.detail


def test_the_detail_says_what_it_was_holding_at():
    verdict = analyze.check_calibration_learned(_context(BRAKED))
    assert "seed 64" in verdict.detail
    assert "first left in place at 3" in verdict.detail
    assert "moved 3 time(s), at most 15" in verdict.detail
    assert "ran as low as 3" in verdict.detail
    row = verdict.numbers["models"][MODEL]
    assert (row["first"], row["peak"], row["low"]) == (64, 64, 3)
    assert (row["knee_first"], row["knee"], row["knee_moves"]) == (3, 15, 3)


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
    assert "at a working size a trial left in place" not in verdict.detail


def test_ramp_progress_does_not_blame_the_unit_budget_for_a_size_left_at_the_seed():
    """A model a trial left at 64 is not the REQUEST_UNIT_BUDGET symptom."""
    braked_at_64 = [(64, 64)] * 10
    verdict = analyze.check_ramp_progress(_context(braked_at_64))
    assert "REQUEST_UNIT_BUDGET" not in verdict.detail
    assert "knee 64" in verdict.detail
    # …while a peak of exactly 64 with no knee still earns the note.
    plain = analyze.check_ramp_progress(_context([(64, None)] * 10))
    assert "REQUEST_UNIT_BUDGET" in plain.detail


# --- a working size no trial has left in place --------------------------------
#
# `knee_units` is set the moment a replica opens and may come from a shipped
# profile; only `knee_is_local` says a trial measured the sizes next to it. A
# leg that sits at its seed without that has measured nothing.


def test_a_size_no_trial_left_in_place_learned_nothing():
    verdict = analyze.check_calibration_learned(
        _context([(64, 64)] * 20, local=False))
    assert verdict.verdict == "FAIL"
    assert "peak unit_budget never left the seed" in verdict.detail
    assert verdict.numbers["models"][MODEL]["knee"] == 0
    # …and it earns the REQUEST_UNIT_BUDGET note again.
    plain = analyze.check_ramp_progress(_context([(64, 64)] * 10, local=False))
    assert "REQUEST_UNIT_BUDGET" in plain.detail


def test_a_ramp_that_never_started_fails_without_any_working_size():
    verdict = analyze.check_calibration_learned(_context([(64, None)] * 20))
    assert verdict.verdict == "FAIL"
    assert "peak unit_budget never left the seed" in verdict.detail
