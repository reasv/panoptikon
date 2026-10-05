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
import tomllib
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


def test_oom_timed_ooms_on_its_first_predicts_then_succeeds():
    """A count, not a time: every recording longer than it shows recovery."""
    registry = tomllib.loads((HERE / "fixtures" / "registry" /
                              "calibration-fixtures.toml").read_text())
    ids = registry["group"]["calibfixture"]["inference_ids"]
    # Short of the smoke tier, which leaves items for the recovery, and the
    # leg's OOM threshold.
    oom_predicts = ids["oom_timed_cuda"]["config"]["oom_predicts"]
    assert 0 < oom_predicts < legs.SMOKE_IMAGES
    expect = legs.S5_FIXTURES["oom_timed"].expect
    assert int(expect[expect.index("--expect-ooms") + 1]) == oom_predicts == 20
    pytest.importorskip("torch")
    impl = _load("_calib_oom_timed",
                 HERE / "fixtures" / "impls" / "oom_timed_cuda_impl.py")
    model = impl.IMPL_CLASS(oom_predicts=2)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="INFERENCE_OOM_BATCH_SIZE_1"):
            model.predict([0])
    assert model.predict([0, 1]) == [{"batch": 2}] * 2


# --- what legs.py prints ---------------------------------------------------


def _leg(model: str) -> "legs.Leg":
    return legs.Leg(args=None, scenario=legs.SCENARIOS["S5"],
                    directory=Path("."), python=sys.executable,
                    config_toml=Path("."), env={}, base="", total_mb=1,
                    supervisor=legs.Supervisor(1.0), models=(model,))


def test_each_fixture_gets_its_own_expectations_not_the_tables(tmp_path):
    assert (_leg("calibfixture/dying_cuda").expectations()
            == legs.S5_FIXTURES["dying"].expect)
    assert (_leg("calibfixture/oom_second_batch_cuda").expectations()
            == legs.S5_FIXTURES["oom_second_batch"].expect)
    # A real model on S5 declares no fault: an OOM over `poison` is a finding.
    real = "tags/wd-vit-tagger-v3"
    expect = _leg(real).expectations()
    assert expect == ()
    assert _verdicts(tmp_path / "real", real, expect,
                     _thresholds(expect))["failures"] == "PASS"


def test_the_dies_on_load_leg_declares_its_empty_setter_both_ways():
    leg = _leg("calibfixture/dies_on_load_cuda")
    assert leg.expects_no_items() is True
    assert "--expect-empty-setters" in leg.expectations()
    assert not _leg("calibfixture/dying_cuda").expects_no_items()


# --- what analyze.py makes of them ------------------------------------------

#: Each count `--expect-*` flag, and the check that judges it.
COUNTS = {"--expect-ooms": "failures", "--expect-deaths": "failures",
          "--expect-failures": "job_outcome",
          "--expect-failed-jobs": "job_outcome"}


def _verdicts(directory: Path, model: str, expect, counts,
              job_ends=("drained",), priced=True):
    """`failures`, `job_outcome` and `deflation_recovery` over a recording
    holding `counts`, a worker deflated with no clean window, and one job
    per `job_ends` entry in legs.json (None: no `job_end`)."""
    directory.mkdir()
    log = ["2026-10-03T00:00:00.000000Z  INFO panoptikon: started"]
    log += [f"2026-10-03T00:00:00.000000Z DEBUG panoptikon::inferio::ledger: "
            f"issued a memory grant model={model}"] * priced
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
    # The model is unloaded once its job ends.
    (directory / "healthrec.jsonl").write_text("".join(json.dumps(
        {"kind": "sample", "t_wall": t, "health": {"workers": workers}}) + "\n"
        for t, workers in ((1.0, [{"inference_id": model, "gpu_uuid": "GPU-0",
                                   "deflation": 1}]), (2.0, []))))
    events = []
    for outcome in job_ends:
        events.append({"event": "job_start"})
        if outcome:
            events.append({"event": "job_end", "outcome": outcome})
    (directory / "legs.json").write_text(json.dumps({"events": events}))
    out = directory / "verdicts.json"
    analyze.main(["--scenario", str(directory), "--checks",
                  "failures,job_outcome,deflation_recovery", "--quiet",
                  "--json", str(out), *expect])
    return {row["name"]: row["verdict"]
            for row in json.loads(out.read_text())["verdicts"]}


def _thresholds(expect):
    return {flag: int(expect[expect.index(flag) + 1]) if flag in expect else 0
            for flag in COUNTS}


@pytest.mark.parametrize("name", sorted(legs.S5_FIXTURES))
def test_each_fixture_passes_at_its_thresholds_and_fails_one_past(tmp_path,
                                                                 name):
    model = f"calibfixture/{name}_cuda"
    expect = list(_leg(model).expectations())
    at = _thresholds(expect)
    # One OOM negative per item: every image of the smoke tier.
    if name == "oom":
        assert at["--expect-ooms"] == legs.SMOKE_IMAGES == 180
    # A failed-item threshold is the item count.
    assert at["--expect-failures"] in (0, legs.SMOKE_IMAGES)
    verdicts = _verdicts(tmp_path / "at", model, expect, at)
    assert verdicts["failures"] == "PASS"
    assert verdicts["job_outcome"] == "PASS"
    # Only `oom` declares that it ends deflated.
    assert ("--expect-deflated" in expect) == (name == "oom")
    assert verdicts["deflation_recovery"] == ("PASS" if name == "oom"
                                              else "WARN")
    for flag, check in COUNTS.items():
        over = _verdicts(tmp_path / flag, model, expect,
                         {**at, flag: at[flag] + 1})
        assert over[check] == "FAIL", flag
    # A declared fault that never fired; once is enough.
    for flag in ("--expect-ooms", "--expect-deaths"):
        if at[flag]:
            none = _verdicts(tmp_path / f"none{flag}", model, expect,
                             {**at, flag: 0})
            assert none["failures"] == "FAIL", flag
            once = _verdicts(tmp_path / f"once{flag}", model, expect,
                             {**at, flag: 1})
            assert once["failures"] == "PASS", flag
    # Unpriced (a `_cpu` twin on a GPU host), it settles no window, but a
    # death is still logged.
    unpriced = _verdicts(tmp_path / "unpriced", model, expect,
                         {**at, "--expect-ooms": 0, "--expect-deaths": 0},
                         priced=False)
    assert unpriced["failures"] == ("FAIL" if at["--expect-deaths"]
                                    else "PASS")


def test_a_job_that_did_not_drain_fails_job_outcome(tmp_path):
    """A job cut at `--job-cap`, or one legs.py never saw end."""
    model = "calibfixture/oom_second_batch_cuda"
    expect = list(_leg(model).expectations())
    for ends in (["cap_exceeded"], [None]):
        verdicts = _verdicts(tmp_path / f"ends-{ends[0]}", model, expect,
                             _thresholds(expect), ends)
        assert verdicts["job_outcome"] == "FAIL"
