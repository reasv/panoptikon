"""Two checks that must SKIP rather than FAIL on a missing input.

`oracle_agreement` subtracts our workers' per-process usage from the GPU's
total. On WDDM nothing prices a process (NVML and `nvidia-smi` both answer
`[N/A]` for every PID), so the subtraction returns
the whole GPU and the "disagreement" is exactly our own footprint —
a recording gap, not a ledger fault. `slope_accuracy` has the same shape:
a probe for a model the leg never ran says the harness passed the wrong
file, not that the ledger learned a wrong slope.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from pathlib import Path

import pytest

ANALYZE = Path(__file__).resolve().parents[1] / "analyze.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_analyze", ANALYZE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze = _load()

GPU = "GPU-0000"


def _args(**overrides):
    values = {"worker_pattern": "inferio", "join_tolerance": 1.0, "probe": []}
    values.update(overrides)
    return argparse.Namespace(**values)


def _context(vramrec=(), healthrec=(), after=None, probes=()):
    return analyze.Context(
        args=_args(), vramrec=list(vramrec), healthrec=list(healthrec),
        hog=[], log=[], before=None, after=after, jobs=None,
        probes=list(probes))


def _vram_sample(procs, used_mb=20000, source="nvml"):
    return {"kind": "sample", "t_wall": 100.0,
            "gpus": [{"index": 0, "uuid": GPU, "total_mb": 32607,
                      "used_mb": used_mb, "free_mb": 32607 - used_mb,
                      "oracle_source": source, "procs": procs}]}


def _health_sample(external_mb):
    return {"kind": "sample", "t_wall": 100.0, "iso": "2026-09-06T07:12:09Z",
            "health": {"ok": True,
                       "vram": [{"gpu_uuid": GPU, "total_mb": 32607,
                                 "external_known": True,
                                 "external_mb": external_mb}]}}


def _proc(pid, used_mb, cmdline):
    return {"pid": pid, "used_mb": used_mb, "cmdline": cmdline, "env": {}}


# --- oracle_agreement ------------------------------------------------------


def test_oracle_agreement_skips_when_the_oracle_priced_no_pid():
    """The WDDM recording: process rows exist, every `used_mb` is null."""
    procs = [_proc(4, None, None), _proc(900, None, "inferio-worker")]
    ctx = _context(vramrec=[_vram_sample(procs, source="nvidia-smi")],
                   healthrec=[_health_sample(19000)])
    verdict = analyze.check_oracle_agreement(ctx)
    assert verdict.verdict == "SKIP"
    assert "priced no PID" in verdict.detail
    assert verdict.numbers["unpriced_samples"] == 1
    assert verdict.numbers["oracle_sources"] == {"nvidia-smi": 1}


def test_oracle_agreement_skips_when_there_are_no_process_rows():
    ctx = _context(vramrec=[_vram_sample([], source="none")],
                   healthrec=[_health_sample(19000)])
    assert analyze.check_oracle_agreement(ctx).verdict == "SKIP"


def test_oracle_agreement_still_passes_where_the_oracle_prices():
    procs = [_proc(4, 19000, None), _proc(900, 1000, "inferio-worker")]
    ctx = _context(vramrec=[_vram_sample(procs)],
                   healthrec=[_health_sample(19000)])
    verdict = analyze.check_oracle_agreement(ctx)
    assert verdict.verdict == "PASS"
    assert verdict.numbers["joined"] == 1
    assert verdict.numbers["unpriced_samples"] == 0


def test_oracle_agreement_still_fails_a_real_disagreement():
    procs = [_proc(4, 19000, None), _proc(900, 1000, "inferio-worker")]
    ctx = _context(vramrec=[_vram_sample(procs)],
                   healthrec=[_health_sample(4000)])
    assert analyze.check_oracle_agreement(ctx).verdict == "FAIL"


def test_an_idle_gpu_counts_as_priced():
    """Nothing on the GPU is not a missing attribution."""
    assert analyze.oracle_prices_pids({"used_mb": 0, "procs": []}) is True


# --- slope_accuracy --------------------------------------------------------


def _probe(model, slope):
    return {"model": model, "fit": {"basis": "peak_allocated_mb",
                                    "slope_mb_per_unit": slope}}


def _after(model, slope):
    return {"profile": [{"inference_id": model, "slope_mb_per_unit": slope,
                         "base_mb": 800}]}


def _cost_health(model, canvas=None, max_tokens=None):
    return {"kind": "sample", "t_wall": 100.0, "iso": "2026-09-06T07:12:09Z",
            "health": {"ok": True, "models": [
                {"inference_id": model, "cost_canvas_pixels": canvas,
                 "cost_max_tokens": max_tokens}]}}


def test_slope_accuracy_skips_when_the_probe_is_for_another_model():
    ctx = _context(after=_after("tags/wd-vit-tagger-v3", 2.0),
                   probes=[_probe("clip/ViT-L", 2.0)])
    verdict = analyze.check_slope_accuracy(ctx)
    assert verdict.verdict == "SKIP"
    assert "tags/wd-vit-tagger-v3" in verdict.detail
    assert verdict.numbers["compared"] == 0


def test_slope_accuracy_judges_the_model_that_is_present():
    ctx = _context(after=_after("tags/wd-vit-tagger-v3", 2.0),
                   probes=[_probe("tags/wd-vit-tagger-v3", 2.0),
                           _probe("clip/ViT-L", 2.0)])
    verdict = analyze.check_slope_accuracy(ctx)
    assert verdict.verdict == "PASS"
    assert verdict.numbers["compared"] == 1


def test_slope_accuracy_still_fails_a_wrong_slope():
    ctx = _context(after=_after("tags/wd-vit-tagger-v3", 0.1),
                   probes=[_probe("tags/wd-vit-tagger-v3", 2.0)])
    assert analyze.check_slope_accuracy(ctx).verdict == "FAIL"


def test_slope_accuracy_names_a_denomination_mismatch():
    """A probe that priced a token model uncapped while the host priced it at
    its window is comparing two currencies, and the ratio says nothing."""
    model = "textembed/all-MiniLM-L6-v2"
    probe = _probe(model, 2.0)
    probe["cost"] = {"canvas_pixels_in_force": None, "max_tokens_in_force": None}
    ctx = _context(after=_after(model, 2.0),
                   healthrec=[_cost_health(model, max_tokens=256)],
                   probes=[probe])
    verdict = analyze.check_slope_accuracy(ctx)
    assert "denomination mismatch" in verdict.detail
    assert "max_tokens=256" in verdict.detail

    probe["cost"]["max_tokens_in_force"] = 256
    agreed = analyze.check_slope_accuracy(
        _context(after=_after(model, 2.0),
                 healthrec=[_cost_health(model, max_tokens=256)],
                 probes=[probe]))
    assert agreed.verdict == "PASS"
    assert "denomination" not in agreed.detail


# --- ledger_invariant ------------------------------------------------------


def _ledger_health(t_wall, charges_mb, limit_mb, load_reservations_mb=0):
    return {"kind": "sample", "t_wall": t_wall,
            "iso": "2026-09-06T07:12:09Z",
            "health": {"ok": True,
                       "vram": [{"gpu_uuid": GPU, "total_mb": 24576,
                                 "charges_mb": charges_mb,
                                 "load_reservations_mb": load_reservations_mb,
                                 "limit_mb": limit_mb, "external_mb": 2000}]}}


def _grant(t_wall, mb, headroom_mb):
    return {"ts": "2026-09-06T07:12:09.000000Z", "t_wall": t_wall,
            "level": "DEBUG", "target": "panoptikon::inferio::ledger",
            "message": "issued a memory grant",
            "fields": {"model": "tags/wd-vit-tagger-v3", "gpu": GPU,
                       "unit_budget": 4, "mb": mb, "headroom_mb": headroom_mb},
            "line": ""}


def _ledger_context(healthrec, log=()):
    ctx = _context(healthrec=healthrec)
    ctx.log = list(log)
    return ctx


def test_ledger_invariant_passes_with_no_breach():
    ctx = _ledger_context([_ledger_health(100.0, 8000, 12000)])
    assert analyze.check_ledger_invariant(ctx).verdict == "PASS"


def test_a_breach_with_no_over_headroom_grant_is_limit_fell():
    """The 24 GB shape: external rose under a footprint we already held."""
    ctx = _ledger_context(
        [_ledger_health(100.0, 8000, 12000),
         _ledger_health(101.0, 8000, 6000)],
        log=[_grant(100.5, 500, 4000)])
    verdict = analyze.check_ledger_invariant(ctx)
    assert verdict.verdict == "WARN"
    assert verdict.numbers["breach_classes"] == {"over_grant": 0,
                                                 "limit_fell": 1}
    assert verdict.numbers["breaches"][0]["class"] == "limit_fell"


def test_a_breach_with_an_over_headroom_grant_in_it_fails():
    ctx = _ledger_context(
        [_ledger_health(100.0, 8000, 12000),
         _ledger_health(101.0, 8000, 6000)],
        log=[_grant(100.5, 5000, 4000)])
    verdict = analyze.check_ledger_invariant(ctx)
    assert verdict.verdict == "FAIL"
    assert verdict.numbers["breach_classes"] == {"over_grant": 1,
                                                 "limit_fell": 0}
    assert verdict.numbers["grants_over_headroom"] == 1


def test_an_over_headroom_grant_outside_the_sample_does_not_class_it():
    """The grant must fall in the window the breaching sample closes."""
    ctx = _ledger_context(
        [_ledger_health(100.0, 8000, 12000),
         _ledger_health(101.0, 8000, 6000),
         _ledger_health(102.0, 8000, 6000)],
        log=[_grant(100.5, 5000, 4000)])
    verdict = analyze.check_ledger_invariant(ctx)
    assert verdict.numbers["breach_classes"] == {"over_grant": 1,
                                                 "limit_fell": 1}


# --- utilization -----------------------------------------------------------


MODEL = "tags/wd-vit-tagger-v3"


def _worker_health(unit_budget):
    return {"kind": "sample", "t_wall": 100.0, "iso": "2026-09-06T07:12:09Z",
            "health": {"ok": True,
                       "workers": [{"inference_id": MODEL,
                                    "unit_budget": unit_budget}]}}


def _budget_grant(unit_budget):
    return {"ts": "2026-09-06T07:12:09.000000Z", "t_wall": 100.0,
            "level": "DEBUG", "target": "panoptikon::inferio::ledger",
            "message": "issued a memory grant",
            "fields": {"model": MODEL, "gpu": GPU,
                       "unit_budget": unit_budget, "mb": 38,
                       "headroom_mb": 1379},
            "line": ""}


def _utilization_context(healthrec, log=(), probes=(), hog=(), vramrec=()):
    ctx = analyze.Context(
        args=_args(utilization_floor=0.25), vramrec=list(vramrec),
        healthrec=list(healthrec), hog=list(hog), log=list(log), before=None,
        after=None, jobs=None, probes=list(probes))
    return ctx


def _bisect_probe(boundary):
    return {"model": MODEL, "bisect": {"largest_ok_units": boundary}}


def test_utilization_scores_the_granted_budget_not_the_published_one():
    """The S4a shape: 512 published, every window issued 1 unit."""
    ctx = _utilization_context([_worker_health(512)],
                               log=[_budget_grant(1), _budget_grant(1)],
                               probes=[_bisect_probe(639)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "FAIL"
    row = verdict.numbers["models"][0]
    assert (row["peak_unit_budget"], row["published_unit_budget"]) == (1, 512)
    assert row["source"] == "grant"


def test_utilization_falls_back_to_the_published_budget_and_says_so():
    ctx = _utilization_context([_worker_health(512)],
                               probes=[_bisect_probe(639)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "PASS"
    assert verdict.numbers["models"][0]["source"] == "published"
    assert "no grant lines" in verdict.detail


# --- utilization and a working size a trial left in place ------------------
#
# A batch grows only while its rate does, so a leg can stop at 64 units
# against a 512-unit probe boundary and score 0.12 — a FAIL for obeying the
# design, while `calibration_learned` on the same recording reads the size a
# trial left in place as learning.


def _trial_over(units, moved=False, largest=64):
    fields = {"model": MODEL, "gpu": GPU, "units": units, "moved": moved,
              "retest_after_windows": 12}
    if largest is not None:
        fields["largest_units"] = largest
    return {"ts": "2026-09-07T03:33:40.929674Z", "t_wall": 100.0,
            "level": "INFO", "target": "panoptikon::inferio::ledger::ramp",
            "message": "a batch size trial is over", "fields": fields,
            "line": ""}


def _settle(max_units_measured):
    return {"ts": "2026-09-07T03:33:40.929688Z", "t_wall": 100.0,
            "level": "DEBUG", "target": "panoptikon::inferio::ledger",
            "message": "settled a granted window",
            "fields": {"model": MODEL, "gpu": GPU, "outcome": "clean",
                       "max_units_measured": max_units_measured},
            "line": ""}


def test_utilization_scores_a_size_left_in_place_against_the_largest_it_ran():
    """64 issued, working size 3, largest trial size 64, probe boundary 512;
    the settle lines' seeded anchor of 256 is not a size that ran."""
    ctx = _utilization_context(
        [_worker_health(64)],
        log=[_budget_grant(64), _trial_over(3), _settle(256)],
        probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "PASS"
    row = verdict.numbers["models"][0]
    assert (row["knee_units"], row["held_rung_units"]) == (3, 64)
    assert (row["denominator_units"], row["ratio"]) == (64, 1.0)
    assert "held at knee_units=3, rung 64" in verdict.detail


def test_a_batch_size_that_stopped_short_of_the_boundary_still_fails():
    """The same numbers with no trial to show for them."""
    ctx = _utilization_context([_worker_health(64)],
                               log=[_budget_grant(64)],
                               probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "FAIL"
    row = verdict.numbers["models"][0]
    assert row["knee_units"] is None
    assert row["denominator_units"] == 512
    assert "held at" not in verdict.detail


def test_a_size_resumed_from_the_store_is_read_from_health_alone():
    """A second process may run no trial: the size is only in `/health`."""
    health = _worker_health(31)
    health["health"]["workers"][0].update(knee_units=7, knee_is_local=True)
    ctx = _utilization_context([health], log=[_budget_grant(31), _settle(64)],
                               probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "PASS"
    assert verdict.numbers["models"][0]["denominator_units"] == 7


def test_a_size_no_trial_left_in_place_is_scored_against_the_boundary():
    """`knee_units` without `knee_is_local` (the size a replica opened at, a
    shipped one, or one a trial just moved) lowers no bar: a replica stuck at
    2 units is a FAIL, as is a trial that moved the size."""
    health = _worker_health(2)
    health["health"]["workers"][0].update(knee_units=2, knee_is_local=False)
    ctx = _utilization_context([health],
                               log=[_budget_grant(2), _trial_over(2, True)],
                               probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "FAIL"
    row = verdict.numbers["models"][0]
    assert (row["knee_units"], row["denominator_units"]) == (None, 512)


def test_the_denominator_never_exceeds_the_probe_boundary():
    """A size above the OOM boundary would score against absent memory."""
    ctx = _utilization_context([_worker_health(64)],
                               log=[_budget_grant(64),
                                    _trial_over(3, largest=4096)],
                               probes=[_bisect_probe(512)])
    row = analyze.check_utilization(ctx).numbers["models"][0]
    assert row["denominator_units"] == 512
    assert "capped at the probe boundary 512" in \
        analyze.check_utilization(ctx).detail


def test_the_denominator_is_bounded_by_the_room_a_hog_leaves():
    """Room = probe boundary - (least hog held + context + least grant
    reserve) / reserved slope, while the model ran, on its GPU."""
    probe = {**_bisect_probe(512),
             "fit": {"basis": "peak_allocated_mb", "slope_mb_per_unit": 10.0},
             "fit_reserved": {"basis": "delta_mb", "slope_mb_per_unit": 20.0}}

    def utilization(target="gpu", held=3800, gpus=(GPU, GPU), unified=False,
                    hog_gpu=GPU, fit=None, states=None, logged=True,
                    extra_gpus=(), trial=False, budget=100, reserves=()):
        states = states or [(10.0, 0), (50.0, held + 400), (110.0, held),
                            (150.0, 0)]
        hog = [{"kind": "header", "target": target, "gpu_uuid": hog_gpu,
                "context_mb": 200}] + [
            {"kind": "state", "t_wall": t_wall, "held_mb": held_mb}
            for t_wall, held_mb in states]
        log = [{**_budget_grant(budget), "t_wall": t_wall,
                "fields": {**_budget_grant(budget)["fields"], "gpu": gpu,
                           **reserve}}
               for t_wall, gpu, reserve in zip(
                   (100.0, 120.0), gpus,
                   [{"reserve_mb": mb} for mb in reserves] or [{}, {}])]
        log += [_trial_over(3)] if trial else []
        healthrec = [_worker_health(100)]
        if not logged:
            log, healthrec = [], [{**_worker_health(100), "t_wall": t_wall}
                                  for t_wall in (100.0, 120.0)]
            for sample in healthrec:
                sample["health"]["workers"][0]["gpu_uuid"] = GPU
        vramrec = [{"kind": "header", "gpus": [
            {"uuid": GPU, "unified": unified}, *extra_gpus]}]
        verdict = analyze.check_utilization(_utilization_context(
            healthrec, log=log, vramrec=vramrec, hog=hog,
            probes=[{**_bisect_probe(512), "fit": fit} if fit else probe]))
        return (verdict.verdict,
                verdict.numbers["models"][0]["denominator_units"])

    mps = analyze.MPS_DEVICE_KEY
    assert utilization() == ("PASS", 312)
    assert utilization(trial=True) == ("PASS", 64)
    # The least grant reserve comes off too: 512 - (3800 + 200 + 1024) / 20.
    assert utilization(budget=70) == ("FAIL", 312)
    assert utilization(budget=70, reserves=(1100, 1024)) == ("PASS", 260)
    assert utilization(gpus=(GPU, "GPU-1111")) == ("FAIL", 512)
    assert utilization(hog_gpu="GPU-1111") == ("FAIL", 512)
    assert utilization(target="ram") == ("FAIL", 512)
    assert utilization(target="ram", unified=True) == ("PASS", 322)
    assert utilization(target="ram", gpus=(mps, mps)) == ("PASS", 322)
    assert utilization(fit={"slope_mb_per_unit": 20.0}) == ("PASS", 312)
    assert utilization(fit={"basis": "peak_allocated_mb",
                            "slope_mb_per_unit": 10.0}) == ("INFO", None)
    assert utilization(held=20000) == ("INFO", None)
    constant = [(10.0, 0), (50.0, 3800), (150.0, 0)]
    assert utilization(states=constant) == ("PASS", 312)
    assert utilization(states=constant, logged=False) == ("PASS", 312)
    assert utilization(target="ram", extra_gpus=(
        {"uuid": "GPU-APU", "unified": True},)) == ("FAIL", 512)


def test_a_size_with_no_trial_size_falls_back_to_itself():
    ctx = _utilization_context([_worker_health(8)],
                               log=[_budget_grant(8),
                                    _trial_over(15, largest=None)],
                               probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    row = verdict.numbers["models"][0]
    assert (row["held_rung_units"], row["denominator_units"]) == (None, 15)
    assert "held at knee_units=15 =" in verdict.detail


def test_the_probeless_leg_still_skips_with_a_size_left_in_place():
    ctx = _utilization_context([_worker_health(64)],
                               log=[_budget_grant(64), _trial_over(3)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "SKIP"
    assert "no probe boundary for any model" in verdict.detail


# --- the macOS oracle: a source that prices nothing by construction --------
#
# `vramrec.py`'s darwin branch records `oracle_source: "mps-ram"` — host RAM,
# the GPU wired limit and our workers' RSS, because Apple Silicon has no
# per-process GPU counter for any instrument to read. All three per-process
# checks must SKIP on it, as they do on WDDM, rather than subtracting a zero
# attribution from a real GPU figure and reporting our own workers as the
# disagreement.

MPS = "GPU-MPS"


def _mps_vram(procs=(), used_mb=30000):
    return {"kind": "sample", "t_wall": 100.0,
            "gpus": [{"index": 0, "uuid": MPS, "name": "Apple M3 Max (128 GB)",
                      "total_mb": 98304, "used_mb": used_mb,
                      "free_mb": 98304 - used_mb, "oracle_source": "mps-ram",
                      "procs": list(procs)}]}


def _mps_health(external_mb=30000, base_mb=None):
    health = {"ok": True,
              "vram": [{"gpu_uuid": MPS, "total_mb": 98304,
                        "external_known": True, "external_mb": external_mb,
                        "footprints_mb": 2000}]}
    if base_mb is not None:
        health["models"] = [{"inference_id": "tags/wd-vit-tagger-v3",
                             "replicas": [{"gpu_uuid": MPS,
                                           "base_mb": base_mb,
                                           "base_method": "mps"}]}]
    return {"kind": "sample", "t_wall": 100.0, "iso": "2026-09-06T07:12:09Z",
            "health": health}


def test_mps_ram_is_not_a_priced_oracle_even_on_an_idle_device():
    """The idle-GPU shortcut must not turn "no counter" into "priced"."""
    assert analyze.oracle_prices_pids(
        {"oracle_source": "mps-ram", "used_mb": 0, "procs": []}) is False


def test_oracle_agreement_skips_on_the_mps_oracle():
    procs = [_proc(900, None, "inferio-worker")]
    ctx = _context(vramrec=[_mps_vram(procs)], healthrec=[_mps_health()])
    verdict = analyze.check_oracle_agreement(ctx)
    assert verdict.verdict == "SKIP"
    assert verdict.numbers["oracle_sources"] == {"mps-ram": 1}


def test_base_accuracy_skips_on_the_mps_oracle():
    procs = [_proc(900, None, "inferio-worker")]
    ctx = _context(vramrec=[_mps_vram(procs)],
                   healthrec=[_mps_health(base_mb=1200)])
    ctx.args.base_window = 5.0
    verdict = analyze.check_base_accuracy(ctx)
    assert verdict.verdict == "SKIP"
    assert "ceiling_probe.py" in verdict.detail


def test_footprint_agreement_skips_on_the_mps_oracle():
    procs = [_proc(900, None, "inferio-worker")]
    ctx = _context(vramrec=[_mps_vram(procs)], healthrec=[_mps_health()])
    verdict = analyze.check_footprint_agreement(ctx)
    assert verdict.verdict == "SKIP"
    assert verdict.numbers["oracle_sources"] == {"mps-ram": 1}


# --- the room a grant is priced against ------------------------------------


def _room_grant(t_wall, mb, headroom_mb, room_mb=None):
    fields = {"model": MODEL, "gpu": GPU, "unit_budget": 4, "mb": mb,
              "headroom_mb": headroom_mb}
    if room_mb is not None:
        fields["room_mb"] = room_mb
    return {"ts": "2026-09-06T07:12:09.000000Z", "t_wall": t_wall,
            "level": "DEBUG", "target": "panoptikon::inferio::ledger",
            "message": "issued a memory grant", "fields": fields, "line": ""}


def test_a_grant_inside_the_room_is_not_over_its_headroom():
    """The share-credit shape: headroom 0, the pool the requester holds is
    what the grant is spent inside."""
    fields = _room_grant(100.0, 8455, 0, 8455)["fields"]
    assert analyze.grant_over_headroom(fields) is False
    assert analyze.grant_own_pool_mb(fields) == 8455.0


def test_a_grant_past_the_room_is_still_over_its_headroom():
    fields = _room_grant(100.0, 9000, 0, 8455)["fields"]
    assert analyze.grant_over_headroom(fields) is True


def test_a_line_without_room_mb_is_judged_on_headroom_alone():
    """Runs recorded before the room was logged still classify."""
    old = _room_grant(100.0, 500, 400)["fields"]
    assert analyze.grant_over_headroom(old) is True
    assert analyze.grant_own_pool_mb(old) == 0.0


def test_the_own_pool_is_never_negative():
    """`room_mb < headroom_mb` cannot happen, and must not credit a debt."""
    assert analyze.grant_own_pool_mb(
        _room_grant(100.0, 10, 4000, 100)["fields"]) == 0.0


def _safety_context(log, vramrec):
    ctx = _context(vramrec=vramrec)
    ctx.log = list(log)
    return ctx


def _free_sample(free_mb):
    return {"kind": "sample", "t_wall": 100.0,
            "gpus": [{"index": 0, "uuid": GPU, "total_mb": 32607,
                      "used_mb": 32607 - free_mb, "free_mb": free_mb,
                      "oracle_source": "nvml", "procs": []}]}


def test_grant_safety_counts_the_requesters_own_pool_as_spendable():
    """The oracle's free reading excludes a pool we already hold; a grant
    spent inside it needs no further cudaMalloc."""
    ctx = _safety_context([_room_grant(100.0, 8455, 0, 8455)],
                          [_free_sample(120)])
    verdict = analyze.check_grant_safety(ctx)
    assert verdict.verdict == "PASS"
    assert verdict.numbers["over_free"] == []


def test_grant_safety_still_fails_a_grant_past_free_plus_that_pool():
    ctx = _safety_context([_room_grant(100.0, 9000, 0, 8455)],
                          [_free_sample(120)])
    verdict = analyze.check_grant_safety(ctx)
    assert verdict.verdict == "FAIL"
    assert verdict.numbers["over_free"][0]["own_pool_mb"] == 8455.0


def _timed(t_wall, free_mb):
    return {**_free_sample(free_mb), "t_wall": t_wall}


def test_grant_safety_never_joins_a_sample_taken_after_the_grant():
    """The next sample already holds the batch this grant admitted."""
    ctx = _safety_context([_room_grant(100.0, 15700, 15700)],
                          [_timed(99.8, 15900), _timed(100.1, 15600)])
    verdict = analyze.check_grant_safety(ctx)
    assert (verdict.verdict, verdict.numbers["joined"]) == ("PASS", 1)


def test_grant_safety_never_passes_a_grant_a_release_may_have_covered():
    """Over the free memory seen before it, with a release in the next sample
    that covers it: WARN, whatever else the leg holds. No release, or one that
    comes later than the join tolerance: FAIL."""
    released = _safety_context([_room_grant(100.0, 15000, 15000),
                                _room_grant(100.0, 100, 100)],
                               [_timed(99.8, 10000), _timed(100.1, 15500)])
    verdict = analyze.check_grant_safety(released)
    assert (verdict.verdict, verdict.numbers["joined"]) == ("WARN", 2)
    assert verdict.numbers["covered_by_release"][0]["released_mb"] == 5500
    no_sample_before = _safety_context([_room_grant(100.0, 100, 100)],
                                       [_timed(100.1, 15500)])
    assert analyze.check_grant_safety(no_sample_before).verdict == "WARN"
    for later in (_timed(100.1, 12000), _timed(160.0, 15500)):
        held = _safety_context([_room_grant(100.0, 15000, 15000)],
                               [_timed(99.8, 10000), later])
        assert analyze.check_grant_safety(held).verdict == "FAIL"
    # `used` moved by up to each row's `skew_mb` while its processes were
    # read: that comes off the release, and an unknown skew proves none.
    def skewed(before_mb, after_mb):
        samples = [_timed(99.8, 10000), _timed(100.1, 15500)]
        samples[0]["gpus"][0]["skew_mb"] = before_mb
        samples[1]["gpus"][0]["skew_mb"] = after_mb
        return analyze.check_grant_safety(_safety_context(
            [_room_grant(100.0, 15000, 15000)], samples)).numbers
    assert skewed(100, 200)["covered_by_release"][0]["released_mb"] == 5200
    assert skewed(300, 300)["over_free"][0]["released_mb"] == 4900
    assert skewed(None, 0)["over_free"][0]["released_mb"] == 0
    assert skewed(0, None)["over_free"][0]["released_mb"] == 0


def test_a_sample_older_than_twice_the_recorder_interval_is_not_joined():
    """vramrec's median gap is 0.25 s: a grant 0.3 s after the last sample is
    judged, one 0.6 s after it is not, unless `--join-tolerance` says
    otherwise. A single sample: twice the header's interval. A health sample
    is joined to an oracle sample the same way."""
    def judge(vramrec, grants, tolerance=None, log=()):
        result = analyze.check_grant_safety(analyze.Context(
            args=_args(join_tolerance=tolerance), vramrec=vramrec, healthrec=[],
            hog=[], log=[*log, *(_room_grant(t, 15000, 15000) for t in grants)],
            before=None, after=None, jobs=None, probes=[]))
        return result.verdict, result.numbers["undecided"]
    samples = [_timed(t, 10000) for t in (97.0, 98.0, 98.25, 98.5, 98.6)]
    assert judge(samples, (98.9, 99.2)) == ("FAIL", 1)
    assert judge(samples, (98.9, 99.2), 1.5) == ("FAIL", 0)
    lone = [{"kind": "header", "interval_s": 0.25}, _timed(99.0, 10000)]
    assert judge(lone, (99.45, 99.55)) == ("FAIL", 1)
    # 901's release 1.2 s after the grant is too late to have covered it.
    workers = [_worker_sample(t, {900: 10000, 901: 6000}, 10000)
               for t in (99.0, 99.25, 99.5, 99.75)]
    assert judge(workers + [_worker_sample(101.0, {900: 10000, 901: 500}, 15500)],
                 (99.8,), log=[_spawn(900, MODEL)]) == ("FAIL", 0)
    rows = [_row(t, 20, {}) for t in (100.0, 100.25, 100.5)]
    for t_wall, joined in ((100.9, 1), (101.1, 0)):
        verdict = _agreement(rows, [_ledger(t_wall, 20)], join_tolerance=None)
        assert verdict.numbers["joined"] == joined


def _spawn(pid, model):
    return {"ts": "2026-09-06T07:12:08.000000Z", "t_wall": 90.0,
            "level": "INFO", "target": "panoptikon::inferio",
            "message": "spawned an inferio worker",
            "fields": {"pid": pid, "inference_id": model, "worker": "w"},
            "line": ""}


def _worker_sample(t_wall, workers, free_mb=None):
    """Workers 900 (the requester) and 901, over 16607 MiB of other use."""
    sample = _timed(t_wall, 16000 - sum(workers.values())
                    if free_mb is None else free_mb)
    sample["gpus"][0]["procs"] = [_proc(pid, mb, "inferio-worker")
                                  for pid, mb in workers.items()]
    return sample


def test_grant_safety_counts_only_releases_by_other_live_processes():
    """The requester emptying its cache after an out-of-memory error, or a
    worker the grant killed, is no release; another worker's is."""
    log = [_spawn(900, MODEL), _spawn(901, "other/model"),
           _room_grant(100.0, 5000, 5000)]
    before = _worker_sample(99.8, {900: 10000, 901: 6000})
    verdicts = [analyze.check_grant_safety(analyze.Context(
        args=_args(), vramrec=[before, _worker_sample(100.1, workers)],
        healthrec=[], hog=[], log=log, before=None, after=None, jobs=None,
        probes=[])).verdict
        for workers in ({900: 6000, 901: 6000}, {900: 10000},
                        {900: 10000, 901: 500})]
    assert verdicts == ["FAIL", "FAIL", "WARN"]


def test_grant_safety_without_a_spawn_line_tries_each_possible_requester():
    """No spawn line names the model, or its PID is not on the GPU: 901
    freeing 5500 MiB covers the grant unless 901 asked for it, so WARN;
    2000 MiB covers it under no choice, so FAIL. A worker spawned for another
    model is never the requester, so its growth is no release. The requester
    may not be on the GPU at all, so 901 alone freeing enough is a WARN."""
    def judge(log, workers, free_mb=None,
              before=_worker_sample(99.8, {900: 10000, 901: 6000})):
        verdict = analyze.check_grant_safety(analyze.Context(
            args=_args(), vramrec=[before, _worker_sample(100.1, workers, free_mb)],
            healthrec=[], hog=[], log=log + [_room_grant(100.0, 5000, 5000)],
            before=None, after=None, jobs=None, probes=[]))
        return verdict.verdict, [row["model"] for row
                                 in verdict.numbers["covered_without_spawn_line"]]
    assert judge([], {900: 10000, 901: 500}) == ("WARN", [MODEL])
    assert judge([], {900: 10000, 901: 4000}) == ("FAIL", [])
    assert judge([], {900: 12000, 901: 500}) == ("WARN", [MODEL])
    assert judge([_spawn(950, MODEL)], {900: 10000, 901: 500}) == ("WARN", [MODEL])
    # 901 grows 4000 MiB while another process frees 6000.
    assert judge([], {900: 10000, 901: 10000}, 2000) == ("WARN", [MODEL])
    assert judge([_spawn(901, "other/model")], {900: 10000, 901: 10000},
                 2000) == ("FAIL", [])
    assert judge([], {901: 500}, 5500,
                 before=_worker_sample(99.8, {901: 6000}, 0)) == ("WARN", [MODEL])


def test_grant_safety_leaves_cpu_grants_to_the_ledger():
    """The oracle records GPUs only: a host-RAM grant is not undecided."""
    cpu = {**_room_grant(100.0, 160970, 160970), "fields": {
        **_room_grant(100.0, 160970, 160970)["fields"], "gpu": "CPU"}}
    verdict = analyze.check_grant_safety(_safety_context(
        [cpu, _room_grant(100.0, 100, 100)], [_timed(99.8, 10000)]))
    assert verdict.verdict == "PASS"
    assert (verdict.numbers["cpu_grants"], verdict.numbers["undecided"]) == (1, 0)


def test_grant_safety_decides_a_zero_grant_without_a_sample():
    """A grant of 0 MiB cannot exceed free memory, sample or not."""
    verdict = analyze.check_grant_safety(_safety_context(
        [_room_grant(99.0, 0, 0), _room_grant(100.0, 100, 100)],
        [_timed(99.8, 10000)]))
    assert verdict.verdict == "PASS"
    assert (verdict.numbers["joined"], verdict.numbers["undecided"]) == (1, 0)


def test_oracle_agreement_skips_the_samples_while_no_job_ran():
    """From a drained `job_end`, or the hog stop, to the next `job_start`,
    the idle gateway keeps its last figure until something asks it to
    refresh. A job cut at the cap is still running."""
    legs = {"events": [
        {"event": "job_start", "iso": "1970-01-01T00:01:39Z"},
        {"event": "job_end", "iso": "1970-01-01T00:01:40.5Z", "outcome": "drained"},
        {"event": "job_start", "iso": "1970-01-01T00:01:42Z"},
        {"event": "job_end", "iso": "1970-01-01T00:01:43Z", "outcome": "cap_exceeded"},
        {"event": "hog_stop_requested", "iso": "1970-01-01T00:01:43.25Z"},
        {"event": "job_start", "iso": "1970-01-01T00:01:43.5Z"},
        {"event": "job_end", "iso": "1970-01-01T00:01:44Z", "outcome": "drained"},
        {"event": "hog_stop_requested", "iso": "1970-01-01T00:01:45Z"}]}
    assert analyze._idle_spans(legs) == [(100.5, 102.0), (103.25, 103.5),
                                         (104.0, math.inf)]
    procs = [_proc(900, 1000, "inferio-worker")]
    stale = {**_health_sample(14000), "t_wall": 101.0}
    ctx = _context(vramrec=[_vram_sample(procs, used_mb=1000)],
                   healthrec=[_health_sample(0), stale])
    assert analyze.check_oracle_agreement(ctx).verdict == "FAIL"
    ctx.idle_spans = analyze._idle_spans(legs)
    verdict = analyze.check_oracle_agreement(ctx)
    assert (verdict.verdict, verdict.numbers["idle_samples"]) == ("PASS", 1)


def _row(t_wall, used_mb, procs):
    """An oracle sample on a 32607 MiB GPU: allowance 1024 MiB. PIDs below
    900 are not ours."""
    sample = _vram_sample([_proc(pid, mb, None if pid < 900 else "inferio-worker")
                           for pid, mb in procs.items()], used_mb, "amdgpu-kfd")
    return {**sample, "t_wall": t_wall}


def _ledger(t_wall, external_mb, age_ms=0):
    sample = _health_sample(external_mb)
    sample["t_wall"] = t_wall
    sample["health"]["vram"][0]["external_sample_age_ms"] = age_ms
    return sample


def _agreement(vramrec, healthrec, hog=(), log=(), join_tolerance=1.0):
    return analyze.check_oracle_agreement(analyze.Context(
        args=_args(join_tolerance=join_tolerance), vramrec=vramrec,
        healthrec=healthrec, hog=list(hog), log=list(log), before=None,
        after=None, jobs=None, probes=[]))


def _recording(levels, until, start=100.0):
    """Oracle rows every 0.25 s from `start` to `until`. A row takes the first
    `(last_t, used_mb, procs)` of `levels` whose `last_t` it has not passed."""
    times = (start + 0.25 * step for step in range(int((until - start) / 0.25) + 1))
    return [_row(t, *next((used, procs) for last, used, procs in levels if t <= last))
            for t in times]


def _gpu_at(vramrec, t_wall):
    return next(row for row in vramrec if row["t_wall"] == t_wall)["gpus"][0]


def _died(t_wall, model):
    return {"ts": "", "t_wall": t_wall, "level": "WARN", "target": "panoptikon::inferio",
            "message": "worker died fatally; dropping model from all caches",
            "fields": {"model": model}, "line": ""}


def test_oracle_agreement_release_window_lasts_the_lag_or_the_drain():
    """A release window opens at the oracle sample before a process's figure
    falls, or at a death logged for one of our workers' model at most 10 s
    before its PID leaves. It lasts 40 ms per GiB freed, or longer until
    `used` is back within the allowance of its level before, but at most 2 s
    past that bound. A ledger figure read, or an oracle sample taken, in it
    is skipped; one past it is judged."""
    vramrec = _recording([
        (100.0, 12020, {900: 12000}),
        (100.25, 1020, {900: 1000}),  # 11000 MiB freed, `used` at once
        (110.0, 7020, {900: 1000, 901: 6000}),
        (111.0, 7020, {900: 1000}),  # 901 exited, `used` drains slower than 40 ms/GiB
        (111.75, 4000, {900: 1000}),
        (112.5, 1520, {900: 1000}),  # within the allowance of the level before
        (115.0, 1520, {900: 100}),  # 900 MiB freed, `used` not yet
        (120.0, 7520, {900: 100, 902: 6000}),
        (125.0, 7520, {900: 100}),  # 902 exited, `used` stays up past the cap
        (126.0, 1520, {900: 100}),
    ], 126.0)
    _gpu_at(vramrec, 112.75)["skew_mb"] = 500
    healthrec = [_ledger(100.5, 11000), _ledger(101.0, 20), _ledger(110.0, 20),
                 _ledger(111.0, 0), _ledger(111.5, 0), _ledger(112.75, 3220),
                 _ledger(121.0, 1420)]
    verdict = _agreement(vramrec, healthrec)
    assert (verdict.verdict, verdict.numbers["joined"],
            verdict.numbers["releasing_samples"]) == ("PASS", 3, 4)
    # Over the larger of the skew and a release under the allowance; just past
    # a window's lag bound; past the drain cap.
    for extra in (_ledger(112.75, 3520), _ledger(113.0, 2920), _ledger(123.0, 1420)):
        assert _agreement(vramrec, healthrec + [extra]).verdict == "FAIL"

    # 900's PID and `used` leave together, after its death line. 901, spawned
    # before the death and configured after it, leaves 30 s later; 950 is a
    # respawn. On amdgpu the PID leaves first and the death line comes later.
    dying = _recording([(101.0, 12020, {900: 6000, 901: 6000}),
                        (131.0, 6020, {901: 6000}), (132.0, 20, {})], 132.0)
    died = [_spawn(900, MODEL), _died(100.5, MODEL)]
    spare = [{**_spawn(901, None), "t_wall": 95.0, "fields": {"pid": 901, "worker": "v"}},
             {**_spawn(901, None), "t_wall": 101.0, "message": f"Configured as {MODEL}",
              "fields": {"worker": "v"}}]
    respawn = [{**_spawn(950, MODEL), "t_wall": 101.5}]
    amdgpu = _recording([(100.0, 6020, {900: 6000}), (100.25, 6020, {}),
                         (101.0, 20, {})], 101.0)
    # `used` lags 900's fall by a row; 901's release never leaves `used`.
    lagging = _recording([(99.75, 12020, {900: 12000}), (100.0, 12020, {900: 1000}),
                          (100.25, 1020, {900: 1000})], 100.25, start=99.75)
    stuck = _recording([(100.0, 7020, {900: 1000, 901: 6000}),
                        (103.0, 7020, {900: 1000})], 103.0)
    for vramrec, log, ledger, expected in (
            (dying, died, _ledger(100.75, 6020), "SKIP"),
            (dying, died[:1], _ledger(100.75, 6020), "FAIL"),
            (dying, died + spare, _ledger(100.75, 6020), "SKIP"),
            (dying, died + spare, _ledger(130.0, 6020), "FAIL"),
            (dying, died + respawn, _ledger(100.75, 6020), "SKIP"),
            (amdgpu, [_spawn(900, MODEL), _died(100.6, MODEL)], _ledger(100.25, 20),
             "SKIP"),
            (lagging, [], _ledger(100.0, 20, age_ms=500), "SKIP"),
            (stuck, [], _ledger(101.5, 20), "SKIP"),
            (stuck, [], _ledger(102.75, 20), "FAIL")):
        assert _agreement(vramrec, [ledger], log=log).verdict == expected


def test_oracle_agreement_counts_another_process_window_when_it_meets_the_span():
    """Another process's window counts when it meets the span from the
    ledger's free reading to the oracle sample. One of our workers' counts
    only when it holds the oracle sample or the ledger's reading."""
    vramrec = _recording([
        (99.75, 18020, {7: 6000, 900: 12000}),
        (100.0, 18020, {900: 12000}),  # 7 freed 6000 MiB, `used` not yet
        (105.0, 12020, {900: 12000}),
        (108.0, 1020, {900: 1000}),
    ], 108.0, start=99.75)
    healthrec = [_ledger(100.25, 6020, age_ms=350), _ledger(100.5, 6020, age_ms=750),
                 _ledger(106.0, 20, age_ms=1500), _ledger(107.0, 11000, age_ms=1800)]
    verdict = _agreement(vramrec, healthrec)
    assert (verdict.verdict, verdict.numbers["joined"],
            verdict.numbers["releasing_samples"],
            verdict.numbers["read_age_samples"],
            verdict.numbers["read_age_worst_mb"]) == ("PASS", 1, 3, 2, 10980)
    assert _agreement(vramrec, healthrec + [_ledger(101.0, 6020)]).verdict == "FAIL"


def test_oracle_agreement_rows_with_a_failed_read_open_no_window():
    """A row whose process query failed, that names an unreadable PID, or that
    has no `used`, opens no window; nor does a figure that turns null."""
    vramrec = _recording([(102.5, 7020, {900: 1000, 901: 6000})], 102.5)
    _gpu_at(vramrec, 100.5).update(procs=[], error="process query failed")
    _gpu_at(vramrec, 101.0).update(procs=[_proc(900, 1000, "inferio-worker")],
                                   unreadable_pids=[901])
    _gpu_at(vramrec, 101.5)["used_mb"] = None
    _gpu_at(vramrec, 101.75)["procs"][1]["used_mb"] = None
    verdict = _agreement(vramrec, [_ledger(t, 20) for t in (100.75, 101.25, 102.0)])
    assert (verdict.verdict, verdict.numbers["joined"]) == ("PASS", 3)


def test_oracle_agreement_skips_samples_while_the_hog_moved():
    """The ledger read free before the hog, PID 4, took or gave back 8 GiB:
    not yet a disagreement. Read after it and still missing it is one. A fill
    writes its state row when done, after the ledger read. A release reaches
    `used` after the hog's figure, and after its state row. A move under the
    allowance is taken off the difference, on the hog's GPU only. A free
    reading counts as at most 10 s old: the ledger refreshes an older one."""
    def judge(ledger, held, rows=(99.0, 99.5, 100.0, 100.5, 101.0, 101.5, 102.0),
              vramrec=None):
        hog = [{"kind": "header", "target": "gpu", "gpu_uuid": GPU}] + [
            {"kind": "state", "t_wall": t, "held_mb": held(t)} for t in rows]
        vramrec = vramrec or [_row(t, 20 + held(t), {4: held(t)})
                              for t in (100.0 + 0.25 * step for step in range(9))]
        verdict = _agreement(vramrec, [ledger], hog)
        return (verdict.verdict, verdict.numbers["hog_moving_samples"],
                verdict.numbers["releasing_samples"], verdict.numbers["read_age_samples"])
    step_up = lambda t: 8192 if t >= 100.5 else 0
    assert judge(_ledger(101.0, 20, age_ms=1000), step_up) == ("SKIP", 1, 0, 1)
    assert judge(_ledger(101.0, 8212, age_ms=1000),
                 lambda t: 8192 - step_up(t))[:3] == ("SKIP", 0, 1)
    lagged = [_row(99.75, 8212, {4: 8192}), _row(100.0, 8212, {4: 196}),
              _row(100.25, 216, {4: 196})]
    assert judge(_ledger(100.25, 8212, age_ms=350), lambda t: 8192 if t <= 99.5 else 0,
                 (99.0, 99.5, 99.81), lagged)[:3] == ("SKIP", 0, 1)
    assert judge(_ledger(102.0, 8212), step_up)[0] == "PASS"
    assert judge(_ledger(102.0, 20), step_up)[0] == "FAIL"
    assert judge(_ledger(101.0, 20), step_up,
                 (99.0, 99.5, 100.0, 101.5)) == ("SKIP", 1, 0, 0)
    assert judge(_ledger(101.5, 20, age_ms=15000), lambda t: 8192 if t >= 89.5 else 0,
                 (86.0, 89.5, 95.0, 100.0, 101.0, 102.0))[0] == "FAIL"
    # Up and back between the two readings, in no oracle sample.
    flat = [_row(t, 20, {}) for t in (100.0, 102.0)]
    assert judge(_ledger(102.0, 1544, age_ms=2000), lambda t: 2048 if t == 101.0 else 0,
                 vramrec=flat)[0] == "FAIL"

    step = lambda t: 512 if t >= 100.5 else 0
    missed = _ledger(101.0, 1832, age_ms=1000)  # 1300 MiB over the oracle
    assert judge(missed, step)[0] == "PASS"
    missed["health"]["vram"].append({**missed["health"]["vram"][0], "gpu_uuid": "GPU-0001"})
    vramrec = [_row(t, 20 + step(t), {4: step(t)}) for t in (100.0, 100.5, 101.0)]
    for row in vramrec:
        row["gpus"].append({**row["gpus"][0], "uuid": "GPU-0001"})
    assert judge(missed, step, vramrec=vramrec)[0] == "FAIL"


def _learning_context(seed, queue_bound=7):
    health = _worker_health(seed)
    health["health"]["workers"][0].update(fit_samples=12, max_units_measured=6293)
    health["health"]["models"] = [{"inference_id": MODEL, "total_batches": 7,
                                   "queue_bound_windows": queue_bound}]
    ctx = _utilization_context([health])
    ctx.args.learning = True
    ctx.after = {"profile": [{"inference_id": MODEL}]}
    return ctx


def test_calibration_learned_cannot_judge_when_every_window_was_queue_bound():
    """Only when every window formed short of the window target."""
    for seed in (64, 120000):
        verdict = analyze.check_calibration_learned(_learning_context(seed))
        assert verdict.verdict == "INFO"
        assert "every window formed short of the window target" in \
            verdict.detail
    stuck = _learning_context(120000, queue_bound=6)
    assert analyze.check_calibration_learned(stuck).verdict == "FAIL"


def test_batch_coverage_counts_every_batch_from_each_workers_seq_1():
    def coverage(*samples, check=analyze.check_batch_coverage,
                 generations=None):
        """Each sample is one replica's ring or a list of rings, one per
        replica; an empty list is a sample without the model, None a failed
        /health read. The model's generation is 1 unless `generations` gives
        one per sample."""
        recording = []
        for rings, generation in zip(samples,
                                     generations or [1] * len(samples)):
            if rings is None:
                recording.append({"kind": "sample", "t_wall": 100.0,
                                  "health": {"ok": False}})
                continue
            rings = rings if isinstance(rings, list) else [rings]
            models = [{"inference_id": MODEL, "generation": generation,
                       "replicas": [{"recent_batches": [{"seq": seq} for seq in ring]}
                                    for ring in rings]}] if rings else []
            recording.append({"kind": "sample", "t_wall": 100.0,
                              "health": {"ok": True, "models": models}})
        verdict = check(_context(healthrec=recording))
        return (verdict.verdict, *(verdict.numbers.get(key) for key
                                   in ("seen", "missed", "missed_per_model")))

    # First seen busy at 7-10: 1-6 were never shown.
    assert coverage(range(7, 11)) == ("WARN", 4, 6, {MODEL: 6})
    # A repeated and an overlapping ring count each batch once.
    assert coverage(range(1, 5), range(1, 5)) == ("PASS", 4, 0, {})
    assert coverage(range(1, 5), range(3, 7)) == ("PASS", 6, 0, {})
    # A seq that goes back is a new worker, counted from its seq 1.
    assert coverage(range(1, 5), (2, 3)) == ("WARN", 6, 1, {MODEL: 1})
    # A respawned worker, a new generation, counts from its own seq 1.
    assert coverage(range(1, 5), range(5, 9), generations=(1, 2)) == (
        "WARN", 8, 4, {MODEL: 4})
    # Each replica is its own series.
    assert coverage([range(1, 5), range(5, 9)],
                    [range(5, 9), range(9, 13)]) == ("WARN", 16, 4, {MODEL: 4})
    # A model missing from a sample was unloaded: the worker that loads it
    # again at the same key counts from its own seq 1.
    assert coverage(range(1, 4), [], range(4, 8)) == ("WARN", 7, 3, {MODEL: 3})
    # So does an empty ring: a new worker seen idle before its first batch.
    assert coverage(range(1, 5), (), range(1, 5)) == ("PASS", 8, 0, {})
    # A failed /health read ends no series.
    assert coverage(range(1, 5), None, range(3, 7)) == ("PASS", 6, 0, {})
    assert coverage((), (), check=analyze.CHECKS["batch_coverage"])[0] == "SKIP"


def test_throughput_corrects_both_sides_for_the_clock_step_or_neither(tmp_path):
    def leg(name, items, server_s, marks):
        """jobs.json holds the first job's LogRecord; a mark is (event, wall
        seconds[, t_mono])."""
        directory = tmp_path / name
        directory.mkdir()
        (directory / "jobs.json").write_text(json.dumps({"history": [
            {"total_segments": items, "start_time": "2026-10-03T10:00:00",
             "end_time": f"2026-10-03T10:00:{server_s:04.1f}"}]}))
        (directory / "legs.json").write_text(json.dumps({"events": [
            {"event": event, "iso": f"2026-10-03T10:00:{wall:06.3f}Z",
             **({"t_mono": mono[0]} if mono else {})}
            for event, wall, *mono in marks]}))
        return directory / "jobs.json"

    # The wall clock stepped back 1.8 s in the first job and forward 5 s in
    # the second, which jobs.json does not record.
    ours = leg("ours", 100, 8.2, [
        ("job_start", 0.0, 1000.25), ("job_end", 8.45, 1010.5),
        ("job_start", 9.0, 1011.0), ("job_end", 17.0, 1014.0)])
    # A baseline whose clock stepped back 5 s: both sides are corrected.
    stepped = leg("stepped", 100, 20.0, [
        ("job_start", 0.0, 500.0), ("job_end", 20.0, 525.0)])
    # An older baseline: marks without t_mono, so neither side is corrected.
    old = leg("c0", 100, 20.0, [("job_start", 0.0), ("job_end", 24.0)])

    def throughput(*argv):
        out = tmp_path / "verdicts.json"
        analyze.main(["--scenario", str(ours.parent), "--checks", "throughput",
                      "--json", str(out), "--quiet", *argv])
        (verdict,) = json.loads(out.read_text())["verdicts"]
        return verdict["numbers"]

    numbers = throughput("--baseline-jobs", str(stepped))
    assert numbers["items_per_s"] == pytest.approx(10.0)
    assert numbers["baseline_items_per_s"] == pytest.approx(4.0)
    assert numbers["baseline_clock_step_s"] == pytest.approx(-5.0)
    numbers = throughput("--baseline-jobs", str(old))
    assert numbers["items_per_s"] == pytest.approx(100 / 8.2)
    assert numbers["baseline_items_per_s"] == pytest.approx(5.0)
    assert (numbers["clock_step_s"], numbers["baseline_clock_step_s"]) == (
        None, None)
    # A baseline under another name may be another job's: neither side.
    other = stepped.with_name("other.json")
    other.write_text(stepped.read_text())
    assert throughput("--baseline-jobs", str(other))[
        "baseline_items_per_s"] == pytest.approx(5.0)
    # An explicit --jobs may be another job's: neither side.
    numbers = throughput("--jobs", str(ours), "--baseline-jobs", str(stepped))
    assert numbers["items_per_s"] == pytest.approx(100 / 8.2)
    assert numbers["baseline_items_per_s"] == pytest.approx(5.0)

    def record(start, end, inference=4.0):
        return {"total_segments": 100, "inference_time": inference,
                "data_load_time": 0.5,
                "start_time": f"2026-10-03 10:00:{start}",
                "end_time": f"2026-10-03 10:00:{end}"}

    # `job_s`: the job's monotonic seconds, which bound the corrected spans.
    def per_s(records, step, job_s=60.0):
        return analyze._items_per_s(records, (step, job_s))

    # A backward step longer than the job: end before start, still the span.
    assert per_s([record(10, "05")], -10.0) == pytest.approx(20.0)
    # A forward step that leaves less than the 4 s inference fell outside
    # the span: no step. The whole-second times allow 1 s per record.
    assert per_s([record(10, 20)], 30.0) == pytest.approx(10.0)
    assert per_s([record(10, 20)], 7.5) == pytest.approx(10.0)
    assert per_s([record(10, 20)], 7.0) == pytest.approx(100 / 3)
    # A backward step the job's 13.5 s cannot hold fell outside the span; the
    # whole-second times allow 1 s per record over the job's time.
    assert per_s([record(10, 20)], -5.0, 13.5) == pytest.approx(10.0)
    assert per_s([record(10, 20)], -5.0, 14.5) == pytest.approx(100 / 15)
    assert per_s([record(10, 20), record(30, 40)], -2.0, 20.5) == pytest.approx(
        200 / 22)
    # A step under 1 s is not subtracted, one of 1 s is. A sub-second job's
    # start equals its end, and that span uses busy time.
    assert per_s([record(10, 20)], -0.9) == pytest.approx(10.0)
    assert per_s([record(10, 20)], -1.0) == pytest.approx(100 / 11)
    assert per_s([record(10, 10)], -0.002) == pytest.approx(100 / 4.5)
    # A step inside records shorter than a second each: busy time.
    assert per_s([record(10, "09")], -1.002) == pytest.approx(100 / 4.5)
    assert per_s([record(10, "09", 1.0), record(20, 21, 1.0)],
                 -1.5) == pytest.approx(200 / 3)
    # Without a step, each record on its own: a span, or busy time.
    assert analyze._items_per_s([record(10, 20), {**record(21, 21),
                                 "end_time": None}]) == pytest.approx(200 / 14.5)
    # With a step and a missing end: busy time for every record.
    assert per_s([record(10, 20), {**record(20, 20), "end_time": None}],
                 -2.0) == pytest.approx(200 / 9)


# --- deflation_recovery ------------------------------------------------------


def _deflation(deflation, outcome="clean", gpu=GPU, clean_windows=0):
    """A settle line, or with `outcome=None` the time-repay line."""
    return {"ts": "2026-10-03T00:00:00.000000Z", "t_wall": 100.0,
            "level": "WARN" if outcome == "negative" else "DEBUG",
            "target": "panoptikon::inferio::ledger",
            "message": ("settled a granted window" if outcome else
                        analyze.DEFLATION_REPAID_LINE),
            "fields": {"model": MODEL, "gpu": gpu, "outcome": outcome,
                       "deflation": deflation,
                       "clean_windows": clean_windows}, "line": ""}


def _deflated_health(deflation):
    sample = _worker_health(8)
    sample["health"]["workers"][0].update(gpu_uuid=GPU, deflation=deflation)
    return sample


def _deflation_recovery(log, healthrec=(), declared=False):
    """A leg's health samples end with the model unloaded."""
    unloaded = {**_worker_health(0), "health": {"ok": True, "workers": []}}
    ctx = _utilization_context([*healthrec, unloaded], log=log)
    ctx.args.expect_deflated = declared
    return analyze.check_deflation_recovery(ctx)


def test_deflation_recovery_reads_the_settle_lines_before_health():
    """A level is repaid on its third clean window, or by elapsed time; the
    window that repays a level does not count toward the next. A 0.3 s job
    no health sample saw gets a verdict, a lone deflated sample is not the
    end, a worker that died restarts at 0, and one that left keeps its last
    value."""
    recovered = [_deflation(1, "negative"), _deflation(1, clean_windows=1),
                 _deflation(1, clean_windows=2), _deflation(0)]
    replaced = [_deflation(0), _deflation(1, "worker_died")]
    two_levels = [_deflation(1, "negative"), _deflation(2, "negative")] + [
        _deflation(level, clean_windows=windows)
        for level, windows in ((2, 1), (2, 2), (1, 0), (1, 1), (1, 2), (0, 0))]
    for log, healthrec in ((recovered, []), (recovered, [_deflated_health(1)]),
                           ([_deflation(1, "negative"), _deflation(0, None)],
                            []),
                           (replaced, [_deflated_health(0)]),
                           (two_levels, [])):
        verdict = _deflation_recovery(log, healthrec)
        assert (verdict.verdict, verdict.numbers["source"]) == ("PASS", "log")
    for log in ([_deflation(0), _deflation(1, "negative")] + [_deflation(1)] * 3,
                [_deflation(1, "negative"), _deflation(1), _deflation(1),
                 _deflation(1, "aborted"), _deflation(1)]):
        stuck = _deflation_recovery(log)
        assert stuck.verdict == "FAIL"
        assert stuck.numbers["clean_windows_at_level"] == {f"{MODEL}@{GPU}": 3}
    # Short of three clean windows at the end: the third window repays a level
    # the clock already lowered; one window short; a time repay restarts the
    # count; a worker that left at 1; a replica at 1 beside one at 0 on the
    # same GPU.
    for log in ([_deflation(3, "negative"), _deflation(3), _deflation(3),
                 _deflation(2, None), _deflation(1)],
                two_levels[:-1],
                [_deflation(2, "negative"), _deflation(2, clean_windows=1),
                 _deflation(1, None), _deflation(1, clean_windows=2),
                 _deflation(1, clean_windows=3)],
                replaced + [_deflation(1, "negative", gpu="GPU-1111")],
                [_deflation(1, "negative"), *[_deflation(0)] * 3,
                 _deflation(1, clean_windows=1)]):
        assert _deflation_recovery(log, [_deflated_health(0)]).verdict == "WARN"
    # Another target at DEBUG, the ledger's at INFO: only negatives logged.
    other = {**_deflation(0), "target": "panoptikon::db", "message": "chose"}
    verdict = _deflation_recovery([other, _deflation(1, "negative")],
                                  [_deflated_health(1), _deflated_health(0)])
    assert (verdict.verdict, verdict.numbers["source"]) == ("PASS", "healthrec")
    assert _deflation_recovery([_deflation(0)], declared=True).verdict == "FAIL"


def test_deflation_that_never_recovers_passes_only_when_declared():
    """Ending deflated WARNs from either source unless declared: the log
    when it holds a DEBUG ledger line such as a grant, `/health` when only
    the WARN negatives were logged."""
    for log, source in (([_budget_grant(1), _deflation(3, "negative")], "log"),
                        ([_deflation(3, "negative")], "healthrec")):
        healthrec = [_deflated_health(0), _deflated_health(3)]
        verdict = _deflation_recovery(log, healthrec)
        assert (verdict.verdict, verdict.numbers["source"]) == ("WARN", source)
        assert _deflation_recovery(log, healthrec, True).verdict == "PASS"


# --- job_outcome and legs.json -------------------------------------------------


def test_job_outcome_fails_a_job_legs_did_not_see_drain():
    """Cut at `--job-cap`, or never ended: neither is in jobs.json."""
    legs = {"events": [{"event": "job_start"},
                       {"event": "job_end", "outcome": "drained"},
                       {"event": "job_start"},
                       {"event": "job_end", "outcome": "cap_exceeded"},
                       {"event": "job_start"}]}
    unfinished = analyze._unfinished_jobs(legs)
    record = {"setter": MODEL, "total_segments": 5, "completed": 1}
    for jobs in ({"history": [record]}, None):
        ctx = _utilization_context([])
        ctx.args.expect_failures = ctx.args.expect_failed_jobs = 0
        ctx.jobs = jobs
        if jobs is not None:
            assert analyze.check_job_outcome(ctx).verdict == "PASS"
        ctx.unfinished_jobs = unfinished
        assert analyze.check_job_outcome(ctx).verdict == "FAIL"
