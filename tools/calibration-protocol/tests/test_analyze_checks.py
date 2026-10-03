"""Two checks that must SKIP rather than FAIL on a missing input.

`oracle_agreement` subtracts our workers' per-process usage from the GPU's
total. On WDDM nothing prices a process (NVML and `nvidia-smi` both answer
`[N/A]` for every PID), so the subtraction returns
the whole board and the "disagreement" is exactly our own footprint —
a recording gap, not a ledger fault. `slope_accuracy` has the same shape:
a probe for a model the leg never ran says the harness passed the wrong
file, not that the ledger learned a wrong slope.

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


def test_an_idle_board_counts_as_priced():
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


def _utilization_context(healthrec, log=(), probes=()):
    ctx = analyze.Context(
        args=_args(utilization_floor=0.25), vramrec=[],
        healthrec=list(healthrec), hog=[], log=list(log), before=None,
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


def _trial_over(units, moved=False):
    return {"ts": "2026-09-07T03:33:40.929674Z", "t_wall": 100.0,
            "level": "INFO", "target": "panoptikon::inferio::ledger::ramp",
            "message": "a batch size trial is over",
            "fields": {"model": MODEL, "gpu": GPU, "units": units,
                       "moved": moved, "largest_units": 64,
                       "retest_after_windows": 12},
            "line": ""}


def _settle(max_units_measured):
    return {"ts": "2026-09-07T03:33:40.929688Z", "t_wall": 100.0,
            "level": "DEBUG", "target": "panoptikon::inferio::ledger",
            "message": "settled a granted window",
            "fields": {"model": MODEL, "gpu": GPU, "outcome": "clean",
                       "max_units_measured": max_units_measured},
            "line": ""}


def test_utilization_scores_a_size_left_in_place_against_the_largest_it_ran():
    """64 issued, working size 3, largest batch 64, probe boundary 512."""
    ctx = _utilization_context(
        [_worker_health(64)],
        log=[_budget_grant(64), _trial_over(3), _settle(64)],
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
                               log=[_budget_grant(64), _settle(64)],
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
    assert verdict.numbers["models"][0]["denominator_units"] == 64


def test_a_size_no_trial_left_in_place_is_scored_against_the_boundary():
    """`knee_units` without `knee_is_local` (the size a replica opened at, a
    shipped one, or one a trial just moved) lowers no bar: a replica stuck at
    2 units is a FAIL, as is a trial that moved the size."""
    health = _worker_health(2)
    health["health"]["workers"][0].update(knee_units=2, knee_is_local=False)
    ctx = _utilization_context([health],
                               log=[_budget_grant(2), _trial_over(2, True),
                                    _settle(4)],
                               probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "FAIL"
    row = verdict.numbers["models"][0]
    assert (row["knee_units"], row["denominator_units"]) == (None, 512)


def test_the_denominator_never_exceeds_the_probe_boundary():
    """A size above the OOM boundary would score against absent memory."""
    ctx = _utilization_context([_worker_health(64)],
                               log=[_budget_grant(64), _trial_over(3),
                                    _settle(4096)],
                               probes=[_bisect_probe(512)])
    row = analyze.check_utilization(ctx).numbers["models"][0]
    assert row["denominator_units"] == 512
    assert "capped at the probe boundary 512" in \
        analyze.check_utilization(ctx).detail


def test_a_size_with_no_settle_line_falls_back_to_itself():
    ctx = _utilization_context([_worker_health(8)],
                               log=[_budget_grant(8), _trial_over(15)],
                               probes=[_bisect_probe(512)])
    verdict = analyze.check_utilization(ctx)
    row = verdict.numbers["models"][0]
    assert (row["held_rung_units"], row["denominator_units"]) == (None, 15)
    assert "held at knee_units=15 =" in verdict.detail


def test_the_probeless_leg_still_skips_with_a_size_left_in_place():
    ctx = _utilization_context([_worker_health(64)],
                               log=[_budget_grant(64), _trial_over(3),
                                    _settle(64)])
    verdict = analyze.check_utilization(ctx)
    assert verdict.verdict == "SKIP"
    assert "no probe boundary for any model" in verdict.detail


# --- the macOS oracle: a source that prices nothing by construction --------
#
# `vramrec.py`'s darwin branch records `oracle_source: "mps-ram"` — host RAM,
# the GPU wired limit and our workers' RSS, because Apple Silicon has no
# per-process GPU counter for any instrument to read. All three per-process
# checks must SKIP on it, as they do on WDDM, rather than subtracting a zero
# attribution from a real board figure and reporting our own workers as the
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
    """The idle-board shortcut must not turn "no counter" into "priced"."""
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


def _spawn(pid, model):
    return {"ts": "2026-09-06T07:12:08.000000Z", "t_wall": 90.0,
            "level": "INFO", "target": "panoptikon::inferio",
            "message": "spawned an inferio worker",
            "fields": {"pid": pid, "inference_id": model, "worker": "w"},
            "line": ""}


def _worker_sample(t_wall, workers):
    """Workers 900 (the requester) and 901, over 16607 MiB of other use."""
    sample = _timed(t_wall, 16000 - sum(workers.values()))
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


def test_oracle_agreement_skips_the_samples_after_the_hog_stop():
    """The idle gateway keeps the hog's last figure until the next refresh."""
    procs = [_proc(900, 1000, "inferio-worker")]
    stale = {**_health_sample(14000), "t_wall": 101.0}
    ctx = _context(vramrec=[_vram_sample(procs, used_mb=1000)],
                   healthrec=[_health_sample(0), stale])
    assert analyze.check_oracle_agreement(ctx).verdict == "FAIL"
    ctx.teardown_t = 100.5
    verdict = analyze.check_oracle_agreement(ctx)
    assert (verdict.verdict, verdict.numbers["teardown_samples"]) == ("PASS", 1)


def _learning_context(seed, largest, queue_bound=7):
    health = _worker_health(seed)
    health["health"]["workers"][0].update(fit_samples=12, max_units_measured=6293)
    health["health"]["models"] = [{"inference_id": MODEL, "total_batches": 7,
                                   "queue_bound_windows": queue_bound}]
    ctx = _utilization_context([health],
                               log=[_settle(largest)] if largest else [])
    ctx.args.learning = True
    ctx.after = {"profile": [{"inference_id": MODEL}]}
    return ctx


def test_calibration_learned_cannot_judge_a_seed_no_window_reached():
    """Only when every window was short for want of work and the settle
    lines say the largest stayed under the seed."""
    verdict = analyze.check_calibration_learned(_learning_context(120000, 6293))
    assert verdict.verdict == "INFO"
    assert "no window reached the seed" in verdict.detail
    for stuck in (_learning_context(64, 64), _learning_context(120000, None),
                  _learning_context(120000, 6293, queue_bound=6)):
        assert analyze.check_calibration_learned(stuck).verdict == "FAIL"


def test_batch_coverage_counts_the_batches_no_health_sample_showed():
    def sample(*seqs):
        replica = {"recent_batches": [{"seq": seq} for seq in seqs]}
        return {"kind": "sample", "t_wall": 100.0, "health": {"models": [
            {"inference_id": MODEL, "generation": 1, "replicas": [replica]}]}}

    # Idle, then 1-4, 7-10: 5 and 6 were evicted between two samples. A seq
    # that goes back is a new worker and only sets a baseline.
    recording = [sample(), sample(1, 2, 3, 4), sample(7, 8, 9, 10), sample(2, 3)]
    verdict = analyze.check_batch_coverage(_context(healthrec=recording))
    assert (verdict.verdict, verdict.numbers["seen"],
            verdict.numbers["missed"]) == ("WARN", 10, 2)
    recording[2] = sample(3, 4, 5, 6)
    verdict = analyze.check_batch_coverage(_context(healthrec=recording))
    assert (verdict.verdict, verdict.numbers["missed"]) == ("PASS", 0)
