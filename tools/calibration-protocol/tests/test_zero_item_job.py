"""A job that ran on no items is a failed leg, not a green one.

`S14-textembed` on a `text` corpus generated before that tier carried scanned
pages rescanned `total_available: 0`, queued a job with zero items, drained in
seconds and returned PASS on every check (run4-deploy, T4). Three guards: the
corpus is stamped and the leg refuses a stale one, a job with no record and a
job with zero items end the leg, and `analyze.py` FAILs on the record.

The one legitimate zero: a scenario whose fixture never loads declares it, in
the S5 table and through `--expect-empty-setters` (ampere final T3).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import json
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


legs = _load("_calib_legs_empty", HERE / "legs.py")
analyze = _load("_calib_analyze_empty", HERE / "analyze.py")
corpus_py = _load("_calib_corpus_empty", HERE / "corpus.py")


# --- the corpus stamp ------------------------------------------------------


def _corpus(tmp_path: Path, **manifest) -> Path:
    out = tmp_path / "text"
    out.mkdir()
    (out / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return out


def test_the_leg_and_the_generator_agree_on_the_version():
    """Two constants, one meaning: bumping one without the other is the bug."""
    assert legs.CORPUS_GENERATOR == corpus_py.GENERATOR_VERSION


def test_a_current_corpus_is_accepted(tmp_path):
    out = _corpus(tmp_path, tier="text", generator=legs.CORPUS_GENERATOR)
    assert legs.corpus_complaint(out, "text") is None


def test_a_stale_corpus_is_refused_with_the_command_to_rebuild_it(tmp_path):
    out = _corpus(tmp_path, tier="text", generator=1)
    complaint = legs.corpus_complaint(out, "text")
    assert complaint and "corpus.py --tier text" in complaint and "--force" in complaint


def test_an_unstamped_corpus_is_refused(tmp_path):
    assert legs.corpus_complaint(_corpus(tmp_path, tier="text"), "text")


def test_a_corpus_of_another_tier_is_refused(tmp_path):
    """The Windows pass ran the `smoke` tier where `text` was needed."""
    out = _corpus(tmp_path, tier="smoke", generator=legs.CORPUS_GENERATOR)
    complaint = legs.corpus_complaint(out, "text")
    assert complaint and "'smoke'" in complaint


def test_a_corpus_that_was_never_generated_is_refused(tmp_path):
    assert legs.corpus_complaint(tmp_path / "nope", "text")


def test_the_generator_stamps_what_it_writes(tmp_path):
    """A tiny `text` corpus, the tier the run4 leg was stale on."""
    out = tmp_path / "text"
    assert corpus_py.main(["--tier", "text", "--out", str(out), "--scale",
                           "0.005", "--jobs", "1"]) == 0
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["generator"] == corpus_py.GENERATOR_VERSION
    # The scanned pages are what makes it the text tier; a corpus of `.txt`
    # alone is the stale shape this stamp exists to catch.
    assert manifest["counts"].get("image")
    assert legs.corpus_complaint(out, "text") is None


# --- the leg's own reading of the job history ------------------------------


class _Leg:
    """`Leg.job_items` against a jobs.json on disk, without a gateway."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    path = legs.Leg.path
    job_items = legs.Leg.job_items


def _jobs(tmp_path: Path, rows) -> _Leg:
    (tmp_path / "jobs.json").write_text(json.dumps(rows), encoding="utf-8")
    return _Leg(tmp_path)


def test_the_setters_own_record_is_the_one_read(tmp_path):
    leg = _jobs(tmp_path, [{"setter": "textembed/all-MiniLM-L6-v2",
                            "total_segments": 0},
                           {"setter": "doctr/db", "total_segments": 300}])
    assert leg.job_items("doctr/db", "") == 300
    assert leg.job_items("textembed/all-MiniLM-L6-v2", "") == 0


def test_a_setter_with_no_record_reads_as_none(tmp_path):
    """`job_never_queued`: the history has nothing for it at all."""
    leg = _jobs(tmp_path, [{"setter": "doctr/db", "total_segments": 300}])
    assert leg.job_items("textembed/all-MiniLM-L6-v2", "") is None
    assert _Leg(tmp_path / "gone").job_items("doctr/db", "") is None


# --- analyze.py ------------------------------------------------------------


def _outcome(records, **overrides):
    args = argparse.Namespace(worker_pattern="inferio", join_tolerance=1.0,
                              probe=[], expect_failures=0,
                              expect_failed_jobs=0, expect_empty_setters=False)
    for name, value in overrides.items():
        setattr(args, name, value)
    ctx = analyze.Context(args=args, vramrec=[], healthrec=[], hog=[], log=[],
                          before=None, after=None, jobs=records, probes=[])
    return analyze.check_job_outcome(ctx)


def _record(setter, items, **extra):
    row = {"setter": setter, "total_segments": items, "completed": 1,
           "failed": 0, "failed_items": 0, "errors": 0, "outcome": "completed"}
    row.update(extra)
    return row


def test_a_zero_item_job_fails():
    verdict = _outcome([_record("textembed/all-MiniLM-L6-v2", 0)])
    assert verdict.verdict == "FAIL"
    assert "NO ITEMS" in verdict.detail
    assert verdict.numbers["empty_jobs"] == ["textembed/all-MiniLM-L6-v2"]


def test_one_empty_job_in_a_chain_fails_the_chain():
    verdict = _outcome([_record("doctr/db", 300),
                        _record("textembed/all-MiniLM-L6-v2", 0)])
    assert verdict.verdict == "FAIL"
    assert verdict.numbers["empty_jobs"] == ["textembed/all-MiniLM-L6-v2"]


def test_an_empty_history_is_a_failure_not_a_skip():
    """`jobs.json` present and empty: the leg queued nothing."""
    verdict = _outcome([])
    assert verdict.verdict == "FAIL"
    assert "nothing was ever queued" in verdict.detail


def test_a_leg_without_the_file_still_skips():
    """analyze.py is also run on legs that never recorded a job history."""
    assert _outcome(None).verdict == "SKIP"


def test_a_declared_zero_item_job_passes():
    """`calibfixture/dies_on_load_cuda` raises inside `load()`: 0 items is
    what the leg set out to record."""
    verdict = _outcome([_record("calibfixture/dies_on_load_cuda", 0)],
                       expect_empty_setters=True)
    assert verdict.verdict == "PASS"
    assert "NO ITEMS" not in verdict.detail and "as declared" in verdict.detail
    assert verdict.numbers["empty_jobs"] == ["calibfixture/dies_on_load_cuda"]
    assert verdict.numbers["expected_empty_setters"] is True


def test_a_declared_zero_item_job_still_fails_on_a_failed_item():
    verdict = _outcome([_record("calibfixture/dies_on_load_cuda", 0,
                                failed_items=1)], expect_empty_setters=True)
    assert verdict.verdict == "FAIL"


def test_a_job_with_items_still_passes():
    verdict = _outcome([_record("doctr/db", 300)])
    assert verdict.verdict == "PASS"
    assert verdict.numbers["empty_jobs"] == []
