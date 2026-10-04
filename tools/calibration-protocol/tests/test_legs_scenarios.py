"""The S14 textembed recipe must reach the model through the extraction route.

`--scenario S14` for `textembed` never queues a job: that scenario's corpus
is the `smoke` tier, and a `.txt`
file is not an input to anything (`build_extension_set` has no text extension
and no model accepts `text/plain`). A text setter's work is `extracted_text`
rows another setter wrote, so the recipe is the `text` tier's scanned pages
with an OCR ahead of the embedder -- one scenario, so neither half can be
left off.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

LEGS = Path(__file__).resolve().parents[1] / "legs.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_scenarios",
                                                  LEGS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()


def test_s14_textembed_runs_the_text_tier_through_an_ocr_chain():
    scenario = legs.SCENARIOS["S14-textembed"]
    assert scenario.corpus == "text"
    assert scenario.models == (legs.DEFAULT_OCR_MODEL, legs.TEXTEMBED_MODEL)
    assert scenario.models[0].startswith("doctr/")
    assert scenario.smoke_api is True


def test_plain_s14_still_runs_one_model_on_the_smoke_tier():
    scenario = legs.SCENARIOS["S14"]
    assert (scenario.corpus, scenario.models) == ("smoke", ())


def test_a_hog_event_squeezes_during_the_job_on_any_hog_target(capsys,
                                                              monkeypatch):
    """`--hog-event` adds timed changes in MiB to a scenario with no hog of
    its own, through the same driver, on host RAM as on a GPU. Its figures
    are bounded by `--min-free-mb` as scaled ones are, and need the device
    total as a scenario's own hog does."""
    assert legs.main(["--scenario", "S2", "--hog-target", "ram",
                      "--gpu-total-mb", "24564", "--no-dotenv", "--dry-run",
                      "--hog-event", "at=60,leave_free=4096",
                      "--hog-event", "at=30,hold=2048",
                      "--hog-event", "at=120,release",
                      "--hog-event", "at=130,leave_free=0",
                      "--hog-event", "at=140,hold=30000"]) == 0
    plan = json.loads(capsys.readouterr().out)
    hog = plan["hog"]
    assert (hog["target"], hog["schedule"], hog["reeval"]) == (
        "ram", ["hold", "0"], 999999)
    assert [(row["at_s"], row.get("leave_free_mb"), row.get("mb"))
            for row in plan["hog_events"]] == [
        (30.0, None, 2048), (60.0, 4096, None), (120.0, None, 0),
        (130.0, 1024, None), (140.0, None, 23540)]
    assert [(row["at"], row["fraction"], row["scaled_mb"], row["resolved_mb"])
            for row in plan["floor_bound"]] == [
        ("at=130,leave_free=0", None, 0, 1024),
        ("at=140,hold=30000", None, 30000, 23540)]

    # Beside a scenario's own events, in time order, its hog unchanged.
    assert legs.main(["--scenario", "S4c", "--gpu-total-mb", "24564",
                      "--no-dotenv", "--dry-run",
                      "--hog-event", "at=95,leave_free=1024"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert [row["at_s"] for row in plan["hog_events"]] == [90.0, 95.0, 100.0]
    assert plan["hog"]["reeval"] is None

    # A scenario with its own check list is judged on the hog too.
    assert legs.main(["--scenario", "S14", "--gpu-total-mb", "24564",
                      "--no-dotenv", "--dry-run",
                      "--hog-event", "at=5,leave_free=4096"]) == 0
    argv = json.loads(capsys.readouterr().out)["analyze_command"]
    assert "hog_tracking" in argv[argv.index("--checks") + 1]

    for bad in ("at=5,leave_free=1,hold=2", "leave_free=1", "at=5,hold=x",
                "at=5,release=x", "at=-1,release", "at=nan,release",
                "at=inf,release", "at=5,hold=-1", "at=1,at=90,hold=5"):
        with pytest.raises(SystemExit):
            legs.main(["--scenario", "S2", "--gpu-total-mb", "24564",
                       "--dry-run", "--hog-event", bad])

    monkeypatch.setattr(legs, "board_total_mb", lambda device: None)
    monkeypatch.setattr(legs.rocm_sysfs, "inventory", lambda *roots: [])
    with pytest.raises(SystemExit):
        legs.main(["--scenario", "S2", "--no-dotenv", "--dry-run",
                   "--hog-event", "at=5,leave_free=4096"])
