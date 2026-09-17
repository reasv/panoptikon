"""The S14 textembed recipe must reach the model through the extraction route.

The Windows pass 2 ran `--scenario S14` for `textembed` and got
`job_never_queued`: that scenario's corpus is the `smoke` tier, and a `.txt`
file is not an input to anything (`build_extension_set` has no text extension
and no model accepts `text/plain`). A text setter's work is `extracted_text`
rows another setter wrote, so the recipe is the `text` tier's scanned pages
with an OCR ahead of the embedder -- one scenario, so neither half can be
left off (T7).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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

