"""A derived text setter needs a corpus with words on it.

`textembed`'s unit of work is an `extracted_text` row another setter wrote.
The MPS pass ran the S14 chain over `smoke`, whose 180 images are gradients:
`doctr` read no words off them, so the `textembed` sub-job drained on 0 items
-- on every pass that ever ran it, reported PASS until the zero-item rule
landed (final MPS F-final-3). The leg now says so before it starts anything.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location("_calib_legs_derived",
                                                  HERE / "legs.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


legs = _load()
TEXTEMBED = legs.TEXTEMBED_MODEL
OCR = legs.DEFAULT_OCR_MODEL


def _corpus(tmp_path: Path, name: str, pages: int) -> Path:
    out = tmp_path / name
    out.mkdir()
    items = [{"id": "img-000", "kind": "image"},
             {"id": "txt-000", "kind": "text"}]
    items += [{"id": f"scan-{n:03}", "kind": "image", "rendered_lines": 34}
              for n in range(pages)]
    (out / "manifest.json").write_text(
        json.dumps({"tier": name, "generator": legs.CORPUS_GENERATOR,
                    "items": items}), encoding="utf-8")
    return out


def test_a_corpus_of_gradients_is_refused_for_a_derived_setter(tmp_path):
    smoke = _corpus(tmp_path, "smoke", pages=0)
    complaint = legs.derived_text_complaint([OCR, TEXTEMBED], smoke)
    assert complaint and TEXTEMBED in complaint
    assert "corpus.py --tier text" in complaint
    assert "0 items" in complaint


def test_a_corpus_with_scanned_pages_is_accepted(tmp_path):
    text = _corpus(tmp_path, "text", pages=300)
    assert legs.corpus_pages(text) == 300
    assert legs.derived_text_complaint([OCR, TEXTEMBED], text) is None


def test_a_chain_without_a_derived_setter_runs_on_anything(tmp_path):
    smoke = _corpus(tmp_path, "smoke", pages=0)
    assert legs.derived_text_complaint([OCR, "tags/wd-vit-tagger-v3"],
                                       smoke) is None


def test_the_other_derived_group_counts_too(tmp_path):
    smoke = _corpus(tmp_path, "smoke", pages=0)
    assert legs.derived_text_complaint(["tclip/jina-clip-v2"], smoke)


def test_the_scenario_that_needs_pages_declares_the_tier_that_has_them():
    assert legs.SCENARIOS["S14-textembed"].corpus == "text"
    assert TEXTEMBED in legs.SCENARIOS["S14-textembed"].models


def test_the_leg_refuses_before_it_starts_anything(tmp_path):
    """`--corpus` names the corpus, so this is the check that catches it."""
    smoke = _corpus(tmp_path, "smoke", pages=0)
    result = subprocess.run(
        [sys.executable, str(HERE / "legs.py"), "--scenario", "S14-textembed",
         "--corpus", str(smoke), "--bin", sys.executable,
         "--results", str(tmp_path / "results"), "--run-id", "t"],
        capture_output=True, text=True)
    assert result.returncode == 1
    assert "corpus.py --tier text" in result.stdout + result.stderr
