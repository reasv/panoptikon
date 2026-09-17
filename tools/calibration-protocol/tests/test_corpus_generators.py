"""Every generator has to survive `_text_page` returning a tuple.

`_text_page` has returned `(image, meta)` since d97094ad, but `gen_pdf` and
`gen_junk` went on using `_base_image`'s result as an image: on Pillow 10.4.0
the `smoke` tier lost all 5 PDFs and the `poison` tier two of its four items
to `AttributeError: 'tuple' object has no attribute 'convert' / 'save'`
(ampere final T1, final-deploy O3). Nothing caught it because no test
generated a tier end to end.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

CORPUS = Path(__file__).resolve().parents[1] / "corpus.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_corpus_gen", CORPUS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


corpus = _load()


def _generate(tmp_path: Path, tier: str) -> dict:
    out = tmp_path / tier
    assert corpus.main(["--tier", tier, "--out", str(out), "--jobs", "4"]) == 0
    return json.loads((out / "manifest.json").read_text(encoding="utf-8"))


def _expected(tier: str) -> int:
    return sum(group.count for group in corpus.tier_groups(tier))


def test_the_smoke_tier_generates_every_item_including_its_pdfs(tmp_path):
    manifest = _generate(tmp_path, "smoke")
    assert _expected("smoke") == 205
    assert manifest["errors"] == []
    assert len(manifest["items"]) == 205
    assert manifest["counts"]["pdf"] == 5


def test_the_poison_tier_generates_all_four_failure_inputs(tmp_path):
    """The truncated JPEG and the named-OOM PNG are two of the four."""
    manifest = _generate(tmp_path, "poison")
    assert manifest["errors"] == []
    assert len(manifest["items"]) == _expected("poison") == 4
    assert {item["group"] for item in manifest["items"]} == {
        "truncated-jpg", "empty", "named-oom", "huge-png"}
