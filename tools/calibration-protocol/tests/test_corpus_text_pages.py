"""The `text` tier has to reach `textembed`, which means printed words.

`textembed`'s work query is `files ⋈ item_data(data_type='text') ⋈
extracted_text`: a unit of work is a text row **another setter wrote**. Two
consequences the Windows (T7) and MPS (T5) passes both ran into:

* a `.txt` file is indexed by nothing (`build_extension_set` has no text
  extension) and accepted by no model, so a corpus of `.txt` alone scans to
  `total_available: 0`;
* an OCR over pages that only *look* like text stores nothing -- the MPS
  chain's `doctr` job read 195 smoke images and wrote **0** text rows.

So the tier ships `.txt` for `loadgen.py` **and** scanned pages with real
words on them for the extraction route, and the pages are drawn with Pillow's
bundled font so a corpus reproduces on every host.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import random
import sys
from pathlib import Path

CORPUS = Path(__file__).resolve().parents[1] / "corpus.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_corpus", CORPUS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


corpus = _load()


# --- the tier carries both routes ------------------------------------------


def test_the_text_tier_carries_scannable_items_not_only_txt():
    groups = corpus.tier_groups("text")
    kinds = {group.kind for group in groups}
    assert "text" in kinds, "the .txt half is what loadgen.py posts"
    pages = [group for group in groups if group.params.get("text_page")]
    assert pages, "a .txt file is indexed by nothing: the tier needs pages"
    assert sum(group.count for group in pages) >= 100


def test_the_ocr_tier_is_still_all_pages():
    groups = corpus.tier_groups("ocr")
    assert groups and all(group.params.get("text_page") for group in groups)


# --- what is actually printed ----------------------------------------------


def _page(width=1240, height=1754, seed=7):
    return corpus._text_page(width, height, random.Random(seed))


def test_a_page_reports_the_words_it_drew():
    image, page = _page()
    assert image.size == (1240, 1754)
    assert page["lines"] >= 10
    assert len(page["text"]) > 200
    # Real words from the fixed vocabulary, not lorem noise.
    assert any(word in page["text"] for word in corpus._WORDS)


def test_the_ink_is_dark_on_light_and_there_is_enough_of_it():
    """A detector needs contrast and coverage; bars used to give neither."""
    image, _ = _page()
    grey = image.convert("L")
    histogram = grey.histogram()
    dark = sum(histogram[:100])
    light = sum(histogram[200:])
    assert light > dark, "the page must read as a light ground"
    assert dark > grey.size[0] * grey.size[1] * 0.005, "too little ink to OCR"


def test_the_same_seed_prints_the_same_page():
    first, first_page = _page()
    second, second_page = _page()
    assert first_page["text"] == second_page["text"]
    assert first.tobytes() == second.tobytes()


def test_a_different_seed_prints_a_different_page():
    _, one = _page(seed=1)
    _, two = _page(seed=2)
    assert one["text"] != two["text"]


def test_the_font_scales_with_the_page_and_stays_legible():
    _, small = _page(256, 256)
    _, large = _page(2480, 3508)
    assert small["font_px"] >= 11
    assert large["font_px"] > small["font_px"]


def test_a_tiny_page_still_prints_something():
    image, page = _page(160, 120)
    assert image.size == (160, 120)
    assert page["lines"] >= 1


# --- the manifest row ------------------------------------------------------


def test_a_page_item_records_what_was_printed(tmp_path):
    spec = {"abspath": str(tmp_path / "page.jpg"), "seed": 11, "index": 0,
            "params": {"w": 620, "h": 877, "format": "JPEG",
                       "text_page": True}}
    record = corpus.gen_image(spec)
    assert record["text_bytes"] > 0
    assert record["rendered_lines"] > 0
    assert record["rendered_text_head"]
    assert (tmp_path / "page.jpg").stat().st_size > 0


def test_an_ordinary_image_gains_no_text_fields(tmp_path):
    spec = {"abspath": str(tmp_path / "plain.jpg"), "seed": 11, "index": 0,
            "params": {"w": 320, "h": 240, "format": "JPEG"}}
    record = corpus.gen_image(spec)
    assert "text_bytes" not in record
    assert "rendered_lines" not in record
