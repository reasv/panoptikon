"""What `loadgen.py` sends out of a corpus manifest.

A manifest records absolute paths, so a corpus copied to another directory or
host must still be read from the copy. `mode=text` sends every item as text,
so it takes only the corpus's text items: the text tier also carries scanned
JPEGs, which would otherwise go out as text.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(f"_calib_loadgen_{name}",
                                                  HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


loadgen = _load("loadgen")
probe = _load("ceiling_probe")
DEFAULTS = argparse.Namespace(corpus=None, requests=None, prewarm_only=False)


def _corpus(root: Path) -> Path:
    """A mixed corpus, as `corpus.py` writes it: absolute root and paths."""
    (root / "img").mkdir(parents=True)
    (root / "txt").mkdir()
    (root / "img" / "a.jpg").write_bytes(b"\xff\xd8 page")
    (root / "txt" / "a.txt").write_text("words")
    items = [{"id": "page", "kind": "image", "path": "img/a.jpg"},
             {"id": "note", "kind": "text", "path": "txt/a.txt"}]
    for item in items:
        item["abspath"] = str(root / item["path"])
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps({"root": str(root), "items": items}))
    return manifest


def test_a_copied_corpus_is_read_from_the_copy(tmp_path):
    _corpus(tmp_path / "made")
    copy = tmp_path / "copy"
    shutil.copytree(tmp_path / "made", copy)
    (tmp_path / "made" / "img" / "a.jpg").write_bytes(b"the original")
    spec = loadgen.ModelSpec(f"id=g/m,mode=file,corpus={copy}", DEFAULTS)
    body, _, _ = loadgen.build_request(spec, spec.pool[:1])
    assert b"\xff\xd8 page" in body and b"the original" not in body
    items, _ = probe.load_items(str(copy), None, "image")
    assert items[0]["abspath"] == str(copy / "img" / "a.jpg")

    # A manifest written apart from its files still finds them by its root.
    apart = tmp_path / "elsewhere" / "manifest.json"
    apart.parent.mkdir()
    shutil.move(str(copy / "manifest.json"), apart)
    assert probe.load_items(str(apart), None, "text")[0][0]["abspath"] == str(
        tmp_path / "made" / "txt" / "a.txt")


def test_mode_text_sends_only_the_text_items(tmp_path):
    manifest = _corpus(tmp_path)
    spec = loadgen.ModelSpec(f"id=g/m,mode=text,corpus={manifest}", DEFAULTS)
    assert [item["id"] for item in spec.pool] == ["note"]
    body, _, meta = loadgen.build_request(spec, spec.pool)
    assert b'{"text": "words"}' in body and meta["item_ids"] == ["note"]
    with pytest.raises(SystemExit, match="mode=text and kind=image"):
        loadgen.ModelSpec(f"id=g/m,mode=text,kind=image,corpus={manifest}",
                          DEFAULTS)
