"""The calibration protocol's ceiling probe, where it shares the worker's rules.

`tools/calibration-protocol/ceiling_probe.py` exists to produce a ground truth
the ledger's own fit can be compared against, which only works while both
sides price a batch in the same quantity. Since run2's R7 that includes the
per-item pixel canvas, so the probe resolves one and prices with it. These
tests pin the two halves of that: the registry rules it applies to the
declaration (`_canvas_pixels`, the tool's copy of `cost.rs:
canvas_from_tables`), and that the pricer it hands the measurement loop
actually carries the resolved figure into `packing.price_inputs`.

The tool is a standalone script rather than a package module, so it is loaded
by path. Its module level imports nothing outside the standard library, which
is what makes that safe here.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from inferio_worker import packing
from inferio_worker.inputs import PredictionInput

PROBE = (
    Path(__file__).resolve().parents[3]
    / "tools"
    / "calibration-protocol"
    / "ceiling_probe.py"
)


@pytest.fixture(scope="module")
def probe():
    spec = importlib.util.spec_from_file_location("ceiling_probe_under_test", PROBE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def png_bytes(width: int, height: int) -> bytes:
    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (width, height)).save(buffer, format="PNG")
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# The declaration: the orchestrator's two registry rules
# ---------------------------------------------------------------------------


def test_the_ids_own_canvas_wins_and_a_group_of_the_same_unit_is_inherited(probe):
    assert probe._canvas_pixels({"canvas_pixels": 1_843_200}, {}, "pixel") == 1_843_200
    assert (
        probe._canvas_pixels(
            {}, {"unit": "pixel", "canvas_pixels": 6_553_600}, "pixel"
        )
        == 6_553_600
    )


def test_a_group_canvas_is_never_inherited_across_a_unit_change(probe):
    """`[group.clip]` is item-priced; its VLM ids are not. Inheriting the CLIP
    tower's canvas into one would cap a tiled VLM at 378^2 and under-price
    every item — the one direction the ledger cannot absorb."""
    assert (
        probe._canvas_pixels({}, {"unit": "item", "canvas_pixels": 142_884}, "pixel")
        is None
    )


def test_a_canvas_is_read_for_pixel_pricing_only(probe):
    assert probe._canvas_pixels({"canvas_pixels": 1_843_200}, {}, "token") is None
    assert probe._canvas_pixels({"canvas_pixels": 1_843_200}, {}, "item") is None


@pytest.mark.parametrize("value", [0, -1, "1843200", 1843200.0, True, None])
def test_an_unusable_declaration_is_uncapped(probe, value):
    assert probe._canvas_pixels({"canvas_pixels": value}, {}, "pixel") is None


# ---------------------------------------------------------------------------
# The pricer: the resolved canvas has to reach `price_inputs`
# ---------------------------------------------------------------------------


def test_the_probe_prices_a_pixel_batch_at_the_declared_canvas(probe):
    """The declaration stands in for the grant, and the batch is priced at
    `min(raw, canvas)` — the quantity the ledger's fit is denominated in."""
    price, canvas = probe.batch_pricer(
        packing,
        {"unit": "pixel", "aggregation": "sum", "canvas_pixels": 1_835_008},
        SimpleNamespace(max_pixels=11_289_600),
    )
    assert canvas == 1_835_008, "a declaration outranks the impl's own attribute"
    inputs = [PredictionInput(file=png_bytes(8000, 6000)) for _ in range(3)]
    assert price(inputs) == 3 * 1_835_008


def test_the_probe_falls_back_to_the_impls_own_canvas(probe):
    """dots.ocr's ceiling lives in a processor downloaded with the weights, so
    the registry declares nothing and the loaded object is the only source."""
    price, canvas = probe.batch_pricer(
        packing,
        {"unit": "pixel", "aggregation": "sum", "canvas_pixels": None},
        SimpleNamespace(model=SimpleNamespace(processor=SimpleNamespace(max_pixels=1_843_200))),
    )
    assert canvas == 1_843_200
    assert price([PredictionInput(file=png_bytes(8000, 6000))]) == 1_843_200


def test_an_uncapped_model_prices_raw_pixels_as_before_run2(probe):
    price, canvas = probe.batch_pricer(
        packing,
        {"unit": "pixel", "aggregation": "sum", "canvas_pixels": None},
        SimpleNamespace(),
    )
    assert canvas is None
    assert price([PredictionInput(file=png_bytes(8000, 6000))]) == 48_000_000


def test_a_small_item_is_untouched_by_the_canvas(probe):
    price, _ = probe.batch_pricer(
        packing,
        {"unit": "pixel", "aggregation": "max-times-count", "canvas_pixels": 1_835_008},
        SimpleNamespace(),
    )
    inputs = [PredictionInput(file=png_bytes(40, 30)), PredictionInput(file=png_bytes(40, 30))]
    assert price(inputs) == 2 * 40 * 30, "a cap, not a price"


def test_a_non_pixel_model_is_priced_by_its_own_aggregation(probe):
    price, canvas = probe.batch_pricer(
        packing,
        {"unit": "item", "aggregation": "count", "canvas_pixels": None},
        SimpleNamespace(max_pixels=1_835_008),
    )
    assert canvas is None, "an area prices nothing outside pixel pricing"
    assert price([PredictionInput(file=png_bytes(8000, 6000))] * 4) == 4
