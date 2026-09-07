"""The shipped-baseline generator: what it copies, and what it refuses.

`baselines.py` turns a Linux/CUDA measurement into the file that ships in
`python/inferio/config/calibration/`. The rules it must not lose: only
linux/cuda rows are measurements, a copy happens only for an id the registry
allowlists, a copy says where its base came from, local authority never
ships, and a second run over the first run's output changes nothing.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path

import pytest

BASELINES = Path(__file__).resolve().parents[1] / "baselines.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_baselines", BASELINES)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


baselines = _load()

REGISTRY = """
[group.tags]
config.impl_class = "wd_tagger"
[group.tags.inference_ids.wd-vit-tagger-v3]
metadata.cost.platform_copies = ["windows"]
[group.tags.inference_ids.wd-convnext-tagger-v3]
metadata.description = "not allowlisted"
"""


def _store(tmp_path: Path, *, schema: int = 3, extra: str = "",
           platform: str = "linux", inference_id: str = "tags/wd-vit-tagger-v3",
           slope: float = 29.859099744349997) -> Path:
    arch = 'arch = "sm_120"\n' if schema == 3 else ""
    path = tmp_path / "calibration.toml"
    path.write_text(f"""schema = {schema}

[[profile]]
inference_id = "{inference_id}"
epoch = 1
{arch}gpu = "NVIDIA RTX PRO 6000 Blackwell Workstation Edition"
platform = "{platform}"
backend = "cuda"
torch = "2.7.1+cu128"
dtype = "fp32"
unit = "item"
aggregation = "count"
base_mb = 964
base_method = "nvml"
slope_mb_per_unit = {slope}
samples = 12
residual_mb = 0.6653657224753715
measured_at = "2026-09-06T03:46:06.472031929Z"
generator = "panoptikon 0.1.8"
max_units_measured = 768
local_samples = 26
sample_units = [1, 2]
sample_delta_mb = [38, 68]
{extra}""")
    return path


def _registry(tmp_path: Path, body: str = REGISTRY) -> Path:
    path = tmp_path / "inference.toml"
    path.write_text(body)
    return path


def test_an_allowlisted_id_gets_a_windows_row_carrying_the_linux_base(tmp_path):
    rows = baselines.generate(
        baselines.read_store(_store(tmp_path), None),
        baselines.read_allowlist(_registry(tmp_path)),
    )
    assert [row["platform"] for row in rows] == ["linux", "windows"]
    linux, windows = rows
    assert "base_platform" not in linux
    assert linux["generator"] == "panoptikon 0.1.8", "measured rows pass through"
    assert windows["base_platform"] == "linux"
    assert windows["base_mb"] == linux["base_mb"] == 964
    assert windows["slope_mb_per_unit"] == linux["slope_mb_per_unit"]
    assert windows["measured_at"] == linux["measured_at"]
    assert windows["generator"] == baselines.GENERATOR


def test_an_id_the_registry_does_not_allowlist_is_not_copied(tmp_path):
    rows = baselines.generate(
        baselines.read_store(
            _store(tmp_path, inference_id="tags/wd-convnext-tagger-v3"), None),
        baselines.read_allowlist(_registry(tmp_path)),
    )
    assert [row["platform"] for row in rows] == ["linux"]


def test_local_authority_never_reaches_the_baseline(tmp_path):
    rows = baselines.generate(
        baselines.read_store(_store(tmp_path), None),
        baselines.read_allowlist(_registry(tmp_path)),
    )
    for row in rows:
        for field in baselines.LOCAL_ONLY:
            assert field not in row, field
    assert "local_samples" not in baselines.render(rows)


def test_a_row_measured_anywhere_but_linux_cuda_is_refused(tmp_path):
    store = baselines.read_store(_store(tmp_path, platform="windows"), None)
    with pytest.raises(baselines.BaselineError, match="linux/cuda"):
        baselines.generate(store, baselines.read_allowlist(_registry(tmp_path)))


def test_a_row_with_no_fit_is_refused_anchor_and_all(tmp_path):
    """What `pending_update_locked` writes before MIN_FIT_SAMPLES: an anchor and
    no slope. It prices nothing, and with no slope the anchor cannot be turned
    into MB either, so it is not a baseline."""
    store = baselines.read_store(_store(tmp_path, slope=0.0), None)
    with pytest.raises(baselines.BaselineError, match="slope_mb_per_unit"):
        baselines.generate(store, baselines.read_allowlist(_registry(tmp_path)))


def test_rerunning_over_its_own_output_changes_nothing(tmp_path):
    allowlist = baselines.read_allowlist(_registry(tmp_path))
    once = baselines.render(
        baselines.generate(baselines.read_store(_store(tmp_path), None), allowlist))
    again = tmp_path / "baseline.toml"
    again.write_text(once)
    twice = baselines.render(
        baselines.generate(baselines.read_store(again, None), allowlist))
    assert twice == once


def test_a_schema_2_store_converts_only_with_a_stated_architecture(tmp_path):
    store = _store(tmp_path, schema=2)
    with pytest.raises(baselines.BaselineError, match="--arch"):
        baselines.read_store(store, None)
    rows = baselines.read_store(store, "sm_120")
    assert rows[0]["arch"] == "sm_120"
    # Schema 3 already states one, so the flag would be overriding evidence.
    with pytest.raises(baselines.BaselineError, match="schema-2"):
        baselines.read_store(_store(tmp_path), "sm_120")


def test_the_allowlist_refuses_a_platform_a_cuda_row_cannot_answer(tmp_path):
    registry = _registry(tmp_path, """
[group.tags]
[group.tags.inference_ids.wd-vit-tagger-v3]
metadata.cost.platform_copies = ["macos"]
""")
    with pytest.raises(baselines.BaselineError, match="macos"):
        baselines.read_allowlist(registry)


def test_the_rendered_file_is_readable_toml_with_the_schema_stamp(tmp_path):
    rows = baselines.generate(
        baselines.read_store(_store(tmp_path), None),
        baselines.read_allowlist(_registry(tmp_path)),
    )
    doc = tomllib.loads(baselines.render(rows))
    assert doc["schema"] == baselines.SCHEMA
    assert len(doc["profile"]) == 2
    assert doc["profile"][1]["slope_mb_per_unit"] == 29.859099744349997


def test_the_shipped_registry_allowlist_names_only_ids_that_exist(tmp_path):
    root = Path(__file__).resolve().parents[3]
    registry_path = root / "python/inferio/config/inference.toml"
    allowed = baselines.read_allowlist(registry_path)
    with registry_path.open("rb") as handle:
        doc = tomllib.load(handle)
    ids = {
        f"{group_name}/{id_name}"
        for group_name, group in doc["group"].items()
        for id_name in (group.get("inference_ids") or {})
    }
    assert allowed, "the shipped registry allowlists at least one id"
    assert set(allowed) <= ids
    # The models whose kernels differ per platform must never be listed.
    assert not any(name.startswith("whisper/") for name in allowed)
    assert "doctr/dots_ocr" not in allowed
