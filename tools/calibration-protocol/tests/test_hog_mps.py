"""`hog.py --target mps`: the unified-memory pressure generator.

The MPS hog is the pressure half of the macOS row in
`docs/batch-calibration-test-protocol.md` §9. Two things in it are
load-bearing and can be wrong quietly: the free reading, which must be the
same `min(recommended_max_memory(), RAM available)` the worker's own mps tier
takes (a hog measuring a different "free" makes every S4 leg's pressure
unreadable), and the release path, which is `torch.mps.empty_cache()` -- the
only place the MPS pool is ever handed back.

Both are driven here through a fake `torch.mps` namespace, the same way the
worker's own MPS tiers are tested, so a Linux host exercises the arithmetic.
The last test runs on real silicon and skips everywhere else.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

HOG = Path(__file__).resolve().parents[1] / "hog.py"
MIB = 1024 * 1024


def _load():
    spec = importlib.util.spec_from_file_location("_calib_hog", HOG)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


hog = _load()


class _Tensor:
    def __init__(self, size: int) -> None:
        self.size = size
        self.touched = False

    def fill_(self, _value):
        self.touched = True
        return self


def _fake_torch(available=True, recommended=98304 * MIB, allocated=0):
    """A `torch` with only what `MpsBackend` reaches for."""
    calls = {"empty_cache": 0, "synchronize": 0, "allocated": []}
    mps = types.SimpleNamespace(
        recommended_max_memory=lambda: recommended,
        driver_allocated_memory=lambda: allocated,
        empty_cache=lambda: calls.__setitem__("empty_cache",
                                              calls["empty_cache"] + 1),
        synchronize=lambda: calls.__setitem__("synchronize",
                                              calls["synchronize"] + 1),
    )
    torch = types.SimpleNamespace(
        __version__="2.9.0",
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: available)),
        mps=mps,
        uint8="uint8",
        device=lambda name: name,
        empty=lambda size, dtype=None, device=None: _Tensor(size),
    )
    return torch, calls


@pytest.fixture()
def backend(monkeypatch):
    torch, calls = _fake_torch()
    monkeypatch.setitem(sys.modules, "torch", torch)
    made = hog.MpsBackend(chunk_mb=128)
    made._calls = calls
    return made


def test_no_mps_device_is_refused_rather_than_silently_hogging_ram(monkeypatch):
    torch, _ = _fake_torch(available=False)
    monkeypatch.setitem(sys.modules, "torch", torch)
    with pytest.raises(SystemExit):
        hog.MpsBackend(chunk_mb=128)


def test_free_is_clamped_by_ram_because_the_memory_is_shared(backend, monkeypatch):
    monkeypatch.setattr(hog, "_meminfo", lambda: {"MemAvailable": 40000})
    assert backend.free_total_mb() == (40000, 98304)


def test_free_is_clamped_by_the_device_total_too(backend, monkeypatch):
    """RAM above the recommended-max does not make the GPU budget bigger."""
    monkeypatch.setattr(hog, "_meminfo", lambda: {"MemAvailable": 120000})
    assert backend.free_total_mb() == (98304, 98304)


def test_no_ram_reading_still_reports_the_device_total(backend, monkeypatch):
    monkeypatch.setattr(hog, "_meminfo", lambda: {})
    assert backend.free_total_mb() == (None, 98304)


def test_every_chunk_is_touched_and_synchronised(backend):
    tensor = backend.alloc()
    assert tensor.size == 128 * MIB and tensor.touched
    assert backend._calls["synchronize"] == 1


def test_release_hands_the_pool_back_to_the_driver(backend):
    chunks = [backend.alloc(), backend.alloc()]
    backend.release(chunks)
    assert chunks == []
    assert backend._calls["empty_cache"] == 1


def test_own_mb_is_the_per_process_driver_figure(monkeypatch):
    torch, _ = _fake_torch(allocated=3 * 1024 * MIB)
    monkeypatch.setitem(sys.modules, "torch", torch)
    assert hog.MpsBackend(chunk_mb=128).own_mb() == 3072


def test_the_header_carries_the_watermark_env_that_decides_where_it_fails(
        backend, monkeypatch):
    monkeypatch.setenv("PYTORCH_MPS_HIGH_WATERMARK_RATIO", "1.0")
    monkeypatch.delenv("PYTORCH_MPS_LOW_WATERMARK_RATIO", raising=False)
    monkeypatch.setattr(hog, "_meminfo", lambda: {"MemAvailable": 40000})
    described = backend.describe()
    assert described["gpu_uuid"] == "GPU-MPS"
    assert described["context_mb"] is None      # no per-process context here
    assert described["watermark"] == {
        "PYTORCH_MPS_HIGH_WATERMARK_RATIO": "1.0",
        "PYTORCH_MPS_LOW_WATERMARK_RATIO": None,
    }


def _real_mps() -> bool:
    try:
        import torch

        return bool(torch.backends.mps.is_available())
    except Exception:
        return False


@pytest.mark.skipif(not _real_mps(), reason="no MPS device on this host")
def test_one_real_chunk_on_real_silicon():
    """256 MiB on the device, then released: the whole hog in miniature."""
    made = hog.MpsBackend(chunk_mb=256)
    free_before, total = made.free_total_mb()
    assert total and free_before
    chunks = [made.alloc()]
    assert (made.own_mb() or 0) >= 256
    made.release(chunks)
