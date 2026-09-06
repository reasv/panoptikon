"""`hog.py` re-touches what it holds, because macOS stops counting it.

Everywhere else, allocating and touching a page once is enough: it stays
against `MemAvailable` and against every NVML figure until it is freed. On
Apple Silicon a page that is then left idle is aged onto the **inactive
queue**, and `free + inactive` is what psutil's `available` -- and therefore
the worker's `min(recommended_max, ram_available)` and the gateway's
`external_mb` -- treats as free. Measured with the hog pinned at a constant
61 440 MiB and releasing nothing: `Pages inactive` grew 25 657 -> 35 699 MiB
in 141 s, **+4.3 GiB/min of memory handed back on paper while nothing was
freed** (MPS pass, F1). S4a's ledger then priced 37-51 GiB of an 89 600 MiB
hog, and S4d watched the pressure disappear 42 s before it was released.

So the hog keeps using its chunks. The sweep, not the writing, is what is
checked here -- the writing needs the platform.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import types
from pathlib import Path

HOG = Path(__file__).resolve().parents[1] / "hog.py"
MIB = 1024 * 1024


def _load():
    spec = importlib.util.spec_from_file_location("_calib_hog_touch", HOG)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


hog_mod = _load()


# --- where it is on, and where it is not -----------------------------------


def test_only_macos_and_only_the_targets_whose_pages_age():
    assert hog_mod.default_touch_period("mps", darwin=True) == 20.0
    assert hog_mod.default_touch_period("ram", darwin=True) == 20.0
    # A CUDA allocation is held by the driver; no page queue ever sees it.
    assert hog_mod.default_touch_period("gpu", darwin=True) == 0.0
    for target in ("mps", "ram", "gpu"):
        assert hog_mod.default_touch_period(target, darwin=False) == 0.0


def test_the_base_backend_touches_nothing():
    assert hog_mod.Backend().touch([object(), object()]) == 0


# --- the sweep -------------------------------------------------------------


class _Recorder(hog_mod.Backend):
    """A backend that only remembers which chunks it was handed."""

    name = "recorder"

    def __init__(self, chunk_mb: int = 16) -> None:
        self._chunk_bytes = chunk_mb * MIB
        self.seen: list = []

    def chunk_bytes(self) -> int:
        return self._chunk_bytes

    def free_total_mb(self):
        return 1000, 2000

    def touch(self, chunks):
        self.seen.extend(chunks)
        return int(len(chunks) * self._chunk_bytes // MIB)


def _hog(chunks: int, touch_period: float, chunk_mb: int = 16):
    backend = _Recorder(chunk_mb)
    args = argparse.Namespace(touch_period=touch_period, progress_every=2.0)
    hog = hog_mod.Hog(backend, hog_mod.Idle(), args)
    hog.chunks = [f"chunk{index}" for index in range(chunks)]
    return hog, backend


def test_a_full_sweep_takes_exactly_one_period():
    """8 chunks, tick 0.5 s, period 4 s -> one chunk a tick, all 8 in 4 s."""
    hog, backend = _hog(8, touch_period=4.0)
    for _ in range(8):
        hog.touch(0.5)
    assert backend.seen == [f"chunk{index}" for index in range(8)]
    assert hog.touch_sweeps == 1


def test_the_cursor_wraps_and_keeps_going_round():
    hog, backend = _hog(4, touch_period=2.0)
    for _ in range(8):
        hog.touch(0.5)
    assert backend.seen == [f"chunk{index}" for index in range(4)] * 2
    assert hog.touch_sweeps == 2


def test_a_short_period_touches_everything_every_tick():
    hog, backend = _hog(4, touch_period=0.5)
    hog.touch(0.5)
    assert backend.seen == ["chunk0", "chunk1", "chunk2", "chunk3"]


def test_a_period_longer_than_the_hog_still_touches_at_least_one_chunk():
    """Rounding must never silently disable the sweep."""
    hog, backend = _hog(2, touch_period=100000.0)
    hog.touch(0.5)
    assert len(backend.seen) == 1


def test_zero_disables_it_entirely():
    hog, backend = _hog(8, touch_period=0.0)
    assert hog.touch(0.5) == 0
    assert backend.seen == []
    assert "touched_mb_total" not in hog.state()


def test_an_empty_hog_touches_nothing():
    hog, backend = _hog(0, touch_period=4.0)
    assert hog.touch(0.5) == 0
    assert backend.seen == []


def test_the_totals_are_reported_in_the_state_record():
    hog, _ = _hog(4, touch_period=2.0, chunk_mb=16)
    hog.touch(0.5)
    hog.touch(0.5)
    state = hog.state()
    assert state["touched_mb_total"] == 32  # two chunks of 16 MiB
    assert state["touch_sweeps"] == 0
    hog.touch(0.5)
    hog.touch(0.5)
    assert hog.state()["touch_sweeps"] == 1


def test_a_failing_touch_is_recorded_and_does_not_kill_the_hog():
    class Broken(_Recorder):
        def touch(self, chunks):
            raise RuntimeError("device went away")

    backend = Broken()
    args = argparse.Namespace(touch_period=1.0, progress_every=2.0)
    hog = hog_mod.Hog(backend, hog_mod.Idle(), args)
    hog.chunks = ["a", "b"]
    assert hog.touch(0.5) == 0
    assert "device went away" in hog.state()["last_error"]


# --- the backends' own writes ----------------------------------------------


def test_the_ram_backend_writes_one_byte_per_page_not_per_byte():
    """A page-strided write marks the page and costs a page-th of the
    traffic -- which matters when the hold is tens of GiB on the machine
    also running the job under test."""
    backend = hog_mod.RamBackend(chunk_mb=1)
    block = backend.alloc()
    block[:] = 0
    assert backend.touch([block]) == 1
    written = int((block == 1).sum())
    expected = -(-block.nbytes // hog_mod.PAGE_BYTES)  # ceil
    assert written == expected
    assert written < block.nbytes


def _fake_mps_torch():
    calls = {"sync": 0}

    class _Tensor:
        def __init__(self) -> None:
            self.fills = 0

        def fill_(self, _value):
            self.fills += 1
            return self

    mps = types.SimpleNamespace(
        synchronize=lambda: calls.__setitem__("sync", calls["sync"] + 1),
        empty_cache=lambda: None,
        recommended_max_memory=lambda: 110100 * MIB,
        driver_allocated_memory=lambda: 0,
    )
    torch = types.SimpleNamespace(
        __version__="2.7.1",
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: True)),
        mps=mps, uint8="uint8",
        device=lambda name: name,
        empty=lambda *a, **k: _Tensor(),
    )
    return torch, calls, _Tensor


def test_the_mps_backend_refills_every_chunk_and_syncs_once(monkeypatch):
    torch, calls, tensor_cls = _fake_mps_torch()
    monkeypatch.setitem(sys.modules, "torch", torch)
    backend = hog_mod.MpsBackend(chunk_mb=64)
    chunks = [tensor_cls(), tensor_cls(), tensor_cls()]
    assert backend.touch(chunks) == 192
    assert [chunk.fills for chunk in chunks] == [1, 1, 1]
    # One synchronize for the whole slice, not one per chunk.
    assert calls["sync"] == 1
    assert backend.touch([]) == 0 and calls["sync"] == 1


# --- the header says what was done -----------------------------------------


def test_the_header_records_the_touch_and_why():
    out = subprocess.run(
        [sys.executable, str(HOG), "--target", "ram", "--chunk-mb", "8",
         "--tick", "0.1", "--duration", "0.25", "--touch-period", "0.2",
         "--quiet", "hold", "16"],
        capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    header = json.loads(out.stdout.splitlines()[0])
    assert header["kind"] == "header"
    assert header["touch"]["period_s"] == 0.2
    assert header["touch"]["method"] == "one byte per page"
    assert header["touch"]["page_bytes"] == hog_mod.PAGE_BYTES
    assert "inactive queue" in header["touch"]["why"]
    states = [json.loads(line) for line in out.stdout.splitlines()[1:]]
    assert states[-1]["touched_mb_total"] > 0


def test_a_run_with_no_touching_says_so_rather_than_being_silent():
    out = subprocess.run(
        [sys.executable, str(HOG), "--target", "ram", "--chunk-mb", "8",
         "--tick", "0.1", "--duration", "0.15", "--touch-period", "0",
         "--quiet", "hold", "8"],
        capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    header = json.loads(out.stdout.splitlines()[0])
    assert header["touch"] == {"period_s": 0.0, "page_bytes": None,
                               "method": None, "why": None}
