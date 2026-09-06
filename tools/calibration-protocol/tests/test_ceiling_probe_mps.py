"""`ceiling_probe.py --device mps`: ground truth on Apple Silicon.

The tool used to resolve `--device` through NVML and exit before loading
anything, and then pin `CUDA_VISIBLE_DEVICES` -- so on the one platform whose
README sends the reader here for ground truth it could not run at all (MPS
pass, T3). The MPS pass measured with a stand-in,
`results/mps/instruments/mpsprobe.py`, which this replaces.

Two things here are load-bearing and Linux can check both: that the device
resolves and the plan is honest with no NVML anywhere, and that the peak
sampler sees a peak the post-batch read misses. That second one is the whole
reason the sampler exists -- `torch.mps` has no peak API, so the worker reads
`driver_allocated_memory()` *after* the batch, and the MPS allocator has by
then freed cached buffers: 16 460 MiB post-batch against 20 064 MiB sampled
at 20 ms on the wd-vit ladder, -18 % (MPS pass, F7).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

PROBE = Path(__file__).resolve().parents[1] / "ceiling_probe.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_probe_mps", PROBE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


probe = _load()


# --- device resolution -----------------------------------------------------


def test_the_device_word_selects_the_unified_backend():
    for word in ("mps", "MPS", " mps "):
        assert probe.wants_mps(word)
    for word in ("0", "1", "cuda", ""):
        assert not probe.wants_mps(word)


def test_the_row_is_keyed_on_the_orchestrator_s_device_constant():
    """So a probe document joins `/health` and `vramrec.jsonl` by uuid."""
    row = probe.mps_device_row(name="Apple M3 Max (128 GB)", total_mb=110100)
    assert row["uuid"] == "GPU-MPS"
    assert row["index"] is None and row["backend"] == "mps"
    assert row["total_mb"] == 110100


def _plan(*device_args):
    out = subprocess.run(
        [sys.executable, str(PROBE), "--model", "tags/wd-vit-tagger-v3",
         "--dry-run", *device_args],
        capture_output=True, text=True, timeout=180)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_the_mps_plan_resolves_with_no_nvml_and_says_so():
    plan = _plan("--device", "mps")
    assert plan["backend"] == "mps"
    assert plan["device"]["uuid"] == "GPU-MPS"
    # The NVML path is not merely tolerated, it is not walked: no error is
    # recorded, because absence of NVML is the normal state here.
    assert plan["nvml_error"] is None and plan["gpus"] == []
    assert plan["sample_ms"] == 20.0
    assert plan["mps_watermark"] is None


def test_the_sampler_and_watermark_are_recorded_in_the_plan():
    plan = _plan("--device", "mps", "--sample-ms", "5", "--mps-watermark", "0.1")
    assert plan["sample_ms"] == 5.0
    assert plan["mps_watermark"] == "0.1"


def test_a_cuda_plan_carries_no_mps_keys():
    """A run on the reference host writes the document it always did."""
    plan = _plan("--device", "0")
    assert plan["backend"] == "cuda"
    assert "sample_ms" not in plan and "mps_watermark" not in plan


def test_a_device_that_is_neither_an_index_nor_mps_is_refused():
    out = subprocess.run(
        [sys.executable, str(PROBE), "--model", "tags/wd-vit-tagger-v3",
         "--dry-run", "--device", "gpu0"],
        capture_output=True, text=True, timeout=180)
    assert out.returncode != 0
    assert "NVML index or `mps`" in out.stderr


# --- the in-batch peak sampler ---------------------------------------------


def test_the_sampler_catches_a_peak_the_post_batch_read_misses():
    """The F7 shape: memory rises during the batch and is released before
    the batch returns, so a reader that only looks afterwards sees a
    fraction of what was held."""
    series = [1000, 8000, 20064, 12000, 16460]
    index = {"i": 0}

    def read():
        value = series[min(index["i"], len(series) - 1)]
        index["i"] += 1
        return value

    sampler = probe.PeakSampler(read, interval_ms=0.0)
    sampler.start()
    deadline = time.monotonic() + 2.0
    while index["i"] < len(series) and time.monotonic() < deadline:
        time.sleep(0.01)
    post_batch = sampler.stop()
    assert sampler.peak == 20064
    assert post_batch == 20064
    assert sampler.samples >= len(series)


def test_the_sampler_reads_once_before_the_batch_so_the_peak_is_never_null():
    sampler = probe.PeakSampler(lambda: 1234, interval_ms=3600_000.0)
    sampler.start()
    assert sampler.stop() == 1234


def test_a_reader_that_answers_none_leaves_the_peak_null():
    """No reading is not a reading of zero: the peak stays null and the
    record carries no bias figure at all."""
    sampler = probe.PeakSampler(lambda: None, interval_ms=0.0)
    sampler.start()
    assert sampler.stop() is None
    assert sampler.samples == 0


def test_a_raising_reader_stops_the_sampler_and_not_the_batch():
    def read():
        raise RuntimeError("device went away")

    sampler = probe.PeakSampler(read, interval_ms=0.0)
    try:
        sampler.start()
    except RuntimeError:  # the pre-batch reading is in the caller's frame
        pass
    assert sampler.stop() is None


def test_the_sampler_thread_is_joined_and_gone():
    before = threading.active_count()
    sampler = probe.PeakSampler(lambda: 1, interval_ms=1.0)
    sampler.start()
    sampler.stop()
    assert threading.active_count() == before


# --- the bias the sampler exists to size -----------------------------------


def test_the_gc_bias_is_the_measured_one():
    """wd-vit batch 128 on the M3 Max, verbatim (MPS pass, F7)."""
    assert probe.gc_bias(20064, 16460) == (3604, 17.963)  # the -18 %


def test_a_batch_with_no_bias_reports_zero_not_null():
    assert probe.gc_bias(1849, 1849) == (0, 0.0)


def test_a_missing_reading_is_never_a_zero_bias():
    assert probe.gc_bias(None, 16460) == (None, None)
    assert probe.gc_bias(20064, None) == (None, None)
