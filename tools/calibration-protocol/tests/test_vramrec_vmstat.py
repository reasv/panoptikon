"""The macOS oracle's parser: `vm_stat`, and the synthetic `GPU-MPS` row.

Apple Silicon has no NVML and no per-process GPU counter, so `vramrec.py`'s
darwin branch records host RAM plus the GPU wired limit and marks the sample
`oracle_source: "mps-ram"` (`docs/batch-calibration-test-protocol.md` §9).
Two things there can be wrong quietly and are checked here: the page-size
arithmetic (this Mac pages at 16 KiB, four times the size every Linux fixture
in this repo assumes, so a hard-coded 4096 would under-report RAM by 4x), and
the unified free/total formula, which has to be the same one
`inferio_worker.memory.mps_free_total_mb` uses or the oracle and the worker
are measuring different quantities.

The `vm_stat` sample is captured from the M3 Max under test, not invented.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

VRAMREC = Path(__file__).resolve().parents[1] / "vramrec.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_vramrec_mps", VRAMREC)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vramrec = _load()

# Captured on the MacBook Pro M3 Max 128 GB (macOS 26.6.1), idle.
CAPTURED = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                  5538325.
Pages active:                                1088866.
Pages inactive:                               968644.
Pages speculative:                            527760.
Pages throttled:                                   0.
Pages wired down:                             205533.
Pages purgeable:                               77793.
"Translation faults":                       21297171.
Pages copy-on-write:                          697644.
Pages zero filled:                          15071735.
Pages reactivated:                              8423.
Pages purged:                                  43732.
File-backed pages:                           1669070.
Anonymous pages:                              916200.
Pages stored in compressor:                        0.
Pages occupied by compressor:                      0.
Decompressions:                                    0.
Compressions:                                      0.
Pageins:                                     2394856.
Pageouts:                                          0.
Swapins:                                           0.
Swapouts:                                          0.
"""

PAGE = 16384
MIB = 1024 * 1024
MEMSIZE = 137438953472  # `sysctl -n hw.memsize` on the same machine


def test_the_page_size_comes_from_the_header_not_a_constant():
    got = vramrec.parse_vm_stat(CAPTURED, total_bytes=MEMSIZE)
    # free + speculative, as psutil counts them on macOS.
    assert got["mem_free_mb"] == (5538325 + 527760) * PAGE // MIB
    # available = free + inactive, the quantity the worker prices MPS against.
    assert got["mem_available_mb"] == (
        (5538325 + 527760) * PAGE // MIB + 968644 * PAGE // MIB)
    assert got["mem_total_mb"] == MEMSIZE // MIB == 131072


def test_a_four_kib_page_is_read_as_four_kib():
    """The same counts at a different page size are a different number of
    bytes; nothing here may assume this machine's page."""
    small = CAPTURED.replace("page size of 16384 bytes", "page size of 4096 bytes")
    assert (vramrec.parse_vm_stat(small, total_bytes=MEMSIZE)["mem_free_mb"]
            == (5538325 + 527760) * 4096 // MIB)


def test_the_compressor_is_reported_because_macos_compresses_before_it_swaps():
    pressured = CAPTURED.replace("Pages occupied by compressor:                      0.",
                                 "Pages occupied by compressor:                 100000.")
    assert (vramrec.parse_vm_stat(pressured, total_bytes=MEMSIZE)["cached_mb"]
            == 100000 * PAGE // MIB)


def test_swap_is_not_invented_from_vm_stat():
    """`vm_stat` counts swap *events*; free swap is not in this output."""
    got = vramrec.parse_vm_stat(CAPTURED, total_bytes=MEMSIZE)
    assert got["swap_free_mb"] is None and got["swap_total_mb"] is None


def test_unparseable_output_yields_nulls_not_zeros():
    got = vramrec.parse_vm_stat("vm_stat: command not found\n")
    assert set(got.values()) == {None}


def _oracle(wired_limit_mb, memsize_mb=131072):
    return vramrec.MpsOracle(memsize_mb=memsize_mb,
                             wired_limit_mb=wired_limit_mb,
                             chip="Apple M3 Max")


def test_a_zero_wired_limit_means_the_driver_default_not_zero_memory():
    oracle = _oracle(0)
    assert oracle.total_mb == 98304
    assert oracle.gpu_name() == "Apple M3 Max (128 GB)"


def test_a_raised_wired_limit_is_the_total():
    assert _oracle(120000).total_mb == 120000


def test_free_is_the_unified_formula_the_worker_uses():
    """`min(total, RAM available)`: RAM is the term that makes external
    pressure visible on a unified device at all."""
    oracle = _oracle(0)
    row = oracle.sample({"mem_available_mb": 40000})[0]
    assert (row["free_mb"], row["used_mb"]) == (40000, 98304 - 40000)
    # RAM above the wired limit does not make the GPU bigger.
    plenty = oracle.sample({"mem_available_mb": 120000})[0]
    assert plenty["free_mb"] == 98304 and plenty["used_mb"] == 0


def test_the_row_is_keyed_on_the_orchestrator_s_device_constant():
    row = _oracle(0).sample({"mem_available_mb": 40000})[0]
    assert row["uuid"] == "GPU-MPS"
    assert row["oracle_source"] == "mps-ram"
    assert row["_procs"] == []


def test_no_ram_reading_leaves_free_null_never_zero():
    row = _oracle(0).sample({})[0]
    assert row["free_mb"] is None and row["used_mb"] is None
