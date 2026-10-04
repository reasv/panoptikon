"""`hog.py leave-free N` on macOS: what "free" means when it solves for N.

The hog priced `free` as `free + speculative + inactive`, which is what psutil
reports on macOS and what the server and the worker stopped reading in
`fix/mps-memory`: it counts another process's held anonymous pages once ageing
moves them onto the inactive queue. Both readings are recomputed below from
one captured set of counters, and they differ by 9 109 MiB -- so a
`leave-free 20480` leg held 9 109 MiB too little and left ~11 371 MiB free
under the reading everything else now uses.

The fill itself stops on the free reading, not on an allocation failure:
where allocating past physical memory succeeds there is no failure to stop it.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

HOG = Path(__file__).resolve().parents[1] / "hog.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_hog_leave_free", HOG)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


hog = _load()

# Captured on the M3 Max (128 GB, macOS 26.6.1) 55.2 s into a leg with a
# `--target ram` hog holding a flat 24 576 MiB. Page counts as
# `vm_stat` prints them, plus the sysctl the anonymous term comes from.
CAPTURED = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                  5252707.
Pages active:                                1378163.
Pages inactive:                              1365372.
Pages speculative:                              9669.
Pages throttled:                                   0.
Pages wired down:                             199046.
Pages purgeable:                               15631.
"Translation faults":                       99549775.
Pages copy-on-write:                         1850054.
Pages zero filled:                         202064996.
Pages reactivated:                            388264.
Pages purged:                               43151189.
File-backed pages:                            731731.
Anonymous pages:                             2021473.
Pages stored in compressor:                   337197.
Pages occupied by compressor:                 124396.
Decompressions:                               203211.
Compressions:                               10108327.
Pageins:                                     5442607.
Pageouts:                                      10645.
Swapins:                                           0.
Swapouts:                                          0.
"""

PAGE = 16384
MIB = 1024 * 1024
MEMSIZE = 137438953472  # `sysctl -n hw.memsize` on the same machine
INTERNAL = 2020480  # `sysctl -n vm.page_pageable_internal_count`, same sample
HELD = 24576  # what the hog held at this sample, flat for 136 s


def test_available_is_the_counters_activity_monitor_calls_used():
    got = hog._mac_available_mb(CAPTURED, MEMSIZE, INTERNAL)
    assert got["MemTotal"] == MEMSIZE // MIB == 131072
    # memsize - wired - compressor - anonymous pageable.
    assert got["MemAvailable"] == 131072 - 3110 - 1943 - 31570 == 94449


def test_the_old_reading_handed_back_9109_mib_the_hog_still_held():
    old = (5252707 + 9669 + 1365372) * PAGE // MIB  # free + speculative + inactive
    assert old == 103558
    honest = hog._mac_available_mb(CAPTURED, MEMSIZE, INTERNAL)["MemAvailable"]
    assert old - honest == 9109, "held pages aged onto the inactive queue"


def test_leave_free_solves_against_the_honest_reading():
    """The leg asked for 20 480 MiB free. Priced the old way it stopped
    9 109 MiB short and left 11 371."""
    honest = hog._mac_available_mb(CAPTURED, MEMSIZE, INTERNAL)["MemAvailable"]
    assert _target(honest, HELD, leave=20480) == HELD + (honest - 20480) == 98545
    old = 103558
    assert _target(old, HELD, leave=20480) == 107654
    # What that target actually leaves, measured by the honest reading.
    assert honest - (107654 - HELD) == 11371


def _target(free_mb: int, held_mb: int, leave: int) -> int:
    """`Hog._leave_free` over one reading, through the real solver."""

    class _Backend(hog.Backend):
        name = "captured"

        def chunk_bytes(self) -> int:
            return 512 * MIB

        def free_total_mb(self):
            return free_mb, 131072

    backend = _Backend()
    args = argparse.Namespace(touch_period=0.0, progress_every=2.0, reeval=0.0)
    made = hog.Hog(backend, hog.Idle(), args)
    # `held_mb` counts the chunks it holds: 48 of 512 MiB is the leg's 24 576.
    made.chunks = [None] * (held_mb * MIB // backend.chunk_bytes())
    assert made.held_mb == held_mb
    return made._leave_free(leave)


def test_the_fill_stops_once_free_reaches_the_level():
    """Where allocation past physical memory succeeds, no failure stops the
    fill. Here every chunk the hog takes, the job's pool takes another, so
    the one solve at the start (10 240 - 6 000 = 4 240 MiB) would overshoot
    the level. Free is re-read once per GiB taken (at 0, 1 024, 2 048 and
    3 072 MiB held), so the fill stops at 3 072 MiB held with 4 096 free,
    from the schedule's level as from a `/set?leave_free=`."""

    class _Backend(hog.Backend):
        name = "overcommits"
        free = 10240

        def chunk_bytes(self) -> int:
            return 128 * MIB

        def alloc(self):
            self.free -= 2 * 128
            return object()

        def free_total_mb(self):
            return self.free, 24576

    args = argparse.Namespace(touch_period=0.0, progress_every=2.0,
                              reeval=999999.0)
    for override in (None, "leave_free"):
        backend = _Backend()
        made = hog.Hog(backend, hog.LeaveFree(6000), args)
        made.override, made.override_mb = override, 6000
        made.target_mb = made.resolve_target(0.0)
        assert made.target_mb == 4240
        made.apply(made.target_mb)
        assert (made.held_mb, made.target_mb, backend.free) == (
            3072, 3072, 4096)
        # The next tick holds there, pinned, though the job gave memory back.
        backend.free = 8192
        made.target_mb = made.resolve_target(0.5)
        made.apply(made.target_mb)
        assert (made.held_mb, made.target_mb) == (3072, 3072)

    # A hold is not stopped by the last leave-free level.
    backend.free = 6000
    made.override, made.override_mb = "mb", 6144
    made.apply(made.resolve_target(1.0))
    assert made.held_mb == 6144
