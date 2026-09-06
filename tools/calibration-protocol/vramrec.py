#!/usr/bin/env python3
"""vramrec.py - out-of-process VRAM/RAM oracle for the batch-calibration protocol.

The independent instrument: it reads NVML directly, never the gateway's own
numbers, at a fixed cadence, and writes one JSON object per sample to JSONL.
Stdlib plus `nvidia-ml-py` (`pynvml`); `psutil`, if importable, is the
non-Linux fallback for RSS -- on Linux `/proc` is read directly.

Usage
-----
    python3 vramrec.py --out results/<run>/<scenario>/vramrec.jsonl \
        [--interval 0.25] [--duration 600] [--filter 'inferio|panoptikon'] \
        [--gpu 0 --gpu 1] [--env-key FOO] [--no-env] [--quiet] \
        [--smi auto|always|never] [--nvidia-smi PATH] [--smi-interval 1.0]

Runs until SIGINT/SIGTERM (or `--duration`), flushes, exits 0. With no
`--out` it writes to stdout.

Output schema (JSONL)
---------------------
Line 1 is a `"kind": "header"` object: argv, interval, host, an `"nvml"` block
(`available`, `driver_version`, `nvml_version`, `error`), an `"smi"` block
(`mode`, `binary`, `min_interval_s`) and the `"gpus"` inventory (`index`,
`uuid`, `name`, `total_mb`, `pci_bus_id`). Then samples:

    {"schema": "vramrec/1", "kind": "sample", "seq", "t_mono", "t_wall",
     "iso", "sample_ms",
     "gpus":  [{"index", "uuid", "name", "total_mb", "used_mb", "free_mb",
                "error", "oracle_source", "oracle_age_ms",
                "procs": [{"pid", "used_mb", "cmdline", "comm",
                "type": "compute"|"graphics", "gone", "rss_mb", "vmhwm_mb",
                "env": {"CUDA_VISIBLE_DEVICES": str, ...}}]}],
     "mem":   {"mem_total_mb", "mem_available_mb", "mem_free_mb",
               "swap_free_mb", "cached_mb", "source" (macOS only)},
     "procs": [{"pid", "cmdline", "comm", "rss_mb", "vmhwm_mb", "gone",
                "env"}]}

`used_mb` per process is NVML's `usedGpuMemory`, `null` (never 0) when the
driver reports N/A -- on Windows WDDM, and in a container started without
`--pid=host` (NVML then lists host PIDs). All MB are MiB.

**The Windows oracle.** Where NVML answers N/A for every process on a GPU,
`nvidia-smi --query-compute-apps=pid,used_memory --format=csv` still answers
(scoped here with `-i <uuid>`, one query per GPU), and that is the attribution
a WDDM pass has -- `docs/batch-calibration-test-protocol.md` §9 and the run1
report §8 both name it. **NVML wins the merge**: the fallback only ever fills
a `used_mb` that is still null and appends a pid NVML never listed, so a pid
both instruments price keeps NVML's figure.

Per GPU, **`oracle_source`** says which instrument priced the processes in
that sample: `"nvml"` (NVML priced *every* process it listed and no
`nvidia-smi` reading was used), `"nvidia-smi"` (NVML priced none and the
fallback priced at least one), `"nvml+nvidia-smi"` (some pids priced by each)
or `"none"` (this GPU has no complete attribution -- an idle board, a
partly-priced one the fallback was not consulted for, or one where the
fallback answered and priced nothing, which is the WDDM shape: a null fill is
not a fill). `oracle_age_ms` is how old the reused `nvidia-smi` reading was,
and it is recorded whenever that reading was consulted, so a `"none"` row
carrying an age is one the fallback ran on. Never read a `used_mb` without
reading the `oracle_source` beside it.

Under `--smi auto` the fallback runs when NVML **lists processes and prices
none of them** -- the WDDM signature. An empty list is not that signature: it
is what an idle GPU looks like everywhere, and S2/S3 sit on one for minutes
before the model loads, so it earns a subprocess only on Windows or once a
query on this host has already priced a GPU NVML could not. `--smi never`
disables the fallback; `--smi always` queries every sample (a subprocess per
GPU per sample -- for a comparison run, not a recording).

Top-level `procs` lists every process whose cmdline matches `--filter`, VRAM
or not, so a CPU-GPU run and a worker's RSS/VmHWM come from one instrument. A
process that vanishes mid-sample yields `"gone": true` and nulls, and an NVML
failure degrades to a per-GPU `"error"`; neither aborts the recorder.

**The macOS oracle** (`oracle_source: "mps-ram"`). Apple Silicon has no NVML
and **no per-process GPU counter at all** -- a process's Metal heap is not
reported by any system instrument -- so nothing here is loaded and the single
GPU row is synthetic: `uuid` is the constant `GPU-MPS` the orchestrator keys
its device on, `total_mb` is the GPU wired limit (`sysctl
iogpu.wired_limit_mb`, or the driver's default of ~75 % of `hw.memsize` when
that sysctl reads 0), and `free_mb`/`used_mb` are the unified formula the
worker itself uses (`min(total, RAM available)`). Per-sample RAM comes from
`psutil` when it is importable and from `vm_stat` otherwise, and the header's
`"darwin"` block says which, alongside `hw.memsize` and the wired limit read
once at start.

The GPU row's `procs` are our own matched workers priced by **RSS only**
(`used_mb` is null for every one of them): the only GPU-side self-report on
this platform is the worker's own `driver_allocated` in `/health`, which is
`torch.mps.driver_allocated_memory()` and per-process by construction.
`analyze.py` reads `oracle_source` and SKIPs its per-process checks here for
the same reason it SKIPs them on WDDM.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

MIB = 1024 * 1024

# The one platform where NVML hides per-process VRAM by design.
IS_WINDOWS = os.name == "nt"
# The one platform with no per-process GPU counter to hide.
IS_DARWIN = sys.platform == "darwin"
HAVE_PROC = os.path.isdir("/proc")

# The orchestrator's device key for the single unified device (`mps.rs`).
MPS_DEVICE_KEY = "GPU-MPS"
# The seed `gpu.rs` uses before a worker reports its recommended-max, and this
# row's fallback when `iogpu.wired_limit_mb` reads 0. It under-states: the M3
# Max measured 110 100 MiB of 131 072 (0.84), against 98 304 here, which is
# exactly why the worker's figure and not this one is authoritative.
MPS_DEFAULT_TOTAL_FRACTION = 0.75

DEFAULT_ENV_KEYS = (
    "CUDA_VISIBLE_DEVICES",
    "CUDA_DEVICE_ORDER",
    "PANOPTIKON_DEVICE_PIN",
    "PANOPTIKON_UNIFIED_GPU",
    "HIP_VISIBLE_DEVICES",
    "ROCR_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
    "INFERIO_DEVICE",
    "INFERIO_WORKER",
    "INFERIO_WORKER_LOG_LEVEL",
    "PYTORCH_CUDA_ALLOC_CONF",
    "PYTORCH_MPS_HIGH_WATERMARK_RATIO",
    "PYTORCH_MPS_LOW_WATERMARK_RATIO",
    # ROCm worker env (worker.rs), for the BC-250 platform pass
    "ROCM_PATH",
    "HIP_PATH",
    "MIOPEN_FIND_MODE",
    "MIOPEN_USER_DB_PATH",
    "MIOPEN_CUSTOM_CACHE_DIR",
    "HSA_OVERRIDE_GFX_VERSION",
    "NO_CUDNN",
)
# Any variable with one of these prefixes is captured too.
ENV_PREFIXES = ("PANOPTIKON_", "INFERIO_")

_stop = False


def _handle_signal(signum, _frame):  # noqa: ANN001
    global _stop
    _stop = True


# --- /proc helpers (Linux); psutil fallback elsewhere ---------------------


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as handle:
            return handle.read().decode("utf-8", "replace")
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None


def proc_argv(pid: int) -> Optional[str]:
    """The process's real argv, or None when /proc/<pid>/cmdline gives nothing.

    Kept separate from `proc_cmdline` so a caller can tell a genuine argv from
    the `[comm]` fallback: identified, versus not yet exec'd (`ProcCache`).
    """
    if not HAVE_PROC:
        return _psutil_argv(pid)
    raw = _read_text(f"/proc/{pid}/cmdline")
    if raw is None:
        return None
    parts = [part for part in raw.split("\0") if part]
    return " ".join(parts) if parts else None


def proc_cmdline(pid: int) -> Optional[str]:
    argv = proc_argv(pid)
    if argv is not None:
        return argv
    # Kernel threads have an empty cmdline; fall back to comm in brackets.
    comm = proc_comm(pid)
    return f"[{comm}]" if comm else None


def proc_comm(pid: int) -> Optional[str]:
    if not HAVE_PROC:
        return _psutil_call(pid, "name")
    raw = _read_text(f"/proc/{pid}/comm")
    return raw.strip() if raw is not None else None


# --- psutil arms: the same three questions where there is no /proc ---------
#
# macOS (and Windows) answer them through psutil, and one difference matters:
# a denial there is **permanent** (another user's process is never going to
# become readable), where an empty `/proc/<pid>/cmdline` on Linux is usually a
# process mid-exec. So a denied read reports the process *name* as its argv
# and a readable-but-empty environ, which is what tells `ProcCache` to memoize
# the identity instead of re-reading every system PID at 4 Hz forever.


def _psutil_call(pid: int, name: str) -> Optional[str]:
    if _PSUTIL is None:
        return None
    try:
        return str(getattr(_PSUTIL.Process(pid), name)())
    except Exception:
        return None


def _psutil_argv(pid: int) -> Optional[str]:
    if _PSUTIL is None:
        return None
    try:
        parts = _PSUTIL.Process(pid).cmdline()
    except Exception as exc:
        if _PSUTIL is not None and isinstance(exc, _PSUTIL.NoSuchProcess):
            return None
        # Denied for good: the name is this platform's whole identity for it.
        return _psutil_call(pid, "name")
    joined = " ".join(part for part in parts if part)
    return joined or _psutil_call(pid, "name")


def _psutil_env(pid: int, keys: Iterable[str]) -> Tuple[Dict[str, str], bool]:
    if _PSUTIL is None:
        return {}, True
    try:
        environ = _PSUTIL.Process(pid).environ()
    except Exception as exc:
        if isinstance(exc, _PSUTIL.NoSuchProcess):
            return {}, False
        return {}, True
    wanted = set(keys)
    return ({name: value for name, value in environ.items()
             if name in wanted or name.startswith(ENV_PREFIXES)}, True)


def proc_env_read(pid: int, keys: Iterable[str]) -> Tuple[Dict[str, str], bool]:
    """`(matched vars, environ was readable)`.

    The flag separates "not readable *yet*" (mid-fork) from "readable and
    carries none of these names" or "unreadable for good". `ProcCache` retries
    only the first.
    """
    if not HAVE_PROC:
        return _psutil_env(pid, keys)
    raw = _read_text(f"/proc/{pid}/environ")
    if raw is None:
        return {}, False
    wanted = set(keys)
    found: Dict[str, str] = {}
    for entry in raw.split("\0"):
        if not entry or "=" not in entry:
            continue
        name, _, value = entry.partition("=")
        if name in wanted or name.startswith(ENV_PREFIXES):
            found[name] = value
    return found, True


def proc_mem(pid: int) -> Dict[str, Optional[int]]:
    """RSS and VmHWM (lifetime high-water RSS) in MiB."""
    raw = _read_text(f"/proc/{pid}/status")
    if raw is None:
        if _PSUTIL is not None:
            try:
                info = _PSUTIL.Process(pid).memory_info()
                return {"rss_mb": int(info.rss // MIB), "vmhwm_mb": None}
            except Exception:
                return {"rss_mb": None, "vmhwm_mb": None}
        return {"rss_mb": None, "vmhwm_mb": None}
    rss = hwm = None
    for line in raw.splitlines():
        if line.startswith("VmRSS:"):
            rss = _kb_to_mib(line)
        elif line.startswith("VmHWM:"):
            hwm = _kb_to_mib(line)
        if rss is not None and hwm is not None:
            break
    return {"rss_mb": rss, "vmhwm_mb": hwm}


def _kb_to_mib(line: str) -> Optional[int]:
    parts = line.split()
    if len(parts) < 2:
        return None
    try:
        return int(int(parts[1]) / 1024)
    except ValueError:
        return None


_MEMINFO_KEYS = {
    "MemTotal": "mem_total_mb",
    "MemFree": "mem_free_mb",
    "MemAvailable": "mem_available_mb",
    "Cached": "cached_mb",
    "SwapFree": "swap_free_mb",
    "SwapTotal": "swap_total_mb",
}


def sysctl(name: str) -> Optional[str]:
    """`sysctl -n <name>` as text, or None off macOS / on any error."""
    if not IS_DARWIN:
        return None
    try:
        result = subprocess.run(["/usr/sbin/sysctl", "-n", name],
                                capture_output=True, text=True, timeout=5.0)
    except Exception:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def sysctl_int(name: str) -> Optional[int]:
    raw = sysctl(name)
    try:
        return int(raw) if raw is not None else None
    except ValueError:
        return None


def parse_vm_stat(text: str, page_size: Optional[int] = None,
                  total_bytes: Optional[int] = None
                  ) -> Dict[str, Optional[int]]:
    """`vm_stat` output -> the same MiB keys `/proc/meminfo` fills.

    The page size comes from the header line (`page size of 16384 bytes`) so
    the parser is right on both 4 KiB and 16 KiB pages; `page_size` overrides
    it (like `total_bytes`, `hw.memsize`) only for the fixture tests. Swap is
    not reported: `vm_stat` counts swap *events*, not free swap, so those keys
    stay null on this path and are filled only by the psutil one.
    "Available" is `free + inactive`, which is
    the arithmetic psutil's macOS `virtual_memory().available` uses and
    therefore the same quantity `inferio_worker.memory` prices MPS against --
    two readers of one number, not two definitions. Speculative pages are
    counted with free, as psutil does, and the compressor's own footprint
    (`occupied by compressor`) is reported as `cached_mb` because that is the
    regime the MPS pass has to watch: macOS compresses rather than swaps until
    it cannot.
    """
    counts: Dict[str, int] = {}
    for line in text.splitlines():
        if page_size is None and "page size of" in line:
            for token in line.replace(".", " ").split():
                if token.isdigit():
                    page_size = int(token)
                    break
            continue
        name, _, rest = line.partition(":")
        digits = rest.strip().rstrip(".")
        if not digits.isdigit():
            continue
        counts[name.strip().lower()] = int(digits)
    out: Dict[str, Optional[int]] = {value: None
                                     for value in _MEMINFO_KEYS.values()}
    if not counts or not page_size:
        return out

    def mib(*names: str) -> Optional[int]:
        pages = 0
        seen = False
        for name in names:
            if name in counts:
                pages += counts[name]
                seen = True
        return int(pages * page_size // MIB) if seen else None

    free = mib("pages free", "pages speculative")
    inactive = mib("pages inactive")
    out["mem_free_mb"] = free
    if free is not None and inactive is not None:
        out["mem_available_mb"] = free + inactive
    out["cached_mb"] = mib("pages occupied by compressor")
    total = total_bytes if total_bytes is not None else sysctl_int("hw.memsize")
    out["mem_total_mb"] = None if total is None else int(total // MIB)
    return out


def darwin_meminfo() -> Dict[str, Optional[int]]:
    """Per-sample RAM on macOS: psutil where it imports, `vm_stat` otherwise.

    psutil is preferred only because it is in-process -- a `vm_stat`
    subprocess per sample at 4 Hz is a cost the recorder does not need -- and
    both read the same `host_statistics64` counters. Which one answered is in
    every sample's `mem["source"]`, never inferred.
    """
    out: Dict[str, Optional[int]] = {value: None
                                     for value in _MEMINFO_KEYS.values()}
    if _PSUTIL is not None:
        try:
            virt = _PSUTIL.virtual_memory()
            swap = _PSUTIL.swap_memory()
            out.update({
                "mem_total_mb": int(virt.total // MIB),
                "mem_available_mb": int(virt.available // MIB),
                "mem_free_mb": int(virt.free // MIB),
                "swap_free_mb": int(swap.free // MIB),
                "swap_total_mb": int(swap.total // MIB),
            })
            out["source"] = "psutil"
            return out
        except Exception:
            pass
    try:
        result = subprocess.run(["/usr/bin/vm_stat"], capture_output=True,
                                text=True, timeout=5.0)
    except Exception:
        out["source"] = "none"
        return out
    out.update(parse_vm_stat(result.stdout))
    out["source"] = "vm_stat"
    return out


def meminfo() -> Dict[str, Optional[int]]:
    raw = _read_text("/proc/meminfo")
    out: Dict[str, Optional[int]] = {value: None for value in _MEMINFO_KEYS.values()}
    if raw is None:
        if IS_DARWIN:
            return darwin_meminfo()
        if _PSUTIL is not None:
            try:
                virt = _PSUTIL.virtual_memory()
                swap = _PSUTIL.swap_memory()
                out["mem_total_mb"] = int(virt.total // MIB)
                out["mem_available_mb"] = int(virt.available // MIB)
                out["mem_free_mb"] = int(virt.free // MIB)
                out["swap_free_mb"] = int(swap.free // MIB)
                out["swap_total_mb"] = int(swap.total // MIB)
            except Exception:
                pass
        return out
    for line in raw.splitlines():
        name, _, rest = line.partition(":")
        key = _MEMINFO_KEYS.get(name)
        if key is None:
            continue
        out[key] = _kb_to_mib(f"{name}: {rest.strip()}")
    return out


def iter_pids() -> List[int]:
    try:
        return sorted(int(name) for name in os.listdir("/proc") if name.isdigit())
    except OSError:
        if _PSUTIL is not None:
            try:
                return sorted(_PSUTIL.pids())
            except Exception:
                return []
        return []


try:  # optional, only as a non-Linux fallback
    import psutil as _PSUTIL  # type: ignore
except Exception:  # pragma: no cover
    _PSUTIL = None  # type: ignore


# --- NVML -----------------------------------------------------------------


class Nvml:
    """Thin NVML wrapper that degrades to "unavailable" instead of raising."""

    def __init__(self, wanted_indexes: Optional[List[int]] = None) -> None:
        self.available = False
        self.error: Optional[str] = None
        self.driver_version: Optional[str] = None
        self.nvml_version: Optional[str] = None
        self.handles: List[Any] = []
        self.meta: List[Dict[str, Any]] = []
        self._pynvml = None
        try:
            import pynvml  # type: ignore
        except Exception as exc:  # pragma: no cover - environment dependent
            self.error = f"pynvml import failed: {exc}"
            return
        self._pynvml = pynvml
        try:
            pynvml.nvmlInit()
        except Exception as exc:
            self.error = f"nvmlInit failed: {exc}"
            return
        self.available = True
        self.driver_version = _nvml_str(pynvml.nvmlSystemGetDriverVersion)
        self.nvml_version = _nvml_str(pynvml.nvmlSystemGetNVMLVersion)
        try:
            count = pynvml.nvmlDeviceGetCount()
        except Exception as exc:
            self.error = f"nvmlDeviceGetCount failed: {exc}"
            count = 0
        for index in range(count):
            if wanted_indexes is not None and index not in wanted_indexes:
                continue
            try:
                handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            except Exception as exc:
                self.meta.append(
                    {"index": index, "uuid": None, "name": None,
                     "total_mb": None, "pci_bus_id": None, "error": str(exc)}
                )
                self.handles.append(None)
                continue
            self.handles.append(handle)
            self.meta.append(
                {
                    "index": index,
                    "uuid": _dev_str(pynvml.nvmlDeviceGetUUID, handle),
                    "name": _dev_str(pynvml.nvmlDeviceGetName, handle),
                    "total_mb": _total_mb(pynvml, handle),
                    "pci_bus_id": _pci_bus_id(pynvml, handle),
                    "error": None,
                }
            )

    #: MPS's flag, so `build_sample` can tell the two oracles apart.
    unified = False

    def shutdown(self) -> None:
        if self.available and self._pynvml is not None:
            try:
                self._pynvml.nvmlShutdown()
            except Exception:
                pass

    def sample(self) -> List[Dict[str, Any]]:
        """Per-GPU totals plus the raw (pid, used_mb, type) process list."""
        rows: List[Dict[str, Any]] = []
        pynvml = self._pynvml
        for meta, handle in zip(self.meta, self.handles):
            row: Dict[str, Any] = {
                "index": meta["index"],
                "uuid": meta["uuid"],
                "name": meta["name"],
                "total_mb": meta["total_mb"],
                "used_mb": None,
                "free_mb": None,
                "error": meta.get("error"),
                "_procs": [],
            }
            if handle is None or pynvml is None:
                rows.append(row)
                continue
            try:
                info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                row["total_mb"] = int(info.total // MIB)
                row["used_mb"] = int(info.used // MIB)
                row["free_mb"] = int(info.free // MIB)
            except Exception as exc:
                row["error"] = f"memoryInfo: {exc}"
            for kind, getter in (
                ("compute", "nvmlDeviceGetComputeRunningProcesses"),
                ("graphics", "nvmlDeviceGetGraphicsRunningProcesses"),
            ):
                fn = _first_attr(pynvml, (getter + "_v3", getter + "_v2", getter))
                if fn is None:
                    continue
                try:
                    for entry in fn(handle):
                        used = getattr(entry, "usedGpuMemory", None)
                        row["_procs"].append(
                            {
                                "pid": int(entry.pid),
                                # N/A is None or the 2**64-1 sentinel, by
                                # pynvml version.
                                "used_mb": (
                                    None
                                    if used is None or used >= 2**63
                                    else int(used // MIB)
                                ),
                                "type": kind,
                            }
                        )
                except Exception as exc:
                    prior = row.get("error")
                    note = f"{getter}: {exc}"
                    row["error"] = note if not prior else f"{prior}; {note}"
            rows.append(row)
        return rows


def _first_attr(module: Any, names: Iterable[str]) -> Optional[Any]:
    for name in names:
        fn = getattr(module, name, None)
        if fn is not None:
            return fn
    return None


def _nvml_str(fn: Any) -> Optional[str]:
    try:
        value = fn()
    except Exception:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _dev_str(fn: Any, handle: Any) -> Optional[str]:
    try:
        value = fn(handle)
    except Exception:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _total_mb(pynvml: Any, handle: Any) -> Optional[int]:
    try:
        return int(pynvml.nvmlDeviceGetMemoryInfo(handle).total // MIB)
    except Exception:
        return None


def _pci_bus_id(pynvml: Any, handle: Any) -> Optional[str]:
    try:
        info = pynvml.nvmlDeviceGetPciInfo(handle)
    except Exception:
        return None
    value = getattr(info, "busId", None)
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


# --- The macOS oracle: RAM, the wired limit, and no per-process figure -----


class MpsOracle:
    """`Nvml`'s shape on Apple Silicon, where there is no NVML and no
    per-process GPU counter.

    One synthetic GPU row keyed `GPU-MPS`, the constant the orchestrator uses
    (`mps.rs::DEVICE_KEY`), so `analyze.py` joins it to `/health` by the same
    uuid match as everywhere else. Its `total` is the GPU wired limit and its
    `free` is `min(total, RAM available)` -- the unified formula
    `inferio_worker.memory.mps_free_total_mb` uses, so the oracle and the
    worker are reading the same quantity from different sides.

    Nothing in the row is per-process: `oracle_source` is `"mps-ram"` and
    every `procs` entry has a null `used_mb`, which is what makes
    `analyze.py`'s three per-process checks SKIP instead of subtracting zero.
    """

    unified = True

    def __init__(self, memsize_mb: Optional[int] = None,
                 wired_limit_mb: Optional[int] = None,
                 chip: Optional[str] = None) -> None:
        # The three arguments are read from `sysctl` unless given; only the
        # fixture tests give them.
        # `available` is NVML's flag and stays false: nothing here is NVML.
        self.available = False
        self.error: Optional[str] = "no NVML on this platform (Apple Silicon)"
        self.driver_version: Optional[str] = None
        self.nvml_version: Optional[str] = None
        self.memsize_mb = self._memsize_mb() if memsize_mb is None else memsize_mb
        self.wired_limit_mb = (sysctl_int("iogpu.wired_limit_mb")
                               if wired_limit_mb is None else wired_limit_mb)
        self.chip = sysctl("machdep.cpu.brand_string") if chip is None else chip
        # A wired limit of 0 means "the driver's default", which is what
        # `recommended_max_memory()` then reports; the worker's own figure in
        # `/health` stays the authoritative one either way.
        if self.wired_limit_mb:
            self.total_mb: Optional[int] = self.wired_limit_mb
            self.total_source = "sysctl iogpu.wired_limit_mb"
        elif self.memsize_mb:
            self.total_mb = int(self.memsize_mb * MPS_DEFAULT_TOTAL_FRACTION)
            self.total_source = "hw.memsize * 0.75 (the driver default)"
        else:
            self.total_mb = None
            self.total_source = "unknown"
        self.meta = [{
            "index": 0,
            "uuid": MPS_DEVICE_KEY,
            "name": self.gpu_name(),
            "total_mb": self.total_mb,
            "pci_bus_id": None,
            "error": None,
        }]

    @staticmethod
    def _memsize_mb() -> Optional[int]:
        size = sysctl_int("hw.memsize")
        return None if size is None else int(size // MIB)

    def gpu_name(self) -> Optional[str]:
        """Byte-identical to the worker's `mps_gpu_name()` for this host."""
        if self.chip is None or not self.memsize_mb:
            return None
        gib = self.memsize_mb / 1024.0
        return f"{self.chip} ({max(int(gib + 0.5), 1)} GB)"

    def shutdown(self) -> None:
        return None

    def sample(self, mem: Dict[str, Optional[int]]) -> List[Dict[str, Any]]:
        available = (mem or {}).get("mem_available_mb")
        free_mb = used_mb = None
        if self.total_mb is not None and available is not None:
            free_mb = max(0, min(self.total_mb, int(available)))
            used_mb = self.total_mb - free_mb
        return [{
            "index": 0,
            "uuid": MPS_DEVICE_KEY,
            "name": self.meta[0]["name"],
            "total_mb": self.total_mb,
            "used_mb": used_mb,
            "free_mb": free_mb,
            "error": None,
            "oracle_source": "mps-ram",
            "oracle_age_ms": None,
            "_procs": [],
        }]


# --- The Windows oracle: nvidia-smi where NVML has no per-process figure ---


def parse_compute_apps(text: str) -> Dict[int, Optional[int]]:
    """`{pid: used_mb}` from `nvidia-smi --query-compute-apps` CSV output.

    NVML's `usedGpuMemory` is **N/A on Windows' WDDM** -- the display driver
    owns the allocations and NVML cannot attribute them per process -- while
    `nvidia-smi --query-compute-apps=pid,used_memory` answers there, because
    it reads the figure through a different path. That asymmetry is what
    `docs/batch-calibration-test-protocol.md` §9 and the run1 report §8 both
    tell a Windows pass to use, and it is the only per-process attribution
    available on that platform.

    Every accepted form of the same output parses here, because the flags a
    caller reaches for vary: with or without `noheader`, with or without
    `nounits`. Values NVML itself could not answer arrive as `[N/A]` or
    `[Not Supported]` and become `None` -- never 0, for the same reason the
    NVML path never turns N/A into 0.
    """
    rows: Dict[int, Optional[int]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            # The header row (`pid, used_gpu_memory [MiB]`), or a message
            # line such as "No running processes found".
            continue
        rows[pid] = _parse_smi_mb(parts[1])
    return rows


def _parse_smi_mb(value: str) -> Optional[int]:
    """`"92908 MiB"` / `"92908"` -> 92908; anything unanswerable -> None.

    The unit is checked rather than stripped. `nvidia-smi` prints
    `used_memory` in MiB and `--format=csv,nounits` prints it bare, so those
    are the two forms; a cell in any *other* unit would be a silent factor of
    1024 in `base_accuracy` and `footprint_agreement` on the one platform with
    no second reader, and `None` makes it visible instead.
    """
    token = value.strip().rstrip("]").lstrip("[")
    if not token or token.lower().startswith(("n/a", "not supported",
                                              "insufficient")):
        return None
    parts = token.split()
    if len(parts) > 1 and parts[1].lower() != "mib":
        return None
    try:
        return int(float(parts[0]))
    except ValueError:
        return None


class SmiOracle:
    """Per-process VRAM from `nvidia-smi`, queried one GPU at a time.

    Only ever consulted when NVML's own per-process figure is unavailable
    (`--smi auto`, the default), because a subprocess per GPU per sample costs
    tens to hundreds of milliseconds and the recorder's cadence is 4 Hz. The
    query is scoped with `-i <uuid>` rather than parsed for a GPU column, so
    attribution comes from the driver and the parser stays the two-column one
    the platform notes name.

    A reading is reused for `min_interval` seconds and every sample says how
    old the one it used was (`oracle_age_ms`), so a slow `nvidia-smi` shows up
    as staleness in the recording instead of as a dropped sample.
    """

    def __init__(self, binary: str, min_interval: float,
                 timeout: float = 5.0) -> None:
        self.binary = binary
        self.min_interval = min_interval
        self.timeout = timeout
        self.error: Optional[str] = None
        #: set once a query has priced a GPU NVML listed but could not price,
        #: which is this host's own proof that an empty NVML list may be a
        #: hidden answer rather than an idle board.
        self.proved_nvml_blind = False
        self._cache: Dict[str, Tuple[float, Dict[int, Optional[int]]]] = {}

    def read(self, uuid: Optional[str]) -> Tuple[Dict[int, Optional[int]],
                                                 Optional[float]]:
        """`(pid -> used_mb, age_seconds)` for one GPU."""
        key = uuid or "*"
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached is not None and now - cached[0] < self.min_interval:
            return cached[1], now - cached[0]
        command = [self.binary]
        if uuid:
            command += ["-i", uuid]
        command += ["--query-compute-apps=pid,used_memory", "--format=csv"]
        try:
            result = subprocess.run(command, capture_output=True, text=True,
                                    timeout=self.timeout)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:200]
            return ({} if cached is None else cached[1],
                    None if cached is None else now - cached[0])
        if result.returncode != 0:
            self.error = (result.stderr or result.stdout).strip()[:200]
            return ({} if cached is None else cached[1],
                    None if cached is None else now - cached[0])
        self.error = None
        rows = parse_compute_apps(result.stdout)
        self._cache[key] = (now, rows)
        return rows, 0.0


def nvml_is_blind(procs: List[Dict[str, Any]]) -> bool:
    """Whether NVML listed processes on this GPU and priced none of them.

    The WDDM signature, and the same shape a container started without
    `--pid=host` produces. An *empty* list is not this shape: it is what an
    idle GPU looks like on every platform, which is why it is judged by
    `should_consult_smi` instead.
    """
    return bool(procs) and all(entry.get("used_mb") is None for entry in procs)


def should_consult_smi(procs: List[Dict[str, Any]],
                       host_proven_blind: bool = False) -> bool:
    """Whether this GPU's NVML answer is worth an `nvidia-smi` subprocess.

    An idle GPU lists nothing, and that is the normal state of S2's and S3's
    board before the model loads, so an empty list only earns a subprocess
    where NVML is known to hide the answer: on Windows, or once a fallback
    query has already out-answered NVML on this host.
    """
    if procs:
        return nvml_is_blind(procs)
    return IS_WINDOWS or host_proven_blind


# --- Recorder -------------------------------------------------------------


class ProcCache:
    """Per-PID cmdline/environ cache; only a complete identity is memoized.

    NVML lists a PID as soon as it touches the driver, which a worker does
    *inside* its fork/exec window, when `cmdline` still reads empty and
    `environ` is not yet the child's. Memoizing that negative would pin it for
    the process's life, so an identity is cached only once complete (a real
    argv, plus a readable environ when env capture is on). A PID that is
    permanently unidentifiable settles into the cache after `MAX_ATTEMPTS`
    reads *and* `MIN_RETRY_S` of wall clock; the wall-clock half is what keeps
    the retry window from shrinking with `--interval`.

    See tools/calibration-protocol/README.md "vramrec.py - the independent
    oracle".
    """

    # Bounds on retrying an unresolved PID; see the class docstring.
    MAX_ATTEMPTS = 64
    MIN_RETRY_S = 60.0

    def __init__(self, env_keys: Iterable[str], capture_env: bool) -> None:
        self.env_keys = tuple(env_keys)
        self.capture_env = capture_env
        self._cache: Dict[int, Dict[str, Any]] = {}
        self._attempts: Dict[int, Tuple[int, float]] = {}

    def get(self, pid: int) -> Dict[str, Any]:
        cached = self._cache.get(pid)
        if cached is not None:
            return cached
        argv = proc_argv(pid)
        comm = proc_comm(pid)
        if self.capture_env:
            env, env_readable = proc_env_read(pid, self.env_keys)
        else:
            env, env_readable = {}, True
        cmdline = argv if argv is not None else (f"[{comm}]" if comm else None)
        entry = {"cmdline": cmdline, "comm": comm, "env": env}
        # A PID read during teardown must not cache nulls for its successor.
        if cmdline is None and comm is None:
            return entry
        now = time.monotonic()
        attempts, since = self._attempts.get(pid, (0, now))
        attempts += 1
        exhausted = attempts >= self.MAX_ATTEMPTS and now - since >= self.MIN_RETRY_S
        if (argv is not None and env_readable) or exhausted:
            self._cache[pid] = entry
            self._attempts.pop(pid, None)
        else:
            self._attempts[pid] = (attempts, since)
        return entry

    def forget_dead(self, live: Iterable[int]) -> None:
        live_set = set(live)
        for pid in [pid for pid in self._cache if pid not in live_set]:
            self._cache.pop(pid, None)
        for pid in [pid for pid in self._attempts if pid not in live_set]:
            self._attempts.pop(pid, None)


def build_sample(
    seq: int,
    nvml: Nvml,
    cache: ProcCache,
    pattern: Optional[re.Pattern],
    started_mono: float,
    smi: Optional[SmiOracle] = None,
    smi_always: bool = False,
) -> Dict[str, Any]:
    sample_started = time.monotonic()
    # Read once and shared: the unified oracle's free/total is derived from
    # the very same RAM reading the sample reports.
    mem_info = meminfo()
    unified = getattr(nvml, "unified", False)
    gpus = nvml.sample(mem_info) if unified else nvml.sample()
    for row in gpus:
        raw = row.pop("_procs")
        # `oracle_source` names which instrument priced this GPU's processes,
        # so a Windows recording is never read as if NVML had answered. NVML
        # wins the merge: the fallback only ever fills a null.
        nvml_priced = sum(1 for entry in raw if entry["used_mb"] is not None)
        if not unified:
            row["oracle_source"] = ("nvml" if raw and nvml_priced == len(raw)
                                    else "none")
            row["oracle_age_ms"] = None
        blind = nvml_is_blind(raw)
        if not unified and smi is not None and (
                smi_always
                or should_consult_smi(raw, smi.proved_nvml_blind)):
            smi_rows, age = smi.read(row.get("uuid"))
            if smi_rows:
                seen = {entry["pid"] for entry in raw}
                filled = 0
                # A null fill is not a fill: on WDDM the fallback answers
                # `[N/A]`, and only a figure earns the `nvidia-smi` label.
                priced = 0
                for entry in raw:
                    if entry["used_mb"] is None and entry["pid"] in smi_rows:
                        entry["used_mb"] = smi_rows[entry["pid"]]
                        filled += 1
                        priced += entry["used_mb"] is not None
                # A pid nvidia-smi sees and NVML did not is still on the GPU.
                for pid, used_mb in smi_rows.items():
                    if pid not in seen:
                        raw.append({"pid": pid, "used_mb": used_mb,
                                    "type": "compute"})
                        filled += 1
                        priced += used_mb is not None
                if blind and priced:
                    smi.proved_nvml_blind = True
                if filled:
                    # The reading was consulted, so its age is recorded even
                    # where it priced nothing.
                    row["oracle_age_ms"] = (None if age is None
                                            else round(age * 1000.0, 1))
                if priced:
                    row["oracle_source"] = ("nvml+nvidia-smi" if nvml_priced
                                            else "nvidia-smi")
            elif smi.error:
                prior = row.get("error")
                note = f"nvidia-smi: {smi.error}"
                row["error"] = note if not prior else f"{prior}; {note}"
        procs = []
        for entry in raw:
            pid = entry["pid"]
            meta = cache.get(pid)
            mem = proc_mem(pid)
            procs.append(
                {
                    "pid": pid,
                    "used_mb": entry["used_mb"],
                    "type": entry["type"],
                    "cmdline": meta["cmdline"],
                    "comm": meta["comm"],
                    "env": meta["env"],
                    "rss_mb": mem["rss_mb"],
                    "vmhwm_mb": mem["vmhwm_mb"],
                    "gone": meta["cmdline"] is None and mem["rss_mb"] is None,
                }
            )
        procs.sort(key=lambda item: (-(item["used_mb"] or 0), item["pid"]))
        row["procs"] = procs

    matched: List[Dict[str, Any]] = []
    live_pids: List[int] = []
    if pattern is not None:
        for pid in iter_pids():
            live_pids.append(pid)
            meta = cache.get(pid)
            cmdline = meta["cmdline"]
            if not cmdline or not pattern.search(cmdline):
                continue
            mem = proc_mem(pid)
            matched.append(
                {
                    "pid": pid,
                    "cmdline": cmdline,
                    "comm": meta["comm"],
                    "rss_mb": mem["rss_mb"],
                    "vmhwm_mb": mem["vmhwm_mb"],
                    "env": meta["env"],
                    "gone": mem["rss_mb"] is None,
                }
            )
        cache.forget_dead(live_pids)

    if unified and gpus:
        # No per-process GPU counter exists here, so the device row carries
        # our matched processes priced by RSS alone and `used_mb` null.
        gpus[0]["procs"] = [
            {"pid": entry["pid"], "used_mb": None, "type": "compute",
             "cmdline": entry["cmdline"], "comm": entry["comm"],
             "env": entry["env"], "rss_mb": entry["rss_mb"],
             "vmhwm_mb": entry["vmhwm_mb"], "gone": entry["gone"]}
            for entry in matched
        ]

    now_mono = time.monotonic()
    return {
        "schema": "vramrec/1",
        "kind": "sample",
        "seq": seq,
        "t_mono": round(now_mono - started_mono, 6),
        "t_wall": round(time.time(), 6),
        "iso": datetime.now(timezone.utc).isoformat(),
        "gpus": gpus,
        "mem": mem_info,
        "procs": matched,
        "sample_ms": round((now_mono - sample_started) * 1000.0, 3),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="NVML/RAM sampler for the batch-calibration test protocol.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--out", help="JSONL output path (default: stdout)")
    parser.add_argument("--interval", type=float, default=0.25,
                        help="seconds between samples")
    parser.add_argument("--duration", type=float, default=None,
                        help="stop after this many seconds (default: run until signalled)")
    parser.add_argument("--filter", default="inferio|panoptikon",
                        help="regex matched against /proc/<pid>/cmdline for the "
                             "RSS/VmHWM process list; empty string disables it")
    parser.add_argument("--gpu", type=int, action="append", dest="gpus",
                        help="restrict to this NVML index (repeatable; default: all)")
    parser.add_argument("--env-key", action="append", dest="env_keys", default=[],
                        help="extra environment variable to capture (repeatable)")
    parser.add_argument("--no-env", action="store_true",
                        help="do not read /proc/<pid>/environ at all")
    parser.add_argument("--smi", choices=("auto", "always", "never"),
                        default="auto",
                        help="per-process VRAM from `nvidia-smi "
                             "--query-compute-apps` when NVML cannot answer "
                             "(auto), on every sample (always), or not at all "
                             "(never). NVML's per-process figure is N/A on "
                             "Windows WDDM; this is the platform's oracle")
    parser.add_argument("--nvidia-smi", default="nvidia-smi",
                        help="path to the nvidia-smi binary")
    parser.add_argument("--smi-interval", type=float, default=1.0,
                        help="minimum seconds between nvidia-smi queries per "
                             "GPU; a sample reuses the last reading and "
                             "records its oracle_age_ms")
    parser.add_argument("--flush-every", type=int, default=1,
                        help="fsync-free flush cadence in samples")
    parser.add_argument("--quiet", action="store_true",
                        help="do not print the startup banner to stderr")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    if hasattr(signal, "SIGBREAK"):
        # Windows has no SIGTERM a parent can send: `legs.py` stops a recorder
        # with CTRL_BREAK, which arrives here. Without this the process is
        # killed instead and the last buffered samples are lost.
        signal.signal(signal.SIGBREAK, _handle_signal)  # type: ignore[attr-defined]

    pattern = re.compile(args.filter) if args.filter else None
    # No NVML is loaded on macOS at all: there is none, and the unified
    # oracle answers a different question rather than a degraded one.
    nvml: Any = MpsOracle() if IS_DARWIN else Nvml(args.gpus)
    cache = ProcCache(tuple(DEFAULT_ENV_KEYS) + tuple(args.env_keys),
                      not args.no_env)
    smi = (None if args.smi == "never" or IS_DARWIN
           else SmiOracle(args.nvidia_smi, max(0.0, args.smi_interval)))

    sink = open(args.out, "a", encoding="utf-8") if args.out else sys.stdout
    started_mono = time.monotonic()
    header = {
        "schema": "vramrec/1",
        "kind": "header",
        "t_wall": round(time.time(), 6),
        "t_mono": 0.0,
        "iso": datetime.now(timezone.utc).isoformat(),
        "host": os.uname().nodename if hasattr(os, "uname") else "unknown",
        "pid": os.getpid(),
        "argv": sys.argv,
        "interval_s": args.interval,
        "filter": args.filter,
        "nvml": {
            "available": nvml.available,
            "driver_version": nvml.driver_version,
            "nvml_version": nvml.nvml_version,
            "error": nvml.error,
        },
        "smi": {
            "mode": args.smi,
            "binary": args.nvidia_smi,
            "min_interval_s": args.smi_interval,
        },
        "gpus": nvml.meta,
    }
    if IS_DARWIN:
        header["oracle_source"] = "mps-ram"
        header["darwin"] = {
            "hw_memsize_mb": nvml.memsize_mb,
            "iogpu_wired_limit_mb": nvml.wired_limit_mb,
            "gpu_total_mb": nvml.total_mb,
            "gpu_total_source": nvml.total_source,
            "cpu_brand": nvml.chip,
            "mem_source": meminfo().get("source"),
            "note": "MPS has no per-process GPU counter; the worker's own "
                    "`driver_allocated` in /health "
                    "(torch.mps.driver_allocated_memory()) is the only "
                    "GPU-side self-report, and the authoritative device "
                    "total is the worker's recommended-max, not this row",
        }
    sink.write(json.dumps(header) + "\n")
    sink.flush()
    if not args.quiet:
        print(
            f"vramrec: {len(nvml.meta)} GPU(s), interval {args.interval}s, "
            f"out={args.out or 'stdout'}, pid={os.getpid()}",
            file=sys.stderr,
        )

    seq = 0
    deadline = None if args.duration is None else started_mono + args.duration
    try:
        while not _stop:
            tick = time.monotonic()
            sample = build_sample(seq, nvml, cache, pattern, started_mono,
                                  smi, args.smi == "always")
            sink.write(json.dumps(sample) + "\n")
            if args.flush_every <= 1 or seq % args.flush_every == 0:
                sink.flush()
            seq += 1
            if deadline is not None and time.monotonic() >= deadline:
                break
            sleep_for = args.interval - (time.monotonic() - tick)
            if sleep_for > 0:
                end = time.monotonic() + sleep_for
                while not _stop and time.monotonic() < end:
                    time.sleep(min(0.05, max(0.0, end - time.monotonic())))
    finally:
        sink.flush()
        if sink is not sys.stdout:
            sink.close()
        nvml.shutdown()
    if not args.quiet:
        print(f"vramrec: wrote {seq} samples", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
