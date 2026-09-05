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
        [--gpu 0 --gpu 1] [--env-key FOO] [--no-env] [--quiet]

Runs until SIGINT/SIGTERM (or `--duration`), flushes, exits 0. With no
`--out` it writes to stdout.

Output schema (JSONL)
---------------------
Line 1 is a `"kind": "header"` object: argv, interval, host, an `"nvml"` block
(`available`, `driver_version`, `nvml_version`, `error`) and the `"gpus"`
inventory (`index`, `uuid`, `name`, `total_mb`, `pci_bus_id`). Then samples:

    {"schema": "vramrec/1", "kind": "sample", "seq", "t_mono", "t_wall",
     "iso", "sample_ms",
     "gpus":  [{"index", "uuid", "name", "total_mb", "used_mb", "free_mb",
                "error", "procs": [{"pid", "used_mb", "cmdline", "comm",
                "type": "compute"|"graphics", "gone", "rss_mb", "vmhwm_mb",
                "env": {"CUDA_VISIBLE_DEVICES": str, ...}}]}],
     "mem":   {"mem_total_mb", "mem_available_mb", "mem_free_mb",
               "swap_free_mb", "cached_mb"},
     "procs": [{"pid", "cmdline", "comm", "rss_mb", "vmhwm_mb", "gone",
                "env"}]}

`used_mb` per process is NVML's `usedGpuMemory`, `null` (never 0) when the
driver reports N/A -- on Windows WDDM, and in a container started without
`--pid=host` (NVML then lists host PIDs). All MB are MiB.

Top-level `procs` lists every process whose cmdline matches `--filter`, VRAM
or not, so a CPU-GPU run and a worker's RSS/VmHWM come from one instrument. A
process that vanishes mid-sample yields `"gone": true` and nulls, and an NVML
failure degrades to a per-GPU `"error"`; neither aborts the recorder.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

MIB = 1024 * 1024

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
    raw = _read_text(f"/proc/{pid}/comm")
    return raw.strip() if raw is not None else None


def proc_env(pid: int, keys: Iterable[str]) -> Dict[str, str]:
    return proc_env_read(pid, keys)[0]


def proc_env_read(pid: int, keys: Iterable[str]) -> Tuple[Dict[str, str], bool]:
    """`(matched vars, environ was readable)`.

    The flag separates "not readable *yet*" (mid-fork) from "readable and
    carries none of these names" or "unreadable for good". `ProcCache` retries
    only the first.
    """
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


def meminfo() -> Dict[str, Optional[int]]:
    raw = _read_text("/proc/meminfo")
    out: Dict[str, Optional[int]] = {value: None for value in _MEMINFO_KEYS.values()}
    if raw is None:
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
) -> Dict[str, Any]:
    sample_started = time.monotonic()
    gpus = nvml.sample()
    for row in gpus:
        procs = []
        for entry in row.pop("_procs"):
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

    now_mono = time.monotonic()
    return {
        "schema": "vramrec/1",
        "kind": "sample",
        "seq": seq,
        "t_mono": round(now_mono - started_mono, 6),
        "t_wall": round(time.time(), 6),
        "iso": datetime.now(timezone.utc).isoformat(),
        "gpus": gpus,
        "mem": meminfo(),
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
    parser.add_argument("--flush-every", type=int, default=1,
                        help="fsync-free flush cadence in samples")
    parser.add_argument("--quiet", action="store_true",
                        help="do not print the startup banner to stderr")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    pattern = re.compile(args.filter) if args.filter else None
    nvml = Nvml(args.gpus)
    cache = ProcCache(tuple(DEFAULT_ENV_KEYS) + tuple(args.env_keys),
                      not args.no_env)

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
        "gpus": nvml.meta,
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
            sample = build_sample(seq, nvml, cache, pattern, started_mono)
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
