#!/usr/bin/env python3
"""hog.py - controllable external memory pressure (GPU via torch, RAM via numpy).

It takes memory the gateway does not own, on a schedule, and gives it back so
the driver sees the release (`torch.cuda.empty_cache()` after every shrink).
Every byte it claims is *touched*, so NVML and `MemAvailable` see real pages.

Usage
-----
    hog.py [common options] <schedule> [schedule args]
    hog.py hold 10240                       # take 10 GiB and keep it
    hog.py step 4096,60 16384,60 0,30       # (MiB, seconds) pairs, then stop
    hog.py ramp 0 40960 600                 # 0 -> 40 GiB over 10 minutes
    hog.py spike 90000 --every 30 --for 10  # spike to 90 GiB for 10 s every 30 s
    hog.py oscillate 0 40960 --period 20    # square wave, 20 s per half-cycle
    hog.py leave-free 12288                 # hold whatever leaves 12 GiB free
    hog.py idle                             # allocate nothing; drive it over HTTP

Common options are in `--help`; `--port N` adds an HTTP control endpoint on
127.0.0.1 (`GET /state`, `POST /set?mb=N|leave_free=N`, `/resume`, `/stop`).

Output schema (JSONL)
---------------------
Header: {"schema": "hog/1", "kind": "header", "target": "gpu"|"ram",
         "device", "gpu_uuid", "gpu_name", "schedule", "argv", "pid",
         "t_wall", "iso", "chunk_mb", "torch", "context_mb"}

Samples: {"schema": "hog/1", "kind": "state", "seq", "t_mono", "t_wall",
          "iso", "pid", "chunks", "total_mb", "last_error", "phase",
          "override": "mb"|"leave_free"|null,
          "target_mb" (asked for), "held_mb" (allocated and touched),
          "free_mb" (GPU, or MemAvailable), "own_mb" (NVML own-PID, or RSS),
          "oom" (cumulative failed allocation attempts)}

`held_mb` is the payload only; on a GPU the process also holds a CUDA context
(reported once as `context_mb` in the header), so GPU `used` rises by
`held_mb + context_mb` from nothing. Compare *deltas of `held_mb`* against
deltas of GPU `used` and of the hog PID's NVML usage (`oracle_calibrate.py`).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

MIB = 1024 * 1024

_stop = threading.Event()


def _handle_signal(signum, _frame):  # noqa: ANN001
    _stop.set()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- Backends --------------------------------------------------------------


class Backend:
    """Common interface: allocate/free touched chunks, report free memory."""

    name = "?"

    def chunk_bytes(self) -> int:
        raise NotImplementedError

    def alloc(self) -> Any:
        """Allocate and touch one chunk. Raises on failure."""
        raise NotImplementedError

    def release(self, chunks: List[Any]) -> None:
        raise NotImplementedError

    def reclaim(self) -> None:
        """Make an already-dropped allocation visible to the OS/driver."""
        return None

    def free_total_mb(self) -> Tuple[Optional[int], Optional[int]]:
        raise NotImplementedError

    def own_mb(self) -> Optional[int]:
        return None

    def describe(self) -> Dict[str, Any]:
        return {}


class GpuBackend(Backend):
    name = "gpu"

    def __init__(self, device: int, chunk_mb: int) -> None:
        import torch  # noqa: PLC0415 - deliberately lazy

        self.torch = torch
        if not torch.cuda.is_available():
            raise SystemExit("hog: --target gpu but torch reports no CUDA device")
        if device >= torch.cuda.device_count():
            raise SystemExit(
                f"hog: CUDA device {device} out of range "
                f"({torch.cuda.device_count()} visible)"
            )
        self.device = torch.device(f"cuda:{device}")
        self.index = device
        self._chunk_bytes = chunk_mb * MIB
        # Realise the context before the first measurement, so `context_mb`
        # separates the cost of being a CUDA process from the payload.
        before_free, _ = self._nvml_free_total()
        torch.zeros(1, dtype=torch.uint8, device=self.device).fill_(1)
        torch.cuda.synchronize(self.device)
        after_free, _ = self._nvml_free_total()
        self.context_mb = (
            None if before_free is None or after_free is None
            else max(0, before_free - after_free)
        )

    # -- NVML: GPU-level truth; mem_get_info is process-scoped for "own" ---
    def _nvml(self):  # noqa: ANN202
        if getattr(self, "_nvml_handle", "unset") == "unset":
            self._nvml_handle = None
            self._pynvml = None
            try:
                import pynvml  # type: ignore

                pynvml.nvmlInit()
                uuid = self.torch.cuda.get_device_properties(self.index).uuid
                target = f"GPU-{uuid}"
                for idx in range(pynvml.nvmlDeviceGetCount()):
                    handle = pynvml.nvmlDeviceGetHandleByIndex(idx)
                    got = pynvml.nvmlDeviceGetUUID(handle)
                    got = got.decode() if isinstance(got, bytes) else str(got)
                    if got == target:
                        self._nvml_handle = handle
                        break
                self._pynvml = pynvml
            except Exception:
                self._nvml_handle = None
        return self._pynvml, self._nvml_handle

    def _nvml_free_total(self) -> Tuple[Optional[int], Optional[int]]:
        pynvml, handle = self._nvml()
        if pynvml is not None and handle is not None:
            try:
                info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                return int(info.free // MIB), int(info.total // MIB)
            except Exception:
                pass
        try:
            free, total = self.torch.cuda.mem_get_info(self.index)
            return int(free // MIB), int(total // MIB)
        except Exception:
            return None, None

    def chunk_bytes(self) -> int:
        return self._chunk_bytes

    def alloc(self) -> Any:
        tensor = self.torch.empty(
            self._chunk_bytes, dtype=self.torch.uint8, device=self.device
        )
        tensor.fill_(1)  # touch: make the pages real
        self.torch.cuda.synchronize(self.device)
        return tensor

    def release(self, chunks: List[Any]) -> None:
        chunks.clear()
        self.reclaim()

    def reclaim(self) -> None:
        # Hand the pages back to the driver, not just to torch's caching
        # allocator, or nothing outside this process sees the release.
        try:
            self.torch.cuda.empty_cache()
            self.torch.cuda.synchronize(self.device)
        except Exception:
            pass

    def free_total_mb(self) -> Tuple[Optional[int], Optional[int]]:
        return self._nvml_free_total()

    def own_mb(self) -> Optional[int]:
        pynvml, handle = self._nvml()
        if pynvml is None or handle is None:
            return None
        pid = os.getpid()
        for getter in (
            "nvmlDeviceGetComputeRunningProcesses_v3",
            "nvmlDeviceGetComputeRunningProcesses_v2",
            "nvmlDeviceGetComputeRunningProcesses",
        ):
            fn = getattr(pynvml, getter, None)
            if fn is None:
                continue
            try:
                for entry in fn(handle):
                    if int(entry.pid) == pid:
                        used = getattr(entry, "usedGpuMemory", None)
                        if used is None or used >= 2**63:
                            return None
                        return int(used // MIB)
            except Exception:
                continue
            return None
        return None

    def describe(self) -> Dict[str, Any]:
        props = self.torch.cuda.get_device_properties(self.index)
        return {
            "device": self.index,
            "gpu_uuid": f"GPU-{props.uuid}",
            "gpu_name": props.name,
            "torch": self.torch.__version__,
            "context_mb": self.context_mb,
        }


class RamBackend(Backend):
    name = "ram"

    def __init__(self, chunk_mb: int) -> None:
        import numpy  # noqa: PLC0415

        self.numpy = numpy
        self._chunk_bytes = chunk_mb * MIB

    def chunk_bytes(self) -> int:
        return self._chunk_bytes

    def alloc(self) -> Any:
        block = self.numpy.empty(self._chunk_bytes, dtype=self.numpy.uint8)
        block[:] = 1  # touch every page
        return block

    def release(self, chunks: List[Any]) -> None:
        chunks.clear()

    def free_total_mb(self) -> Tuple[Optional[int], Optional[int]]:
        info = _meminfo()
        return info.get("MemAvailable"), info.get("MemTotal")

    def own_mb(self) -> Optional[int]:
        try:
            with open(f"/proc/{os.getpid()}/status", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        return int(int(line.split()[1]) / 1024)
        except OSError:
            pass
        return None

    def describe(self) -> Dict[str, Any]:
        return {"device": None, "numpy": self.numpy.__version__}


def _meminfo() -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                name, _, rest = line.partition(":")
                parts = rest.split()
                if parts:
                    try:
                        out[name] = int(int(parts[0]) / 1024)
                    except ValueError:
                        continue
    except OSError:
        try:
            import psutil  # type: ignore

            virt = psutil.virtual_memory()
            out["MemTotal"] = int(virt.total // MIB)
            out["MemAvailable"] = int(virt.available // MIB)
        except Exception:
            pass
    return out


# --- Schedules: elapsed s -> (target MiB, or None for leave-free; phase) --


class Schedule:
    kind = "?"
    finite = False

    def target(self, elapsed: float) -> Tuple[Optional[int], Optional[int], str]:
        """(absolute_mb, leave_free_mb, phase): one of the first two is set,
        or both are None, meaning "release everything"."""
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"kind": self.kind}

    def done(self, elapsed: float) -> bool:
        return False


class Hold(Schedule):
    kind = "hold"

    def __init__(self, mb: int) -> None:
        self.mb = mb

    def target(self, elapsed: float):
        return self.mb, None, "hold"

    def describe(self):
        return {"kind": self.kind, "mb": self.mb}


class Idle(Schedule):
    kind = "idle"

    def target(self, elapsed: float):
        return 0, None, "idle"


class LeaveFree(Schedule):
    kind = "leave-free"

    def __init__(self, mb: int) -> None:
        self.mb = mb

    def target(self, elapsed: float):
        return None, self.mb, "leave-free"

    def describe(self):
        return {"kind": self.kind, "leave_free_mb": self.mb}


class Step(Schedule):
    kind = "step"
    finite = True

    def __init__(self, steps: List[Tuple[int, float]]) -> None:
        self.steps = steps
        self.total = sum(seconds for _, seconds in steps)

    def target(self, elapsed: float):
        acc = 0.0
        for index, (mb, seconds) in enumerate(self.steps):
            acc += seconds
            if elapsed < acc:
                return mb, None, f"step[{index}]={mb}MiB"
        mb = self.steps[-1][0] if self.steps else 0
        return mb, None, "step[end]"

    def done(self, elapsed: float) -> bool:
        return elapsed >= self.total

    def describe(self):
        return {"kind": self.kind, "steps": self.steps}


class Ramp(Schedule):
    kind = "ramp"
    finite = True

    def __init__(self, start: int, end: int, seconds: float) -> None:
        self.start, self.end, self.seconds = start, end, max(0.001, seconds)

    def target(self, elapsed: float):
        frac = min(1.0, elapsed / self.seconds)
        return int(round(self.start + (self.end - self.start) * frac)), None, "ramp"

    def done(self, elapsed: float) -> bool:
        return elapsed >= self.seconds

    def describe(self):
        return {"kind": self.kind, "from_mb": self.start,
                "to_mb": self.end, "seconds": self.seconds}


class Spike(Schedule):
    kind = "spike"

    def __init__(self, mb: int, every: float, hold: float, base_mb: int) -> None:
        self.mb, self.every, self.hold, self.base = mb, max(0.001, every), hold, base_mb

    def target(self, elapsed: float):
        phase = elapsed % self.every
        if phase < self.hold:
            return self.mb, None, "spike"
        return self.base, None, "base"

    def describe(self):
        return {"kind": self.kind, "mb": self.mb, "every_s": self.every,
                "for_s": self.hold, "base_mb": self.base}


class Oscillate(Schedule):
    kind = "oscillate"

    def __init__(self, lo: int, hi: int, period: float) -> None:
        self.lo, self.hi, self.period = lo, hi, max(0.001, period)

    def target(self, elapsed: float):
        high = int(elapsed // self.period) % 2 == 1
        return (self.hi if high else self.lo), None, "hi" if high else "lo"

    def describe(self):
        return {"kind": self.kind, "lo_mb": self.lo, "hi_mb": self.hi,
                "half_period_s": self.period}


# --- Hog -------------------------------------------------------------------


class Hog:
    def __init__(self, backend: Backend, schedule: Schedule, args: argparse.Namespace) -> None:
        self.backend = backend
        self.schedule = schedule
        self.args = args
        self.chunks: List[Any] = []
        self.lock = threading.RLock()
        self.override: Optional[str] = None
        self.override_mb: int = 0
        self.oom = 0
        self.last_error: Optional[str] = None
        self.seq = 0
        self.started = time.monotonic()
        self.phase = "init"
        self.target_mb = 0
        self._last_free_eval = -1e9
        self._leave_free_target: Optional[int] = None

    # -- state ------------------------------------------------------------
    @property
    def held_mb(self) -> int:
        return int(len(self.chunks) * self.backend.chunk_bytes() // MIB)

    def state(self) -> Dict[str, Any]:
        free_mb, total_mb = self.backend.free_total_mb()
        return {
            "schema": "hog/1",
            "kind": "state",
            "seq": self.seq,
            "t_mono": round(time.monotonic() - self.started, 6),
            "t_wall": round(time.time(), 6),
            "iso": _now_iso(),
            "pid": os.getpid(),
            "backend": self.backend.name,
            "target_mb": self.target_mb,
            "held_mb": self.held_mb,
            "chunks": len(self.chunks),
            "free_mb": free_mb,
            "total_mb": total_mb,
            "own_mb": self.backend.own_mb(),
            "override": self.override,
            "phase": self.phase,
            "oom": self.oom,
            "last_error": self.last_error,
        }

    # -- allocation -------------------------------------------------------
    def apply(self, want_mb: int, emit=None) -> None:  # noqa: ANN001
        """Move the held amount towards `want_mb`.

        `emit`, when given, gets a `"progress"` state record every
        `--progress-every` seconds while an allocation is still in flight, so
        a slow ramp-up is recorded rather than being a gap.
        """
        chunk_mb = max(1, self.backend.chunk_bytes() // MIB)
        want_chunks = max(0, int(round(want_mb / chunk_mb)))
        last_emit = time.monotonic()
        with self.lock:
            if want_chunks < len(self.chunks):
                # Drop the excess, then hand the pages back so the driver
                # sees the release.
                del self.chunks[want_chunks:]
                self.backend.reclaim()
                return
            while len(self.chunks) < want_chunks:
                if _stop.is_set():
                    return
                try:
                    self.chunks.append(self.backend.alloc())
                except Exception as exc:  # OOM or any allocator failure
                    self.oom += 1
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    # Back off: hold what we got and stop trying this tick.
                    self.backend.reclaim()
                    return
                if emit is not None and (
                    time.monotonic() - last_emit >= self.args.progress_every
                ):
                    record = self.state()
                    record["kind"] = "progress"
                    emit(record)
                    last_emit = time.monotonic()

    def release_all(self) -> None:
        with self.lock:
            self.backend.release(self.chunks)

    # -- schedule ---------------------------------------------------------
    def resolve_target(self, elapsed: float) -> int:
        if self.override == "mb":
            self.phase = "override:mb"
            return self.override_mb
        if self.override == "leave_free":
            self.phase = "override:leave-free"
            return self._leave_free(self.override_mb)
        absolute, leave_free, phase = self.schedule.target(elapsed)
        self.phase = phase
        if leave_free is not None:
            return self._leave_free(leave_free)
        return int(absolute or 0)

    def _leave_free(self, leave_mb: int) -> int:
        now = time.monotonic()
        if (
            self._leave_free_target is not None
            and now - self._last_free_eval < self.args.reeval
        ):
            return self._leave_free_target
        free_mb, _ = self.backend.free_total_mb()
        self._last_free_eval = now
        if free_mb is None:
            self._leave_free_target = self.held_mb
            return self._leave_free_target
        # Re-solve from where we actually are: hold what we hold, plus
        # whatever free memory exceeds the amount we were told to leave.
        self._leave_free_target = max(0, self.held_mb + (free_mb - leave_mb))
        return self._leave_free_target


# --- HTTP control ----------------------------------------------------------


def make_handler(hog: Hog):  # noqa: ANN201
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):  # silence the default stderr spam
            return

        def _reply(self, code: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            path = urlparse(self.path).path
            if path in ("/state", "/"):
                self._reply(200, hog.state())
            else:
                self._reply(404, {"error": "unknown path", "path": path})

        def do_POST(self):  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            path = parsed.path
            if path == "/set":
                if "mb" in query:
                    hog.override, hog.override_mb = "mb", int(float(query["mb"][0]))
                    hog._leave_free_target = None
                    hog._last_free_eval = -1e9
                elif "leave_free" in query:
                    hog.override = "leave_free"
                    hog.override_mb = int(float(query["leave_free"][0]))
                    hog._leave_free_target = None
                    hog._last_free_eval = -1e9
                else:
                    self._reply(400, {"error": "need ?mb= or ?leave_free="})
                    return
                self._reply(200, hog.state())
            elif path == "/resume":
                hog.override = None
                hog._leave_free_target = None
                hog._last_free_eval = -1e9
                self._reply(200, hog.state())
            elif path == "/stop":
                self._reply(200, {"stopping": True, **hog.state()})
                _stop.set()
            else:
                self._reply(404, {"error": "unknown path", "path": path})

    return Handler


# --- CLI -------------------------------------------------------------------


def parse_steps(values: List[str]) -> List[Tuple[int, float]]:
    steps: List[Tuple[int, float]] = []
    for value in values:
        mb_str, _, sec_str = value.partition(",")
        if not sec_str:
            raise SystemExit(f"hog: step {value!r} must be MB,SECONDS")
        steps.append((int(float(mb_str)), float(sec_str)))
    return steps


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="External memory pressure generator (GPU or RAM).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="Schedules: hold, step, ramp, spike, oscillate, leave-free, idle.",
    )
    parser.add_argument("--target", choices=("gpu", "ram"), default="gpu")
    parser.add_argument("--device", type=int, default=0,
                        help="CUDA device index (--target gpu)")
    parser.add_argument("--chunk-mb", type=int, default=128,
                        help="allocation granularity in MiB")
    parser.add_argument("--tick", type=float, default=0.5,
                        help="seconds between schedule evaluations")
    parser.add_argument("--reeval", type=float, default=2.0,
                        help="seconds between leave-free re-evaluations")
    parser.add_argument("--progress-every", type=float, default=2.0,
                        help="seconds between `progress` records while a large "
                             "allocation is still in flight")
    parser.add_argument("--duration", type=float, default=None,
                        help="exit after this many seconds")
    parser.add_argument("--port", type=int, default=None,
                        help="HTTP control port on 127.0.0.1")
    parser.add_argument("--out", default=None, help="JSONL log path (default: stdout)")
    parser.add_argument("--hold-at-end", action="store_true",
                        help="keep holding after a finite schedule finishes")
    parser.add_argument("--quiet", action="store_true")

    subs = parser.add_subparsers(dest="schedule", required=True)

    sub = subs.add_parser("hold", help="hold a fixed amount")
    sub.add_argument("mb", type=int)

    sub = subs.add_parser("step", help="a list of MB,SECONDS steps")
    sub.add_argument("steps", nargs="+", metavar="MB,SECONDS")

    sub = subs.add_parser("ramp", help="linear ramp between two levels")
    sub.add_argument("from_mb", type=int)
    sub.add_argument("to_mb", type=int)
    sub.add_argument("seconds", type=float)

    sub = subs.add_parser("spike", help="periodic spike above a base level")
    sub.add_argument("mb", type=int)
    sub.add_argument("--every", type=float, required=True, help="period in seconds")
    sub.add_argument("--for", dest="hold_s", type=float, required=True,
                     help="spike duration in seconds")
    sub.add_argument("--base-mb", type=int, default=0)

    sub = subs.add_parser("oscillate", help="square wave between two levels")
    sub.add_argument("lo_mb", type=int)
    sub.add_argument("hi_mb", type=int)
    sub.add_argument("--period", type=float, required=True,
                     help="seconds per half-cycle")

    sub = subs.add_parser("leave-free", help="hold so that exactly N MiB stays free")
    sub.add_argument("mb", type=int)

    subs.add_parser("idle", help="allocate nothing; drive it over HTTP")
    return parser


def build_schedule(args: argparse.Namespace) -> Schedule:
    name = args.schedule
    if name == "hold":
        return Hold(args.mb)
    if name == "step":
        return Step(parse_steps(args.steps))
    if name == "ramp":
        return Ramp(args.from_mb, args.to_mb, args.seconds)
    if name == "spike":
        return Spike(args.mb, args.every, args.hold_s, args.base_mb)
    if name == "oscillate":
        return Oscillate(args.lo_mb, args.hi_mb, args.period)
    if name == "leave-free":
        return LeaveFree(args.mb)
    return Idle()


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    backend: Backend = (
        GpuBackend(args.device, args.chunk_mb)
        if args.target == "gpu"
        else RamBackend(args.chunk_mb)
    )
    schedule = build_schedule(args)
    hog = Hog(backend, schedule, args)

    sink = open(args.out, "a", encoding="utf-8") if args.out else sys.stdout
    header = {
        "schema": "hog/1",
        "kind": "header",
        "t_wall": round(time.time(), 6),
        "iso": _now_iso(),
        "pid": os.getpid(),
        "argv": sys.argv,
        "target": backend.name,
        "chunk_mb": args.chunk_mb,
        "schedule": schedule.describe(),
        **backend.describe(),
    }
    sink.write(json.dumps(header) + "\n")
    sink.flush()

    server = None
    if args.port:
        server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(hog))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        if not args.quiet:
            print(f"hog: control on http://127.0.0.1:{args.port}/state", file=sys.stderr)

    if not args.quiet:
        print(
            f"hog: {backend.name} schedule={schedule.kind} pid={os.getpid()} "
            f"chunk={args.chunk_mb}MiB",
            file=sys.stderr,
        )

    deadline = None if args.duration is None else hog.started + args.duration
    try:
        while not _stop.is_set():
            tick_started = time.monotonic()
            elapsed = tick_started - hog.started
            hog.target_mb = hog.resolve_target(elapsed)

            def _emit(record):  # noqa: ANN001, ANN202
                sink.write(json.dumps(record) + "\n")
                sink.flush()

            hog.apply(hog.target_mb, _emit)
            sink.write(json.dumps(hog.state()) + "\n")
            sink.flush()
            hog.seq += 1
            if deadline is not None and time.monotonic() >= deadline:
                break
            if (
                schedule.finite
                and hog.override is None
                and schedule.done(elapsed)
                and not args.hold_at_end
            ):
                break
            sleep_for = args.tick - (time.monotonic() - tick_started)
            if sleep_for > 0:
                _stop.wait(sleep_for)
    finally:
        hog.release_all()
        final = hog.state()
        final["kind"] = "final"
        sink.write(json.dumps(final) + "\n")
        sink.flush()
        if sink is not sys.stdout:
            sink.close()
        if server is not None:
            server.shutdown()
    if not args.quiet:
        print(f"hog: released; {hog.oom} failed allocation(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
