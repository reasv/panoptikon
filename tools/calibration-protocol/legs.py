#!/usr/bin/env python3
"""legs.py - run one platform-pass scenario end to end, on any OS.

The platform-pass set is S1, S2, S3, S4a-S4d, S5 and S14
(`docs/batch-calibration-test-protocol.md` §9: "A platform passes when S1, S2,
S3, S4a-d, S5, S14 and the platform's own field-pass items pass"). Until now
each of those was a bash driver written for this host - `runjob2.sh`, `s4g.sh`,
`drive-b.sh`, `drive-d.sh` - built out of `curl`, `nohup`, `%1` job control,
`kill -TERM`, `/proc` and shell process substitution. None of that exists on
Windows, and the Windows/WDDM pass is the one that finally exercises the
degraded base tier (run1 report §8, run2 report §10). This tool is those
drivers, in Python: stdlib only, `pathlib` for every path, `urllib` instead of
`curl`, `subprocess` with an explicit termination protocol instead of job
control, and no shell anywhere.

Usage
-----
    legs.py --scenario S2 --bin PATH --config C1 --results DIR \\
            [--run-id run3] [--gpu-total-mb 24564] [--python PATH] \\
            [--model ID] [--corpus DIR] [--note "..."] [--port N] \\
            [--seed-calibration FILE] [--job-cap S] [--settle S] \\
            [--hog-device N] [--min-free-mb 4096] [--list] [--dry-run]

`--list` prints the scenario table and exits; `--dry-run` resolves everything
(directory, ports, hog schedule in MiB, the job plan) and prints it without
starting a process.

What it does, in order
----------------------
1. `newrun.py --scenario ... --run-id ...` for the results directory and
   `host.json`, so the layout is byte-identical to every earlier run and
   `analyze.py` reads it with no arguments beyond `--scenario`.
2. Starts `vramrec.py` (and, for the S4 legs, `hog.py`), waits for the hog to
   reach its target.
3. Starts `healthrec.py`, then the server binary with `--config <toml>
   --root <dir>/root --disable-update-check`, and samples the gateway's
   descriptor count into `fds.jsonl` from a thread (see "Descriptors").
4. Waits for `/api/client-config`, snapshots `/api/inference/health`, creates
   the `cal` databases, points the job config at the corpus, rescans.
5. Posts the extraction job, fires the scenario's timed hog events, waits for
   the queue to drain.
6. Snapshots jobs / failures / metadata / health, copies
   `calibration.toml` out as `calibration.after.toml`, stops everything in the
   reverse order it started them, copies `panoptikon.log`.
7. Writes `legs.json`: every resolved parameter, every process, every event
   with its wall clock, and the `analyze.py` command line for this scenario.

It does **not** run `analyze.py`: a leg's verdicts often need a `--probe` from
`ceiling_probe.py` and a `--baseline-jobs` from a C0 run, and those are the
reader's choice. The command it *would* run is printed at the end and stored
in `legs.json` under `analyze_command`.

`--gpu-total-mb`, and the scaling rule
--------------------------------------
Every hog figure in the scenario table is a **fraction of the GPU's total**,
not a number of MiB, because a schedule written for a 97 887 MiB board says
nothing on a 24 564 MiB one: `leave-free 12288` is comfortable on the first
and more than the whole model plus corpus on the second. The rule is:

    mib = round(fraction x gpu_total_mb)

with `--gpu-total-mb` defaulting to the board `vramrec.py` reports for
`--hog-device`. A `leave-free` figure is then floored at `--min-free-mb`
(default 4 096) so the model under test still fits on a small card, and a
`hold` figure is capped at `gpu_total_mb - --min-free-mb` for the same reason.
Both the fraction and the resolved MiB are recorded in `legs.json`, and the
reference column in `--list` is the figure this host's runs used, so a
cross-platform comparison can state what changed.

Descriptors
-----------
`fds.jsonl` is sampled here rather than by a shell loop, in the JSONL form
`analyze.py::read_fds` accepts (`{"iso", "fds", "sockets", "limit"}`). On
Linux the count comes from `/proc/<pid>/fd` and the limit from
`/proc/<pid>/limits`; elsewhere from `psutil` if it is importable, and
otherwise the file is simply not written and `peak_fds` SKIPs as before. This
matters on any leg whose `unit_budget` passes ~100: with local inference each
in-flight predict is loopback HTTP inside one process and costs **two**
sockets in one descriptor table (Phase 6's F6).

Stopping a process, portably
----------------------------
POSIX: `SIGTERM` to the process, `SIGKILL` to its process group after
`--stop-grace` seconds. Windows: `CTRL_BREAK_EVENT` to the process group the
child was created in (`CREATE_NEW_PROCESS_GROUP`), then `TerminateProcess`.
The recorders handle `SIGBREAK` for exactly this reason, so a Windows
teardown flushes its last samples instead of losing them. `hog.py` is asked to
release over its own HTTP endpoint first, on every platform, because that is
the only stop that is observably complete before the process exits.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

HERE = Path(__file__).resolve().parent
IS_WINDOWS = os.name == "nt"

# The board every figure in SCENARIOS was measured against, so `--list` can
# print what this host actually ran beside the fraction.
REFERENCE_TOTAL_MB = 97887

DEFAULT_MODEL = "tags/wd-vit-tagger-v3"
DEFAULT_DB = "cal"


# --- the scenario table ----------------------------------------------------


@dataclass(frozen=True)
class HogEvent:
    """One timed change to the hog, `at_s` seconds after the job is posted."""

    at_s: float
    #: exactly one of these
    hold_fraction: Optional[float] = None
    leave_free_fraction: Optional[float] = None
    label: str = ""


@dataclass(frozen=True)
class Scenario:
    key: str
    note: str
    #: `count`, `ramp`, `ramp8`, `smoke` ... resolved under `results/corpus/`
    corpus: str
    model: str = DEFAULT_MODEL
    #: the hog's opening schedule, as a fraction of the board
    hog_hold_fraction: Optional[float] = None
    hog_leave_free_fraction: Optional[float] = None
    #: `--reeval` for the hog: 999999 pins it, so a shrinking `free` reading
    #: caused by *our own* pool does not make the hog take more.
    hog_reeval: Optional[int] = None
    events: Tuple[HogEvent, ...] = ()
    #: S3 only: stop the gateway after the first job and run a second one
    restart: bool = False
    #: S14 only: run the CI smoke assertions after the job
    smoke_api: bool = False
    #: what `analyze.py` should be asked for
    checks: str = "all"
    learning: bool = False
    expect: Tuple[str, ...] = ()
    #: what the scenario needs of the host before it starts
    preconditions: Tuple[str, ...] = ()


SCENARIOS: Dict[str, Scenario] = {
    "S1": Scenario(
        key="S1",
        note="inventory, identity and a full GPU (the neighbour still resident)",
        corpus="smoke",
        checks="oracle_agreement,base_accuracy,footprint_agreement,"
               "grant_safety,failures,job_outcome,ledger_invariant,peak_fds",
        preconditions=(
            "the GPU's other tenant is STILL RUNNING - this is the only leg "
            "that wants a full board",
        ),
    ),
    "S2": Scenario(
        key="S2",
        note="cold ramp on an idle GPU: empty store, the ramp corpus",
        corpus="ramp",
        checks="all",
        learning=True,
        preconditions=(
            "the GPU is idle (stop any other tenant)",
            "no calibration.toml is seeded: the ramp must start from the seed",
            "run ceiling_probe.py for the same model first, and pass its "
            "--probe/bisect files to analyze.py",
        ),
    ),
    "S3": Scenario(
        key="S3",
        note="restart and resume: the second job must start from the "
             "persisted anchor, not from the seed",
        corpus="ramp",
        restart=True,
        checks="all",
        preconditions=(
            "the GPU is idle",
            "seed the store with --seed-calibration <an S2 leg's "
            "calibration.after.toml>, or let the first job here write one",
        ),
    ),
    "S4a": Scenario(
        key="S4a",
        note="constant external pressure: the hog holds the board down to a "
             "fixed free level for the whole job",
        corpus="ramp",
        hog_leave_free_fraction=12288 / REFERENCE_TOTAL_MB,
        hog_reeval=999999,
        checks="all",
        preconditions=(
            "the GPU is idle apart from the hog",
            "judge `utilization` against the probe's boundary AT THE HOG'S "
            "FREE LEVEL, never the full-GPU boundary (run2 report §4.5)",
        ),
    ),
    "S4b": Scenario(
        key="S4b",
        note="step up: the hog takes another ~31% of the board 60 s into the "
             "job, between windows",
        corpus="ramp8",
        hog_hold_fraction=0.0,
        events=(HogEvent(at_s=60.0, hold_fraction=30720 / REFERENCE_TOTAL_MB,
                         label="step up"),),
        checks="all",
        preconditions=(
            "the GPU is idle apart from the hog",
            "the corpus must outlive the event: ramp8, not ramp",
            "judge the step latency against ONE IN-FLIGHT WINDOW, not a "
            "wall-clock 'few seconds' (run2 finding S4b-A1)",
        ),
    ),
    "S4c": Scenario(
        key="S4c",
        note="spike: the hog squeezes the board to ~2 GB free for 10 s at "
             "t = 90 s, then releases",
        corpus="ramp8",
        hog_hold_fraction=0.0,
        events=(
            HogEvent(at_s=90.0, leave_free_fraction=2048 / REFERENCE_TOTAL_MB,
                     label="spike"),
            HogEvent(at_s=100.0, hold_fraction=0.0, label="release"),
        ),
        checks="all",
        preconditions=(
            "the GPU is idle apart from the hog",
            "on Windows/WDDM the spike shows as a THROUGHPUT COLLAPSE, not an "
            "OOM: read `throughput_collapse` and per-batch `duration_ms`, and "
            "run the leg once more with 'Prefer No Sysmem Fallback' set",
        ),
    ),
    "S4d": Scenario(
        key="S4d",
        note="step down: the hog starts holding the board and releases "
             "everything at t = 120 s; the budget must grow back",
        corpus="ramp8",
        hog_leave_free_fraction=8192 / REFERENCE_TOTAL_MB,
        hog_reeval=999999,
        events=(HogEvent(at_s=120.0, hold_fraction=0.0, label="release"),),
        checks="all",
        preconditions=("the GPU is idle apart from the hog",),
    ),
    "S5": Scenario(
        key="S5",
        note="OOM backstop: a fault-injection fixture, whose failures must be "
             "absorbed without losing the job",
        corpus="smoke",
        model="calibfixture/oom_second_batch_cuda",
        checks="all",
        expect=("--expect-ooms", "1"),
        preconditions=(
            "run fixtures/install-fixtures.sh first, or point the gateway's "
            "config_dirs/impl_dirs at fixtures/registry and fixtures/impls",
            "record which classifier tier fired on every OOM "
            "(`oom_class.source`); on ROCm and MPS a missing message pattern "
            "means NO DEFLATION AT ALL (run2 report §10)",
        ),
    ),
    "S14": Scenario(
        key="S14",
        note="regression sanity: the CI smoke sequence plus one job",
        corpus="smoke",
        smoke_api=True,
        checks="failures,job_outcome,grant_safety,peak_fds",
    ),
}


# --- HTTP (urllib, because `curl` is not a given) --------------------------


class HttpError(RuntimeError):
    pass


def request(
    url: str,
    method: str = "GET",
    body: Optional[bytes] = None,
    content_type: Optional[str] = None,
    timeout: float = 30.0,
) -> Tuple[int, bytes]:
    """One HTTP call. Returns `(status, body)` and raises only on transport
    failure, so a 4xx/5xx a scenario *expects* is data rather than a crash."""
    req = urllib.request.Request(url, data=body, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()
    except Exception as exc:
        raise HttpError(f"{method} {url}: {type(exc).__name__}: {exc}") from exc


def get_json(url: str, timeout: float = 30.0) -> Any:
    status, payload = request(url, timeout=timeout)
    if status >= 400:
        raise HttpError(f"GET {url} -> {status}: {payload[:200]!r}")
    return json.loads(payload)


def save(url: str, path: Path, method: str = "GET",
         body: Optional[bytes] = None, content_type: Optional[str] = None,
         timeout: float = 60.0) -> int:
    """Fetch into a file; a non-2xx body is stored too, because that is often
    the evidence (S4g's per-model failure reason, S14's 403)."""
    try:
        status, payload = request(url, method, body, content_type, timeout)
    except HttpError as exc:
        path.write_text(json.dumps({"error": str(exc)}), encoding="utf-8")
        return 0
    path.write_bytes(payload)
    return status


# --- process supervision ---------------------------------------------------


@dataclass
class Child:
    name: str
    popen: subprocess.Popen
    log: Optional[Any] = None

    @property
    def pid(self) -> int:
        return self.popen.pid


class Supervisor:
    """Every child process this leg starts, and the one way to stop them.

    Started detached (a new session on POSIX, a new process group on Windows)
    so a stop signal reaches the child and its own children - a worker the
    gateway spawned, a `nvidia-smi` the recorder is waiting on - and never the
    driver itself.
    """

    def __init__(self, grace: float) -> None:
        self.grace = grace
        self.children: List[Child] = []

    def start(self, name: str, argv: Sequence[str], *,
              log_path: Optional[Path] = None,
              cwd: Optional[Path] = None,
              env: Optional[Dict[str, str]] = None) -> Child:
        handle = log_path.open("wb") if log_path is not None else None
        creationflags = 0
        if IS_WINDOWS:
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        popen = subprocess.Popen(
            [str(part) for part in argv],
            stdout=handle if handle is not None else subprocess.DEVNULL,
            stderr=subprocess.STDOUT if handle is not None
            else subprocess.DEVNULL,
            cwd=str(cwd) if cwd else None,
            env=env,
            creationflags=creationflags,
            start_new_session=not IS_WINDOWS,
        )
        child = Child(name, popen, handle)
        self.children.append(child)
        return child

    def stop(self, child: Child, grace: Optional[float] = None) -> str:
        """Ask a child to stop, then make it. Returns what it took."""
        if child.popen.poll() is not None:
            return f"already exited rc={child.popen.returncode}"
        grace = self.grace if grace is None else grace
        how = "SIGTERM"
        try:
            if IS_WINDOWS:
                how = "CTRL_BREAK"
                child.popen.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
            else:
                child.popen.send_signal(signal.SIGTERM)
        except Exception as exc:  # pragma: no cover - race with exit
            how = f"signal failed: {exc}"
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            if child.popen.poll() is not None:
                self._close(child)
                return f"{how}, rc={child.popen.returncode}"
            time.sleep(0.2)
        # Nothing polite worked. On POSIX the whole session goes, so a worker
        # the gateway leaked goes with it; on Windows TerminateProcess is all
        # there is.
        try:
            if IS_WINDOWS:
                child.popen.kill()
            else:
                os.killpg(os.getpgid(child.pid), signal.SIGKILL)
        except Exception:
            try:
                child.popen.kill()
            except Exception:
                pass
        try:
            child.popen.wait(timeout=10)
        except Exception:
            pass
        self._close(child)
        return f"{how} then killed after {grace:.0f}s, rc={child.popen.returncode}"

    def stop_all(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for child in reversed(self.children):
            out[child.name] = self.stop(child)
        return out

    @staticmethod
    def _close(child: Child) -> None:
        if child.log is not None:
            try:
                child.log.close()
            except Exception:
                pass
            child.log = None


# --- descriptors -----------------------------------------------------------


def fd_reader(pid: int) -> Optional[Callable[[], Tuple[int, Optional[int]]]]:
    """`() -> (open descriptors, of which sockets)` for `pid`, or None.

    Linux reads `/proc/<pid>/fd` directly - one `listdir` and one `readlink`
    per entry, which is what the shell loop did with `ls | wc -l`. Everywhere
    else this needs `psutil`, and where that is absent the recording is simply
    skipped: `analyze.py`'s `peak_fds` is report-only and SKIPs cleanly.
    """
    proc_fd = Path(f"/proc/{pid}/fd")
    if proc_fd.is_dir():
        def read_linux() -> Tuple[int, Optional[int]]:
            entries = list(proc_fd.iterdir())
            sockets = 0
            for entry in entries:
                try:
                    if os.readlink(entry).startswith("socket:"):
                        sockets += 1
                except OSError:
                    pass
            return len(entries), sockets
        return read_linux
    try:
        import psutil  # type: ignore
    except Exception:
        return None
    handle = psutil.Process(pid)

    def read_psutil() -> Tuple[int, Optional[int]]:
        try:
            count = (handle.num_fds() if hasattr(handle, "num_fds")
                     else handle.num_handles())
        except Exception:
            return 0, None
        try:
            sockets = len(handle.net_connections(kind="inet"))
        except Exception:
            sockets = None
        return int(count), sockets
    return read_psutil


def fd_limit(pid: int) -> Optional[int]:
    """The process's OWN soft limit, not the shell's.

    The gateway raises its soft limit to the hard one at startup
    (`panoptikon/src/rlimit.rs`), so reading the limit anywhere but from the
    gateway's own `/proc/<pid>/limits` puts a number up to 512x too small in
    the `limit=` column - run1's Phase 7b did exactly that.
    """
    limits = Path(f"/proc/{pid}/limits")
    if limits.is_file():
        for line in limits.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("Max open files"):
                parts = line.split()
                if len(parts) >= 4 and parts[3].isdigit():
                    return int(parts[3])
        return None
    try:
        import psutil  # type: ignore

        soft, _hard = psutil.Process(pid).rlimit(psutil.RLIMIT_NOFILE)  # type: ignore[attr-defined]
        return int(soft)
    except Exception:
        return None


class FdRecorder(threading.Thread):
    """`fds.jsonl` in the JSONL shape `analyze.py::read_fds` accepts."""

    def __init__(self, pid: int, path: Path, interval: float = 0.5) -> None:
        super().__init__(daemon=True)
        self.reader = fd_reader(pid)
        self.limit = fd_limit(pid)
        self.path = path
        self.interval = interval
        self._stop = threading.Event()

    def run(self) -> None:
        if self.reader is None:
            return
        with self.path.open("a", encoding="utf-8") as sink:
            while not self._stop.is_set():
                try:
                    count, sockets = self.reader()
                except Exception:
                    break
                sink.write(json.dumps({
                    "iso": iso_now(), "fds": count, "sockets": sockets,
                    "limit": self.limit,
                }) + "\n")
                sink.flush()
                self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()


# --- small helpers ---------------------------------------------------------


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def wait_for(predicate: Callable[[], bool], timeout: float,
             interval: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


_ENV_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
_ENV_SUBST = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def read_env_file(path: Path, base: Dict[str, str]) -> Dict[str, str]:
    """`KEY=value` lines from a `config/env.C*` file, `${VAR:-default}` aware.

    `run-gateway.sh` did this with `set -a` and `.`; a Windows pass has no
    shell to do it with, and the substitution form is the only shell feature
    those files actually use.
    """
    out: Dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        if match is None:
            continue
        name, raw = match.group(1), match.group(2).strip()
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
            raw = raw[1:-1]

        def expand(hit: "re.Match[str]") -> str:
            var, default = hit.group(1), hit.group(2)
            merged = {**base, **out}
            return merged.get(var, default if default is not None else "")

        out[name] = _ENV_SUBST.sub(expand, raw)
    return out


def board_total_mb(device: int) -> Optional[int]:
    """The board's total, from NVML, for the hog scaling rule."""
    try:
        import pynvml  # type: ignore

        pynvml.nvmlInit()
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(device)
            return int(pynvml.nvmlDeviceGetMemoryInfo(handle).total // (1024 ** 2))
        finally:
            pynvml.nvmlShutdown()
    except Exception:
        return None


def scale_mb(fraction: float, total_mb: int) -> int:
    return int(round(fraction * total_mb))


# --- the driver ------------------------------------------------------------


@dataclass
class Leg:
    args: argparse.Namespace
    scenario: Scenario
    directory: Path
    python: str
    config_toml: Path
    env: Dict[str, str]
    base: str
    total_mb: int
    supervisor: Supervisor
    events: List[Dict[str, Any]] = field(default_factory=list)
    processes: Dict[str, Any] = field(default_factory=dict)

    # -- recording ----------------------------------------------------------

    def mark(self, name: str, **detail: Any) -> None:
        record = {"iso": iso_now(), "event": name, **detail}
        self.events.append(record)
        print(f"[{record['iso']}] {name}"
              + (f" {json.dumps(detail)}" if detail else ""), flush=True)

    def path(self, name: str) -> Path:
        return self.directory / name

    # -- the job API --------------------------------------------------------

    def queue_length(self) -> int:
        return len(get_json(f"{self.base}/api/jobs/queue", timeout=10)
                   .get("queue", []))

    def wait_for_queue(self, cap: float) -> str:
        """Wait for a job to appear and then drain, as the shell drivers did.

        The appear phase is bounded separately: a job that never enters the
        queue is a different fault from one that never leaves it, and the
        combined wait would report the wrong one.
        """
        if not wait_for(lambda: self.queue_length() != 0, 30.0):
            self.mark("job_never_queued")
        started = time.monotonic()
        while True:
            try:
                if self.queue_length() == 0:
                    return "drained"
            except Exception:
                pass
            if time.monotonic() - started > cap:
                self.mark("job_cap_exceeded", cap_s=cap)
                return "cap_exceeded"
            time.sleep(2.0)

    def prepare_database(self, corpus: Path) -> None:
        db = DEFAULT_DB
        save(f"{self.base}/api/db/create?new_index_db={db}&new_user_data_db={db}",
             self.path("dbcreate.json"), method="POST")
        config = get_json(f"{self.base}/api/jobs/config?index_db={db}")
        config["included_folders"] = [str(corpus)]
        self.path("config-set.json").write_text(json.dumps(config),
                                                encoding="utf-8")
        save(f"{self.base}/api/jobs/config?index_db={db}",
             self.path("config-put.json"), method="PUT",
             body=json.dumps(config).encode("utf-8"),
             content_type="application/json")
        self.mark("rescan_start", corpus=str(corpus))
        save(f"{self.base}/api/jobs/folders/rescan?index_db={db}",
             self.path("rescan.json"), method="POST")
        self.wait_for_queue(self.args.rescan_cap)
        save(f"{self.base}/api/jobs/folders/history?index_db={db}"
             f"&page=1&page_size=5", self.path("folders.json"))
        self.mark("rescan_done")

    def run_job(self, model: str, tag: str) -> str:
        db = DEFAULT_DB
        self.mark("job_start", model=model, tag=tag)
        status = save(
            f"{self.base}/api/jobs/data/extraction?index_db={db}"
            f"&inference_ids={urllib.parse.quote(model)}",
            self.path(f"extraction{tag}.json"), method="POST")
        self.mark("job_posted", http_status=status)
        outcome = self.wait_for_queue(self.args.job_cap)
        self.mark("job_end", outcome=outcome)
        save(f"{self.base}/api/jobs/data/history?index_db={db}"
             f"&page=1&page_size=50", self.path(f"jobs{tag}.json"))
        save(f"{self.base}/api/jobs/data/failures?index_db={db}",
             self.path(f"failures{tag}.json"))
        return outcome

    # -- the hog ------------------------------------------------------------

    def hog_url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.args.hog_port}{path}"

    def hog_schedule(self) -> Tuple[List[str], Dict[str, Any]]:
        """The opening schedule in MiB, plus what it was derived from."""
        scenario = self.scenario
        detail: Dict[str, Any] = {"gpu_total_mb": self.total_mb,
                                  "min_free_mb": self.args.min_free_mb}
        if scenario.hog_leave_free_fraction is not None:
            mib = max(self.args.min_free_mb,
                      scale_mb(scenario.hog_leave_free_fraction, self.total_mb))
            detail.update({"kind": "leave-free",
                           "fraction": scenario.hog_leave_free_fraction,
                           "mib": mib,
                           "reference_mib": scale_mb(
                               scenario.hog_leave_free_fraction,
                               REFERENCE_TOTAL_MB)})
            return ["leave-free", str(mib)], detail
        if scenario.hog_hold_fraction is not None:
            mib = min(scale_mb(scenario.hog_hold_fraction, self.total_mb),
                      max(0, self.total_mb - self.args.min_free_mb))
            detail.update({"kind": "hold",
                           "fraction": scenario.hog_hold_fraction,
                           "mib": mib,
                           "reference_mib": scale_mb(
                               scenario.hog_hold_fraction,
                               REFERENCE_TOTAL_MB)})
            return ["hold", str(mib)], detail
        return [], {}

    def resolved_events(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for event in self.scenario.events:
            row: Dict[str, Any] = {"at_s": event.at_s, "label": event.label}
            if event.leave_free_fraction is not None:
                row["leave_free_mb"] = max(
                    self.args.min_free_mb,
                    scale_mb(event.leave_free_fraction, self.total_mb))
                row["fraction"] = event.leave_free_fraction
            else:
                row["mb"] = min(
                    scale_mb(event.hold_fraction or 0.0, self.total_mb),
                    max(0, self.total_mb - self.args.min_free_mb))
                row["fraction"] = event.hold_fraction
            out.append(row)
        return out

    def drive_hog(self, events: List[Dict[str, Any]], posted_at: float) -> None:
        """Fire the scenario's timed hog changes, on a thread, from job-post.

        Timed from the job's POST rather than from the leg's start, because
        what the scenario is describing is a change *during* the job: run2's
        S4b step lands 60.0 s after the submit, not 60 s after the recorders
        came up.
        """
        for event in events:
            delay = posted_at + event["at_s"] - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            query = ("leave_free=%d" % event["leave_free_mb"]
                     if "leave_free_mb" in event else "mb=%d" % event["mb"])
            self.mark("hog_event_request", label=event["label"], query=query,
                      at_s=event["at_s"])
            try:
                request(self.hog_url(f"/set?{query}"), method="POST",
                        timeout=10)
            except HttpError as exc:
                self.mark("hog_event_failed", label=event["label"],
                          error=str(exc))
                continue
            self.mark("hog_event_ack", label=event["label"])
            # Record the fill, so the runlog can state how long the board took
            # to change and `analyze.py`'s hog_tracking has a wall clock.
            for _ in range(40):
                try:
                    state = get_json(self.hog_url("/state"), timeout=5)
                except HttpError:
                    break
                self.mark("hog_state", label=event["label"],
                          held_mb=state.get("held_mb"),
                          target_mb=state.get("target_mb"),
                          free_mb=state.get("free_mb"))
                target = state.get("target_mb") or 0
                held = state.get("held_mb") or 0
                if target == 0 and held == 0:
                    break
                if target and held >= target - 256:
                    break
                time.sleep(1.0)

    # -- gateway ------------------------------------------------------------

    def start_gateway(self) -> Child:
        root = self.path("root")
        root.mkdir(parents=True, exist_ok=True)
        argv = [str(self.args.bin), "--config", str(self.config_toml),
                "--root", str(root), "--disable-update-check"]
        child = self.supervisor.start("gateway", argv,
                                      log_path=self.path("gateway.out"),
                                      env=self.env)
        self.mark("gateway_started", pid=child.pid, argv=argv)
        return child

    def wait_for_gateway(self) -> bool:
        ok = wait_for(
            lambda: request(f"{self.base}/api/client-config", timeout=4)[0] == 200,
            self.args.startup_cap)
        self.mark("gateway_ready" if ok else "gateway_never_answered")
        return ok

    def snapshot_calibration(self, name: str) -> None:
        source = self.path("root") / "data" / "inferio" / "calibration.toml"
        if source.is_file():
            shutil.copyfile(source, self.path(name))
        else:
            self.mark("no_calibration_toml", expected=str(source))

    def copy_log(self) -> None:
        source = self.path("root") / "data" / "panoptikon.log"
        if source.is_file():
            shutil.copyfile(source, self.path("panoptikon.log"))
        else:
            # `[logging] file = ""` sends everything to the console; the
            # gateway's stdout capture is then the only log there is, and
            # `analyze.py` strips its ANSI escapes.
            gateway_out = self.path("gateway.out")
            if gateway_out.is_file():
                shutil.copyfile(gateway_out, self.path("panoptikon.log"))

    # -- S14 ----------------------------------------------------------------

    def smoke_assertions(self, corpus: Path) -> Dict[str, Any]:
        """The CI smoke sequence (`.github/workflows/release.yml`), as data.

        Every call records its status rather than asserting, because what a
        platform pass wants is the whole row - a 403 that came back 200 and a
        thumbnail that came back empty are different findings and both should
        survive into the runlog.
        """
        db = DEFAULT_DB
        out: Dict[str, Any] = {}
        # `PqlQuery.query` is an `Option<QueryElement>`, so **omitting** it
        # selects everything; sending `{}` does not match any variant of the
        # untagged enum and 400s. The CI's tag-match filter is fixture-
        # specific, and what this leg is asserting is that search, thumbnails
        # and file serving work at all on this platform.
        query = json.dumps({"page_size": 5}).encode("utf-8")
        status, payload = request(f"{self.base}/api/search/pql?index_db={db}",
                                  method="POST", body=query,
                                  content_type="application/json")
        out["pql"] = {"status": status}
        self.path("pql.json").write_bytes(payload)
        sha = None
        if status == 200:
            try:
                results = json.loads(payload).get("results") or []
                out["pql"]["count"] = json.loads(payload).get("count")
                sha = results[0].get("sha256") if results else None
            except Exception:
                pass
        if sha:
            out["thumbnail"] = {
                "status": save(
                    f"{self.base}/api/items/item/thumbnail?"
                    f"id={sha}&id_type=sha256&index_db={db}",
                    self.path("thumb.bin")),
                "bytes": self.path("thumb.bin").stat().st_size
                if self.path("thumb.bin").is_file() else 0,
            }
        else:
            out["thumbnail"] = {"skipped": "no sha256 from the PQL search"}
        # Original bytes by path, the way the scan attached them. `id_type=
        # path` is the branch that has to agree with the platform's own
        # separator, which is why it is worth asserting off Linux at all.
        served = sorted(entry for entry in corpus.rglob("*") if entry.is_file()
                        and entry.name != "manifest.json")
        if served:
            params = urllib.parse.urlencode({"id": str(served[0]),
                                             "id_type": "path",
                                             "index_db": db})
            status = save(f"{self.base}/api/items/item/file?{params}",
                          self.path("file.bin"))
            out["file"] = {"status": status, "path": str(served[0]),
                           "bytes": self.path("file.bin").stat().st_size
                           if self.path("file.bin").is_file() else 0}
        else:
            out["file"] = {"skipped": "the corpus directory holds no file"}
        if self.args.legacy_port:
            url = f"http://127.0.0.1:{self.args.legacy_port}/api/jobs/queue"
            try:
                out["legacy_ui_queue"] = {
                    "status": request(url, timeout=10)[0],
                    "expected_under_docker": 403,
                    "note": "the 403 is a Docker-only assertion: docker.toml "
                            "gives this port the public endpoint with "
                            "restricted_demo, while default.toml gives it "
                            "legacy_ui with the localhost policy, where 200 "
                            "is correct",
                }
            except HttpError as exc:
                out["legacy_ui_queue"] = {"error": str(exc)}
        return out

    # -- the whole leg ------------------------------------------------------

    def analyze_command(self) -> List[str]:
        argv = [self.python, str(HERE / "analyze.py"),
                "--scenario", str(self.directory),
                "--checks", self.scenario.checks]
        if self.scenario.learning:
            argv.append("--learning")
        argv += list(self.scenario.expect)
        argv += ["--json", str(self.path("verdicts.json"))]
        return argv


def resolve_config(args: argparse.Namespace) -> Tuple[Path, Path]:
    """`--config` as either a `C<n>` id or a path. Returns (toml, env file)."""
    given = str(args.config)
    candidate = Path(given)
    if candidate.is_file():
        env = candidate.parent / f"env.{candidate.stem.replace('server-', '')}"
        return candidate, env
    toml = HERE / "config" / f"server-{given}.toml"
    if not toml.is_file():
        raise SystemExit(
            f"legs.py: no config {given!r} - pass a path, or one of "
            + ", ".join(sorted(path.stem.replace("server-", "")
                               for path in (HERE / "config").glob("server-*.toml")))
        )
    return toml, HERE / "config" / f"env.{given}"


def config_port(toml: Path, key: str = "port") -> Optional[int]:
    try:
        import tomllib

        document = tomllib.loads(toml.read_text(encoding="utf-8"))
        return int(document.get("server", {}).get(key))
    except Exception:
        return None


def print_table() -> None:
    print(f"{'scenario':<7} {'corpus':<7} {'hog (fraction -> this host)':<40} "
          f"events")
    for scenario in SCENARIOS.values():
        if scenario.hog_leave_free_fraction is not None:
            hog = (f"leave-free {scenario.hog_leave_free_fraction:.5f} "
                   f"-> {scale_mb(scenario.hog_leave_free_fraction, REFERENCE_TOTAL_MB)} MiB")
        elif scenario.hog_hold_fraction == 0.0:
            hog = "hold 0 (up, holding only its CUDA context)"
        elif scenario.hog_hold_fraction is not None:
            hog = (f"hold {scenario.hog_hold_fraction:.5f} "
                   f"-> {scale_mb(scenario.hog_hold_fraction, REFERENCE_TOTAL_MB)} MiB")
        else:
            hog = "-"
        events = "; ".join(
            f"t+{event.at_s:g}s {event.label}" for event in scenario.events
        ) or "-"
        print(f"{scenario.key:<7} {scenario.corpus:<7} {hog:<40} {events}")
        print(f"        {scenario.note}")
        for line in scenario.preconditions:
            print(f"        ! {line}")
    print(f"\nThe hog fractions are of the GPU's total; the MiB column is "
          f"this host's reference board ({REFERENCE_TOTAL_MB} MiB). "
          f"--gpu-total-mb re-resolves them.")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one platform-pass calibration scenario, on any OS.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--scenario", help="one of: " + ", ".join(SCENARIOS))
    parser.add_argument("--list", action="store_true",
                        help="print the scenario table and exit")
    parser.add_argument("--bin", help="the panoptikon binary")
    parser.add_argument("--config", default="C1",
                        help="a server-C*.toml id, or a path to one")
    parser.add_argument("--results", default=str(HERE / "results"),
                        help="results root (newrun.py's --results)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--note", default=None)
    parser.add_argument("--python", default=sys.executable,
                        help="interpreter for the recorder subprocesses")
    parser.add_argument("--model", default=None,
                        help="override the scenario's inference id")
    parser.add_argument("--corpus", default=None,
                        help="override the corpus directory")
    parser.add_argument("--port", type=int, default=None,
                        help="gateway port (default: read from the config)")
    parser.add_argument("--legacy-port", type=int, default=None,
                        help="S14's second listener, for the 403 probe")
    parser.add_argument("--gpu-total-mb", type=int, default=None,
                        help="board total the hog figures scale against "
                             "(default: NVML's, for --hog-device)")
    parser.add_argument("--min-free-mb", type=int, default=4096,
                        help="floor under a scaled leave-free figure")
    parser.add_argument("--hog-device", type=int, default=0)
    parser.add_argument("--hog-port", type=int, default=6401)
    parser.add_argument("--seed-calibration",
                        help="calibration.toml copied into the fresh root "
                             "before the gateway starts")
    parser.add_argument("--job-cap", type=float, default=1800.0)
    parser.add_argument("--rescan-cap", type=float, default=900.0)
    parser.add_argument("--startup-cap", type=float, default=240.0)
    parser.add_argument("--settle", type=float, default=40.0,
                        help="seconds between the job and the final snapshots")
    parser.add_argument("--stop-grace", type=float, default=60.0)
    parser.add_argument("--vram-interval", type=float, default=0.25)
    parser.add_argument("--health-interval", type=float, default=0.5)
    parser.add_argument("--health-full", action="store_true",
                        help="healthrec.py --full (keeps the raw payload; "
                             "~2x the file, needed for inference_clients and "
                             "reserve_* )")
    parser.add_argument("--repo", default=str(HERE.parents[1]),
                        help="repository root; its .env is loaded into the "
                             "gateway's environment because --root chdirs "
                             "away from it")
    parser.add_argument("--no-dotenv", action="store_true",
                        help="do not load <repo>/.env")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.list:
        print_table()
        return 0
    if not args.scenario:
        parser.error("--scenario is required (or --list)")
    scenario = SCENARIOS.get(args.scenario)
    if scenario is None:
        parser.error(f"unknown scenario {args.scenario!r}; "
                     f"one of: {', '.join(SCENARIOS)}")
    if not args.bin and not args.dry_run:
        parser.error("--bin is required")

    config_toml, env_file = resolve_config(args)
    port = args.port or config_port(config_toml) or 6342
    base = f"http://127.0.0.1:{port}"
    model = args.model or scenario.model
    corpus = Path(args.corpus) if args.corpus else (
        Path(args.results) / "corpus" / scenario.corpus)
    total_mb = args.gpu_total_mb or board_total_mb(args.hog_device) \
        or REFERENCE_TOTAL_MB

    env = dict(os.environ)
    # The repo's own `.env` normally auto-loads from the CWD, but `--root`
    # chdirs away from it, so every `${PDFIUM_PATH:-}` / `${SAUCENAO_API_KEY}`
    # template in the config would fall back to empty. Loaded first so the
    # per-configuration env file wins, and never echoed: it holds API keys.
    dotenv = Path(args.repo).resolve() / ".env"
    if not args.no_dotenv:
        env.update(read_env_file(dotenv, env))
    env.update(read_env_file(env_file, env))
    env.setdefault("RUST_LOG", "info,panoptikon::inferio=trace")
    env.setdefault("INFERIO_WORKER_LOG_LEVEL", "DEBUG")

    if args.dry_run:
        directory = Path(args.results) / (args.run_id or "<run-id>") / scenario.key
    else:
        newrun = [args.python, str(HERE / "newrun.py"), "--scenario",
                  scenario.key, "--results", str(args.results)]
        if args.run_id:
            newrun += ["--run-id", args.run_id]
        newrun += ["--config", str(args.config), "--note",
                   args.note or scenario.note]
        result = subprocess.run(newrun, capture_output=True, text=True)
        if result.returncode != 0:
            raise SystemExit(f"legs.py: newrun.py failed:\n{result.stderr}")
        directory = Path(result.stdout.strip().splitlines()[-1])

    leg = Leg(args=args, scenario=scenario, directory=directory,
              python=args.python, config_toml=config_toml, env=env, base=base,
              total_mb=total_mb, supervisor=Supervisor(args.stop_grace))
    schedule, schedule_detail = leg.hog_schedule()
    events = leg.resolved_events()

    plan = {
        "schema": "legs/1",
        "scenario": scenario.key,
        "note": args.note or scenario.note,
        "directory": str(directory),
        "platform": {"system": platform.system(), "release": platform.release(),
                     "machine": platform.machine(),
                     "python": platform.python_version()},
        "bin": str(args.bin) if args.bin else None,
        "config": str(config_toml),
        "env_file": str(env_file) if env_file.is_file() else None,
        "dotenv": (None if args.no_dotenv
                   else str(dotenv) if dotenv.is_file() else None),
        "base_url": base,
        "legacy_port": args.legacy_port,
        "model": model,
        "corpus": str(corpus),
        "gpu_total_mb": total_mb,
        "gpu_total_mb_source": ("--gpu-total-mb" if args.gpu_total_mb
                                else "nvml" if board_total_mb(args.hog_device)
                                else "reference default"),
        "hog": {"schedule": schedule, **schedule_detail} if schedule else None,
        "hog_events": events,
        "checks": scenario.checks,
        "learning": scenario.learning,
        "preconditions": list(scenario.preconditions),
        "analyze_command": leg.analyze_command(),
    }

    if args.dry_run:
        print(json.dumps(plan, indent=1))
        return 0

    print(json.dumps(plan, indent=1), flush=True)
    for line in scenario.preconditions:
        print(f"PRECONDITION: {line}", flush=True)
    if not corpus.is_dir():
        raise SystemExit(
            f"legs.py: corpus {corpus} does not exist - generate it with "
            f"`corpus.py --tier {scenario.corpus} --out {corpus}`")

    root = leg.path("root")
    root.mkdir(parents=True, exist_ok=True)
    if args.seed_calibration:
        target = root / "data" / "inferio"
        target.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.seed_calibration, target / "calibration.toml")
        shutil.copyfile(args.seed_calibration,
                        leg.path("calibration.before.toml"))
        leg.mark("store_seeded", source=str(args.seed_calibration))

    fds: Optional[FdRecorder] = None
    outcome = "incomplete"
    try:
        # 1. the oracle, before anything of ours is on the GPU
        leg.supervisor.start("vramrec", [
            args.python, str(HERE / "vramrec.py"), "--out",
            str(leg.path("vramrec.jsonl")), "--interval",
            str(args.vram_interval), "--quiet"])
        leg.mark("vramrec_started")

        # 2. the hog, filled before the gateway sees the board
        if schedule:
            hog_argv = [args.python, str(HERE / "hog.py"), "--target", "gpu",
                        "--device", str(args.hog_device), "--port",
                        str(args.hog_port), "--out", str(leg.path("hog.jsonl")),
                        "--hold-at-end", "--quiet"]
            if scenario.hog_reeval is not None:
                hog_argv += ["--reeval", str(scenario.hog_reeval)]
            hog_argv += schedule
            leg.supervisor.start("hog", hog_argv)
            leg.mark("hog_started", schedule=schedule, detail=schedule_detail)
            if not wait_for(lambda: port_is_open("127.0.0.1", args.hog_port),
                            60.0):
                leg.mark("hog_control_never_answered")
            target = schedule_detail.get("mib", 0)
            if schedule_detail.get("kind") == "leave-free":
                filled = wait_for(
                    lambda: (get_json(leg.hog_url("/state"), timeout=5)
                             .get("free_mb") or 10 ** 9) <= target * 2, 180.0)
            elif target:
                filled = wait_for(
                    lambda: (get_json(leg.hog_url("/state"), timeout=5)
                             .get("held_mb") or 0) >= target - 256, 180.0)
            else:
                filled = True
            try:
                leg.mark("hog_filled" if filled else "hog_fill_timeout",
                         state=get_json(leg.hog_url("/state"), timeout=5))
            except HttpError as exc:
                leg.mark("hog_state_unreadable", error=str(exc))

        # 3. the gateway's own view, then the gateway
        health_argv = [args.python, str(HERE / "healthrec.py"), "--base", base,
                       "--out", str(leg.path("healthrec.jsonl")), "--interval",
                       str(args.health_interval), "--quiet"]
        if args.health_full:
            health_argv.append("--full")
        leg.supervisor.start("healthrec", health_argv)
        leg.mark("healthrec_started")

        gateway = leg.start_gateway()
        fds = FdRecorder(gateway.pid, leg.path("fds.jsonl"))
        fds.start()
        if fds.reader is None:
            leg.mark("fds_not_recorded",
                     reason="no /proc/<pid>/fd and no psutil; peak_fds will "
                            "SKIP")
        if not leg.wait_for_gateway():
            raise SystemExit("legs.py: the gateway never answered "
                             "/api/client-config; see gateway.out")
        save(f"{base}/api/inference/health", leg.path("health-t0.json"))

        # 4. corpus, database, rescan
        leg.prepare_database(corpus)

        # 5. the job, with the scenario's timed hog changes beside it
        posted_at = time.monotonic()
        driver: Optional[threading.Thread] = None
        if events and schedule:
            driver = threading.Thread(
                target=leg.drive_hog, args=(events, posted_at), daemon=True)
            driver.start()
        outcome = leg.run_job(model, "")
        if driver is not None:
            driver.join(timeout=max(60.0, max(e["at_s"] for e in events) + 60))

        # 6. S3's second half: the resume must come from the persisted anchor
        if scenario.restart:
            leg.snapshot_calibration("calibration.after-first.toml")
            save(f"{base}/api/inference/health",
                 leg.path("health-before-restart.json"))
            leg.mark("gateway_stopping_for_restart")
            leg.mark("gateway_stopped",
                     how=leg.supervisor.stop(gateway))
            if fds is not None:
                fds.stop()
            time.sleep(3.0)
            gateway = leg.start_gateway()
            fds = FdRecorder(gateway.pid, leg.path("fds.jsonl"))
            fds.start()
            if not leg.wait_for_gateway():
                raise SystemExit("legs.py: the gateway did not come back")
            save(f"{base}/api/inference/metadata",
                 leg.path("metadata-after-restart.json"))
            # A fresh index DB, so the second job has work: the first one
            # already extracted every item in this one.
            leg.mark("second_job_prepares_a_fresh_db")
            leg.prepare_database(corpus)
            outcome = leg.run_job(model, "-resume")

        if scenario.smoke_api:
            smoke = leg.smoke_assertions(corpus)
            leg.path("smoke.json").write_text(json.dumps(smoke, indent=1),
                                              encoding="utf-8")
            leg.mark("smoke_assertions", **{"summary": smoke})

        # 7. the tail
        save(f"{base}/api/inference/metadata", leg.path("metadata-after.json"))
        leg.mark("settling", seconds=args.settle)
        time.sleep(args.settle)
        save(f"{base}/api/inference/health", leg.path("health-end.json"))
        leg.snapshot_calibration("calibration.after.toml")
    except KeyboardInterrupt:
        outcome = "interrupted"
        leg.mark("interrupted")
    except SystemExit as exc:
        outcome = f"aborted: {exc}"
        leg.mark("aborted", error=str(exc))
    except Exception as exc:  # pragma: no cover - the leg reports its own fault
        outcome = f"error: {type(exc).__name__}: {exc}"
        leg.mark("error", error=str(exc))
    finally:
        if fds is not None:
            fds.stop()
        # The hog is asked to release over HTTP first: that is the only stop
        # whose completion is observable before the process goes away.
        if schedule:
            try:
                request(leg.hog_url("/stop"), method="POST", timeout=5)
                leg.mark("hog_stop_requested")
                time.sleep(3.0)
            except HttpError:
                pass
        stopped = leg.supervisor.stop_all()
        leg.mark("processes_stopped", **stopped)
        leg.copy_log()

    plan["outcome"] = outcome
    plan["events"] = leg.events
    plan["processes"] = {child.name: {"pid": child.pid,
                                      "returncode": child.popen.returncode}
                         for child in leg.supervisor.children}
    leg.path("legs.json").write_text(json.dumps(plan, indent=1),
                                     encoding="utf-8")
    print(f"\nDONE {directory}  outcome={outcome}")
    print("analyze with:\n  " + " ".join(leg.analyze_command()))
    return 0 if outcome == "drained" else 1


if __name__ == "__main__":
    raise SystemExit(main())
