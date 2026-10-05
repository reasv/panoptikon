#!/usr/bin/env python3
"""legs.py - run one platform-pass scenario end to end, on any OS.

A platform passes when S1, S2, S3, S4a-S4d, S5 and S14 pass, plus the
platform's own field-pass items. Those legs used to be bash drivers built out
of `curl`, `nohup`, `%1` job control, `kill -TERM`, `/proc` and shell process
substitution, none of which exists on Windows, and the Windows/WDDM pass is
the one that exercises the degraded base tier. This tool is those drivers, in
Python: stdlib only, `pathlib` for every path, `urllib` instead of
`curl`, `subprocess` with an explicit termination protocol instead of job
control, and no shell anywhere.

Usage
-----
    legs.py --scenario S2 --bin PATH --config C1 --results DIR \\
            [--run-id ID] [--gpu-total-mb 24564] [--python PATH] \\
            [--model ID] [--models a,b,c] [--scan-audio] [--corpus DIR] \\
            [--note "..."] [--port N] [--inference-url URL] \\
            [--seed-calibration FILE] [--job-cap S] [--settle S] \\
            [--hog-device N] [--hog-target gpu|mps|ram] [--min-free-mb 1024] \\
            [--hog-event at=S,leave_free=MIB|hold=MIB|release ...] \\
            [--list] [--dry-run]

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
   the `cal` databases, points the job config at the corpus (with
   `scan_audio` where `--scan-audio` asks for it), rescans.
5. Posts the extraction job - or one per `--models` entry, in order, in the
   same database - fires the scenario's timed hog events, waits for the queue
   to drain.
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
not a number of MiB, because a schedule written for a 97 887 MiB GPU says
nothing on a 24 564 MiB one: `leave-free 12288` is comfortable on the first
and more than the whole model plus corpus on the second. The rule is:

    mib = round(fraction x gpu_total_mb)

with `--gpu-total-mb` defaulting to the GPU total `vramrec.py` reports for
`--hog-device`. A `leave-free` figure is then floored at `--min-free-mb`
(default 1 024) so the model under test still fits on a small card, and a
`hold` figure is capped at `gpu_total_mb - --min-free-mb` for the same reason.
Whenever the floor or the cap actually binds, the leg writes a `floor_bound`
event into `legs.json` and prints a `PRECONDITION:` line: the leg is then
applying the floor's pressure, not the fraction's, and two legs written to
different fractions can land on the same level.

S4c's spike is not a fraction: it squeezes the GPU to about 2 GB free, so it
is 2 048 MiB on every GPU, not scaled, and raised only by a `--min-free-mb`
above it. A `--hog-event` figure is in MiB too: not scaled,
but bounded like every figure.
Both the fraction and the resolved MiB are recorded in `legs.json`, and the
reference column in `--list` is the figure this host's runs used, so a
cross-platform comparison can state what changed.

**On a unified-memory device there is no GPU total for NVML to report**, so a leg
with a hog refuses to start until `--gpu-total-mb` is given rather than
scaling against this host's 97 887 MiB reference. The figure to give is the
total the *worker adopts* -- `recommended_max_memory()`, which `selftest.py`
prints as `device.gpu_total_mb` and `/health` reports per replica -- and never
`hw.memsize`: the device is the GPU wired limit, not the machine's RAM, and
the two differ by a quarter on a stock Mac (98 304 of 131 072 MiB on the M3
Max under test). Give the same figure to `--hog-target mps`, whose pressure is
tensors on that device, or to `--hog-target ram`, whose pressure is numpy on
the RAM term of the same budget.

Descriptors
-----------
`fds.jsonl` is sampled here rather than by a shell loop, in the JSONL form
`analyze.py::read_fds` accepts (`{"iso", "fds", "sockets", "limit"}`). On
Linux the count comes from `/proc/<pid>/fd` and the limit from
`/proc/<pid>/limits`; elsewhere from `psutil` if it is importable, and
otherwise the file is simply not written and `peak_fds` SKIPs as before. This
matters on any leg whose `unit_budget` passes ~100: with local inference each
in-flight predict is loopback HTTP inside one process and costs **two**
sockets in one descriptor table.

Stopping a process, portably
----------------------------
POSIX: `SIGTERM` to the process, `SIGKILL` to its process group after
`--stop-grace` seconds. Windows: `CTRL_BREAK_EVENT` to the process group the
child was created in (`CREATE_NEW_PROCESS_GROUP`), then `TerminateProcess`.
The recorders handle `SIGBREAK` for exactly this reason, so a Windows
teardown flushes its last samples instead of losing them. `hog.py` is asked to
release over its own HTTP endpoint first, on every platform, because that is
the only stop that is observably complete before the process exits.
SIGTERM, SIGHUP (an ssh drop; not under nohup) or SIGBREAK sent to `legs.py`
itself ends the leg as Ctrl-C does: the same teardown, and `legs.json` with
the outcome `interrupted`.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import shutil
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

HERE = Path(__file__).resolve().parent
IS_WINDOWS = os.name == "nt"

sys.path.insert(0, str(HERE))
import corpus as corpus_tiers  # noqa: E402
import rocm_sysfs  # noqa: E402

# The GPU total every figure in SCENARIOS was measured against, so `--list` can
# print what this host actually ran beside the fraction.
REFERENCE_TOTAL_MB = 97887

#: `corpus.py::GENERATOR_VERSION` at the time this leg table was written. A
#: corpus stamped below it predates a tier composition change: an
#: S14-textembed leg on a `text` corpus generated before that tier carried
#: scanned pages drains in seconds, with zero items and every check PASS.
CORPUS_GENERATOR = 2

DEFAULT_MODEL = "tags/wd-vit-tagger-v3"
DEFAULT_DB = "cal"
#: The OCR that puts `extracted_text` rows in front of a derived text setter.
DEFAULT_OCR_MODEL = "doctr/db_resnet50_crnn_mobilenet_v3_small"
TEXTEMBED_MODEL = "textembed/all-MiniLM-L6-v2"


# --- the scenario table ----------------------------------------------------


@dataclass(frozen=True)
class HogEvent:
    """One timed change to the hog, `at_s` seconds after the job is posted."""

    at_s: float
    #: exactly one of these
    hold_fraction: Optional[float] = None
    leave_free_fraction: Optional[float] = None
    #: absolute levels, for a figure stated in MiB rather than as a share of
    #: the GPU's total: S4c squeezes to about 2 GB free, the same number on a
    #: 24 GB card as on a 96 GB one, and `--hog-event` is in MiB.
    leave_free_mb: Optional[int] = None
    hold_mb: Optional[int] = None
    label: str = ""
    #: a `--hog-event`: a leave-free level is solved once, at the event, and
    #: then held, on any scenario
    pinned: bool = False


def parse_hog_event(text: str) -> HogEvent:
    """`at=S,leave_free=MIB`, `at=S,hold=MIB` or `at=S,release`: a hog change
    S seconds after the job is posted, in MiB, not scaled. S and MIB are
    finite and not negative."""
    pairs = [part.partition("=")[::2] for part in text.split(",")]
    fields = dict(pairs)
    try:
        if len(fields) != len(pairs):
            raise ValueError("a key is repeated")
        at_s = float(fields.pop("at"))
        if fields == {"release": ""}:
            fields = {"hold": "0"}
        ((key, value),) = fields.items()
        mib = int(value)
        if math.isfinite(at_s) and at_s >= 0 and mib >= 0:
            if key == "leave_free":
                return HogEvent(at_s, leave_free_mb=mib, label=text,
                                pinned=True)
            if key == "hold":
                return HogEvent(at_s, hold_mb=mib, label=text, pinned=True)
    except (KeyError, ValueError):
        pass
    raise argparse.ArgumentTypeError(
        f"{text!r}: want at=S,leave_free=MIB, at=S,hold=MIB or at=S,release")


@dataclass(frozen=True)
class Scenario:
    key: str
    note: str
    #: `count`, `ramp`, `ramp8`, `smoke` ... resolved under `results/corpus/`
    corpus: str
    model: str = DEFAULT_MODEL
    #: the hog's opening schedule, as a fraction of the GPU's total
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
    #: a chain of extraction jobs in one database, in order, for a recipe a
    #: single `model` cannot express (`--models` overrides it)
    models: Tuple[str, ...] = ()
    #: what `analyze.py` should be asked for
    checks: str = "all"
    learning: bool = False
    expect: Tuple[str, ...] = ()
    #: what the scenario needs of the host before it starts
    preconditions: Tuple[str, ...] = ()
    #: healthrec's polling interval, unless `--health-interval` sets one
    health_interval: float = 0.5


@dataclass(frozen=True)
class Fixture:
    """What one S5 fault-injection fixture is designed to produce."""

    #: the `analyze.py --expect-*` flags its verdicts must be read against
    expect: Tuple[str, ...] = ()
    #: it never loads, so its setter records zero items by construction and
    #: the zero-item rule is not a finding here
    no_items: bool = False


#: The smoke tier's images, the only items the fixtures' handler accepts.
SMOKE_IMAGES = sum(group.count for group in corpus_tiers.tier_groups("smoke")
                   if group.kind == "image")

#: Keyed by the inference id with its `_cuda`/`_cpu` suffix stripped: the two
#: variants differ in whether the ledger prices them, not in what they
#: inject. The thresholds are per fixture, against the smoke tier; a leg on a
#: bigger corpus raises them by hand. A fixture that OOMs on every batch logs
#: at most one OOM negative per item, and `oom` runs no clean window, so its
#: deflation does not return to 0 within the leg's settle, and the model is
#: unloaded when its job ends. An OOM negative holds at least one predict that
#: OOMed, so `oom_timed` logs at most its registry `oom_predicts` (20).
S5_FIXTURES: Dict[str, Fixture] = {
    "oom_second_batch": Fixture(("--expect-ooms", "1")),
    "oom": Fixture(("--expect-ooms", str(SMOKE_IMAGES),
                    "--expect-failures", str(SMOKE_IMAGES),
                    "--expect-failed-jobs", "1", "--expect-deflated")),
    "oom_timed": Fixture(("--expect-ooms", "20",
                          "--expect-failures", str(SMOKE_IMAGES),
                          "--expect-failed-jobs", "1")),
    "failbatch": Fixture(),
    "failbatch_oomtext": Fixture(),
    "dying": Fixture(("--expect-deaths", "200",
                      "--expect-failures", str(SMOKE_IMAGES),
                      "--expect-failed-jobs", "1")),
    "dies_on_load": Fixture(("--expect-failed-jobs", "1",
                             "--expect-empty-setters"), no_items=True),
}

_FIXTURE_VARIANT = re.compile(r"_(cuda|cpu)$")


def fixture_for(model: str) -> Optional[Fixture]:
    """The S5 table's entry for an inference id, or None for a real model."""
    group, _, name = model.partition("/")
    if group != "calibfixture":
        return None
    return S5_FIXTURES.get(_FIXTURE_VARIANT.sub("", name))


SCENARIOS: Dict[str, Scenario] = {
    "S1": Scenario(
        key="S1",
        note="inventory, identity and a full GPU (the neighbour still resident)",
        corpus="smoke",
        checks="oracle_agreement,base_accuracy,footprint_agreement,"
               "grant_safety,failures,job_outcome,ledger_invariant,peak_fds",
        preconditions=(
            "the GPU's other tenant is STILL RUNNING - this is the only leg "
            "that wants a full GPU",
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
        note="constant external pressure: the hog holds the GPU down to a "
             "fixed free level for the whole job",
        corpus="ramp",
        hog_leave_free_fraction=12288 / REFERENCE_TOTAL_MB,
        hog_reeval=999999,
        checks="all",
        preconditions=(
            "the GPU is idle apart from the hog",
            "judge `utilization` against the idle-GPU probe: the check "
            "subtracts what the hog held",
        ),
    ),
    "S4b": Scenario(
        key="S4b",
        note="step up: the hog takes another ~31% of the GPU 60 s into the "
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
            "wall-clock 'few seconds'",
        ),
    ),
    "S4c": Scenario(
        key="S4c",
        note="spike: the hog squeezes the GPU to ~2 GB free for 10 s at "
             "t = 90 s, then releases",
        corpus="ramp8",
        hog_hold_fraction=0.0,
        events=(
            HogEvent(at_s=90.0, leave_free_mb=2048, label="spike"),
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
        note="step down: the hog starts holding the GPU and releases "
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
        # A fixture's job can last 0.3 s.
        health_interval=0.1,
        preconditions=(
            "run fixtures/install-fixtures.sh first, or point the gateway's "
            "config_dirs/impl_dirs at fixtures/registry and fixtures/impls",
            "record which classifier tier fired on every OOM "
            "(`oom_class.source`); on ROCm and MPS a missing message pattern "
            "means NO DEFLATION AT ALL",
        ),
    ),
    "S14": Scenario(
        key="S14",
        note="regression sanity: the CI smoke sequence plus one job",
        corpus="smoke",
        smoke_api=True,
        checks="failures,job_outcome,grant_safety,peak_fds",
    ),
    "S14-textembed": Scenario(
        key="S14-textembed",
        note="regression sanity for a derived text setter: OCR the text "
             "tier's scanned pages, then embed the rows the OCR wrote",
        corpus="text",
        models=(DEFAULT_OCR_MODEL, TEXTEMBED_MODEL),
        smoke_api=True,
        checks="failures,job_outcome,grant_safety,peak_fds",
        preconditions=(
            "`corpus.py --tier text` first: this leg needs that tier's "
            "SCANNED PAGES, not its .txt files. No file scan indexes a .txt "
            "on any platform and no model accepts text/plain, so a text "
            "model's only route is `extracted_text` rows another setter "
            "wrote - on the smoke tier the job is never queued",
        ),
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
    the evidence (a per-model failure reason, S14's 403)."""
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
        # A stop signal waits until the child is registered, so the teardown
        # stops it, and is then raised again.
        held: List[int] = []
        saved = {sig: signal.signal(sig, lambda signum, _: held.append(signum))
                 for sig in (signal.SIGINT, *STOP_SIGNALS)
                 if signal.getsignal(sig) is not signal.SIG_IGN}
        try:
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
        finally:
            for sig, handler in saved.items():
                signal.signal(sig, handler)
            for signum in held[:1]:
                signal.raise_signal(signum)
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
    the `limit=` column.
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

        # `Process.rlimit` is Linux-only; macOS answers None rather than
        # substituting *this* process's limit, which would be a wrong number
        # in the `limit=` column instead of an absent one.
        soft, _hard = psutil.Process(pid).rlimit(psutil.RLIMIT_NOFILE)  # type: ignore[attr-defined]
        return int(soft)
    except Exception:
        return None


class FdRecorder(threading.Thread):
    """`fds.jsonl` in the JSONL shape `analyze.py::read_fds` accepts."""

    def __init__(self, pid: int, path: Path, interval: float = 0.5) -> None:
        super().__init__(daemon=True)
        self.pid = pid
        self.reader = fd_reader(pid)
        self.limit = fd_limit(pid)
        self.path = path
        self.interval = interval
        # NOT `_stop`: `threading.Thread` uses that name for its own internal
        # method, and `Thread.join` calls it (`_wait_for_tstate_lock`) once the
        # thread has finished. Shadowing it with an Event makes the join
        # raise `TypeError: 'Event' object is not callable` at teardown --
        # which aborts the leg before any artefact is written and leaves the
        # gateway and the recorders running.
        self._stopped = threading.Event()

    def run(self) -> None:
        if self.reader is None:
            return
        with self.path.open("a", encoding="utf-8") as sink:
            while not self._stopped.is_set():
                try:
                    count, sockets = self.reader()
                except Exception:
                    break
                # Re-read every sample: the gateway raises its soft limit to
                # the hard one a few milliseconds after it starts
                # (`rlimit.rs`), so the one limit read at construction is the
                # pre-raise 1024 for the whole run and every `peak_fds`
                # percentage is ~1024x too large.
                self.limit = fd_limit(self.pid)
                sink.write(json.dumps({
                    "iso": iso_now(), "fds": count, "sockets": sockets,
                    "limit": self.limit,
                }) + "\n")
                sink.flush()
                self._stopped.wait(self.interval)

    def stop(self) -> None:
        self._stopped.set()
        # Joined, so no sample lands in the file after the gateway is gone.
        try:
            self.join(timeout=2)
        except RuntimeError:
            pass  # never started: nothing to join


# --- small helpers ---------------------------------------------------------


def iso_now() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


#: The signals that end a leg as Ctrl-C does, where the OS has them.
STOP_SIGNALS = [getattr(signal, name) for name in
                ("SIGTERM", "SIGHUP", "SIGBREAK") if hasattr(signal, name)]


def ignore_stop_signals() -> None:
    for sig in (signal.SIGINT, *STOP_SIGNALS):
        signal.signal(sig, signal.SIG_IGN)


def stop_on_signals() -> None:
    """SIGTERM, SIGHUP and SIGBREAK end the leg as Ctrl-C does, through its
    teardown: the children run in their own sessions, so a driver killed
    outright leaves them running. A signal already ignored (nohup) stays
    ignored. Once the teardown starts, stop signals are ignored."""
    def interrupt(signum: int, _frame: Any) -> None:
        ignore_stop_signals()
        raise KeyboardInterrupt(signal.Signals(signum).name)

    for sig in STOP_SIGNALS:
        if signal.getsignal(sig) is not signal.SIG_IGN:
            signal.signal(sig, interrupt)


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


#: How long the gateway's start waits for the recorders' first samples.
RECORDER_START_S = 30.0


def port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


#: What a leg masks its devices with. Printed with the plan (values, not just
#: names: these are not secrets) so a mask that never reached the gateway is
#: visible in `legs.json` instead of being inferred from where the job ran.
DEVICE_ENV = ("CUDA_VISIBLE_DEVICES", "HIP_VISIBLE_DEVICES",
              "ROCR_VISIBLE_DEVICES", "GPU_DEVICE_ORDINAL")

#: `group/id`, the only form `/api/jobs/data/extraction` accepts.
_INFERENCE_ID = re.compile(r"^[^\s/]+/[^\s]+$")

_ENV_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
_ENV_SUBST = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def read_env_file(path: Path, base: Dict[str, str]) -> Dict[str, str]:
    """`KEY=value` lines from an env file, `${VAR:-default}` aware.

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
        # `KEY=` is an assignment of the empty string, which is how a leg
        # masks every GPU (`CUDA_VISIBLE_DEVICES=`); it is never a line to
        # skip.
        name, raw = match.group(1), match.group(2).strip()
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
            raw = raw[1:-1]

        def expand(hit: "re.Match[str]") -> str:
            var, default = hit.group(1), hit.group(2)
            merged = {**base, **out}
            return merged.get(var, default if default is not None else "")

        out[name] = _ENV_SUBST.sub(expand, raw)
    return out


def corpus_tier_scale(name: str) -> Tuple[str, float]:
    """The `corpus.py` tier and scale a corpus DIRECTORY name asks for.

    A scenario names a directory convention, not a tier: `ramp8` is the
    `ramp` tier at `--scale 8`, which is how `results/corpus/ramp8` was
    generated and what the README calls it. `corpus.py` has no `ramp8` tier
    and never had one, so comparing the whole name against the manifest's
    `tier` would refuse S4b, S4c and S4d on every platform.
    """
    head = name.rstrip("0123456789")
    if head and head != name:
        return head, float(name[len(head):])
    return name, 1.0


def corpus_dir(args: argparse.Namespace, scenario: Scenario) -> Path:
    # Absolute, always: the gateway chdirs into `--root`, so a relative
    # `included_folders` entry resolves against a different directory there and
    # the rescan quietly indexes nothing.
    return (Path(args.corpus) if args.corpus
            else Path(args.results) / "corpus" / scenario.corpus).resolve()


def corpus_command(corpus: Path, tier: str, scale: float) -> str:
    return (f"corpus.py --tier {tier}"
            + (f" --scale {scale:g}" if scale != 1.0 else "")
            + f" --out {corpus} --force")


def corpus_complaint(corpus: Path, wanted: Optional[str]) -> Optional[str]:
    """Why this corpus cannot be used, or None.

    `wanted` is the scenario's corpus directory name, or None when `--corpus`
    named the directory: the operator chose that corpus on purpose (S5 on
    `poison`), so its own tier is the one this leg wants and only the stamp
    is checked.
    """
    tier, scale = corpus_tier_scale(
        wanted if wanted is not None else corpus.name)
    regenerate = f"generate it with `{corpus_command(corpus, tier, scale)}`"
    if not corpus.is_dir():
        return f"corpus {corpus} does not exist - {regenerate}"
    manifest = corpus / "manifest.json"
    if not manifest.is_file():
        return (f"corpus {corpus} has no manifest.json, so nothing says which "
                f"tier it is or when it was generated - {regenerate}")
    try:
        document = json.loads(manifest.read_text(encoding="utf-8"))
    except Exception as exc:
        return f"corpus {corpus}: manifest.json is unreadable ({exc}) - {regenerate}"
    found = document.get("tier")
    if wanted is None:
        # Whatever it holds is what was asked for; the remedy still has to
        # rebuild *this* corpus, so it names the tier the manifest stamped.
        command = corpus_command(corpus, str(found or tier), scale)
        regenerate = f"generate it with `{command}`"
    elif found != tier:
        return (f"corpus {corpus} is the {found!r} tier, this leg needs "
                f"{tier!r} - {regenerate}")
    generator = int(document.get("generator") or 0)
    if generator < CORPUS_GENERATOR:
        return (f"corpus {corpus} was generated by corpus.py version "
                f"{generator or 'unstamped'}, this leg needs "
                f"{CORPUS_GENERATOR} - {regenerate}")
    return None


#: Groups whose unit of work is an `extracted_text` row another setter wrote,
#: not a file: `textembed`'s work query is `files x item_data x
#: extracted_text`.
DERIVED_TEXT_GROUPS = ("textembed", "tclip")


def corpus_pages(corpus: Path) -> Optional[int]:
    """How many items of this corpus are scanned pages with words on them."""
    try:
        document = json.loads(
            (corpus / "manifest.json").read_text(encoding="utf-8"))
    except Exception:
        return None
    return sum(1 for item in document.get("items") or []
               if item.get("rendered_lines"))


def derived_text_complaint(models: List[str], corpus: Path) -> Optional[str]:
    """Why a derived text setter would find nothing in this corpus, or None.

    The S14 chain over `smoke` finds nothing: its 180 images are gradients,
    so `doctr` reads no words off them, writes no rows, and the `textembed`
    sub-job drains on 0 items. A `.txt` file is no route either; no file scan
    indexes one.
    """
    derived = [model for model in models
               if model.partition("/")[0] in DERIVED_TEXT_GROUPS]
    if not derived or corpus_pages(corpus):
        return None
    return (f"{', '.join(derived)} runs on `extracted_text` rows another "
            f"setter wrote, and corpus {corpus} carries no scanned page for "
            f"one to read: the job would drain on 0 items. Generate the tier "
            f"that has them with `corpus.py --tier text --out {corpus.parent}"
            f"/text` and run this leg on it")


def nvml_total_mb(device: int) -> Optional[int]:
    """The GPU's total, from NVML, for the hog scaling rule.

    None on a host with no NVML, which includes every Mac: the unified
    device's total is the worker's adopted recommended-max and only
    `--gpu-total-mb` can carry it here.
    """
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


def rocm_total_mb(device: int,
                  roots: rocm_sysfs.Roots = rocm_sysfs.Roots()) -> Optional[int]:
    """HIP device `device`'s total from amdgpu sysfs, as `/health` totals it
    (carve-out plus GTT on a unified GPU); None on a host with no KFD GPU."""
    gpu = next((gpu for gpu in rocm_sysfs.inventory(roots)
                if gpu.index == device), None)
    memory = None if gpu is None else rocm_sysfs.memory_mb(roots, gpu)
    return None if memory is None else memory[0]


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
    #: the index/user-data database the job API is pointed at; S3's second
    #: job gets its own, so the first job's extractions are not its work
    db: str = DEFAULT_DB
    events: List[Dict[str, Any]] = field(default_factory=list)
    processes: Dict[str, Any] = field(default_factory=dict)
    floor_notes: List[Dict[str, Any]] = field(default_factory=list)
    #: the config's extra `[[server.endpoints]]` listeners, `{"name","port"}`
    endpoints: List[Dict[str, Any]] = field(default_factory=list)
    #: the extraction chain this leg runs, after `--model` / `--models`
    models: Tuple[str, ...] = ()

    # -- recording ----------------------------------------------------------

    def mark(self, name: str, **detail: Any) -> None:
        # `t_mono` measures how far the wall clock steps (WSL2 steps it).
        record = {"iso": iso_now(), "t_mono": round(time.monotonic(), 3),
                  "event": name, **detail}
        self.events.append(record)
        try:
            print(f"[{record['iso']}] {name}"
                  + (f" {json.dumps(detail)}" if detail else ""), flush=True)
        except OSError:
            # a hung-up terminal or a closed pipe: the rest of the console
            # output is dropped, so the exit status stays the leg's
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())

    def path(self, name: str) -> Path:
        return self.directory / name

    def wait_for_recorders(self, health: Sequence[str],
                           timeout: float = RECORDER_START_S) -> None:
        """Waits up to `timeout` for vramrec's and each `health` recorder's
        recording to hold a sample; marks `recorder_sample_timeout` with those
        that still hold none."""
        paths = [self.path(f"{name}.jsonl") for name in ("vramrec", *health)]

        def sampled(path: Path) -> bool:
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                return False
            for line in lines:
                try:
                    if json.loads(line).get("kind") == "sample":
                        return True
                except ValueError:  # a partly written last line
                    pass
            return False

        wait_for(lambda: all(map(sampled, paths)), timeout, interval=0.1)
        missing = [path.name for path in paths if not sampled(path)]
        if missing:
            self.mark("recorder_sample_timeout", files=missing,
                      waited_s=timeout)

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

    def prepare_database(self, corpus: Path, db: Optional[str] = None,
                         tag: str = "") -> int:
        """Create a database, point it at `corpus`, rescan. Sticky: every
        later call on this leg (the job, the smoke assertions) uses it."""
        self.db = db or self.db
        db = self.db
        save(f"{self.base}/api/db/create?new_index_db={db}&new_user_data_db={db}",
             self.path(f"dbcreate{tag}.json"), method="POST")
        config = get_json(f"{self.base}/api/jobs/config?index_db={db}")
        config["included_folders"] = [str(corpus)]
        if self.args.scan_audio:
            # whisper's items only exist when the scan attaches audio.
            config["scan_audio"] = True
        self.path(f"config-set{tag}.json").write_text(json.dumps(config),
                                                      encoding="utf-8")
        save(f"{self.base}/api/jobs/config?index_db={db}",
             self.path(f"config-put{tag}.json"), method="PUT",
             body=json.dumps(config).encode("utf-8"),
             content_type="application/json")
        self.mark("rescan_start", corpus=str(corpus), index_db=db,
                  scan_audio=bool(self.args.scan_audio))
        save(f"{self.base}/api/jobs/folders/rescan?index_db={db}",
             self.path(f"rescan{tag}.json"), method="POST")
        self.wait_for_queue(self.args.rescan_cap)
        save(f"{self.base}/api/jobs/folders/history?index_db={db}"
             f"&page=1&page_size=5", self.path(f"folders{tag}.json"))
        indexed = self.indexed_files(tag)
        self.mark("rescan_done", indexed_files=indexed, index_db=db)
        if not indexed:
            # A leg whose corpus indexed nothing still "completes" its job in
            # two seconds and every check passes on no data at all, which is
            # the most expensive way to learn that a path was wrong.
            self.mark("rescan_indexed_nothing",
                      corpus=str(corpus),
                      hint="check the corpus path and the config's "
                           "included_folders; a relative path resolves "
                           "against the gateway's --root")
            raise SystemExit(f"legs.py: the rescan of {corpus} indexed no "
                             f"file, so this leg has no work and nothing it "
                             f"measures would mean anything")
        return indexed

    def indexed_files(self, tag: str = "") -> int:
        """How many files the last rescan actually attached, or 0."""
        try:
            history = json.loads(self.path(f"folders{tag}.json")
                                 .read_text(encoding="utf-8"))
        except Exception:
            return 0
        rows = history if isinstance(history, list) else history.get("history")
        if not rows:
            return 0
        # Newest first, and `total_available` is what the scan found on disk -
        # a re-scan of an already-indexed corpus reports it under
        # `unchanged_files`, so summing the two halves is the fallback.
        row = rows[0]
        total = row.get("total_available")
        if total is not None:
            return int(total)
        return int(row.get("new_items") or 0) + int(row.get("unchanged_files")
                                                    or 0)

    def run_job(self, model: str, tag: str) -> str:
        db = self.db
        self.mark("job_start", model=model, tag=tag, index_db=db)
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
        items = self.job_items(model, tag)
        if outcome == "drained" and not items and not self.expects_no_items():
            # The queue draining is not the result: a setter with no item to
            # work on drains in seconds and every analyze.py check passes on
            # no data at all.
            self.mark("job_no_items", model=model, index_db=db,
                      record="none" if items is None else "0 items",
                      hint="the model found nothing to run on - a derived "
                           "text setter needs extracted_text rows another "
                           "setter wrote first, and every setter needs a "
                           "corpus its handler accepts")
            return "no_items"
        self.mark("job_items", model=model, items=items)
        return outcome

    def expects_no_items(self) -> bool:
        """Is a job with no items this leg's whole point?

        `calibfixture/dies_on_load_cuda` never becomes resident, so its
        setter records zero items by construction, and the rule that catches
        a stale corpus must not mark the leg `no_items` for it."""
        return any((fixture_for(model) or Fixture()).no_items
                   for model in self.models)

    def expectations(self) -> Tuple[str, ...]:
        """`analyze.py --expect-*` for this leg: the fixture's own, where it
        runs one, a failed-item ceiling on a `poison` corpus, and the
        scenario's otherwise."""
        for model in self.models:
            fixture = fixture_for(model)
            if fixture is not None:
                return fixture.expect
        try:
            manifest = corpus_dir(self.args, self.scenario) / "manifest.json"
            tier = json.loads(manifest.read_text(encoding="utf-8")).get("tier")
        except Exception:  # no corpus, or no readable manifest
            tier = None
        if tier == "poison":
            # poison's items are built to fail as input.
            return ("--expect-failures", str(sum(
                group.count for group in corpus_tiers.tier_groups("poison"))))
        return self.scenario.expect

    def job_items(self, model: str, tag: str) -> Optional[int]:
        """`total_segments` of this setter's newest job record, or None when
        the history has no record for it at all."""
        try:
            payload = json.loads(self.path(f"jobs{tag}.json")
                                 .read_text(encoding="utf-8"))
        except Exception:
            return None
        rows = payload if isinstance(payload, list) else payload.get("history")
        for row in rows or []:
            if isinstance(row, dict) and row.get("setter") == model:
                return int(row.get("total_segments") or 0)
        return None

    # -- the hog ------------------------------------------------------------

    def hog_url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.args.hog_port}{path}"

    def hog_schedule(self) -> Tuple[List[str], Dict[str, Any]]:
        """The opening schedule in MiB, plus what it was derived from."""
        scenario = self.scenario
        detail: Dict[str, Any] = {"gpu_total_mb": self.total_mb,
                                  "min_free_mb": self.args.min_free_mb}
        if scenario.hog_leave_free_fraction is not None:
            scaled = scale_mb(scenario.hog_leave_free_fraction, self.total_mb)
            mib = max(self.args.min_free_mb, scaled)
            self.note_floor("leave-free", "opening",
                            scenario.hog_leave_free_fraction, scaled, mib)
            detail.update({"kind": "leave-free",
                           "fraction": scenario.hog_leave_free_fraction,
                           "scaled_mib": scaled,
                           "mib": mib,
                           "reference_mib": scale_mb(
                               scenario.hog_leave_free_fraction,
                               REFERENCE_TOTAL_MB)})
            return ["leave-free", str(mib)], detail
        if scenario.hog_hold_fraction is not None:
            scaled = scale_mb(scenario.hog_hold_fraction, self.total_mb)
            mib = min(scaled, max(0, self.total_mb - self.args.min_free_mb))
            self.note_floor("hold", "opening", scenario.hog_hold_fraction,
                            scaled, mib)
            detail.update({"kind": "hold",
                           "fraction": scenario.hog_hold_fraction,
                           "scaled_mib": scaled,
                           "mib": mib,
                           "reference_mib": scale_mb(
                               scenario.hog_hold_fraction,
                               REFERENCE_TOTAL_MB)})
            return ["hold", str(mib)], detail
        return [], {}

    def note_floor(self, kind: str, at: str, fraction: Optional[float],
                   scaled_mb: int, resolved_mb: int) -> None:
        """Record a figure `--min-free-mb` moved off its stated value.

        A bound floor makes two legs written to different fractions apply the
        same pressure, so the leg says so instead of letting a reader compare
        them as if the schedule had held.
        """
        if resolved_mb == scaled_mb:
            return
        self.floor_notes.append({
            "kind": kind, "at": at, "fraction": fraction,
            "min_free_mb": self.args.min_free_mb, "gpu_total_mb": self.total_mb,
            "scaled_mb": scaled_mb, "resolved_mb": resolved_mb,
        })

    def resolved_events(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for event in self.scenario.events:
            at = event.label or f"t+{event.at_s:g}s"
            row: Dict[str, Any] = {"at_s": event.at_s, "label": event.label,
                                   "pinned": event.pinned}
            if (event.leave_free_mb is not None
                    or event.leave_free_fraction is not None):
                fraction = event.leave_free_fraction
                figure = (event.leave_free_mb if fraction is None
                          else scale_mb(fraction, self.total_mb))
                row["leave_free_mb"] = max(self.args.min_free_mb, figure)
                row["fraction"] = fraction
                self.note_floor("leave-free", at, fraction, figure,
                                row["leave_free_mb"])
            else:
                fraction = (None if event.hold_mb is not None
                            else event.hold_fraction or 0.0)
                figure = (event.hold_mb if fraction is None
                          else scale_mb(fraction, self.total_mb))
                row["mb"] = min(figure,
                                max(0, self.total_mb - self.args.min_free_mb))
                row["fraction"] = fraction
                self.note_floor("hold", at, fraction, figure, row["mb"])
            out.append(row)
        return out

    def drive_hog(self, events: List[Dict[str, Any]], posted_at: float) -> None:
        """Fire the scenario's timed hog changes, on a thread, from job-post.

        Timed from the job's POST rather than from the leg's start, because
        what the scenario is describing is a change *during* the job: the S4b
        step lands 60.0 s after the submit, not 60 s after the recorders came
        up.
        """
        for event in events:
            delay = posted_at + event["at_s"] - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            query = ("leave_free=%d" % event["leave_free_mb"]
                     if "leave_free_mb" in event else "mb=%d" % event["mb"])
            self.mark("hog_event_request", label=event["label"], query=query,
                      at_s=event["at_s"], pinned=event["pinned"])
            try:
                request(self.hog_url(f"/set?{query}"
                                     + ("&pin=1" if event["pinned"] else "")),
                        method="POST", timeout=10)
            except HttpError as exc:
                self.mark("hog_event_failed", label=event["label"],
                          error=str(exc))
                continue
            self.mark("hog_event_ack", label=event["label"])
            # Record the fill, so `legs.json` states how long the GPU took
            # to change.
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
        # S3 starts a second one, and both accountings key on the name, so
        # the restarted gateway needs its own or the first one's row
        # overwrites it.
        started = sum(1 for child in self.supervisor.children
                      if child.name.startswith("gateway"))
        # `child=`, not `name=`: `mark`'s first parameter is called `name`.
        label = "gateway" if not started else f"gateway-{started + 1}"
        child = self.supervisor.start(label, argv,
                                      log_path=self.path("gateway.out"),
                                      env=self.env)
        self.mark("gateway_started", child=label, pid=child.pid, argv=argv)
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

    def smoke_assertions(self) -> Dict[str, Any]:
        """The CI smoke sequence (`.github/workflows/release.yml`), as data.

        Every call records its status rather than asserting, because what a
        platform pass wants is the whole row - a 403 that came back 200 and a
        thumbnail that came back empty are different faults and both should
        survive into `legs.json`.
        """
        db = self.db
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
        served = None
        if status == 200:
            try:
                results = json.loads(payload).get("results") or []
                out["pql"]["count"] = json.loads(payload).get("count")
                sha = results[0].get("sha256") if results else None
                served = results[0].get("path") if results else None
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
        # separator, which is why it is worth asserting off Linux at all. The
        # path comes from the search result, not from the corpus directory: a
        # file on disk that this leg's scan never indexed (audio without
        # `--scan-audio`) is a 404 by construction, not a fault.
        if served:
            params = urllib.parse.urlencode({"id": served,
                                             "id_type": "path",
                                             "index_db": db})
            status = save(f"{self.base}/api/items/item/file?{params}",
                          self.path("file.bin"))
            out["file"] = {"status": status, "path": served,
                           "bytes": self.path("file.bin").stat().st_size
                           if self.path("file.bin").is_file() else 0}
        else:
            out["file"] = {"skipped": "no path from the PQL search"}
        out["endpoints"] = self.endpoint_assertions()
        # Kept under its old key as well: earlier recordings and the CI recipe
        # both name `legacy_ui_queue`, and a platform comparison reads them.
        legacy = next((row for row in out["endpoints"]
                       if row["name"] == "legacy_ui"), None)
        if legacy is not None:
            out["legacy_ui_queue"] = legacy
        return out

    def endpoint_assertions(self) -> List[Dict[str, Any]]:
        """Every extra listener answers `/api/jobs/queue`, and with a 200.

        The same gateway serves the same routes on each `[[server.endpoints]]`
        port; only the policy matched by name differs. On these configs both
        the `test` (6343) and `legacy_ui` (6339) listeners are the localhost
        policy, so **200 is the pass** -- the 403 belongs to Docker alone,
        where `docker.toml` gives the second port the public endpoint with
        `restricted_demo`. A connection failure is a fault, not a blank.
        """
        rows: List[Dict[str, Any]] = []
        wanted = list(self.endpoints)
        if self.args.legacy_port and not any(
                row["port"] == self.args.legacy_port for row in wanted):
            wanted.append({"name": "legacy_ui", "port": self.args.legacy_port})
        for entry in wanted:
            row: Dict[str, Any] = {"name": entry["name"], "port": entry["port"],
                                   "expected": 200,
                                   "expected_under_docker": 403}
            url = f"http://127.0.0.1:{entry['port']}/api/jobs/queue"
            try:
                row["status"] = request(url, timeout=10)[0]
            except HttpError as exc:
                row["error"] = str(exc)
            row["ok"] = row.get("status") == 200
            row["note"] = ("the 403 is a Docker-only assertion: docker.toml "
                           "gives this port the public endpoint with "
                           "restricted_demo, while these configs give it the "
                           "localhost policy, where 200 is correct")
            rows.append(row)
        return rows

    # -- the whole leg ------------------------------------------------------

    def analyze_command(self) -> List[str]:
        argv = [self.python, str(HERE / "analyze.py"),
                "--scenario", str(self.directory),
                "--checks", self.scenario.checks]
        if self.scenario.learning:
            argv.append("--learning")
        argv += list(self.expectations())
        argv += ["--json", str(self.path("verdicts.json"))]
        return argv


#: The gateway configurations `--config` can name by id. Each is the tree's
#: shipped `config/server/default.toml` with the lines `render_config` changes,
#: started with the environment `config_env` returns.
#:   port_offset  added to every listener and upstream port, so configurations
#:                can run side by side
#:   tree         the checkout whose binary, venv and inference sources are
#:                used, relative to `--repo`
#:   registry     an extra `config_dirs` entry, a directory under `config/`
#:   env          extra variables for the gateway's environment
#:   accelerator  `[inference_local.python_env] accelerator`; `rocm` also drops
#:                the cuDNN loader path and adds `batch_auto` debug lines
CONFIGS: Dict[str, Dict[str, Any]] = {
    # the branch under test, both GPUs visible
    "C1": {},
    # the "before" baseline: the master checkout beside this one
    "C0": {"port_offset": 10, "tree": "../panoptikon-master"},
    # one GPU named by UUID: the inventory narrows to it and stays priced
    "C2": {"port_offset": 20,
           "env": {"CUDA_VISIBLE_DEVICES":
                   "GPU-01c61d5b-6b4c-bd6a-019b-150586096a47"}},
    # one GPU named by index: the inventory is blank until the first load
    # report names the GPU by UUID
    "C3": {"port_offset": 30, "env": {"CUDA_VISIBLE_DEVICES": "1"}},
    # a user registry: MobileCLIP-S1 pinned to GPU 1, batched easyOCR
    "C7": {"port_offset": 40, "registry": "registry-C7"},
    # C7 with the easyOCR canvas raised past every input
    "C7nc": {"port_offset": 50, "registry": "registry-C7nc"},
    # C1 on ROCm. The accelerator is named because with `python` set, `auto`
    # finds ROCm only through /opt/rocm or rocm-smi, and wheel-bundled ROCm
    # has neither
    "R1": {"port_offset": 60, "accelerator": "rocm"},
    # R7 under an ambient HIP-layer restriction to device 1: the inventory
    # stays unknown, every model runs unpriced and the pin is dropped
    "R2": {"port_offset": 70, "accelerator": "rocm", "registry": "registry-R7",
           "env": {"HIP_VISIBLE_DEVICES": "1"}},
    # the same at the ROCr layer, which keeps the pin: device 1 is the only
    # one left, so the pin names it as 0
    "R3": {"port_offset": 80, "accelerator": "rocm", "registry": "registry-R3",
           "env": {"ROCR_VISIBLE_DEVICES": "1"}},
    # R1 with MobileCLIP-S1 pinned to HIP device 1
    "R7": {"port_offset": 90, "accelerator": "rocm", "registry": "registry-R7"},
}


def config_tree(name: str, repo: Path) -> Path:
    return (repo / CONFIGS[name].get("tree", ".")).resolve()


def venv_python(venv: Path) -> Path:
    """The interpreter of virtualenv `venv` on this OS."""
    if IS_WINDOWS:
        return venv / "Scripts" / "python.exe"
    return venv / "bin" / "python"


def cudnn_library_dir(python: Path) -> Optional[Path]:
    """The `nvidia/cudnn/lib` directory in the venv of interpreter `python`,
    or None when that venv has none."""
    found = sorted(python.parent.parent.glob(
        "lib/python3*/site-packages/nvidia/cudnn/lib"))
    return found[-1] if found else None


_TOML_BASE_URL = re.compile(r'^(\s*base_url\s*=\s*"[^"]*:)(\d+)(.*)$')


def render_config(name: str, repo: Path) -> str:
    """The server config for configuration `name`.

    Every deviation from the shipped file is one of three: the ports, the UI
    upstream off (the legs are API-only, and a `next build` would compete with
    every throughput measurement), and absolute inference paths. The paths
    are forced by `--root`, which the gateway implements as a chdir, so the
    shipped relative defaults would resolve under the results directory.
    Setting `python` also skips the startup auto-setup, which would re-sync
    the venv without its `test` group.
    """
    spec = CONFIGS[name]
    tree = config_tree(name, repo)
    shipped = tree / "config" / "server" / "default.toml"
    if not shipped.is_file():
        raise SystemExit(f"legs.py: {name} is built from {shipped}, which does "
                         f"not exist - pass --repo <checkout>")
    text = shipped.read_text(encoding="utf-8")
    offset = int(spec.get("port_offset", 0))
    text = repin_ports(text, offset)
    out: List[str] = []
    section = ""
    for line in text.splitlines():
        header = _TOML_SECTION.match(line)
        if header:
            section = header.group(1).strip().strip("[]")
        elif section.startswith("upstreams."):
            hit = _TOML_BASE_URL.match(line)
            if hit:
                line = (f"{hit.group(1)}{int(hit.group(2)) + offset}"
                        f"{hit.group(3)}")
        out.append(line)
    text = "\n".join(out) + "\n"
    text = set_toml_key(text, "upstreams.ui", "local", "false")

    def paths(*parts: Path) -> str:
        return "[" + ", ".join(json.dumps(str(part)) for part in parts) + "]"

    config_dirs = [tree / "python" / "inferio" / "config",
                   tree / "config" / "inference"]
    if spec.get("registry"):
        config_dirs.append(tree / "tools" / "calibration-protocol" / "config"
                           / spec["registry"])
    for key, value in (
            ("python", json.dumps(str(venv_python(tree / "python" / ".venv")))),
            ("impl_dirs", paths(tree / "python" / "inferio" / "impl",
                                tree / "inferio_custom")),
            ("config_dirs", paths(*config_dirs)),
            ("pythonpath", paths(tree / "python"))):
        text = set_toml_key(text, "inference_local", key, value)
    if spec.get("accelerator"):
        text = set_toml_key(text, "inference_local.python_env", "accelerator",
                            json.dumps(spec["accelerator"]))
    return text


def config_env(name: str, repo: Path, base: Dict[str, str],
               python: Optional[str] = None) -> Dict[str, str]:
    """The gateway's environment for configuration `name`, over `base`.

    The trace directive carries the ledger's grant/settle/refit lines, and the
    worker's DEBUG level its batch plans. `LD_LIBRARY_PATH` names the cuDNN
    in the worker's venv (`python`, else the tree's) because CTranslate2
    (faster-whisper) dlopens `libcudnn_ops.so.9` and that directory is not on
    the loader path; torch finds its own copy. It is left out when that venv
    has no cuDNN.
    """
    tree = config_tree(name, repo)
    cudnn = cudnn_library_dir(Path(shutil.which(python) or python) if python
                              else venv_python(tree / "python" / ".venv"))
    # A configuration that names its own tree (C0, the master baseline) runs
    # that tree's binary whatever the caller exported; the others share the
    # checkout, so a caller's PANOPTIKON_BIN only picks which build of it.
    caller_bin = None if "tree" in CONFIGS[name] else base.get("PANOPTIKON_BIN")
    env = {
        "PANOPTIKON_TREE": str(tree),
        "PANOPTIKON_BIN": (caller_bin
                           or str(tree / "target" / "release" / "panoptikon")),
        "RUST_LOG": "info,panoptikon::inferio=trace",
        "INFERIO_WORKER_LOG_LEVEL": "DEBUG",
    }
    if CONFIGS[name].get("accelerator") == "rocm":
        env["RUST_LOG"] += ",panoptikon::db::batch_auto=debug"
    elif cudnn is not None:
        env["LD_LIBRARY_PATH"] = str(cudnn)
    return {**env, **CONFIGS[name].get("env", {})}


def resolve_config(args: argparse.Namespace, base: Dict[str, str],
                   python: Optional[str] = None
                   ) -> Tuple[str, str, Dict[str, str], str]:
    """`--config` as a configuration id or a path to a TOML.

    Returns (config text, file name, environment, where the environment came
    from). A path's environment is the `env.<id>` file beside it, if any. An
    id's paths follow `--repo`, so its venv must exist there unless
    `--python` (`python`) replaces it or no worker is started (`--dry-run`,
    `--inference-url`).
    """
    given = str(args.config)
    candidate = Path(given)
    if candidate.is_file():
        # Absolute for the same reason the corpus is: the gateway chdirs into
        # `--root` and would resolve a relative `--config` against it.
        candidate = candidate.resolve()
        env_file = (candidate.parent
                    / f"env.{candidate.stem.replace('server-', '')}")
        env = read_env_file(env_file, base)
        if python:
            env.pop("LD_LIBRARY_PATH", None)
            cudnn = cudnn_library_dir(Path(shutil.which(python) or python))
            if cudnn is not None:
                env["LD_LIBRARY_PATH"] = str(cudnn)
        return (candidate.read_text(encoding="utf-8"), str(candidate), env,
                str(env_file) if env_file.is_file() else "")
    if given not in CONFIGS:
        raise SystemExit(f"legs.py: no config {given!r} - pass a path, or one "
                         f"of {', '.join(CONFIGS)}")
    repo = Path(args.repo).resolve()
    text = render_config(given, repo)
    venv = venv_python(config_tree(given, repo) / "python" / ".venv")
    if (not venv.exists() and not python and not args.dry_run
            and not args.inference_url):
        raise SystemExit(f"legs.py: {given} runs the worker on {venv}, which "
                         f"does not exist - pass --repo <checkout with a "
                         f"synced venv> or --python")
    return (text, f"server-{given}.toml", config_env(given, repo, base, python),
            f"CONFIGS[{given!r}]")


_TOML_SECTION = re.compile(r"^\s*\[([^\]]+)\]")


def refuse_inherited_visibility(text: str, config_vars: Dict[str, str],
                                inherited: Dict[str, str]) -> None:
    """Stop a ROCm config that would start under a GPU visibility variable it
    does not set itself: the gateway then leaves every GPU unpriced, and the
    tools' device indices stop naming torch's devices."""
    try:
        import tomllib

        python_env = (tomllib.loads(text).get("inference_local", {})
                      .get("python_env", {}))
    except Exception:
        return
    stray = [name for name in DEVICE_ENV
             if name in inherited and name not in config_vars]
    if python_env.get("accelerator") == "rocm" and stray:
        raise SystemExit(
            f"legs.py: {', '.join(stray)} is set in this environment and the "
            f"ROCm config does not set it; unset it, or name it in the "
            f"config's own environment")


def set_toml_key(text: str, table: str, key: str, value: str) -> str:
    """`key = value` in `[table]`, replacing the key's line or adding one.

    `value` is a TOML literal. The table is created at the end when absent.
    """
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
    line_out = f"{key} = {value}"
    out: List[str] = []
    section = ""
    done = False
    for line in text.splitlines():
        header = _TOML_SECTION.match(line)
        if header:
            if section == table and not done:
                out.append(line_out)
                done = True
            section = header.group(1).strip().strip("[]")
        elif section == table and not done and pattern.match(line):
            out.append(line_out)
            done = True
            continue
        out.append(line)
    if not done:
        if section != table:
            out.append("")
            out.append(f"[{table}]")
        out.append(line_out)
    return "\n".join(out) + "\n"


def repin_inference_python(text: str, python: str) -> str:
    """`[inference_local] python = <python>`, in a copy of the config.

    `--python` has to reach the *worker*, not only the recorders: a config
    that pins the interpreter otherwise silently wins, and a CPU-only leg
    runs on the GPU venv the config names.
    """
    return set_toml_key(text, "inference_local", "python", json.dumps(python))


_TOML_PORT = re.compile(r"^(\s*port\s*=\s*)(\d+)(.*)$")


def repin_ports(text: str, offset: int) -> str:
    """Move every listener the config declares by `offset`, in a copy.

    `--port` has to move what the gateway binds, not only the URL the leg
    polls. The extra `[[server.endpoints]]` listeners move with the primary
    one, so a configuration's set stays disjoint from another's.
    """
    out: List[str] = []
    section = ""
    for line in text.splitlines():
        header = _TOML_SECTION.match(line)
        if header:
            section = header.group(1).strip().strip("[]")
        elif section in ("server", "server.endpoints"):
            hit = _TOML_PORT.match(line)
            if hit:
                line = (f"{hit.group(1)}{int(hit.group(2)) + offset}"
                        f"{hit.group(3)}")
        out.append(line)
    return "\n".join(out) + "\n"


def remote_inference(text: str, url: str) -> str:
    """`url` as the config's one `[[upstreams.inference]]` server, with
    `[inference_local]` off: the gateway forwards every inference request
    there."""
    out: List[str] = []
    section = ""
    for line in text.splitlines():
        header = _TOML_SECTION.match(line)
        if header:
            section = header.group(1).strip().strip("[]")
        if section != "upstreams.inference":
            out.append(line)
    text = set_toml_key("\n".join(out) + "\n", "inference_local", "enabled",
                        "false")
    return text + f"\n[[upstreams.inference]]\nbase_url = {json.dumps(url)}\n"


def leg_config(text: str, python: Optional[str], port: Optional[int],
               inference_url: Optional[str]) -> str:
    """The config the gateway runs: `python` as the worker's interpreter,
    every listener moved so the gateway binds `port`, and `inference_url` as
    its inference server. `--write-config` writes the same text."""
    if python:
        text = repin_inference_python(text, python)
    if port:
        text = repin_ports(text, port - (config_port(text) or 6342))
    if inference_url:
        text = remote_inference(text, inference_url)
    return text


def config_inference_python(text: str) -> Optional[str]:
    """`[inference_local] python`, or None when the config leaves it to the
    gateway's own managed venv."""
    try:
        import tomllib

        document = tomllib.loads(text)
        value = document.get("inference_local", {}).get("python")
    except Exception:
        return None
    return str(value) if value else None


def config_port(text: str, key: str = "port") -> Optional[int]:
    try:
        import tomllib

        document = tomllib.loads(text)
        return int(document.get("server", {}).get(key))
    except Exception:
        return None


def config_endpoints(toml: Path) -> List[Dict[str, Any]]:
    """The extra `[[server.endpoints]]` listeners, as `{"name", "port"}`.

    The same gateway serving the same routes on another port, matched to a
    different policy by name (`legacy_ui` on 6339 keeps old bookmarks alive,
    `test` on 6343 is pinned to the stdtest DBs). They are configuration, not
    code, so a platform pass has to read them from the config it is running
    rather than hard-coding a number.
    """
    try:
        return endpoints_in(toml.read_text(encoding="utf-8"))
    except OSError:
        return []


def endpoints_in(text: str) -> List[Dict[str, Any]]:
    try:
        import tomllib

        document = tomllib.loads(text)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry in document.get("server", {}).get("endpoints") or []:
        if not isinstance(entry, dict):
            continue
        try:
            port = int(entry["port"])
        except (KeyError, TypeError, ValueError):
            continue
        out.append({"name": str(entry.get("name") or "?"), "port": port})
    return out


def print_table() -> None:
    print(f"{'scenario':<14} {'corpus':<7} {'hog (fraction -> this host)':<33} "
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
        print(f"{scenario.key:<14} {scenario.corpus:<7} {hog:<33} {events}")
        print(f"        {scenario.note}")
        if scenario.models:
            print(f"        chain: {' -> '.join(scenario.models)}")
        for line in scenario.preconditions:
            print(f"        ! {line}")
    print(f"\nThe hog fractions are of the GPU's total; the MiB column is "
          f"this host's reference GPU ({REFERENCE_TOTAL_MB} MiB). "
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
                        help="a configuration id (" + ", ".join(CONFIGS)
                             + ") or a path to a server TOML")
    parser.add_argument("--write-config", metavar="DIR", default=None,
                        help="write the --config's server TOML and env file "
                             "into DIR and exit")
    parser.add_argument("--results", default=str(HERE / "results"),
                        help="results root (newrun.py's --results)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--note", default=None)
    parser.add_argument("--python", default=None,
                        help="interpreter for the recorder subprocesses, and "
                             "for the gateway's [inference_local] python, "
                             "which it is written over in a per-leg copy of "
                             "the config (default: this interpreter, and the "
                             "config's own value for the worker)")
    parser.add_argument("--models", default=None,
                        help="a,b,c: a chain of extraction jobs in one "
                             "database, run in the order given (the derived "
                             "setters need their source extracted first)")
    parser.add_argument("--scan-audio", action="store_true",
                        help="set the job config's scan_audio before the "
                             "rescan, so whisper has items")
    parser.add_argument("--model", default=None,
                        help="override the scenario's inference id")
    parser.add_argument("--corpus", default=None,
                        help="override the corpus directory")
    parser.add_argument("--port", type=int, default=None,
                        help="gateway port: every listener the config "
                             "declares moves with it, in the per-leg copy "
                             "(default: read from the config)")
    parser.add_argument("--inference-url", default=None,
                        help="an inference server on another host: the "
                             "gateway forwards inference to it instead of "
                             "running its own, and a second healthrec polls "
                             "its /health into healthrec-remote.jsonl")
    parser.add_argument("--legacy-port", type=int, default=None,
                        help="an extra listener to probe on top of the ones "
                             "the config declares (S14 probes every "
                             "[[server.endpoints]] port and expects 200)")
    parser.add_argument("--gpu-total-mb", type=int, default=None,
                        help="GPU total the hog figures scale against "
                             "(default: NVML's, or amdgpu sysfs', for "
                             "--hog-device)")
    parser.add_argument("--min-free-mb", type=int, default=1024,
                        help="floor under a leave-free figure, and what "
                             "a hold leaves free")
    parser.add_argument("--hog-device", type=int, default=0)
    parser.add_argument("--hog-target", choices=("gpu", "mps", "ram"),
                        default="gpu",
                        help="what the hog takes: CUDA/HIP tensors (gpu), "
                             "Apple Silicon tensors on the unified device "
                             "(mps), or host RAM (ram) -- on a unified "
                             "device `ram` is pressure on the same budget "
                             "by the other route, so a macOS pass runs "
                             "both")
    parser.add_argument("--hog-event", action="append", default=[],
                        type=parse_hog_event,
                        metavar="at=S,leave_free=MIB|hold=MIB|release",
                        help="a hog change S seconds after the job is posted, "
                             "beside the scenario's own (repeatable); a "
                             "leave-free level is solved once, at the event, "
                             "and then held; on a scenario without a hog, one "
                             "starts holding 0")
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
    parser.add_argument("--health-interval", type=float, default=None,
                        help="seconds; default: the scenario's")
    parser.add_argument("--health-full", action="store_true",
                        help="healthrec.py --full (keeps the raw payload; "
                             "~2x the file, needed for inference_clients and "
                             "reserve_* )")
    parser.add_argument("--repo", default=str(HERE.parents[1]),
                        help="repository root: a configuration id's "
                             "binary, venv and inference sources come from "
                             "it (C0's from ../panoptikon-master beside it), "
                             "and its .env is loaded into the gateway's "
                             "environment because --root chdirs away from it")
    parser.add_argument("--no-dotenv", action="store_true",
                        help="do not load <repo>/.env")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    # Only an explicit `--python` repins the worker; the default is this
    # interpreter, which is the right recorder but not the right worker. A
    # path is made absolute, since the gateway runs in `--root`; never
    # resolved, since a venv's interpreter is a symlink out of the venv.
    explicit_python = (os.path.abspath(args.python)
                       if args.python and os.path.dirname(args.python)
                       else args.python)
    args.python = args.python or sys.executable

    if args.list:
        print_table()
        return 0
    env = dict(os.environ)
    # The repo's own `.env` normally auto-loads from the CWD, but `--root`
    # chdirs away from it, so every `${PDFIUM_PATH:-}` / `${SAUCENAO_API_KEY}`
    # template in the config would fall back to empty. Loaded first so the
    # configuration's own environment wins, and never echoed: it holds API
    # keys. `run-gateway.sh` sources it too.
    dotenv = Path(args.repo).resolve() / ".env"
    if not args.no_dotenv:
        env.update(read_env_file(dotenv, env))
    if args.write_config:
        # What `config/run-gateway.sh` starts a gateway with.
        text, name, variables, _ = resolve_config(args, dict(os.environ),
                                                  explicit_python)
        refuse_inherited_visibility(text, variables, env)
        text = leg_config(text, explicit_python, args.port, args.inference_url)
        out = Path(args.write_config)
        out.mkdir(parents=True, exist_ok=True)
        stem = Path(name).stem.replace("server-", "")
        (out / f"server-{stem}.toml").write_text(text, encoding="utf-8")
        (out / f"env.{stem}").write_text(
            "".join(f"{key}={shlex.quote(value)}\n"
                    for key, value in variables.items()),
            encoding="utf-8")
        print(out / f"server-{stem}.toml")
        return 0
    if not args.scenario:
        parser.error("--scenario is required (or --list)")
    scenario = SCENARIOS.get(args.scenario)
    if scenario is None:
        parser.error(f"unknown scenario {args.scenario!r}; "
                     f"one of: {', '.join(SCENARIOS)}")
    if not args.bin and not args.dry_run:
        parser.error("--bin is required")
    if args.bin and not Path(args.bin).is_file():
        parser.error(f"--bin {args.bin} is not a file; a typo here otherwise "
                     f"starts the recorders and the hog before it is noticed")

    original, config_name, config_vars, env_source = resolve_config(
        args, env, explicit_python)
    refuse_inherited_visibility(original, config_vars, env)
    env.update(config_vars)
    env.setdefault("RUST_LOG", "info,panoptikon::inferio=trace")
    env.setdefault("INFERIO_WORKER_LOG_LEVEL", "DEBUG")
    port = args.port or config_port(original) or 6342
    base = f"http://127.0.0.1:{port}"
    health_urls = {"healthrec": base}
    if args.inference_url:
        # The server's own report: the gateway answers 504 for a server it
        # declared frozen. healthrec adds `/api/inference/health`, and the
        # gateway accepts the URL with or without `/api/inference`.
        health_urls["healthrec-remote"] = (
            args.inference_url.rstrip("/").removesuffix("/api/inference"))
    # `--models` beats the scenario's own chain, which beats a single model.
    models = ([m.strip() for m in args.models.split(",") if m.strip()]
              if args.models else
              list(scenario.models) or [args.model or scenario.model])
    model = models[0]
    # The extraction POST takes `group/id`; a bare id is a 400 from the
    # gateway 40 seconds into a leg that has already started its recorders.
    bare = [name for name in models if not _INFERENCE_ID.match(name)]
    if bare:
        parser.error(
            f"--model/--models takes a `group/id` inference id, not "
            f"{', '.join(repr(name) for name in bare)}; the extraction POST "
            f"rejects a bare id (`tags/wd-vit-tagger-v3`, "
            f"`textembed/all-MiniLM-L6-v2`). `--list` prints each scenario's "
            f"own id")
    corpus = corpus_dir(args, scenario)
    measured_total_mb = nvml_total_mb(args.hog_device)
    measured_source = "nvml"
    rocm_host = measured_total_mb is None and bool(rocm_sysfs.inventory())
    if rocm_host:
        measured_total_mb = rocm_total_mb(args.hog_device)
        measured_source = "amdgpu-sysfs"
    total_mb = args.gpu_total_mb or measured_total_mb or REFERENCE_TOTAL_MB
    wants_hog = (scenario.hog_hold_fraction is not None
                 or scenario.hog_leave_free_fraction is not None)
    if args.inference_url and (wants_hog or args.hog_event or scenario.restart
                               or scenario.learning):
        raise SystemExit(
            "legs.py: --inference-url with a hog, a restart or learning: each "
            "acts on this host, not on the inference server's")
    if ((wants_hog or args.hog_event) and not args.gpu_total_mb
            and measured_total_mb is None):
        # Scaling or bounding a figure by another machine's total is a
        # different experiment, not a degraded measurement.
        raise SystemExit(
            "legs.py: this leg drives a hog and no device total could be "
            "read (no NVML or amdgpu sysfs here). Pass --gpu-total-mb with "
            "the total the "
            "worker adopts -- on macOS that is the recommended-max "
            "`selftest.py` prints as device.gpu_total_mb, not hw.memsize")
    if args.hog_event:
        timed = tuple(sorted(scenario.events + tuple(args.hog_event),
                             key=lambda event: event.at_s))
        if wants_hog:
            scenario = replace(scenario, events=timed)
        else:
            scenario = replace(scenario, events=timed, hog_hold_fraction=0.0)
            if scenario.checks != "all":
                scenario = replace(scenario, checks=scenario.checks
                                   + ",hog_tracking,deflation_recovery")

    if args.dry_run:
        directory = Path(args.results) / (args.run_id or "<run-id>") / scenario.key
    else:
        newrun = [args.python, str(HERE / "newrun.py"), "--scenario",
                  scenario.key, "--results", str(args.results)]
        if args.run_id:
            newrun += ["--run-id", args.run_id]
        result = subprocess.run(newrun, capture_output=True, text=True)
        if result.returncode != 0:
            raise SystemExit(f"legs.py: newrun.py failed:\n{result.stderr}")
        directory = Path(result.stdout.strip().splitlines()[-1])

    # The gateway reads a per-leg copy when `--python` has to win over the
    # config's own `[inference_local] python`.
    inference_python = config_inference_python(original)
    python_source = "config" if inference_python else "the gateway's managed venv"
    if explicit_python:
        inference_python, python_source = explicit_python, "--python"
    text = leg_config(original, explicit_python, args.port, args.inference_url)
    # A generated config is always written; a config given by path is used in
    # place unless `--python`, `--port` or `--inference-url` changed it.
    gateway_config = Path(config_name)
    if text != original or not gateway_config.is_absolute():
        gateway_config = directory / gateway_config.name
        if not args.dry_run:
            gateway_config.write_text(text, encoding="utf-8")

    leg = Leg(args=args, scenario=scenario, directory=directory,
              python=args.python, config_toml=gateway_config, env=env, base=base,
              total_mb=total_mb, supervisor=Supervisor(args.stop_grace),
              endpoints=endpoints_in(text), models=tuple(models))
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
        "config": config_name if Path(config_name).is_absolute() else args.config,
        "gateway_config": str(gateway_config),
        "inference_python": inference_python,
        "inference_python_source": python_source,
        "device_env": {name: env[name] for name in DEVICE_ENV if name in env},
        "env_source": env_source or None,
        "dotenv": (None if args.no_dotenv
                   else str(dotenv) if dotenv.is_file() else None),
        "base_url": base,
        "inference_url": args.inference_url,
        "health_urls": health_urls,
        "bound_ports": {"gateway": port,
                        **{row["name"]: row["port"] for row in leg.endpoints}},
        "legacy_port": args.legacy_port,
        "endpoints": leg.endpoints,
        "model": model,
        "models": models,
        "scan_audio": bool(args.scan_audio),
        "corpus": str(corpus),
        "gpu_total_mb": total_mb,
        "gpu_total_mb_source": ("--gpu-total-mb" if args.gpu_total_mb
                                else measured_source if measured_total_mb
                                else "reference default"),
        "hog": ({"target": args.hog_target, "schedule": schedule,
                 "reeval": scenario.hog_reeval,
                 **schedule_detail} if schedule else None),
        "hog_events": events,
        "floor_bound": leg.floor_notes,
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
    for note in leg.floor_notes:
        print(f"PRECONDITION: the --min-free-mb {note['min_free_mb']} floor "
              f"binds on this {note['gpu_total_mb']} MiB GPU - {note['at']} "
              f"{note['kind']} {note['scaled_mb']} -> {note['resolved_mb']} "
              f"MiB, so this leg applies the floor's pressure"
              + ("" if note["fraction"] is None else ", not the fraction's"),
              flush=True)
        leg.mark("floor_bound", **note)
    leg.mark("inference_python", python=inference_python,
             source=python_source, config=str(gateway_config))
    complaint = corpus_complaint(
        corpus, None if args.corpus else scenario.corpus)
    if complaint is not None:
        raise SystemExit(f"legs.py: {complaint}")
    complaint = derived_text_complaint(models, corpus)
    if complaint is not None:
        raise SystemExit(f"legs.py: {complaint}")
    # A corpus of the right tier but a smaller scale runs: the scale is how
    # long the leg's job lasts, and a short job is a weaker measurement, not
    # an invalid one. It is announced and recorded, like the hog floor.
    wanted_tier, wanted_scale = corpus_tier_scale(scenario.corpus)
    try:
        have_scale = float(json.loads(
            (corpus / "manifest.json").read_text(encoding="utf-8")
        ).get("scale") or 1.0)
    except Exception:
        have_scale = wanted_scale
    if have_scale < wanted_scale:
        print(f"PRECONDITION: this leg is defined on the {wanted_tier} tier at "
              f"--scale {wanted_scale:g} and {corpus} is --scale "
              f"{have_scale:g}, so its job is shorter than the scenario's "
              f"profile window", flush=True)
        leg.mark("corpus_scale_short", tier=wanted_tier,
                 wanted_scale=wanted_scale, have_scale=have_scale,
                 corpus=str(corpus))

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
    stop_on_signals()
    try:
        # 1. the oracle, before anything of ours is on the GPU
        vram_argv = [
            args.python, str(HERE / "vramrec.py"), "--out",
            str(leg.path("vramrec.jsonl")), "--interval",
            str(args.vram_interval), "--quiet"]
        if platform.system() == "Darwin" or rocm_host:
            # The unified device's total is the worker's recommended-max, and
            # only the gateway knows it. Without this the row prices grants
            # against the 0.75 seed -- 98 304 against a real
            # 110 100 on an M3 Max -- and `grant_safety` fails legs that were
            # never near the device. The recorder starts before the gateway
            # and asks again until it answers. On ROCm it keys each GPU as the
            # gateway's `gpus` row does.
            vram_argv += ["--health-url", base]
        leg.supervisor.start("vramrec", vram_argv)
        leg.mark("vramrec_started")

        # 2. the hog, filled before the gateway sees the GPU
        if schedule:
            hog_argv = [args.python, str(HERE / "hog.py"), "--target",
                        args.hog_target, "--port", str(args.hog_port),
                        "--out", str(leg.path("hog.jsonl")),
                        "--hold-at-end", "--quiet"]
            if args.hog_target == "gpu":
                hog_argv += ["--device", str(args.hog_device)]
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
        for name, url in health_urls.items():
            health_argv = [
                args.python, str(HERE / "healthrec.py"), "--base", url,
                "--out", str(leg.path(f"{name}.jsonl")), "--interval",
                str(leg.scenario.health_interval if args.health_interval is None
                    else args.health_interval), "--quiet"]
            if url != base:
                health_argv.append("--no-queue")
            elif args.inference_url:
                # The gateway answers for a server that does not answer only
                # after its 10 s health deadline.
                health_argv += ["--timeout", "15"]
            if args.health_full:
                health_argv.append("--full")
            leg.supervisor.start(name, health_argv)
            leg.mark(f"{name}_started")
        leg.wait_for_recorders(list(health_urls))

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
        outcome = "drained"
        for index, chained in enumerate(models, start=1):
            step = leg.run_job(chained, "" if index == 1 else f"-{index}")
            if step != "drained":
                outcome = step
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
            # A *differently named* index DB, so the second job has work:
            # re-creating `cal` keeps every item the first job extracted.
            second_db = f"{DEFAULT_DB}2"
            leg.mark("second_job_prepares_a_fresh_db", index_db=second_db)
            leg.prepare_database(corpus, db=second_db, tag="-2")
            outcome = leg.run_job(model, "-resume")

        if scenario.smoke_api:
            smoke = leg.smoke_assertions()
            failed = [f"{row['name']}:{row['port']} -> "
                      f"{row.get('status', row.get('error'))}"
                      for row in smoke.get("endpoints") or []
                      if not row.get("ok")]
            if failed:
                # Recorded as an event of its own, not buried in the summary:
                # a second listener that never came up is a platform finding
                # and nobody re-reads smoke.json looking for one.
                leg.mark("endpoint_assertion_failed", endpoints=failed)
            leg.path("smoke.json").write_text(json.dumps(smoke, indent=1),
                                              encoding="utf-8")
            leg.mark("smoke_assertions", **{"summary": smoke})

        # 7. the tail
        save(f"{base}/api/inference/metadata", leg.path("metadata-after.json"))
        leg.mark("settling", seconds=args.settle)
        time.sleep(args.settle)
        save(f"{base}/api/inference/health", leg.path("health-end.json"))
        leg.snapshot_calibration("calibration.after.toml")
    except KeyboardInterrupt as exc:
        outcome = "interrupted"
        leg.mark("interrupted", signal=str(exc) or "SIGINT")
    except SystemExit as exc:
        outcome = f"aborted: {exc}"
        leg.mark("aborted", error=str(exc))
    except Exception as exc:  # pragma: no cover - the leg reports its own fault
        outcome = f"error: {type(exc).__name__}: {exc}"
        leg.mark("error", error=str(exc))
    finally:
        ignore_stop_signals()
        leg.mark("stopping", stop_grace_s=args.stop_grace)
        if fds is not None:
            fds.stop()
        # The hog is asked to release over HTTP first: that is the only stop
        # whose completion is observable before the process goes away.
        if schedule:
            try:
                request(leg.hog_url("/stop"), method="POST", timeout=5)
                leg.mark("hog_stop_requested")
                time.sleep(3.0)
            except (HttpError, OSError):
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
