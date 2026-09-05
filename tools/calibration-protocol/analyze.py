#!/usr/bin/env python3
"""analyze.py - join one scenario's recordings and print the verdict table.

Implements the verdict table of `docs/batch-calibration-test-protocol.md` §6.
It joins, by wall-clock timestamp:

    vramrec.jsonl (the independent NVML/RAM oracle), healthrec.jsonl (the
    gateway's own ledger view), hog.jsonl, fds.jsonl (optional), the ledger's
    structured lines in panoptikon.log, calibration.before/after.toml,
    jobs.json (jobs history and/or queue) and ceiling_probe.py's probe*.json.

Usage
-----
    analyze.py --scenario results/<run>/<scenario> [--checks a,b,c]
               [--json verdicts.json] [--plot timeline.png] | --list-checks
    # or override any file individually:
    analyze.py --vramrec a.jsonl --healthrec b.jsonl --log c.log --probe p.json

Options are in `--help`. A scenario declares which verdicts apply by naming
them in `--checks`; a cold-ramp leg must add `--learning`. Exit code 1 if any
selected check FAILs.

Verdicts
--------
    PASS  the threshold held               FAIL  it did not
    WARN  close to a threshold, or the scenario expected the deviation
    INFO  measured and reported, never judged (report-only rows in §6)
    SKIP  the inputs for this check were not present

Every row prints the numbers behind the verdict, so a threshold missed by a
small margin can be adjudicated by a human rather than by this script.

`SKIP` means *the harness did not record the input*, never "the run produced
no measurement" -- and a SKIP never sets the exit code. So "the store was
never written" is a **result** (WARN, or FAIL under `--learning`), while "no
probe file / no log was given to me" is a **harness omission** (SKIP, with a
pointer to what to pass). `grant_safety` is the check that decides safety, and
it reports WARN, never PASS, without `vramrec.jsonl`.

See tools/calibration-protocol/README.md: "`analyze.py` - the verdict table",
"Checks, one by one", "How a replica is tied to a process".
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# --- Loading ---------------------------------------------------------------


def read_jsonl(path: Optional[Path]) -> List[Dict[str, Any]]:
    if path is None or not path.is_file():
        return []
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # a truncated tail from a killed recorder
    return rows


LOG_LINE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T[0-9:.]+Z)\s+(?P<level>[A-Z]+)\s+"
    r"(?P<target>[A-Za-z0-9_:.\-]+):\s*(?P<rest>.*)$"
)
FIELD_START = re.compile(r"(?<![\w.])([a-z_][a-z0-9_]*)=")
FIELD = re.compile(r'([a-z_][a-z0-9_]*)=("(?:[^"\\]|\\.)*"|\S+)')
# The gateway colours a terminal-less stdout, so a `docker logs` capture is
# full of escapes that break LOG_LINE on its first field.
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def parse_log(path: Optional[Path]) -> List[Dict[str, Any]]:
    """Parse `tracing_subscriber`'s text format
    `<rfc3339> <LEVEL> <target>: <message> k=v k="v" ...`; the message is
    everything before the first `k=`. ANSI escapes are stripped first."""
    if path is None or not path.is_file():
        return []
    events: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = LOG_LINE.match(ANSI.sub("", line).rstrip("\n"))
            if match is None:
                continue
            rest = match.group("rest")
            first = FIELD_START.search(rest)
            message = rest[: first.start()].strip() if first else rest.strip()
            fields: Dict[str, Any] = {}
            if first:
                for key, raw in FIELD.findall(rest[first.start():]):
                    fields[key] = _coerce(raw)
            events.append({
                "ts": match.group("ts"),
                "t_wall": _iso_epoch(match.group("ts")),
                "level": match.group("level"),
                "target": match.group("target"),
                "message": message,
                "fields": fields,
                "line": line.rstrip("\n"),
            })
    return events


def _coerce(raw: str) -> Any:
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if raw in ("true", "false"):
        return raw == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def _iso_epoch(text: str) -> Optional[float]:
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


FDREC_LINE = re.compile(
    r"^(?P<ts>\S+)\s+fds=(?P<fds>\d+)(?:\s+sockets=(?P<sockets>\d+))?"
    r"(?:\s+limit=(?P<limit>\d+))?\s*$"
)


def read_fds(path: Optional[Path]) -> List[Dict[str, Any]]:
    """Descriptor samples for the gateway process, if anything recorded them.

    No tool in this directory records them; both the plain
    `<iso> fds=N sockets=M [limit=N]` form and a JSONL form with the same keys
    are read. See the README's "Recording file descriptors".
    """
    if path is None or not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
        else:
            match = FDREC_LINE.match(line)
            if match is None:
                continue
            row = {
                "iso": match.group("ts"),
                "fds": int(match.group("fds")),
                "sockets": (int(match.group("sockets"))
                            if match.group("sockets") is not None else None),
                "limit": (int(match.group("limit"))
                          if match.group("limit") is not None else None),
            }
        if row.get("t_wall") is None and row.get("iso"):
            row["t_wall"] = _iso_epoch(str(row["iso"]).replace("Z", "+00:00")
                                       if str(row["iso"]).endswith("Z")
                                       else str(row["iso"]))
        if row.get("fds") is not None:
            rows.append(row)
    return rows


def read_toml(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if path is None or not path.is_file():
        return None
    import tomllib

    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except Exception as exc:
        return {"__error__": f"{type(exc).__name__}: {exc}"}


def read_json(path: Optional[Path]) -> Optional[Any]:
    if path is None or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# --- Context ---------------------------------------------------------------


@dataclass
class Verdict:
    name: str
    verdict: str          # PASS | FAIL | WARN | INFO | SKIP
    detail: str
    numbers: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Context:
    args: argparse.Namespace
    vramrec: List[Dict[str, Any]]
    healthrec: List[Dict[str, Any]]
    hog: List[Dict[str, Any]]
    log: List[Dict[str, Any]]
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    jobs: Optional[Any]
    probes: List[Dict[str, Any]]
    fds: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.vram_samples = [row for row in self.vramrec if row.get("kind") == "sample"]
        self.health_samples = [row for row in self.healthrec if row.get("kind") == "sample"]
        self.hog_samples = [row for row in self.hog if row.get("kind") in ("state", "final")]
        self._vram_times = [row["t_wall"] for row in self.vram_samples]
        self._hog_times = [row["t_wall"] for row in self.hog_samples]
        self.worker_re = re.compile(self.args.worker_pattern)
        self.worker_spawns = _worker_spawns(self.log)
        self.spawned_pids = {spawn["pid"] for spawn in self.worker_spawns}
        self._pid_first_seen: Optional[Dict[int, float]] = None

    def vram_at(self, t_wall: float) -> Optional[Dict[str, Any]]:
        return _nearest(self.vram_samples, self._vram_times, t_wall,
                        self.args.join_tolerance)

    def hog_at(self, t_wall: float) -> Optional[Dict[str, Any]]:
        return _nearest(self.hog_samples, self._hog_times, t_wall,
                        self.args.join_tolerance)

    def oracle_gpu(self, sample: Dict[str, Any], uuid: str) -> Optional[Dict[str, Any]]:
        for gpu in sample.get("gpus", []):
            if gpu.get("uuid") == uuid:
                return gpu
        return None

    def our_pids_mb(self, gpu: Dict[str, Any]) -> Tuple[int, List[int]]:
        """Sum of NVML per-process usage for PIDs that are our workers.

        A PID is ours on any of three routes: a `spawned an inferio worker`
        line named it; its cmdline matches `--worker-pattern`; or its environ
        carries BOTH `INFERIO_WORKER` and `PANOPTIKON_DEVICE_PIN` (the pair
        the orchestrator sets, which `ceiling_probe.py` and `hog.py` lack).
        See the README's "How a replica is tied to a process".
        """
        total = 0
        pids: List[int] = []
        for proc in gpu.get("procs", []):
            cmdline = proc.get("cmdline") or ""
            env = proc.get("env") or {}
            ours = proc["pid"] in self.spawned_pids or bool(
                self.worker_re.search(cmdline)
            ) or (
                "PANOPTIKON_DEVICE_PIN" in env and "INFERIO_WORKER" in env
            )
            if not ours:
                continue
            pids.append(proc["pid"])
            if proc.get("used_mb"):
                total += int(proc["used_mb"])
        return total, pids

    def pid_first_seen(self) -> Dict[int, float]:
        """The first oracle sample in which each PID held memory on any GPU.

        NVML lists a process from its first allocation, so this is a lower
        bound on "was this PID there before the replica under test was
        spawned?". "Each PID" is really each *residency*: a pid absent from
        every GPU for longer than `PID_REUSE_GAP_S` and back is a new process.
        """
        if self._pid_first_seen is None:
            first: Dict[int, float] = {}
            last: Dict[int, float] = {}
            for sample in self.vram_samples:  # in time order
                t_wall = sample["t_wall"]
                for gpu in sample.get("gpus", []):
                    for proc in gpu.get("procs", []):
                        pid = proc["pid"]
                        if (pid not in last
                                or t_wall - last[pid] > PID_REUSE_GAP_S):
                            first[pid] = t_wall
                        last[pid] = t_wall
            self._pid_first_seen = first
        return self._pid_first_seen

    def replica_spawn(self, model: str,
                      admitted_t: float) -> Optional[Dict[str, Any]]:
        """The spawn record of the worker process behind this replica, if known.

        The latest such line `_worker_spawns` could tie to this model and that
        precedes the admission; `None` when the log carries none, and the
        caller must then say so rather than pretend the cross-check ran.
        """
        best: Optional[Dict[str, Any]] = None
        for spawn in self.worker_spawns:
            if spawn["model"] != model or spawn["t_wall"] > admitted_t:
                continue
            if best is None or spawn["t_wall"] > best["t_wall"]:
                best = spawn
        return best

    def attribute_replica_pid(
        self, pids: List[int], admitted_t: float, spawn_t: Optional[float],
        spawn_pid: Optional[int] = None,
    ) -> Tuple[Optional[int], Optional[str]]:
        """Which of our PIDs on the GPU is the replica that was just admitted.

        The pid the spawn line states, when it states one. Otherwise the
        freshest sighting inside [spawn, admission]; several equally plausible
        candidates decline the row rather than guess. Returns `(pid, None)` or
        `(None, why)`. See the README's "How a replica is tied to a process".
        """
        first = self.pid_first_seen()
        if spawn_pid is not None and (spawn_pid in pids
                                      or first.get(spawn_pid) is not None):
            return spawn_pid, None
        floor_t = None if spawn_t is None else spawn_t - SPAWN_CLOCK_SLACK_S
        candidates = [(first[pid], pid) for pid in pids
                      if first.get(pid) is not None
                      and (floor_t is None or first[pid] >= floor_t)]
        if not candidates:
            return None, (f"none of the {len(pids)} worker PIDs on the GPU "
                          "was first sighted at or after this replica's spawn "
                          "line, so the replica that loaded is not "
                          "attributable in the oracle")
        resident = [pair for pair in candidates if pair[0] <= admitted_t]
        if resident:
            return max(resident)[1], None
        if len(candidates) == 1:
            return candidates[0][1], None
        return None, (f"none of the {len(pids)} worker PIDs on the GPU was "
                      "sighted before the admission -- the oracle first saw "
                      "them all afterwards -- so which one loaded is not "
                      "attributable")

    def replica_departed_t(self, model: str, uuid: str,
                           admitted_t: float) -> Optional[float]:
        """When this model's replica left the GPU, if the recording says.

        Whichever comes first of the `credited a departed replica's footprint`
        line for this model/GPU and the first health sample at or after the
        admission that no longer lists it. Either ends the window in which the
        per-process figure is still `base_mb`.
        """
        end: Optional[float] = None
        for event in self.log:
            if not event["message"].startswith(DEPARTED_REPLICA):
                continue
            fields = event["fields"]
            if str(fields.get("model")) != model or str(fields.get("gpu")) != uuid:
                continue
            t_wall = event["t_wall"]
            if t_wall is None or t_wall < admitted_t:
                continue
            end = t_wall if end is None else min(end, t_wall)
            break
        for sample in self.health_samples:
            if sample["t_wall"] < admitted_t:
                continue
            resident = any(
                entry.get("inference_id") == model
                and any((replica.get("gpu_uuid") or replica.get("gpu")) == uuid
                        for replica in entry.get("replicas") or [])
                for entry in (sample.get("health") or {}).get("models") or []
            )
            if not resident:
                end = (sample["t_wall"] if end is None
                       else min(end, sample["t_wall"]))
                break
        return end

    def log_events(self, message: str) -> List[Dict[str, Any]]:
        return [event for event in self.log if event["message"] == message]

    def log_matching(self, needle: str) -> List[Dict[str, Any]]:
        return [event for event in self.log if needle in event["message"]]

    def idle_cutoff(self) -> Optional[float]:
        if not self.health_samples:
            return None
        return self.health_samples[-1]["t_wall"] - self.args.idle_window


SPAWN_PID = re.compile(r"(\d+)")
CONFIGURED_AS = "Configured as "
# What the spawn line carries when the worker is prewarmed, not claimed.
UNCONFIGURED_WORKER = "<unconfigured>"
DEPARTED_REPLICA = "credited a departed replica's footprint"
# One INFO per window settled as an OOM negative, carrying `source`, `trust`,
# `exception`, `free_mb_at_failure`, `grant_mb`, `oom_samples`; prefix match.
OOM_TIER_LINE = "classified this window as an out-of-memory negative"

# A pid absent this long and then back is read as a different process.
PID_REUSE_GAP_S = 60.0

# Slack on "sighted before its own worker was forked": enough for an NTP step,
# far too little for a genuinely older PID.
SPAWN_CLOCK_SLACK_S = 2.0


def _worker_spawns(log: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every `spawned an inferio worker` line, with the model it went on to be.

    A spawn line stating `inference_id=` beside `pid=` is the only pairing
    that cannot be wrong, and is used as-is. The `Configured as` queue below
    is the fallback, FIFO per impl class and fail-closed: such a line arriving
    with more than one spawn of its class pending marks the whole queue
    ambiguous, and an ambiguous spawn never takes a model. See the README's
    "How a replica is tied to a process".
    """
    spawns: List[Dict[str, Any]] = []
    pending: Dict[str, List[Dict[str, Any]]] = {}
    for event in log:
        worker = str(event["fields"].get("worker"))
        if event["message"] == "spawned an inferio worker":
            match = SPAWN_PID.search(str(event["fields"].get("pid", "")))
            if match is None or event["t_wall"] is None:
                continue
            stated = str(event["fields"].get("inference_id", "")).strip()
            if stated in ("", UNCONFIGURED_WORKER, "None"):
                stated = None
            spawn = {"pid": int(match.group(1)), "worker": worker,
                     "t_wall": event["t_wall"], "model": stated,
                     "ambiguous": False}
            spawns.append(spawn)
            pending.setdefault(worker, []).append(spawn)
            continue
        index = event["message"].find(CONFIGURED_AS)
        if index < 0:
            continue
        queue = pending.get(worker)
        if not queue:
            continue
        if len(queue) > 1:
            # Two of this class in the air and the log cannot say which.
            for spawn in queue:
                spawn["ambiguous"] = True
        head = queue.pop(0)
        if not head["ambiguous"] and head["model"] is None:
            head["model"] = event["message"][index + len(CONFIGURED_AS):].strip()
    return spawns


def _nearest(rows: List[Dict[str, Any]], times: List[float], target: float,
             tolerance: float) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    index = bisect.bisect_left(times, target)
    best = None
    best_dt = None
    for candidate in (index - 1, index, index + 1):
        if 0 <= candidate < len(rows):
            dt = abs(times[candidate] - target)
            if best_dt is None or dt < best_dt:
                best, best_dt = rows[candidate], dt
    if best_dt is None or best_dt > tolerance:
        return None
    return best


def _at_or_after(rows: List[Dict[str, Any]], times: List[float], target: float,
                 tolerance: float) -> Optional[Dict[str, Any]]:
    """The first sample at or after `target`, else the nearest one before it.

    `_nearest` is wrong for a quantity still *rising* at `target`: NVML's
    per-process figure climbs throughout a load while `base_mb` is reported
    only once it finished, so a sample just before the admission can be
    hundreds of MiB short where one just after agrees.
    """
    if not rows:
        return None
    index = bisect.bisect_left(times, target)
    if index < len(rows) and times[index] - target <= tolerance:
        return rows[index]
    return _nearest(rows, times, target, tolerance)


def _pct(value: float, of: float) -> float:
    return 100.0 * value / of if of else float("inf")


def health_gpus(health: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The per-GPU ledger rows of one `healthrec.py` sample.

    `"vram"` is the server's own name for the section; `"boards"` is the same
    list under an older name, read so `results/run1` and `results/run2` stay
    analysable. Not `"gpus"`, which is the GPU *inventory* in the same sample.
    """
    health = health or {}
    return health.get("vram") or health.get("boards") or []


def _pid_mb(gpu: Dict[str, Any], pid: int) -> Optional[int]:
    """One PID's NVML per-process usage on this GPU, if it holds any."""
    for proc in gpu.get("procs", []):
        if proc["pid"] == pid and proc.get("used_mb"):
            return int(proc["used_mb"])
    return None


# How long a hog must hold, and how much, before `external_mb` not moving at
# all is a fault rather than staleness. See the README's "Checks, one by one".
HOG_STALL_SECONDS = 60.0
HOG_STALL_MB = 1024


def _declared_learning(ctx: "Context") -> bool:
    """Did this leg declare itself a learning / cold-ramp scenario?

    Either `--learning`, or naming `calibration_learned` in `--checks`
    (`--checks all` is the default and declares nothing)."""
    if getattr(ctx.args, "learning", False):
        return True
    return "calibration_learned" in getattr(ctx.args, "explicit_checks", set())


def _no_store_verdict(ctx: "Context") -> str:
    """"Nothing was written to the calibration store" is a result, not a SKIP."""
    return "FAIL" if _declared_learning(ctx) else "WARN"


NO_STORE_HINT = ("this is a *result*, not a missing input: WARN, or FAIL when "
                 "the leg declares itself a learning scenario (--learning / "
                 "--checks calibration_learned)")


def _budget_series(ctx: "Context") -> Tuple[Dict[str, List[int]], Dict[str, int]]:
    """Per-model `unit_budget` over time and the best `fit_samples` seen: one
    source for `ramp_progress` and `calibration_learned`, so they agree."""
    series: Dict[str, List[int]] = {}
    fits: Dict[str, int] = {}
    for sample in ctx.health_samples:
        for worker in (sample.get("health") or {}).get("workers") or []:
            key = worker["inference_id"]
            series.setdefault(key, []).append(int(worker.get("unit_budget") or 0))
            if worker.get("fit_samples"):
                fits[key] = max(fits.get(key, 0), int(worker["fit_samples"]))
    return series, fits


def _budget_rows(ctx: "Context") -> Dict[str, Dict[str, int]]:
    series, fits = _budget_series(ctx)
    return {
        model: {"first": values[0], "peak": max(values), "last": values[-1],
                "fit_samples": fits.get(model, 0)}
        for model, values in series.items()
    }


# --- Checks ----------------------------------------------------------------


def check_oracle_agreement(ctx: Context) -> Verdict:
    """`external_mb` vs (GPU used - our workers' NVML usage): +/-1 GiB or 2%."""
    if not ctx.health_samples or not ctx.vram_samples:
        return Verdict("oracle_agreement", "SKIP",
                       "needs both healthrec.jsonl and vramrec.jsonl")
    joined = 0
    worst = 0.0
    worst_row: Dict[str, Any] = {}
    breaches = 0
    per_gpu: Dict[str, float] = {}
    for sample in ctx.health_samples:
        health = sample.get("health") or {}
        if not health.get("ok"):
            continue
        vram = ctx.vram_at(sample["t_wall"])
        if vram is None:
            continue
        for gpu in health_gpus(health):
            uuid = gpu.get("gpu_uuid")
            oracle = ctx.oracle_gpu(vram, uuid)
            if oracle is None or oracle.get("used_mb") is None:
                continue
            if not gpu.get("external_known"):
                continue
            ours, _ = ctx.our_pids_mb(oracle)
            oracle_external = max(0, int(oracle["used_mb"]) - ours)
            delta = abs(int(gpu.get("external_mb") or 0) - oracle_external)
            total = int(gpu.get("total_mb") or oracle.get("total_mb") or 0)
            allowance = max(1024.0, 0.02 * total)
            joined += 1
            per_gpu[uuid] = max(per_gpu.get(uuid, 0.0), float(delta))
            if delta > worst:
                worst = float(delta)
                worst_row = {
                    "iso": sample["iso"], "gpu": uuid,
                    "external_mb": gpu.get("external_mb"),
                    "oracle_external_mb": oracle_external,
                    "gpu_used_mb": oracle.get("used_mb"),
                    "our_pids_mb": ours,
                    "allowance_mb": round(allowance),
                }
            if delta > allowance:
                breaches += 1
    if joined == 0:
        return Verdict("oracle_agreement", "SKIP",
                       "no health sample could be joined to a vramrec sample "
                       f"within {ctx.args.join_tolerance}s")
    verdict = "PASS" if breaches == 0 else "FAIL"
    return Verdict(
        "oracle_agreement", verdict,
        f"worst |external_mb - oracle| = {worst:.0f} MiB over {joined} joined "
        f"GPU-samples; {breaches} outside the allowance",
        {"joined": joined, "breaches": breaches, "worst_mb": worst,
         "per_gpu_worst_mb": per_gpu, "worst_sample": worst_row},
    )


def check_base_accuracy(ctx: Context) -> Verdict:
    """`base_mb` vs the oracle's per-process usage at load time: +/-10% (nvml).

    The reading is the *minimum* over [admission, first grant or predict):
    past that edge the process holds the batch's workspace too. A replica that
    starts its first batch inside one sample period leaves the window empty --
    the oracle's cadence, not the ledger -- so the row is reported, not judged.
    See the README's "How a replica is tied to a process".
    """
    if not ctx.health_samples or not ctx.vram_samples:
        return Verdict("base_accuracy", "SKIP", "needs healthrec and vramrec")

    # When was each replica admitted? The log line is exact, the first health
    # sample that shows it is the fallback.
    admitted: Dict[Tuple[str, str], float] = {}
    # Both spellings: older recordings carry the pre-rename message.
    for event in (ctx.log_events("admitted a worker to a GPU's ledger")
                  + ctx.log_events("admitted a worker to a board's ledger")):
        key = (str(event["fields"].get("model")), str(event["fields"].get("gpu")))
        if event["t_wall"] and key not in admitted:
            admitted[key] = event["t_wall"]

    seen: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for sample in ctx.health_samples:
        for model in (sample.get("health") or {}).get("models") or []:
            for replica in model.get("replicas") or []:
                uuid = replica.get("gpu_uuid") or replica.get("gpu")
                if replica.get("base_mb") is None or uuid is None:
                    continue
                key = (model["inference_id"], uuid)
                if key in seen:
                    continue
                seen[key] = {
                    "t_wall": admitted.get(key, sample["t_wall"]),
                    "anchor": "log" if key in admitted else "first health sample",
                    "base_mb": replica["base_mb"],
                    "base_method": replica.get("base_method"),
                }
    if not seen:
        return Verdict("base_accuracy", "SKIP", "no replica reported a base_mb")

    # When did each model first do work? Either line ends the clean window.
    work: Dict[str, List[float]] = {}
    for message in ("issued a memory grant", "processing local inference predict"):
        for event in ctx.log_events(message):
            name = event["fields"].get("model")
            if name is None or event["t_wall"] is None:
                continue
            work.setdefault(str(name), []).append(event["t_wall"])
    for times in work.values():
        times.sort()

    rows: List[Dict[str, Any]] = []
    for (model, uuid), info in seen.items():
        vram = _at_or_after(ctx.vram_samples, ctx._vram_times, info["t_wall"],
                            ctx.args.base_window)
        if vram is None:
            rows.append({"model": model, "gpu": uuid, **info,
                         "note": "no oracle sample near the admission"})
            continue
        oracle = ctx.oracle_gpu(vram, uuid)
        if oracle is None:
            rows.append({"model": model, "gpu": uuid, **info,
                         "note": "GPU absent from the oracle sample"})
            continue
        ours, pids = ctx.our_pids_mb(oracle)
        if not pids:
            rows.append({"model": model, "gpu": uuid, **info, "pids": pids,
                         "oracle_sum_mb": ours,
                         "note": "no worker PID of ours on the GPU at the "
                                 "admission"})
            continue
        # Which of them is this replica -- never "the only one on the GPU".
        spawn = ctx.replica_spawn(model, info["t_wall"])
        spawn_t = None if spawn is None else spawn["t_wall"]
        spawn_pid = None if spawn is None else spawn["pid"]
        pid, why = ctx.attribute_replica_pid(pids, info["t_wall"], spawn_t,
                                             spawn_pid)
        if pid is None:
            rows.append({"model": model, "gpu": uuid, **info, "pids": pids,
                         "oracle_sum_mb": ours, "note": why})
            continue
        if spawn_pid is not None and pid == spawn_pid:
            spawn_check = ("pid is the one the spawn line names for this "
                           "inference id")
        elif spawn_t is not None:
            spawn_check = "pid is at or after the replica's spawn line"
        else:
            spawn_check = ("no spawn line for this model in the log (the "
                           "gateway predates the line, or it is below "
                           "panoptikon::inferio=debug): the PID is the "
                           "freshest sighting at the admission, not "
                           "cross-checked against a spawn")
        # The window in which the process holds its base and nothing else:
        # the load `ok` to the first grant or predict, or to its departure.
        times = work.get(model, [])
        index = bisect.bisect_left(times, info["t_wall"])
        busy_t = times[index] if index < len(times) else None
        gone_t = ctx.replica_departed_t(model, uuid, info["t_wall"])
        edges = [edge for edge in (busy_t, gone_t) if edge is not None]
        end_t = min(edges) if edges else None
        window: List[Tuple[float, int]] = []
        floor = None            # post-load minimum: base + whatever never freed
        for sample in ctx.vram_samples:
            if sample["t_wall"] < info["t_wall"]:
                continue
            if gone_t is not None and sample["t_wall"] >= gone_t:
                break
            gpu = ctx.oracle_gpu(sample, uuid)
            if gpu is None:
                continue
            for proc in gpu.get("procs", []):
                if proc["pid"] != pid or not proc.get("used_mb"):
                    continue
                used = int(proc["used_mb"])
                floor = used if floor is None else min(floor, used)
                if end_t is None or sample["t_wall"] < end_t:
                    window.append((sample["t_wall"], used))
        if window:
            sample_t, reading = min(window, key=lambda pair: pair[1])
            cadence = None
        else:
            # No sample in the window: report the nearest reading but do not
            # judge it, and take this PID's figure, never the GPU's sum.
            sample_t = vram["t_wall"]
            reading = _pid_mb(oracle, pid)
            if reading is None:
                rows.append({"model": model, "gpu": uuid, **info, "pids": pids,
                             "oracle_sum_mb": ours,
                             "note": f"PID {pid} holds nothing in the oracle "
                                     "sample at the admission"})
                continue
            edge = ("first work" if busy_t is not None and end_t == busy_t
                    else "the replica's departure")
            gap = "" if end_t is None else (
                f" ({edge} {round((end_t - info['t_wall']) * 1000.0)}ms "
                "after the load, oracle period is longer)")
            cadence = ("no oracle sample fell between the load and this "
                       "replica's first grant or predict" + gap)
        error = abs(info["base_mb"] - reading)
        rows.append({"model": model, "gpu": uuid, **info, "pid": pid,
                     "oracle_pid_mb": reading, "oracle_pid_min_mb": floor,
                     "error_mb": error, "spawn_check": spawn_check,
                     "oracle_window_samples": len(window),
                     "first_work_dt_ms": (None if busy_t is None else
                                          round((busy_t - info["t_wall"]) * 1000.0)),
                     "cadence_blind": cadence is not None,
                     **({"cadence_note": cadence} if cadence else {}),
                     # How far after the admission the reading sits.
                     "oracle_dt_ms": round((sample_t - info["t_wall"]) * 1000.0),
                     "error_pct": round(_pct(error, max(1, reading)), 2)})

    reported = [row for row in rows if row.get("error_pct") is not None]
    judged = [row for row in reported if row.get("base_method") == "nvml"
              and not row.get("cadence_blind")]
    if not reported:
        return Verdict("base_accuracy", "SKIP",
                       "; ".join(f"{row['model']}: {row.get('note')}" for row in rows),
                       {"rows": rows})
    detail = "; ".join(
        f"{row['model']} base_mb={row['base_mb']} ({row['base_method']}) vs oracle "
        f"PID {row['oracle_pid_mb']} MiB at admission+{row['oracle_dt_ms']}ms = "
        f"{row['error_pct']}% (post-load min {row['oracle_pid_min_mb']})"
        + (f" [not judged: {row['cadence_note']}]" if row.get("cadence_blind") else "")
        for row in reported
    )
    if not judged:
        reasons = []
        if any(row.get("cadence_blind") for row in reported):
            reasons.append("no oracle sample fell between the load and the "
                           "replica's first work")
        if any(row.get("base_method") != "nvml" for row in reported):
            reasons.append("base_method is not nvml")
        return Verdict("base_accuracy", "INFO",
                       detail + "  [report-only: " + "; ".join(reasons) + "]",
                       {"rows": rows})
    worst = max(judged, key=lambda row: row["error_pct"])
    verdict = "PASS" if worst["error_pct"] <= 10.0 else "FAIL"
    return Verdict("base_accuracy", verdict, detail + "  [threshold 10%]",
                   {"rows": rows, "worst": worst})


def check_footprint_agreement(ctx: Context) -> Verdict:
    """Per GPU: `footprints_mb` vs the summed NVML usage of our PIDs."""
    if not ctx.health_samples or not ctx.vram_samples:
        return Verdict("footprint_agreement", "SKIP", "needs healthrec and vramrec")
    worst = 0.0
    worst_row: Dict[str, Any] = {}
    joined = 0
    for sample in ctx.health_samples:
        health = sample.get("health") or {}
        vram = ctx.vram_at(sample["t_wall"])
        if vram is None:
            continue
        for gpu in health_gpus(health):
            oracle = ctx.oracle_gpu(vram, gpu.get("gpu_uuid"))
            if oracle is None:
                continue
            ours, pids = ctx.our_pids_mb(oracle)
            if not pids:
                continue
            joined += 1
            delta = abs(int(gpu.get("footprints_mb") or 0) - ours)
            if delta > worst:
                worst = float(delta)
                worst_row = {"iso": sample["iso"], "gpu": gpu.get("gpu_uuid"),
                             "footprints_mb": gpu.get("footprints_mb"),
                             "oracle_our_pids_mb": ours, "pids": pids}
    if joined == 0:
        return Verdict("footprint_agreement", "SKIP", "no worker PID seen on any GPU")
    return Verdict("footprint_agreement", "INFO",
                   f"worst |footprints_mb - Sum(our PIDs)| = {worst:.0f} MiB "
                   f"over {joined} GPU-samples (report-only: footprints "
                   f"exclude pool growth the grant already counts)",
                   {"joined": joined, "worst_mb": worst, "worst_sample": worst_row})


def check_slope_accuracy(ctx: Context) -> Verdict:
    """Persisted slope vs ceiling_probe's: -30% .. +100%.

    The two ways this check cannot run do not share a verdict: "no store was
    written" is what the run did, "no probe file was passed" what the harness
    forgot."""
    profiles = (ctx.after or {}).get("profile") or []
    if not profiles:
        return Verdict(
            "slope_accuracy", _no_store_verdict(ctx),
            ("no calibration.after.toml in the scenario directory"
             if ctx.after is None else
             "calibration.after.toml carries no [[profile]]")
            + " -- there is no learned slope to compare, because nothing was "
              "written to the store; " + NO_STORE_HINT,
            {"profiles": 0, "after_present": ctx.after is not None,
             "learning": _declared_learning(ctx)})
    if not ctx.probes:
        return Verdict("slope_accuracy", "SKIP",
                       "no ceiling_probe result: pass --probe "
                       "probe-<model>.json (and bisect-<model>.json), or drop "
                       "probe*.json into the scenario directory. The store "
                       f"itself holds {len(profiles)} profile(s), so this is a "
                       "missing input, not a missing measurement",
                       {"profiles": len(profiles)})
    rows = []
    verdict = "PASS"
    for probe in ctx.probes:
        fit = probe.get("fit")
        if not fit:
            rows.append({"model": probe.get("model"), "probe_slope": None,
                         "note": "probe produced no fit"})
            continue
        model = probe.get("model")
        match = next((p for p in profiles if p.get("inference_id") == model), None)
        if match is None:
            rows.append({"model": model, "probe_slope": fit["slope_mb_per_unit"],
                         "note": "model absent from the store"})
            verdict = "FAIL" if verdict != "FAIL" else verdict
            continue
        ledger = float(match.get("slope_mb_per_unit") or 0.0)
        probe_slope = float(fit["slope_mb_per_unit"])
        ratio = ledger / probe_slope if probe_slope else float("inf")
        ok = 0.70 <= ratio <= 2.00
        rows.append({"model": model, "ledger_slope": ledger,
                     "probe_slope": probe_slope, "ratio": round(ratio, 4),
                     "ok": ok,
                     "ledger_base_mb": match.get("base_mb"),
                     "probe_base_mb": (probe.get("load") or {}).get("base_nvml_mb")})
        if not ok:
            verdict = "FAIL"
    detail = "; ".join(
        f"{row['model']}: ledger {row.get('ledger_slope')} vs probe "
        f"{row.get('probe_slope')} MiB/unit (ratio {row.get('ratio')})"
        if row.get("ratio") is not None else f"{row['model']}: {row.get('note')}"
        for row in rows
    )
    return Verdict("slope_accuracy", verdict,
                   detail + "  [allowed 0.70x .. 2.00x]", {"models": rows})


def check_grant_safety(ctx: Context) -> Verdict:
    """THE safety check: grants vs their priced headroom AND the oracle's free memory.

    The second clause is the one with teeth: it joins each grant to
    `vramrec.jsonl` and asks whether it exceeded the GPU's *live* free memory.
    Without that file the check reports WARN, never PASS -- the priced-headroom
    clause alone only re-checks the ledger's arithmetic against itself.
    """
    grants = ctx.log_events("issued a memory grant")
    if not grants:
        return Verdict("grant_safety", "SKIP",
                       "no 'issued a memory grant' lines "
                       "(RUST_LOG=info,panoptikon::inferio=trace?)")
    over_headroom = []
    over_free = []
    joined = 0
    for event in grants:
        fields = event["fields"]
        mb = fields.get("mb")
        headroom = fields.get("headroom_mb")
        if isinstance(mb, (int, float)) and isinstance(headroom, (int, float)):
            if mb > headroom:
                over_headroom.append({"iso": event["ts"], **fields})
        vram = ctx.vram_at(event["t_wall"]) if event["t_wall"] else None
        if vram is not None and isinstance(mb, (int, float)):
            oracle = ctx.oracle_gpu(vram, fields.get("gpu"))
            if oracle and oracle.get("free_mb") is not None:
                joined += 1
                if mb > oracle["free_mb"]:
                    over_free.append({"iso": event["ts"], "mb": mb,
                                      "oracle_free_mb": oracle["free_mb"],
                                      "gpu": fields.get("gpu"),
                                      "model": fields.get("model")})
    zero_mb = sum(1 for event in grants if event["fields"].get("mb") == 0)
    if over_headroom or over_free:
        verdict = "FAIL"
    elif joined == 0:
        # The clause that decides safety never ran, so this must not read PASS.
        verdict = "WARN"
    else:
        verdict = "PASS"
    detail = (f"{len(grants)} grants; {len(over_headroom)} exceeded the headroom "
              f"they were priced against; {len(over_free)} exceeded the oracle's "
              f"live free memory ({joined} joined); {zero_mb} were memory-blind "
              f"(mb=0, B1)")
    if verdict == "WARN":
        detail += ("  -- ORACLE CLAUSE NOT RUN: "
                   + ("no vramrec.jsonl in the scenario (record it with "
                      "vramrec.py; it is what makes this the check that decides "
                      "safety)"
                      if not ctx.vram_samples else
                      f"no grant joined a vramrec sample within "
                      f"{ctx.args.join_tolerance}s")
                   + ". Only the ledger's own arithmetic was verified")
    return Verdict(
        "grant_safety", verdict, detail,
        {"grants": len(grants), "over_headroom": over_headroom[:10],
         "over_free": over_free[:10], "zero_mb_grants": zero_mb,
         "joined": joined, "vramrec_samples": len(ctx.vram_samples),
         "oracle_clause_ran": joined > 0},
    )


def check_failures(ctx: Context) -> Verdict:
    """OOM negatives, worker deaths and merged-window fallbacks in the log.

    Where the log names the tier that classified each negative, it is tallied
    as `source/trust`; a recording predating that line carries none, and the
    clause is then omitted rather than reported empty."""
    if not ctx.log:
        return Verdict("failures", "SKIP", "no panoptikon.log")
    negatives = [
        event for event in ctx.log_events("settled a granted window")
        if event["level"] == "WARN"
    ]
    reasons: Dict[str, int] = {}
    for event in negatives:
        reason = str(event["fields"].get("reason", "?"))
        reasons[reason] = reasons.get(reason, 0) + 1
    ooms = reasons.get("oom", 0)
    collapses = reasons.get("throughput_collapse", 0)
    # `"unified_board_death"` is the pre-rename spelling in older recordings.
    unified_deaths = (reasons.get("unified_device_death", 0)
                      + reasons.get("unified_board_death", 0))
    deaths = len(ctx.log_matching("worker died fatally"))
    fallbacks = ctx.log_matching("falling back to per-request prediction")
    oom_fallbacks = sum(1 for event in fallbacks if event["fields"].get("oom") is True)
    tiers: Dict[str, int] = {}
    for event in ctx.log_matching(OOM_TIER_LINE):
        key = "{}/{}".format(event["fields"].get("source", "?"),
                             event["fields"].get("trust", "?"))
        tiers[key] = tiers.get(key, 0) + 1
    tier_clause = ""
    if tiers:
        named = ", ".join(f"{key}={count}" for key, count in sorted(tiers.items()))
        # A negative with no tier line is a hole in the attribution: say so.
        unnamed = ooms - sum(tiers.values())
        tier_clause = f"; tiers {named}"
        if unnamed:
            tier_clause += f", {unnamed} unnamed"
    expected_ooms = ctx.args.expect_ooms
    expected_deaths = ctx.args.expect_deaths
    bad = ooms > expected_ooms or deaths > expected_deaths
    verdict = "FAIL" if bad else ("WARN" if (ooms or deaths) else "PASS")
    return Verdict(
        "failures", verdict,
        f"{ooms} OOM negatives (expected <= {expected_ooms}), "
        f"{collapses} throughput-collapse negatives, "
        f"{unified_deaths} unified-memory-device death negatives, "
        f"{deaths} fatal worker deaths (expected <= {expected_deaths}), "
        f"{len(fallbacks)} merged-window fallbacks ({oom_fallbacks} OOM)"
        f"{tier_clause}",
        {"negative_reasons": reasons, "worker_deaths": deaths,
         "fallbacks": len(fallbacks), "oom_fallbacks": oom_fallbacks,
         **({"oom_tiers": tiers} if tiers else {}),
         "first_death": (ctx.log_matching("worker died fatally") or [{}])[0].get("line")},
    )


def check_deflation_recovery(ctx: Context) -> Verdict:
    """Deflation must return to 0 within 3 clean windows per level."""
    if not ctx.health_samples:
        return Verdict("deflation_recovery", "SKIP", "no healthrec.jsonl")
    peak: Dict[str, int] = {}
    last: Dict[str, int] = {}
    series: Dict[str, List[Tuple[float, int]]] = {}
    for sample in ctx.health_samples:
        for worker in (sample.get("health") or {}).get("workers") or []:
            key = f"{worker['inference_id']}@{worker.get('gpu_uuid')}"
            value = int(worker.get("deflation") or 0)
            peak[key] = max(peak.get(key, 0), value)
            last[key] = value
            series.setdefault(key, []).append((sample["t_wall"], value))
    if not peak:
        return Verdict("deflation_recovery", "SKIP", "no workers in any health sample")
    cutoff = ctx.idle_cutoff()
    stuck = {key: value for key, value in last.items() if value > 0}
    max_peak = max(peak.values())
    if max_peak == 0:
        return Verdict("deflation_recovery", "PASS",
                       "deflation never left 0 on any worker",
                       {"peak": peak})
    verdict = "PASS" if not stuck else "FAIL"
    return Verdict(
        "deflation_recovery", verdict,
        f"peak deflation {max_peak} ({', '.join(f'{k}={v}' for k, v in peak.items())}); "
        f"at the end of the recording {len(stuck)} worker(s) were still deflated"
        + (f" ({stuck})" if stuck else ""),
        {"peak": peak, "final": last, "idle_cutoff": cutoff},
    )


def check_idle_liveness(ctx: Context) -> Verdict:
    """`grants_outstanding` must be 0 once the load stops."""
    if not ctx.health_samples:
        return Verdict("idle_liveness", "SKIP", "no healthrec.jsonl")
    cutoff = ctx.idle_cutoff()
    tail = [row for row in ctx.health_samples if row["t_wall"] >= (cutoff or 0)]
    if not tail:
        return Verdict("idle_liveness", "SKIP", "no samples in the idle window")
    busy = []
    for sample in tail:
        health = sample.get("health") or {}
        pending = sum(
            int(worker.get("pending_requests") or 0)
            for worker in health.get("workers") or []
        )
        outstanding = sum(
            int(gpu.get("grants_outstanding") or 0)
            for gpu in health_gpus(health)
        )
        if outstanding and not pending:
            busy.append({"iso": sample["iso"], "grants_outstanding": outstanding})
    last = tail[-1]
    final = sum(int(gpu.get("grants_outstanding") or 0)
                for gpu in health_gpus(last.get("health")))
    verdict = "PASS" if final == 0 else "FAIL"
    return Verdict(
        "idle_liveness", verdict,
        f"final grants_outstanding = {final} over the last "
        f"{ctx.args.idle_window:.0f}s ({len(tail)} samples); "
        f"{len(busy)} sample(s) held a grant with no pending request",
        {"final_outstanding": final, "idle_samples": len(tail),
         "grant_without_demand": busy[:5]},
    )


def check_utilization(ctx: Context) -> Verdict:
    """Admitted units vs the probe's OOM boundary (or knee).

    Same split as `slope_accuracy`: "no worker was ever admitted" is a result,
    "no probe boundary was passed" a harness omission."""
    if not ctx.health_samples:
        return Verdict("utilization", "SKIP",
                       "no healthrec.jsonl in the scenario -- record the "
                       "gateway's own view with healthrec.py")
    peak: Dict[str, int] = {}
    for sample in ctx.health_samples:
        for worker in (sample.get("health") or {}).get("workers") or []:
            key = worker["inference_id"]
            peak[key] = max(peak.get(key, 0), int(worker.get("unit_budget") or 0))
    if not peak:
        return Verdict("utilization", _no_store_verdict(ctx),
                       f"no worker appears in any of the "
                       f"{len(ctx.health_samples)} health samples: nothing was "
                       f"ever admitted, so there is no utilization to measure; "
                       + NO_STORE_HINT,
                       {"health_samples": len(ctx.health_samples),
                        "learning": _declared_learning(ctx)})
    boundaries: Dict[str, Optional[int]] = {}
    for probe in ctx.probes:
        bisect_info = probe.get("bisect") or {}
        boundaries[probe.get("model")] = (
            bisect_info.get("largest_ok_units")
            or (probe.get("batches") or [{}])[-1].get("units")
        )
    rows = []
    verdict = "INFO"
    threshold = ctx.args.utilization_floor
    for model, admitted in peak.items():
        boundary = boundaries.get(model)
        if not boundary:
            rows.append({"model": model, "peak_unit_budget": admitted,
                         "boundary_units": None})
            continue
        ratio = admitted / boundary
        ok = ratio >= threshold
        rows.append({"model": model, "peak_unit_budget": admitted,
                     "boundary_units": boundary, "ratio": round(ratio, 4),
                     "ok": ok})
        verdict = "PASS" if (verdict in ("INFO", "PASS") and ok) else "FAIL"
    detail = "; ".join(
        f"{row['model']}: peak unit_budget {row['peak_unit_budget']}"
        + (f" / probe boundary {row['boundary_units']} = {row['ratio']:.2f}"
           if row.get("boundary_units") else " (no probe boundary)")
        for row in rows
    )
    if not any(row.get("boundary_units") for row in rows):
        # Nothing to divide by: a missing input, so SKIP, never a green row.
        return Verdict("utilization", "SKIP",
                       detail + "  -- no probe boundary for any model: pass "
                       "--probe bisect-<model>.json (preferred: the check uses "
                       "`bisect.largest_ok_units`) and/or "
                       "--probe probe-<model>.json",
                       {"models": rows})
    return Verdict("utilization", verdict,
                   detail + f"  [floor {threshold:.2f}]", {"models": rows})


def check_throughput(ctx: Context) -> Verdict:
    """Items/s from `LogRecord` vs the C0 baseline."""
    records = _log_records(ctx.jobs)
    if not records:
        return Verdict("throughput", "SKIP",
                       "jobs.json has no LogRecord history entries")
    ours = _items_per_s(records)
    baseline = ctx.args.baseline_items_per_s
    if baseline is None and ctx.args.baseline_jobs:
        baseline_records = _log_records(read_json(Path(ctx.args.baseline_jobs)))
        baseline = _items_per_s(baseline_records)
    if not baseline:
        return Verdict("throughput", "INFO",
                       f"{ours:.3f} items/s over {len(records)} job(s); "
                       "no baseline given (--baseline-jobs/--baseline-items-per-s)",
                       {"items_per_s": ours, "jobs": len(records)})
    ratio = ours / baseline if baseline else float("inf")
    verdict = "PASS" if ratio >= ctx.args.throughput_floor else "FAIL"
    return Verdict("throughput", verdict,
                   f"{ours:.3f} items/s vs baseline {baseline:.3f} = "
                   f"{ratio:.2f}x  [floor {ctx.args.throughput_floor:.2f}x]",
                   {"items_per_s": ours, "baseline_items_per_s": baseline,
                    "ratio": ratio, "jobs": len(records)})


def _log_records(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        for key in ("history", "logs", "records", "data"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
        else:
            payload = [payload] if "total_segments" in payload else []
    if not isinstance(payload, list):
        return []
    return [row for row in payload
            if isinstance(row, dict) and "total_segments" in row]


def _items_per_s(records: List[Dict[str, Any]]) -> float:
    items = 0.0
    seconds = 0.0
    for record in records:
        items += float(record.get("total_segments") or 0)
        start = _iso_epoch(str(record.get("start_time", "")).replace(" ", "T"))
        end = _iso_epoch(str(record.get("end_time", "")).replace(" ", "T"))
        if start and end and end > start:
            seconds += end - start
        else:
            seconds += float(record.get("inference_time") or 0) + float(
                record.get("data_load_time") or 0)
    return items / seconds if seconds else 0.0


def check_persistence(ctx: Context) -> Verdict:
    """The store must be written within 30 s of an anchor advance.

    Same split again: an absent or empty store is a result (WARN, FAIL under
    `--learning`), an absent *log* a harness omission (SKIP)."""
    queued = ctx.log_events("queued a calibration profile update for the store")
    writes = ctx.log_events("wrote the local calibration store")
    after = ctx.after
    profiles = (after or {}).get("profile") or []
    if after is None or not profiles:
        return Verdict(
            "persistence", _no_store_verdict(ctx),
            ("no calibration.after.toml in the scenario directory"
             if after is None else
             "calibration.after.toml carries no [[profile]]")
            + " -- nothing was persisted; " + NO_STORE_HINT,
            {"profiles": 0, "after_present": after is not None,
             "queued": len(queued), "writes": len(writes),
             "learning": _declared_learning(ctx)},
        )
    if not queued and not writes:
        return Verdict(
            "persistence", "SKIP",
            f"{len(profiles)} profile(s) on disk, but the log carries no store "
            "lines to time the debounce against: capture panoptikon.log with "
            "RUST_LOG=info,panoptikon::inferio=trace (the store is there, so "
            "this is a missing input, not a missing measurement)",
            {"profiles": len(profiles), "log_events": len(ctx.log)},
        )
    worst = None
    for event in queued:
        if event["fields"].get("reason") != "anchor_advanced":
            continue
        later = [write for write in writes
                 if write["t_wall"] and event["t_wall"]
                 and write["t_wall"] >= event["t_wall"]]
        if not later:
            worst = float("inf")
            break
        delay = later[0]["t_wall"] - event["t_wall"]
        worst = delay if worst is None else max(worst, delay)
    anchors = {
        profile.get("inference_id"): profile.get("max_units_measured")
        for profile in profiles
    }
    before_anchors = {
        profile.get("inference_id"): profile.get("max_units_measured")
        for profile in ((ctx.before or {}).get("profile") or [])
    }
    regressions = {
        model: (before_anchors[model], anchors[model])
        for model in anchors
        if model in before_anchors
        and (before_anchors[model] or 0) > (anchors[model] or 0)
    }
    if worst is None:
        verdict = "INFO"
        detail = (f"{len(profiles)} profile(s); no anchor_advanced update was "
                  f"queued during the recording")
    elif worst == float("inf"):
        verdict = "FAIL"
        detail = "an anchor advance was queued but the store was never written"
    else:
        verdict = "PASS" if worst <= 30.0 else "FAIL"
        detail = (f"worst anchor-advance -> store-write delay {worst:.1f}s "
                  f"[threshold 30s]; {len(writes)} write(s), "
                  f"{len(profiles)} profile(s)")
    if regressions:
        verdict = "FAIL"
        detail += f"; anchor regressed for {regressions}"
    return Verdict("persistence", verdict, detail,
                   {"queued": len(queued), "writes": len(writes),
                    "profiles": len(profiles), "anchors": anchors,
                    "before_anchors": before_anchors,
                    "regressions": regressions})


def check_job_outcome(ctx: Context) -> Verdict:
    """Jobs must complete; item failures only where the scenario poisoned them."""
    records = _log_records(ctx.jobs)
    queue_outcomes: List[Dict[str, Any]] = []
    if isinstance(ctx.jobs, dict) and isinstance(ctx.jobs.get("outcomes"), list):
        queue_outcomes = ctx.jobs["outcomes"]
    for sample in reversed(ctx.health_samples):
        if queue_outcomes:
            break
        outcomes = (sample.get("queue") or {}).get("outcomes")
        if outcomes:
            queue_outcomes = outcomes
    if not records and not queue_outcomes:
        return Verdict("job_outcome", "SKIP", "no jobs.json and no queue outcomes")
    # `completed` and `failed` on a job record are flags (this job completed /
    # this job failed), not item counts: `failed_items` is the count
    # `--expect-failures` judges. Run1 records predate `failed_items` and carry
    # `failed = 0`, so the fallback keeps their reading unchanged.
    failed = sum(int(record["failed_items"]) if record.get("failed_items") is not None
                 else int(record.get("failed") or 0)
                 for record in records)
    errors = sum(int(record.get("errors") or 0) for record in records)
    completed = sum(int(record.get("completed") or 0) for record in records)
    bad_outcomes = [row for row in queue_outcomes
                    if row.get("status") not in (None, "completed")]
    over = failed > ctx.args.expect_failures
    # A scenario can declare that a whole job is *meant* to fail, or it would
    # report `job_outcome FAIL` for doing exactly what it set out to do.
    expected_bad = ctx.args.expect_failed_jobs
    over_jobs = len(bad_outcomes) > expected_bad
    verdict = "FAIL" if (over or over_jobs) else "PASS"
    return Verdict(
        "job_outcome", verdict,
        f"{len(records)} job record(s): {completed} completed, "
        f"{failed} failed item(s) (expected <= {ctx.args.expect_failures}), "
        f"{errors} errors; queue outcomes: "
        f"{[row.get('status') for row in queue_outcomes] or 'none'}"
        + (f" ({len(bad_outcomes)} not completed, expected <= {expected_bad})"
           if (bad_outcomes or expected_bad) else ""),
        {"completed": completed, "failed": failed, "errors": errors,
         "outcomes": queue_outcomes, "records": len(records),
         "failed_jobs": len(bad_outcomes),
         "expected_failed_jobs": expected_bad},
    )


def check_ledger_invariant(ctx: Context) -> Verdict:
    """The admission invariant, in both of the forms it has.

    Strict form (§6): on every GPU sample, our charges plus our load
    reservations are at most `limit_mb`. It cannot hold on a nearly-full GPU,
    where `limit_mb` reaches 0 while our own residents legitimately hold
    gigabytes, so a zero-limit breach reports WARN while one against a
    non-zero limit FAILs. The form that must always hold is `grant_safety`'s,
    restated on this row. See the README's "Checks".
    """
    if not ctx.health_samples:
        return Verdict("ledger_invariant", "SKIP", "no healthrec.jsonl")
    breaches = []
    zero_limit_breaches = []
    checked = 0
    zero_limit_samples = 0
    for sample in ctx.health_samples:
        for gpu in health_gpus(sample.get("health")):
            limit = gpu.get("limit_mb")
            if limit is None:
                continue
            checked += 1
            if int(limit) == 0:
                zero_limit_samples += 1
            used = int(gpu.get("charges_mb") or 0) + int(
                gpu.get("load_reservations_mb") or 0)
            if used > int(limit):
                row = {"iso": sample["iso"], "gpu": gpu.get("gpu_uuid"),
                       "charges_mb": gpu.get("charges_mb"),
                       "load_reservations_mb": gpu.get("load_reservations_mb"),
                       "limit_mb": limit,
                       "external_mb": gpu.get("external_mb")}
                (zero_limit_breaches if int(limit) == 0 else breaches).append(row)
    if checked == 0:
        return Verdict("ledger_invariant", "SKIP", "no GPU carried a limit_mb")

    # The restated form, from the grant lines rather than the health samples.
    grants = ctx.log_events("issued a memory grant")
    over_headroom = 0
    for event in grants:
        fields = event["fields"]
        mb, headroom = fields.get("mb"), fields.get("headroom_mb")
        if isinstance(mb, (int, float)) and isinstance(headroom, (int, float)):
            over_headroom += int(mb > headroom)

    total_breaches = len(breaches) + len(zero_limit_breaches)
    if breaches:
        verdict = "FAIL"
    elif zero_limit_breaches:
        verdict = "WARN"
    else:
        verdict = "PASS"
    detail = (f"strict: {total_breaches} of {checked} GPU-samples had "
              f"charges + load reservations > limit_mb")
    if zero_limit_breaches:
        detail += (f", of which {len(zero_limit_breaches)} had limit_mb = 0 "
                   f"(the margin zeroes the limit once external x (1+margin) "
                   f"> total -- our own residents cannot be bounded by it; "
                   f"T6/P5-2)")
    detail += (f"; {zero_limit_samples} of {checked} samples were at "
               f"limit_mb = 0. our own residents: "
               + (f"{over_headroom} of {len(grants)} grants exceeded the "
                  f"headroom they were priced against"
                  if grants else "no grant lines to check"))
    return Verdict("ledger_invariant", verdict, detail,
                   {"checked": checked, "breaches": breaches[:10],
                    "zero_limit_breaches": zero_limit_breaches[:10],
                    "zero_limit_samples": zero_limit_samples,
                    "grants": len(grants),
                    "grants_over_headroom": over_headroom})


def check_hog_tracking(ctx: Context) -> Verdict:
    """`external_mb` must follow what hog.py actually holds (INFO, but see the FAIL form).

    Report-only in general: `external` is a window-boundary quantity with a
    real staleness, so a GPU that updates *late* is behaving as designed. One
    shape is not staleness and FAILs: a hog held at `HOG_STALL_MB` or more for
    `HOG_STALL_SECONDS` while `external_mb` never moved. See the README.
    """
    if not ctx.hog_samples or not ctx.health_samples:
        return Verdict("hog_tracking", "SKIP", "needs hog.jsonl and healthrec.jsonl")
    header = next((row for row in ctx.hog if row.get("kind") == "header"), {})
    gpu_uuid = header.get("gpu_uuid")
    if header.get("target") == "ram":
        return Verdict("hog_tracking", "INFO",
                       "the hog pressured RAM, not a GPU; see the vramrec "
                       "MemAvailable series", {"header": header})
    rows = []
    worst_lag = 0.0
    worst = None
    for sample in ctx.health_samples:
        gpu = next((entry for entry in health_gpus(sample.get("health"))
                      if entry.get("gpu_uuid") == gpu_uuid), None)
        if gpu is None or not gpu.get("external_known"):
            continue
        hog = ctx.hog_at(sample["t_wall"])
        if hog is None:
            continue
        vram = ctx.vram_at(sample["t_wall"])
        oracle_used = None
        if vram is not None:
            oracle = ctx.oracle_gpu(vram, gpu_uuid)
            oracle_used = oracle.get("used_mb") if oracle else None
        rows.append({"t_wall": sample["t_wall"], "iso": sample["iso"],
                     "external_mb": gpu.get("external_mb"),
                     "hog_held_mb": hog.get("held_mb"),
                     "gpu_used_mb": oracle_used,
                     "sample_age_ms": gpu.get("external_sample_age_ms")})
    if not rows:
        return Verdict("hog_tracking", "SKIP",
                       f"no health sample joined the hog on GPU {gpu_uuid}")
    ages = [row["sample_age_ms"] for row in rows if row["sample_age_ms"] is not None]
    max_age = max(ages) / 1000.0 if ages else None
    # Track the correlation of the two deltas rather than absolute agreement:
    # `external` also contains whatever else lives on the GPU.
    deltas = []
    for previous, current in zip(rows, rows[1:]):
        d_hog = (current["hog_held_mb"] or 0) - (previous["hog_held_mb"] or 0)
        d_ext = (current["external_mb"] or 0) - (previous["external_mb"] or 0)
        if abs(d_hog) >= 256:
            deltas.append({"iso": current["iso"], "d_hog_mb": d_hog,
                           "d_external_mb": d_ext})

    # The FAIL form: wall time between two consecutive joined samples that
    # were *both* above the threshold, so a spike buys no time.
    held_seconds = 0.0
    for previous, current in zip(rows, rows[1:]):
        if ((previous["hog_held_mb"] or 0) >= HOG_STALL_MB
                and (current["hog_held_mb"] or 0) >= HOG_STALL_MB):
            held_seconds += current["t_wall"] - previous["t_wall"]
    externals = {row["external_mb"] for row in rows}
    external_moved = len(externals) > 1
    stalled = held_seconds > HOG_STALL_SECONDS and not external_moved

    detail = (
        f"{len(rows)} joined samples on {gpu_uuid}; hog held "
        f"{min(row['hog_held_mb'] or 0 for row in rows)}.."
        f"{max(row['hog_held_mb'] or 0 for row in rows)} MiB; external_mb "
        f"{min(row['external_mb'] or 0 for row in rows)}.."
        f"{max(row['external_mb'] or 0 for row in rows)} MiB; worst "
        f"external_sample_age {max_age if max_age is None else round(max_age, 1)}s; "
        f"{len(deltas)} step(s) >= 256 MiB"
    )
    if stalled:
        detail += (f"  -- FAIL: the hog held >= {HOG_STALL_MB} MiB for "
                   f"{held_seconds:.0f}s (> {HOG_STALL_SECONDS:.0f}s) and "
                   f"external_mb never moved from "
                   f"{next(iter(externals))} across the whole recording. That "
                   f"is not the B2 staleness window -- a GPU that updates "
                   f"late still moves")
    else:
        detail += (f"; the hog held >= {HOG_STALL_MB} MiB for "
                   f"{held_seconds:.0f}s and external_mb "
                   + ("moved" if external_moved else "never moved")
                   + f" [FAIL needs both: > {HOG_STALL_SECONDS:.0f}s held and "
                     f"no movement at all]")
    return Verdict(
        "hog_tracking", "FAIL" if stalled else "INFO", detail,
        {"joined": len(rows), "steps": deltas[:20],
         "max_external_sample_age_s": max_age,
         "hog_held_seconds_over_threshold": round(held_seconds, 1),
         "hog_threshold_mb": HOG_STALL_MB,
         "stall_threshold_s": HOG_STALL_SECONDS,
         "external_moved": external_moved,
         "distinct_external_mb": len(externals)},
    )


def check_ramp_progress(ctx: Context) -> Verdict:
    """The ramp must actually move: ramp_step / unit_budget over time."""
    if not ctx.health_samples:
        return Verdict("ramp_progress", "SKIP", "no healthrec.jsonl")
    rows = _budget_rows(ctx)
    if not rows:
        return Verdict("ramp_progress", "SKIP", "no workers in any health sample")
    stalled_at_64 = [model for model, row in rows.items() if row["peak"] == 64]
    detail = "; ".join(
        f"{model}: unit_budget {row['first']} -> peak {row['peak']} "
        f"(last {row['last']}, fit samples {row['fit_samples']})"
        for model, row in rows.items()
    )
    if stalled_at_64:
        detail += (f"  [peak exactly 64 for {', '.join(stalled_at_64)}: check "
                   f"REQUEST_UNIT_BUDGET, finding B16]")
    return Verdict("ramp_progress", "INFO", detail, {"models": rows})


def check_calibration_learned(ctx: Context) -> Verdict:
    """A learning leg must end having learned something: fit samples, a store, a moved anchor.

    `ramp_progress`'s three numbers promoted to a verdict, so a leg that
    stopped measuring cannot come back green on SKIPs alone. FAILs only for a
    leg that declares itself a learning scenario, on any one of: `fit samples
    == 0` for some model, no `[[profile]]` in `calibration.after.toml`, a peak
    `unit_budget` no higher than the first recorded. See the README's "Checks".
    """
    learning = _declared_learning(ctx)
    profiles = (ctx.after or {}).get("profile") or []
    rows = _budget_rows(ctx)

    reasons: List[str] = []
    if not rows:
        reasons.append("no worker appears in any health sample"
                       if ctx.health_samples else
                       "no healthrec.jsonl, so no unit_budget or fit samples "
                       "could be read")
    else:
        no_fit = sorted(model for model, row in rows.items()
                        if row["fit_samples"] == 0)
        if no_fit:
            reasons.append("fit samples == 0 for " + ", ".join(no_fit))
        stuck = sorted(f"{model} (seed {rows[model]['first']}, peak "
                       f"{rows[model]['peak']})"
                       for model in rows
                       if rows[model]["peak"] <= rows[model]["first"])
        if stuck:
            reasons.append("peak unit_budget never left the seed for "
                           + ", ".join(stuck))
    if ctx.after is None:
        reasons.append("no calibration.after.toml in the scenario directory")
    elif not profiles:
        reasons.append("calibration.after.toml carries no [[profile]]")

    detail = "; ".join(
        f"{model}: unit_budget {row['first']} -> peak {row['peak']} "
        f"(last {row['last']}, fit samples {row['fit_samples']})"
        for model, row in rows.items()
    ) or "no worker series"
    detail += f"; {len(profiles)} profile(s) in the store"
    if reasons:
        detail += "  -- NOTHING WAS LEARNED: " + "; ".join(reasons)
    if learning:
        verdict = "FAIL" if reasons else "PASS"
    else:
        verdict = "INFO"
        detail += ("  [report-only: this leg did not declare itself a learning "
                   "scenario -- pass --learning, or name calibration_learned "
                   "in --checks, to make it a verdict]")
    return Verdict("calibration_learned", verdict, detail,
                   {"models": rows, "profiles": len(profiles),
                    "learning": learning, "reasons": reasons})


def check_peak_fds(ctx: Context) -> Verdict:
    """Peak open descriptors for the gateway process, if anything recorded them.

    Report-only: with local inference every in-flight predict is loopback HTTP
    inside one process and so costs *two* sockets in one descriptor table. No
    recorder here produces the input; see the README's "Recording file
    descriptors".
    """
    rows = ctx.fds
    if not rows:
        # Second-chance sources, in case a future recorder carries the count.
        for sample in ctx.health_samples:
            value = (sample.get("health") or {}).get("open_fds")
            if value is not None:
                rows = rows or []
                rows.append({"iso": sample["iso"], "t_wall": sample["t_wall"],
                             "fds": int(value), "sockets": None, "limit": None})
    if not rows:
        for sample in ctx.vram_samples:
            for proc in sample.get("procs", []) or []:
                value = proc.get("num_fds")
                if value is not None:
                    rows.append({"iso": sample["iso"], "t_wall": sample["t_wall"],
                                 "fds": int(value), "sockets": None,
                                 "limit": None})
    if not rows:
        return Verdict(
            "peak_fds", "SKIP",
            "no fds.jsonl / fdrec.txt in the scenario and no descriptor count "
            "in healthrec or vramrec -- record it per the README "
            '("Recording file descriptors") on any run that reaches a '
            "unit_budget above ~100, and on every containerised run")
    peak = max(int(row["fds"]) for row in rows)
    sockets = [int(row["sockets"]) for row in rows if row.get("sockets") is not None]
    limits = [int(row["limit"]) for row in rows if row.get("limit") is not None]
    peak_sockets = max(sockets) if sockets else None
    limit = min(limits) if limits else None
    detail = f"peak {peak} open descriptors over {len(rows)} samples"
    if peak_sockets is not None:
        detail += f", {peak_sockets} of them sockets at the peak of that series"
    if limit is not None:
        detail += (f"; soft limit {limit} "
                   f"({_pct(peak, limit):.0f}% of it)")
        if peak >= limit:
            detail += " -- AT THE LIMIT: expect EMFILE (F6)"
    return Verdict("peak_fds", "INFO", detail,
                   {"peak_fds": peak, "peak_sockets": peak_sockets,
                    "soft_limit": limit, "samples": len(rows)})


CHECKS: Dict[str, Callable[[Context], Verdict]] = {
    "oracle_agreement": check_oracle_agreement,
    "base_accuracy": check_base_accuracy,
    "footprint_agreement": check_footprint_agreement,
    "slope_accuracy": check_slope_accuracy,
    "grant_safety": check_grant_safety,
    "failures": check_failures,
    "deflation_recovery": check_deflation_recovery,
    "idle_liveness": check_idle_liveness,
    "utilization": check_utilization,
    "throughput": check_throughput,
    "persistence": check_persistence,
    "job_outcome": check_job_outcome,
    "ledger_invariant": check_ledger_invariant,
    "peak_fds": check_peak_fds,
    "hog_tracking": check_hog_tracking,
    "ramp_progress": check_ramp_progress,
    "calibration_learned": check_calibration_learned,
}


# --- Plot (optional) -------------------------------------------------------


def make_plot(ctx: Context, path: Path) -> str:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        return f"skipped ({type(exc).__name__}: matplotlib not available)"
    figure, axes = plt.subplots(3, 1, figsize=(13, 9), sharex=True)
    base = None
    for row in ctx.vram_samples:
        base = row["t_wall"]
        break
    if base is None:
        return "skipped (no vramrec samples)"

    gpus: Dict[str, List[Tuple[float, int]]] = {}
    for row in ctx.vram_samples:
        for gpu in row.get("gpus", []):
            if gpu.get("used_mb") is not None:
                gpus.setdefault(gpu["uuid"], []).append(
                    (row["t_wall"] - base, gpu["used_mb"]))
    for uuid, points in gpus.items():
        axes[0].plot([p[0] for p in points], [p[1] for p in points],
                     label=f"oracle used {uuid[:16]}", linewidth=1)
    if ctx.hog_samples:
        axes[0].plot([row["t_wall"] - base for row in ctx.hog_samples],
                     [row.get("held_mb") or 0 for row in ctx.hog_samples],
                     label="hog held", linewidth=1, linestyle="--")
    axes[0].set_ylabel("MiB")
    axes[0].legend(fontsize=7)
    axes[0].set_title("GPU memory")

    series: Dict[str, List[Tuple[float, Any]]] = {}
    for sample in ctx.health_samples:
        for gpu in health_gpus(sample.get("health")):
            for key in ("external_mb", "headroom_mb", "grants_mb"):
                series.setdefault(f"{key} {GPU.get('gpu_uuid', '')[:12]}", []).append(
                    (sample["t_wall"] - base, gpu.get(key)))
    for label, points in series.items():
        axes[1].plot([p[0] for p in points], [p[1] for p in points],
                     label=label, linewidth=1)
    axes[1].set_ylabel("MiB")
    axes[1].legend(fontsize=7)
    axes[1].set_title("ledger view")

    budgets: Dict[str, List[Tuple[float, int]]] = {}
    deflations: Dict[str, List[Tuple[float, int]]] = {}
    for sample in ctx.health_samples:
        for worker in (sample.get("health") or {}).get("workers") or []:
            budgets.setdefault(worker["inference_id"], []).append(
                (sample["t_wall"] - base, worker.get("unit_budget")))
            deflations.setdefault(worker["inference_id"], []).append(
                (sample["t_wall"] - base, worker.get("deflation")))
    for label, points in budgets.items():
        axes[2].plot([p[0] for p in points], [p[1] for p in points],
                     label=f"unit_budget {label}", linewidth=1)
    for label, points in deflations.items():
        axes[2].plot([p[0] for p in points], [p[1] for p in points],
                     label=f"deflation {label}", linewidth=1, linestyle=":")
    for event in ctx.log:
        if event["level"] == "WARN" and event["message"] == "settled a granted window":
            if event["t_wall"]:
                axes[2].axvline(event["t_wall"] - base, color="red", alpha=0.3,
                                linewidth=0.8)
    axes[2].set_ylabel("units")
    axes[2].set_xlabel("seconds since the first oracle sample")
    axes[2].legend(fontsize=7)
    axes[2].set_title("admission (red = negative sample)")

    figure.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=110)
    return f"wrote {path}"


# --- CLI -------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Join one scenario's recordings and print the §6 verdict table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--scenario", help="directory with the standard file names")
    parser.add_argument("--vramrec")
    parser.add_argument("--healthrec")
    parser.add_argument("--hog")
    parser.add_argument("--log", dest="logfile")
    parser.add_argument("--before", help="calibration.before.toml")
    parser.add_argument("--after", help="calibration.after.toml")
    parser.add_argument("--jobs", help="jobs.json")
    parser.add_argument("--probe", action="append", default=[],
                        help="ceiling_probe.py result (repeatable)")
    parser.add_argument("--checks", default="all",
                        help="comma-separated check names, or `all`")
    parser.add_argument("--list-checks", action="store_true")
    parser.add_argument("--learning", action="store_true",
                        help="this leg is a learning / cold-ramp scenario and "
                             "is meant to end with a fitted profile on disk. "
                             "Naming calibration_learned in --checks declares "
                             "the same thing. Makes calibration_learned a "
                             "verdict, and makes the 'no store was written' "
                             "branch of slope_accuracy / utilization / "
                             "persistence FAIL instead of WARN")
    parser.add_argument("--expect-ooms", type=int, default=0)
    parser.add_argument("--expect-deaths", type=int, default=0)
    parser.add_argument("--expect-failures", type=int, default=0)
    parser.add_argument("--expect-failed-jobs", type=int, default=0,
                        help="whole jobs whose outcome is meant to be a "
                             "failure (S4g and the load-failure fixtures)")
    parser.add_argument("--baseline-jobs")
    parser.add_argument("--baseline-items-per-s", type=float, default=None)
    parser.add_argument("--throughput-floor", type=float, default=0.9)
    parser.add_argument("--utilization-floor", type=float, default=0.25)
    parser.add_argument("--idle-window", type=float, default=60.0)
    parser.add_argument("--join-tolerance", type=float, default=1.5)
    parser.add_argument("--base-window", type=float, default=10.0,
                        help="max |dt| between a worker admission and the "
                             "oracle sample base_accuracy compares against")
    parser.add_argument("--worker-pattern", default="inferio_worker")
    parser.add_argument("--json", dest="json_out")
    parser.add_argument("--plot")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    # Which checks did the scenario name for itself? `all` is the default and
    # declares nothing (see `_declared_learning`).
    args.explicit_checks = (
        set()
        if args.checks.strip() == "all"
        else {name.strip() for name in args.checks.split(",") if name.strip()}
    )

    if args.list_checks:
        for name, fn in CHECKS.items():
            summary = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"{name:22s} {summary}")
        print()
        print("The check that decides safety is `grant_safety`, and within it "
              "the oracle clause:")
        print("  every `issued a memory grant` line is joined to "
              "vramrec.jsonl and the grant is")
        print("  compared with the GPU's LIVE FREE MEMORY at that instant. "
              "That clause needs")
        print("  vramrec.jsonl; without it grant_safety reports WARN, never "
              "PASS, so a silently")
        print("  skipped safety clause is visible in the table. "
              "`ledger_invariant`'s strict form is")
        print("  not a substitute -- it passed on a ledger whose `external` "
              "was hard-zeroed (0 of")
        print("  498 GPU-samples over the limit) while this clause caught "
              "335 of 335 grants")
        print("  (run1 S15 mutation 2).")
        print()
        print("SKIP means an input was not recorded, never that the run "
              "produced no measurement:")
        print("  `slope_accuracy`, `utilization` and `persistence` report "
              "WARN when the store was")
        print("  never written, and FAIL when the leg declares itself a "
              "learning scenario with")
        print("  --learning (or by naming `calibration_learned` in --checks).")
        return 0

    root = Path(args.scenario).resolve() if args.scenario else None

    def pick(explicit: Optional[str], default_name: str) -> Optional[Path]:
        if explicit:
            return Path(explicit)
        if root is None:
            return None
        candidate = root / default_name
        return candidate if candidate.exists() else None

    probes = []
    probe_paths = [Path(path) for path in args.probe]
    if root is not None and not probe_paths:
        probe_paths = sorted(root.glob("probe*.json"))
    for path in probe_paths:
        payload = read_json(path)
        if payload:
            probes.append(payload)

    ctx = Context(
        args=args,
        vramrec=read_jsonl(pick(args.vramrec, "vramrec.jsonl")),
        healthrec=read_jsonl(pick(args.healthrec, "healthrec.jsonl")),
        hog=read_jsonl(pick(args.hog, "hog.jsonl")),
        log=parse_log(pick(args.logfile, "panoptikon.log")),
        before=read_toml(pick(args.before, "calibration.before.toml")),
        after=read_toml(pick(args.after, "calibration.after.toml")),
        jobs=read_json(pick(args.jobs, "jobs.json")),
        probes=probes,
        fds=read_fds(pick(None, "fds.jsonl")) or read_fds(pick(None, "fdrec.txt")),
    )

    selected = (
        list(CHECKS)
        if args.checks.strip() == "all"
        else [name.strip() for name in args.checks.split(",") if name.strip()]
    )
    unknown = [name for name in selected if name not in CHECKS]
    if unknown:
        parser.error(f"unknown check(s): {', '.join(unknown)}")

    verdicts = [CHECKS[name](ctx) for name in selected]

    if not args.quiet:
        print(f"scenario: {root or '(explicit paths)'}")
        print(f"inputs:   vramrec {len(ctx.vram_samples)} samples, "
              f"healthrec {len(ctx.health_samples)}, hog {len(ctx.hog_samples)}, "
              f"log {len(ctx.log)} events, probes {len(ctx.probes)}, "
              f"profiles after {len((ctx.after or {}).get('profile') or [])}")
        print()
    width = max(len(verdict.name) for verdict in verdicts) if verdicts else 10
    print(f"{'CHECK'.ljust(width)}  VERDICT  DETAIL")
    print(f"{'-' * width}  -------  {'-' * 60}")
    for verdict in verdicts:
        print(f"{verdict.name.ljust(width)}  {verdict.verdict:<7}  {verdict.detail}")

    if args.plot:
        note = make_plot(ctx, Path(args.plot))
        if not args.quiet:
            print(f"\nplot: {note}")

    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(
            json.dumps(
                {
                    "scenario": str(root) if root else None,
                    "inputs": {
                        "vramrec_samples": len(ctx.vram_samples),
                        "healthrec_samples": len(ctx.health_samples),
                        "hog_samples": len(ctx.hog_samples),
                        "log_events": len(ctx.log),
                        "probes": len(ctx.probes),
                    },
                    "learning": _declared_learning(ctx),
                    "verdicts": [
                        {"name": v.name, "verdict": v.verdict, "detail": v.detail,
                         "numbers": v.numbers}
                        for v in verdicts
                    ],
                },
                indent=1, default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        if not args.quiet:
            print(f"json: wrote {args.json_out}")

    return 1 if any(verdict.verdict == "FAIL" for verdict in verdicts) else 0


if __name__ == "__main__":
    raise SystemExit(main())
