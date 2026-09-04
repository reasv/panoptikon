#!/usr/bin/env python3
"""analyze.py - join one scenario's recordings and print the verdict table.

Implements the verdict table of `docs/batch-calibration-test-protocol.md` §6.
It joins, by wall-clock timestamp:

    vramrec.jsonl         the independent NVML/RAM oracle
    healthrec.jsonl       the gateway's own ledger view
    hog.jsonl             what the external pressure generator actually held
    fds.jsonl             optional descriptor recording for the gateway pid
                          (`{"t_wall"|"iso", "fds", "sockets"[, "limit"]}`;
                          the plain `<iso> fds=N sockets=M` form Phase 6 used
                          is read too)
    panoptikon.log        the ledger's structured log lines (commit 49822c8b)
    calibration.before/after.toml   the persisted cost profiles
    jobs.json             `/api/jobs/data/history` (LogRecord) and/or
                          `/api/jobs/queue` output
    probe*.json           one or more ceiling_probe.py results

Usage
-----
    analyze.py --scenario results/<run>/<scenario>
    analyze.py --scenario DIR --checks oracle_agreement,grant_safety,failures
    analyze.py --scenario DIR --json verdicts.json --plot timeline.png
    analyze.py --list-checks

    # any file can be overridden individually
    analyze.py --vramrec a.jsonl --healthrec b.jsonl --log c.log --probe p.json

Options:
    --scenario DIR        directory holding the standard file names
    --checks LIST         comma-separated check names, or `all` (default: all
                          checks whose inputs are present). A scenario declares
                          which verdicts apply by passing this list.
    --learning            this leg is a learning / cold-ramp scenario: it is
                          *meant* to end with a fitted profile on disk. Naming
                          `calibration_learned` in `--checks` declares the same
                          thing. Under that declaration `calibration_learned`
                          FAILs (rather than reporting) when nothing was
                          learned, and the "no store was written" branch of
                          `slope_accuracy`, `utilization` and `persistence`
                          FAILs rather than WARNs. See "Declaring a learning
                          leg" below.
    --expect-ooms N       the scenario deliberately provokes N OOMs   (default 0)
    --expect-deaths N     ... and N worker deaths                     (default 0)
    --expect-failures N   ... and N failed items                      (default 0)
    --expect-failed-jobs N  ... and N whole *jobs* that end not-`completed`.
                          A scenario whose job is supposed to fail as a whole
                          (S4g's "no room to load", a load-failure fixture)
                          would otherwise FAIL `job_outcome` for succeeding at
                          its own point                                (default 0)
    --baseline-jobs PATH  a C0 `jobs.json` for the throughput comparison
    --baseline-items-per-s F   or the number directly
    --idle-window S       trailing seconds treated as "idle" for the
                          liveness/recovery checks                  (default 60)
    --join-tolerance S    max |dt| when joining two recordings      (default 1.5)
    --base-window S       max |dt| between a worker's admission and the oracle
                          sample `base_accuracy` compares it against (default 10)
    --throughput-floor F  ratio of the C0 baseline that passes      (default 0.9)
    --utilization-floor F ratio of the probe boundary that passes  (default 0.25)
    --worker-pattern RE   which vramrec PIDs are ours
                          (default `inferio_worker`)
    --json PATH           machine-readable verdicts
    --plot PATH           PNG timeline (skipped, with a note, if matplotlib is
                          not importable -- it is never required)
    --quiet               table only

Exit code is 1 if any selected check FAILs, else 0.

Verdicts
--------
    PASS  the threshold held               FAIL  it did not
    WARN  close to a threshold, or the scenario expected the deviation
    INFO  measured and reported, never judged (report-only rows in §6)
    SKIP  the inputs for this check were not present

Every row prints the numbers behind the verdict, so a threshold that is missed
by a small margin can be adjudicated by a human rather than by this script.

SKIP is never a result
----------------------
`SKIP` means *the harness did not record the input*. It must never be the
answer to "the run produced no measurement", because a fault that destroys the
measurement usually destroys the evidence with it, and a SKIP never sets the
exit code. Run1's S15 mutation 1 (the worker halving its reported
`peak_reserved_mb`) learned nothing at all, wrote no store, and reported
`slope_accuracy SKIP` + `persistence SKIP` -- all green but for one row that
only existed because that leg happened to be run with `--probe`. So:

* "the store was never written" is a **result**: WARN, or FAIL under
  `--learning`.
* "no probe file / no log / no recording was given to me" is a **harness
  omission**: SKIP, with a pointer to what to pass.
* `calibration_learned` turns the three numbers `ramp_progress` already prints
  into a verdict, so a leg that stopped measuring cannot come back green.

The check that decides safety
-----------------------------
It is `grant_safety`, and specifically its **second clause**: every
`issued a memory grant` line is joined to the vramrec oracle and the grant is
compared with the board's *live free memory* at that instant. That clause needs
`vramrec.jsonl`; without it `grant_safety` reports **WARN**, never PASS, so the
silently-skipped clause is visible in the table. `ledger_invariant`'s strict
form is not a substitute: it passes on a completely broken ledger (run1 S15
mutation 2 zeroed `external`, making `limit_mb == total_mb`, and 0 of 498
board-samples were over the limit while 335 of 335 grants exceeded the oracle's
live free memory).
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

# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


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
# `docker logs` hands back exactly what the process wrote, and the gateway
# writes ANSI colour to a terminal-less stdout, so a container leg's log is
# full of `\x1b[32m` runs that break LOG_LINE on the very first field (the
# level). Stripping them here rather than in every caller is what keeps a
# Docker scenario from silently reporting `log 0 events` and three green
# SKIPs where it should have three PASSes (run1 Phase 7b, S11-C4-fixed).
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def parse_log(path: Optional[Path]) -> List[Dict[str, Any]]:
    """Parse `tracing_subscriber`'s default text format.

    A line is `<rfc3339> <LEVEL> <target>: <message> k=v k="v" ...`; the
    message is everything before the first `k=` token. ANSI escapes are
    stripped first, so a raw `docker logs` capture parses like a file sink.
    """
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

    No tool in this directory records them: Phase 6 needed the number after
    the fact (finding F6, the container `nofile` blocker) and sampled
    `/proc/<pid>/fd` from a shell loop into `fdrec.txt`. Both that plain form
    (`<iso> fds=N sockets=M [limit=N]`) and a JSONL form with the same keys
    are accepted, so a scenario can produce whichever is cheaper. See the
    README's "Recording file descriptors" for the recipe and the gap.
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


# --------------------------------------------------------------------------
# Context
# --------------------------------------------------------------------------


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

    # -- joining ----------------------------------------------------------
    def vram_at(self, t_wall: float) -> Optional[Dict[str, Any]]:
        return _nearest(self.vram_samples, self._vram_times, t_wall,
                        self.args.join_tolerance)

    def hog_at(self, t_wall: float) -> Optional[Dict[str, Any]]:
        return _nearest(self.hog_samples, self._hog_times, t_wall,
                        self.args.join_tolerance)

    # -- oracle -----------------------------------------------------------
    def oracle_board(self, sample: Dict[str, Any], uuid: str) -> Optional[Dict[str, Any]]:
        for board in sample.get("gpus", []):
            if board.get("uuid") == uuid:
                return board
        return None

    def our_pids_mb(self, board: Dict[str, Any]) -> Tuple[int, List[int]]:
        """Sum of NVML per-process usage for PIDs that are our workers.

        A PID is ours on any of three routes: the gateway's own
        `spawned an inferio worker ... pid=Some(N)` line named it; its recorded
        cmdline matches `--worker-pattern`; or its recorded environ carries
        BOTH `INFERIO_WORKER` and `PANOPTIKON_DEVICE_PIN` -- the pair the
        orchestrator sets on a spawned worker (`worker.rs:597-681`).
        `ceiling_probe.py` sets the pin alone, and `hog.py` neither, so neither
        is mistaken for a resident.

        The log route exists because the other two believe the recording. A
        worker first sighted by NVML inside its own fork/exec window used to be
        recorded with the `[comm]` cmdline of the spawning helper and an empty
        env for its whole life (run1/S9: `"[panoptikon-spaw]"`, 815 of 815
        samples), which made a resident nemotron worker holding up to 66 GiB
        count as *external* here and, in `base_accuracy`, handed the row a
        different worker's process. `vramrec.py`'s `ProcCache` no longer
        memoizes that negative, but the log route is what lets a recording
        already on disk -- run1's included -- be re-analysed correctly, and it
        is the route that survives any future `/proc` read failure.
        """
        total = 0
        pids: List[int] = []
        for proc in board.get("procs", []):
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


def _worker_spawns(log: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every `spawned an inferio worker` line, with the model it went on to be.

    The spawn line names the impl class (`worker=nemotron-embed-vl`) and the
    OS pid (`pid=Some(1998478)`), not the inference id. The worker itself logs
    `Configured as <inference id>` a moment later under the same `worker=`
    field, so pairing the two in order -- FIFO per impl class -- names the
    model behind each pid. A spawn with no such line stays `model: None` and
    is only ever used as "this pid is one of ours", never as evidence about
    which model it is.
    """
    spawns: List[Dict[str, Any]] = []
    pending: Dict[str, List[Dict[str, Any]]] = {}
    for event in log:
        worker = str(event["fields"].get("worker"))
        if event["message"] == "spawned an inferio worker":
            match = SPAWN_PID.search(str(event["fields"].get("pid", "")))
            if match is None or event["t_wall"] is None:
                continue
            spawn = {"pid": int(match.group(1)), "worker": worker,
                     "t_wall": event["t_wall"], "model": None}
            spawns.append(spawn)
            pending.setdefault(worker, []).append(spawn)
            continue
        index = event["message"].find(CONFIGURED_AS)
        if index < 0:
            continue
        queue = pending.get(worker)
        if not queue:
            continue
        queue.pop(0)["model"] = event["message"][index + len(CONFIGURED_AS):].strip()
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

    `_nearest` is wrong for a quantity that is still *rising* at `target`.
    NVML's per-process figure climbs throughout a model load and the worker
    reports `base_mb` only once the load has finished, so an oracle sample
    taken 60 ms *before* the admission line can be hundreds of MiB short while
    the sample 190 ms after it agrees to within the driver's own 8-10 MiB
    per-process offset. Measured in run1/S1: 812 MiB at 05:53:38.108 against a
    reported 964, and 974 MiB at 05:53:38.353.
    """
    if not rows:
        return None
    index = bisect.bisect_left(times, target)
    if index < len(rows) and times[index] - target <= tolerance:
        return rows[index]
    return _nearest(rows, times, target, tolerance)


def _pct(value: float, of: float) -> float:
    return 100.0 * value / of if of else float("inf")


# The hog is judged to have been held "long enough for the ledger to have no
# excuse" after this many seconds. `ledger.rs` refreshes `external` at grant
# time with a 10 s staleness window and nothing polls, so the honest bound is
# the staleness window plus one admission window; a window can be tens of
# seconds under load (run1 measured `external_sample_age_ms` of 85.5 s with a
# resident and 166.9 s overall, finding B2/T3). 60 s is the practical
# threshold: it is far above any staleness window seen in run1 and far below
# the length of any hog hold a scenario sets up, so a *late* update still
# reports INFO and only a board that never moved at all FAILs.
HOG_STALL_SECONDS = 60.0

# A hold this large is unambiguous pressure: no allocator jitter reaches it.
HOG_STALL_MB = 1024


def _declared_learning(ctx: "Context") -> bool:
    """Did this leg declare itself a learning / cold-ramp scenario?

    Two equivalent declarations, so a scenario need not repeat itself: the
    explicit `--learning` flag, or naming `calibration_learned` in `--checks`
    (`--checks all` does not count -- it is the default and declares nothing).
    """
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
    """Per-model `unit_budget` over time and the best `fit_samples` seen.

    The single source for `ramp_progress` and `calibration_learned`, so the
    verdict and the report-only row can never disagree about the numbers.
    """
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


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------


def check_oracle_agreement(ctx: Context) -> Verdict:
    """`external_mb` vs (board used - our workers' NVML usage): +/-1 GiB or 2%."""
    if not ctx.health_samples or not ctx.vram_samples:
        return Verdict("oracle_agreement", "SKIP",
                       "needs both healthrec.jsonl and vramrec.jsonl")
    joined = 0
    worst = 0.0
    worst_row: Dict[str, Any] = {}
    breaches = 0
    per_board: Dict[str, float] = {}
    for sample in ctx.health_samples:
        health = sample.get("health") or {}
        if not health.get("ok"):
            continue
        vram = ctx.vram_at(sample["t_wall"])
        if vram is None:
            continue
        for board in health.get("boards") or []:
            uuid = board.get("gpu_uuid")
            oracle = ctx.oracle_board(vram, uuid)
            if oracle is None or oracle.get("used_mb") is None:
                continue
            if not board.get("external_known"):
                continue
            ours, _ = ctx.our_pids_mb(oracle)
            oracle_external = max(0, int(oracle["used_mb"]) - ours)
            delta = abs(int(board.get("external_mb") or 0) - oracle_external)
            total = int(board.get("total_mb") or oracle.get("total_mb") or 0)
            allowance = max(1024.0, 0.02 * total)
            joined += 1
            per_board[uuid] = max(per_board.get(uuid, 0.0), float(delta))
            if delta > worst:
                worst = float(delta)
                worst_row = {
                    "iso": sample["iso"], "gpu": uuid,
                    "external_mb": board.get("external_mb"),
                    "oracle_external_mb": oracle_external,
                    "board_used_mb": oracle.get("used_mb"),
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
        f"board-samples; {breaches} outside the allowance",
        {"joined": joined, "breaches": breaches, "worst_mb": worst,
         "per_board_worst_mb": per_board, "worst_sample": worst_row},
    )


def check_base_accuracy(ctx: Context) -> Verdict:
    """`base_mb` vs the oracle's per-process usage at load time: +/-10% (nvml).

    The comparison is anchored at the moment the worker was admitted, not at an
    arbitrary later sample: NVML's per-process figure includes allocator pool
    growth, so after the first window it measures base + pool, and only the
    load-time reading is comparable to `base_mb`.
    """
    if not ctx.health_samples or not ctx.vram_samples:
        return Verdict("base_accuracy", "SKIP", "needs healthrec and vramrec")

    # When was each replica admitted? The ledger's own log line is exact; the
    # first health sample that shows it is the fallback.
    admitted: Dict[Tuple[str, str], float] = {}
    for event in ctx.log_events("admitted a worker to a board's ledger"):
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

    rows: List[Dict[str, Any]] = []
    for (model, uuid), info in seen.items():
        vram = _at_or_after(ctx.vram_samples, ctx._vram_times, info["t_wall"],
                            ctx.args.base_window)
        if vram is None:
            rows.append({"model": model, "gpu": uuid, **info,
                         "note": "no oracle sample near the admission"})
            continue
        oracle = ctx.oracle_board(vram, uuid)
        if oracle is None:
            rows.append({"model": model, "gpu": uuid, **info,
                         "note": "board absent from the oracle sample"})
            continue
        ours, pids = ctx.our_pids_mb(oracle)
        if len(pids) != 1:
            rows.append({"model": model, "gpu": uuid, **info, "pids": pids,
                         "oracle_sum_mb": ours,
                         "note": f"{len(pids)} worker PIDs on the board: "
                                 "attribution is ambiguous"})
            continue
        # The lifetime minimum is a second, weaker estimate of the same thing.
        floor = None
        for sample in ctx.vram_samples:
            board = ctx.oracle_board(sample, uuid)
            if board is None:
                continue
            for proc in board.get("procs", []):
                if proc["pid"] == pids[0] and proc.get("used_mb"):
                    floor = proc["used_mb"] if floor is None else min(floor, proc["used_mb"])
        error = abs(info["base_mb"] - ours)
        rows.append({"model": model, "gpu": uuid, **info, "pid": pids[0],
                     "oracle_pid_mb": ours, "oracle_pid_min_mb": floor,
                     "error_mb": error,
                     # How far after the admission the oracle sample sits. A
                     # model that loads and runs its first batch inside one
                     # sample period cannot be resolved: the sample then
                     # carries the batch's cuBLAS/cuDNN workspace as well as
                     # the base, and the row is a comment on the cadence, not
                     # on the ledger (run1/S14: MiniLM, 654 vs 774).
                     "oracle_dt_ms": round((vram["t_wall"] - info["t_wall"]) * 1000.0),
                     "error_pct": round(_pct(error, max(1, ours)), 2)})

    judged = [row for row in rows if row.get("error_pct") is not None
              and row.get("base_method") == "nvml"]
    reported = [row for row in rows if row.get("error_pct") is not None]
    if not reported:
        return Verdict("base_accuracy", "SKIP",
                       "; ".join(f"{row['model']}: {row.get('note')}" for row in rows),
                       {"rows": rows})
    detail = "; ".join(
        f"{row['model']} base_mb={row['base_mb']} ({row['base_method']}) vs oracle "
        f"PID {row['oracle_pid_mb']} MiB at admission+{row['oracle_dt_ms']}ms = "
        f"{row['error_pct']}% (lifetime min {row['oracle_pid_min_mb']})"
        for row in reported
    )
    if not judged:
        return Verdict("base_accuracy", "INFO",
                       detail + "  [report-only: base_method is not nvml]",
                       {"rows": rows})
    worst = max(judged, key=lambda row: row["error_pct"])
    verdict = "PASS" if worst["error_pct"] <= 10.0 else "FAIL"
    return Verdict("base_accuracy", verdict, detail + "  [threshold 10%]",
                   {"rows": rows, "worst": worst})


def check_footprint_agreement(ctx: Context) -> Verdict:
    """Per board: `footprints_mb` vs the summed NVML usage of our PIDs."""
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
        for board in health.get("boards") or []:
            oracle = ctx.oracle_board(vram, board.get("gpu_uuid"))
            if oracle is None:
                continue
            ours, pids = ctx.our_pids_mb(oracle)
            if not pids:
                continue
            joined += 1
            delta = abs(int(board.get("footprints_mb") or 0) - ours)
            if delta > worst:
                worst = float(delta)
                worst_row = {"iso": sample["iso"], "gpu": board.get("gpu_uuid"),
                             "footprints_mb": board.get("footprints_mb"),
                             "oracle_our_pids_mb": ours, "pids": pids}
    if joined == 0:
        return Verdict("footprint_agreement", "SKIP", "no worker PID seen on any board")
    return Verdict("footprint_agreement", "INFO",
                   f"worst |footprints_mb - Sum(our PIDs)| = {worst:.0f} MiB "
                   f"over {joined} board-samples (report-only: footprints "
                   f"exclude pool growth the grant already counts)",
                   {"joined": joined, "worst_mb": worst, "worst_sample": worst_row})


def check_slope_accuracy(ctx: Context) -> Verdict:
    """Persisted slope vs ceiling_probe's: -30% .. +100%.

    The two ways this check cannot run are *not* the same thing and no longer
    share a verdict (run1 S15 hole H2). "No store was written" is what the run
    did; "no probe file was passed" is what the harness forgot.
    """
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

    The second clause is the one with teeth, and it is the only one: it joins
    each grant to `vramrec.jsonl` and asks whether the grant exceeded the
    board's *live free memory*. `ledger_invariant`'s strict form cannot stand
    in for it -- run1's S15 mutation 2 zeroed `external`, so `limit_mb` became
    `total_mb`, `ledger_invariant` passed on 0 of 498 breaches, and this clause
    caught 335 of 335 grants over the oracle's free memory.

    Without `vramrec.jsonl` that clause silently does not run, so the check
    reports **WARN** rather than PASS: a scenario must never read as "safety
    verified" on the priced-headroom clause alone, which only re-checks the
    ledger's own arithmetic against itself.
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
            oracle = ctx.oracle_board(vram, fields.get("gpu"))
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
        # The oracle clause never ran. This is the clause that decides safety,
        # so the row must not read PASS (run1 S15 hole H4).
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
    """OOM negatives, worker deaths and merged-window fallbacks in the log."""
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
    unified_deaths = reasons.get("unified_board_death", 0)
    deaths = len(ctx.log_matching("worker died fatally"))
    fallbacks = ctx.log_matching("falling back to per-request prediction")
    oom_fallbacks = sum(1 for event in fallbacks if event["fields"].get("oom") is True)
    expected_ooms = ctx.args.expect_ooms
    expected_deaths = ctx.args.expect_deaths
    bad = ooms > expected_ooms or deaths > expected_deaths
    verdict = "FAIL" if bad else ("WARN" if (ooms or deaths) else "PASS")
    return Verdict(
        "failures", verdict,
        f"{ooms} OOM negatives (expected <= {expected_ooms}), "
        f"{collapses} throughput-collapse negatives, "
        f"{unified_deaths} unified-board death negatives, "
        f"{deaths} fatal worker deaths (expected <= {expected_deaths}), "
        f"{len(fallbacks)} merged-window fallbacks ({oom_fallbacks} OOM)",
        {"negative_reasons": reasons, "worker_deaths": deaths,
         "fallbacks": len(fallbacks), "oom_fallbacks": oom_fallbacks,
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
            int(board.get("grants_outstanding") or 0)
            for board in health.get("boards") or []
        )
        if outstanding and not pending:
            busy.append({"iso": sample["iso"], "grants_outstanding": outstanding})
    last = tail[-1]
    final = sum(int(board.get("grants_outstanding") or 0)
                for board in (last.get("health") or {}).get("boards") or [])
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

    Same H2 split as `slope_accuracy`: "no worker was ever admitted" is a
    result, "no probe boundary was passed" is a harness omission.
    """
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
        # Nothing to divide by: a missing input, so SKIP with the pointer --
        # never a green row that looks like a measurement.
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

    Same H2 split again: an absent or empty store is a result (WARN, FAIL under
    `--learning`); an absent *log* is a harness omission (SKIP with a pointer).
    """
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
    failed = sum(int(record.get("failed") or 0) for record in records)
    errors = sum(int(record.get("errors") or 0) for record in records)
    completed = sum(int(record.get("completed") or 0) for record in records)
    bad_outcomes = [row for row in queue_outcomes
                    if row.get("status") not in (None, "completed")]
    over = failed > ctx.args.expect_failures
    # A scenario can declare that a whole job is *meant* to fail -- S4g asks a
    # 2.5 GB model to load onto a board with 1 GB free, and the correct
    # behaviour is a failed job with a readable per-model error. Without this
    # knob such a scenario reports `job_outcome FAIL` for doing exactly what it
    # set out to do (Phase 3, tool note 1).
    expected_bad = ctx.args.expect_failed_jobs
    over_jobs = len(bad_outcomes) > expected_bad
    verdict = "FAIL" if (over or over_jobs) else "PASS"
    return Verdict(
        "job_outcome", verdict,
        f"{len(records)} job record(s): {completed} completed items, "
        f"{failed} failed (expected <= {ctx.args.expect_failures}), "
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
    """The admission invariant, in both of the forms run1 showed it has.

    **Strict form** (as §6 states it): on every board sample, the sum of our
    charges plus our load reservations is at most `limit_mb`.

    **"Our own residents" form** (findings T6 and P5-2): the strict form
    *cannot* hold once the board is nearly full, and not because anything
    over-committed. `limit = total - external x (1 + margin)` charges the
    margin against the neighbour's level, so the limit reaches **0** while a
    model we already loaded legitimately holds gigabytes -- measured at
    `limit_mb = 2813` with 10 GB free and `0` with 4 GB free, with our own
    residents holding 1.2-3.8 GB, and nothing in the design would unload
    them. What still has to hold there is the form the ledger actually
    enforces: **a grant never exceeds the headroom it was priced against, nor
    the oracle's live free memory** (that is `grant_safety`, reported here
    too so the two are read together).

    So a breach in a sample whose `limit_mb` is 0 is arithmetic, not
    over-commitment, and this check reports it as **WARN**; a breach against a
    non-zero limit is a real violation and FAILs.
    """
    if not ctx.health_samples:
        return Verdict("ledger_invariant", "SKIP", "no healthrec.jsonl")
    breaches = []
    zero_limit_breaches = []
    checked = 0
    zero_limit_samples = 0
    for sample in ctx.health_samples:
        for board in (sample.get("health") or {}).get("boards") or []:
            limit = board.get("limit_mb")
            if limit is None:
                continue
            checked += 1
            if int(limit) == 0:
                zero_limit_samples += 1
            used = int(board.get("charges_mb") or 0) + int(
                board.get("load_reservations_mb") or 0)
            if used > int(limit):
                row = {"iso": sample["iso"], "gpu": board.get("gpu_uuid"),
                       "charges_mb": board.get("charges_mb"),
                       "load_reservations_mb": board.get("load_reservations_mb"),
                       "limit_mb": limit,
                       "external_mb": board.get("external_mb")}
                (zero_limit_breaches if int(limit) == 0 else breaches).append(row)
    if checked == 0:
        return Verdict("ledger_invariant", "SKIP", "no board carried a limit_mb")

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
    detail = (f"strict: {total_breaches} of {checked} board-samples had "
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

    Report-only in general, because `external` is a window-boundary quantity
    with a real staleness (finding B2/T3) and a board that updates *late* is
    behaving as designed. There is exactly one shape that is not staleness at
    all, and it FAILs: a hog that held at least `HOG_STALL_MB` for longer than
    `HOG_STALL_SECONDS` while `external_mb` never moved by a single MiB across
    the whole recording. That is not a late update, it is no update -- run1's
    S15 mutation 2 (`external_locked` patched to return 0) reported
    `external_mb 0..0 MiB` against a hog holding 30 720 MiB, and this row
    reported it as an observation.
    """
    if not ctx.hog_samples or not ctx.health_samples:
        return Verdict("hog_tracking", "SKIP", "needs hog.jsonl and healthrec.jsonl")
    header = next((row for row in ctx.hog if row.get("kind") == "header"), {})
    gpu_uuid = header.get("gpu_uuid")
    if header.get("target") == "ram":
        return Verdict("hog_tracking", "INFO",
                       "the hog pressured RAM, not a board; see the vramrec "
                       "MemAvailable series", {"header": header})
    rows = []
    worst_lag = 0.0
    worst = None
    for sample in ctx.health_samples:
        board = next((entry for entry in (sample.get("health") or {}).get("boards") or []
                      if entry.get("gpu_uuid") == gpu_uuid), None)
        if board is None or not board.get("external_known"):
            continue
        hog = ctx.hog_at(sample["t_wall"])
        if hog is None:
            continue
        vram = ctx.vram_at(sample["t_wall"])
        oracle_used = None
        if vram is not None:
            oracle = ctx.oracle_board(vram, gpu_uuid)
            oracle_used = oracle.get("used_mb") if oracle else None
        rows.append({"t_wall": sample["t_wall"], "iso": sample["iso"],
                     "external_mb": board.get("external_mb"),
                     "hog_held_mb": hog.get("held_mb"),
                     "board_used_mb": oracle_used,
                     "sample_age_ms": board.get("external_sample_age_ms")})
    if not rows:
        return Verdict("hog_tracking", "SKIP",
                       f"no health sample joined the hog on board {gpu_uuid}")
    ages = [row["sample_age_ms"] for row in rows if row["sample_age_ms"] is not None]
    max_age = max(ages) / 1000.0 if ages else None
    # Track the correlation of the two deltas rather than absolute agreement:
    # `external` also contains whatever else lives on the board.
    deltas = []
    for previous, current in zip(rows, rows[1:]):
        d_hog = (current["hog_held_mb"] or 0) - (previous["hog_held_mb"] or 0)
        d_ext = (current["external_mb"] or 0) - (previous["external_mb"] or 0)
        if abs(d_hog) >= 256:
            deltas.append({"iso": current["iso"], "d_hog_mb": d_hog,
                           "d_external_mb": d_ext})

    # --- the FAIL form (run1 S15 hole H3) --------------------------------
    # How long did the hog hold real pressure, counting only the wall time
    # between two consecutive joined samples that were *both* above the
    # threshold? (A single spike between two idle samples buys no time, and an
    # oscillating hog is charged only for the intervals it was actually up.)
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
                   f"is not the B2 staleness window -- a board that updates "
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

    This is `ramp_progress`'s three numbers promoted to a verdict, and it exists
    because of run1's S15 hole H1: a fault that destroys the *measurement* also
    destroys the *evidence the other checks read*, and `analyze.py` turned that
    into SKIP, which never sets the exit code. Mutation 1 (the worker halving
    its reported `peak_reserved_mb`) put the high-water below the post-load
    baseline, so the `grew` test never fired: `high_water_samples = 0` on all 96
    grants, no fit ever formed, `unit_budget` never left the seed of 8 and no
    store was ever written. `slope_accuracy` and `persistence` both SKIPped and
    the leg was caught only by `utilization`, and only because it happened to
    have been run with `--probe`.

    FAILs -- rather than reporting -- only for a leg that declares itself a
    learning scenario (`--learning`, or `calibration_learned` in `--checks`),
    because a seeded or short leg legitimately learns nothing new. Three
    conditions, any one of which is a failure:

      * `fit samples == 0` for some model,
      * no `[[profile]]` in `calibration.after.toml`,
      * peak `unit_budget` never rose above the first value recorded.

    The third reads the first health sample as "the seed". At healthrec's
    default 500 ms that is within a sample of admission, and a leg that ramps
    at all leaves it far behind (run1 S2: 8 -> 1024); a leg whose peak equals
    its first sample never moved.
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

    Report-only, and it exists because of Phase 6's F6: with local inference
    every in-flight predict is loopback HTTP inside one process, so it costs
    **two** sockets in one descriptor table, and the in-flight budget now
    follows the ledger's grant. In the shipped container (`nofile` soft 1024)
    a 2000-item job reached 983 sockets, `accept` began failing with EMFILE,
    SQLite could not open its files and 1849 items went unprocessed. The
    branch raises its own soft limit at startup and clamps the in-flight
    ceiling by the descriptor budget, so this row is how a platform pass
    re-verifies that on its own deployment shape.

    No recorder in this directory produces the input; see the README's
    "Recording file descriptors".
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


# --------------------------------------------------------------------------
# Plot (optional)
# --------------------------------------------------------------------------


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

    boards: Dict[str, List[Tuple[float, int]]] = {}
    for row in ctx.vram_samples:
        for board in row.get("gpus", []):
            if board.get("used_mb") is not None:
                boards.setdefault(board["uuid"], []).append(
                    (row["t_wall"] - base, board["used_mb"]))
    for uuid, points in boards.items():
        axes[0].plot([p[0] for p in points], [p[1] for p in points],
                     label=f"oracle used {uuid[:16]}", linewidth=1)
    if ctx.hog_samples:
        axes[0].plot([row["t_wall"] - base for row in ctx.hog_samples],
                     [row.get("held_mb") or 0 for row in ctx.hog_samples],
                     label="hog held", linewidth=1, linestyle="--")
    axes[0].set_ylabel("MiB")
    axes[0].legend(fontsize=7)
    axes[0].set_title("board memory")

    series: Dict[str, List[Tuple[float, Any]]] = {}
    for sample in ctx.health_samples:
        for board in (sample.get("health") or {}).get("boards") or []:
            for key in ("external_mb", "headroom_mb", "grants_mb"):
                series.setdefault(f"{key} {board.get('gpu_uuid', '')[:12]}", []).append(
                    (sample["t_wall"] - base, board.get(key)))
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


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


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
    # declares nothing, so it must not count as a declaration (see
    # `_declared_learning`).
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
        print("  compared with the board's LIVE FREE MEMORY at that instant. "
              "That clause needs")
        print("  vramrec.jsonl; without it grant_safety reports WARN, never "
              "PASS, so a silently")
        print("  skipped safety clause is visible in the table. "
              "`ledger_invariant`'s strict form is")
        print("  not a substitute -- it passed on a ledger whose `external` "
              "was hard-zeroed (0 of")
        print("  498 board-samples over the limit) while this clause caught "
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
