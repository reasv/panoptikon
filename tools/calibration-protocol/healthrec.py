#!/usr/bin/env python3
"""healthrec.py - poll the gateway's own view of the ledger into JSONL.

Records `GET /api/inference/health` and `GET /api/jobs/queue` at a fixed
cadence (`docs/batch-calibration-test-protocol.md` §2). This is *not* an
independent oracle -- it is the feature's own numbers -- but it is the only
continuous record of grants, ramp steps and deflation, so `analyze.py` joins it
against `vramrec.jsonl` by wall-clock timestamp.

Usage
-----
    healthrec.py --out results/<run>/<scenario>/healthrec.jsonl \
        [--base http://127.0.0.1:6342] [--interval 0.5] [--duration 3600] \
        [--no-queue] [--full] [--timeout 4]

Runs until SIGINT/SIGTERM or `--duration`. A refused connection or a 5xx is
recorded as a sample with `ok: false` and an `error`, and polling continues:
the recorder must outlive a server restart.

Output schema (JSONL)
---------------------
Header:
    {"schema": "healthrec/1", "kind": "header", "base": str, "interval_s": float,
     "t_wall": float, "iso": str, "pid": int, "argv": [...]}

Sample:
    {"schema": "healthrec/1", "kind": "sample", "seq": int,
     "t_mono": float, "t_wall": float, "iso": str,
     "health": {
       "ok": bool, "status_code": int|null, "latency_ms": float,
       "error": str|null,
       "status": str, "shutting_down": bool, "registry_ok": bool,
       "model_count": int,
       "gpus": [{"index": int, "uuid": str, "name": str, "total_mb": int,
                 "compute_cap": str|null, "pci_bdf": str|null}],
       "boards": [{"gpu_uuid","gpu_name","total_mb","external_mb",
                   "external_known","external_source","external_sample_age_ms",
                   "limit_mb","headroom_mb","charges_mb","footprints_mb",
                   "load_reservations_mb","grants_mb","grants_outstanding",
                   "margin","cap_fraction","n_workers"}],
       "workers": [{"gpu_uuid","gpu_name","inference_id","footprint_mb",
                    "charge_mb","base_mb","reserved_at_load_mb","reserved_mb",
                    "grants_outstanding","grants_mb","pending_requests",
                    "seed_units","ramp_step","deflation","clean_windows",
                    "unit_budget","max_units_measured","knee_units",
                    "knee_is_local","throughput_samples","local_samples",
                    "effective_margin","fit_slope_mb_per_unit",
                    "fit_intercept_mb","fit_residual_mb","fit_samples",
                    "fit_transient_samples"}],
       "models": [{"inference_id","generation","queue_depth",
                   "in_flight_windows","last_grant_units","last_window_items",
                   "total_predict_requests","total_batches","replicas_total",
                   "replicas_free","cost_unit","cost_aggregation","cost_epoch",
                   "cost_seed_units","cost_degraded",
                   "replicas":[{"gpu","gpu_uuid","gpu_name","torch_version",
                                "base_mb","base_method","reserved_at_load_mb",
                                "dtype","free_mb","total_mb","free_source",
                                "reserved_mb","allocated_mb","memory_age_ms",
                                "measurements_recorded","recent_batches":[...]}]}],
       "prewarm": {"enabled": bool, "lazy": bool,
                   "warm": [{"impl_class": str, "state": str}]},
       "raw": {...}            # only with --full
     },
     "queue": {"ok": bool, "status_code": int|null, "latency_ms": float,
               "error": str|null, "running": [JobModel], "queued": [JobModel],
               "outcomes": [{"queue_id","status","error"}]}}

Field names are taken from `panoptikon/src/inferio/ledger.rs` (`GpuBudgetHealth`,
`LedgerWorkerHealth`, `FitHealth`), `manager.rs` (`HealthReport`, `ModelHealth`,
`ReplicaTelemetryHealth`, `BatchHealth`) and `jobs/queue.rs`
(`QueueStatusModel`). Anything the server adds later survives verbatim in
`--full` mode; unknown keys are never dropped from `raw`.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_stop = False


def _handle_signal(signum, _frame):  # noqa: ANN001
    global _stop
    _stop = True


def fetch(url: str, timeout: float) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read()
            payload = json.loads(body.decode("utf-8"))
            return {
                "ok": True,
                "status_code": response.status,
                "latency_ms": round((time.monotonic() - started) * 1000.0, 3),
                "error": None,
                "payload": payload,
            }
    except urllib.error.HTTPError as exc:
        return {
            "ok": False,
            "status_code": exc.code,
            "latency_ms": round((time.monotonic() - started) * 1000.0, 3),
            "error": f"HTTP {exc.code}: {exc.reason}",
            "payload": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "latency_ms": round((time.monotonic() - started) * 1000.0, 3),
            "error": f"{type(exc).__name__}: {exc}",
            "payload": None,
        }


BOARD_KEYS = (
    "gpu_uuid", "gpu_name", "total_mb", "external_mb", "external_known",
    "external_source", "external_sample_age_ms", "limit_mb", "headroom_mb",
    "charges_mb", "footprints_mb", "load_reservations_mb", "grants_mb",
    "grants_outstanding", "margin", "cap_fraction",
)
WORKER_KEYS = (
    "inference_id", "footprint_mb", "charge_mb", "base_mb",
    "reserved_at_load_mb", "reserved_mb", "grants_outstanding", "grants_mb",
    "pending_requests", "seed_units", "ramp_step", "deflation",
    "clean_windows", "unit_budget", "max_units_measured", "knee_units",
    "knee_is_local", "throughput_samples", "local_samples", "effective_margin",
)
FIT_KEYS = (
    "slope_mb_per_unit", "intercept_mb", "residual_mb", "samples",
    "transient_samples",
)
REPLICA_KEYS = (
    "gpu", "gpu_uuid", "gpu_name", "gpu_bdf", "torch_version", "base_mb",
    "base_method", "reserved_at_load_mb", "dtype", "free_mb", "total_mb",
    "free_source", "reserved_mb", "allocated_mb", "memory_age_ms",
    "measurements_recorded", "recent_batches",
)
MODEL_KEYS = (
    "inference_id", "generation", "queue_depth", "in_flight_windows",
    "last_grant_units", "last_window_items", "total_predict_requests",
    "total_batches",
)


def flatten_health(result: Dict[str, Any], full: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": result["ok"],
        "status_code": result["status_code"],
        "latency_ms": result["latency_ms"],
        "error": result["error"],
    }
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return out
    out["status"] = payload.get("status")
    out["shutting_down"] = payload.get("shutting_down")
    out["registry_ok"] = payload.get("registry_ok")
    out["model_count"] = payload.get("model_count")
    out["gpus"] = payload.get("gpus", [])
    out["prewarm"] = payload.get("prewarm")

    boards: List[Dict[str, Any]] = []
    workers: List[Dict[str, Any]] = []
    for board in payload.get("vram") or []:
        row = {key: board.get(key) for key in BOARD_KEYS}
        board_workers = board.get("workers") or []
        row["n_workers"] = len(board_workers)
        boards.append(row)
        for worker in board_workers:
            flat = {
                "gpu_uuid": board.get("gpu_uuid"),
                "gpu_name": board.get("gpu_name"),
            }
            flat.update({key: worker.get(key) for key in WORKER_KEYS})
            fit = worker.get("fit") or {}
            for key in FIT_KEYS:
                flat[f"fit_{key}"] = fit.get(key)
            workers.append(flat)
    out["boards"] = boards
    out["workers"] = workers

    models: List[Dict[str, Any]] = []
    for model in payload.get("models") or []:
        row = {key: model.get(key) for key in MODEL_KEYS}
        replicas = model.get("replicas") or {}
        row["replicas_total"] = replicas.get("total")
        row["replicas_free"] = replicas.get("free")
        cost = model.get("cost") or {}
        row["cost_unit"] = cost.get("unit")
        row["cost_aggregation"] = cost.get("aggregation")
        row["cost_epoch"] = cost.get("epoch")
        row["cost_seed_units"] = cost.get("seed_units")
        row["cost_degraded"] = cost.get("degraded")
        row["replicas"] = [
            {key: replica.get(key) for key in REPLICA_KEYS}
            for replica in (model.get("replicas_detail") or [])
        ]
        models.append(row)
    out["models"] = models
    if full:
        out["raw"] = payload
    return out


def flatten_queue(result: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": result["ok"],
        "status_code": result["status_code"],
        "latency_ms": result["latency_ms"],
        "error": result["error"],
    }
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return out
    queue = payload.get("queue") or []
    out["running"] = [job for job in queue if job.get("running")]
    out["queued"] = [job for job in queue if not job.get("running")]
    out["outcomes"] = payload.get("outcomes") or []
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Poll /api/inference/health and /api/jobs/queue into JSONL.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--base", default="http://127.0.0.1:6342",
                        help="gateway base URL")
    parser.add_argument("--out", help="JSONL output path (default: stdout)")
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--timeout", type=float, default=4.0,
                        help="per-request timeout in seconds")
    parser.add_argument("--no-queue", action="store_true",
                        help="poll /api/inference/health only")
    parser.add_argument("--full", action="store_true",
                        help="also store the untouched health JSON under health.raw")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    base = args.base.rstrip("/")
    health_url = f"{base}/api/inference/health"
    queue_url = f"{base}/api/jobs/queue"

    sink = open(args.out, "a", encoding="utf-8") if args.out else sys.stdout
    started_mono = time.monotonic()
    sink.write(
        json.dumps(
            {
                "schema": "healthrec/1",
                "kind": "header",
                "base": base,
                "interval_s": args.interval,
                "t_wall": round(time.time(), 6),
                "iso": datetime.now(timezone.utc).isoformat(),
                "pid": os.getpid(),
                "argv": sys.argv,
            }
        )
        + "\n"
    )
    sink.flush()
    if not args.quiet:
        print(f"healthrec: polling {health_url} every {args.interval}s",
              file=sys.stderr)

    seq = 0
    failures = 0
    deadline = None if args.duration is None else started_mono + args.duration
    try:
        while not _stop:
            tick = time.monotonic()
            health = flatten_health(fetch(health_url, args.timeout), args.full)
            sample: Dict[str, Any] = {
                "schema": "healthrec/1",
                "kind": "sample",
                "seq": seq,
                "t_mono": round(time.monotonic() - started_mono, 6),
                "t_wall": round(time.time(), 6),
                "iso": datetime.now(timezone.utc).isoformat(),
                "health": health,
            }
            if not args.no_queue:
                sample["queue"] = flatten_queue(fetch(queue_url, args.timeout))
            if not health["ok"]:
                failures += 1
            sink.write(json.dumps(sample) + "\n")
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
    if not args.quiet:
        print(f"healthrec: {seq} samples, {failures} failed poll(s)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
