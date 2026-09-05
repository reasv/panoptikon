#!/usr/bin/env python3
"""loadgen.py - concurrent predict driver for the batch-calibration protocol.

The job queue runs exactly one job at a time, so multi-model contention on one
GPU never arises from jobs alone (`docs/batch-calibration-test-protocol.md`
§2/§6). `loadgen.py` drives `POST /api/inference/predict/{group}/{id}` directly,
several models at once, at a chosen per-model concurrency and request size, out
of a `corpus.py` manifest.

Usage
-----
    loadgen.py --base http://127.0.0.1:6342 --out loadgen.jsonl \
        --corpus results/corpus/ramp/manifest.json \
        --model 'id=tags/wd-vit-tagger-v3,concurrency=2,items=64' \
        --model 'id=clip/apple_MobileCLIP-S1,concurrency=2,items=64' \
        --model 'id=textembed/all-MiniLM-L6-v2,concurrency=1,items=32,corpus=.../text/manifest.json' \
        --duration 600

Model spec (comma-separated `key=value`, repeatable `--model`):
    id=<inference_id>       required, e.g. `tags/wd-vit-tagger-v3`
    concurrency=N           in-flight requests for this model      (default 1)
    items=N                 corpus items per request               (default 8)
    corpus=PATH             manifest.json (or its directory) for this model
                            (default: --corpus)
    group=NAME              only corpus items whose `group` == NAME
    kind=NAME               only corpus items whose `kind` == NAME
    mode=file|text|auto     how inputs are built                   (default auto)
    requests=N              stop this model after N requests
    max_batch=N             `?max_batch=` query parameter
    cache_key=S             `?cache_key=`                    (default `loadgen`)
    lru_size=N              `?lru_size=`                            (default 1)
    ttl_seconds=N           `?ttl_seconds=`                       (default 600)
    order=sequential|random iteration order over the corpus (default sequential)
    interval=SECONDS        minimum wall time between the *starts* of two
                            consecutive requests on one slot, so a long soak
                            can hold a low steady rate (default 0 = flat out).
                            With concurrency=N the model's rate is N/interval.
    data=<json object>      merged into every input entry (e.g. `{"threshold":0.1}`)

Other options: --duration S, --requests N (per model, overridden by the spec),
--timeout S, --seed N, --warmup-load/--no-warmup-load (PUT /api/inference/load
once per model first), --quiet.

`--prewarm-only [--hold S]` is the S2-base plateau leg: PUT the load for every
`--model`, hold them resident and idle for S seconds (default 60), and exit
without issuing a single predict, so `analyze.py`'s `base_accuracy` has a
window of samples in which each worker holds its base and nothing else. It
needs no corpus, and it writes a `{"kind": "hold", ...}` record. Give the
models room to co-reside -- `lru_size=<number of models>`, or a distinct
`cache_key=` each -- or the second load evicts the first from the slot.

Wire format
-----------
`multipart/form-data` with a `data` field holding
`{"inputs": [<entry>, ...]}` and one `files` part per file-backed entry whose
*filename* is the integer index of the entry it attaches to
(`panoptikon/src/inferio/http.rs: parse_input_request`). `cache_key`,
`lru_size` and `ttl_seconds` are REQUIRED query parameters (`PredictParams`
has no serde defaults); omitting them is a 400.

In `auto` mode a corpus item of kind `text` becomes a file-less entry
`{"text": "<contents>"}` (what `sentence_transformers` asserts on) and every
other kind becomes `{}` plus its bytes as a file part.

Output schema (JSONL)
---------------------
Header: {"schema": "loadgen/1", "kind": "header", "base", "t_wall", "iso",
         "pid", "argv", "models": [<resolved spec + item count>]}

Per request:
    {"schema": "loadgen/1", "kind": "request", "seq": int, "model": str,
     "slot": int, "t_start_wall": float, "t_start_mono": float,
     "t_end_wall": float, "latency_ms": float, "status": int|null,
     "ok": bool, "items": int, "bytes_sent": int,
     "item_ids": [str], "units": {"item": int, "pixel": int|null,
                                  "token": int|null, "audio-second": int|null},
     "outputs": int|null, "output_errors": int,
     "desired_in_flight_items": int|null, "desired_source": "body"|"header"|null,
     "error": str|null, "body_head": str|null}

`desired_in_flight_items` is the additive feedback field being added to the
predict response (plan §8 G7); it is read from the JSON body and, failing that,
from an `x-panoptikon-desired-in-flight-items` (the shipped name; the older `x-desired-in-flight-items` is also accepted) response header, and is `null` when the
server does not send it. Its absence is never an error.

Trailer: {"schema": "loadgen/1", "kind": "summary", "elapsed_s": float,
          "models": {<id>: {"requests", "ok", "failed", "items", "items_per_s",
                            "latency_ms": {"p50","p90","p99","max","mean"},
                            "statuses": {"200": n, ...},
                            "desired_in_flight_items_last": int|null}}}
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

_stop = threading.Event()
_write_lock = threading.Lock()


def _handle_signal(signum, _frame):  # noqa: ANN001
    _stop.set()


# --------------------------------------------------------------------------
# Corpus
# --------------------------------------------------------------------------


def load_manifest(path: str) -> Dict[str, Any]:
    candidate = Path(path)
    if candidate.is_dir():
        candidate = candidate / "manifest.json"
    if not candidate.is_file():
        raise SystemExit(f"loadgen: no manifest at {candidate}")
    return json.loads(candidate.read_text(encoding="utf-8"))


def select_items(manifest: Dict[str, Any], group: Optional[str],
                 kind: Optional[str]) -> List[Dict[str, Any]]:
    items = manifest.get("items", [])
    if group:
        items = [item for item in items if item.get("group") == group]
    if kind:
        items = [item for item in items if item.get("kind") == kind]
    if not items:
        raise SystemExit(
            f"loadgen: manifest {manifest.get('root')} has no items matching "
            f"group={group!r} kind={kind!r}"
        )
    return items


# --------------------------------------------------------------------------
# Multipart
# --------------------------------------------------------------------------


def encode_multipart(data_json: str,
                     files: List[Tuple[int, bytes]]) -> Tuple[bytes, str]:
    boundary = f"----panoptikon-loadgen-{uuid.uuid4().hex}"
    out = bytearray()
    marker = f"--{boundary}\r\n".encode()
    out += marker
    out += b'Content-Disposition: form-data; name="data"\r\n'
    out += b"Content-Type: application/json\r\n\r\n"
    out += data_json.encode("utf-8")
    out += b"\r\n"
    for index, blob in files:
        out += marker
        out += (
            f'Content-Disposition: form-data; name="files"; filename="{index}"\r\n'
        ).encode()
        out += b"Content-Type: application/octet-stream\r\n\r\n"
        out += blob
        out += b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return bytes(out), f"multipart/form-data; boundary={boundary}"


# --------------------------------------------------------------------------
# Model spec
# --------------------------------------------------------------------------


class ModelSpec:
    def __init__(self, raw: str, defaults: argparse.Namespace) -> None:
        fields: Dict[str, str] = {}
        for chunk in _split_spec(raw):
            key, _, value = chunk.partition("=")
            fields[key.strip()] = value.strip()
        if "id" not in fields:
            raise SystemExit(f"loadgen: model spec {raw!r} has no id=")
        self.raw = raw
        self.id = fields["id"]
        self.concurrency = int(fields.get("concurrency", 1))
        self.items = int(fields.get("items", 8))
        self.corpus = fields.get("corpus") or defaults.corpus
        self.group = fields.get("group") or None
        self.kind = fields.get("kind") or None
        self.mode = fields.get("mode", "auto")
        self.requests = int(fields["requests"]) if "requests" in fields else defaults.requests
        self.max_batch = int(fields["max_batch"]) if "max_batch" in fields else None
        self.cache_key = fields.get("cache_key", "loadgen")
        self.lru_size = int(fields.get("lru_size", 1))
        self.ttl_seconds = int(fields.get("ttl_seconds", 600))
        self.order = fields.get("order", "sequential")
        self.interval = float(fields.get("interval", 0.0))
        self.data = json.loads(fields["data"]) if "data" in fields else {}
        if getattr(defaults, "prewarm_only", False):
            # No predict is ever issued, so a corpus is not just unnecessary,
            # requiring one would mean building images for a leg whose whole
            # point is that nothing is inferred (`--prewarm-only`).
            self.manifest = {}
            self.pool = []
        else:
            if not self.corpus:
                raise SystemExit(
                    f"loadgen: model {self.id} has no corpus= and no --corpus")
            self.manifest = load_manifest(self.corpus)
            self.pool = select_items(self.manifest, self.group, self.kind)
        self.cursor = 0
        self.lock = threading.Lock()
        self.sent = 0

    def describe(self) -> Dict[str, Any]:
        return {
            "id": self.id, "concurrency": self.concurrency, "items": self.items,
            "corpus": self.corpus, "group": self.group, "kind": self.kind,
            "mode": self.mode, "requests": self.requests, "interval": self.interval,
            "max_batch": self.max_batch, "cache_key": self.cache_key,
            "order": self.order, "pool_size": len(self.pool),
            "data": self.data,
        }

    def take(self, count: int, rng: random.Random) -> Optional[List[Dict[str, Any]]]:
        with self.lock:
            if self.requests is not None and self.sent >= self.requests:
                return None
            self.sent += 1
            if self.order == "random":
                return [rng.choice(self.pool) for _ in range(count)]
            picked = []
            for _ in range(count):
                picked.append(self.pool[self.cursor % len(self.pool)])
                self.cursor += 1
            return picked


def _split_spec(raw: str) -> List[str]:
    """Split on commas that are not inside a JSON object (the `data=` value)."""
    parts: List[str] = []
    depth = 0
    current = ""
    for char in raw:
        if char in "{[":
            depth += 1
        elif char in "}]":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
            continue
        current += char
    if current:
        parts.append(current)
    return parts


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def build_request(spec: ModelSpec, items: List[Dict[str, Any]]) -> Tuple[bytes, str, Dict[str, Any]]:
    inputs: List[Any] = []
    files: List[Tuple[int, bytes]] = []
    units = {"item": 0, "pixel": 0, "token": 0, "audio-second": 0}
    for index, item in enumerate(items):
        path = Path(item.get("abspath") or (Path(spec.manifest["root"]) / item["path"]))
        as_text = spec.mode == "text" or (spec.mode == "auto" and item["kind"] == "text")
        if as_text:
            entry: Dict[str, Any] = {"text": path.read_text(encoding="utf-8", errors="replace")}
            entry.update(spec.data)
            inputs.append(entry)
        else:
            entry = dict(spec.data)
            inputs.append(entry)
            files.append((index, path.read_bytes()))
        for key in units:
            value = (item.get("units") or {}).get(key)
            if value:
                units[key] += int(value)
    body, content_type = encode_multipart(json.dumps({"inputs": inputs}), files)
    meta = {
        "item_ids": [item["id"] for item in items],
        "units": {key: (value or None) for key, value in units.items()},
        "items": len(items),
        "bytes_sent": len(body),
    }
    meta["units"]["item"] = len(items)
    return body, content_type, meta


def predict_url(base: str, spec: ModelSpec) -> str:
    query = (
        f"cache_key={spec.cache_key}&lru_size={spec.lru_size}"
        f"&ttl_seconds={spec.ttl_seconds}"
    )
    if spec.max_batch is not None:
        query += f"&max_batch={spec.max_batch}"
    return f"{base}/api/inference/predict/{spec.id}?{query}"


def extract_desired(payload: Any, headers: Any) -> Tuple[Optional[int], Optional[str]]:
    """The additive in-flight feedback field, wherever the server puts it."""
    if isinstance(payload, dict):
        for key in ("desired_in_flight_items", "desired_inflight_items",
                    "desired_in_flight"):
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return int(value), "body"
    if headers is not None:
        for key in ("x-desired-in-flight-items", "x-panoptikon-desired-in-flight-items"):
            value = headers.get(key)
            if value is not None:
                try:
                    return int(value), "header"
                except ValueError:
                    continue
    return None, None


def do_request(base: str, spec: ModelSpec, items: List[Dict[str, Any]],
               timeout: float) -> Dict[str, Any]:
    body, content_type, meta = build_request(spec, items)
    request = urllib.request.Request(
        predict_url(base, spec), data=body, method="POST",
        headers={"Content-Type": content_type, "Accept": "application/json"},
    )
    started_mono = time.monotonic()
    started_wall = time.time()
    record: Dict[str, Any] = {
        "model": spec.id,
        "t_start_wall": round(started_wall, 6),
        "t_start_mono": round(started_mono, 6),
        "status": None,
        "ok": False,
        "outputs": None,
        "output_errors": 0,
        "desired_in_flight_items": None,
        "desired_source": None,
        "error": None,
        "body_head": None,
        **meta,
    }
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            record["status"] = response.status
            record["ok"] = 200 <= response.status < 300
            payload = None
            if response.headers.get_content_type() == "application/json":
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except Exception as exc:
                    record["error"] = f"json decode: {exc}"
            if isinstance(payload, dict):
                outputs = payload.get("outputs")
                if isinstance(outputs, list):
                    record["outputs"] = len(outputs)
                    record["output_errors"] = sum(
                        1 for entry in outputs
                        if isinstance(entry, dict) and "__error__" in entry
                    )
            else:
                # octet-stream / multipart-mixed: count is not recoverable
                record["outputs"] = None
            desired, source = extract_desired(payload, response.headers)
            record["desired_in_flight_items"] = desired
            record["desired_source"] = source
    except urllib.error.HTTPError as exc:
        record["status"] = exc.code
        detail = exc.read()[:1000].decode("utf-8", "replace")
        record["error"] = f"HTTP {exc.code}: {exc.reason}"
        record["body_head"] = detail
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
    record["latency_ms"] = round((time.monotonic() - started_mono) * 1000.0, 3)
    record["t_end_wall"] = round(time.time(), 6)
    return record


def warmup_load(base: str, spec: ModelSpec, timeout: float) -> Dict[str, Any]:
    url = (
        f"{base}/api/inference/load/{spec.id}?cache_key={spec.cache_key}"
        f"&lru_size={spec.lru_size}&ttl_seconds={spec.ttl_seconds}"
    )
    request = urllib.request.Request(url, method="PUT")
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return {"model": spec.id, "ok": True, "status": response.status,
                    "latency_ms": round((time.monotonic() - started) * 1000, 3),
                    "body": response.read()[:400].decode("utf-8", "replace")}
    except Exception as exc:
        return {"model": spec.id, "ok": False, "status": None,
                "latency_ms": round((time.monotonic() - started) * 1000, 3),
                "error": f"{type(exc).__name__}: {exc}"}


def prewarm_hold(args: argparse.Namespace, warmups: List[Dict[str, Any]],
                 emit: Any) -> int:
    """Hold the loaded models resident and idle, then leave (`--prewarm-only`).

    The point of the leg is the *absence* of work. `base_accuracy` compares a
    replica's reported `base_mb` against the oracle's per-process reading, and
    the only samples that measure the same quantity are the ones between the
    load and the replica's first grant or predict -- after that the process
    also holds the batch's cuBLAS/cuDNN workspace. A demand-driven load starts
    its first batch tens of milliseconds later, so at 1-4 Hz that window is
    normally empty and every row is reported unjudged. Loading and then doing
    nothing for a minute turns it into hundreds of samples of flat plateau
    (run1/S6-b18-loadstall got 714 of them and reads 0.0%).

    Nothing here talks to the gateway: the loads already happened, and holding
    is exactly what an idle client does. The hold is interruptible, so a
    SIGINT ends the leg cleanly rather than leaving the recorders running.
    """
    failed = [row for row in warmups if not row.get("ok")]
    if len(failed) == len(warmups):
        print("loadgen: nothing loaded, so there is no plateau to hold: " +
              ", ".join(f"{row['model']} {row.get('error') or row.get('status')}"
                        for row in failed), file=sys.stderr)
        return 1
    started = time.monotonic()
    t_start = time.time()
    if not args.quiet:
        print(f"loadgen: {len(warmups) - len(failed)} model(s) loaded; holding "
              f"idle for {args.hold:.0f}s", file=sys.stderr)
    _stop.wait(args.hold)
    held = time.monotonic() - started
    emit({
        "schema": "loadgen/1", "kind": "hold",
        "iso": datetime.now(timezone.utc).isoformat(),
        "t_start_wall": round(t_start, 6), "t_end_wall": round(time.time(), 6),
        "requested_s": args.hold, "held_s": round(held, 3),
        "interrupted": _stop.is_set(),
        "models_loaded": [row["model"] for row in warmups if row.get("ok")],
        "models_failed": [row["model"] for row in failed],
    })
    emit({
        "schema": "loadgen/1", "kind": "summary", "elapsed_s": round(held, 3),
        "prewarm_only": True, "models": {},
    })
    if failed:
        print("loadgen: load failed for " +
              ", ".join(row["model"] for row in failed), file=sys.stderr)
        return 1
    return 0 if held >= args.hold - 1.0 else 1


def percentile(values: List[float], fraction: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
    return round(ordered[index], 3)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Concurrent /api/inference/predict driver.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--base", default="http://127.0.0.1:6342")
    parser.add_argument("--out", help="JSONL output (default: stdout)")
    parser.add_argument("--corpus", help="default manifest.json for every model")
    parser.add_argument("--model", action="append", dest="models", default=[],
                        help="model spec (repeatable); see the module docstring")
    parser.add_argument("--duration", type=float, default=None,
                        help="stop after this many seconds")
    parser.add_argument("--requests", type=int, default=None,
                        help="default per-model request cap")
    parser.add_argument("--timeout", type=float, default=600.0,
                        help="per-request timeout in seconds")
    parser.add_argument("--seed", type=int, default=20260903)
    parser.add_argument("--warmup-load", action="store_true",
                        help="PUT /api/inference/load once per model before driving")
    parser.add_argument("--prewarm-only", action="store_true",
                        help="load every model, hold them resident and idle for "
                             "--hold seconds, and exit without a single predict "
                             "(the S2-base plateau leg). Implies --warmup-load "
                             "and needs no corpus.")
    parser.add_argument("--hold", type=float, default=60.0,
                        help="seconds to hold the models resident and idle under "
                             "--prewarm-only")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    if not args.models:
        parser.error("at least one --model is required")
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    base = args.base.rstrip("/")
    specs = [ModelSpec(raw, args) for raw in args.models]
    sink = open(args.out, "a", encoding="utf-8") if args.out else sys.stdout

    def emit(record: Dict[str, Any]) -> None:
        with _write_lock:
            sink.write(json.dumps(record) + "\n")
            sink.flush()

    emit({
        "schema": "loadgen/1", "kind": "header", "base": base,
        "t_wall": round(time.time(), 6),
        "iso": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(), "argv": sys.argv,
        "models": [spec.describe() for spec in specs],
    })

    warmups: List[Dict[str, Any]] = []
    if args.warmup_load or args.prewarm_only:
        for spec in specs:
            result = warmup_load(base, spec, args.timeout)
            warmups.append(result)
            emit({"schema": "loadgen/1", "kind": "warmup",
                  "iso": datetime.now(timezone.utc).isoformat(), **result})

    if args.prewarm_only:
        return prewarm_hold(args, warmups, emit)

    started = time.monotonic()
    deadline = None if args.duration is None else started + args.duration
    counter = {"seq": 0}
    counter_lock = threading.Lock()
    stats: Dict[str, Dict[str, Any]] = {
        spec.id: {"requests": 0, "ok": 0, "failed": 0, "items": 0,
                  "latencies": [], "statuses": {},
                  "desired_in_flight_items_last": None}
        for spec in specs
    }
    stats_lock = threading.Lock()

    def worker(spec: ModelSpec, slot: int) -> None:
        rng = random.Random(args.seed + slot * 7919 + hash(spec.id) % 100003)
        next_start = time.monotonic()
        while not _stop.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                return
            if spec.interval > 0.0:
                # Pace the *starts*, so a slow response does not add to the gap
                # and the rate stays what the scenario asked for.
                while not _stop.is_set():
                    wait = next_start - time.monotonic()
                    if wait <= 0:
                        break
                    if deadline is not None and time.monotonic() >= deadline:
                        return
                    _stop.wait(min(wait, 0.25))
                if _stop.is_set():
                    return
                now = time.monotonic()
                next_start = max(now, next_start + spec.interval)
            items = spec.take(spec.items, rng)
            if items is None:
                return
            record = do_request(base, spec, items, args.timeout)
            with counter_lock:
                record["seq"] = counter["seq"]
                counter["seq"] += 1
            record["schema"] = "loadgen/1"
            record["kind"] = "request"
            record["slot"] = slot
            emit(record)
            with stats_lock:
                bucket = stats[spec.id]
                bucket["requests"] += 1
                bucket["items"] += record["items"]
                bucket["latencies"].append(record["latency_ms"])
                key = str(record["status"])
                bucket["statuses"][key] = bucket["statuses"].get(key, 0) + 1
                if record["ok"]:
                    bucket["ok"] += 1
                else:
                    bucket["failed"] += 1
                if record["desired_in_flight_items"] is not None:
                    bucket["desired_in_flight_items_last"] = record["desired_in_flight_items"]

    threads: List[threading.Thread] = []
    for spec in specs:
        for slot in range(spec.concurrency):
            thread = threading.Thread(target=worker, args=(spec, slot), daemon=True)
            thread.start()
            threads.append(thread)
    if not args.quiet:
        print(f"loadgen: {len(threads)} thread(s) over {len(specs)} model(s)",
              file=sys.stderr)
    try:
        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                thread.join(timeout=0.25)
                if _stop.is_set():
                    break
            if _stop.is_set():
                break
    except KeyboardInterrupt:
        _stop.set()
    _stop.set()
    for thread in threads:
        thread.join(timeout=args.timeout)

    elapsed = time.monotonic() - started
    summary: Dict[str, Any] = {}
    for model_id, bucket in stats.items():
        latencies = bucket.pop("latencies")
        summary[model_id] = {
            **bucket,
            "items_per_s": round(bucket["items"] / elapsed, 4) if elapsed else None,
            "latency_ms": {
                "p50": percentile(latencies, 0.50),
                "p90": percentile(latencies, 0.90),
                "p99": percentile(latencies, 0.99),
                "max": round(max(latencies), 3) if latencies else None,
                "mean": round(sum(latencies) / len(latencies), 3) if latencies else None,
            },
        }
    emit({"schema": "loadgen/1", "kind": "summary",
          "elapsed_s": round(elapsed, 3),
          "iso": datetime.now(timezone.utc).isoformat(),
          "models": summary})
    if sink is not sys.stdout:
        sink.close()
    if not args.quiet:
        for model_id, bucket in summary.items():
            print(f"loadgen {model_id}: {bucket['requests']} req "
                  f"({bucket['failed']} failed), {bucket['items']} items, "
                  f"{bucket['items_per_s']} items/s, "
                  f"p50 {bucket['latency_ms']['p50']} ms", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
