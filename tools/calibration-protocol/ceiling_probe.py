#!/usr/bin/env python3
"""ceiling_probe.py - ground-truth base / slope / OOM boundary, outside the ledger.

Loads a shipped `inferio` impl the same way the worker does -- same registry
entry, same impl class, same device pin -- but with no orchestrator, no packer
and no grant, then measures what a batch of N units actually costs
(`docs/batch-calibration-test-protocol.md` §2). Its `base` and
`slope_mb_per_unit` are the numbers the ledger's fit should converge to, and
its OOM boundary is the line the ledger's grants must stay under.

Usage
-----
    # resolve everything and print the plan, touching no GPU
    ceiling_probe.py --model tags/wd-vit-tagger-v3 \
        --corpus results/corpus/ramp/manifest.json --dry-run

    # the real probe on GPU 0, batches 1,2,4,...,64
    ceiling_probe.py --model tags/wd-vit-tagger-v3 \
        --corpus results/corpus/ramp/manifest.json \
        --device 0 --max-batch 64 --repeats 2 \
        --out results/<run>/<scenario>/probe-wd-vit.json

    # with hog.py holding `leave-free 12288` on the same GPU, find the
    # largest batch that still runs at 12 GiB free
    ceiling_probe.py --model tags/wd-vit-tagger-v3 --corpus ... \
        --device 0 --bisect-oom --bisect-max 4096

Key options
-----------
    --model ID          inference_id, e.g. `tags/wd-vit-tagger-v3`  (required)
    --corpus PATH       corpus.py manifest.json (or its directory)
    --group / --kind    restrict which corpus items are used
    --device N          NVML GPU index; translated to
                        `CUDA_VISIBLE_DEVICES=GPU-<uuid>` exactly as the
                        orchestrator pins a worker (`gpu.rs: resolve_pin`)
    --batches 1,2,4,8   explicit batch sizes (default: powers of two up to
                        --max-batch)
    --max-batch N       largest power-of-two batch to measure     (default 64)
    --repeats N         measurements per batch size               (default 1)
    --warmup N          untimed batches of size 1 before measuring (default 1)
    --bisect-oom        binary-search the largest batch that does not OOM
    --bisect-max N      upper bound for the search               (default 1024)
    --bisect-start N    first size the doubling phase probes; skips the cheap
                        sizes when the boundary is known to be far above them
    --bisect-budget S   stop refining after S seconds of bisect probes and
                        report the bracket reached (0 = no limit)
    --repo PATH         repo root (default: two levels above this file)
    --impl-dir PATH     extra impl dir (repeatable)
    --registry PATH     extra registry TOML (repeatable)
    --out PATH          JSON result file (default: stdout)
    --dry-run           resolve and plan only

Measurement
-----------
Per batch: `torch.cuda.reset_peak_memory_stats()`, then `instance.predict(...)`,
then `max_memory_reserved` / `max_memory_allocated` / `memory_reserved` and the
NVML per-process figure for this PID. `delta_mb = peak_reserved_mb -
reserved_at_load_mb` is exactly the ledger's `FitSample.delta_mb`
(`ledger.rs: ingest_locked`), and the slope is fitted with the same Theil-Sen
estimator (`ledger.rs: robust_fit`: median pairwise slope, median intercept,
median absolute residual, >= 3 samples, slope > 0), so the two numbers are
directly comparable.

Units are priced with the worker's own `packing.price_inputs` /
`packing.batch_units`, so `units` means the same thing on both sides —
including the per-item pixel canvas (run2 R7): a `pixel` model is priced at
`min(raw pixels, canvas)`, resolved from the registry declaration first and
the loaded impl's own attribute second, exactly as a worker under a grant
resolves it. `cost.canvas_pixels_in_force` in the output says which figure
priced the run (`null` = uncapped), because a slope fitted under a cap and one
fitted without it are different numbers.

Output schema (JSON)
--------------------
    {"schema": "ceiling_probe/1", "model": str, "impl_class": str,
     "config": {...}, "cost": {"unit": str, "aggregation": str,
                               "seed_units": int|null, "epoch": int|null,
                               "canvas_pixels": int|null,
                               "canvas_pixels_in_force": int|null},
     "device": {"index": int, "uuid": str, "name": str, "total_mb": int,
                "cuda_visible_devices": str},
     "torch": str, "dtype": str|null, "python": str,
     "load": {"seconds": float, "base_nvml_mb": int|null,
              "base_free_delta_mb": int|null, "reserved_at_load_mb": int,
              "allocated_at_load_mb": int, "free_before_mb": int,
              "free_after_mb": int},
     "batches": [{"batch": int, "repeat": int, "units": int,
                  "items": int, "ok": bool, "oom": bool,
                  "oom_class": {"source": str, "exception": str,
                                "free_mb_at_failure": int|null,
                                "device": str} | null,
                  "absorbed_halvings": int, "index_limit_events": int,
                  "duration_ms": float,
                  "peak_reserved_mb": int, "peak_allocated_mb": int,
                  "reserved_before_mb": int, "reserved_after_mb": int,
                  "nvml_own_mb": int|null, "gpu_free_mb": int|null,
                  "delta_mb": int, "error": str|null}],
     "fit": {"slope_mb_per_unit": float, "intercept_mb": float,
             "residual_mb": float, "samples": int} | null,
     "bisect": {"free_mb_at_start": int|null,
                "reserved_at_bisect_start_mb": int|null,
                "largest_ok_units": int|null,
                "largest_ok_items": int|null, "first_oom_items": int|null,
                "first_index_limit_items": int|null,
                "low_items": int, "high_items": int, "stopped_early": bool,
                "trace": [{"items": int, "ok": bool, "units": int,
                           "oom": bool, "absorbed_halvings": int,
                           "index_limit_events": int,
                           "error": str|null}]} | null}

`free_mb_at_start` is the GPU's free memory when the search begins, which is
*after* the `--batches` sweep: the caching allocator is still holding what that
sweep reserved, so the memory a bisect probe can actually use is
`free_mb_at_start + reserved_at_bisect_start_mb`. Compare a boundary against
that sum, not against `free_mb_at_start` alone.

Caveats
-------
* `oom` is decided by the worker's own classifier (`packing.classify_oom`),
  imported rather than copied, so the boundary this tool draws is the boundary
  the ledger acts on; `oom_class` says which tier decided (`typed_exception`,
  `marker`, `message_pattern`) and what the GPU had free at the time.
* Impls with their own `run_with_oom_retry` (wd taggers, openclip) absorb OOMs
  by halving internally, so a "successful" batch can still have hit one; the
  probe reads `inferio.impl.utils.total_oom_halvings()` across every call and
  reports `absorbed_halvings`, and the bisect treats a batch with absorbed
  halvings as an OOM.
* A batch can also be cut short by a **shape ceiling** rather than by memory:
  a kernel whose 32-bit element index cannot address the tensor the batch
  builds refuses it with the whole GPU free. `index_limit_events` counts
  those (`inferio.impl.utils.total_index_limit_events()`, diffed the same
  way); such a batch is **not** `ok` for the sweep or the bisect, and its
  boundary is recorded as `first_index_limit_items`, not `first_oom_items`.
  Without this both easyOCR bisects reported 37 against a true 28
  (`run2-probes-report.md`, S1/S4).
* `clap` and `sentence_transformers` have no impl-side retry: an OOM there is a
  raised exception.
* Whisper (`faster_whisper`) uses CTranslate2, not the torch allocator: its
  reserved/allocated figures stay near zero and only the NVML own-PID figure
  moves. That is a property of the model, not a probe failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

MIB = 1024 * 1024


# --------------------------------------------------------------------------
# Registry resolution (no torch, no gateway)
# --------------------------------------------------------------------------


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _merge_registry_document(merged: Dict[str, Any], document: Dict[str, Any]) -> None:
    """Fold one registry file into the accumulator, the way the loader does.

    Group-level `config` and `metadata` merge key by key across files, but an
    `[group.G.inference_ids.ID]` table **replaces** any earlier definition of
    that id wholesale — `registry.rs: load_file` inserts the parsed entry into
    `entry.inference_ids`, it does not merge into it, and the Python loader it
    mirrors does the same (`config.py:61-76`). Deep-merging the id tables
    instead lets a shipped key survive an override that deliberately omits it,
    which is not a cosmetic difference: `registry-C7nc.toml` exists precisely
    to run easyOCR *without* `metadata.cost.canvas_pixels`, and under a deep
    merge the shipped `6553600` leaked back in and priced the uncapped control
    at the cap.
    """
    for key, value in document.items():
        if key != "group" or not isinstance(value, dict):
            merged[key] = (
                _deep_merge(merged[key], value)
                if isinstance(value, dict) and isinstance(merged.get(key), dict)
                else value
            )
            continue
        groups = merged.setdefault("group", {})
        for group_name, group_data in value.items():
            if not isinstance(group_data, dict):
                groups[group_name] = group_data
                continue
            target = groups.setdefault(group_name, {})
            for sub_key, sub_value in group_data.items():
                if sub_key != "inference_ids" or not isinstance(sub_value, dict):
                    target[sub_key] = (
                        _deep_merge(target[sub_key], sub_value)
                        if isinstance(sub_value, dict)
                        and isinstance(target.get(sub_key), dict)
                        else sub_value
                    )
                    continue
                ids = target.setdefault("inference_ids", {})
                for inference_id, id_table in sub_value.items():
                    ids[inference_id] = id_table


def load_registries(paths: List[Path]) -> Dict[str, Any]:
    import tomllib

    merged: Dict[str, Any] = {"group": {}}
    for path in paths:
        if not path.is_file():
            continue
        with path.open("rb") as handle:
            document = tomllib.load(handle)
        _merge_registry_document(merged, document)
    return merged


def registry_files(repo: Path, extra: List[str]) -> List[Path]:
    files = sorted((repo / "python" / "inferio" / "config").glob("*.toml"))
    files += sorted((repo / "config" / "inference").glob("*.toml"))
    files += [Path(path) for path in extra]
    return files


def resolve_model(registry: Dict[str, Any], inference_id: str) -> Dict[str, Any]:
    """Group + inference-id merge, mirroring the Rust registry loader."""
    group_name, _, model_name = inference_id.partition("/")
    if not model_name:
        raise SystemExit(f"ceiling_probe: {inference_id!r} is not <group>/<id>")
    groups = registry.get("group") or {}
    group = groups.get(group_name)
    if group is None:
        raise SystemExit(
            f"ceiling_probe: group {group_name!r} not in the registry "
            f"(have: {', '.join(sorted(groups))})"
        )
    entries = group.get("inference_ids") or {}
    entry = entries.get(model_name)
    if entry is None:
        raise SystemExit(
            f"ceiling_probe: inference id {model_name!r} not in group "
            f"{group_name!r} (have: {', '.join(sorted(entries))})"
        )
    config = _deep_merge(group.get("config") or {}, entry.get("config") or {})
    metadata = _deep_merge(group.get("metadata") or {}, entry.get("metadata") or {})
    cost = metadata.get("cost") or {}
    unit = cost.get("unit", "item")
    impl_class = config.pop("impl_class", None)
    if not impl_class:
        raise SystemExit(f"ceiling_probe: no impl_class for {inference_id}")
    return {
        "inference_id": inference_id,
        "impl_class": impl_class,
        "config": config,
        "metadata": metadata,
        "cost": {
            "unit": unit,
            "aggregation": cost.get("aggregation", "count"),
            "seed_units": cost.get("seed_units"),
            "epoch": cost.get("epoch"),
            "canvas_pixels": _canvas_pixels(
                (entry.get("metadata") or {}).get("cost") or {},
                (group.get("metadata") or {}).get("cost") or {},
                unit,
            ),
            "degraded": not cost,
        },
    }


def _canvas_pixels(
    id_cost: Dict[str, Any], group_cost: Dict[str, Any], unit: str
) -> Optional[int]:
    """`metadata.cost.canvas_pixels`, under the orchestrator's own two rules.

    Resolved here rather than off the merged metadata because the merge is
    key-by-key and this key is **scale-bound**: `cost.rs: canvas_from_tables`
    reads it only for a `pixel` unit, and never inherits a group's value into
    an id that redeclares the unit — `[group.clip]` is `item`-priced and its
    VLM ids are not, so inheriting the CLIP tower's canvas there would cap a
    tiled VLM at 378² and under-price every one of its items.
    """
    if unit != "pixel":
        return None
    declared = id_cost.get("canvas_pixels")
    if declared is None:
        if group_cost.get("unit", "item") != unit:
            return None
        declared = group_cost.get("canvas_pixels")
    if not isinstance(declared, int) or isinstance(declared, bool) or declared < 1:
        return None
    return declared


def batch_pricer(
    packing: Any, cost: Dict[str, Any], instance: Any
) -> Tuple[Any, Optional[int]]:
    """The probe's per-batch price, in the ledger's own denomination.

    Returns the pricing function and the per-item pixel canvas that is
    actually in force, which the run record carries so a reader can tell
    which denomination a fit is in.

    The canvas is resolved through the worker's own
    `packing.resolve_canvas_pixels`, with the registry's declaration standing
    in for the grant the orchestrator would have sent (`_canvas_pixels` has
    already applied the two registry rules) — so this tool walks exactly the
    order a worker walks: declaration first, the loaded impl's own attribute
    second, uncapped third.

    Pricing raw pixels here while the ledger prices capped ones would make
    the one comparison this tool exists for meaningless: the probe's slope is
    MiB per unit, and after run2's R7 a "unit" of a canvassed model is a
    *capped* pixel. Two slopes over different denominators cannot be
    compared at all, which is precisely the shape of run1's spurious 4.33x
    disagreement on nemotron (report §4, Q3/W1) — with the sides swapped.
    """
    unit = cost["unit"]
    aggregation = cost["aggregation"]
    canvas_pixels = packing.resolve_canvas_pixels(
        {"canvas_pixels": cost.get("canvas_pixels")}, instance, unit
    )

    def price(inputs) -> int:  # noqa: ANN001
        priced = packing.price_inputs(inputs, unit, canvas_pixels)
        return packing.batch_units(range(len(inputs)), priced, aggregation)

    return price, canvas_pixels


# --------------------------------------------------------------------------
# NVML (GPU identity and own-PID usage)
# --------------------------------------------------------------------------


class Nvml:
    def __init__(self) -> None:
        self.ok = False
        self.error: Optional[str] = None
        self._pynvml = None
        try:
            import pynvml

            pynvml.nvmlInit()
            self._pynvml = pynvml
            self.ok = True
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"

    def gpus(self) -> List[Dict[str, Any]]:
        if not self.ok:
            return []
        pynvml = self._pynvml
        out = []
        for index in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            uuid = pynvml.nvmlDeviceGetUUID(handle)
            name = pynvml.nvmlDeviceGetName(handle)
            info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            out.append({
                "index": index,
                "uuid": uuid.decode() if isinstance(uuid, bytes) else str(uuid),
                "name": name.decode() if isinstance(name, bytes) else str(name),
                "total_mb": int(info.total // MIB),
                "free_mb": int(info.free // MIB),
                "used_mb": int(info.used // MIB),
            })
        return out

    def handle_for_uuid(self, uuid: str):  # noqa: ANN201
        if not self.ok:
            return None
        pynvml = self._pynvml
        for index in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            got = pynvml.nvmlDeviceGetUUID(handle)
            got = got.decode() if isinstance(got, bytes) else str(got)
            if got == uuid:
                return handle
        return None

    def free_mb(self, handle) -> Optional[int]:  # noqa: ANN001
        if not self.ok or handle is None:
            return None
        try:
            return int(self._pynvml.nvmlDeviceGetMemoryInfo(handle).free // MIB)
        except Exception:
            return None

    def own_mb(self, handle) -> Optional[int]:  # noqa: ANN001
        """NVML per-process usage for this PID: the worker's `base_method="nvml"`."""
        if not self.ok or handle is None:
            return None
        pid = os.getpid()
        pynvml = self._pynvml
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


# --------------------------------------------------------------------------
# Theil-Sen, matching ledger.rs robust_fit
# --------------------------------------------------------------------------


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        return (ordered[mid - 1] + ordered[mid]) / 2.0
    return ordered[mid]


def theil_sen(samples: List[Tuple[int, int]], min_samples: int = 3) -> Optional[Dict[str, Any]]:
    """(units, delta_mb) -> the same fit `ledger.rs: robust_fit` would produce."""
    if len(samples) < min_samples:
        return None
    slopes: List[float] = []
    for index, (x0, y0) in enumerate(samples):
        for x1, y1 in samples[index + 1:]:
            dx = float(x1) - float(x0)
            if dx == 0.0:
                continue
            slopes.append((float(y1) - float(y0)) / dx)
    slope = _median(slopes)
    if slope is None or slope <= 0.0:
        return None
    intercepts = [float(y) - slope * float(x) for x, y in samples]
    intercept = _median(intercepts)
    if intercept is None:
        return None
    residuals = [abs(float(y) - (intercept + slope * float(x))) for x, y in samples]
    return {
        "slope_mb_per_unit": slope,
        "intercept_mb": intercept,
        "residual_mb": _median(residuals) or 0.0,
        "samples": len(samples),
    }


# --------------------------------------------------------------------------
# Corpus -> PredictionInput
# --------------------------------------------------------------------------


def load_items(corpus: Optional[str], group: Optional[str],
               kind: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not corpus:
        return [], None
    path = Path(corpus)
    if path.is_dir():
        path = path / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    items = manifest.get("items", [])
    if group:
        items = [item for item in items if item.get("group") == group]
    if kind:
        items = [item for item in items if item.get("kind") == kind]
    if not items:
        raise SystemExit(f"ceiling_probe: no corpus items match group={group} kind={kind}")
    return items, manifest.get("root")


def build_inputs(items: List[Dict[str, Any]], count: int, mode: str,
                 data_template: Dict[str, Any]):  # noqa: ANN201
    from inferio.inferio_types import PredictionInput

    inputs = []
    for index in range(count):
        item = items[index % len(items)]
        path = Path(item["abspath"])
        as_text = mode == "text" or (mode == "auto" and item["kind"] == "text")
        if as_text:
            payload = dict(data_template)
            payload["text"] = path.read_text(encoding="utf-8", errors="replace")
            inputs.append(PredictionInput(data=payload, file=None))
        else:
            inputs.append(PredictionInput(data=dict(data_template),
                                          file=path.read_bytes()))
    return inputs


# --------------------------------------------------------------------------
# Probe
# --------------------------------------------------------------------------


def parse_batches(text: Optional[str], max_batch: int) -> List[int]:
    if text:
        return [int(value) for value in text.replace(" ", "").split(",") if value]
    sizes: List[int] = []
    size = 1
    while size <= max_batch:
        sizes.append(size)
        size *= 2
    return sizes


def ran_whole_batch(record: Dict[str, Any]) -> bool:
    """Did this batch execute as **one** batch of `items`?

    Three ways it did not, and all three disqualify it as a boundary point:
    it raised; the classifier called it an out-of-memory condition; or the
    impl absorbed the failure itself. Absorption has two forms now — the
    halving loop swallowing an OOM (`absorbed_halvings`, reported through
    `oom`), and a **shape ceiling** the impl either hit or pre-empted
    (`index_limit_events`). run2's S4: both easyOCR bisects reported
    `largest_ok_items: 37` against a true 28, because at 29 and above CRAFT's
    pooling kernel overflowed its 32-bit index, the impl fell back to
    per-image processing, and the probe saw a slow success. A bisect is only
    a ground truth if "ok" means the whole batch ran.
    """
    return (
        bool(record["ok"])
        and not record["oom"]
        and not record.get("index_limit_events")
    )


def _boundary_key(record: Dict[str, Any]) -> str:
    """Which boundary a failing bisect probe marks.

    A shape ceiling and an out-of-memory condition are different facts about
    a model and the ledger acts on them differently, so the bisect records
    them under different keys rather than calling both "the first OOM".
    """
    return (
        "first_index_limit_items"
        if record.get("index_limit_events") and not record["oom"]
        else "first_oom_items"
    )


def main(argv: Optional[List[str]] = None) -> int:
    here = Path(__file__).resolve()
    parser = argparse.ArgumentParser(
        description="Ground-truth base/slope/OOM boundary for one impl.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--model", required=True, help="inference_id")
    parser.add_argument("--corpus", help="corpus.py manifest.json or its directory")
    parser.add_argument("--group", help="corpus group filter")
    parser.add_argument("--kind", help="corpus kind filter")
    parser.add_argument("--mode", choices=("auto", "file", "text"), default="auto")
    parser.add_argument("--data", default="{}",
                        help="JSON merged into every input's data dict")
    parser.add_argument("--device", type=int, default=0, help="NVML GPU index")
    parser.add_argument("--batches", help="explicit comma-separated batch sizes")
    parser.add_argument("--max-batch", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=1,
                        help="untimed single-item batches before measuring")
    parser.add_argument("--bisect-oom", action="store_true")
    parser.add_argument("--bisect-max", type=int, default=1024)
    parser.add_argument("--bisect-start", type=int, default=1,
                        help="first batch size the doubling phase probes")
    parser.add_argument("--bisect-budget", type=float, default=0.0,
                        help="seconds of bisect probing before the refinement "
                             "stops and reports the bracket (0 = no limit)")
    parser.add_argument("--repo", default=str(here.parents[2]),
                        help="repository root")
    parser.add_argument("--impl-dir", action="append", default=[])
    parser.add_argument("--registry", action="append", default=[])
    parser.add_argument("--out", help="JSON output path (default: stdout)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-loaded", action="store_true",
                        help="skip unload() at the end (leaves VRAM held)")
    args = parser.parse_args(argv)

    repo = Path(args.repo).resolve()
    registry = load_registries(registry_files(repo, args.registry))
    resolved = resolve_model(registry, args.model)
    impl_dirs = [str(repo / "python" / "inferio" / "impl"),
                 str(repo / "inferio_custom")] + list(args.impl_dir)
    items, corpus_root = load_items(args.corpus, args.group, args.kind)
    batches = parse_batches(args.batches, args.max_batch)
    data_template = json.loads(args.data)

    nvml = Nvml()
    gpus = nvml.gpus()
    gpu = next((entry for entry in gpus if entry["index"] == args.device), None)

    plan = {
        "schema": "ceiling_probe/1",
        "model": args.model,
        "impl_class": resolved["impl_class"],
        "config": resolved["config"],
        "cost": resolved["cost"],
        "impl_dirs": impl_dirs,
        "registry_files": [str(path) for path in registry_files(repo, args.registry)],
        "corpus": {"path": args.corpus, "root": corpus_root, "items": len(items),
                   "group": args.group, "kind": args.kind, "mode": args.mode},
        "batches": batches,
        "repeats": args.repeats,
        "device": gpu,
        "gpus": gpus,
        "nvml_error": nvml.error,
        "python": sys.version.split()[0],
    }

    if args.dry_run:
        print(json.dumps(plan, indent=1))
        return 0

    if gpu is None:
        raise SystemExit(
            f"ceiling_probe: NVML has no GPU with index {args.device} "
            f"(nvml error: {nvml.error})"
        )
    if not items:
        raise SystemExit("ceiling_probe: --corpus is required for a real run")

    # Pin exactly as the orchestrator does, BEFORE torch is imported.
    os.environ["CUDA_VISIBLE_DEVICES"] = gpu["uuid"]
    os.environ.setdefault("PANOPTIKON_DEVICE_PIN", gpu["uuid"])
    sys.path.insert(0, str(repo / "python"))

    handle = nvml.handle_for_uuid(gpu["uuid"])
    from inferio_worker.discovery import find_impl_class
    from inferio_worker import packing

    import logging

    logging.basicConfig(level=os.environ.get("INFERIO_WORKER_LOG_LEVEL", "WARNING"))
    impl_cls = find_impl_class(resolved["impl_class"], impl_dirs,
                               logging.getLogger("ceiling_probe"))

    free_before = nvml.free_mb(handle)
    load_started = time.monotonic()
    instance = impl_cls(**resolved["config"])
    instance.load()
    import torch

    torch.cuda.synchronize()
    load_seconds = time.monotonic() - load_started
    free_after = nvml.free_mb(handle)
    reserved_at_load = int(torch.cuda.memory_reserved() // MIB)
    allocated_at_load = int(torch.cuda.memory_allocated() // MIB)
    base_nvml = nvml.own_mb(handle)

    try:
        from inferio.impl import utils as impl_utils
    except Exception:
        impl_utils = None

    def halvings() -> int:
        reader = getattr(impl_utils, "total_oom_halvings", None) if impl_utils else None
        try:
            return int(reader()) if reader else 0
        except Exception:
            return 0

    def index_limit_events() -> int:
        """`inferio.impl.utils.total_index_limit_events()`, or 0.

        The *shape* ceiling, which is not a memory event and must never be
        counted as one: a kernel that cannot address the tensor a batch
        builds refuses it however much the GPU has free. run2 measured
        easyOCR falling off exactly this cliff at batch 29 while both
        `--bisect-oom` runs reported 37 as fine, because the impl turned the
        failure into a slower success (`run2-probes-report.md`, S1/S4).
        """
        reader = (getattr(impl_utils, "total_index_limit_events", None)
                  if impl_utils else None)
        try:
            return int(reader()) if reader else 0
        except Exception:
            return 0

    price, canvas_in_force = batch_pricer(packing, resolved["cost"], instance)

    def run_batch(count: int, repeat: int) -> Dict[str, Any]:
        inputs = build_inputs(items, count, args.mode, data_template)
        units = price(inputs)
        torch.cuda.synchronize()
        reserved_before = int(torch.cuda.memory_reserved() // MIB)
        torch.cuda.reset_peak_memory_stats()
        before_halvings = halvings()
        before_index_limits = index_limit_events()
        started = time.monotonic()
        error: Optional[str] = None
        failure: Optional[BaseException] = None
        ok = True
        try:
            instance.predict(inputs)
            torch.cuda.synchronize()
        except Exception as exc:
            ok = False
            failure = exc
            error = f"{type(exc).__name__}: {exc}"[:600]
        duration_ms = (time.monotonic() - started) * 1000.0
        peak_reserved = int(torch.cuda.max_memory_reserved() // MIB)
        peak_allocated = int(torch.cuda.max_memory_allocated() // MIB)
        reserved_after = int(torch.cuda.memory_reserved() // MIB)
        absorbed = max(0, halvings() - before_halvings)
        index_limits = max(0, index_limit_events() - before_index_limits)
        # The worker's own classifier, imported rather than reimplemented
        # (run2 R3, `packing.classify_oom`): three tiers over the whole
        # exception chain — a typed `torch.OutOfMemoryError`, our
        # `INFERENCE_OOM_*` markers, then a closed list of allocator spellings
        # plus "out of memory" scoped to a whole-word device token. This tool
        # used to match a bare `"out of memory"` substring, which is precisely
        # what run1's B11 showed to be wrong (a caption cache "out of memory
        # slots" is not a GPU out of memory), and a probe that draws the OOM
        # boundary somewhere the ledger does not is not a ground truth for it.
        oom_class = packing.classify_oom(failure, absorbed)
        oom = oom_class is not None
        return {
            "batch": count,
            "repeat": repeat,
            "units": units,
            "items": count,
            "ok": ok,
            "oom": bool(oom),
            "oom_class": oom_class,
            "absorbed_halvings": absorbed,
            "index_limit_events": index_limits,
            "duration_ms": round(duration_ms, 3),
            "peak_reserved_mb": peak_reserved,
            "peak_allocated_mb": peak_allocated,
            "reserved_before_mb": reserved_before,
            "reserved_after_mb": reserved_after,
            "nvml_own_mb": nvml.own_mb(handle),
            "gpu_free_mb": nvml.free_mb(handle),
            "delta_mb": max(0, peak_reserved - reserved_at_load),
            "error": error,
        }

    for _ in range(max(0, args.warmup)):
        try:
            run_batch(1, -1)
        except Exception:
            break

    records: List[Dict[str, Any]] = []
    for count in batches:
        for repeat in range(args.repeats):
            record = run_batch(count, repeat)
            records.append(record)
            print(
                f"batch {count:5d} units {record['units']:9d} "
                f"peak_reserved {record['peak_reserved_mb']:6d} MiB "
                f"delta {record['delta_mb']:6d} MiB "
                f"nvml {record['nvml_own_mb']} "
                f"{record['duration_ms']:.0f} ms"
                + ("  OOM" if record["oom"] else "")
                + ("  INDEX-LIMIT" if record["index_limit_events"] else "")
                + (f"  ERROR {record['error']}" if record["error"] else ""),
                file=sys.stderr,
            )
            if not ran_whole_batch(record):
                break
        if records and not ran_whole_batch(records[-1]):
            break

    clean = [
        (record["units"], record["delta_mb"])
        for record in records
        if ran_whole_batch(record) and record["delta_mb"] > 0
    ]
    fit = theil_sen(clean)

    def settle_after_failure() -> None:
        """Return the allocator to a clean state between bisect probes.

        An OOM leaves the caching allocator fragmented and holding blocks the
        next attempt cannot reuse, so without this the search would find a
        boundary that depends on the order it probed in rather than on the
        model.
        """
        try:
            torch.cuda.empty_cache()
            torch.cuda.synchronize()
        except Exception:
            pass

    bisect: Optional[Dict[str, Any]] = None
    if args.bisect_oom:
        bisect = {"free_mb_at_start": nvml.free_mb(handle),
                  "reserved_at_bisect_start_mb": int(torch.cuda.memory_reserved() // MIB),
                  "trace": [],
                  "largest_ok_items": None, "largest_ok_units": None,
                  "first_oom_items": None,
                  "first_index_limit_items": None, "stopped_early": False}
        bisect_started = time.monotonic()
        low, high = 1, args.bisect_max
        # Grow first: double until something fails or the ceiling is hit.
        probe = max(1, args.bisect_start)
        while probe <= args.bisect_max:
            record = run_batch(probe, -2)
            bisect["trace"].append({"items": probe, "ok": ran_whole_batch(record),
                                    "units": record["units"], "oom": record["oom"],
                                    "absorbed_halvings": record["absorbed_halvings"],
                                    "index_limit_events": record["index_limit_events"],
                                    "error": record["error"]})
            if ran_whole_batch(record):
                low = probe
                bisect["largest_ok_items"] = probe
                bisect["largest_ok_units"] = record["units"]
                probe *= 2
            else:
                high = probe
                bisect[_boundary_key(record)] = probe
                settle_after_failure()
                break
        else:
            high = args.bisect_max
        while high - low > 1:
            if args.bisect_budget > 0 and (
                    time.monotonic() - bisect_started > args.bisect_budget):
                bisect["stopped_early"] = True
                break
            mid = (low + high) // 2
            record = run_batch(mid, -2)
            bisect["trace"].append({"items": mid, "ok": ran_whole_batch(record),
                                    "units": record["units"], "oom": record["oom"],
                                    "absorbed_halvings": record["absorbed_halvings"],
                                    "index_limit_events": record["index_limit_events"],
                                    "error": record["error"]})
            if ran_whole_batch(record):
                low = mid
                bisect["largest_ok_items"] = mid
                bisect["largest_ok_units"] = record["units"]
            else:
                high = mid
                bisect[_boundary_key(record)] = mid
                settle_after_failure()
        bisect["low_items"] = low
        bisect["high_items"] = high

    result = {
        **plan,
        # The declared canvas is in `plan`; this is the one that priced every
        # batch below, which is the impl's own attribute for a model the
        # registry cannot state statically (dots.ocr).
        "cost": {**resolved["cost"], "canvas_pixels_in_force": canvas_in_force},
        "torch": torch.__version__,
        "dtype": _resolve_dtype(instance),
        "device": {**gpu, "cuda_visible_devices": os.environ["CUDA_VISIBLE_DEVICES"]},
        "load": {
            "seconds": round(load_seconds, 3),
            "base_nvml_mb": base_nvml,
            "base_free_delta_mb": (
                None if free_before is None or free_after is None
                else max(0, free_before - free_after)
            ),
            "reserved_at_load_mb": reserved_at_load,
            "allocated_at_load_mb": allocated_at_load,
            "free_before_mb": free_before,
            "free_after_mb": free_after,
        },
        "batches": records,
        "fit": fit,
        "bisect": bisect,
    }

    if not args.keep_loaded:
        try:
            instance.unload()
        except Exception:
            pass
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass

    text = json.dumps(result, indent=1, default=str)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text + "\n", encoding="utf-8")
        print(f"ceiling_probe: wrote {args.out}", file=sys.stderr)
    else:
        print(text)
    if fit:
        print(
            f"ceiling_probe: base(nvml)={base_nvml} MiB  "
            f"slope={fit['slope_mb_per_unit']:.6g} MiB/unit  "
            f"intercept={fit['intercept_mb']:.4g} MiB  "
            f"residual={fit['residual_mb']:.4g} MiB  n={fit['samples']}",
            file=sys.stderr,
        )
    return 0


def _resolve_dtype(instance: Any) -> Optional[str]:
    for attribute in ("dtype", "torch_dtype", "_dtype"):
        value = getattr(instance, attribute, None)
        if value is not None:
            return str(value)
    model = getattr(instance, "model", None)
    parameters = getattr(model, "parameters", None)
    if callable(parameters):
        try:
            return str(next(parameters()).dtype)
        except Exception:
            return None
    return None


if __name__ == "__main__":
    raise SystemExit(main())
