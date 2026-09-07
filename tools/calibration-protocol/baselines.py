#!/usr/bin/env python3
"""baselines.py - build a shipped calibration baseline from a measured store.

The production process is in `docs/model-cost-measurement.md`, "Producing a
shipped baseline": measure on Linux/CUDA, then generate the Windows rows for
the ids the registry says run the same kernels there. This tool is the second
half. It takes a calibration store (a local `calibration.toml`, a leg's
`calibration.after.toml`, or an already-shipped baseline) plus the model
registry, and writes the file that goes in
`python/inferio/config/calibration/`.

- Only **linux/cuda** rows are accepted as measurements; any other measured
  platform is refused, because nothing here can tell whether its numbers
  travel.
- A row is copied to another platform only when its id declares
  `metadata.cost.platform_copies` in the registry. Absent means do not copy.
- A copy carries the Linux `base_mb` and says so with `base_platform`, keeps
  `measured_at`, and names this tool in `generator`.
- Local-authority fields (`max_units_measured`, `local_samples`,
  `knee_clean_windows`, the sample ring) are dropped: they are one machine's
  evidence, and the orchestrator strips them on import anyway.
- A row with no `slope_mb_per_unit` is refused: it prices nothing, and a
  ratchet anchor with no slope beside it confers nothing either.

Idempotent: rerunning it on its own output reproduces it byte for byte,
because generated rows are recognised by `base_platform` and regenerated from
their source rather than treated as measurements.

Usage
-----
    baselines.py --store <calibration.toml> --out <baseline.toml>
    baselines.py --store <calibration.toml> --dry-run     # stdout, no write

A store written before the architecture re-key (`schema = 2`) has no `arch`
field. It can still be converted, but the architecture has to be stated:
`--arch sm_120`. The tool never guesses one from the `gpu` name.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# The schema the orchestrator reads (`panoptikon/src/inferio/calibration.rs`).
SCHEMA = 3
# What a measurement may have been taken on.
MEASURED_ON = ("linux", "cuda")
# Platforms a CUDA row may be copied to. macOS is not one: a cuda-keyed row
# can never answer an mps lookup.
COPYABLE_TO = ("windows",)
GENERATOR = "calibration-protocol baselines.py"

# Field order, matching the Rust struct so a generated file and a written one
# read the same. Everything after `aggregation` is measurement.
KEY_FIELDS = (
    "inference_id", "epoch", "arch", "gpu", "platform", "backend", "torch",
    "dtype", "unit", "aggregation",
)
VALUE_FIELDS = (
    "base_mb", "base_method", "base_platform", "dtype_method",
    "slope_mb_per_unit", "knee_units", "samples", "residual_mb",
    "measured_at", "generator",
)
# One machine's own evidence; never shipped.
LOCAL_ONLY = (
    "max_units_measured", "local_samples", "knee_clean_windows",
    "sample_units", "sample_delta_mb",
)


class BaselineError(Exception):
    """A refusal: the input cannot be turned into a baseline."""


# --- Input ---


def read_store(path: Path, arch: Optional[str]) -> List[Dict[str, Any]]:
    """Every `[[profile]]` of `path`, with schema-2 rows given `arch`."""
    with path.open("rb") as handle:
        doc = tomllib.load(handle)
    schema = doc.get("schema")
    if schema not in (2, SCHEMA):
        raise BaselineError(
            f"{path}: schema {schema!r} is neither {SCHEMA} nor the "
            f"convertible schema 2"
        )
    rows = list(doc.get("profile", []))
    if schema == 2:
        if not arch:
            raise BaselineError(
                f"{path}: a schema-2 store predates the architecture key and "
                f"names no `arch`; pass --arch (sm_120, gfx1100, ...) for the "
                f"card that measured it"
            )
        for row in rows:
            row["arch"] = arch
    elif arch:
        raise BaselineError(
            f"{path}: --arch only converts a schema-2 store; this one already "
            f"states its own architecture"
        )
    return rows


def read_allowlist(path: Path) -> Dict[str, Tuple[str, ...]]:
    """`{inference_id: platforms}` from `metadata.cost.platform_copies`."""
    with path.open("rb") as handle:
        doc = tomllib.load(handle)
    allowed: Dict[str, Tuple[str, ...]] = {}
    for group_name, group in (doc.get("group") or {}).items():
        for id_name, entry in (group.get("inference_ids") or {}).items():
            cost = ((entry or {}).get("metadata") or {}).get("cost") or {}
            copies = cost.get("platform_copies")
            if copies is None:
                continue
            full = f"{group_name}/{id_name}"
            if not isinstance(copies, list) or not all(
                isinstance(item, str) for item in copies
            ):
                raise BaselineError(
                    f"{full}: metadata.cost.platform_copies must be a list of "
                    f"platform names"
                )
            bad = [item for item in copies if item not in COPYABLE_TO]
            if bad:
                raise BaselineError(
                    f"{full}: metadata.cost.platform_copies names {bad}, and "
                    f"only {list(COPYABLE_TO)} can be copied to"
                )
            allowed[full] = tuple(copies)
    return allowed


# --- Generation ---


def generate(
    rows: Iterable[Dict[str, Any]], allowlist: Dict[str, Tuple[str, ...]]
) -> List[Dict[str, Any]]:
    """Measured rows plus their allowed copies, sorted for a stable file."""
    measured: List[Dict[str, Any]] = []
    for row in rows:
        # A previous run's copy: dropped, and regenerated below from the row
        # it was copied from. That is what makes this idempotent.
        if row.get("base_platform"):
            continue
        platform, backend = row.get("platform"), row.get("backend")
        if (platform, backend) != MEASURED_ON:
            raise BaselineError(
                f"{row.get('inference_id')}: measured on "
                f"{platform}/{backend}, and a baseline is generated from "
                f"{'/'.join(MEASURED_ON)} rows only"
            )
        # A row with no fit prices nothing, and its ratchet anchor confers
        # nothing either: there is no slope to bound the anchor in MB with.
        # Refused rather than shipped as a row the importer would log about.
        if not float(row.get("slope_mb_per_unit") or 0.0) > 0.0:
            raise BaselineError(
                f"{row.get('inference_id')}: no slope_mb_per_unit, so the row "
                "prices nothing and its anchor confers nothing; measure it "
                "further before shipping it"
            )
        measured.append({key: value for key, value in row.items()
                         if key not in LOCAL_ONLY})

    out = list(measured)
    for row in measured:
        for platform in allowlist.get(row["inference_id"], ()):
            copy = dict(row)
            copy["platform"] = platform
            copy["base_platform"] = row["platform"]
            copy["generator"] = GENERATOR
            out.append(copy)
    out.sort(key=lambda row: tuple(
        str(row.get(field, "")) for field in
        ("inference_id", "platform", "arch", "torch", "dtype", "unit")
    ))
    return out


# --- Output ---


def render(rows: List[Dict[str, Any]]) -> str:
    """The baseline file. Written by hand so field order is the struct's."""
    parts = [f"schema = {SCHEMA}\n"]
    for row in rows:
        parts.append("\n[[profile]]\n")
        for field in KEY_FIELDS + VALUE_FIELDS:
            if field in row and row[field] is not None:
                parts.append(f"{field} = {_toml_value(row[field])}\n")
    return "".join(parts)


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, float):
        return repr(value)
    return str(value)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="baselines.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    here = Path(__file__).resolve().parents[2]
    parser.add_argument("--store", required=True, type=Path,
                        help="the measured calibration store to read")
    parser.add_argument("--registry", type=Path,
                        default=here / "python/inferio/config/inference.toml",
                        help="the model registry holding the allowlist")
    parser.add_argument("--arch", default=None,
                        help="architecture for a schema-2 store, e.g. sm_120")
    parser.add_argument("--out", type=Path, default=None,
                        help="where to write; omit or --dry-run for stdout")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the file instead of writing it")
    args = parser.parse_args(argv)

    try:
        rows = generate(
            read_store(args.store, args.arch), read_allowlist(args.registry)
        )
    except BaselineError as error:
        print(f"baselines.py: {error}", file=sys.stderr)
        return 2
    body = render(rows)
    if args.dry_run or args.out is None:
        sys.stdout.write(body)
    else:
        args.out.write_text(body, encoding="utf-8")
        copies = sum(1 for row in rows if row.get("base_platform"))
        print(f"{args.out}: {len(rows) - copies} measured, {copies} generated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
