#!/usr/bin/env python3
"""Outro-detector equivalence harness (docs/video-outro-detection-design.md §12).

Runs **both** implementations over the same deterministic sample of real
files and requires identical verdicts and identical K values:

* the Python reference of §3.3 (`reference.py` in this directory), and
* the Rust detector `media_tools::outro::detect_outro`, driven by the
  `#[cfg(test)]` harness `media_tools::outro_equivalence` (a `cargo test`
  target, so the shipped binary gains no surface).

Sampling is `ORDER BY sha256 LIMIT n` over each index DB's video items —
fixed and reproducible, with no RNG to seed. Databases are opened
**read-only** (`mode=ro`): they may be in use.

Run it from the repo root, **from PowerShell**: most media lives on the `Z:`
network mount (`\\\\192.168.1.16\\z`), which the Bash sandbox cannot reach.

    python tools/outro-equivalence/run_equivalence.py --sample 150

Useful flags:

  --sample N          files per DB (default 150)
  --dbs a,b,c         override the DB list
  --jobs N            worker processes/threads per engine (default 8)
  --out DIR           where manifest/results/report land (default this dir)
  --reuse-rust        keep an existing rust.jsonl instead of re-running cargo
  --reuse-python      keep an existing python.jsonl
  --skip-rust         compare against an existing rust.jsonl

The pass bar (§12): zero verdict mismatches, zero K mismatches, negatives all
rejected, and positive K clustering on the discrete generation values
(2.00 / 3.00 / 4.00). A file missing from either result stream fails the run
outright — including when it is missing from *both*, which would otherwise
read as agreement over something neither engine ever measured.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reference  # noqa: E402

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

# Design §12's own reproduction list: positives first, then the general and
# adversarial sets that must stay at zero.
POSITIVE_DBS = ["tiktok", "camera"]
NEGATIVE_DBS = ["screenshots", "default", "rustest", "rustest2"]

# `items.type` is a mime type; this is the index-friendly form of "video/*"
# used everywhere in the codebase.
VIDEO_ITEMS = """
    SELECT i.sha256, i.width, i.height, i.duration, MIN(f.path) AS path
    FROM items i
    JOIN files f ON f.item_id = i.id
    WHERE i.type >= 'video/' AND i.type < 'video0' AND f.available = 1
    GROUP BY i.id
    ORDER BY i.sha256
    LIMIT ?
"""


def sample_db(db: str, count: int) -> list[dict]:
    path = os.path.join(REPO, "data", "index", db, "index.db")
    if not os.path.exists(path):
        raise SystemExit(f"no index DB at {path}")
    # Read-only, and never creating: the user's databases may be in use.
    con = sqlite3.connect(f"file:{path.replace(os.sep, '/')}?mode=ro", uri=True)
    try:
        rows = con.execute(VIDEO_ITEMS, (count,)).fetchall()
        total = con.execute(
            "SELECT COUNT(*) FROM items WHERE type >= 'video/' AND type < 'video0'"
        ).fetchone()[0]
    finally:
        con.close()
    sampled = [
        {
            "db": db,
            "sha256": sha256,
            "width": width,
            "height": height,
            "duration": duration,
            "path": path,
        }
        for (sha256, width, height, duration, path) in rows
    ]
    print(f"  {db}: {total} video items, sampled {len(sampled)}")
    return sampled


def write_manifest(jobs: list[dict], out: str) -> None:
    with open(out, "w", encoding="utf-8", newline="\n") as handle:
        for job in jobs:
            width = job["width"] if job["width"] else ""
            height = job["height"] if job["height"] else ""
            handle.write(f"{width}\t{height}\t{job['path']}\n")


def run_rust(manifest: str, results: str, jobs: int) -> float:
    env = dict(os.environ)
    env["OUTRO_EQUIV_INPUT"] = manifest
    env["OUTRO_EQUIV_OUTPUT"] = results
    env["OUTRO_EQUIV_JOBS"] = str(jobs)
    # Pin both sides to the same binaries: the detector otherwise resolves
    # ffmpeg through the managed venv's static-ffmpeg, and a different build
    # is a divergence the comparison would blame on the algorithm.
    env["OUTRO_EQUIV_FFMPEG"] = reference.FFMPEG
    env["OUTRO_EQUIV_FFPROBE"] = reference.FFPROBE
    started = time.perf_counter()
    proc = subprocess.run(
        # `--release`: the per-frame pixel loops are a debug build's worst
        # case, and an unoptimized run turns a few minutes into an hour
        # without changing a single verdict.
        [
            "cargo",
            "test",
            "--release",
            "--bin",
            "panoptikon",
            "outro_equivalence",
            "--",
            "--ignored",
            "--nocapture",
        ],
        cwd=REPO,
        env=env,
    )
    if proc.returncode != 0:
        raise SystemExit(f"the Rust harness failed (exit {proc.returncode})")
    return time.perf_counter() - started


def _first_line(args: list[str], cwd: str | None = None) -> str:
    """First line of a command's output, for the report's provenance block.
    Never fatal: a missing `git` must not stop a validation run."""
    try:
        proc = subprocess.run(
            args, cwd=cwd, capture_output=True, text=True, errors="replace"
        )
    except OSError as err:
        return f"<unavailable: {err}>"
    if proc.returncode != 0:
        return f"<exit {proc.returncode}>"
    return (proc.stdout or proc.stderr).strip().splitlines()[0].strip()


def _reference_one(path: str) -> str:
    return reference.detect_outro(path).as_json()


def run_python(jobs_list: list[dict], results: str, jobs: int) -> float:
    started = time.perf_counter()
    paths = [job["path"] for job in jobs_list]
    done = 0
    with open(results, "w", encoding="utf-8", newline="\n") as handle:
        with ProcessPoolExecutor(max_workers=jobs) as pool:
            for line in pool.map(_reference_one, paths, chunksize=1):
                handle.write(line + "\n")
                done += 1
                if done % 50 == 0:
                    print(
                        f"  reference: {done}/{len(paths)} "
                        f"({time.perf_counter() - started:.1f}s)",
                        flush=True,
                    )
    return time.perf_counter() - started


def load_jsonl(path: str) -> tuple[dict[str, dict], dict]:
    """Records keyed by normalised path, plus any header record.

    A line without a `path` is a header (the Rust harness emits one carrying
    the ffmpeg it actually resolved), never a result.
    """
    records: dict[str, dict] = {}
    header: dict = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if "path" not in record:
                header = record
                continue
            records[os.path.normcase(record["path"])] = record
    return records, header


def outcome(record: dict | None) -> tuple:
    """The comparable shape of one record: verdict and K, or the error class.

    Errors compare by **class** — `spawn` (ffmpeg never ran, never a verdict
    on the media) vs `decode` (it ran and failed) map to different ledger
    outcomes in §7.2, so a spawn-vs-decode pairing is a divergence. The
    messages themselves are not compared: the two sides word them
    differently by design (the reference has its own ffprobe step), and §12
    asks for identical *verdicts*, not identical prose.

    `missing` is deliberately its own shape and is never treated as an
    agreeing error: two absent records are a harness failure, not a
    concordant verdict.
    """
    if record is None:
        return ("missing",)
    if record.get("status") == "error":
        return ("error", record.get("class"))
    if record.get("verdict") == "tiktok_card":
        return ("tiktok_card", record.get("k"))
    return ("none", record.get("reason"))


def compare(jobs_list: list[dict], rust: dict, python: dict) -> dict:
    per_db: dict[str, dict] = collections.defaultdict(
        lambda: {
            "files": 0,
            "verdict_match": 0,
            "verdict_mismatch": 0,
            "k_match": 0,
            "k_mismatch": 0,
            "both_error": 0,
            "rust_error_only": 0,
            "python_error_only": 0,
            "missing": 0,
            "accepted": 0,
            "accepted_rust": 0,
            "accepted_python": 0,
            "rust_reasons": collections.Counter(),
            "python_reasons": collections.Counter(),
            "k_values": collections.Counter(),
            "rust_ms": 0,
            "python_ms": 0,
        }
    )
    mismatches: list[dict] = []
    errors: list[dict] = []
    missing: list[dict] = []

    for job in jobs_list:
        key = os.path.normcase(job["path"])
        rust_record, python_record = rust.get(key), python.get(key)
        stats = per_db[job["db"]]
        stats["files"] += 1
        stats["rust_ms"] += (rust_record or {}).get("ms") or 0
        stats["python_ms"] += (python_record or {}).get("ms") or 0

        rust_outcome, python_outcome = outcome(rust_record), outcome(python_record)
        # A record absent from a stream is a hole in the harness, and a hole
        # on *both* sides is the dangerous one: it looks like agreement while
        # meaning nothing was measured at all. It fails the run, always, and
        # never reaches the error-agreement branch below.
        if rust_outcome[0] == "missing" or python_outcome[0] == "missing":
            stats["missing"] += 1
            missing.append(
                {
                    "db": job["db"],
                    "path": job["path"],
                    "in_rust": rust_record is not None,
                    "in_python": python_record is not None,
                }
            )
            continue

        rust_error = rust_outcome[0] == "error"
        python_error = python_outcome[0] == "error"
        if rust_record.get("verdict") == "tiktok_card":
            stats["accepted_rust"] += 1
        if python_record.get("verdict") == "tiktok_card":
            stats["accepted_python"] += 1
        if rust_error and python_error:
            stats["both_error"] += 1
        elif rust_error:
            stats["rust_error_only"] += 1
        elif python_error:
            stats["python_error_only"] += 1

        if rust_error or python_error:
            # Agreement means the *same class*, not merely "both unhappy":
            # `spawn` (ffmpeg never ran) and `decode` (it ran and failed) take
            # different routes through the visuals ledger (§7.2), so pairing
            # one against the other is a divergence — as is an error on one
            # side only. Reported as an error class rather than a verdict
            # mismatch so the two populations stay legible.
            errors.append(
                {
                    "db": job["db"],
                    "path": job["path"],
                    "dims": [job["width"], job["height"]],
                    "agreed": rust_outcome == python_outcome,
                    "class": [rust_outcome[1:], python_outcome[1:]],
                    "rust": rust_record,
                    "python": python_record,
                }
            )
            continue

        if rust_outcome[0] == "none":
            stats["rust_reasons"][rust_outcome[1]] += 1
        if python_outcome[0] == "none":
            stats["python_reasons"][python_outcome[1]] += 1

        # Verdict equality first (accept vs reject, and which rule fired),
        # then K equality — exact, as §12 requires. Both sides emit shortest
        # round-trip floats, so the doubles compare bit-for-bit.
        verdict_same = rust_outcome[0] == python_outcome[0] and (
            rust_outcome[0] != "none" or rust_outcome[1] == python_outcome[1]
        )
        k_same = rust_record.get("k") == python_record.get("k")
        if verdict_same:
            stats["verdict_match"] += 1
        else:
            stats["verdict_mismatch"] += 1
        if k_same:
            stats["k_match"] += 1
        else:
            stats["k_mismatch"] += 1
        # `accepted` and the K histogram are the *agreed* population only, so
        # the summary table cannot quietly present one engine's numbers as
        # the run's findings when the two disagree. `accepted_rust` and
        # `accepted_python` above keep both sides visible either way.
        if verdict_same and k_same and rust_outcome[0] == "tiktok_card":
            stats["accepted"] += 1
            stats["k_values"][round(float(rust_record["k"]), 4)] += 1
        if not (verdict_same and k_same):
            mismatches.append(
                {
                    "db": job["db"],
                    "path": job["path"],
                    "dims": [job["width"], job["height"]],
                    "duration": job["duration"],
                    "rust": rust_record,
                    "python": python_record,
                }
            )

    return {
        "per_db": {
            db: {
                **{
                    key: value
                    for key, value in stats.items()
                    if not isinstance(value, collections.Counter)
                },
                "rust_reasons": dict(stats["rust_reasons"]),
                "python_reasons": dict(stats["python_reasons"]),
                "k_values": dict(sorted(stats["k_values"].items())),
            }
            for db, stats in per_db.items()
        },
        "mismatches": mismatches,
        "errors": errors,
        "missing": missing,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=150, help="files per DB")
    parser.add_argument("--dbs", default=",".join(POSITIVE_DBS + NEGATIVE_DBS))
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--reuse-rust", action="store_true")
    parser.add_argument("--reuse-python", action="store_true")
    parser.add_argument(
        "--skip-rust",
        action="store_true",
        help="compare against an existing rust.jsonl; it must still cover the "
        "whole sample, or the run fails as incomplete",
    )
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    manifest = os.path.join(args.out, "manifest.tsv")
    rust_results = os.path.join(args.out, "rust.jsonl")
    python_results = os.path.join(args.out, "python.jsonl")
    report_path = os.path.join(args.out, "report.json")

    # Resolve the toolchain to absolute paths *once* and push them back into
    # the environment: the reference's worker processes re-import the module
    # and read them from there, and the Rust harness asserts the path it
    # resolved is the one it was handed. A bare `ffmpeg` would leave both
    # sides trusting an unrecorded PATH lookup.
    tools: dict[str, str] = {}
    for tool, configured in (("ffmpeg", reference.FFMPEG), ("ffprobe", reference.FFPROBE)):
        found = shutil.which(configured) or (
            configured if os.path.isfile(configured) else None
        )
        if not found:
            raise SystemExit(f"{tool} not found ({configured})")
        found = os.path.abspath(found)
        tools[tool] = found
        os.environ[f"OUTRO_EQUIV_{tool.upper()}"] = found
        print(f"{tool}: {found}")
    reference.FFMPEG, reference.FFPROBE = tools["ffmpeg"], tools["ffprobe"]
    provenance = {
        "dbs": args.dbs,
        "ffmpeg": tools["ffmpeg"],
        "ffprobe": tools["ffprobe"],
        "ffmpeg_version": _first_line([tools["ffmpeg"], "-version"]),
        "commit": _first_line(["git", "rev-parse", "HEAD"], cwd=REPO),
    }
    print(f"ffmpeg version: {provenance['ffmpeg_version']}")
    print(f"commit: {provenance['commit']}")

    print("sampling:")
    jobs_list: list[dict] = []
    skipped: dict[str, int] = collections.Counter()
    for db in args.dbs.split(","):
        db = db.strip()
        if not db:
            continue
        for job in sample_db(db, args.sample):
            if not job["path"] or not os.path.exists(job["path"]):
                skipped[db] += 1
                continue
            jobs_list.append(job)
    if skipped:
        print(f"  skipped (not on disk): {dict(skipped)}")
    if not jobs_list:
        raise SystemExit("nothing to run")
    print(f"  total: {len(jobs_list)} files")

    write_manifest(jobs_list, manifest)

    rust_seconds = 0.0
    if not args.skip_rust and not (args.reuse_rust and os.path.exists(rust_results)):
        print("running the Rust detector (cargo test harness)...")
        rust_seconds = run_rust(manifest, rust_results, args.jobs)
    python_seconds = 0.0
    if not (args.reuse_python and os.path.exists(python_results)):
        print("running the Python reference...")
        python_seconds = run_python(jobs_list, python_results, args.jobs)

    rust, rust_header = (
        load_jsonl(rust_results) if os.path.exists(rust_results) else ({}, {})
    )
    python, _ = load_jsonl(python_results)

    # Completeness before comparison: a stream that is short by a file makes
    # the comparison silently smaller, and if *both* are short by the same
    # file it looks like a clean run over a sample that was never measured.
    # The manifest may repeat a path (rustest is a subset of rustest2), so
    # the expectation is the unique set, not the row count.
    expected = {os.path.normcase(job["path"]) for job in jobs_list}
    for name, records in (("rust", rust), ("python", python)):
        absent, extra = expected - set(records), set(records) - expected
        if absent or extra:
            raise SystemExit(
                f"{name}.jsonl is not the manifest: {len(records)} records for "
                f"{len(expected)} unique files ({len(absent)} missing, "
                f"{len(extra)} unexpected). First missing: "
                f"{sorted(absent)[:3] or None}"
            )

    # The Rust harness reports the ffmpeg it actually resolved; if that is not
    # the pinned one the two engines were never compared on equal footing.
    if rust_header.get("ffmpeg") and os.path.normcase(
        rust_header["ffmpeg"]
    ) != os.path.normcase(tools["ffmpeg"]):
        raise SystemExit(
            f"the Rust harness resolved {rust_header['ffmpeg']}, not the pinned "
            f"{tools['ffmpeg']}"
        )

    report = compare(jobs_list, rust, python)
    report["sample"] = args.sample
    report["skipped"] = dict(skipped)
    report["provenance"] = {**provenance, "rust_harness": rust_header}
    report["files"] = {"rows": len(jobs_list), "unique": len(expected)}
    report["wall_clock"] = {"rust_s": rust_seconds, "python_s": python_seconds}
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=str)

    verdict_mismatch = sum(s["verdict_mismatch"] for s in report["per_db"].values())
    k_mismatch = sum(s["k_mismatch"] for s in report["per_db"].values())
    print()
    header = (
        f"{'db':<12}{'files':>7}{'ok':>6}{'v-mis':>7}{'k-mis':>7}"
        f"{'accept':>8}{'err=':>6}{'errR':>6}{'errP':>6}{'miss':>6}"
    )
    print(header)
    for db, stats in report["per_db"].items():
        # `accept` is the agreed population; when the engines disagree on it
        # the raw per-side counts are printed rather than hidden.
        accepted = str(stats["accepted"])
        if stats["accepted_rust"] != stats["accepted_python"]:
            accepted = f"{stats['accepted_rust']}/{stats['accepted_python']}"
        print(
            f"{db:<12}{stats['files']:>7}{stats['verdict_match']:>6}"
            f"{stats['verdict_mismatch']:>7}{stats['k_mismatch']:>7}"
            f"{accepted:>8}{stats['both_error']:>6}"
            f"{stats['rust_error_only']:>6}{stats['python_error_only']:>6}"
            f"{stats['missing']:>6}"
        )
    print()
    for db, stats in report["per_db"].items():
        if stats["k_values"]:
            print(f"{db} K distribution: {stats['k_values']}")
        if stats["rust_reasons"]:
            print(f"{db} rejections (rust):   {stats['rust_reasons']}")
            print(f"{db} rejections (python): {stats['python_reasons']}")
    print()
    print(f"wall clock: rust {rust_seconds:.1f}s, python {python_seconds:.1f}s")
    print(f"report: {report_path}")
    if report["missing"]:
        print(f"FAIL: {len(report['missing'])} files were measured by neither engine")
        for row in report["missing"][:20]:
            print(f"  {row['path']} (rust={row['in_rust']}, python={row['in_python']})")
        return 1
    if verdict_mismatch or k_mismatch:
        print(f"FAIL: {verdict_mismatch} verdict, {k_mismatch} K mismatches")
        for row in report["mismatches"][:20]:
            print(f"  {row['path']}\n    rust={row['rust']}\n    python={row['python']}")
        return 1
    divergent = [row for row in report["errors"] if not row.get("agreed")]
    if divergent:
        print(f"FAIL: {len(divergent)} files errored on one side only, or in "
              f"different classes")
        for row in divergent[:20]:
            print(f"  {row['path']}\n    rust={row['rust']}\n    python={row['python']}")
        return 1
    print("PASS: identical verdicts and identical K values")
    return 0


if __name__ == "__main__":
    sys.exit(main())
