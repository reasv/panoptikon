#!/usr/bin/env python3
"""Quant recall harness (docs/vector-index-design.md, Validation).

Runs the same semantic queries through exact search and each ready quant
profile on a live gateway, and reports overlap@N plus rank agreement —
Panoptikon's compare-methods philosophy as a recall QA tool. Stdlib only.

Usage:
    python tools/quant-recall/run_recall.py \
        --api-url http://127.0.0.1:6342 --index-db default \
        --queries tools/quant-recall/queries.txt

The gateway must be running with inference available (queries are embedded
server-side). Results are only meaningful once the quant profiles are Ready
(scan page → Vector quantization). Exact search stays first-class precisely
so this comparison is always available.
"""

import argparse
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

DEFAULT_QUERIES = [
    "a photograph of a cat",
    "text document with a table of numbers",
    "sunset over the ocean",
    "a diagram or chart",
    "people in a group photo",
]
TOP_N = (10, 50, 100)
PAGE_SIZE = max(TOP_N)


def api(base, path, params, body=None):
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def search(base, dbs, filter_element, order_dir="asc"):
    body = {
        "query": {"and_": [filter_element]},
        "entity": "file",
        "select": ["sha256", "path"],
        "page": 1,
        "page_size": PAGE_SIZE,
        "results": True,
        "count": False,
        "check_path": False,
        # The cache would make repeat timings meaningless; bypass skips both
        # the read and the write.
        "cache": False,
        "prefetch_rows": 0,
    }
    start = time.perf_counter()
    data = api(base, "/api/search/pql", dbs, body)
    elapsed = time.perf_counter() - start
    rows = [row["sha256"] for row in data.get("results", [])]
    return rows, elapsed


def semantic_filter(kind, query, model, index, variant=None):
    args = {"query": query, "model": model, "index": index}
    if variant:
        args["variant"] = variant
    return {
        "order_by": True,
        "direction": "asc",
        "row_n": True,
        "row_n_direction": "asc",
        kind: args,
    }


def similar_filter(target, model, index, variant=None):
    args = {
        "target": target,
        "model": model,
        "index": index,
        "force_distance_function": False,
    }
    if variant:
        args["variant"] = variant
    return {
        "order_by": True,
        "direction": "asc",
        "row_n": True,
        "row_n_direction": "asc",
        "similar_to": args,
    }


def overlap(exact_rows, quant_rows, n):
    exact_top = set(exact_rows[:n])
    if not exact_top:
        return None
    quant_top = set(quant_rows[:n])
    return len(exact_top & quant_top) / len(exact_top)


def compare(name, base, dbs, make_filter, profiles, report):
    exact_rows, exact_time = search(base, dbs, make_filter("exact", None))
    if not exact_rows:
        print(f"  {name}: no results, skipped")
        return
    entry = {"name": name, "exact_time": exact_time, "profiles": {}}
    for profile in profiles:
        quant_rows, quant_time = search(base, dbs, make_filter("quant", profile))
        overlaps = {f"overlap@{n}": overlap(exact_rows, quant_rows, n) for n in TOP_N}
        same_members = set(exact_rows) == set(quant_rows)
        entry["profiles"][profile] = {
            "time": quant_time,
            "same_membership_at_page": same_members,
            **overlaps,
        }
        stats = " ".join(
            f"{key}={value:.3f}" for key, value in overlaps.items() if value is not None
        )
        print(
            f"  {name} [{profile}]: exact {exact_time * 1000:.0f}ms → "
            f"quant {quant_time * 1000:.0f}ms · {stats}"
            f"{'' if same_members else ' · MEMBERSHIP DIFFERS'}"
        )
    report.append(entry)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default="http://127.0.0.1:6342")
    parser.add_argument("--index-db", default=None)
    parser.add_argument("--user-data-db", default=None)
    parser.add_argument("--queries", help="file with one text query per line")
    parser.add_argument("--similar", type=int, default=3,
                        help="also compare similar_to for the top N exact hits (0 = off)")
    parser.add_argument("--json", dest="json_out", help="write the full report to this file")
    args = parser.parse_args()

    base = args.api_url.rstrip("/")
    dbs = {}
    if args.index_db:
        dbs["index_db"] = args.index_db
    if args.user_data_db:
        dbs["user_data_db"] = args.user_data_db

    queries = DEFAULT_QUERIES
    if args.queries:
        with open(args.queries, encoding="utf-8") as handle:
            queries = [line.strip() for line in handle if line.strip()]

    stats = api(base, "/api/search/stats", dbs)
    setters = stats.get("setters", [])
    clip_models = [name for kind, name in setters
                   if kind == "clip" and not name.startswith("tclip/")]
    text_models = [name for kind, name in setters
                   if kind == "text-embedding" and not name.startswith("tclip/")]

    quants = api(base, "/api/jobs/quants", dbs)
    ready_profiles = []
    for profile in quants.get("profiles", []):
        if profile["state"] == "active" and profile["setters"] and all(
            setter["state"] == "ready" for setter in profile["setters"]
        ):
            ready_profiles.append(profile["name"])
    if not ready_profiles:
        print("No fully-ready quant profiles; nothing to compare.", file=sys.stderr)
        print("Profiles:", json.dumps(quants, indent=2), file=sys.stderr)
        return 1
    print(f"Comparing exact vs profiles {ready_profiles} "
          f"(clip: {clip_models}, text: {text_models})")

    report = []
    for model in clip_models[:1]:
        print(f"\nSemantic image search — {model}")
        for query in queries:
            compare(
                f"image:{query!r}", base, dbs,
                lambda index, variant, query=query, model=model: semantic_filter(
                    "image_embeddings", query, model, index, variant),
                ready_profiles, report,
            )
    for model in text_models[:1]:
        print(f"\nSemantic text search — {model}")
        for query in queries:
            compare(
                f"text:{query!r}", base, dbs,
                lambda index, variant, query=query, model=model: semantic_filter(
                    "text_embeddings", query, model, index, variant),
                ready_profiles, report,
            )
    if args.similar > 0 and clip_models:
        model = clip_models[0]
        seed_rows, _ = search(
            base, dbs, semantic_filter("image_embeddings", queries[0], model, "exact"))
        print(f"\nItem similarity — {model}")
        for target in seed_rows[: args.similar]:
            compare(
                f"similar:{target[:12]}", base, dbs,
                lambda index, variant, target=target, model=model: similar_filter(
                    target, model, index, variant),
                ready_profiles, report,
            )

    # Aggregate: the headline number per profile.
    print("\nAggregates:")
    for profile in ready_profiles:
        for n in TOP_N:
            values = [
                entry["profiles"][profile][f"overlap@{n}"]
                for entry in report
                if entry["profiles"].get(profile, {}).get(f"overlap@{n}") is not None
            ]
            if values:
                print(
                    f"  [{profile}] mean overlap@{n} = {statistics.mean(values):.3f} "
                    f"(min {min(values):.3f}, {len(values)} runs)"
                )

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(f"\nFull report written to {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
