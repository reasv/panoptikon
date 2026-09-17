#!/usr/bin/env python3
"""Approximate the ring's per-bucket relative MAD from worker batch lines.

Each `Running inference on N images` line starts a batch; its duration is the
gap to the next worker line of any kind that follows the batch's completion.
We use the gap to the next `Running inference` line as the batch wall time,
which over-states long gaps at a window boundary, so windows' last batches are
dropped (the ledger's own series uses the worker's reported duration_ms).
"""
import re, sys
from datetime import datetime
from statistics import median

PAT = re.compile(r'^(\S+Z).*Running inference on (\d+) images')

def rel_mad(vals):
    c = median(vals)
    return median([abs(v - c) for v in vals]) / c if c > 0 else None

for path in sys.argv[1:]:
    rows = []
    for l in open(path, errors='replace'):
        l = re.sub(r'\x1b\[[0-9;]*m', '', l)
        m = PAT.match(l)
        if m:
            rows.append((datetime.fromisoformat(m.group(1)[:-1]), int(m.group(2))))
    buckets = {}
    for i in range(len(rows) - 1):
        t, n = rows[i]
        dt = (rows[i + 1][0] - t).total_seconds()
        if dt <= 0 or dt > 30 * n:      # drop window-boundary gaps
            continue
        if n != rows[i + 1][1]:          # drop the last batch of a rung
            continue
        buckets.setdefault(max(n, 1).bit_length() - 1, []).append(n / dt)
    print(f"\n# {path}")
    print("| log2 bucket | sizes | n | median items/s | relative MAD | > 0.20? |")
    print("|---|---|---|---|---|---|")
    for b in sorted(buckets):
        v = buckets[b]
        if len(v) < 2:
            continue
        rm = rel_mad(v)
        print(f"| {b} | {2**b}-{2**(b+1)-1} | {len(v)} | {median(v):.2f} | {rm:.3f} | "
              f"{'**yes**' if rm > 0.20 else 'no'} |")
