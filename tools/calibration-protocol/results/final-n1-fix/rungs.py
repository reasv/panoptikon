#!/usr/bin/env python3
"""Per-rung table from a gateway log: grant->settle windows for one model.

rung = the window's `unit_budget`; items/s = unit_budget / (settle - grant).
Also prints the knee lines verbatim and the held rung.
"""
import re, sys, json
from datetime import datetime

ANSI = re.compile(r'\x1b\[[0-9;]*m')
TS = re.compile(r'^(\d{4}-\d\d-\d\dT[\d:.]+)Z')

def ts(line):
    m = TS.match(line)
    return datetime.fromisoformat(m.group(1)) if m else None

def field(line, key):
    m = re.search(rf'\b{key}=("?)([^ "]*)\1', line)
    return m.group(2) if m else None

def main(path, model, rss_path=None):
    lines = [ANSI.sub('', l.rstrip('\n')) for l in open(path, errors='replace')]
    windows, pending = [], None
    knee_lines, other = [], []
    for l in lines:
        if 'issued a memory grant' in l and field(l, 'model') == model:
            pending = (ts(l), int(field(l, 'unit_budget')), int(field(l, 'mb')))
        elif 'settled a granted window' in l and field(l, 'model') == model and pending:
            t0, units, mb = pending
            t1 = ts(l)
            dt = (t1 - t0).total_seconds()
            windows.append({'t0': t0.isoformat(), 't1': t1.isoformat(), 'units': units,
                            'mb': mb, 'secs': dt,
                            'rate': units / dt if dt > 0 else None,
                            'outcome': field(l, 'outcome')})
            pending = None
        if 'ledger' in l and ('knee' in l or 'throughput curve' in l or 'ramp is free' in l):
            knee_lines.append(l)
    # rung table
    from statistics import median
    rungs = {}
    for w in windows:
        rungs.setdefault(w['units'], []).append(w)
    print(f"# {path}\n# model={model}  windows={len(windows)}")
    print("| rung | n | items/s (median) | median grant MB | total s |")
    print("|---|---|---|---|---|")
    for u in sorted(rungs):
        ws = rungs[u]
        rates = [w['rate'] for w in ws if w['rate']]
        print(f"| {u} | {len(ws)} | {median(rates):.3f} | {median([w['mb'] for w in ws])} | "
              f"{sum(w['secs'] for w in ws):.1f} |")
    print(f"\n# total window wall time: {sum(w['secs'] for w in windows):.1f} s")
    print(f"# last rung: {windows[-1]['units'] if windows else None}")
    print("\n# knee / curve lines")
    for l in knee_lines:
        print(l)
    return windows

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else 'tags/wd-vit-tagger-v3')
