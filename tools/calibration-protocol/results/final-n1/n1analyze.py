#!/usr/bin/env python3
"""N1: per-rung table, knee narrative and RSS for one S2 wd-vit ramp leg."""
import json, re, sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

ANSI = re.compile(r'\x1b\[[0-9;]*m')
TS = re.compile(r'^(\d{4}-\d\d-\d\dT[\d:.]+)Z')
MODEL = 'tags/wd-vit-tagger-v3'

def ts(line):
    m = TS.match(line)
    return datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc) if m else None

def field(line, key):
    m = re.search(rf'\b{key}=("?)([^ "]*)\1', line)
    return m.group(2) if m else None

def load_rss(path):
    out = []
    for l in open(path):
        r = json.loads(l)
        if r.get('kind') != 'sample':
            continue
        t = datetime.fromisoformat(r['iso'])
        workers = [p for p in r.get('procs', [])
                   if 'inferio_worker' in (p.get('cmdline') or '')]
        out.append((t, sum(p.get('rss_mb') or 0 for p in workers),
                    len(workers), r.get('mem', {}).get('mem_available_mb')))
    return out

def main(leg):
    leg = Path(leg)
    log = leg / 'panoptikon.log'
    if not log.is_file():
        log = leg / 'gateway.out'
    lines = [ANSI.sub('', l.rstrip('\n')) for l in open(log, errors='replace')]
    windows, pending, narrative = [], None, []
    for l in lines:
        if 'issued a memory grant' in l and field(l, 'model') == MODEL:
            pending = (ts(l), int(field(l, 'unit_budget')), int(field(l, 'mb')),
                       field(l, 'external_mb'))
        elif 'settled a granted window' in l and field(l, 'model') == MODEL and pending:
            t0, units, mb, ext = pending
            t1 = ts(l); dt = (t1 - t0).total_seconds()
            windows.append({'t0': t0, 't1': t1, 'units': units, 'mb': mb,
                            'secs': dt, 'rate': units / dt if dt > 0 else 0.0,
                            'external_mb': ext, 'outcome': field(l, 'outcome')})
            pending = None
        if '::ledger' in l and any(k in l for k in (
                'knee', 'throughput curve', 'throughput ramp', 'holding the throughput')):
            narrative.append(l)
    rss = load_rss(leg / 'vramrec.jsonl') if (leg / 'vramrec.jsonl').is_file() else []

    def peak_rss(t0, t1):
        vals = [v for (t, v, n, _) in rss if t0 <= t <= t1 and v]
        return max(vals) if vals else None

    rungs = {}
    for w in windows:
        rungs.setdefault(w['units'], []).append(w)
    print(f"## {leg}")
    print(f"windows={len(windows)}  total window wall {sum(w['secs'] for w in windows):.1f} s")
    print("\n| rung | n | items/s (median) | median grant MB | peak worker RSS MB | s at rung |")
    print("|---|---|---|---|---|---|")
    for u in sorted(rungs):
        ws = rungs[u]
        pk = [peak_rss(w['t0'], w['t1']) for w in ws]
        pk = [p for p in pk if p]
        print(f"| {u} | {len(ws)} | {median([w['rate'] for w in ws]):.3f} | "
              f"{median([w['mb'] for w in ws]):.0f} | {max(pk) if pk else '-'} | "
              f"{sum(w['secs'] for w in ws):.1f} |")
    print(f"\nlast rung: {windows[-1]['units'] if windows else None}")
    # peak foreign-worker count sanity
    maxw = max((n for (_, _, n, _) in rss), default=0)
    avail = [a for (_, _, _, a) in rss if a]
    print(f"vramrec: max concurrent inferio_worker procs = {maxw}; "
          f"mem_available_mb min/max = {min(avail) if avail else '-'}/{max(avail) if avail else '-'}")
    # store row
    after = leg / 'calibration.after.toml'
    if after.is_file():
        txt = after.read_text()
        print("\n### calibration.after.toml (wd-vit row)")
        keep = False
        for l in txt.splitlines():
            if l.startswith('[['):
                keep = False
            if 'wd-vit-tagger-v3' in l:
                keep = True
            if keep:
                print(l)
    print("\n### knee / ramp narrative (verbatim)")
    for l in narrative:
        print(l)
    jobs = leg / 'jobs.json'
    if jobs.is_file():
        d = json.loads(jobs.read_text())
        print("\n### jobs")
        print(json.dumps(d)[:1500])

if __name__ == '__main__':
    main(sys.argv[1])
