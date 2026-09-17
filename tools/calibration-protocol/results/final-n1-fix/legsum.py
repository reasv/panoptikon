#!/usr/bin/env python3
"""Per-leg summary for the N1-fix verification: knee narrative, store, RSS."""
import json, re, sys, os
from pathlib import Path
ANSI = re.compile(r'\x1b\[[0-9;]*m')
def field(l, k):
    m = re.search(rf'\b{k}=("?)([^ "]*)\1', l)
    return m.group(2) if m else None
leg = Path(sys.argv[1])
log = leg / 'root/data/panoptikon.log'
lines = [ANSI.sub('', l.rstrip()) for l in open(log, errors='replace')]
ev = []
for l in lines:
    if 'declining to read this model' in l:
        ev.append(('DECLINE', l[:19], f"bucket={field(l,'bucket')} obs={field(l,'observations')} disp={field(l,'dispersion')} threshold={field(l,'threshold')}"))
    elif 'fitted a throughput knee' in l:
        ev.append(('FIT', l[:19], f"knee_units={field(l,'knee_units')} previous={field(l,'previous')} obs={field(l,'observations')}"))
    elif 'the throughput ramp is free to grow again' in l:
        ev.append(('GROW', l[:19], ''))
    elif 'widening the cap' in l or 'worth re-testing' in l:
        ev.append(('WIDEN', l[:19], f"from={field(l,'knee_units')} to={field(l,'widened_to')}"))
    elif 'withdraw' in l and 'ledger' in l:
        ev.append(('WITHDRAW', l[:19], l.split('ledger: ')[-1][:90]))
windows = sum('settled a granted window' in l for l in lines)
# store
cal = (leg / 'calibration.after.toml').read_text() if (leg/'calibration.after.toml').is_file() else ''
def tval(k):
    m = re.search(rf'^{k} = (.*)$', cal, re.M)
    return m.group(1) if m else None
mx = 0; avail = None; foreign = 0
for l in open(leg/'vramrec.jsonl'):
    r = json.loads(l)
    if r.get('kind') != 'sample': continue
    w = [p for p in r.get('procs', []) if 'inferio_worker' in (p.get('cmdline') or '')]
    foreign = max(foreign, len(w))
    mx = max(mx, sum(p.get('rss_mb') or 0 for p in w))
    a = r.get('mem', {}).get('mem_available_mb')
    if a is not None: avail = a if avail is None else min(avail, a)
job = json.load(open(leg/'jobs.json'))[0]
print(f"== {leg}")
print(f"   inference_time={job['inference_time']:.1f}s errors={job['errors']} windows={windows}")
print(f"   store: knee_units={tval('knee_units')} max_units_measured={tval('max_units_measured')} slope={tval('slope_mb_per_unit')} base_mb={tval('base_mb')}")
print(f"   peak own-worker RSS={mx} MB  max concurrent workers={foreign}  mem_available floor={avail} MB")
counts = {}
for k, _, _ in ev: counts[k] = counts.get(k, 0) + 1
print(f"   counts: {counts}")
# a GROW that follows a DECLINE is the defect
bad = [ev[i] for i in range(1, len(ev)) if ev[i][0] == 'GROW' and any(e[0] == 'DECLINE' for e in ev[:i])]
print(f"   GROW lines after any DECLINE: {len(bad)}")
for k, t, d in ev: print(f"     {t}  {k:9} {d}")
