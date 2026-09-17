"""R1 table: per S2-clip run, the ramp's end state and the peak pool."""
import json, sys, glob, os

def rows(p):
    for line in open(p):
        try: yield json.loads(line)
        except Exception: pass

def one(d):
    hr = os.path.join(d, "healthrec.jsonl")
    out = {"run": os.path.basename(os.path.dirname(d))}
    peak_pool = 0; peak_budget = 0; knee = 0; knee_first = 0
    held = 0; held_units = 0; held_cert = 0; maxmeas = 0; last = None
    rungs = set()
    for r in rows(hr):
        for w in ((r.get("health") or {}).get("workers") or []):
            if not w.get("inference_id"): continue
            peak_pool = max(peak_pool, int(w.get("reserved_mb") or 0))
            ub = int(w.get("unit_budget") or 0)
            peak_budget = max(peak_budget, ub)
            if ub: rungs.add(ub)
            maxmeas = max(maxmeas, int(w.get("max_units_measured") or 0))
            k = int(w.get("knee_units") or 0)
            if k:
                knee = k
                if not knee_first: knee_first = k
            if w.get("ramp_held"):
                held += 1
                held_units = max(held_units, int(w.get("held_units") or 0))
                if w.get("held_certified"): held_cert += 1
            last = w
    out.update(knee_units=knee, knee_first=knee_first, peak_unit_budget=peak_budget,
               max_units_measured=maxmeas, rungs_visited=sorted(rungs),
               ramp_held_samples=held, held_units=held_units,
               held_certified_samples=held_cert, peak_pool_mb=peak_pool)
    if last is not None:
        out["final"] = {k: last.get(k) for k in
                        ("unit_budget", "knee_units", "ramp_held", "held_units",
                         "held_certified", "reserved_mb", "clean_windows",
                         "throughput_samples", "local_samples")}
    # vramrec peak free-side usage
    vr = os.path.join(d, "vramrec.jsonl")
    if os.path.exists(vr):
        lo = None
        for r in rows(vr):
            for g in (r.get("gpus") or []):
                f = g.get("free_mb")
                if f is not None: lo = f if lo is None else min(lo, f)
        out["vramrec_min_free_mb"] = lo
    # job throughput
    for name in ("extraction.json", "jobs.json"):
        p = os.path.join(d, name)
        if os.path.exists(p):
            try:
                j = json.load(open(p))
            except Exception:
                continue
            out[name] = _thr(j)
    return out

def _thr(j):
    def walk(o):
        if isinstance(o, dict):
            got = {k: o[k] for k in ("items_per_second", "items_per_s", "total_items",
                                     "completed", "duration_s", "elapsed_s", "outcome")
                   if k in o}
            if got: yield got
            for v in o.values(): yield from walk(v)
        elif isinstance(o, list):
            for v in o: yield from walk(v)
    return list(walk(j))[:3]

if __name__ == "__main__":
    res = []
    for pat in sys.argv[1:]:
        for d in sorted(glob.glob(pat)):
            if os.path.isdir(d): res.append(one(d))
    print(json.dumps(res, indent=1))
