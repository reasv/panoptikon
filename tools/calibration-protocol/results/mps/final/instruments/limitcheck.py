"""limit = min(recommended_max, memsize - external - reserve) on every health sample."""
import json, sys, glob, os
MEMSIZE = 131072

def run(d):
    p = os.path.join(d, "healthrec.jsonl")
    n = ok = 0; bad = []
    tot = set(); lim = []; ext = []
    for line in open(p):
        try: o = json.loads(line)
        except Exception: continue
        for v in ((o.get("health") or {}).get("vram") or []):
            t = v.get("total_mb"); l = v.get("limit_mb")
            e = v.get("external_mb"); r = v.get("reserve_mb") or 0
            if t is None or l is None: continue
            tot.add(t); lim.append(l)
            if e is not None: ext.append(e)
            if e is None or not v.get("external_known"):
                continue
            n += 1
            pred = min(t, MEMSIZE - e - r)
            if abs(pred - l) <= 1: ok += 1
            elif len(bad) < 5: bad.append({"total": t, "limit": l, "ext": e,
                                           "reserve": r, "pred": pred})
    return {"leg": os.path.basename(os.path.dirname(d)),
            "formula_ok": ok, "formula_n": n,
            "pct": round(100.0 * ok / n, 1) if n else None,
            "totals": sorted(x for x in tot if x),
            "limit_min": min(lim) if lim else None,
            "limit_max": max(lim) if lim else None,
            "ext_max": max(ext) if ext else None,
            "examples": bad}

if __name__ == "__main__":
    out = []
    for pat in sys.argv[1:]:
        for d in sorted(glob.glob(pat)):
            if os.path.isdir(d) and os.path.exists(os.path.join(d, "healthrec.jsonl")):
                out.append(run(d))
    print(json.dumps(out, indent=1))
