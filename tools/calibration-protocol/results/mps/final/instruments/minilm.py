"""MiniLM loadgen table: hold lines, whether each is answered within 10 samples,
throughput, and the window_requests histogram."""
import json, sys, glob, os, re

HOLD = "holding the throughput ramp at this rung"
LIFT = "the throughput ramp is free to grow again"


def rows(p):
    if not os.path.exists(p):
        return
    for line in open(p, errors="replace"):
        try:
            yield json.loads(line)
        except Exception:
            pass


def one(d):
    run = os.path.basename(os.path.dirname(d))
    out = {"run": run}
    log = os.path.join(d, "panoptikon.log")
    holds, lifts, hist = [], 0, {}
    if os.path.exists(log):
        for line in open(log, errors="replace"):
            if HOLD in line:
                m = re.search(r"units=(\d+)", line)
                c = re.search(r"certified=(\w+)", line)
                holds.append({"units": int(m.group(1)) if m else None,
                              "certified": (c.group(1) if c else None),
                              "reason": line.split(": ", 2)[-1][:90].strip()})
            if LIFT in line:
                lifts += 1
            if "issued a memory grant" in line:
                m = re.search(r"window_requests[=:]\s*(\d+)", line)
                if m:
                    k = int(m.group(1))
                    hist[k] = hist.get(k, 0) + 1
    out["hold_log_lines"] = len(holds)
    out["holds"] = holds[:4]
    out["lift_log_lines"] = lifts
    out["window_requests_hist"] = {str(k): hist[k] for k in sorted(hist)}
    out["grants"] = sum(hist.values())

    # health samples: when the hold engaged, how long it held, what answered it
    samples, first_hold, held, held_cert, held_units = 0, None, 0, 0, 0
    budgets, final_budget, answered_within = [], None, None
    for r in rows(os.path.join(d, "healthrec.jsonl")):
        for w in ((r.get("health") or {}).get("workers") or []):
            if not w.get("inference_id"):
                continue
            samples += 1
            ub = int(w.get("unit_budget") or 0)
            if ub:
                budgets.append(ub)
                final_budget = ub
            if w.get("ramp_held"):
                held += 1
                held_units = max(held_units, int(w.get("held_units") or 0))
                if first_hold is None:
                    first_hold = samples
                if w.get("held_certified"):
                    held_cert += 1
                    if answered_within is None:
                        answered_within = samples - first_hold
            elif first_hold is not None and answered_within is None:
                answered_within = samples - first_hold
    out.update(worker_samples=samples, hold_first_sample=first_hold,
               held_samples=held, held_certified_samples=held_cert,
               held_units=held_units, peak_unit_budget=max(budgets) if budgets else None,
               final_unit_budget=final_budget,
               samples_until_answered=answered_within)

    # throughput, from loadgen's own trailer
    for r in rows(os.path.join(d, "loadgen.jsonl")):
        if r.get("kind") == "summary":
            out["loadgen"] = {k: r.get(k) for k in
                              ("elapsed_s", "requests", "items", "ok", "errors",
                               "items_per_s", "units_per_s") if k in r}
            if "items_per_s" not in r and r.get("elapsed_s"):
                out["loadgen"]["items_per_s_calc"] = round(
                    (r.get("items") or 0) / r["elapsed_s"], 3)
    return out


if __name__ == "__main__":
    res = []
    for pat in sys.argv[1:]:
        for d in sorted(glob.glob(pat)):
            if os.path.isdir(d):
                res.append(one(d))
    print(json.dumps(res, indent=1))
