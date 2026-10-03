#!/usr/bin/env python3
"""Acceptance table for the batch-size rule: runs the ledger's closed-loop simulator (`sizing_sim`,
an ignored test in the panoptikon crate) over every scenario and device class, one markdown table
per scenario. `--set verify4` prints the trace-replay cells of the 2026-10-02 study instead; the
gpu-ram flat, soft-curve and late-rise rows use that study's seeds and compare with it directly.
"""
import argparse, collections, math, os, shutil, statistics as st, subprocess, tempfile

# A model: (trace or curve, MiB of pool per unit, base MiB, RSS MiB per unit, seed batch).
CUDA = {"wdvit": ("trace=cuda-wdvit.txt", 46, 698, 5, 64), "vith": ("trace=cuda-vith.txt", 45, 2730, 5, 64),
        "mclip": ("trace=cuda-mclip.txt", 17, 700, 5, 64), "doctr": ("trace=cuda-doctr.txt", 9, 700, 60, 64),
        "flor": ("trace=cuda-flor.txt", 480, 700, 5, 64), "minilm": ("trace=cuda-minilm.txt", 4, 700, 1, 64)}
MAC = {"vith": ("trace=mac2-vith.txt tmin=3", 250, 4144, 0, 8), "mclip": ("trace=mac2-mclip.txt tmin=3", 32, 1000, 0, 64)}
# No archived run measured a rate curve on the CPU device: synthetic ones with CPU-like scatter.
CPU = {"knee8": ("curve=knee:1.25:8:20 noise=0.2 ndist=g ovh=50", 60, 700, 0, 8),
       "rise": ("curve=geo:1.1:20 noise=0.2 ndist=g ovh=50", 60, 700, 0, 8)}
DAILY = ["wdvit", "vith", "mclip", "doctr", "flor"]
GPU_FLAT = [("wd", 64), ("vh", 128), ("fl", 64), ("w8", 8)]
# Per device class: device and memory, models, the models of a daily job, flat series (name, size).
CLASSES = {"gpu-ram": ("dev=gpu-ram room=95000 total=97000", CUDA, DAILY, GPU_FLAT),
           "gpu": ("dev=gpu room=95000 total=97000", CUDA, DAILY, GPU_FLAT),
           "mac": ("dev=mac room=105956 total=110100 queue=first1", MAC, list(MAC), [("mac", 8)]),
           "cpu": ("dev=cpu room=40000 total=48000", CPU, list(CPU), [("cpu", 8)])}
CAPS = [1 << k for k in range(10)]
SYNTH = "pu=46 base=698 rsspu=5 seed=64"  # synthetic curves: 46 MiB a unit
# Curve, the window it changes in, windows.
SHAPES = {"late at 20": ("sw:20:flat:22;knee:1.3:256:8", 20, 1520), "late at 300": ("sw:300:flat:22;knee:1.3:256:8", 300, 1520),
          "plateau, rise at 600": ("sw:600:knee:1.3:64:8;knee:1.3:1024:8", 600, 1600),
          "dip at 128": ("lad:1/10,32/30,64/34,128/31,256/38,512/40", 0, 1000)}
# Soft curves over 1 500 windows; scatter and two host levels over 10 starts of 300 windows.
SYNTHETIC = {**{f"{g}x a doubling, σ {sd:.0%}": f"geo:{g}:22 noise={sd} ndist=g win=1500"
                for g in (1.03, 1.08, 1.2) for sd in (0.03, 0.1)},
             "scatter 35 %, rising": "knee:1.3:256:8 noise=0.35 ndist=g win=300 starts=10",
             "scatter 35 %, flat": "flat:22 noise=0.35 ndist=g win=300 starts=10",
             "two levels ±10 %, rising": "knee:1.3:256:8 levels=0.1:60 win=300 starts=10",
             "two levels ±10 %, flat": "flat:22 levels=0.1:60 win=300 starts=10"}
FLAT_JOBS = [("s12", "win=12 starts=40"), ("s60", "win=60 starts=40"), ("six", "win=20000 items=12000 starts=6")]


def curve_rate(spec, w, units):
    """The simulator's synthetic curves: units/s at a batch of `units` in window `w`."""
    if spec.startswith("sw:"):
        at, rest = spec[3:].split(":", 1)
        return curve_rate(rest.split(";")[int(w >= int(at))], w, units)
    p, lg = spec.split(":"), math.log2(max(units, 1))
    if p[0] == "flat":
        return float(p[1])
    if p[0] in ("geo", "knee"):
        top = lg if p[0] == "geo" else min(lg, math.log2(float(p[2])))
        return float(p[-1]) * float(p[1]) ** top
    pts = [tuple(map(float, x.split("/"))) for x in p[1].split(",")]
    for (u0, r0), (u1, r1) in zip(pts, pts[1:]):
        if units <= u1:
            return r0 + (r1 - r0) * max(0, lg - math.log2(u0)) / math.log2(u1 / u0)
    return pts[-1][1]


# The batch sizes up to `top`, and a synthetic curve's best rate among them.
sizes = lambda top: [1 << k for k in range(top.bit_length())] + [top]
best_rate = lambda top, spec, w=0: max(curve_rate(spec, w, u) for u in sizes(top))


def implied(rates, gain):
    """The size the rule implies: a larger size only where it clears `gain` per doubling."""
    best = min(rates)
    for s in sorted(rates):
        if rates[s] >= rates[best] * (1 + gain) ** math.log2(s / best):
            best = s
    return best


def model_tokens(cls, model):
    src, pu, base, rss, seed = CLASSES[cls][1][model]
    return f"{CLASSES[cls][0]} {src} pu={pu} base={base} rsspu={rss} seed={seed}", pu


def acceptance_lines(a):
    lines, seeds = [], range(1, a.seeds + 1)
    for cls in a.classes:
        dev, models, daily, flats = CLASSES[cls]
        ship = " ship=192" if cls.startswith("gpu") else ""
        for model, s in ((m, s) for m in models for s in seeds):
            job = f"{model_tokens(cls, model)[0]} win=400000 v=1 compact=1 lag=1 tseed={200 + s} nseed={s}"
            name = lambda sc, mode, var: f"name={sc}|{cls}|{model}|{mode}|{var}|{s} {job} mode={mode}"
            for c in CAPS:
                lines.append(name("fixed", "-", c) + f" secs=3600 qfloor={3 * c} cap={c}{ship}")
            for mode in a.modes:
                lines.append(name("long", mode, "-") + " secs=3600 qfloor=192")
                if cls.startswith("gpu") and s <= max(2, a.seeds // 4):
                    lines.append(name("server", mode, "-") + " secs=18000 qfloor=192")
                if model in daily:
                    lines.append(name("daily", mode, "warm") + " items=30 starts=7 restart=0")
                    lines.append(name("daily", mode, "restart") + " items=30 starts=7")
        for mode, s in ((m, s) for m in a.modes for s in seeds):
            syn = f"{dev} {SYNTH} mode={mode} nseed={600 + s}"
            for (flat, ref), prof, (kind, job) in (
                    (f, p, k) for f in flats for p in ([0, 1] if ship else [0]) for k in FLAT_JOBS):
                lines.append(f"name=flat|{cls}|{flat}{prof}|{mode}|{kind}|{s} {syn} {job} "
                             f"trace=flat-{flat}.txt lag=1 v=1 compact=1 qfloor=192 tshared=1 "
                             f"tref={ref} tseed={300 + s}{ship if prof else ''}")
            lines += [f"name=syn|{cls}|{k.replace(' ', '_')}|{mode}|-|{s} {syn} curve={c} v=1 compact=1" for k, c in SYNTHETIC.items()]
            lines += [f"name=shape|{cls}|{k.replace(' ', '_')}|{mode}|-|{s} {syn} curve={c} noise=0.03 ndist=g win={win} v=1 compact=1"
                      for k, (c, _, win) in SHAPES.items()]
    return lines


def run(lines, a):
    """Every line on the simulator, in `a.jobs` shards; one dict per process start."""
    work, procs, rows = tempfile.mkdtemp(prefix="sizing-"), [], []
    test = "inferio::ledger::tests::sizing_sim::sizing_sim"
    for i in range(a.jobs):
        spec, out = f"{work}/spec{i}", f"{work}/out{i}"
        open(spec, "w").write("\n".join(lines[i::a.jobs]) + "\n")
        env = dict(os.environ, SIZING_SPEC=spec, SIZING_OUT=out, SIZING_TRACES=a.traces)
        cmd = ["nice", "-n", "8", a.bin, "--ignored", "--exact", "-q", test]
        procs.append((subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL), out))
    for proc, out in procs:
        proc.wait()
        for line in open(out):
            if line.startswith("panic"):
                print("PANIC", line[:300])
                continue
            r = dict(t.split("=", 1) for t in line.split())
            rows.append(dict(r, key=tuple(r["name"].replace("_", " ").split("|"))))
    shutil.rmtree(work)
    return rows


def arr(r, k):
    """A per-window column, run-length decoded."""
    out = []
    for x in filter(None, r.get(k, "").split(",")):
        v, _, n = x.partition("x")
        out += [int(v)] * int(n or 1)
    return out


ips = lambda r: int(r["items"]) / max(float(r["ms"]), 1) * 1000  # as the job saw it
true_ips = lambda r: int(r["items"]) / float(r["true_s"])  # without the measurement noise
above = lambda r: 0 < int(r["open"]) < int(r["W"])
num = lambda k: lambda r: float(r[k])
gb = lambda mb: f"{mb / 1024:.1f}"
mean = lambda rs, f: st.mean(map(f, rs))
dist = lambda vs: " ".join(f"{k}:{n}" for k, n in sorted(collections.Counter(vs).items()))
ws = lambda rs: dist(int(r["W"]) for r in rs)


table = lambda title, cols: print(f"\n### {title}\n\n| " + " | ".join(cols) + " |\n" + "|---" * len(cols) + "|")
row = lambda *cells: print("| " + " | ".join(map(str, cells)) + " |")


def tables(rows, a):
    g, scen = collections.defaultdict(list), collections.defaultdict(list)
    top = collections.defaultdict(int)  # the largest batch a synthetic curve ran, per class
    for r in rows:
        g[r["key"][:5]].append(r)
        scen[r["key"]].append(r)
        if r["key"][0] in ("syn", "shape"):
            top[r["key"][1]] = max(top[r["key"][1]], max(arr(r, "budgets")))
    gain = lambda cls, mode: a.throughput if mode == "throughput" else a.gpu if cls.startswith("gpu") else a.strict
    fixed = collections.defaultdict(dict)
    for (sc, cls, model, _, var), rs in g.items():
        if sc == "fixed":
            fixed[(cls, model)][int(var)] = mean(rs, ips)
    for sc, title in (("long", "Long desktop job (1 h)"),
                      ("server", "Dedicated server job (5 h; best fixed size of the 1 h job)")):
        table(title, ["class", "model", "mode", "items/s", "best fixed size", "vs best", "W at end",
                      "implied W", "pool GB: time-weighted", "at the end", "peak", "trials' extra"])
        for (s, cls, model, mode, _), rs in sorted(g.items()):
            if s != sc:
                continue
            fx, pu = fixed[(cls, model)], model_tokens(cls, model)[1]
            best = max(fx, key=fx.get)
            extra = mean(rs, lambda r: float(r["poolmean"]) - pu * max(int(r["W"]), 0))
            row(cls, model, mode, f"{mean(rs, ips):.2f}", f"{best}: {fx[best]:.2f}",
                f"{100 * (mean(rs, ips) / fx[best] - 1):+.1f} %", ws(rs), implied(fx, gain(cls, mode)),
                gb(mean(rs, num("poolmean"))), gb(mean(rs, num("poollast"))),
                gb(max(map(num("poolpeak"), rs))), gb(extra))
    table("Daily job: 30 items per model, 7 days, wall time summed over the models",
          ["class", "mode", "first day s", "warm, days 2-7 s", "after restart, days 2-7 s",
           "ideal s", "warm / ideal", "restart / ideal"])
    pick = lambda rs, first: [r for r in rs if (r["start"] == "0") == first]
    for cls in a.classes:
        daily = CLASSES[cls][2]
        # 30 items per model at the best sustained rate of a batch of at most 32.
        ideal = sum(30 / max(v for c, v in fixed[(cls, m)].items() if c <= 32) for m in daily)
        for mode in a.modes:
            day = lambda var, first: sum(mean(pick(g[("daily", cls, m, mode, var)], first), num("ms"))
                                         for m in daily) / 1000
            w, rs = day("warm", False), day("restart", False)
            row(cls, mode, f"{day('restart', True):.1f}", f"{w:.1f}", f"{rs:.1f}", f"{ideal:.1f}",
                f"{w / ideal:.2f}", f"{rs / ideal:.2f}")
    table("Flat model, one host level for every size: starts ending above their opening size",
          ["class", "mode", "12-window starts", "60-window starts", "12 000-item starts",
           "stored above the first opening", "largest budget"])
    first = lambda rs: next((int(r["open"]) for r in rs if int(r["open"]) > 0), 0)
    for cls, mode in ((c, m) for c in a.classes for m in a.modes):
        mine = [rs for k, rs in scen.items() if k[0] == "flat" and k[1] == cls and k[3] == mode]
        cells = []
        for kind, _ in FLAT_JOBS:
            rs = [r for v in mine if v[0]["key"][4] == kind for r in v]
            cells.append(f"{100 * sum(map(above, rs)) / max(len(rs), 1):.1f} % of {len(rs)}")
        stored = sum(int(r["stored"]) > first(v) > 0 for v in mine for r in v)
        row(cls, mode, *cells, f"{stored} of {sum(map(len, mine))}",
            max(max(arr(r, "budgets")) for v in mine for r in v))
    table("Synthetic curves: share of the best rate at any batch size the device granted",
          ["class", "case", "mode", "% of best", "starts ending above their opening", "W at end", "implied W"])
    for (s, cls, label, mode, _), rs in sorted(g.items()):
        if s == "syn":
            c = SYNTHETIC[label].split()[0]
            rates = {u: curve_rate(c, 0, u) for u in sizes(top[cls])}
            row(cls, label, mode, f"{100 * mean(rs, true_ips) / best_rate(top[cls], c):.1f}",
                f"{100 * sum(map(above, rs)) / len(rs):.1f} %", ws(rs), implied(rates, gain(cls, mode)))
    table("Rate shapes: minutes until the batch size runs within 10 % of the best rate",
          ["class", "shape", "mode", "minutes, median (max)", "% of best over the job", "W at end"])
    for (s, cls, label, mode, _), rs in sorted(g.items()):
        if s != "shape":
            continue
        c, at, _ = SHAPES[label]
        mins = []
        for b, ms in ((arr(r, "budgets"), arr(r, "wms")) for r in rs):
            good = (i for i in range(at, len(b)) if curve_rate(c, i, b[i]) >= 0.9 * best_rate(top[cls], c, at))
            hit = next(good, None)
            mins.append(math.inf if hit is None else sum(ms[at:hit]) / 60000)
        row(cls, label, mode, f"{st.median(mins):.1f} ({max(mins):.1f})",
            f"{100 * mean(rs, true_ips) / best_rate(top[cls], c, at):.1f}", ws(rs))

# The 2026-10-02 study: model: (pu, base, rsspu, items of one run, trace).
V4 = {"wdvit": (46, 698, 5, 12000, "cuda-wdvit"), "wdvitF": (46, 698, 5, 12000, "cuda-wdvit-fixed"),
      "vith": (45, 2730, 5, 12000, "cuda-vith"), "vithF": (45, 2730, 5, 12000, "cuda-vith-fixed"),
      "mclip": (17, 700, 5, 12000, "cuda-mclip"), "doctr": (9, 700, 60, 5000, "cuda-doctr"),
      "flor": (480, 700, 5, 6000, "cuda-flor"), "minilm": (4, 700, 1, 12000, "cuda-minilm")}


def verify4_lines(a):
    lines, gpu = [], "dev=gpu-ram room=95000 total=97000 seed=64"
    for (m, (pu, base, rss, items, tr)), s in ((m, s) for m in V4.items() for s in range(201, 201 + a.seeds)):
        job = f"{gpu} pu={pu} base={base} rsspu={rss} trace={tr}.txt lag=1 win=20000 v=1 qfloor=192 tseed={s}"
        lines += [f"name=v4|{m}|run|{s} {job} items={items} starts=2",
                  f"name=v4|{m}|hour|{s} {job} items={10 * items}"]
        if m.startswith(("wdvit", "vith")):
            lines.append(f"name=v4|{m}|profile|{s} {job} items=6000 starts=2 ship=192")
    return lines


def verify4_report(rows):
    g = collections.defaultdict(list)
    for r in rows:
        g[r["key"][1:3] + (r["start"],)].append(r)
    for key, rs in sorted(g.items()):
        print(*key, f"items/s {mean(rs, ips):.2f} W ({ws(rs)}) pool time-weighted "
              f"{gb(mean(rs, num('poolmean')))} trial windows {st.median(int(r['trialw']) for r in rs)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bin", required=True, help="the panoptikon test binary (see the README)")
    ap.add_argument("--traces", default=os.path.expanduser("~/panoptikon-archive/sizing-traces"))
    ap.add_argument("--set", choices=("acceptance", "verify4"), default="acceptance")
    ap.add_argument("--classes", default=",".join(CLASSES), type=lambda v: v.split(","))
    ap.add_argument("--modes", default="balanced,throughput", type=lambda v: v.split(","))
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--gpu", type=float, default=0.05, help="balanced gain per doubling, GPU")
    ap.add_argument("--strict", type=float, default=0.15, help="balanced gain, CPU and unified")
    ap.add_argument("--throughput", type=float, default=0.02, help="throughput gain, every device")
    a = ap.parse_args()
    if a.set == "verify4":
        verify4_report(run(verify4_lines(a), a))
    else:
        tables(run(acceptance_lines(a), a), a)


if __name__ == "__main__":
    main()
