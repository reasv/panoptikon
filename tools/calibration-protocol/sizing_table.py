#!/usr/bin/env python3
"""Acceptance tables for the batch-size rule: runs the ledger's closed-loop simulator (`sizing_sim`,
an ignored test in the panoptikon crate) over every scenario and device class, one markdown table
per scenario. `--set fidelity` replays real jobs of the sizing code under test beside what they
measured, `--set traces` lists the batches behind each traced size, and `--set reference` prints
the trace-replay cells the earlier study measured.
"""
import argparse, collections, math, os, shutil, statistics as st, subprocess, sys, tempfile

# A model: the simulator keys of its trace or curve, memory and seed. `pu`: MiB of pool per unit.
CUDA = {"wdvit": "trace=cuda-wdvit.txt pu=52 base=964 rsspu=8.7 seed=64",
        "vith": "trace=cuda-vith.txt pu=61 base=2588 rsspu=9.4 seed=64",
        "mclip": "trace=cuda-mclip.txt pu=17 base=732 rsspu=6 seed=192",
        "doctr": "trace=cuda-doctr.txt pu=10 base=798 rsspu=70 seed=128",
        "flor": "trace=cuda-flor.txt pu=480 base=2032 rsspu=21 seed=4",
        "minilm": "trace=cuda-minilm.txt cost=token upi=192:200 pu=0.0204 base=700 rsspu=0.005 "
                  "seed=120000"}
# Seeds as the registry derives them (2 GiB over the MiB a unit costs), at a Mac's cost.
MAC = {"vith": "trace=mac2-vith.txt tmin=9 pu=250 base=4144 rsspu=0 seed=8",
       "mclip": "trace=mac2-mclip.txt tmin=9 pu=32 base=1000 rsspu=0 seed=64"}
# No archived run measured a rate curve on the CPU device: synthetic curves carrying the real
# noise of the CPU device's windows.
CPU_NOISE = "trace=flat-cpu.txt tnoise=1 tshared=1 tref=8 ovh=50 pu=60 base=700 rsspu=0 seed=8"
CPU = {"knee8": f"curve=knee:1.25:8:20 {CPU_NOISE}", "rise": f"curve=geo:1.1:20 {CPU_NOISE}"}
DAILY = ["wdvit", "vith", "mclip", "doctr", "flor"]
GPU_FLAT = [("wd", 64), ("vh", 128), ("fl", 64), ("w8", 8)]
MAC_FLAT = [("mac", 8)]
# Per device class: device and memory, models, the models of a daily job, flat series (name,
# size), and which gain a size must show in balanced mode.
CLASSES = {
    "gpu-ram": ("dev=gpu-ram room=95000 total=97000", CUDA, DAILY, GPU_FLAT, "gpu"),
    "gpu": ("dev=gpu room=95000 total=97000", CUDA, DAILY, GPU_FLAT, "gpu"),
    "apu": ("dev=apu room=20000 total=24000", MAC, list(MAC), MAC_FLAT, "unified"),
    "mac": ("dev=mac room=105956 total=110100 queue=first1", MAC, list(MAC), MAC_FLAT, "unified"),
    "cpu": ("dev=cpu room=40000 total=48000", CPU, list(CPU), [("cpu", 8)], "unified")}
SIZES = [1 << k for k in range(11)]
SYNTH = "pu=46 base=698 rsspu=5 seed=64"  # synthetic curves: 46 MiB a unit
JOB = "lag=2 qfloor=192 v=1 compact=1"
GPU_BIG = "dev=gpu-ram room=95000 total=97000"
# Curve, the window it changes in, windows.
SHAPES = {"late at 20": ("sw:20:flat:22;knee:1.3:256:8", 20, 1520),
          "late at 300": ("sw:300:flat:22;knee:1.3:256:8", 300, 1520),
          "plateau, rise at 600": ("sw:600:knee:1.3:64:8;knee:1.3:1024:8", 600, 1600),
          "dip at 128": ("lad:1/10,32/30,64/34,128/31,256/38,512/40", 0, 1000),
          "flat to 16, rising above": ("lad:1/22,16/22,64/30,256/40,1024/48", 0, 1000)}
# After a restart the rate rises with size; the stored row still waits 384 windows to trial.
RESTART_CHANGE = ("curve=flat:22 curve2=knee:1.3:256:8 swstart=1 starts=2 noise=0.03 ndist=g "
                  "win=1000 prof=a:256:k:64:f:3:w:384")
GAUSS = {f"{g}x a doubling, σ {sd:.0%}": f"curve=geo:{g}:22 noise={sd} ndist=g win=1500"
         for g in (1.03, 1.05, 1.08) for sd in (0, 0.03, 0.1)}
# Real host noise around a curve, one host level for every size: the series, its size.
REAL_NOISE = {"gpu-ram": ("wd", 64), "gpu": ("wd", 64), "apu": ("mac", 8), "mac": ("mac", 8),
              "cpu": ("cpu", 8)}
REAL = {f"{g}x a doubling, real noise": g for g in (1.03, 1.05, 1.06, 1.15, 1.16)}
NOISE = {"scatter 35 %, rising": "curve=knee:1.3:256:8 noise=0.35 ndist=g win=300 starts=10",
         "scatter 35 %, flat": "curve=flat:22 noise=0.35 ndist=g win=300 starts=10",
         "two levels ±10 %, rising": "curve=knee:1.3:256:8 levels=0.1:60 win=300 starts=10",
         "two levels ±10 %, flat": "curve=flat:22 levels=0.1:60 win=300 starts=10"}
FLAT_JOBS = [("s12", "win=12 starts=40"), ("s60", "win=60 starts=40"),
             ("six", "win=20000 items=12000 starts=6")]
DAILY_ITEMS = [1, 10, 30, 50]
IDEAL_SIZES = [8, 16, 32, 64]  # a daily job's ideal: the best of these fixed sizes, from a load
CARD16 = "dev=gpu-ram room=14500 total=15500"
MAC8 = "dev=mac room=4000 total=6144 queue=first1"
# Desktop memory cases: device, model and what happens to memory.
DESKTOP = {
    "16 GB card, another app takes 12 GB at window 60 for 140 windows":
        (CARD16, "wdvit", "roomsched=60:2500,200:14500"),
    "the same, the free reading stale when it happens":
        (CARD16, "wdvit", "roomsched=60:2500,200:14500 roomlate=1"),
    "16 GB card, Florence, room halves at window 40": (CARD16, "flor", "roomsched=40:7000"),
    "host RAM free falls from 51 to 12 GB at window 30 (docTR, 70 MiB a unit)":
        (GPU_BIG, "doctr", "hostsched=30:12000"),
    "worker dies at window 30, host RAM booked": (GPU_BIG, "flor", "die=30"),
    "8 GB Mac, pressure warning at 40, paging at 80, normal at 160":
        (MAC8, "mmclip", "pressure=40:warning,80:paging,160:normal"),
    "8 GB Mac, room falls to 2 GB at window 60": (MAC8, "mmclip", "roomsched=60:2000"),
    "APU, 24 GB, room falls to 6 GB at window 60, stale":
        ("dev=apu room=20000 total=24000", "mvith", "roomsched=60:6000 roomlate=1"),
    "CPU, 16 GB, room falls to 4 GB at window 60":
        ("dev=cpu room=11000 total=16000", "rise", "roomsched=60:4000")}
DESKTOP_MODELS = {"wdvit": CUDA["wdvit"], "flor": CUDA["flor"], "doctr": CUDA["doctr"],
                  "mvith": MAC["vith"], "mmclip": MAC["mclip"], "rise": CPU["rise"]}
# Items of varying cost: a token-priced trace, and pixel- and token-priced curves.
UNITS = {"MiniLM, tokens (trace)": CUDA["minilm"],
         "pixels, 0.2-2 MP items": "cost=pixel upi=200000:2000000 noise=0.03 ndist=g "
                                   "curve=lad:1000000/4000000,16000000/9000000,64000000/10000000 "
                                   "pu=0.00002 base=800 rsspu=0.00006 seed=8000000",
         "tokens, 16-512 a text": "cost=token upi=16:512 noise=0.03 ndist=g "
                                  "curve=lad:512/20000,32768/120000,131072/150000 "
                                  "pu=0.004 base=700 rsspu=0.001 seed=16384"}
# The caller and the queue: how late the caller learns a new size, how much it keeps queued.
QUEUES = {"caller two windows late (as the dispatcher)": "lag=2",
          "caller one window late": "lag=1",
          "queue of 1.5 times the size run": "lag=2 qmul=1.5"}


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
    if units <= pts[0][0]:
        return pts[0][1]
    for (u0, r0), (u1, r1) in zip(pts, pts[1:]):
        if units <= u1:
            return r0 + (r1 - r0) * (lg - math.log2(u0)) / math.log2(u1 / u0)
    return pts[-1][1]


# The batch sizes up to `top`, and a synthetic curve's best rate among them.
sizes = lambda top: [1 << k for k in range(top.bit_length())] + [top]
best_rate = lambda top, spec, w=0: max(curve_rate(spec, w, u) for u in sizes(top))


def value(tokens, name, default=""):
    """A simulator key's value in a spec."""
    return next((t.split("=", 1)[1] for t in tokens.split() if t.startswith(name + "=")), default)


def per_item(tokens):
    """Units per item of a model: the middle of its `upi` range."""
    lo, _, hi = value(tokens, "upi", "1").partition(":")
    return (int(lo) + int(hi or lo)) // 2 if value(tokens, "cost", "item") != "item" else 1


def fixed_sizes(tokens):
    """The fixed sizes a model runs, in items: powers of two and its seed's doublings."""
    seed = int(value(tokens, "seed")) // per_item(tokens)
    chain = {seed << k for k in range(4)} | {seed >> k for k in range(1, 8) if seed % (1 << k) == 0}
    return sorted(set(SIZES) | {c for c in chain if 1 <= c <= 2048})


def step_size(rates, start, gain):
    """The size a step-wise rule settles at: from `start`, double while the doubled size clears
    `gain`, then halve while the size does not clear it against its half."""
    s = min(rates, key=lambda u: abs(math.log2(u / max(start, 1))))
    while 2 * s in rates and rates[2 * s] >= rates[s] * (1 + gain):
        s *= 2
    while s // 2 in rates and rates[s] < rates[s // 2] * (1 + gain):
        s //= 2
    return s


def model_lines(a, cls, seeds):
    dev, models, daily, _, _ = CLASSES[cls]
    gpu, lines = cls.startswith("gpu"), []
    for model, s in ((m, s) for m in models for s in seeds):
        spec = f"{dev} {models[model]} win=400000 {JOB} tseed={200 + s} nseed={s}"
        name = lambda sc, mode, var: f"name={sc}|{cls}|{model}|{mode}|{var}|{s} {spec} mode={mode}"
        upi = per_item(models[model])
        fix = lambda c: f" qfloor={3 * c * upi} fixed={c * upi}"
        for c in fixed_sizes(models[model]):
            lines.append(name("fixed", "-", c) + f" secs=3600{fix(c)}")
            if model in daily:
                lines.append(name("fixed2000", "-", c) + f" items=2000{fix(c)}")
        if model in daily:
            lines += [name("ideal", "-", f"{n}:{c}") + f" items={n}{fix(c)}"
                      for n in DAILY_ITEMS for c in IDEAL_SIZES]
        for mode in a.modes:
            lines.append(name("long", mode, "-") + " secs=3600")
            if gpu:
                lines.append(name("server", mode, "-") + " secs=18000")
            if model not in daily:
                continue
            for n in DAILY_ITEMS:
                lines.append(name("daily", mode, f"warm{n}") + f" items={n} starts=7 restart=0")
                lines.append(name("daily", mode, f"restart{n}") + f" items={n} starts=7")
            lines.append(name("first", mode, "none") + " items=2000")
            if gpu:
                lines.append(name("first", mode, "shipped") + " items=2000 ship=192")
    return lines


def curve_lines(a, cls, seeds):
    dev, _, _, flats, _ = CLASSES[cls]
    gpu, lines = cls.startswith("gpu"), []
    label = lambda k: k.replace(" ", "_")
    for mode, s in ((m, s) for m in a.modes for s in seeds):
        syn = f"{dev} {SYNTH} mode={mode} nseed={600 + s} v=1 compact=1"
        name = lambda sc, case, var="-": f"name={sc}|{cls}|{label(case)}|{mode}|{var}|{s} {syn}"
        for (flat, ref), prof, (kind, job) in (
                (f, p, k) for f in flats for p in ([0, 1] if gpu else [0]) for k in FLAT_JOBS):
            lines.append(name("flat", f"{flat}{prof}", kind) + f" {job} trace=flat-{flat}.txt "
                         f"lag=2 qfloor=192 tshared=1 tref={ref} tseed={300 + s}"
                         + " ship=192" * prof)
        lines += [name("syn", k) + f" {c}" for k, c in {**GAUSS, **NOISE}.items()]
        flat, ref = REAL_NOISE[cls]
        lines += [name("syn", k) + f" curve=geo:{g}:22 win=1500 trace=flat-{flat}.txt tnoise=1 "
                  f"tshared=1 tref={ref} tseed={300 + s}" for k, g in REAL.items()]
        lines += [name("shape", k) + f" curve={c} noise=0.03 ndist=g win={win}"
                  for k, (c, _, win) in SHAPES.items()]
        if gpu:
            lines.append(name("shape", "rate changes at a restart") + f" {RESTART_CHANGE}")
        if cls in ("mac", "apu"):
            lines.append(f"name=flips|{cls}|vith|{mode}|-|{s} {dev} {MAC['vith']} mode={mode} "
                         f"secs=3600 levels=0.25:60 {JOB} tseed={200 + s} nseed={s}")
    return lines


def acceptance_lines(a):
    seeds = range(1, a.seeds + 1)
    lines = [line for cls in a.classes for make in (model_lines, curve_lines)
             for line in make(a, cls, seeds)]
    cases = {**{k: f"{GPU_BIG} {spec} lag=2 qfloor=192" for k, spec in UNITS.items()},
             **{k: f"{GPU_BIG} {CUDA['wdvit']} {q} qfloor=192" for k, q in QUEUES.items()}}
    for mode, s in ((m, s) for m in a.modes for s in seeds):
        tail = f"mode={mode} tseed={200 + s} nseed={s}"
        for case, (dev, model, what) in DESKTOP.items():
            lines.append(f"name=desk|-|{case.replace(' ', '_')}|{mode}|-|{s} {dev} "
                         f"{DESKTOP_MODELS[model]} {what} win=400 {JOB} {tail}")
        for case, spec in cases.items():
            lines.append(f"name=units|-|{case.replace(' ', '_')}|{mode}|-|{s} {spec} secs=1800 "
                         f"v=1 compact=1 {tail}")
    return lines


def run(lines, a):
    """Every line on the simulator, in `a.jobs` shards; one dict per process start. A scenario
    that panics fails the run."""
    work, procs, rows, panics = tempfile.mkdtemp(prefix="sizing-"), [], [], []
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
                panics.append(line[:300])
                continue
            r = dict(t.split("=", 1) for t in line.split())
            rows.append(dict(r, key=tuple(r["name"].replace("_", " ").split("|"))))
    shutil.rmtree(work)
    if panics:
        sys.exit("scenarios panicked:\n" + "".join(panics))
    return rows


def arr(r, k):
    """A per-window column, run-length decoded."""
    out = []
    for x in filter(None, r.get(k, "").split(",")):
        v, _, n = x.partition("x")
        out += [int(v)] * int(n or 1)
    return out


ips = lambda r: int(r["items"]) / max(float(r["ms"]), 1) * 1000  # as the job saw it
true_ips = lambda r: int(r["items"]) / float(r["true_s"])  # at the curve's own rates
above = lambda r: 0 < int(r["open"]) < int(r["W"])
num = lambda k: lambda r: float(r[k])
total = lambda rs, k: int(sum(map(num(k), rs)))
gb = lambda mb: f"{mb / 1024:.1f}"
mean = lambda rs, f: st.mean(map(f, rs))
dist = lambda vs: " ".join(f"{k}:{n}" for k, n in sorted(collections.Counter(vs).items()))
ws = lambda rs, upi=1: dist(int(r["W"]) // upi for r in rs)
spread = lambda vs: f"{st.mean(vs):.2f} ({min(vs):.2f})"  # mean (worst)
share = lambda n, d: f"{100 * n / max(d, 1):.1f} %"
table = lambda title, cols: print(f"\n### {title}\n\n| " + " | ".join(cols) + " |\n"
                                  + "|---" * len(cols) + "|")
row = lambda *cells: print("| " + " | ".join(map(str, cells)) + " |")


def tables(rows, a):
    g, scen = collections.defaultdict(list), collections.defaultdict(list)
    top = collections.defaultdict(int)  # the largest batch a synthetic curve ran, per class
    for r in rows:
        g[r["key"][:5]].append(r)
        scen[r["key"]].append(r)
        if r["key"][0] in ("syn", "shape"):
            top[r["key"][1]] = max(top[r["key"][1]], max(arr(r, "budgets")))
    gain = lambda cls, mode: (a.throughput if mode == "throughput"
                              else a.gpu if CLASSES[cls][4] == "gpu" else a.strict)
    fixed, by_seed = collections.defaultdict(dict), collections.defaultdict(dict)
    for (sc, cls, model, _, var), rs in g.items():
        if sc in ("fixed", "fixed2000"):
            for r in rs:
                seed = (sc, cls, model, r["key"][5])
                by_seed[seed][int(var)] = max(by_seed[seed].get(int(var), 0), ips(r))
        if sc == "fixed":
            fixed[(cls, model)][int(var)] = mean(rs, ips)
    # Against the best fixed size of the same seed, so both see the same host noise.
    best_of = lambda r, sc: max(by_seed[(sc,) + r["key"][1:3] + r["key"][5:6]].values())
    versus = lambda r, sc="fixed": 100 * (ips(r) / best_of(r, sc) - 1)
    for sc, title in (("long", "Long desktop job (1 h)"),
                      ("server", "Dedicated server job (5 h; best fixed size of the 1 h job)")):
        table(title, ["class", "model", "mode", "items/s mean (worst)", "best fixed size",
                      "vs the seed's best fixed, mean (worst)", "W at end", "step-rule W",
                      "trials started", "pool GB: time-weighted", "at the end", "peak",
                      "in trials, peak", "at W", "host RAM GB: booked peak", "real peak"])
        for (s, cls, model, mode, _), rs in sorted(g.items()):
            if s != sc:
                continue
            tokens = CLASSES[cls][1][model]
            fx, pu, upi = fixed[(cls, model)], float(value(tokens, "pu")), per_item(tokens)
            best = max(fx, key=fx.get)
            v, vs = [ips(r) for r in rs], [versus(r) for r in rs]
            seed = int(value(tokens, "seed")) // upi
            row(cls, model, mode, spread(v), f"{best}: {fx[best]:.2f}",
                f"{st.mean(vs):+.1f} % ({min(vs):+.1f} %)", ws(rs, upi),
                step_size(fx, seed, gain(cls, mode)), f"{mean(rs, num('trials')):.1f}",
                gb(mean(rs, num("poolmean"))), gb(mean(rs, num("poollast"))),
                gb(max(map(num("poolpeak"), rs))), gb(max(map(num("trialpeak"), rs))),
                gb(max(pu * max(int(r["W"]), 0) for r in rs)), gb(max(map(num("rssbook"), rs))),
                gb(max(map(num("rsspeak"), rs))))
    daily_tables(g, versus, a)
    curve_tables(g, scen, top, gain, a)
    memory_tables(g)


def daily_tables(g, versus, a):
    table("Daily job: items per model, 7 days, wall time summed over the models",
          ["class", "mode", "items", "first day s", "warm, days 2-7 s", "after restart, days 2-7 s",
           "ideal s", "warm / ideal", "restart / ideal", "trials started, mean (max)",
           "later starts opening at the stored size", "pool GB, time-weighted"])
    pick = lambda rs, first: [r for r in rs if (r["start"] == "0") == first]
    for cls, mode, n in ((c, m, n) for c in a.classes for m in a.modes for n in DAILY_ITEMS):
        daily = CLASSES[cls][2]
        # n items per model at the best of the ideal sizes, from a load.
        ideal = sum(min(mean(g[("ideal", cls, m, "-", f"{n}:{c}")], num("ms")) for c in IDEAL_SIZES)
                    for m in daily) / 1000
        runs = lambda var, m: g[("daily", cls, m, mode, f"{var}{n}")]
        day = lambda var, first: sum(mean(pick(runs(var, m), first), num("ms"))
                                     for m in daily) / 1000
        w, again = day("warm", False), day("restart", False)
        both = [r for var in ("warm", "restart") for m in daily for r in runs(var, m)]
        later = [r for r in both if r["start"] != "0" and int(r["ostored"]) > 0]
        row(cls, mode, n, f"{day('restart', True):.1f}", f"{w:.1f}", f"{again:.1f}", f"{ideal:.1f}",
            f"{w / ideal:.2f}", f"{again / ideal:.2f}",
            f"{mean(both, num('trials')):.2f} ({max(map(num('trials'), both)):.0f})",
            f"{share(sum(r['firstw'] == '0' for r in later), len(later))} of {len(later)}",
            gb(mean(both, num("poolmean"))))
    table("First job of 2 000 items: items/s against the best fixed size of the same job",
          ["class", "model", "mode", "store", "items/s mean (worst)", "vs best, mean (worst)",
           "trials started", "pool GB, time-weighted"])
    for (s, cls, model, mode, var), rs in sorted(g.items()):
        if s == "first":
            v, vs = [ips(r) for r in rs], [versus(r, "fixed2000") for r in rs]
            row(cls, model, mode, var, spread(v), f"{st.mean(vs):+.1f} % ({min(vs):+.1f} %)",
                f"{mean(rs, num('trials')):.1f}", gb(mean(rs, num("poolmean"))))


def curve_tables(g, scen, top, gain, a):
    table("Flat model, one host level for every size: starts ending above their opening size",
          ["class", "mode", "12-window starts", "60-window starts", "12 000-item starts",
           "stored above the first opening", "largest budget"])
    first = lambda rs: next((int(r["open"]) for r in rs if int(r["open"]) > 0), 0)
    for cls, mode in ((c, m) for c in a.classes for m in a.modes):
        mine = [rs for k, rs in scen.items() if k[0] == "flat" and k[1] == cls and k[3] == mode]
        cells = []
        for kind, _ in FLAT_JOBS:
            rs = [r for v in mine if v[0]["key"][4] == kind for r in v]
            cells.append(f"{share(sum(map(above, rs)), len(rs))} of {len(rs)}")
        stored = sum(int(r["stored"]) > first(v) > 0 for v in mine for r in v)
        row(cls, mode, *cells, f"{stored} of {sum(map(len, mine))}",
            max(max(arr(r, "budgets")) for v in mine for r in v))
    table("Rate curves: share of the best rate at any batch size the device granted",
          ["class", "case", "mode", "% of best", "starts ending above their opening", "W at end",
           "step-rule W"])
    for (s, cls, label, mode, _), rs in sorted(g.items()):
        if s == "syn":
            c = (f"geo:{REAL[label]}:22" if label in REAL
                 else value({**GAUSS, **NOISE}[label], "curve"))
            rates = {u: curve_rate(c, 0, u) for u in sizes(top[cls])}
            row(cls, label, mode, f"{100 * mean(rs, true_ips) / best_rate(top[cls], c):.1f}",
                share(sum(map(above, rs)), len(rs)), ws(rs), step_size(rates, 64, gain(cls, mode)))
    table("Rate shapes: minutes until the batch size runs within 10 % of the best rate",
          ["class", "shape", "mode", "minutes, median (max)", "% of best over the job", "W at end"])
    for (s, cls, label, mode, _), rs in sorted(g.items()):
        if s != "shape":
            continue
        c, at, _ = SHAPES.get(label, ("knee:1.3:256:8", 0, 0))
        if label not in SHAPES:  # the rate changed at the restart: the second start
            rs = [r for r in rs if r["start"] == "1"]
        mins = []
        for b, ms in ((arr(r, "budgets"), arr(r, "wms")) for r in rs):
            target = 0.9 * best_rate(top[cls], c, at)
            hit = next((i for i in range(at, len(b)) if curve_rate(c, i, b[i]) >= target), None)
            mins.append(math.inf if hit is None else sum(ms[at:hit]) / 60000)
        row(cls, label, mode, f"{st.median(mins):.1f} ({max(mins):.1f})",
            f"{100 * mean(rs, true_ips) / best_rate(top[cls], c, at):.1f}", ws(rs))
    table("Two host levels ±25 % held 60 s on unified-memory traces (ViT-H, 1 h)",
          ["class", "mode", "items/s mean (worst)", "working-size changes, mean (max)", "W at end"])
    for (s, cls, _, mode, _), rs in sorted(g.items()):
        if s == "flips":
            row(cls, mode, spread([ips(r) for r in rs]),
                f"{mean(rs, num('flips')):.1f} ({max(map(num('flips'), rs)):.0f})", ws(rs))


def memory_tables(g):
    table("Desktop memory: room and pressure changes, deaths (400 windows)",
          ["case", "mode", "items/s mean (worst)", "W at end", "out of memory", "deaths",
           "clamped batches", "trims", "least room a batch left, MiB", "peak pool GB",
           "windows the pool held memory another process asked for",
           "windows grown under pressure", "largest budget after a death, of the one that died"])
    for (s, _, case, mode, _), rs in sorted(g.items()):
        if s != "desk":
            continue
        after = []
        for r in rs:
            b, died = arr(r, "budgets"), arr(r, "died")
            if died:
                after.append(f"{max(b[died[0] + 1:] or [0])} of {b[died[0]]}")
        row(case, mode, spread([ips(r) for r in rs]), ws(rs), total(rs, "oom"), total(rs, "deaths"),
            total(rs, "clamps"), total(rs, "trims"), min(int(r["slack"]) for r in rs),
            gb(max(map(num("poolpeak"), rs))), total(rs, "overroom"), total(rs, "grewp"),
            dist(after) or "-")
    table("Items of varying cost, and the caller's queue (30 min, gpu-ram)",
          ["case", "mode", "items/s mean (worst)", "W at end", "queue-bound windows",
           "batches above the working size, outside a trial", "clamped batches"])
    for (s, _, case, mode, _), rs in sorted(g.items()):
        if s == "units":
            row(case, mode, spread([ips(r) for r in rs]), ws(rs),
                share(total(rs, "qbound"), total(rs, "windows")), total(rs, "offsize"),
                total(rs, "clamps"))


def fidelity(a):
    """Each real job beside its replay on the same day's traces and on the other day's."""
    jobs = [dict(t.split("=") for t in line.split()) for line in open(f"{a.traces}/jobs.txt")]
    pairs = collections.defaultdict(list)
    for job in jobs:
        pairs[job["pair"]].append(job)
    lines = []
    for pair, js in pairs.items():
        j = js[0]
        for day in ("cuda", "cuda2"):
            spec = CUDA[j["model"]].replace("trace=cuda-", f"trace={day}-")
            lines += [f"name=fid|{pair}|{day}|-|-|{s} {GPU_BIG} {spec} {JOB} win=100000 "
                      f"items={j['items']} starts={len(js)} tseed={s}"
                      + " ship=192" * (j["prof"] == "p") for s in range(1, a.seeds + 1)]
    out = collections.defaultdict(list)
    for r in run(lines, a):
        out[(r["key"][1], r["key"][2], r["start"])].append(r)
    table("Replay against real jobs of the same sizing code (replay: mean, worst..best of "
          "seeds; real batches: how much faster the job's batches ran than the trace at the same "
          "sizes)", ["model", "items", "profile", "start", "real items/s", "real W", "real pool GB",
                     "replay, same day", "gap", "real batches", "W", "pool GB", "replay, other day",
                     "gap", "real batches", "W", "pool GB"])
    for job in sorted(jobs, key=lambda j: (int(j["pair"]), j["start"])):
        cells = []
        for day in ("cuda2", "cuda"):
            rs = out[(job["pair"], day, job["start"])]
            v = [ips(r) for r in rs]
            cells += [f"{st.mean(v):.2f} ({min(v):.2f}..{max(v):.2f})",
                      f"{100 * (st.mean(v) / float(job['ips']) - 1):+.1f} %",
                      f"{100 * (float(job[day]) - 1):+.1f} %", ws(rs),
                      gb(mean(rs, num("poolmean")))]
        row(job["model"], job["items"], job["prof"], job["start"], job["ips"], job["W"],
            gb(int(job["pool"])), *cells)


def trace_sizes(a):
    table("Batches behind each traced size", ["trace", "size: batches"])
    for name in sorted(os.listdir(a.traces)):
        if name.endswith(".txt") and name != "jobs.txt":
            heads = [line.split() for line in open(f"{a.traces}/{name}")
                     if line.startswith("size ")]
            row(name, " ".join(f"{h[1]}:{h[5]}" for h in heads))


# The earlier study's trace cells: model: (pu, base, rsspu, items of one run, trace).
REFERENCE = {"wdvit": (46, 698, 5, 12000, "cuda-wdvit"),
             "wdvitF": (46, 698, 5, 12000, "cuda-wdvit-fixed"),
             "vith": (45, 2730, 5, 12000, "cuda-vith"),
             "vithF": (45, 2730, 5, 12000, "cuda-vith-fixed"),
             "mclip": (17, 700, 5, 12000, "cuda-mclip"), "doctr": (9, 700, 60, 5000, "cuda-doctr"),
             "flor": (480, 700, 5, 6000, "cuda-flor"),
             "minilm": (0.0204, 700, 0.005, 12000, "cuda-minilm cost=token upi=192:200 seed=12544")}


def reference(a):
    lines = []
    for (m, (pu, base, rss, items, tr)), s in ((m, s) for m in REFERENCE.items()
                                               for s in range(201, 201 + a.seeds)):
        tr, _, cost = tr.partition(" ")
        job = (f"{GPU_BIG} pu={pu} base={base} rsspu={rss} trace={tr}.txt {cost or 'seed=64'} "
               f"{JOB} win=20000 tseed={s}")
        lines += [f"name=ref|{m}|run|{s} {job} items={items} starts=2",
                  f"name=ref|{m}|hour|{s} {job} items={10 * items}"]
        if m.startswith(("wdvit", "vith")):
            lines.append(f"name=ref|{m}|profile|{s} {job} items=6000 starts=2 ship=192")
    g = collections.defaultdict(list)
    for r in run(lines, a):
        g[r["key"][1:3] + (r["start"],)].append(r)
    for k, rs in sorted(g.items()):
        print(*k, f"items/s {mean(rs, ips):.2f} W ({ws(rs)}) pool time-weighted "
              f"{gb(mean(rs, num('poolmean')))} trials started "
              f"{st.median(int(r['trials']) for r in rs)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bin", required=True, help="the panoptikon test binary (see the README)")
    ap.add_argument("--traces", default=os.path.expanduser("~/panoptikon-archive/sizing-traces"))
    ap.add_argument("--set", default="acceptance",
                    choices=("acceptance", "fidelity", "traces", "reference"))
    ap.add_argument("--classes", default=",".join(CLASSES), type=lambda v: v.split(","))
    ap.add_argument("--modes", default="balanced,throughput", type=lambda v: v.split(","))
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--gpu", type=float, default=0.05, help="balanced gain per doubling, GPU")
    ap.add_argument("--strict", type=float, default=0.15, help="balanced gain, CPU and unified")
    ap.add_argument("--throughput", type=float, default=0.02, help="throughput gain, every device")
    a = ap.parse_args()
    sets = {"acceptance": lambda: tables(run(acceptance_lines(a), a), a),
            "fidelity": lambda: fidelity(a), "traces": lambda: trace_sizes(a),
            "reference": lambda: reference(a)}
    sets[a.set]()


if __name__ == "__main__":
    main()
