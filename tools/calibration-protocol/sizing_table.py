#!/usr/bin/env python3
"""Acceptance tables for the batch-size rule: runs the ledger's closed-loop simulator (`sizing_sim`,
an ignored test in the panoptikon crate) over every scenario and device class, one markdown table
per scenario. CUDA rows run on each day's traces (`--days`); a verdict must hold on both.
`--set fidelity` replays real jobs of the sizing code under test beside what they measured,
`--set traces` lists the batches behind each traced size and the gains per doubling, and
`--set reference` prints the trace-replay cells the earlier study measured.
"""
import argparse, collections, functools, math, os, random, shutil, statistics as st, subprocess
import sys, tempfile

# A model: the simulator keys of its trace or curve and seed. Memory (`pu`: MiB a unit allocates,
# `ratio`: pool over allocated, `base`, `rsspu`: the ledger's host RAM a unit) comes from the
# traces' `models.txt` unless given. docTR's host RAM is as the host-RAM runs measured it: 55 MiB
# a unit within ±20 % per batch, ~900 MiB kept by the first batch after a load, and the resident
# set read after a batch ~75 MiB below where the next one starts.
CUDA = {"wdvit": "trace=cuda-wdvit.txt seed=64", "vith": "trace=cuda-vith.txt seed=64",
        "mclip": "trace=cuda-mclip.txt seed=192",
        "doctr": "trace=cuda-doctr.txt seed=128 rsspu=55 rsssd=0.2 rssfirst=900 rsslag=75",
        "flor": "trace=cuda-flor.txt seed=4",
        "minilm": "trace=cuda-minilm.txt cost=token upi=192:200 pu=0.0204 ratio=1 base=700 "
                  "rsspu=0.005 seed=120000"}
# Runs of one user-capped size each (the first day only), with their model's memory.
CUDA_FIXED = {"wdvit-fixed": ("trace=cuda-wdvit-fixed.txt seed=64", "wdvit"),
              "vith-fixed": ("trace=cuda-vith-fixed.txt seed=64", "vith")}
# Seeds as the registry derives them (2 GiB over the MiB a unit costs), at a Mac's cost.
MAC = {"vith": "trace=mac2-vith.txt tmin=9 seed=8", "mclip": "trace=mac2-mclip.txt tmin=9 seed=64"}
# No archived run measured a rate curve on the CPU device: synthetic curves carrying the real
# noise of the CPU device's windows (one recorded window per value).
CPU_NOISE = "trace=flat-cpu.txt tnoise=1 tshared=1 tref=1 ovh=50 pu=60 base=700 rsspu=0 seed=8"
CPU = {"flat-above-8": f"curve=knee:1.25:8:20 {CPU_NOISE}",
       "rising": f"curve=geo:1.1:20 {CPU_NOISE}"}
DAILY = ["wdvit", "vith", "mclip", "doctr", "flor"]
GPU_FLAT = [("wd", 64), ("vh", 128), ("fl", 64), ("w8", 8)]
MAC_FLAT = [("mac", 8)]
# Per device class: device and memory, models, the models of a daily job, flat series (name,
# size), which gain a size must show in balanced mode, and the traces it replays.
CLASSES = {
    "gpu-ram": ("dev=gpu-ram room=95000 total=97000", CUDA, DAILY, GPU_FLAT, "gpu", "cuda"),
    "gpu": ("dev=gpu room=95000 total=97000", CUDA, DAILY, GPU_FLAT, "gpu", "cuda"),
    "apu": ("dev=apu room=20000 total=24000", MAC, list(MAC), MAC_FLAT, "unified", "mac"),
    "mac": ("dev=mac room=105956 total=110100 queue=first1", MAC, list(MAC), MAC_FLAT, "unified",
            "mac"),
    "cpu": ("dev=cpu room=40000 total=48000", CPU, list(CPU), [("cpu", 1)], "unified", "cpu")}
SIZES = [1 << k for k in range(11)]
MIN_BATCHES = 30  # a fixed size resting on fewer recorded batches is no reference
SYNTH = "pu=46 base=698 rsspu=5 seed=64"  # synthetic curves: 46 MiB a unit
JOB = "lag=2 qfloor=192 v=1 compact=1"
GPU_BIG = "dev=gpu-ram room=95000 total=97000"
CARD16 = "dev=gpu-ram room=14500 total=15500"
SHIPPED_W = "ship=192:k:128"  # a shipped row whose working size is 128
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
              "cpu": ("cpu", 1)}
REAL = {f"{g}x a doubling, real noise": g for g in (1.01, 1.015, 1.03, 1.05, 1.06, 1.15, 1.16)}
NOISE = {"scatter 35 %, rising": "curve=knee:1.3:256:8 noise=0.35 ndist=g win=300 starts=10",
         "scatter 35 %, flat": "curve=flat:22 noise=0.35 ndist=g win=300 starts=10",
         "two levels ±10 %, rising": "curve=knee:1.3:256:8 levels=0.1:60 win=300 starts=10",
         "two levels ±10 %, flat": "curve=flat:22 levels=0.1:60 win=300 starts=10"}
FLAT_JOBS = [("s12", "win=12 starts=40"), ("s60", "win=60 starts=40"),
             ("six", "win=20000 items=12000 starts=6")]
DAILY_ITEMS = [1, 10, 30, 50, 100, 200, 500, 1000]
IDEAL_SIZES = [1, 2, 4, 8, 16, 32, 64, 128, 256]  # a daily job's ideal: the best of these, from a load
MAC8 = "dev=mac room=4000 total=6144 queue=first1"
MAC_REAL = "dev=mac room=105956 total=110100 queue=first1"
# Desktop memory cases: device, model and what happens to memory.
DESKTOP = {
    "16 GB card, another app takes 12 GB at window 60 for 140 windows":
        (CARD16, "wdvit", "roomsched=60:2500,200:14500"),
    "the same, the free reading stale when it happens":
        (CARD16, "wdvit", "roomsched=60:2500,200:14500 roomlate=1"),
    "16 GB card, Florence, room halves at window 40": (CARD16, "flor", "roomsched=40:7000"),
    "host RAM free falls from 51 to 12 GB at window 30 (docTR)":
        (GPU_BIG, "doctr", "hostsched=30:12000"),
    "worker dies at window 30, host RAM booked": (GPU_BIG, "flor", "die=30"),
    "8 GB Mac, pressure warning at 40, paging at 80, normal at 160":
        (MAC8, "mmclip", "pressure=40:warning,80:paging,160:normal"),
    "8 GB Mac, room falls to 2 GB at window 60": (MAC8, "mmclip", "roomsched=60:2000"),
    "APU, 24 GB, room falls to 6 GB at window 60, stale":
        ("dev=apu room=20000 total=24000", "mvith", "roomsched=60:6000 roomlate=1"),
    "CPU, 16 GB, room falls to 4 GB at window 60":
        ("dev=cpu room=11000 total=16000", "rising", "roomsched=60:4000")}
# A restart of docTR: host RAM booked in the first full window against what it used.
RESTARTS = {"docTR restart, first window of costly pages": "rsshot=1",
            "docTR restart, pages as they come": ""}
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


@functools.lru_cache(maxsize=None)
def read_trace(path):
    """{size: recorded values} of a trace file."""
    lines, out = open(path).read().split("\n"), {}
    for i, line in enumerate(lines):
        if line.startswith("size "):
            out[int(line.split()[1])] = [float(x) for x in lines[i + 1].split()]
    return out


def models_file(a):
    """`models.txt`: each model's memory keys, and the within-run host levels."""
    out = {}
    for line in open(f"{a.traces}/models.txt"):
        head, _, rest = line.strip().partition(" ")
        out[head.removeprefix("model=")] = rest
    return out


def class_models(a, cls):
    """{key: spec} of a class's models with their memory; CUDA keys carry their trace day."""
    mem = models_file(a)
    def with_mem(tokens, m):
        """`tokens` with the measured memory keys it does not set; as given if it sets `pu`."""
        given = {t.split("=")[0] for t in tokens.split()}
        return tokens if "pu" in given else " ".join(
            [tokens] + [t for t in mem[m].split() if t.split("=")[0] not in given])
    family = CLASSES[cls][5]
    if family == "mac":
        return {m: with_mem(t, f"m{m}") for m, t in MAC.items()}
    if family == "cpu":
        return dict(CPU)
    out = {}
    for day in a.days:
        for m, t in CUDA.items():
            t = t.replace("trace=cuda-", f"trace={day}-")
            if os.path.isfile(f"{a.traces}/{value(t, 'trace')}"):
                out[f"{m}@{day}"] = with_mem(t, m)
    for m, (t, like) in CUDA_FIXED.items():
        if os.path.isfile(f"{a.traces}/{value(t, 'trace')}"):
            out[f"{m}@cuda"] = with_mem(t, like)
    return out


def fixed_sizes(tokens):
    """The fixed sizes a model runs, in items: powers of two and its seed's doublings."""
    seed = int(value(tokens, "seed")) // per_item(tokens)
    chain = {seed << k for k in range(4)} | {seed >> k for k in range(1, 8) if seed % (1 << k) == 0}
    return sorted(set(SIZES) | {c for c in chain if 1 <= c <= 2048})


def behind(a, tokens, items):
    """The traced size a fixed size of `items` replays and its recorded values; None for a
    synthetic curve."""
    trace = value(tokens, "trace")
    if not trace or value(tokens, "tnoise") == "1":
        return None
    series = read_trace(f"{a.traces}/{trace}")
    units = items * per_item(tokens)
    near = min(series, key=lambda s: abs(math.log2(s / max(units, 1))))
    return near, series[near]


def interval(values, block=10, draws=400):
    """90 % block-bootstrap interval of the mean of `values`, as factors on the rate."""
    rnd, n, mean = random.Random(0), len(values), st.mean(values)
    blocks = [values[i:i + block] for i in range(0, n, block)]
    boot = sorted(mean / st.mean([v for _ in range(len(blocks)) for v in rnd.choice(blocks)])
                  for _ in range(draws))
    return boot[draws // 20], boot[-draws // 20]


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
    dev, _, daily, _, _, family = CLASSES[cls]
    gpu, lines = family == "cuda", []
    models = class_models(a, cls)
    for model, s in ((m, s) for m in models for s in seeds):
        tokens, base = models[model], model.split("@")[0]
        spec = f"{dev} {tokens} win=400000 {JOB} tseed={200 + s} nseed={s}"
        name = lambda sc, mode, var: f"name={sc}|{cls}|{model}|{mode}|{var}|{s} {spec} mode={mode}"
        upi, is_daily = per_item(tokens), base in daily
        fix = lambda c: f" qfloor={3 * c * upi} fixed={c * upi}"
        for c in fixed_sizes(tokens):
            lines.append(name("fixed", "-", c) + f" secs=3600{fix(c)}")
            if is_daily:
                lines.append(name("fixed2000", "-", c) + f" items=2000{fix(c)}")
        if is_daily:
            lines += [name("ideal", "-", f"{n}:{c}") + f" items={n}{fix(c)}"
                      for n in DAILY_ITEMS for c in IDEAL_SIZES]
        for mode in a.modes:
            lines.append(name("long", mode, "-") + " secs=3600")
            if gpu:
                lines.append(name("server", mode, "-") + " secs=18000")
            if not is_daily:
                continue
            for n in DAILY_ITEMS:
                lines.append(name("daily", mode, f"warm{n}") + f" items={n} starts=7 restart=0")
                lines.append(name("daily", mode, f"restart{n}") + f" items={n} starts=7")
                if gpu:
                    lines.append(name("daily", mode, f"shipped{n}") + f" items={n} starts=7 "
                                 + SHIPPED_W)
            lines.append(name("first", mode, "none") + " items=2000")
            if gpu:
                lines.append(name("first", mode, "shipped") + " items=2000 ship=192")
                lines.append(name("first", mode, "shipped,_W_128") + f" items=2000 {SHIPPED_W}")
    return lines


def curve_lines(a, cls, seeds):
    dev, _, _, flats, _, family = CLASSES[cls]
    gpu, lines = family == "cuda", []
    label = lambda k: k.replace(" ", "_")
    for mode, s in ((m, s) for m in a.modes for s in seeds):
        syn = f"{dev} {SYNTH} mode={mode} nseed={600 + s} v=1 compact=1"
        name = lambda sc, case, var="-": f"name={sc}|{cls}|{label(case)}|{mode}|{var}|{s} {syn}"
        for (flat, ref), prof, (kind, job) in (
                (f, p, k) for f in flats for p in ([0, 1] if gpu else [0]) for k in FLAT_JOBS):
            lines.append(name("flat", f"{flat}{prof}", kind) + f" {job} trace=flat-{flat}.txt "
                         f"lag=2 qfloor=192 tshared=1 tref={ref} tseed={300 + s}"
                         + " ship=192" * prof)
        if cls == "gpu-ram":  # a flat model on a 16 GB card
            for kind, job in FLAT_JOBS:
                lines.append(f"name=flat|16_GB_card|wd0|{mode}|{kind}|{s} {CARD16} {SYNTH} "
                             f"mode={mode} nseed={600 + s} v=1 compact=1 {job} trace=flat-wd.txt "
                             f"lag=2 qfloor=192 tshared=1 tref=64 tseed={300 + s}")
        lines += [name("syn", k) + f" {c}" for k, c in {**GAUSS, **NOISE}.items()]
        flat, ref = REAL_NOISE[cls]
        lines += [name("syn", k) + f" curve=geo:{g}:22 win=1500 trace=flat-{flat}.txt tnoise=1 "
                  f"tshared=1 tref={ref} tseed={300 + s}" for k, g in REAL.items()]
        lines += [name("shape", k) + f" curve={c} noise=0.03 ndist=g win={win}"
                  for k, (c, _, win) in SHAPES.items()]
        if gpu:
            lines.append(name("shape", "rate changes at a restart") + f" {RESTART_CHANGE}")
        if cls in ("mac", "apu"):
            lines.append(f"name=flips|{cls}|vith|{mode}|±25_%_held_60_s|{s} {dev} "
                         f"{class_models(a, cls)['vith']} mode={mode} secs=3600 levels=0.25:60 "
                         f"{JOB} tseed={200 + s} nseed={s}")
        if cls == "gpu-ram":  # the levels measured within a run, on top of the traces' own
            held = models_file(a)["held"]
            amp, hold = value(held, "amp"), value(held, "hold")
            for m, tokens in class_models(a, cls).items():
                if m.split("@")[0] in ("wdvit", "vith"):
                    lines.append(f"name=flips|{cls}|{m}|{mode}|±{float(amp):.1%}_held_{hold}_s|{s} "
                                 f"{dev} {tokens} mode={mode} secs=3600 levels={amp}:{hold} {JOB} "
                                 f"tseed={200 + s} nseed={s}")
    return lines


def acceptance_lines(a):
    seeds = range(1, a.seeds + 1)
    lines = [line for cls in a.classes for make in (model_lines, curve_lines)
             for line in make(a, cls, seeds)]
    cuda, mac = class_models(a, "gpu-ram"), class_models(a, "mac")
    day = a.days[0]
    models = {"wdvit": cuda[f"wdvit@{day}"], "flor": cuda[f"flor@{day}"],
              "doctr": cuda[f"doctr@{day}"], "mvith": mac["vith"], "mmclip": mac["mclip"],
              "rising": CPU["rising"]}
    cases = {**{k: f"{GPU_BIG} {spec} lag=2 qfloor=192" for k, spec in UNITS.items()},
             **{k: f"{GPU_BIG} {models['wdvit']} {q} qfloor=192" for k, q in QUEUES.items()}}
    for mode, s in ((m, s) for m in a.modes for s in seeds):
        tail = f"mode={mode} tseed={200 + s} nseed={s}"
        for case, (dev, model, what) in DESKTOP.items():
            lines.append(f"name=desk|-|{case.replace(' ', '_')}|{mode}|-|{s} {dev} "
                         f"{models[model]} {what} win=400 {JOB} {tail}")
        for case, spec in cases.items():
            lines.append(f"name=units|-|{case.replace(' ', '_')}|{mode}|-|{s} {spec} secs=1800 "
                         f"v=1 compact=1 {tail}")
        for case, what in RESTARTS.items():
            lines.append(f"name=restart|-|{case.replace(' ', '_')}|{mode}|-|{s} {GPU_BIG} "
                         f"{models['doctr']} {what} items=2000 starts=2 win=100000 {JOB} {tail}")
    return lines


def run(lines, a):
    """Every line on the simulator, in `a.jobs` shards; one dict per process start. A scenario
    that panics fails the run. With `--rows`, the simulator's lines are kept in that file, and
    read from it instead when it exists."""
    if a.rows and os.path.isfile(a.rows):
        return [dict(r, key=tuple(r["name"].replace("_", " ").split("|")))
                for r in (dict(t.split("=", 1) for t in line.split()) for line in open(a.rows))]
    work, procs, rows, panics = tempfile.mkdtemp(prefix="sizing-"), [], [], []
    test = "inferio::ledger::tests::sizing_sim::sizing_sim"
    for i in range(a.jobs):
        spec, out = f"{work}/spec{i}", f"{work}/out{i}"
        open(spec, "w").write("\n".join(lines[i::a.jobs]) + "\n")
        env = dict(os.environ, SIZING_SPEC=spec, SIZING_OUT=out, SIZING_TRACES=a.traces)
        cmd = ["nice", "-n", "8", a.bin, "--ignored", "--exact", "-q", test]
        procs.append((subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL), out))
    kept = open(a.rows, "w") if a.rows else None
    for proc, out in procs:
        proc.wait()
        for line in open(out):
            if kept and not line.startswith("panic"):
                kept.write(line)
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
DAY = {"cuda": "10-02", "cuda2": "10-03"}
split = lambda model: (model.split("@") + ["-"])[:2]  # model, trace day


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
    specs = {cls: class_models(a, cls) for cls in a.classes}
    fixed, by_seed = collections.defaultdict(dict), collections.defaultdict(dict)
    for (sc, cls, model, _, var), rs in g.items():
        # A fixed size that does not fit runs out of memory: no reference.
        if sc in ("fixed", "fixed2000") and not any(int(r["oom"]) for r in rs):
            fixed[(sc, cls, model)][int(var)] = mean(rs, ips)
            for r in rs:
                by_seed[(sc, cls, model, r["key"][5])][int(var)] = ips(r)

    def thin(cls, model, items):
        """A fixed size resting on fewer than `MIN_BATCHES` recorded batches."""
        b = behind(a, specs[cls][model], items)
        return b is not None and len(b[1]) < MIN_BATCHES

    def usable(sc, cls, model):
        """The fixed sizes with enough batches; all of them if none has."""
        fx = fixed[(sc, cls, model)]
        return {c: x for c, x in fx.items() if not thin(cls, model, c)} or fx

    def best(sc, cls, model):
        """The fixed size with the best mean over seeds."""
        fx = usable(sc, cls, model)
        return max(fx, key=fx.get)

    # Against the best fixed size's rate on the same seed, so both see the same host noise.
    versus = lambda r, sc="fixed": 100 * (ips(r) / by_seed[(sc,) + r["key"][1:3] + r["key"][5:6]][
        best(sc, *r["key"][1:3])] - 1)
    fixed_table(a, fixed, specs, thin)
    for sc, title in (("long", "Long desktop job (1 h)"),
                      ("server", "Dedicated server job (5 h; best fixed size of the 1 h job)")):
        table(title, ["class", "model", "trace day", "mode", "items/s mean (worst)",
                      "best fixed size", "vs the best fixed size on the same seed, mean (worst)",
                      "W at end", "step-rule W", "trials started", "pool GB: time-weighted",
                      "at the end", "peak", "in trials, peak", "at W", "host RAM GB: at W",
                      "booked peak", "real peak", "in trials, booked", "in trials, real"])
        for (s, cls, model, mode, _), rs in sorted(g.items()):
            if s != sc:
                continue
            tokens = specs[cls][model]
            fx, upi = fixed[("fixed", cls, model)], per_item(tokens)
            pool_w, rss_w = (float(value(tokens, k, "1")) for k in ("pu", "rsspu"))
            ratio = value(tokens, "ratio", "1")
            ratio_at = lambda u: (curve_rate("lad:" + ratio.replace(":", "/"), 0, u)
                                  if ":" in ratio else float(ratio))
            b, v, vs = best("fixed", cls, model), [ips(r) for r in rs], [versus(r) for r in rs]
            seed = int(value(tokens, "seed")) // upi
            w_end = max(max(int(r["W"]), 0) for r in rs)
            row(cls, *split(model), mode, spread(v),
                f"{b}: {fx[b]:.2f}" + "*" * thin(cls, model, b),
                f"{st.mean(vs):+.1f} % ({min(vs):+.1f} %)", ws(rs, upi),
                step_size(usable("fixed", cls, model), seed, gain(cls, mode)),
                f"{mean(rs, num('trials')):.1f}",
                gb(mean(rs, num("poolmean"))), gb(mean(rs, num("poollast"))),
                gb(max(map(num("poolpeak"), rs))), gb(max(map(num("trialpeak"), rs))),
                gb(pool_w * ratio_at(w_end) * w_end), gb(rss_w * w_end),
                gb(max(map(num("rssbook"), rs))), gb(max(map(num("rsspeak"), rs))),
                gb(max(map(num("trialrssbook"), rs))),
                gb(max(map(num("trialrss"), rs))))
    daily_tables(g, versus, a)
    curve_tables(g, scen, top, gain, a)
    memory_tables(g)


def fixed_table(a, fixed, specs, thin):
    table(f"Fixed sizes, 1 h: items/s (90 % block-bootstrap interval of the traced size's mean, "
          f"batches behind it); * fewer than {MIN_BATCHES} batches, left out of the best fixed "
          "size and the step-rule W unless every size is", ["class", "model", "trace day",
                                                            "size: items/s"])
    shown = set()
    for (sc, cls, model), fx in sorted(fixed.items()):
        if sc != "fixed" or (CLASSES[cls][5], model) in shown:
            continue
        shown.add((CLASSES[cls][5], model))
        cells = []
        for c, rate in sorted(fx.items()):
            b = behind(a, specs[cls][model], c)
            if b is None:
                cells.append(f"{c}: {rate:.2f}")
                continue
            lo, hi = interval(b[1])
            cells.append(f"{c}: {rate:.2f} ({rate * lo:.2f}-{rate * hi:.2f}, {len(b[1])})"
                         + "*" * thin(cls, model, c))
        row(cls, *split(model), "; ".join(cells))


def daily_tables(g, versus, a):
    table("Daily job: items per model, 7 days, wall time summed over the models (no real short "
          "job validates these rows; they rest on the traces' first-batch and warm-up figures)",
          ["class", "mode", "store", "items", "first day s", "warm, days 2-7 s",
           "after restart, days 2-7 s", "ideal s", "first / ideal", "warm / ideal",
           "restart / ideal", "trials started, mean (max)",
           "later starts opening at the stored size", "pool GB, time-weighted"])
    pick = lambda rs, first: [r for r in rs if (r["start"] == "0") == first]
    for cls, mode, n in ((c, m, n) for c in a.classes for m in a.modes for n in DAILY_ITEMS):
        models = [m for m in class_models(a, cls) if m.split("@")[0] in CLASSES[cls][2]]
        for day in sorted({split(m)[1] for m in models}):
            daily = [m for m in models if split(m)[1] == day]
            ideal = sum(min(mean(g[("ideal", cls, m, "-", f"{n}:{c}")], num("ms"))
                            for c in IDEAL_SIZES) for m in daily) / 1000
            runs = lambda var, m: g[("daily", cls, m, mode, f"{var}{n}")]
            day_s = lambda var, first: sum(mean(pick(runs(var, m), first), num("ms"))
                                           for m in daily) / 1000
            for store, vars_ in (("none", ("warm", "restart")), (SHIPPED_W, ("shipped",))):
                if not runs(vars_[0], daily[0]):
                    continue
                both = [r for var in vars_ for m in daily for r in runs(var, m)]
                later = [r for r in both if r["start"] != "0" and int(r["ostored"]) > 0]
                first = day_s(vars_[-1], True)
                w = day_s("warm", False) if "warm" in vars_ else None
                again = day_s(vars_[-1], False)
                row(cls, mode, f"{store} ({DAY.get(day, day)})" if day != "-" else store, n,
                    f"{first:.1f}", "-" if w is None else f"{w:.1f}", f"{again:.1f}",
                    f"{ideal:.1f}", f"{first / ideal:.2f}",
                    "-" if w is None else f"{w / ideal:.2f}", f"{again / ideal:.2f}",
                    f"{mean(both, num('trials')):.2f} ({max(map(num('trials'), both)):.0f})",
                    f"{share(sum(r['firstw'] == '0' for r in later), len(later))} of {len(later)}",
                    gb(mean(both, num("poolmean"))))
    table("First job of 2 000 items: items/s against the best fixed size of the same job",
          ["class", "model", "trace day", "mode", "store", "items/s mean (worst)",
           "vs the best fixed size on the same seed, mean (worst)", "trials started",
           "pool GB, time-weighted"])
    for (s, cls, model, mode, var), rs in sorted(g.items()):
        if s == "first":
            v, vs = [ips(r) for r in rs], [versus(r, "fixed2000") for r in rs]
            row(cls, *split(model), mode, var, spread(v),
                f"{st.mean(vs):+.1f} % ({min(vs):+.1f} %)", f"{mean(rs, num('trials')):.1f}",
                gb(mean(rs, num("poolmean"))))


def curve_tables(g, scen, top, gain, a):
    table("Flat model, one host level for every size: starts ending above their opening size",
          ["class", "mode", "12-window starts", "60-window starts", "12 000-item starts",
           "stored above the first opening", "largest budget", "peak pool GB"])
    first = lambda rs: next((int(r["open"]) for r in rs if int(r["open"]) > 0), 0)
    for cls, mode in ((c, m) for c in a.classes + ["16 GB card"] for m in a.modes):
        mine = [rs for k, rs in scen.items() if k[0] == "flat" and k[1] == cls and k[3] == mode]
        if not mine:
            continue
        cells = []
        for kind, _ in FLAT_JOBS:
            rs = [r for v in mine if v[0]["key"][4] == kind for r in v]
            cells.append(f"{share(sum(map(above, rs)), len(rs))} of {len(rs)}")
        stored = sum(int(r["stored"]) > first(v) > 0 for v in mine for r in v)
        row(cls, mode, *cells, f"{stored} of {sum(map(len, mine))}",
            max(max(arr(r, "budgets")) for v in mine for r in v),
            gb(max(int(r["poolpeak"]) for v in mine for r in v)))
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
    table("Two shared host levels held for an exponential time, added on top of the traces' own "
          "noise (1 h)", ["class", "model", "trace day", "levels", "mode", "items/s mean (worst)",
                          "working-size changes, mean (max)", "trials started", "W at end"])
    for (s, cls, model, mode, var), rs in sorted(g.items()):
        if s == "flips":
            row(cls, *split(model), var, mode, spread([ips(r) for r in rs]),
                f"{mean(rs, num('flips')):.1f} ({max(map(num('flips'), rs)):.0f})",
                f"{mean(rs, num('trials')):.1f}", ws(rs))


def memory_tables(g):
    table("Desktop memory: room and pressure changes, deaths (400 windows)",
          ["case", "mode", "items/s mean (worst)", "W at end", "out of memory", "deaths",
           "clamped batches", "trims", "least room a batch's allocation left, MiB",
           "peak pool GB", "windows the pool held memory another process asked for",
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
    table("Host RAM after a restart: booked against used in the first window of 16 items or more",
          ["case", "mode", "booked / used after the restart, mean (worst)",
           "windows before it", "in the first job, median", "host RAM GB: booked peak",
           "used peak"])
    for (s, _, case, mode, _), rs in sorted(g.items()):
        if s != "restart":
            continue
        ratios, before, in_job = [], [], []
        for r in rs:
            b, book, used = arr(r, "budgets"), arr(r, "rambook"), arr(r, "ramused")
            at = next((i for i, x in enumerate(b) if x >= 16), None)
            pairs = [(x, y) for x, y in zip(book, used) if x and y]
            if r["start"] == "0":
                in_job += [x / y for x, y in pairs[6:]]
            elif at is not None and used[at]:
                ratios.append(book[at] / used[at])
                before.append(at)
        row(case, mode, f"{st.mean(ratios):.2f} ({max(ratios):.2f})", dist(before),
            f"{st.median(in_job):.2f}" if in_job else "-", gb(max(map(num("rssbook"), rs))),
            gb(max(map(num("rsspeak"), rs))))
    table("Items of varying cost, and the caller's queue (30 min, gpu-ram)",
          ["case", "mode", "items/s mean (worst)", "W at end", "queue-bound windows",
           "windows that found the queue dry", "batches above the working size, outside a trial",
           "clamped batches"])
    for (s, _, case, mode, _), rs in sorted(g.items()):
        if s == "units":
            row(case, mode, spread([ips(r) for r in rs]), ws(rs),
                share(total(rs, "qbound"), total(rs, "windows")),
                share(total(rs, "dry"), total(rs, "windows")), total(rs, "offsize"),
                total(rs, "clamps"))


def fidelity(a):
    """Each real job beside its replay on the same day's traces and, for CUDA, the other day's."""
    jobs = [dict(t.split("=") for t in line.split()) for line in open(f"{a.traces}/jobs.txt")]
    pairs = collections.defaultdict(list)
    for job in jobs:
        pairs[job["pair"]].append(job)
    lines, cuda, mac = [], class_models(a, "gpu-ram"), class_models(a, "mac")
    for pair, js in pairs.items():
        j = js[0]
        for day in (("mac2",) if j["dev"] == "mac" else ("cuda", "cuda2")):
            spec = (f"{MAC_REAL} {mac[j['model'][1:]]}" if day == "mac2"
                    else f"{GPU_BIG} {cuda[j['model'] + '@' + day]}")
            lines += [f"name=fid|{pair}|{day}|-|-|{s} {spec} {JOB} win=100000 "
                      f"items={j['items']} starts={len(js)} tseed={s}"
                      + " ship=192" * (j["prof"] == "p") for s in range(1, a.seeds + 1)]
    out = collections.defaultdict(list)
    for r in run(lines, a):
        out[(r["key"][1], r["key"][2], r["start"])].append(r)
    table("Replay against real jobs of the same sizing code (replay: mean, worst..best of "
          "seeds; real batches: how much faster the job's batches ran than the trace at the same "
          "sizes; pool: time-weighted, above the pool at load; releases: the replay's trims and "
          "releases; queue: real windows after which the next grant waited 15 ms or more, "
          "replay windows that found the queue dry; trial windows: windows at a size other than "
          "the working size)",
          ["model", "items", "profile", "start", "real items/s", "real W", "real pool GB",
           "real releases", "real queue", "real trial windows", "replay, same day", "gap",
           "real batches", "W", "pool GB (range)", "pool gap", "releases", "queue",
           "trial windows", "replay, other day", "gap", "real batches", "W", "pool GB (range)",
           "pool gap"])
    for job in sorted(jobs, key=lambda j: (int(j["pair"]), j["start"])):
        cells = []
        for k, day in enumerate(("mac2",) if job["dev"] == "mac" else ("cuda2", "cuda")):
            rs = out[(job["pair"], day, job["start"])]
            v = [ips(r) for r in rs]
            pools = [float(r["poolmean"]) for r in rs]
            cells += [f"{st.mean(v):.2f} ({min(v):.2f}..{max(v):.2f})",
                      f"{100 * (st.mean(v) / float(job['ips']) - 1):+.1f} %",
                      f"{100 * (float(job[day]) - 1):+.1f} %", ws(rs),
                      f"{gb(st.mean(pools))} ({gb(min(pools))}..{gb(max(pools))})",
                      f"{100 * (st.mean(pools) / max(float(job['pool']), 1) - 1):+.0f} %"]
            if k == 0:
                trial = lambda r: sum(b != w for b, w in zip(arr(r, "budgets"), arr(r, "working")))
                cells += [f"{mean(rs, lambda r: int(r['trims']) + int(r['releases'])):.1f}",
                          f"{mean(rs, num('dry')):.1f}", f"{mean(rs, trial):.1f}"]
        cells += ["-"] * (25 - 10 - len(cells))
        dash = lambda k: job[k] if job[k] != "-1" else "-"
        row(job["model"], job["items"], job["prof"], job["start"], job["ips"], job["W"],
            gb(int(job["pool"])), dash("releases"), job["waits"], dash("trialw"), *cells)


def trace_sizes(a):
    table("Batches behind each traced size", ["trace", "size: batches"])
    names = sorted(n for n in os.listdir(a.traces) if n.endswith(".txt")
                   and n not in ("jobs.txt", "models.txt"))
    for name in names:
        heads = [line.split() for line in open(f"{a.traces}/{name}") if line.startswith("size ")]
        row(name, " ".join(f"{h[1]}:{h[5]}" + "*" * (int(h[5]) < MIN_BATCHES) for h in heads))
    table("Gain per doubling: the pooled trace (after run levels) and within one run (median "
          "over the runs with 5 or more batches at both sizes)",
          ["trace", "size", "pooled", "pooled batches", "within a run", "runs"])
    for name in (n for n in names if not n.startswith("flat-")):
        series = read_trace(f"{a.traces}/{name}")
        within = {int(p[2]): (p[4], p[7]) for p in (line.split() for line in
                  open(f"{a.traces}/{name}") if line.startswith("# within"))}
        for s in sorted(series):
            enough = 2 * s in series and min(len(series[s]), len(series[2 * s])) >= MIN_BATCHES
            if s in within or enough:
                pooled = st.mean(series[s]) / st.mean(series[2 * s]) - 1
                w, runs = within.get(s, ("-", "0"))
                row(name, f"{s}->{2 * s}", f"{100 * pooled:+.1f} %",
                    f"{len(series[s])} / {len(series[2 * s])}",
                    f"{w} %" if w != "-" else "-", runs)


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
               f"{JOB} win=20000 tseed={s} tlevel=0")
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
    ap.add_argument("--days", default="cuda,cuda2", type=lambda v: v.split(","),
                    help="the CUDA trace days")
    ap.add_argument("--modes", default="balanced,throughput", type=lambda v: v.split(","))
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--rows", help="keep the simulator's output here; reuse it if it exists")
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
