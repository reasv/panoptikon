#!/usr/bin/env python3
"""Build the batch-size simulator's trace files from archived real runs.

Which runs feed which trace is listed in a manifest kept in the archive (`--manifest`); the
traces hold private data and stay outside the repository.

A trace lists, per batch size, the time-ordered ms per unit of every recorded batch (from the
worker's tap), or of every full window where a run has no tap (Mac). Before pooling, each run's
batches are divided by that run's speed level, estimated on the sizes it shares with the pool;
the levels are written to the trace so a replay can draw one per start. `flat-*` traces replay
one real series, levels kept, at every size: a model whose rate does not depend on the batch
size, with the host's real noise. `jobs.txt` lists real jobs of the sizing code under test with
what they measured, and `models.txt` each model's memory as its real runs stored it.
"""
import argparse, glob, json, math, os, re, statistics as st, subprocess
from datetime import datetime

CUDA_MODELS = {"wdvit": "tags/wd-vit-tagger-v3", "vith": "clip/ViT-H-14-378-quickgelu_dfn5b",
               "mclip": "clip/apple_MobileCLIP-S1",
               "doctr": "doctr/db_resnet50_crnn_mobilenet_v3_small",
               "flor": "florence2/msft_large-more-detailed", "minilm": "textembed/all-MiniLM-L6-v2"}
SKIP_BATCHES = 6  # warm-up batches dropped at the start of each run
MIN_WINDOWS = 3  # windows behind a measured overhead or a window-level size
MIN_SHARED = 5  # batches of a size a run needs for its level and its own gains
WAIT_S = 0.015  # a gap between a settle and the next grant this long: the queue ran short
# A noise series whose time-weighted mean exceeds its median by more than this holds
# workload variation, not host noise.
MAX_MEAN_OVER_MEDIAN = 1.2

kv = lambda line: dict(re.findall(r'(\w+)=("[^"]*"|\S+)', line))
ts = lambda line: datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp()


def read_log(run_dir):
    path = os.path.join(run_dir, "panoptikon.log")
    if os.path.isfile(path):
        return open(path, errors="replace").read().splitlines()
    zst = subprocess.run(["zstd", "-dc", path + ".zst"], capture_output=True, text=True,
                         errors="replace")
    return zst.stdout.splitlines() if zst.returncode == 0 else None


def windows(lines):
    """Per model, the settled windows: grant time, settle time, budget, requests, outcome."""
    models = {}
    for line in lines:
        if "issued a memory grant" in line:
            g = kv(line)
            models.setdefault(g["model"], []).append(
                {"t": ts(line), "e": None, "budget": int(g["unit_budget"]),
                 "req": int(g["window_requests"]), "b": []})
        elif "settled a granted window" in line:
            s = kv(line)
            open_ = models.get(s["model"])
            if open_ and open_[-1]["e"] is None:
                open_[-1]["e"], open_[-1]["outcome"] = ts(line), s["outcome"]
    return {m: [w for w in ws if w["e"]] for m, ws in models.items()}


def tapped_runs(archive):
    """{run: {model: windows}} with each tapped batch (units, ms) inside the window it ran in."""
    out = {}
    for run_dir in sorted(glob.glob(os.path.join(archive, "runs", "*"))):
        lines = read_log(run_dir)
        if lines is None:
            continue
        # A line cut short when the run stopped does not end in a brace.
        taps = sorted((json.loads(l) for f in glob.glob(f"{run_dir}/tap/*.jsonl") for l in open(f)
                       if l.rstrip().endswith("}")), key=lambda b: b["t"])
        for model, ws in windows(lines).items():
            for b in taps:
                w = next((w for w in ws if w["t"] - 0.002 <= b["t"] <= w["e"] + 0.002), None)
                if w is not None:
                    w["b"].append((b["units"], b["duration_ms"]))
            out.setdefault(os.path.basename(run_dir), {})[model] = ws
    return out


def bucket(units):
    """The power of two, or else three times a power of two, `units` is a full batch of, or None."""
    for base in (1, 3):
        p = base << max(0, round(math.log2(max(units, 1) / base)))
        if units >= 0.8 * p and units * 0.9 <= p:
            return p
    return None


def write(path, first, series, overhead, digits=1, warm=(), levels=(), notes=()):
    """`overhead`: window ms outside the batches per size; a size without one is written `-`."""
    with open(path, "w") as f:
        f.writelines(f"# {n}\n" for n in notes)
        f.write(f"first {first:.1f}\n")
        if warm:
            f.write("warm " + " ".join(f"{x:.1f}" for x in warm) + "\n")
        if levels:
            f.write("levels " + " ".join(f"{x:.4f}" for x in levels) + "\n")
        for size, v in sorted(series.items()):
            ovh = f"{overhead[size]:.{digits}f}" if size in overhead else "-"
            f.write(f"size {size} ovh {ovh} n {len(v)}\n" + " ".join(f"{x:.4f}" for x in v) + "\n")


def pool_runs(runs):
    """Pool `runs` ([(run, [(size, ms per unit)])] in time order) after dividing each run by its
    level: its time per unit against the pool's, a batch-weighted geometric mean over the sizes
    where it has `MIN_SHARED` batches. Returns the series, the levels and, per doubling, the
    median gain within a run with the number of runs it rests on."""
    own = {run: {s: [v for u, v in b if u == s] for s in {u for u, _ in b}} for run, b in runs}
    level, weight = {run: 1.0 for run in own}, {run: len(b) for run, b in runs}
    for _ in range(5):
        pooled = {}
        for run, b in runs:
            for s, v in b:
                pooled.setdefault(s, []).append(v / level[run])
        pooled = {s: st.mean(v) for s, v in pooled.items()}
        for run, sizes in own.items():
            shared = {s: v for s, v in sizes.items() if len(v) >= MIN_SHARED}
            if shared:
                level[run] = math.exp(sum(len(v) * math.log(st.mean(v) / pooled[s])
                                          for s, v in shared.items())
                                      / sum(map(len, shared.values())))
        g = math.exp(sum(weight[r] * math.log(level[r]) for r in level)
                     / max(sum(weight.values()), 1))
        level = {r: l / g for r, l in level.items()}
    series, gains = {}, {}
    for run, b in runs:
        for s, v in b:
            series.setdefault(s, []).append(v / level[run])
    for sizes in own.values():
        m = {s: st.mean(v) for s, v in sizes.items() if len(v) >= MIN_SHARED}
        for s in m:
            if 2 * s in m:
                gains.setdefault(s, []).append(m[s] / m[2 * s] - 1)
    notes = [f"within {s} {2 * s} {100 * st.median(g):+.1f} % runs {len(g)}"
             for s, g in sorted(gains.items())]
    return series, [level[run] for run, b in runs if b], notes


def tap_trace(runs, model, path, only=None):
    """Pool every run of `model` in time order; `only` picks the runs the batches come from.
    Returns the raw series per size, run levels kept, for the flat traces.

    A size's window overhead is the median wall time outside the batches of windows of three
    equal full batches, or failing that of every window whose largest batch is that size."""
    picked = sorted((ws[0]["t"], run, ws) for run, ms in runs.items() for m, ws in ms.items()
                    if m == model and ws)
    equal, largest, first, warm, per_run = {}, {}, [], [], []
    for _, run, ws in picked:
        n = 0
        for w in ws:
            sizes = [bucket(u) for u, _ in w["b"]]
            wall, busy = (w["e"] - w["t"]) * 1000, sum(d for _, d in w["b"])
            if w["b"] and n >= SKIP_BATCHES and wall >= busy:
                if len(sizes) == 3 and sizes[0] and len(set(sizes)) == 1:
                    equal.setdefault(sizes[0], []).append(wall - busy)
                top = bucket(max(u for u, _ in w["b"]))
                if top:
                    largest.setdefault(top, []).append(wall - busy)
            n += len(w["b"])
    for _, run, ws in picked:
        if only and not only(run):
            continue
        batches = [b for w in ws for b in w["b"]]
        if batches:
            first.append(batches[0][1])
            warm.append(batches[1:SKIP_BATCHES])
        per_run.append((run, [(bucket(u), d / u) for u, d in batches[SKIP_BATCHES:] if bucket(u)]))
    raw = {}
    for _, b in per_run:
        for s, v in b:
            raw.setdefault(s, []).append(v)
    if not raw:
        return {}
    series, levels, notes = pool_runs(per_run)
    steady = {s: st.median(v) for s, v in series.items()}
    near = lambda u: steady[min(steady, key=lambda s: abs(math.log2(s / max(u, 1))))]
    # What each batch after the first takes on top of its steady time, median over loads.
    extra = [st.median(d - u * near(u) for u, d in (b[i] for b in warm if len(b) > i))
             for i in range(SKIP_BATCHES - 1) if any(len(b) > i for b in warm)]
    overhead = {s: st.median(g) for s, g in largest.items() if len(g) >= MIN_WINDOWS}
    overhead.update({s: st.median(g) for s, g in equal.items() if len(g) >= MIN_WINDOWS})
    write(path, st.median(first), series, {s: v for s, v in overhead.items() if s in series},
          warm=[max(0.0, x) for x in extra], levels=levels, notes=notes)
    return raw


def window_trace(leg_dirs, model, path, capped):
    """Each full clean window after the third adds three batches at its ms per item; a leg whose
    name matches `capped` ran under the user cap the pattern's group gives."""
    per_run, counted, first = [], {}, []
    for leg in leg_dirs:
        cap = re.match(capped, os.path.basename(os.path.dirname(leg.rstrip("/"))))
        b = []
        for n, w in enumerate(windows(read_log(leg) or []).get(model, []), 1):
            if n == 1:
                first.append((w["e"] - w["t"]) * 1000)
                continue
            size = min(w["budget"], int(cap.group(1))) if cap else w["budget"]
            if w["outcome"] != '"clean"' or n <= 3 or w["req"] < size or not bucket(size):
                continue
            b += [(bucket(size), (w["e"] - w["t"]) * 1000 / w["req"])] * 3
            counted[bucket(size)] = counted.get(bucket(size), 0) + 1
        per_run.append((leg, b))
    keep = {s for s, n in counted.items() if n >= MIN_WINDOWS}
    series, levels, notes = pool_runs([(r, [x for x in b if x[0] in keep]) for r, b in per_run])
    write(path, st.median(first), series, {s: 1.0 for s in series}, levels=levels, notes=notes)


def flat(series, path):
    """Replay one recorded series at every size from 1 to 4096 units, with 3 % of a window
    outside the batches."""
    mean, sizes = sum(series) / len(series), [1 << k for k in range(13)]
    write(path, mean, {s: series for s in sizes}, {s: 0.03 * s * mean for s in sizes}, digits=2)


def cpu_series(leg_dirs, model):
    """The ms of every clean window after the third on the CPU device, scaled to the mean
    window's tokens: the window's requests are the load generator's requests that ended when it
    settled."""
    out = []
    for leg in leg_dirs:
        ws = windows([l for l in read_log(leg) or [] if "gpu=CPU" in l]).get(model, [])
        text = subprocess.run(["zstd", "-dc", os.path.join(leg, "loadgen.jsonl.zst")],
                              capture_output=True, text=True).stdout
        reqs = [r for r in map(json.loads, text.splitlines())
                if r.get("kind") == "request" and r["model"] == model and r["ok"]]
        for n, w in enumerate(ws, 1):
            tokens = [r["units"]["token"] for r in reqs
                      if w["e"] - 0.05 <= r["t_end_wall"] <= w["e"] + 1]
            if n > 3 and w["outcome"] == '"clean"' and len(tokens) == w["req"]:
                out.append(((w["e"] - w["t"]) * 1000, sum(tokens)))
    mean = st.mean(t for _, t in out)
    out = [ms * mean / t for ms, t in out]
    ratio = sum(v * v for v in out) / sum(out) / st.median(out)
    if ratio > MAX_MEAN_OVER_MEDIAN:
        raise SystemExit(f"CPU noise series: time-weighted mean {ratio:.2f}x its median; "
                         "it holds workload variation, not host noise")
    return out


def speed(batches, trace):
    """How much faster `batches` ((units, ms)) ran than a trace's mean at the same sizes."""
    lines = open(trace).read().split("\n")
    m = {int(l.split()[1]): st.mean(map(float, lines[i + 1].split()))
         for i, l in enumerate(lines) if l.startswith("size ")}
    kept = [(u, d) for u, d in batches if bucket(u) in m]
    return sum(u * m[bucket(u)] for u, _ in kept) / max(sum(d for _, d in kept), 1e-9)


def waits(ws):
    """Windows after which the next grant came `WAIT_S` or more later."""
    return sum(b["t"] - a["e"] >= WAIT_S for a, b in zip(ws, ws[1:]))


def summary_figures(text):
    """A CUDA run summary's figures: stored slope and base, the pool (time-weighted, peak, at
    load), the largest batch measured, host RAM per unit and pool releases."""
    grab = lambda pattern: (lambda m: float(m[1]) if m else None)(re.search(pattern, text))
    pool = "POOL reserved_mb during the job: time-weighted mean"
    return {"slope": grab(r"slope_mb_per_unit = ([\d.]+)"), "base": grab(r"base_mb = (\d+)"),
            "mean": grab(pool + r" (\d+)"), "peak": grab(pool + r" \d+\s+peak (\d+)"),
            "at_load": grab(r'"reserved_at_load_mb": (\d+)'),
            "units": grab(r'"max_units_measured": (\d+)'),
            "rss": grab(r'"ram_mb_per_unit": ([\d.]+)'),
            "releases": grab(r'"pool_releases": (\d+)')}


def summaries(archive):
    """{(run, model id): summary text} for every run with a summary."""
    out = {}
    for path in sorted(glob.glob(os.path.join(archive, "runs", "*", "summary-*.txt"))):
        text = open(path).read()
        model = re.search(r"^model (\S+)", text, re.M)
        if model:
            out[(os.path.basename(os.path.dirname(path)), model[1])] = text
    return out


def ladder(ratios, least):
    """`size:ratio,...`: the median pool over allocated per size with `least` samples."""
    return ",".join(f"{u}:{st.median(v):.2f}" for u, v in sorted(ratios.items()) if len(v) >= least)


def cuda_memory(archives):
    """Per item-priced model: the stored slope (allocated MiB a unit), base and host RAM per
    unit (medians over the runs), and per size the pool over allocated where a batch grew the
    pool (tap)."""
    short = {model: name for name, model in CUDA_MODELS.items()}
    figs, ratios = {}, {}
    for archive in archives:
        for (run, model), text in summaries(archive).items():
            f, unit = summary_figures(text), re.search(r"worker batches \d+\s+unit (\w+)", text)
            if model not in short or not f["slope"] or not unit or unit[1] != "item":
                continue
            figs.setdefault(short[model], []).append(f)
            for path in glob.glob(os.path.join(archive, "runs", run, "tap", "*.jsonl")):
                for b in (json.loads(l) for l in open(path) if l.rstrip().endswith("}")):
                    u = bucket(b["units"]) if b.get("unit") == unit[1] else None
                    if u and u >= 16 and b["peak"] and b["peak"] > b["before"]:
                        grown = (b["peak"] - (f["at_load"] or 0)) / (f["slope"] * b["units"])
                        ratios.setdefault(short[model], {}).setdefault(u, []).append(grown)
    med = lambda fs, k: st.median(f[k] for f in fs if f[k])
    return [f"model={m} pu={med(fs, 'slope'):.4g} ratio={ladder(ratios[m], MIN_SHARED)} "
            f"base={med(fs, 'base'):.0f} rsspu={med(fs, 'rss'):.3g}"
            for m, fs in sorted(figs.items()) if ratios.get(m)]


def held_levels(tapped, block_ms=30000, least_ms=180000):
    """Within a run, the host's level: the median deviation of 30 s block means from the mean of
    each stretch of three minutes or more at one size, and how long a level holds (s)."""
    devs, holds = [], []
    for runs in tapped:
        for ms in runs.values():
            for ws in ms.values():
                stretches, cur = [], None
                for u, d in [b for w in ws for b in w["b"]][SKIP_BATCHES:]:
                    if bucket(u) != cur:
                        stretches.append([])
                        cur = bucket(u)
                    stretches[-1].append((u, d))
                for stretch in (x for x in stretches if sum(d for _, d in x) >= least_ms):
                    mean, blocks, acc, t = st.mean(d / u for u, d in stretch), [], [], 0
                    for u, d in stretch:
                        acc.append(d / u)
                        t += d
                        if t >= block_ms:
                            blocks.append(st.mean(acc) / mean)
                            acc, t = [], 0
                    if len(blocks) >= 4:
                        devs += [abs(x - 1) for x in blocks]
                        flips = sum((a > 1) != (b > 1) for a, b in zip(blocks, blocks[1:]))
                        holds.append(block_ms / 1000 * len(blocks) / max(flips, 1))
    return f"held amp={st.median(devs):.3f} hold={st.median(holds):.0f}"


def real_jobs(archive, build, tapped, trace, tags):
    """One line per real job of `build`: the model, items, profile, which start of its
    calibration root it was, the job's items/s, working size at the end, time-weighted pool
    above the pool at load (MiB), pool releases, windows, windows after which the queue ran
    short, windows at a size other than the working size, and how much faster its batches ran
    than each trace (`tag=factor`)."""
    short = {model: name for name, model in CUDA_MODELS.items()}
    runs, lines, texts = [], [], summaries(archive)
    for run_dir in sorted(glob.glob(os.path.join(archive, "runs", "*"))):
        described = os.path.join(run_dir, "run.txt")
        info = open(described).read().split() if os.path.isfile(described) else []
        run = os.path.basename(run_dir)
        if len(info) < 6 or info[0] != build or info[1] not in short or (run, info[1]) not in texts:
            continue
        text = texts[(run, info[1])]
        job = re.search(r"^items (\d+) .*END-TO-END ([\d.]+) items/s\s+windows (\d+)", text, re.M)
        working = re.search(r"^working_units after each window: .*?(\d+)(?: x\d+)?$", text, re.M)
        trial = re.search(r"other than the working size \(trial windows\): (\d+)", text)
        f = summary_figures(text)
        if job and working and f["mean"]:
            root = info[5].removeprefix("root=")
            first = root == os.path.join(os.path.dirname(root.rstrip("/")), "root") and \
                os.path.basename(os.path.dirname(root)) == run
            ws = tapped.get(run, {}).get(info[1], [])
            batches = [(u, d) for w in ws for u, d in w["b"]][SKIP_BATCHES:]
            host = " ".join(f"{tag}={speed(batches, trace(tag, short[info[1]])):.3f}"
                            for tag in tags)
            runs.append((root, 0 if first else 1, short[info[1]], info[3], job.groups(),
                         working[1], f["mean"] - (f["at_load"] or 0), int(f["releases"] or 0),
                         waits(ws), trial[1] if trial else -1, host))
    roots = sorted({r[0] for r in runs})
    for root, start, model, prof, (items, ips, n), working, pool, rel, wt, tw, host in sorted(runs):
        lines.append(f"model={model} dev=gpu items={items} prof={prof} pair={roots.index(root)} "
                     f"start={start} ips={ips} W={working} pool={pool:.0f} releases={rel} "
                     f"windows={n} waits={wt} trialw={tw} {host}")
    return lines


def mac_jobs(archive, spec, trace, pair0):
    """The Mac's real jobs, as `real_jobs` (pool above base); a group's later legs started from
    the first one's calibration store. Also each model's memory, as `cuda_memory`, with the
    pool over allocated per size from the health recorder's pool at each budget."""
    text = open(os.path.join(archive, spec["summaries"])).read()
    lines, memory, pair = [], [], pair0
    for model, groups in spec["jobs"].items():
        mem, ratios = [], {}
        for legs in groups:
            for start, leg in enumerate(legs):
                part = re.search(rf"^== {re.escape(leg)}: (.*?)(?=^== |\Z)", text, re.M | re.S)[1]
                g = lambda pattern: re.search(pattern, part)[1]
                base = float(g(r"base (\d+) included"))
                s2 = os.path.join(archive, "results", leg, "S2")
                cal = open(os.path.join(s2, "calibration.after.toml")).read()
                slope = float(re.search(r"slope_mb_per_unit = ([\d.]+)", cal)[1])
                units = float(g(r"final max_units_measured (\d+)"))
                mem.append((slope, base))
                rec = subprocess.run(["zstd", "-dc", os.path.join(s2, "healthrec.jsonl.zst")],
                                     capture_output=True, text=True).stdout
                for sample in map(json.loads, rec.splitlines()):
                    for w in (sample.get("health") or {}).get("workers") or []:
                        if w.get("reserved_mb") is not None and w.get("unit_budget"):
                            u = w["unit_budget"]
                            ratios.setdefault(u, []).append((w["reserved_mb"] - base) / (slope * u))
                ws = windows(read_log(s2) or []).get(CUDA_MODELS[model], [])
                batches = [(w["budget"], (w["e"] - w["t"]) * 1000 * w["budget"] / w["req"])
                           for w in ws[3:] if w["req"] >= w["budget"] and w["outcome"] == '"clean"']
                items, ips, working = g(r"predicts (\d+)"), g(r"items/s over the job ([\d.]+)"), \
                    g(r"knee_units=(\d+)")
                pool, n = float(g(r"time-weighted mean (\d+)")) - base, g(r"grants (\d+)")
                lines.append(f"model=m{model} dev=mac items={items} prof=np pair={pair} "
                             f"start={start} ips={ips} W={working} pool={pool:.0f} releases=-1 "
                             f"windows={n} waits={waits(ws)} trialw=-1 "
                             f"mac2={speed(batches, trace(model)):.3f}")
            pair += 1
        memory.append(f"model=m{model} pu={st.median(m[0] for m in mem):.4g} "
                      f"ratio={ladder(ratios, 3 * MIN_SHARED)} "
                      f"base={st.median(m[1] for m in mem):.0f} rsspu=0")
    return lines, memory


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--archive", default=os.path.expanduser("~/panoptikon-archive"))
    ap.add_argument("--manifest", help="default: <archive>/sizing-manifest.json")
    ap.add_argument("--out", default=os.path.expanduser("~/panoptikon-archive/sizing-traces"))
    a = ap.parse_args()
    man = json.load(open(a.manifest or os.path.join(a.archive, "sizing-manifest.json")))
    os.makedirs(a.out, exist_ok=True)
    out = lambda name: os.path.join(a.out, name)
    legs = lambda root, pattern: sorted(
        glob.glob(os.path.join(a.archive, root, "results", pattern, "S2/")))
    tapped, raw, fixed = {}, {}, man["fixed"]
    for tag, archive in man["cuda_days"].items():
        tapped[archive] = tapped_runs(os.path.join(a.archive, archive))
        runs = {r: ms for r, ms in tapped[archive].items()
                if f"{archive}/{r}" not in man["excluded"]}
        for short, model in CUDA_MODELS.items():
            raw[f"{tag}-{short}"] = tap_trace(runs, model, out(f"{tag}-{short}.txt"))
        for short in fixed["models"] if tag == fixed["day"] else []:  # one size per run
            tap_trace(runs, CUDA_MODELS[short], out(f"{tag}-{short}-fixed.txt"),
                      only=lambda r: r.startswith(fixed["prefix"]))
    mac = man["mac"]
    for short, patterns in mac["traces"].items():
        window_trace([leg for p in patterns for leg in legs(mac["archive"], p)],
                     CUDA_MODELS[short], out(f"mac2-{short}.txt"), mac["capped_leg"])
    for name, src, size in man["flat"]:
        series = raw.get(src, {}).get(size)
        if series is None:  # a window-level trace: its pooled series
            lines = open(out(f"{src}.txt")).read().split("\n")
            i = next(i for i, l in enumerate(lines) if l.startswith(f"size {size} "))
            series = [float(x) for x in lines[i + 1].split()]
        flat(series, out(f"flat-{name}.txt"))
    jobs = real_jobs(os.path.join(a.archive, man["jobs"]["archive"]), man["jobs"]["build"],
                     tapped[man["jobs"]["archive"]], lambda tag, model: out(f"{tag}-{model}.txt"),
                     list(man["cuda_days"]))
    mjobs, mmem = mac_jobs(os.path.join(a.archive, mac["archive"]), mac,
                           lambda model: out(f"mac2-{model}.txt"),
                           1 + max(int(kv(j)["pair"]) for j in jobs))
    with open(out("jobs.txt"), "w") as f:
        f.write("\n".join(jobs + mjobs) + "\n")
    with open(out("models.txt"), "w") as f:
        archives = [os.path.join(a.archive, x) for x in man["cuda_days"].values()]
        f.write("\n".join(cuda_memory(archives) + mmem + [held_levels(tapped.values())]) + "\n")
    cpu = man["cpu"]
    cpu_legs = [leg for p in cpu["legs"] for leg in legs(cpu["archive"], p)]
    flat(cpu_series(cpu_legs, CUDA_MODELS[cpu["model"]]), out("flat-cpu.txt"))


if __name__ == "__main__":
    main()
