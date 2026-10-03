#!/usr/bin/env python3
"""Build the batch-size simulator's trace files from archived real runs.

A trace lists, per batch size, the time-ordered ms per unit of every recorded
batch (from the worker's tap), or of every full window where a run has no tap
(Mac). `flat-*` traces replay one real series at every size: a model whose rate
does not depend on the batch size, with the host's real noise. `jobs.txt` lists
real jobs of the sizing code under test with what they measured, for the
replay-against-real table. Traces hold private data and stay outside the
repository.
"""
import argparse, glob, json, math, os, re, statistics as st, subprocess
from datetime import datetime

CUDA_MODELS = {"wdvit": "tags/wd-vit-tagger-v3", "vith": "clip/ViT-H-14-378-quickgelu_dfn5b",
               "mclip": "clip/apple_MobileCLIP-S1",
               "doctr": "doctr/db_resnet50_crnn_mobilenet_v3_small",
               "flor": "florence2/msft_large-more-detailed", "minilm": "textembed/all-MiniLM-L6-v2"}
# Runs left out of the pooled CUDA traces: user-capped runs without a queue
# that kept the GPU fed, and two ViT-H runs on a contended host.
EXCLUDED = {f"gain-gpu-measure-2026-10-02/{run}" for run in
            [f"fix-wdvit-{n}" for n in (1, 2, 4, 8, 16, 32, 64)]
            + ["vith12k-br-np", "vith12k-br-np-2"]}
SKIP_BATCHES = 6  # warm-up batches dropped at the start of each run
# The archive with real jobs of the sizing code under test, and that build's tag.
JOBS = ("gain-gpu-measure2-2026-10-03", "br")
MIN_WINDOWS = 3  # windows behind a measured overhead or a window-level size

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


def write(path, first, series, overhead, digits=1, warm=()):
    """`overhead`: window ms outside the batches per size; a size without one is written `-`."""
    with open(path, "w") as f:
        f.write(f"first {first:.1f}\n")
        if warm:
            f.write("warm " + " ".join(f"{x:.1f}" for x in warm) + "\n")
        for size, v in sorted(series.items()):
            ovh = f"{overhead[size]:.{digits}f}" if size in overhead else "-"
            f.write(f"size {size} ovh {ovh} n {len(v)}\n" + " ".join(f"{x:.4f}" for x in v) + "\n")


def tap_trace(runs, model, path, only=None):
    """Pool every run of `model` in time order; `only` picks the runs the batches come from.

    A size's window overhead is the median wall time outside the batches of windows of three
    equal full batches, or failing that of every window whose largest batch is that size."""
    picked = sorted((ws[0]["t"], run, ws) for run, ms in runs.items() for m, ws in ms.items()
                    if m == model and ws)
    series, equal, largest, first, warm = {}, {}, {}, [], []
    if not picked:
        return
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
        for u, d in batches[SKIP_BATCHES:]:
            if bucket(u):
                series.setdefault(bucket(u), []).append(d / u)
    if not series:
        return
    steady = {s: st.median(v) for s, v in series.items()}
    near = lambda u: steady[min(steady, key=lambda s: abs(math.log2(s / max(u, 1))))]
    # What each batch after the first takes on top of its steady time, median over loads.
    extra = [st.median(d - u * near(u) for u, d in (b[i] for b in warm if len(b) > i))
             for i in range(SKIP_BATCHES - 1) if any(len(b) > i for b in warm)]
    overhead = {s: st.median(g) for s, g in largest.items() if len(g) >= MIN_WINDOWS}
    overhead.update({s: st.median(g) for s, g in equal.items() if len(g) >= MIN_WINDOWS})
    write(path, st.median(first), series, {s: v for s, v in overhead.items() if s in series},
          warm=[max(0.0, x) for x in extra])


def window_trace(leg_dirs, model, path):
    """Each full clean window after the third adds three batches at its ms per item; a leg named
    `F-<n>` or `Fs-<n>` ran under a user cap of n."""
    series, counted, first = {}, {}, []
    for leg in leg_dirs:
        cap = re.match(r"Fs?-(\d+)$", os.path.basename(os.path.dirname(leg.rstrip("/"))))
        lines = read_log(leg) or []
        for n, w in enumerate(windows(lines).get(model, []), 1):
            if n == 1:
                first.append((w["e"] - w["t"]) * 1000)
                continue
            size = min(w["budget"], int(cap.group(1))) if cap else w["budget"]
            if w["outcome"] != '"clean"' or n <= 3 or w["req"] < size or not bucket(size):
                continue
            series.setdefault(bucket(size), []).extend([(w["e"] - w["t"]) * 1000 / w["req"]] * 3)
            counted[bucket(size)] = counted.get(bucket(size), 0) + 1
    series = {s: v for s, v in series.items() if counted[s] >= MIN_WINDOWS}
    write(path, st.median(first), series, {s: 1.0 for s in series})


def flat(src, size, path, series=None):
    """Replay one recorded series at every size from 1 to 4096 units, with 1 % of a window
    outside the batches."""
    if series is None:
        lines = open(src).read().split("\n")
        i = next(i for i, l in enumerate(lines) if l.startswith(f"size {size} "))
        series = [float(x) for x in lines[i + 1].split()]
    mean, sizes = sum(series) / len(series), [1 << k for k in range(13)]
    write(path, mean, {s: series for s in sizes}, {s: 0.03 * s * mean for s in sizes}, digits=2)


def cpu_series(leg_dirs, model):
    """ms per request of every clean window after the third on the CPU device."""
    out = []
    for leg in leg_dirs:
        ws = windows([l for l in read_log(leg) or [] if "gpu=CPU" in l]).get(model, [])
        out += [(w["e"] - w["t"]) * 1000 / w["req"] for n, w in enumerate(ws, 1)
                if n > 3 and w["outcome"] == '"clean"']
    return out


def speed(windows_, trace):
    """How much faster a run's batches ran than a trace's mean at the same sizes."""
    lines = open(trace).read().split("\n")
    means = {int(l.split()[1]): st.mean(map(float, lines[i + 1].split()))
             for i, l in enumerate(lines) if l.startswith("size ")}
    batches = [(u, d) for w in windows_ for u, d in w["b"]][SKIP_BATCHES:]
    expected = sum(u * means[bucket(u)] for u, _ in batches if bucket(u) in means)
    return expected / max(sum(d for u, d in batches if bucket(u) in means), 1e-9)


def real_jobs(archive, build, tapped, trace, tags):
    """One line per real job of `build`: the model, items, profile, which start of its
    calibration root it was, the job's items/s, working size at the end, time-weighted pool
    MiB and windows, and how much faster its batches ran than each trace (`tag=factor`)."""
    short = {model: name for name, model in CUDA_MODELS.items()}
    runs, lines = [], []
    for run_dir in sorted(glob.glob(os.path.join(archive, "runs", "*"))):
        described = os.path.join(run_dir, "run.txt")
        info = open(described).read().split() if os.path.isfile(described) else []
        if len(info) < 6 or info[0] != build or info[1] not in short:
            continue
        summary = os.path.join(run_dir, f"summary-{info[1].replace('/', '_')}.txt")
        text = open(summary).read() if os.path.isfile(summary) else ""
        job = re.search(r"^items (\d+) .*END-TO-END ([\d.]+) items/s\s+windows (\d+)", text, re.M)
        working = re.search(r"^working_units after each window: .*?(\d+)(?: x\d+)?$", text, re.M)
        pool = re.search(r"^POOL reserved_mb during the job: time-weighted mean (\d+)", text, re.M)
        if job and working and pool:
            root = info[5].removeprefix("root=")
            first = root == os.path.join(os.path.dirname(root.rstrip("/")), "root") and \
                os.path.basename(os.path.dirname(root)) == os.path.basename(run_dir)
            ws = tapped.get(os.path.basename(run_dir), {}).get(info[1], [])
            host = " ".join(f"{tag}={speed(ws, trace(tag, short[info[1]])):.3f}" for tag in tags)
            runs.append((root, 0 if first else 1, short[info[1]], info[3], job.groups(),
                         working[1], pool[1], host))
    roots = sorted({r[0] for r in runs})
    for root, start, model, prof, (items, ips, windows), working, pool, host in sorted(runs):
        lines.append(f"model={model} items={items} prof={prof} pair={roots.index(root)} "
                     f"start={start} ips={ips} W={working} pool={pool} windows={windows} {host}")
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--archive", default=os.path.expanduser("~/panoptikon-archive"))
    ap.add_argument("--out", default=os.path.expanduser("~/panoptikon-archive/sizing-traces"))
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    out = lambda name: os.path.join(a.out, name)
    tapped = {}
    for tag, archive in (("cuda", "gain-gpu-measure-2026-10-02"),
                         ("cuda2", "gain-gpu-measure2-2026-10-03")):
        tapped[archive] = tapped_runs(os.path.join(a.archive, archive))
        runs = {r: ms for r, ms in tapped[archive].items() if f"{archive}/{r}" not in EXCLUDED}
        for short, model in CUDA_MODELS.items():
            tap_trace(runs, model, out(f"{tag}-{short}.txt"))
            if tag == "cuda" and short in ("wdvit", "vith"):  # the user-capped runs: one size each
                tap_trace(runs, model, out(f"cuda-{short}-fixed.txt"),
                          only=lambda r: r.startswith("fix-"))
    legs = lambda root, pattern: sorted(
        glob.glob(os.path.join(a.archive, root, "results", pattern, "S2/")))
    mac2 = "mac-gain-measure-2026-10-03"
    window_trace(legs(mac2, "V-*") + legs(mac2, "F*-*"), CUDA_MODELS["vith"], out("mac2-vith.txt"))
    window_trace(legs(mac2, "M-*"), CUDA_MODELS["mclip"], out("mac2-mclip.txt"))
    for name, src, size in (("wd", "cuda-wdvit", 64), ("vh", "cuda-vith", 128),
                            ("fl", "cuda-flor", 64), ("w8", "cuda-wdvit", 8),
                            ("mac", "mac2-vith", 8)):
        flat(out(f"{src}.txt"), size, out(f"flat-{name}.txt"))
    with open(out("jobs.txt"), "w") as f:
        jobs = real_jobs(os.path.join(a.archive, JOBS[0]), JOBS[1], tapped[JOBS[0]],
                         lambda tag, model: out(f"{tag}-{model}.txt"), ("cuda", "cuda2"))
        f.write("\n".join(jobs) + "\n")
    # MiniLM on a Mac's CPU device.
    cpu = cpu_series(legs("mac-regression3-2026-10-02", "X*-*"), CUDA_MODELS["minilm"])
    flat(None, 0, out("flat-cpu.txt"), cpu)


if __name__ == "__main__":
    main()
