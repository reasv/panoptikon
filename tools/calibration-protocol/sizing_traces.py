#!/usr/bin/env python3
"""Build the batch-size simulator's trace files from archived real runs.

A trace lists, per batch size, the time-ordered ms per unit of every recorded
batch (from the worker's tap), or of every full window where a run has no tap
(Mac). `flat-*` traces replay one real series at every size: a model whose rate
does not depend on the batch size, with the host's real noise. Traces hold
private data and stay outside the repository.
"""
import argparse, glob, json, math, os, re, statistics as st, subprocess
from datetime import datetime

CUDA_MODELS = {"wdvit": "tags/wd-vit-tagger-v3", "vith": "clip/ViT-H-14-378-quickgelu_dfn5b",
               "mclip": "clip/apple_MobileCLIP-S1", "doctr": "doctr/db_resnet50_crnn_mobilenet_v3_small",
               "flor": "florence2/msft_large-more-detailed", "minilm": "textembed/all-MiniLM-L6-v2"}
# Units per item where the cost unit is not the item (MiniLM: tokens).
UNIT = {"minilm": 196}
# Runs left out of the pooled CUDA traces: user-capped runs without a queue
# that kept the GPU fed, and two ViT-H runs on a contended host.
EXCLUDED = {f"gain-gpu-measure-2026-10-02/{run}" for run in
            [f"fix-wdvit-{n}" for n in (1, 2, 4, 8, 16, 32, 64)] + ["vith12k-br-np", "vith12k-br-np-2"]}
SKIP_BATCHES = 6  # warm-up batches dropped at the start of each run

kv = lambda line: dict(re.findall(r'(\w+)=("[^"]*"|\S+)', line))
ts = lambda line: datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp()


def read_log(run_dir):
    path = os.path.join(run_dir, "panoptikon.log")
    if os.path.isfile(path):
        return open(path, errors="replace").read().splitlines()
    zst = subprocess.run(["zstd", "-dc", path + ".zst"], capture_output=True, text=True, errors="replace")
    return zst.stdout.splitlines() if zst.returncode == 0 else None


def windows(lines):
    """Per model, the settled windows: grant time, settle time, budget, requests, outcome."""
    models = {}
    for line in lines:
        if "issued a memory grant" in line:
            g = kv(line)
            models.setdefault(g["model"], []).append(
                {"t": ts(line), "e": None, "budget": int(g["unit_budget"]), "req": int(g["window_requests"]), "b": []})
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
    """The power of two `units` is a full batch of, or None."""
    p = 1 << max(0, round(math.log2(max(units, 1))))
    return p if units >= 0.8 * p and units * 0.9 <= p else None


def write(path, first, series, overhead, digits=1):
    with open(path, "w") as f:
        f.write(f"first {first:.1f}\n")
        for size, v in sorted(series.items()):
            f.write(f"size {size} ovh {overhead.get(size, 30.0):.{digits}f} n {len(v)}\n" + " ".join(f"{x:.4f}" for x in v) + "\n")


def tap_trace(runs, model, unit, path, only=None):
    """Pool every run of `model` in time order; `only` picks the runs the batches come from."""
    picked = sorted((ws[0]["t"], run, ws) for run, ms in runs.items() for m, ws in ms.items() if m == model and ws)
    series, gaps, first = {}, {}, []
    if not picked:
        return
    for _, run, ws in picked:
        n = 0
        for w in ws:
            sizes = [bucket(u / unit) for u, _ in w["b"]]
            wall, busy = (w["e"] - w["t"]) * 1000, sum(d for _, d in w["b"])
            if len(sizes) == 3 and sizes[0] and n >= SKIP_BATCHES and wall >= busy and len(set(sizes)) == 1:
                gaps.setdefault(sizes[0], []).append(wall - busy)
            n += len(w["b"])
    for _, run, ws in picked:
        if only and not only(run):
            continue
        n = 0
        for w in ws:
            if w["b"] and n == 0:
                first.append(w["b"][0][1])
            for u, d in w["b"]:
                n += 1
                if n > SKIP_BATCHES and bucket(u / unit):
                    series.setdefault(bucket(u / unit), []).append(d / (u / unit))
    write(path, st.median(first), series, {s: st.median(g) for s, g in gaps.items()})


def window_trace(leg_dirs, model, path):
    """Each full clean window after the third adds three batches at its ms per item; a leg named
    `F-<n>` or `Fs-<n>` ran under a user cap of n."""
    series, first = {}, []
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
    write(path, st.median(first), series, {s: 1.0 for s in series})


def flat(src, size, path, series=None):
    """Replay one recorded series at every size from 1 to 4096 units, 1 % of a window outside the batches."""
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
        out += [(w["e"] - w["t"]) * 1000 / w["req"] for n, w in enumerate(ws, 1) if n > 3 and w["outcome"] == '"clean"']
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--archive", default=os.path.expanduser("~/panoptikon-archive"))
    ap.add_argument("--out", default=os.path.expanduser("~/panoptikon-archive/sizing-traces"))
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    out = lambda name: os.path.join(a.out, name)
    for tag, archive in (("cuda", "gain-gpu-measure-2026-10-02"), ("cuda2", "gain-gpu-measure2-2026-10-03")):
        runs = {r: ms for r, ms in tapped_runs(os.path.join(a.archive, archive)).items()
                if f"{archive}/{r}" not in EXCLUDED}
        for short, model in CUDA_MODELS.items():
            tap_trace(runs, model, UNIT.get(short, 1), out(f"{tag}-{short}.txt"))
            if tag == "cuda" and short in ("wdvit", "vith"):  # the user-capped runs: one size each
                tap_trace(runs, model, 1, out(f"cuda-{short}-fixed.txt"), only=lambda r: r.startswith("fix-"))
    legs = lambda root, pattern: sorted(glob.glob(os.path.join(a.archive, root, "results", pattern, "S2/")))
    mac2 = "mac-gain-measure-2026-10-03"
    window_trace(legs(mac2, "V-*") + legs(mac2, "F*-*"), CUDA_MODELS["vith"], out("mac2-vith.txt"))
    window_trace(legs(mac2, "M-*"), CUDA_MODELS["mclip"], out("mac2-mclip.txt"))
    for name, src, size in (("wd", "cuda-wdvit", 64), ("vh", "cuda-vith", 128), ("fl", "cuda-flor", 64),
                            ("w8", "cuda-wdvit", 8), ("mac", "mac2-vith", 8)):
        flat(out(f"{src}.txt"), size, out(f"flat-{name}.txt"))
    cpu = cpu_series(legs("mac-regression3-2026-10-02", "X*-*"), CUDA_MODELS["minilm"])  # MiniLM on a Mac's CPU
    flat(None, 0, out("flat-cpu.txt"), cpu)


if __name__ == "__main__":
    main()
