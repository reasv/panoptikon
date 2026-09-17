# N1 on a quiet CPU host: is the throughput brake lost, or was the deploy run contaminated?

Repo `6c9e6857` (the final tip). Binary and CPU venv **reused** from the
final-deploy run — `panoptikon-wt/final-deploy/target/release/panoptikon` and
`panoptikon-wt/final-deploy/pyenv/.venv` — so no `cargo build` ran on the box
during any leg. Config `server-N1.toml` is final-deploy's `server-CPU.toml`
with the paths repointed, `[inference_local.python_env] accelerator = "cpu"`
added, and the listeners moved to 6912/6913/6909 (`legs.py --port` at this tip
only moves the URL the driver talks to, not the listeners, so the ports were
edited in the TOML). `CUDA_VISIBLE_DEVICES=""` for every gateway,
`TMPDIR=$HOME/tmp-n1`, no containers, `~/docker` untouched.

Each leg: `legs.py --scenario S2 --model tags/wd-vit-tagger-v3` over the
2 000-image `ramp` corpus, fresh store, `RUST_LOG=info,panoptikon::inferio=trace`
and `INFERIO_WORKER_LOG_LEVEL=DEBUG`.

**Quiet gate.** Before each leg, `quiet.sh` polled every 60 s (max 20 min) for
load < 4 and for no `cargo|rustc|target/release/panoptikon|inferio_worker|pytest`
other than its own. Waits: **n1-a 0 s** (one earlier attempt waited 60 s for a
`cargo build` that was running *over ssh on another host* — the pattern matched
the ssh client's command line, and the check was narrowed to exclude it),
**n1-b 0 s**, **n1-c 0 s**, **n1-ctl 0 s**. `vramrec.jsonl` confirms it from the
other side: in all four legs the only `inferio_worker` process on the box was
this run's own, against the deploy run's **three foreign worker pids
(8 905 / 4 452 / 1 308 MB RSS)**.

---

## The three quiet runs

| | **n1-a** | **n1-b** | **n1-c** |
|---|---|---|---|
| inference time (2 000 items) | **339.6 s** | **357.3 s** | **335.4 s** |
| errors | 0 | 0 | 0 |
| grant→settle windows | 16 | 70 | 46 |
| knee ever fitted | **never** | 7 (13 obs), refit 7 ×3, refit 15 | 15 (13 obs), refit 15 ×2 |
| widenings | — | 7→15 ×4 | 15→31 ×3 |
| withdrawals | — | 1 (from 15) | 0 |
| `declining to read … outside this ledger was moving` | **16** | **0** | **0** |
| held rung | none — free to 256 | 7, then 15 | 15 |
| store `knee_units` | **absent** | **15** | **15** |
| store `slope_mb_per_unit` | 34.635 | 36.661 | 34.000 |
| store `base_mb` | 478 | 478 | 478 |
| store `max_units_measured` | **256** | 32 | 32 |
| peak own-worker RSS | 8 387 MB (rung 256) | 1 904 MB | 1 890 MB |
| `free -m` available, before → after | 91 305 → 91 135 | 91 157 → 91 243 | 91 240 → 91 170 |

`free -m` moves by at most 170 MB across a leg, in both directions — page
cache, nothing else on the box.

### Per rung

**n1-a** — no knee, ramp runs away to 256

| rung | n | items/s | median grant MB | peak worker RSS MB |
|---|---|---|---|---|
| 1 | 1 | 1.980 | 88 754 | 751 |
| 2 | 1 | 0.936 | 88 684 | 814 |
| 4 | 1 | 1.792 | 88 614 | 873 |
| 8 | 1 | 1.980 | 464 | 990 |
| 16 | 2 | 1.772 | 724 | 1 329 |
| 32 | 2 | 1.482 | 1 200 | 1 901 |
| 64 | 3 | 3.402 | 2 310 | 2 992 |
| 128 | 3 | 3.795 | 4 571 | 5 125 |
| 133 | 1 | 6.522 | 4 642 | 4 797 |
| 256 | 1 | 4.505 | 9 142 | **8 387** |

**n1-b** — knee 7, widened four times, refitted three times, withdrawn once, refitted at 15

| rung | n | items/s | median grant MB | peak worker RSS MB |
|---|---|---|---|---|
| 1 | 1 | 1.848 | 88 678 | 753 |
| 2 | 1 | 1.006 | 88 601 | 824 |
| 4 | 1 | 1.795 | 88 573 | 866 |
| 7 | **45** | 2.010 | 259 | 1 029 |
| 8 | 1 | 2.077 | 370 | 990 |
| 15 | 16 | 2.068 | 549 | 1 301 |
| 16 | 3 | 2.112 | 602 | 1 334 |
| 32 | 2 | 1.520 | 1 153 | **1 904** |

**n1-c** — knee 15, widened three times, refitted each time, never withdrawn

| rung | n | items/s | median grant MB | peak worker RSS MB |
|---|---|---|---|---|
| 1 | 1 | 2.232 | 88 726 | 753 |
| 2 | 1 | 1.078 | 88 510 | 815 |
| 4 | 2 | 3.362 | 44 299 | 914 |
| 8 | 1 | 1.821 | 467 | 1 037 |
| 15 | **34** | 2.216 | 519 | 1 323 |
| 16 | 3 | 1.937 | 595 | 1 259 |
| 31 | 2 | 1.648 | 1 072 | 1 867 |
| 32 | 2 | 2.904 | 1 142 | **1 890** |

(`items/s` is the window's `unit_budget` over its grant→settle wall time; the
same computation reproduces the final-deploy report's wd-vit table to the
digit, `rungs.py` against `logs/F-tags-ramp.log`.)

### n1-a's refusal, verbatim

The gate fires on the **first** fit attempt and then on every one after it,
**with the same number every time** — 16 lines, all identical but for the
timestamp:

```
03:06:59.264955Z DEBUG ledger: declining to read this model's throughput curve: the
  observations in one batch-size bucket disagree with each other by more than the knee's
  own decision band, so something outside this ledger was moving throughput while they
  were taken bucket=1 observations=3 dispersion=0.2919601485536646 threshold=0.2
…
03:12:36.220110Z DEBUG ledger: declining to read … bucket=1 observations=3
  dispersion=0.2919601485536646 threshold=0.2
```

`bucket=1` is batch sizes 2–3, and the three observations are the three
2-image batches of the **second** window:

```
03:06:57.160788Z  Running inference on 2 images
03:06:58.103975Z  Running inference on 2 images   (+0.943 s → 2.12 items/s)
03:06:58.770672Z  Running inference on 2 images   (+0.667 s → 3.00 items/s)
                  window settles 03:06:59.26      (+0.49 s  → ~4.08 items/s)
```

median 3.00, MAD 0.88, relative MAD **0.292**. That is ONNX Runtime still
warming its thread pool and arena. Only the **first** settled window is tagged
`warmup` (`ledger.rs:5537`, `entry.settled_windows == 0`), and on this model the
first window is the single 1-image batch — so the warm-up *tail* lands in the
ring as honest evidence. `KNEE_RING` is 128 and the whole job produces 39 quiet
samples, so those three never age out: one bucket poisons every fit for the
whole job, and `ramp_still_gains` reads the same refusal as "no answer → keep
doubling" (`the throughput ramp is free to grow again`). The ramp went 16 → 32
→ 64 → 128 → 133 → 256, and the store ends with **no `knee_units`**.

n1-b and n1-c ran the identical binary, config and corpus on the identical
quiet box and logged **zero** such lines. The difference is purely which way
the first three 2-image batches happened to scatter.

### What the quiet CPU curve actually looks like

Batch-level relative MAD per log2 bucket, reconstructed from the worker's own
batch lines (`bucketmad.py`; the first batch of each rung is dropped, so these
*under*-state the noise the ring sees):

| bucket (sizes) | n1-a | n1-b | n1-c | n1-ctl |
|---|---|---|---|---|
| 1 (2–3) | 0.172 | 0.031 | 0.029 | 0.057 |
| 2 (4–7) | 0.018 | 0.029 | 0.006 | 0.027 |
| 3 (8–15) | 0.015 | 0.104 | 0.032 | 0.163 |
| 4 (16–31) | 0.128 | **0.196** | **0.185** | 0.150 |
| 5 (32–63) | 0.161 | 0.065 | 0.128 | — |
| 6 (64–127) | 0.046 | — | — | — |
| 7 (128–255) | 0.040 | — | — | — |

A quiet CPU sits at 0.13–0.20 in the buckets the ramp lives in, against a
threshold of 0.20. The design derives that 0.20 from **GPU** series — "quiet
GPUs 0.003 (`S2-wdvit-loadgen`) and 0.052 (`S2-minilm`)"
(`docs/batch-calibration-design.md`, R1 (c)) — two orders of magnitude below
what this device's quiet noise floor is.

---

## The control: deliberate external RAM motion

`ramhog.py` alongside the leg: allocate 6 GB, touch every page, free it, sleep
5 s, repeat, for the whole run. `vramrec` sees it — `mem_available_mb` floor
**82 832 MB**, against 88 345/88 440 in the two quiet legs that fitted knees.

| | **n1-ctl** |
|---|---|
| inference time | 361.1 s, 0 errors |
| windows | 76 |
| `declining to read …` | **2**, both `bucket=3 observations=9 dispersion=0.2226692707231629 threshold=0.2` |
| knee | fitted 7, widened 7→15 ×4, refitted 7 ×3, withdrawn once, refitted 7 |
| held rung | 7 (52 windows), then 15 (16) |
| store | `knee_units = 7`, `slope 36.0`, `base_mb 478`, `max_units_measured 16` |
| peak own-worker RSS | 1 335 MB |
| `free -m` available, before → after | 91 166 → 90 992 |

The two refusals land in the window straight after a widening, on bucket 3 —
a bucket that was **quiet in both n1-b and n1-c**:

```
03:43:14.150273Z  INFO  … widening the cap by one batch-size step …
03:43:21.265797Z DEBUG  declining to read … bucket=3 observations=9 dispersion=0.2227 threshold=0.2
03:43:21.265866Z DEBUG  declining to read … bucket=3 observations=9 dispersion=0.2227 threshold=0.2
03:43:31.659717Z DEBUG  fitted a throughput knee … knee_units=7 previous=Some(15) observations=91
```

So the gate does fire on genuine external motion, and it delays — here by
10 s, one window — the refit that puts the brake back. It did **not** cost the
knee: the control ends with `knee_units = 7` in the store and `max_units_measured
= 16`, i.e. the tightest result of all four legs.

---

## The gate, exactly

`panoptikon/src/inferio/ledger.rs`, `quiet_medians` (line 7955):

```rust
let dispersion = relative_mad(&mut only_rates)?;
if dispersion > KNEE_MAX_BUCKET_DISPERSION { … return None; }
```

* **The quantity** is `relative_mad` = `median(|rate − median(rates)|) / median(rates)`
  over the **units/sec of the batches in one log2 batch-size bucket**
  (`ledger.rs:8377`). A rate enters the ring only if the batch was warm,
  unclamped, ≥ `FULL_BATCH_RATIO` (0.8) of its granted budget, and taken under
  **sole occupancy** (`occupants == 0`); samples from the replica's first
  settled window are tagged `warmup` and dropped. The ring is `KNEE_RING = 128`
  deep and ages only by eviction.
* **The threshold** is `KNEE_MAX_BUCKET_DISPERSION = 0.20`, a single global
  constant — no per-device, per-arch or per-backend variant exists.
* **The scope** is *any* participating bucket (≥ `MIN_KNEE_BUCKET_SAMPLES` = 2
  observations). One bucket over the band refuses the **whole** fit, deliberately:
  "dropping a noisy one would silently move the answer to its neighbour".
* **"Moving" means nothing about RAM.** On the CPU device the condition never
  reads `mem_available`, the cgroup limit, `external_mb` or any memory figure.
  It is purely *the batch rates inside one bucket disagreeing with each other*,
  and on CPU the things that make them disagree are the runtime warming up,
  thread-pool scheduling, and core contention.
* **Two callers, opposite consequences.** `fit_knee` propagates the `None` and
  fits nothing (so no knee is installed, and `knee_best` is not updated either).
  `ramp_still_gains` turns it into `return true` — "a ring too noisy to
  summarize does not stall the ramp" — i.e. noise **releases** the brake and
  buys another doubling. Every one of n1-a's 16 refusals is one of those.

**Withdrawal itself is not this gate.** `note_knee_window_locked`
(`ledger.rs:5243`) widens the knee `k → 2k+1` after `KNEE_EXPIRY_CLEAN_WINDOWS`
(= 12) clean windows run at the knee with `RATCHET_FACTOR`× headroom, and
withdraws it outright once the widened value reaches `uncapped_units`. That is
by design — "a knee is a brake, not a ceiling". The gate's role in N1 is that
it can refuse the **refit** that would put the knee back after a widening
(rule 5 already requires fresh evidence above the widening), so widenings
accumulate unanswered until the cap can no longer bind.

---

## Verdict

**Both halves are true, and the deploy N1 was mostly the first one.** The
withdrawal machinery is correct and self-healing on a quiet CPU host — n1-b
widened the knee four times and refitted it three, n1-c widened three times and
refitted every time, and the one withdrawal n1-b took was answered by a fresh
knee at 15 fifty seconds later; both stores end with `knee_units = 15` and
`max_units_measured = 32`, and the control, run with 6 GB of external RAM
churning every five seconds, ends tighter still at `knee_units = 7`. So
*withdrawal under motion* is not the defect, and the gate does fire on purpose
when something really is moving (the control's two `bucket=3` refusals, on a
bucket both quiet legs read cleanly). But the brake **is** lost too easily on
CPU, by a different route than the deploy report attributed it to: in n1-a, one
in three identical quiet runs, the three 2-image batches of the second window
scattered by a relative MAD of 0.292 — pure ONNX Runtime warm-up tail, since
only the *first* settled window is tagged `warmup` and this model's first window
is a single 1-image batch — and because `KNEE_RING` is 128 while the whole job
produces 39 quiet samples, that one bucket refused **every** fit for the entire
job, the ramp read the refusal as "keep doubling", and the store ended with no
`knee_units` at all and `max_units_measured = 256` (8 387 MB peak RSS against
1 890 MB when the knee held). The 0.20 band was derived from GPU series at
0.003 and 0.052; this CPU device's quiet buckets sit at 0.13–0.20, so the
threshold has no headroom here at all. Two independent fixes suggest
themselves and neither needs the band retuned blindly: tag more than the first
window as warm-up (or drop a bucket whose samples all predate the model's first
*n* windows), and give `KNEE_MAX_BUCKET_DISPERSION` a per-device value the way
the registry holds every other device-specific curve — the CPU device's own
quiet floor is measurable and is not the GPU's.

---

## Files

`legs/{n1-a,n1-b,n1-c,n1-ctl}/` — `legs.json`, `calibration.after.toml`,
`jobs.json`, `health-t0.json`, `health-end.json`, `failures.json`, `driver.log`
(with the `free -m` and `uptime` pairs). `logs/*-S2.log` — the full gateway
logs, ANSI stripped. `server-N1.toml` / `env.N1` — the exact configuration.
`rungs.py`, `n1analyze.py`, `bucketmad.py`, `quiet.sh`, `ramhog.py` — the
tooling, including the hog the control ran against.
