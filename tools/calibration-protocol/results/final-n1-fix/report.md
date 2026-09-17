# Verification of `fix/cpu-knee-gate`: the knee reads a CPU device's own noise floor and warm-up tail

Verifier run. `ca228f26` cherry-picked onto the branch tip `e0658a10` as
**`04c97774`** on `verify/cpu-knee-gate` (worktree
`panoptikon-wt/verify-kneegate`). The cherry-pick was **clean** — one
auto-merge in `panoptikon/src/inferio/ledger.rs`, no conflicts; `e0658a10`'s
refusal follow-ups in `note_floor_oom_locked`/`UnrunnableReplica` do not touch
anything the knee commit touches.

Host: this box, `CUDA_VISIBLE_DEVICES=""`, `TMPDIR=$HOME/tmp-verify-kneegate`,
release binary built at the commit *before* any measurement
(`target/release/panoptikon`, 04:57:40Z) and not rebuilt afterwards. CPU venv
`panoptikon-wt/final-deploy/pyenv/.venv` (torch 2.7.1+cpu,
`torch.cuda.is_available() == False`, onnxruntime 1.21.0). Corpus, config and
harness reused from `results/final-n1`.

---

## 1. Static checks

| check | result |
|---|---|
| full suite (`cargo test -p panoptikon`) | **1755 passed, 0 failed, 10 ignored** (574.8 s) |
| ledger tests (`inferio::ledger`) | **328 passed, 0 failed** |
| `cargo fmt -p panoptikon --check` | clean |
| `cargo fmt --all --check` | one diff in `panoptikon-desktop/src-tauri/src/lib.rs` — **pre-existing on `e0658a10`**, file untouched by this commit |
| `cargo clippy -p panoptikon --all-targets -- -D warnings` | 6 `chunks_exact_to_as_chunks` findings in `db/vector_quants.rs`, `jobs/extraction/input_handlers/audio.rs`, `media_tools/animation.rs`, `media_tools/outro.rs` — **pre-existing on `e0658a10`**, none in a file this commit touches |

(The implementer quoted 327 ledger / 1754 full; both counts are one higher
here because `e0658a10` landed after that measurement.)

### Each of the four rules is load-bearing

Reverted one at a time, running the four tests the commit adds
(`reversion-checks.out` has the raw output):

| revert | which test fails |
|---|---|
| R1 `warmup_tail: … ran_batches <= KNEE_WARMUP_BATCHES` → `false` | `a_first_window_of_one_batch_does_not_exhaust_the_warm_up` |
| R2 `cpu::DEFAULT_KNEE_MAX_BUCKET_DISPERSION` 0.35 → 0.20 | `the_bucket_variance_band_is_the_devices_own`, `a_refused_fit_never_tells_the_ramp_it_still_gains` |
| R3 `with_shipped_gpu_defaults` stops shipping the CPU band | `the_cpu_device_ships_its_own_band_and_a_user_overrides_it` |
| R4 `ramp_still_gains` returns `true` again on a refused fit | `a_refused_fit_never_tells_the_ramp_it_still_gains` |

No revert leaves all four green, and no test fails for a rule it does not
name (R2 also fails the refusal test because that test uses the CPU constant
as its "band that can read this ring" — a legitimate coupling).

### Config authoring, against CLAUDE.md

`knee_max_bucket_dispersion` is a **tunable default** and is shipped as one:
`#[serde(default)] Option<f64>` on both `VramConfig` and `VramOverride`, a
**commented example only** (`# knee_max_bucket_dispersion = 0.20`) in all five
`config/server/*.toml`, never a live line — the shipped-TOML test asserts both
halves (absent → `None`, uncommented → `Some(0.20)`) for every file.
Validation rejects non-finite, `<= 0` and `> 1` at the section and per-GPU
levels. `VramConfig::for_gpu` extends the existing override/inherit rule to the
third field, case-folded as before. Nothing is retired or renamed, so no
migration is owed; per-DB `config.toml` is untouched.

---

## 2. Adversarial questions

Probes in `adversarial-probes.rs`, raw output in `adversarial-probes.out`.
They were inserted into `mod tests`, run, and removed; they are not part of
the commit.

### (a) `warmup_tail` counted by batches RUN

`ran_batches` is per `WorkerEntry`, starts at 0 at registration, is never
persisted, and is incremented **after** the OOM/collapse `continue` — so a
batch that failed does not spend warm-up. `warmup_tail` is
`!warmup_window && ran_batches <= KNEE_WARMUP_BATCHES` (3), i.e. the allowance
is **three batches in total, the first window's included**.

* **A GPU replica whose first three batches are all real and fast loses
  nothing** — but only because the first window itself already spends the
  allowance. Constructed with a first window at depth (3 batches) and a second
  window carrying the *only* observations of its bucket: knee **15** (P1). The
  same series with a one-batch first window drops that window entirely and
  knees at **31** (P2). So the rule is not free: on a thin-first-window
  replica whose second window is honest it discards real evidence and moves
  the knee up one log2 bucket.
* **A replica whose first three are all warm-up-slow does *not* get its tail
  excluded.** A CPU replica with a 3-batch first window and the N1 tail
  (relative MAD 0.293) still refuses every fit under the accelerator band
  (P4 → `None`); the implementer's own control test asserts exactly this. It is
  covered on the CPU device only by the wider band (P5 → knee 15).
* A further residual: a thin first window followed by a **4**-batch warm-up
  window leaves two tail samples in the ring, and the surviving pair can be
  *noisier* than the whole tail was — 0.166 for the four, 0.279 for the two
  that survive — so the fit is refused at 0.20 (P6) where the unmarked tail
  would have passed. 0.35 covers it (P7).

Net: on the CPU device the two halves overlap and every shape tested is
covered. On an accelerator the warm-up-tail rule only helps a replica whose
first window is thinner than three batches.

### (b) Is 0.35 wide enough, and is it too wide?

* **Wide enough.** A plateau whose per-bucket scatter is 0.30 (samples
  `0.7m, m, 1.3m`) is refused at 0.20 and read at 0.35, knee 15 (P8).
* **Not too wide.** The band decides only *whether* the medians may be read,
  never *what* they say; the plateau test is still `KNEE_RATIO` on the
  medians. A genuinely rising curve with the same 0.30 scatter fits no knee and
  leaves the ramp gaining, at 2×/doubling (P9), 1.5×/doubling (Q4) and
  1.15×/doubling (P10) — the last is only 15 % of gain and is still read as
  rising, not as noise.
* The strongest form of the worry — noise dragging a rising bucket's own
  median down onto its neighbour's — is not a band property. `relative_mad` is
  median-based, so for the median to be displaced a majority of the bucket's
  samples must sit at the wrong value, and then the dispersion is *small*: the
  displaced-median ring I built reads 0.0 and fits the same fake knee at 0.20
  and at 0.35 (Q3). Widening 0.20 → 0.35 cannot manufacture a plateau that
  0.20 would have caught.

### (c) Does a refused fit hold the ramp permanently?

No; the hold is bounded by ring turnover, and how long depends on where the
noise is.

* Noise **at the frontier**: the ramp holds, and the next clean samples land
  in the same bucket and collapse its MAD — free again after **2 windows**
  (Q1, Q2).
* Noise in a bucket **below** the frontier, which the ramp has left behind and
  no longer runs: it persists until eviction from the 128-deep ring —
  **40 windows** of 3 clean batches each (P11, P12).

During the hold the ramp parks at the rung it reached; it does not shrink, and
`fit_knee` refuses on the same ring, so nothing is installed from the noisy
evidence either. Against the old behaviour (refusal → "free to grow"), this is
the conservative direction.

One subtlety the wording "no `free to grow again` after a `declining to read`"
does not capture: `quiet_medians` is called by both `fit_knee` (with the
warm-up tail dropped) and `ramp_still_gains` (with it kept), over **different
sample sets**. A single `declining to read` line is one caller refusing; only a
duplicated pair at the same instant is both. In the forced-refusal leg below,
96 instants emitted one line and 9 emitted two, and the two `free to grow
again` lines both follow single (fit-side) refusals — the ramp's own read
succeeded. That is correct, not a leak.

### (d) Does the per-device band reach the CPU device on a mixed host?

Yes, and the GPU rows keep 0.20. `with_cpu_device` appends the CPU device to
every host's inventory, `VramLedger::new` runs `with_shipped_gpu_defaults` over
that inventory and stores the result in `self.budgets`, and both
`ramp_gate_locked` and `refit_knee_locked` read
`self.budgets.for_gpu(&entry.gpu).knee_dispersion_in_force()`. Over a
CUDA + CPU inventory: GPU band **0.20**, CPU band **0.35**, CPU
`cap_fraction` 0.75, GPU `cap_fraction` `None` (P13), identically through
`VramLedger::new` (P14). No decision path still reads the global constant.

The runtime leg below confirms the config half end-to-end: a
`knee_max_bucket_dispersion = 0.02` in `[inference_local.vram]` reaches the CPU
device and every refusal line prints `threshold=0.02`.

---

## 3. The CPU legs

`legs.py --scenario S2 --model tags/wd-vit-tagger-v3` over the 2 000-image
`ramp` corpus, fresh store each, `RUST_LOG=info,panoptikon::inferio=trace`,
`INFERIO_WORKER_LOG_LEVEL=DEBUG`, `[inference_local.python_env] accelerator =
"cpu"`, and **no `knee_max_bucket_dispersion` in the TOML** (so the shipped
0.35 applies) except where stated.

**Quiet gate, and a deviation.** `quiet.sh` as in `final-n1` (load < 4, no
foreign `cargo|rustc|panoptikon|inferio_worker|pytest`), polling every 60 s for
at most 20 min. Leg **f2 passed that gate and was then contaminated**: the
docker container `dsv4-flash-sglang-vision` started at 05:34Z, one minute into
the leg, and loaded a vision model. f2 came back at *half* this host's
per-batch throughput (bucket 3 median 3.41 items/s against 6.85 in every other
leg) and took 597 s. The gate was therefore tightened for f3–f5 and the control
to also refuse when any foreign process exceeds 50 % of a core, excluding
SGLang's two permanently busy-waiting `sglang::schedul` threads (~85 % of a
core each, a standing feature of this box on 48 cores). f3–f5, the control and
the forced-refusal leg all ran under that identical steady baseline; f1 ran
before SGLang existed. Waits: f1 0 s, f2 60 s, f3 0 s, f4 0 s, f5 0 s,
control 120 s, fband002 0 s.

| | **f1** | **f3** | **f4** | **f5** | **fctl** (ramhog) | **f2** (contaminated) | **fband002** (band 0.02) |
|---|---|---|---|---|---|---|---|
| inference time (2 000 items) | 345.7 s | 328.0 s | 336.3 s | 339.0 s | 440.3 s | 597.2 s | 360.5 s |
| errors | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| grant→settle windows | 42 | 58 | 42 | 44 | 38 | 33 | 111 |
| knee fits | 3 | 3 | 3 | 3 | 2 | 1 | **0** |
| widenings | 2 | 2 | 2 | 2 | 2 | 1 | — |
| withdrawals | 0 | 1 | 0 | 0 | 0 | 1 | — |
| `declining to read …` | **0** | **0** | **0** | **0** | **0** | **0** | **114** (`threshold=0.02`) |
| store `knee_units` | **15** | **15** | **15** | **15** | **15** | **absent** | absent |
| store `max_units_measured` | **32** | **32** | **32** | **32** | **32** | 32 | **32** |
| store `slope_mb_per_unit` | 36.0 | 36.25 | 36.0 | 34.64 | 36.58 | 35.81 | 35.79 |
| store `base_mb` | 478 | 479 | 478 | 478 | 478 | 478 | 478 |
| peak own-worker RSS | **1 901 MB** | **1 902 MB** | **1 902 MB** | **1 900 MB** | **1 902 MB** | 1 903 MB | **1 902 MB** |
| `mem_available` floor | 87 421 MB | 96 681 MB | 96 741 MB | 96 759 MB | 90 155 MB | 94 973 MB | 96 198 MB |

Against `final-n1` on the same box and corpus: **n1-a** ended with no
`knee_units`, `max_units_measured = 256` and **8 387 MB** of worker RSS after
16 refusals; n1-b and n1-c ended at `knee_units = 15`, `max_units_measured =
32` and 1 904 / 1 890 MB. Every leg here is in the n1-b/n1-c class, and
`max_units_measured` never left 32.

### Per-bucket relative MAD (`bucketmad.py`, batch lines)

| bucket (sizes) | f1 | f3 | f4 | f5 | fctl |
|---|---|---|---|---|---|
| 1 (2–3) | 0.119 | 0.086 | 0.050 | 0.036 | 0.040 |
| 2 (4–7) | 0.015 | 0.036 | 0.055 | 0.139 | 0.085 |
| 3 (8–15) | 0.037 | 0.030 | 0.033 | 0.018 | 0.132 |
| 4 (16–31) | 0.101 | 0.085 | 0.052 | 0.052 | 0.072 |
| 5 (32–63) | 0.082 | 0.021 | 0.093 | 0.195 | 0.038 |

**These six legs never reproduced N1-a.** The warm-up tail that scattered by
0.293 in one N1 run in three did not recur here — the highest reconstructed
bucket is 0.195, under even the accelerator band. So the legs establish that
the fix does not *break* the quiet CPU case; they do not by themselves show it
repairing the failure, which the unit test and probe P3 do.

### The control

`ramhog.py` alongside the leg (6 GB allocated, every page touched, freed,
every 5 s, for the whole run): `mem_available` floor 90 155 MB against 96 681–
96 759 MB in the three quiet legs, so the hog was doing its job. The control
still fitted a knee at 15 and ended holding it, `max_units_measured = 32`,
1 902 MB of RSS, no runaway, and no `free to grow again` after a `declining to
read` (there were none). It did **not** reproduce `final-n1`'s control
numbers (knee 7, `max_units_measured` 16, 1 335 MB): it is the same shape as
the quiet legs, and it logged **zero** dispersion refusals where n1-ctl logged
two (`bucket=3 dispersion=0.2227 threshold=0.2`). That is the direct
consequence of the wider band — see concern 1.

### The forced-refusal leg, `fband002`

To exercise on real hardware the half the quiet legs could not reach, one leg
ran with `knee_max_bucket_dispersion = 0.02` in `[inference_local.vram]`
(`server-BAND.toml`) — a band no honest CPU bucket can meet, so every fit is
refused. This is `n1-a`'s situation with the refusals guaranteed rather than
lucky:

* 114 `declining to read …` lines, all `threshold=0.02` — the configured band
  reaches the **CPU device** through `http.rs::vram_budgets` end to end.
* **No knee was ever fitted**, exactly as in n1-a.
* The ramp **held**: 95 of 111 windows ran at rung 4, the ramp reached 32 units
  at most, `max_units_measured = 32`, peak RSS **1 902 MB**. Under the pre-fix
  `ramp_still_gains` this is the run that went 1 → 256 units and 8 387 MB.

---

## 4. Verdict

**VERIFIED WITH CONCERNS.** The commit does what it says, every rule in it is
load-bearing, the config follows the authoring rules, and nothing in the suite,
fmt or clippy regressed. Six CPU legs on this host all end in the n1-b/n1-c
class, and the forced-refusal leg shows the released-brake half repaired on
real hardware. The concerns are about what the fix costs and what it no longer
catches, not about its correctness.

1. **The CPU device loses the gate's sensitivity to real external motion.**
   `final-n1`'s control produced two genuine refusals under the same 6 GB/5 s
   hog at 0.20; at 0.35 this control produced none. On the CPU device the
   dispersion gate now only fires on motion considerably larger than that hog.
   That is the deliberate trade, but the band's own doc comment justifies 0.35
   as "the quiet ceiling with ~1.8× over it" without noting that the known
   true-positive is inside the new band.
2. **The warm-up-tail rule covers only a thin first window.** Three batches
   total, the first window's included, so a replica whose first window ran at
   depth gets nothing (P4), and a tail longer than the remaining allowance can
   leave survivors *noisier* than the whole tail (P6, 0.166 → 0.279). On the
   CPU the 0.35 band covers both; on an accelerator neither is covered, and
   `KNEE_MAX_BUCKET_DISPERSION` stays 0.20 there.
3. **The rule discards honest evidence on a thin-first-window replica**: the
   same curve knees at 15 with a deep first window and at 31 with a
   one-batch one (P1 vs P2) — up to 2× the batch size where the second window
   was perfectly honest.
4. **A section-wide `knee_max_bucket_dispersion` silently replaces the CPU's
   shipped 0.35** (P15: a section value of 0.28 gives *both* the GPU and the
   CPU 0.28). This follows `cap_fraction`'s existing precedent exactly, but the
   README paragraph documents only "default 0.20, or 0.35 on the CPU device"
   and does not say that setting the section key narrows the CPU device. The
   same applies to a user who uncomments the shipped example to see the
   default: on a CPU-only host that *tightens* the band from 0.35 to 0.20 and
   restores the N1-a exposure.
5. `VramBudget::knee_dispersion_in_force` guards against non-finite and
   non-positive bands but not against `> 1.0`, which `Settings::validate`
   rejects. Cosmetic; a `> 1` band simply disables the gate, which is what a
   user writing it would mean.
6. Leg f2 ended with no `knee_units` in the store, through
   widening → withdrawal, not through the dispersion gate (its 12 refusals are
   all "the plateau is not established above the knee yet" and its six others
   "too few observations"). That is the pre-existing machinery `final-n1`
   classed as by-design and unrelated to this commit — n1-b took the same path
   and refitted in time, f3 took it and refitted at 15, f2 did not because the
   job ended four seconds after the withdrawal. It is also the leg SGLang
   contaminated, and worth noting only so the row is not read as a regression.

## Files

`legs/{f1,f2,f3,f4,f5,fctl,fband002}/` — `legs.json`,
`calibration.after.toml`, `jobs.json`, `health-t0.json`, `health-end.json`,
`failures.json`, `driver.log` (with the `free -m` and `uptime` pairs).
`logs/*-S2.log` — the full gateway logs, ANSI stripped. `server-N1FIX.toml` /
`env.N1FIX` — the configuration the six default-band legs ran;
`server-BAND.toml` — the `0.02` deviation for `fband002`.
`adversarial-probes.rs` / `.out` — the verifier's probes and their output.
`reversion-checks.out` — the four rules reverted one at a time.
`quiet.sh` (with the SGLang deviation), `ramhog.py`, `runleg.sh`, `legsum.py`,
`rungs.py`, `bucketmad.py`, `n1analyze.py` — the tooling.
