# run4 — the MPS regression pass on `0d7f5671`, at the default wired limit

2026-09-16 17:32–18:50 UTC, over SSH, on the MacBook Pro M3 Max 128 GB
(`moot@10.11.11.3`). Tip under test **`0d7f5671`** ("Merge origin/master into
the batch-calibration branch"), which carries `fix/knee-race` (`8135ec6e`) and
`fix/visible-devices` (`2ca30b89`) on top of the tip §4.25 measured.

The comparand is the fourteen-leg set of run2 report §4.25, run on
**`27ff0790`**. Both sets were re-analysed here with **one** `analyze.py` — the
one at `0d7f5671` — so every verdict below is like-for-like; §4.25's own
printed verdicts came from the older analyzer and are not quoted as the
comparand.

## Platform, and the one deliberate difference from §4.25

| | §4.25 (`27ff0790`) | run4 (`0d7f5671`) |
|---|---|---|
| `iogpu.wired_limit_mb` | 122 880 (raised by hand) | **0 — the driver default, what every real user has** |
| `recommended_max_memory()` adopted | 122 880 | **110 100** |
| `hw.memsize_mb` | 131 072 | 131 072 |
| `--gpu-total-mb` seeded into `vramrec.py` | 110 100 | 110 100 |
| torch / macOS | 2.7.1 / 26.6.1 | 2.7.1 / **26.6.2** (build 25G83) |
| `PYTORCH_MPS_*_WATERMARK_RATIO` | unset | unset (torch default) |
| `ulimit -n` / `-Hn` in the leg shell | 256 / unlimited | 256 / unlimited |

`sysctl` needs sudo and was not available to this pass, so the limit was left
at its default throughout — **every number in this report is at wired limit
0**. That moves the adopted total 122 880 → 110 100 and with it every hog hold
(the hogs are sized `leave-free 20480` against the adopted total) and every
`limit_mb` ceiling. Differences traceable to that are named where they appear.

The machine was idle for the duration and each leg ran alone, one gateway at a
time, so the throughput figures are authoritative. A `caffeinate -is` of this
pass's own held the machine awake and was killed at the end.

### Instruments

`selftest.py` on this tip: **`VERDICT: no degraded tiers`**. `free_source`
`mps` (107 463 / 110 100 MiB), `base_method` **`mps` 1 384 MiB** on wd-vit with
the `alloc_delta` tier under it answering 860 (−38 %, the §4.16 figure),
`gpu_total_mb` 110 100, fp32, one batch of 8 at 5.745 units/s, `empty_cache`
true and 106 863 MiB free after teardown. Full output in
`instruments/selftest.log`, JSON in `instruments/mps-selftest.json`.

`cargo build --release -p panoptikon` exit 0 in 52.4 s (cargo 1.97.1).
`uv sync --locked --extra cpu` exit 0. `oracle_calibrate.py` is not run on MPS
(no per-process GPU counter to calibrate).

## 1. Verdict table — this tip against `27ff0790`

Both columns list only the checks that are **not** PASS. `INFO` and `SKIP` rows
are omitted; they were identical on the two tips on every leg except where
noted. Full tables: `analysis/analyze-0d7f5671.txt`,
`analysis/analyze-27ff0790.txt`.

| leg | `27ff0790` non-PASS | `0d7f5671` non-PASS | same? | items/s old→new | `grant_safety` over_oracle/grants |
|---|---|---|---|---|---|
| S1 | grant_safety FAIL, ledger_invariant WARN | grant_safety FAIL, ledger_invariant WARN | yes | – | 4/5 → 5/6 |
| S2-wdvit | grant_safety FAIL, ledger_invariant WARN | grant_safety FAIL, ledger_invariant WARN | yes | 28.169 → 28.169 | 5/149 → 3/149 |
| S2-clip | grant_safety FAIL | grant_safety FAIL | yes | 105.263 → 105.263 | 3/15 → 1/70 |
| S2-textembed | grant_safety FAIL, calibration_learned FAIL | calibration_learned FAIL | **NO (better)** | 237.6 → 224.7 † | 2/203 → **0/318** |
| S3-clip | grant_safety FAIL | grant_safety FAIL | yes | 90.909 → 90.909 | 3/33 → 3/33 |
| S3-wdvit | grant_safety FAIL, ledger_invariant WARN | grant_safety FAIL, ledger_invariant WARN | yes | 28.571 → 28.169 | 4/321 → 4/303 |
| S4a-mps | ledger_invariant WARN | ledger_invariant WARN | yes | 28.369 → 25.723 ‡ | 0/644 → 0/411 |
| S4a-ram | clean | **ledger_invariant WARN** | **NO (worse)** | 29.197 → 28.674 | 0/643 → 0/643 |
| S4d-mps | grant_safety FAIL, ledger_invariant WARN | ledger_invariant WARN | **NO (better)** | 29.091 → 28.777 | 1/638 → **0/643** |
| S14-tags | grant_safety FAIL | grant_safety FAIL | yes | – | 4/6 → 5/6 |
| S14-clip | grant_safety FAIL | grant_safety FAIL | yes | – | 3/6 → 3/6 |
| S14-whisper | clean | clean | yes | – | no grant lines (CPU/CTranslate2) |
| S14-ocr | grant_safety FAIL | grant_safety FAIL | yes | – | 3/17 → 3/17 |
| S14-chain | grant_safety FAIL | grant_safety FAIL | yes | – | 3/17 → 3/19 |

† the loadgen figure, not the analyzer's; see finding 2. ‡ see finding 3.

**Eleven of fourteen legs reproduce their `27ff0790` verdict set check for
check.** Two improved (`grant_safety` FAIL → PASS on S2-textembed and
S4d-mps). One moved the other way, `ledger_invariant` PASS → WARN on S4a-ram,
and it is the documented WARN class (finding 4).

Invariants that held on both tips:

* `over_headroom` — a grant beyond the headroom it was priced against — is
  **0 on every leg on both tips** (5 899 and 6 232 grants respectively). The
  clause the ledger actually enforces never moved.
* `limit = min(recommended_max, memsize − external − reserve)` on **5 899 of
  5 899** health samples here and 6 232 of 6 232 there — **100 % both times**,
  with the adopted total 98 304 (the pre-adoption 0.75 seed) → 110 100 here and
  → 122 880 there. `analysis/limit-formula-*.json`.
* **0 worker deaths, 0 OOM negatives, 0 throughput-collapse negatives, 0 failed
  items, 0 failed jobs** on every leg of both tips.
* S3 resumes on the persisted knee, not the seed: CLIP **31**, wd-vit **3**,
  identical `calibration.after.toml` knees on the two tips.
* The knees themselves are unchanged: wd-vit **15** on S2/S3/S4a/S4d, CLIP
  **127** on S3.
* `hog_tracking`: `external_mb` moved with all three hog holds on both tips.
* `persistence` PASS everywhere, worst anchor→store delay 29.8 s against a 30 s
  threshold on both tips.
* Descriptors: peak 81 of an unlimited hard limit (soft 256, raised by the
  gateway); `peak_fds` INFO, unchanged.

Florence2 was excluded from every leg (unbounded MPS growth per batch).

### Legs with no `27ff0790` comparand

§4.25's set is fourteen legs and contains no **S4b**, **S4c** or **S5**, so
those have nothing to compare against and were not run. Two of them are also
ruled out here on their own merits: **S5**'s fixtures are the CUDA-touching
`calibfixture/*_cuda` impls (`fixtures/README.md`), which have no MPS variant;
and **S4c** squeezes the board to ~2 GB free, which on a unified-memory machine
means leaving *the laptop* with 2 GB — the memory-pressure/jetsam experiment
this pass was told not to run. S4b (step-up at t=60 s) is safe and could be
added later; it is simply not part of the comparand.

## 2. R1 — five consecutive cold CLIP ramps on this tip

Fresh store each (`legs.py` builds a new root and DB per run-id), corpus
`ramp4`, 8 000 items, `clip/apple_MobileCLIP-S1`. Ramp *a* is the one inside
the fourteen-leg sequence; *b–e* ran back to back afterwards, mirroring
§4.25's `f-2long{,-b,-c,-d,-e}`.

**`0d7f5671` — 5 of 5 pass. No ramp ran away.**

| run | knee first | knee last | held rung (`held_units`) | `ramp_held` samples | `held_certified` | peak `unit_budget` | max units measured | peak pool (MB) | items/s |
|---|---|---|---|---|---|---|---|---|---|
| r4-2long (a) | 31 | 31 | **128** | 123 | **123 of 123** | 192 | 127 | **4 344** | 105.263 |
| r4-2long-b | 31 | 63 | **128** | 122 | **122 of 122** | 192 | 64 | **3 271** | 114.286 |
| r4-2long-c | 31 | 127 | **128** | 124 | **124 of 124** | 192 | 127 | **6 400** | 112.676 |
| r4-2long-d | 31 | 31 | **128** | 123 | **123 of 123** | 192 | 127 | **4 344** | 112.676 |
| r4-2long-e | 31 | 63 | **128** | 122 | **122 of 122** | 192 | 64 | **3 271** | 112.676 |

`27ff0790`, the same five slots, for contrast — run *a* is the defect:

| run | knee first | knee last | held rung | peak `unit_budget` | peak pool (MB) | items/s |
|---|---|---|---|---|---|---|
| f-2long (a) | **none ever** | none | field absent | **1 024** | **65 893** | 105.263 |
| f-2long-b | 31 | 63 | field absent | 192 | 3 271 | 114.286 |
| f-2long-c | 31 | 63 | field absent | 64 | 3 271 | 115.942 |
| f-2long-d | 31 | 63 | field absent | 192 | 3 271 | 114.286 |
| f-2long-e | 31 | 31 | field absent | 192 | 5 376 | 112.676 |

**Pass criterion met.** Every one of the five fitted a knee at 31 on its first
fit, every one declared the hold at rung **128**, and every hold was
**certified on every sample it was published in** — `held_certified` equals
`ramp_held` exactly, 5 times out of 5. Peak pool 3 271–6 400 MB against the
defect's 65 893 MB, a **10–20x reduction**, and no run visited a rung above
192. The gateway logged the hold once per run, with the healthy reason:

```
INFO panoptikon::inferio::ledger: holding the throughput ramp at this rung:
  the rung is the top of a measured plateau
  model=clip/apple_MobileCLIP-S1 gpu=GPU-MPS units=128
  certified=true knee_binds=false
```

Throughput: the five here average **111.5** items/s against the old five's
**112.5**, a ratio of **0.991** — inside the measure's own quantization (the
figure is 8 000 items over a whole-second job duration, so the ladder is
115.9 / 114.3 / 112.7 / 105.3 for 69 / 70 / 71 / 76 s).

One nuance worth recording against §4.25: that section read the runaway run's
**105.3 items/s = 0.92x** as a cost of the runaway. Here the in-sequence ramp
*a* also measures **105.263** while holding correctly at a 4 344 MB pool, and
the four repeats are 112.7–114.3 on both tips. The slot, not the runaway,
carries most of that 8 %.

## 3. Findings

### F-run4-1 — `tools/.../test_vramrec_mps_total.py` cannot pass on a Mac (tooling, pre-existing)

`python -m pytest tools/calibration-protocol/tests -q` is **1 failed, 166
passed** on this machine:

```
test_no_reading_at_all_leaves_the_total_null_never_zero
    oracle = _oracle(memsize_mb=None)
>   assert oracle.total_mb is None
E   assert 98304 is None
```

The test's helper documents itself as "Every reading injected: no sysctl, no
torch, no socket", and passes `memsize_mb=None` to mean *hw.memsize could not
be read*. But `vramrec.py:783` uses `None` as the sentinel for the opposite:

```python
self.memsize_mb = self._memsize_mb() if memsize_mb is None else memsize_mb
```

so on a real Mac the helper's "no reading" falls through to the live sysctl,
gets 131 072, and the 0.75 seed answers 98 304. On Linux and Windows
`hw.memsize` does not exist, `_memsize_mb()` returns `None`, and the test
passes — which is why CI has never seen it.

**Not a regression:** `git diff 27ff0790 0d7f5671 -- vramrec.py
tests/test_vramrec_mps_total.py` is empty. It was never caught before because
the earlier Mac passes never ran pytest there — `uv sync --extra cpu` does not
install it, and this pass had to add `--group test` to get a runner at all.
Product code is unaffected: the sentinel is only reachable through the test
seam. A fix would give `MpsOracle` a distinct "unset" sentinel, or give the
test its own subclass.

### F-run4-2 — an early uncertified hold can pin a rising, queue-limited curve for a whole leg (1 of 3)

The S2-textembed leg drives `textembed/all-MiniLM-L6-v2` with `loadgen`,
4 threads x 64 items — so every window is **queue-sized**, and §4.16 measured
MiniLM's curve as *rising to the end* with no knee to fit. On this tip the
throughput brake engaged on all three runs of that leg, always uncertified:

```
holding the throughput ramp at this rung: the ring cannot certify this rung yet
  model=textembed/all-MiniLM-L6-v2 gpu=GPU-MPS units=79360
  certified=false knee_binds=false
```

| run | hold engaged at worker-sample | `held_units` | lifted? | final `unit_budget` | held samples | items/s |
|---|---|---|---|---|---|---|
| r4-2t-loadgen (a) | **7 of 427** | 79 360 | **no** | **79 360** | **421** | **224.7** |
| r4-2t-loadgen-b | 13 of 425 | 83 456 | **yes** | 92 672 | 13 | 264.4 |
| r4-2t-loadgen-c | 23 of 424 | 84 992 | **yes** | 92 672 | 4 | 281.2 |

`27ff0790`, same leg, three runs: final budgets 92 160 / 89 088 / 90 624 and
237.6 / 266.2 / 280.8 items/s.

The lift path works — b and c each logged `the throughput ramp is free to grow
again` and finished at 92 672, matching the old tip's 89–92 k to within 4 %,
and their throughput matches the old repeats to within **0.7 %** (264.4 vs
266.2, 281.2 vs 280.8). Run *a* is the one that did not lift: the hold landed
~3.5 s into a 180 s leg and pinned the published budget at 79 360 for **421 of
427** worker-samples, **14 % below** what b and c reached, and it was the
slowest of the three at 224.7 items/s against the matched old run's 237.6
(**−5.4 %**).

This is an observation, not a proven regression: n = 1 for the condition, the
three-run means are 256.8 (new) against 261.5 (old) — **0.982x**, ranges
overlapping — and the slow slot is the in-sequence one on *both* tips. But the
mechanism is new on this tip, it is visible, and commit `e299149e` states the
rule it sits closest to ("a queue-sized window is no rung, and a hold must be
able to lift"). The thing to watch is that `ring_certifies_reached` needs
`MIN_KNEE_BUCKET_SAMPLES` = 2 observations in the frontier log2 bucket, and a
hold declared 3.5 s in, before the ring has filled, can sit at a rung the rest
of the leg never revisits — there is no path back if every subsequent window is
sized by the queue rather than by the budget. No product code was changed.

### F-run4-3 — S4a-mps is 9.3 % slower, and the granted rung explains it

28.369 → **25.723** items/s, the largest throughput move in the table. The
cause is visible in `utilization` and `ramp_progress`: the old leg granted at
most **32** units (published 32), this one granted **64** (published 64), with
the same fitted knee of **15** on both. §4.16's own wd-vit curve is 29.9
units/s at 8 units falling to 25.8 at 256 — so running the job at 64 instead of
32 is *expected* to cost roughly what it cost. The sibling legs move the same
way in miniature and in both directions (S4d granted 64 old / 32 new and is
0.989x; S4a-ram granted 32 on both and is 0.982x), which is the signature of
run-to-run variation in which rung the ramp is sitting on when the job drains,
not of a systematic change. n = 1 per tip, and the hog holds differ (0–100 224
MiB then, 0–88 704 now) because the hog is sized against the adopted total.

### F-run4-4 — S4a-ram `ledger_invariant` PASS → WARN is the documented `limit_fell` class

2 of 675 GPU-samples, **`over_grant` 0, `limit_fell` 2**. S4a-mps moved 1/679 →
4/743 and S4d held at 1. Per the README, `limit_fell` — the limit dropping
under a footprint or reservation already held — is WARN by construction and
cannot hold on a nearly-full device; `over_grant` is the FAIL class and is 0
everywhere on both tips. Under a hog sized against a 12 780 MiB smaller adopted
total the limit crosses a held footprint a couple more times. Known residue
class, not a new defect.

### Known residues that reproduced unchanged

* **`grant_safety` FAIL on 9 legs, oracle clause only** (§4.16 T2 / §4.20).
  `over_headroom` is 0 on all 5 899 grants; the failures are `oracle_free_mb` =
  **98 304 exactly** — `vramrec.py`'s 0.75-of-`hw.memsize` seed pricing the
  grants issued before the gateway's `/health` publishes the adopted total.
  Counts are in the same range on both tips (e.g. S2-wdvit 5/149 → 3/149,
  S14-ocr 3/17 → 3/17) and two legs improved to 0.
* **`oracle_agreement`, `base_accuracy`, `footprint_agreement` SKIP** on every
  leg, both tips — no per-process GPU counter on MPS, by design.
* **`slope_accuracy`, `utilization` SKIP** — no `--probe` files were passed on
  either pass.
* **`calibration_learned` FAIL on S2-textembed**, both tips: MiniLM's seed
  (120 000 tokens) *is* its peak, so "peak never left the seed" fires on a
  model that has nothing to learn. Pre-existing.
* **`job_outcome` SKIP** on the three loadgen legs — `minilm.sh` runs no job
  queue, by design.
* `S14-whisper` has no grant lines: CTranslate2 takes CPU on macOS, so whisper
  has no MPS replica (§4.16).

## 4. What is in this directory

```
report.md                    this file
legs/r4-*/                   the 20 leg recordings (14 regression + 4 extra
                             CLIP ramps + 2 extra MiniLM repeats). `root/`
                             (the per-leg DB and media, ~360 MB each) is not
                             kept; files over 100 KB are gzipped.
                             verdicts.json beside each is analyze.py's output.
instruments/                 selftest JSON+log, build log, the run scripts and
                             their .status files, the C1 server config, the
                             loadgen logs, and the two ad-hoc extractors
analysis/analyze-*.txt       analyze.py over both tips, one analyzer version
analysis/r1-*.json           the R1 ramp extraction, both tips
analysis/limit-formula-*.json  the limit identity over every health sample
```

The Mac's own copies stay under `~/projects/panoptikon-pr27/.mac/run4/` with
the leg trees (including `root/`) under
`~/projects/panoptikon-pr27/tools/calibration-protocol/results/r4-*`.

## 5. Wall clock

| task | span (UTC) | elapsed |
|---|---|---|
| 1 — push, checkout, build, `uv sync`, pytest, selftest | 17:32–17:38 | **6 min** |
| 2 — the fourteen legs | 17:38:44–18:23:36 | **44 min 52 s** |
| 2 — analyze.py over both tips (35 scenario dirs) | 18:44–18:49 | **5 min** |
| 3 — R1 ramps b–e (ramp a is inside task 2) | 18:24:53–18:35:00 | **10 min 7 s** |
| extra — two MiniLM repeats for finding 2 | 18:35:38–18:43:55 | **8 min 17 s** |
