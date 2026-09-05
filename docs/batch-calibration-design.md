# Batch calibration and VRAM budgets — design

Package 2 of the GPU compatibility work: the concrete design for items 5–8
of `gpu-compatibility-design.md` (self-calibrating batch size, pixel-budget
admission, footprint recording, VRAM-aware behaviour). Decided 2026-07-30;
revised the same day after design review (grant ledger, dispatcher window
rule, profile-key fallback, tiered base measurement); second review pass the
same day (envelope fit, pool-aware ledger + idle-resident trim, grant dual
denomination, reset and residual questions settled); third review pass the
same day (WDDM throughput-collapse signal, extrapolation ratchet,
non-local-profile margins, dispatcher pricing declared estimate-only);
fourth review pass the same day (single-currency driver-MB ledger,
load-phase reservations, universal worker→GPU pinning, free-intercept
fit); fifth review pass the same day (persisted ratchet state, local-store
write policy, pre-fit WDDM comparator, dtype-unknown load reservations,
concrete per-DB migration mechanics); sixth review pass the same day, on
the implemented steps 4 and 5 (knee-fit mechanics settled and its downward
ratchet closed, cost dimension made part of the profile key). Supersedes
the one-line itemization in that document.

**Status**: rollout steps 1a (worker-side memory sensing on the `load` and
`predict` responses), 1b (the per-GPU ledger, grants and fit snapshots on
request frames, load reservations, the worker's packing harness and defensive
clamp, universal worker→GPU pinning, removal of the dispatcher's cap rule) and
1c (the calibration store: the local TOML round trip for the ratchet anchor,
the sample ring and the fit, shipped-baseline lookup with the torch fallback
hierarchy, non-local-profile margin widening, and the `/api/inference/metadata`
calibration overlay) are implemented. Step 2 (budget configuration with per-GPU-UUID
overrides, the worker's reactive `empty_cache()` shrink with hysteresis, and
the orchestrator-initiated idle-resident `trim` message) is implemented.
Step 5 (the taxonomy table's impl-time verifications) is done — see the
table's status column and the registry's `metadata.cost` comments, both of
which now cite the code. Step 3 (auto everywhere with the number as a cap,
plus the stamped one-time config migration) is implemented. Step 4's
throughput-knee half is implemented: the knee is fitted orchestrator-side in
units/sec from warm-pool batches, gated on a minimum sample count across a
minimum number of geometric size buckets, enforced as a unit-side cap on
every grant, and persisted through the existing store write path — see
"Throughput knee: what was decided at implementation" below. The
shipped-baseline *directory* has been wired since 1c but no actual baselines
exist yet, which is the remainder of step 4. The easyOCR acceptance test of
step 1 is still outstanding (see "Remaining for the easyOCR acceptance test"
below).

**Vocabulary.** A *device* is the memory pool a model is admitted to; on a
CUDA or ROCm host that is one GPU, on an APU or Apple Silicon host it is the
unified memory, and `CPU` is host RAM. This document and the code say "GPU"
wherever the thing really is a discrete card, and "device" only where the CPU
and unified-memory pools are covered too — the admission key (`device_key`,
`inferio/gpu.rs::resolve_device_key`) is the clearest example.

### Throughput knee: what was decided at implementation

The design says "records units/sec per tried size and caps at the throughput
knee". Everything below is the concrete reading of that, settled while
implementing step 4 and recorded here because several parts are load-bearing
in ways the one-line statement is not:

- **Fitted in units/sec over log2 size buckets, one median per bucket.**
  Buckets because the ramp itself is geometric (a linear binning would leave
  every bucket but one empty) and because `sum`-dimension models never repeat
  an exact unit count; a median per bucket because one batch that raced a
  compositor redraw is a factor-of-two outlier and must not move a cap that
  is permanent in practice.
- **`KNEE_RATIO = 0.9`** makes "stopped improving" concrete: the knee is the
  smallest bucket already within 90% of the best rate.
- **Quantized to the top of its bucket.** Every size in a bucket is equally
  supported by the one median that summarizes it, so the cap does not creep
  downward as the ring ages, and "the knee changed materially" is decidable
  by equality for the store's write policy.
- **Fed by warm, full-budget batches only.** Pool-growing (high-water)
  batches are excluded — they pay `cudaMalloc` for the size they are
  *reaching*, and since every ramp step is high-water, including them would
  bend the curve downward with size and manufacture a knee out of allocator
  behaviour. Batches that did not spend their window's granted unit budget
  (below 80% of it) are excluded too: window tails, user-capped batches and
  contention-squeezed ones all ran small because there was nothing bigger to
  run, which is not evidence about the size. A batch that filled a
  *deflated* grant is admitted at its small size — that is honest data about
  running at that size. Measurements carrying no allocator reading at all are
  excluded rather than assumed warm. The cost is a rate, not a bias: a
  variable-shape model whose every window is a fresh high-water mark fills
  the knee ring slowly, and its curve is described by the sizes it repeats.
- **Frontier guard on the knee bucket** (the design phrases it on the best
  bucket): never cap at a size nothing was measured past. On real hardware
  the largest bucket is a hair above its predecessor essentially always, so
  requiring the *best* bucket to be interior would mean no knee is ever
  fitted; where the curve is genuinely still climbing, the knee bucket is the
  frontier too and the guard declines anyway.
- **Sticky, with a historical anchor.** A knee is replaced, never withdrawn:
  once it caps the budget, the sizes past it stop being run, so the ring can
  no longer answer and that silence must not be read as "no knee". The same
  effect applies to the *reference rate* — the peak that defined the knee
  ages out of the ring — so the threshold is taken against the best bucket
  median this model has ever shown here (a runtime-only high-water mark),
  never against the surviving ring's alone. Without both this and the
  full-budget rule, each refit lands lower than the last and the cap walks
  itself down to a single unit, absorbingly.
- **Enforced on the unit side.** `slope × knee_units` and a `min` on admitted
  units are the same constraint post-fit and the unit-side one also binds
  pre-fit; the same equivalence gives the contention appetite as `slope ×
  min(anchor, knee)`.
- **A profile's knee may be seeded from a shipped baseline** (unlike the
  ratchet anchor): it can only ever make a grant smaller. It is never written
  back out under our own generator stamp, and never overwrites a knee this
  machine fitted.

### Throughput knee: what run2 changed (R1)

Run1 measured the knee estimator firing on the wrong evidence and then
outliving it: `knee_units` 1 on S4d, 63 on S4e, 7 under the loadgen, 31 for
MobileCLIP against an optimum of 128, and — the half that made the rest
permanent — a soak whose knee was fitted **once**, four minutes in, and never
refitted for 7 h 55 m across 13 job passes and 56 worker spawns, because it
is persisted and every new replica is reseeded from it (report §4, findings
N1 / T1 / P5-4 / F-A). Four changes, and they are deliberately layered: three
of them narrow what may *become* evidence, and the fourth bounds the damage
of a cap fitted from evidence that was wrong anyway.

**(a) A window that was not free to choose its size describes no curve.**
Three exclusions on top of the full-budget rule above, all of which the
ledger already knows without asking anyone:

- a **squeezed** window (`Grant.squeezed`: the GPU could afford less than
  the anchor asked for) — its batches did spend their granted budget, so
  `FULL_BATCH_RATIO` waves them through, but the budget *was* the squeeze;
- a **memory-blind** window (the grant's `mb` is 0: a pre-fit grant on a full
  GPU, priced against nothing);
- a batch the **worker's defensive clamp** shrank (the measurement carries a
  `clamped` map). This one is per batch rather than per window, because the
  clamp fires per batch.

All three still feed the **cost fit** and the ratchet: a clean high-water
batch's allocator envelope is an honest point on the memory curve whatever
decided its size. Only the throughput ring is protected.

**(b) The contention tag: only a sole occupant may describe a curve.** Every
throughput sample carries the largest number of *other* replicas on the same
GPU that held an outstanding window overlapping it, and the knee is fitted
from the zero-tagged ones alone. A rate measured while a neighbour was
running is a rate for that GPU state, not for that batch size; run1 fitted
`knee_units = 7` under the loadgen and produced three throughput-collapse
negatives on MiniLM purely from sharing a GPU (P5-4, P5-5).

The tag is maintained by the grant path, which is the only moment occupancy
can rise, and it is a **high-water mark over the window's life**: a window
that starts alone and is joined half way through is tagged as contended.
Granularity is per window rather than per sample, because a measurement
carries a duration and no start instant — the approximation is one-sided, so
it costs honest samples (a knee found late) and never admits contended ones
(a wrong knee, which is the one that is permanent).

The same tag decides the **throughput-collapse verdict**. The worker's
collapse flag is a comparison between two consecutive batches, and a
comparison is only meaningful inside one occupancy regime, so the host trusts
it only from a window that had the GPU to itself throughout. A suppressed
collapse is discarded whole rather than counted as a clean batch: "we cannot
tell whether this was a spill" is not the finding "this was not a spill".
What is suppressed is the *verdict*, never the measurement: one batch can
carry `oom` and `throughput_collapse` together — an impl whose own halving
loop absorbed an out-of-memory runs its retries inside the same wall clock,
so its rate collapses for the most structural reason there is — and the OOM
is a statement about that batch which no neighbour explains.
Samples are **tagged and kept**, not dropped at ingest, so `/health`'s
`throughput_samples` still reports everything the replica produced.

**(c) The bucket-variance filter: no knee out of noisy evidence.** A
desktop's own VRAM and GPU churn moves throughput without moving anything
the ledger can observe — the contention tag sees our neighbours and nothing
else. The remaining defence is to notice that the observations disagree with
each other. So: a log2 bucket takes part in a fit only when it holds at least
**2 observations** (a singleton's dispersion is zero by construction, which
would wave through exactly the evidence being tested for), and if any
participating bucket's **relative median absolute deviation** — `MAD /
median` of its units/sec — exceeds **0.20**, no knee is fitted, none is
persisted, and the historical peak is not updated either.

Relative MAD rather than a coefficient of variation because the knee's own
per-bucket summary is a median: the same robustness that stops one
compositor-redraw outlier moving a permanent cap must stop it *blocking*
one. Run1's quiet wd-vit series is the case in point — relative MAD 0.003
against a CV of 0.252.

0.20 comes from the run1 series and from the knee's own arithmetic, and the
two agree. Measured (request-level items/s per fixed-size batch, which
over-states the noise of the batch-level series the ring holds): quiet GPUs
0.003 (`S2-wdvit-loadgen`) and 0.052 (`S2-minilm`); `S6-contend` 0.034 for
wd-vit and MobileCLIP and **0.899** for MiniLM, which is the series P5-5's
three spurious collapse negatives came out of. The geometric mean of 0.052
and 0.899 is 0.216. Independently, `KNEE_RATIO = 0.9` makes the knee a
decision about a 10% gap between bucket medians, and twice that gap is the
loosest per-sample scatter under which those medians still mean anything.

The cost is one-sided on purpose. A false negative is a knee found late:
bounded, self-correcting, paid in throughput on a model whose curve has
genuinely flattened. A false positive is F-A — `knee_units = 1` fitted four
minutes into a soak, persisted, reseeded into 56 replicas, 4 281 of 4 285
grants at one item for 7 h 55 m.

**(d) The knee is a brake with an expiry, not a ceiling.** The three rules
above narrow what may become evidence; this one bounds the damage of a cap
fitted from evidence that was wrong anyway — which no filter can rule out,
and which run1 showed nothing ever revisits.

After **12 clean windows** run *at* the knee, on a GPU that had room for
`RATCHET_FACTOR × appetite` while they ran, the cap **widens by one log2
bucket** (`knee_units` is the top of its bucket, so `2k + 1` is the top of the
next) and the counter resets. Once a widening reaches the extrapolation
ratchet's own ceiling — `RATCHET_FACTOR × anchor`, past which it could not cap
anything — the knee is withdrawn outright.

- *12*, because [`MIN_KNEE_SAMPLES`] is 12: twelve honest observations is what
  the estimator demands before it may cap anything, so twelve clean windows at
  that cap is the symmetric price of re-testing it.
- *At the knee* means the knee was the binding constraint **and** the window
  carried enough work to reach it. A window short of work, or held down by the
  ramp or the ratchet, says nothing about the cap.
- *With room to spare* means `headroom ≥ RATCHET_FACTOR × slope × min(anchor,
  knee)` and the window was not squeezed — exactly what the widened budget
  would cost. Re-widening into a full GPU would be a squeeze, not a probe.
- A **negative** window resets the counter. A model that just ran out of
  memory is not a model asking to be let out.

**One step, not a clearing.** Re-widening by a bucket rather than removing the
cap is the whole difference between a brake and no brake: if the knee was
right, the excursion costs one bucket's worth of throughput for one window and
the next refit puts it back; if it was wrong, the model climbs out of it one
step per twelve windows instead of never. The counter lives per (model,
GPU), not per replica — F-A's damage was done *across* 56 worker spawns, so
a counter that died with the replica would never have reached its threshold —
and it is **persisted** alongside `knee_units` as a local-only store field, so
a restart does not hand a stored knee a fresh twelve windows to be right in.
A knee that expires all the way to withdrawal is erased from the store by an
explicit signal, because the merge rule otherwise reads an absent knee as
"nothing fitted this run".

**The oscillation guard.** Immediately after a widening the sample ring is
exactly what it was when the knee expired, so a refit would hand the same
number straight back before the model ever ran at the wider size. A widened
knee therefore records the old cap's bucket as a frontier, and no refit may
install a knee at or below it until the model has actually been observed
running above it. Once it has, the refit runs normally — and on a genuinely
flat curve it re-establishes the same knee, which is the expiry working, not
failing: the steady-state cost is one probing window in thirteen, at twice the
capped size, in exchange for a cap that can never again outlive its evidence.
(R1e below replaces the "one warm batch above" test with a per-sample sequence
mark, which is what makes "after the widening" mean what it says.)

### Throughput knee: what run2 changed again (R1e)

Run2 ran R1 and R1d on hardware and measured them working — the expiry fired
ten times, every one at exactly twelve clean windows and one bucket; the
variance filter fired 59 times on MiniLM; `knee_clean_windows` survived a
restart — and it measured the estimator itself producing a knee that no
filter could have caught, because the evidence behind it was quiet, sole
occupancy, warm-pool and full-budget throughout. That is finding **F1**:

- **wd-vit**, whose measured curve is flat (35.9 items/s at batch 1, 36.1 at
  2 048, and no knee at all in run1) fitted `knee_units = 3` at 14
  observations, oscillated 3 ↔ 7 for the rest of the job as the expiry widened
  and the next refit put it straight back, and persisted 7. `utilization`
  0.11 against run1's 0.40.
- **S3**, seeded with that store, then held a *fresh* 2 000-item job between 7
  and 31 units for its entire length — 75 windows, `utilization` 0.01 against
  run1's 0.80. F-A across a restart.

Rebuilding the ring from `S2-wdvit/panoptikon.log` reproduces all five of that
leg's fits exactly, and the first one shows the mechanism:

| bucket | units | n | median units/s | in the fit? |
|---|---|---|---|---|
| 1 | 2 | 2 | **40.77** | yes — and the ring's *best* |
| 2 | 4 | 2 | 34.96 | yes |
| 3 | 8 | 2 | 40.31 | yes |
| 4 | 16 | 1 | — | dropped: a singleton cannot be certified quiet |
| 6 | 64 | 6 | 39.79 | yes |
| 7 | 136 | 1 | — | dropped, **and it was the frontier** |

The threshold is `KNEE_RATIO ×` the best bucket median = 36.69, and the
estimator returns the smallest bucket that clears it. The smallest bucket
*was* the best bucket, so it cleared its own threshold at the first
comparison, and the frontier guard passed only because the real frontier
(136 units, one sample) had been dropped and bucket 6 stood in for it.

The flaw is that the plateau test was **self-referential and one-sided**: it
compared the candidate to the ring's own maximum and never looked above the
candidate at all. On a ramp the ring is dense at the bottom — a window at a
small budget runs many warm batches — and sparse at the top, where a window
runs one; so the bottom bucket wins on both count and stability for any model
whose curve is flat, which is exactly the model that has nothing to gain from
being capped.

Five rules replace the single frontier guard. All five say the same thing: *a
knee is a claim about the curve above it, and may only be made from honest,
quiet samples taken in the regime the model is actually in.*

1. **The frontier must be quiet.** The largest bucket the ring *observed* —
   before the two-sample retain, not merely the largest that survived it —
   must itself pass the retain and the variance filter, and the knee may not
   be it. An unknown top end may be climbing. This is the rule that refuses
   wd-vit's first fit.
2. **The floor must be interior too.** The knee may not be the *smallest*
   bucket the ring observed. A plateau that starts at the first size ever
   measured is not a bend; it is the observation that nothing in the measured
   range gained anything, which is a statement about the range and not about a
   size. This is the rule that refuses every later wd-vit fit, including the
   ones taken entirely from samples the cap itself produced.
3. **The plateau must be established above the knee** —
   `KNEE_PLATEAU_BUCKETS = 2` quiet buckets strictly above the candidate, none
   of them faster than it by `KNEE_RATIO`. One bucket above is a single
   comparison between two medians: the same "two points are not a curve"
   objection that `MIN_KNEE_BUCKETS` answers for the fit and
   `MIN_KNEE_BUCKET_SAMPLES` answers inside a bucket. Two means the flat
   stretch spans a factor of four in batch size and, with rule 1, that it
   reaches the largest size the model has been let out to try. Not three,
   because each bucket is another doubling the ramp has to reach one window at
   a time before any knee may be fitted.
4. **No ramp-era knee below the anchor.** If the candidate is below the bucket
   of the largest batch the model has been *seen* to run cleanly at full
   budget — `max_units_measured` or the largest anchor the ring's own
   observations were taken under, whichever is greater, since DP-2's halving
   of the former unmeasures none of them — its own bucket must hold two
   observations taken
   once the ramp had already reached a *larger* bucket. A rate measured at 2
   units while the ramp was on its way past 2 units is not evidence that the
   model stops gaining at 2 — the ramp's next step is the standing evidence
   against it, and it is about to be taken. A rate measured at 2 units after
   the model has run 136 is a different thing: a steady-state window that
   happened to be small, and it counts.
5. **After a widening, the evidence must be newer than the widening.** Every
   observation carries a sequence number, and a widening records the mark it
   happened at. A knee at or below the widened-from bucket may only be
   installed once the smallest quiet bucket *above* that one carries
   `MIN_KNEE_BUCKET_SAMPLES` observations from after the mark. R1d's version
   cleared on "the ring now contains something bigger", which the ring already
   did — it still held the pre-knee ramp — so the widening survived about a
   second, five times over.

Two more changes carry the same principle outside `fit_knee`:

**A replica's first settled window contributes no throughput observations.**
cuDNN autotune, first-of-shape kernels, lazy module init and the JIT'd
preprocessing path all happen exactly once and none of them is a property of
the batch size; the high-water exclusion catches the pool growth and nothing
else. (This is *not* what produced F1 — wd-vit's first window contributed
nothing anyway — but a first window's rates are not on the curve, and one of
them landing in a bucket of two is enough to move a cap.)

**A knee this process never measured is provisional.** "Never measured here"
is exactly `!knee_is_local`, which the store and seed paths already set: while
it holds, the expiry counter is `KNEE_SEED_REVALIDATION_WINDOWS = 4` rather
than 12. A knee restored from disk is backed by nothing this process has seen
— the hardware, the driver, the corpus and the neighbours may all have moved
— so it brakes, because it is still the best evidence there is until this run
has better, but it goes on trial at once. A local refit installing a knee is
what makes it this run's measurement and restores the full twelve. S3 is what
treating the two alike costs.

**What the recorded rings do under these rules.** Every ring is rebuilt from
its leg's `panoptikon.log` and replayed sample by sample; the wd-vit and S3
rebuilds reproduce every logged fit of the original run exactly, which is what
makes them replays rather than models.

| leg | what the run fitted | under R1e |
|---|---|---|
| run2 `S2-wdvit` (218 obs) | 3, five times | **no knee at any point** |
| run2 `S3-wdvit` (205 obs) | 7, four times | **no knee at any point** |
| run2 `S2-minilm` (993 obs) | none | none (the variance filter, unchanged) |
| run1 `S6-contend` | 15 / 31 / 16 383 | none — two of the three models have *no* sole-occupancy observations at all |
| run2 `S2-mobileclip` (23 obs) | 127 | **no knee on this ring** — see below |

MobileCLIP is the one-sided cost, and it is worth stating plainly. Its bend is
real (31 units/s at 2 units, 94 at 64) and 127 describes its curve correctly.
R1e declines it because the ring has exactly **one** quiet bucket above the
bend: the ramp stalled at 136 units for reasons that have nothing to do with
throughput (run2 observation S1 — queue depth under multiplexed h2c), so
nothing at 256 units was ever measured. Two observations there and the same
ring answers 127. This is a knee found late, not a knee lost — and the leg
that fitted it ran at 0.94x master, where run1's leg on the same model with no
knee at all ran at 1.00x.

### Shape ceiling: the third brake (run2 S1)

The knee and the extrapolation ratchet are both statements the *ledger* makes
about a model. Run2's easyOCR leg found a third constraint that the ledger
cannot derive at all, because it is a property of the impl's kernels: CRAFT's
first `MaxPool2d` (`vgg16_bn.features[6]`) launches over its output element
count as a signed `int32`, so `64 × ⌊H/2⌋ × ⌊W/2⌋ × B` may not exceed
`2^31 − 1` — 28 items at the 1824×2560 padded tensor the shipped 2560 canvas
produces — whatever the GPU has free. The worker reports the trim as
`clamped: {from_units, to_units, free_mb?, reason: "index_limit"}`, and
`to_units` is denominated in the canvas and cost epoch the window was priced
under.

The ledger keeps that as a per-(model, GPU) **shape ceiling** and does five
things with it:

1. **`admitted_units` is min'd with it** — a second pure `min` beside the
   knee. Every unit admitted above it is admission the model cannot spend: the
   worker plans a bigger batch, trims it back to the same size, and the grant
   reserved memory for a batch that never existed. That is the over-admission
   S1 measured, invisible then because the trim was silent.
2. **The ramp takes no step past it.** The knee and the ratchet cap the budget
   and leave the exponent free to climb; this one stops the exponent too,
   because a window trimmed back to the ceiling is no evidence that a bigger
   batch would work and never can be — every window from here on is trimmed to
   the same size. Otherwise the ramp walks to `MAX_RAMP_STEP` against a wall
   and the budget jumps straight to the ratchet ceiling the moment the ceiling
   clears, with nothing measured in between.
3. **A clipped run is never read as a plateau.** A clamped batch is already
   out of the throughput ring, so no bucket, frontier or `observed_top` is
   built from one; additionally, a window the *ceiling* held down is not
   credited to the knee's expiry (the `knee_bound` comparand keeps the ceiling
   applied and drops only the knee), or a run of clipped windows would widen a
   knee on evidence the knee had nothing to do with.
4. **It never deflates anything.** An `index_limit` clamp carries no `oom` —
   the impl said "not this shape", not "not this much memory". The trap is the
   throughput-collapse flag: a batch trimmed from 200 units to 28 runs a
   fraction of the work at a fraction of the amortization and the worker's
   rate comparison sees a collapse, which used to be a negative sample. That
   verdict is now suppressed for an `index_limit` clamp, and only that verdict
   — a genuine allocator failure on the same measurement is read as usual.
5. **It is reported** on `/health` per replica as `shape_ceiling_units`, and
   logged at INFO once per (model, GPU) when it is set, lowered or cleared.

**Runtime-only, deliberately.** Two of its three inputs are not properties of
the machine: the padded dims come from *this corpus*, and the units figure is
denominated in the pixel canvas (R7) and cost epoch the clamped window was
priced under. It appears in no `ProfileUpdate` and no `ProfileSeed`; a restart
re-learns it from the first clamped window. It is stamped with the canvas and
epoch it was observed under, applied only to a replica whose own canvas and
epoch match, and cleared outright when they move.

**How it moves.** The smallest report wins — the binding padded frame is the
element-wise max over a batch, so a report from a batch of smaller pages fits
more of them under the same element limit and does not describe the frame that
bound. A batch *larger* than the ceiling that the impl did **not** cut retires
it: the dims moved. It is cleared rather than raised to the size just
demonstrated, and that is the only non-deadlocking choice — a ceiling caps
admission, so raising it to the largest batch seen would pin the budget at
exactly that size and make the next, larger demonstration impossible.

**Known limitation, stated rather than hidden.** Within one process the
ceiling only ever ratchets *downward*: while it caps admission no batch above
it can be granted, so the contradiction rule is reachable only from a window
granted before the ceiling existed, or from a canvas/epoch change. A
pessimistic report — a mixed batch whose one oversized page sets the padded
frame for the rest — therefore holds until the model is reloaded. The cost is
throughput, never a failure, and for a `sum` model it is small: the ceiling in
*units* is close to shape-invariant (`B × 16·H·W ≤ 2^31` means
`units ≈ B·H·W ≤ 2^31/16`), which is exactly why it is denominated in units
rather than in items. Letting a model climb back out would need a knee-style
expiry probe — one deliberately over-wide window every N, which the impl
trims, prices and reports harmlessly. That is not implemented.

## Core decision: learn a cost model, not a max batch size

Calibration does **not** learn "the batch size that fits". It learns a
per-model **memory cost model**

```
memory ≈ base + slope × units
```

where `base` is the load footprint (weights + fixed overhead), `slope` is
the marginal cost per unit of input, and *unit* is a model-specific cost
dimension declared in the model's metadata. Every batch is then sized
against the **currently available budget**: read live free memory, account
for every claimant, divide the remainder by `slope`, pack inputs up to that
many units.

Why this and not a learned max batch:

- A max batch bakes in the free memory at learning time. On desktops other
  processes' VRAM usage changes constantly, including mid-job; a cost model
  is re-evaluated against *live* free memory before every batch.
- A max batch bakes in the input composition it was tried with. For models
  whose memory scales with input size, one large image moves the number
  wildly; a cost model prices the actual batch being assembled.
- A max batch is per-card-capacity. A cost model is shared by every card of
  the same GPU model: a 12 GB and a 6 GB variant have identical slopes and
  different budgets. This is what makes profiles shippable.
- Learning a max batch means probing until OOM. On a desktop with a GUI on
  the GPU that is exactly the experience we must not create. The cost model
  is fitted from *measurements of our own usage at safe sizes* and
  extrapolated — the OOM boundary is predicted, never sought. The Package-1
  OOM halving loop remains as a backstop for prediction error and
  external-usage races, not as the mechanism.

OOM is not a reliable signal (anything can be using the GPU); our own
measured usage is. All calibration derives from the latter.

"Ideal" is bounded by a second observation: some models stop gaining (or
lose) throughput past a certain batch size. Calibration therefore also
records units/sec per tried size (rollout item 4: heterogeneous batches
make *items*/sec noisy for `sum` models) and caps at the **throughput
knee** even when memory would allow more.

## Cost dimension taxonomy

A model's cost dimension is `(unit, aggregation)`:

- `unit`: `item` | `pixel` | `token` | `audio-second`
- `aggregation`: how per-input units combine into batch units:
  - `count` — batch units = number of items (unit is fixed-size per item)
  - `sum` — batch units = Σ per-item units (e.g. total decoded pixels)
  - `max-times-count` — batch units = (largest item's units) × item count
    (padded/uniform batches: every slot pays for the largest member)
- `none` — no meaningful GPU batch scaling (remote APIs, sequential
  engines). No admission; at most a `base` footprint is recorded.

Declared in `inference.toml` metadata per group, overridable per inference
ID (same layering as every other metadata key). Missing declaration
degrades to `(item, count)` with a conservative slope — worse packing,
never a crash.

### Classification of the shipped registry

| impl_class (group) | dimension | basis | status |
|---|---|---|---|
| `wd_tagger` (tags) | `item` / `count` | model's own preprocess transform resizes to a fixed square (448px class) | verified in code |
| `moondream_tagger`, `moondream_captioner` (tags, vlm) | `none` | sequential engine: `predict` loops one image at a time (moondream's `encode_image` takes a single image), so batch size prices nothing — a packed batch's peak is the largest single item's. The tiling cap is real but moot: `max_crops = 12` + the global crop at 378px ≈ 1.9 MP ceiling per item. Both impls now declare `enable_batching = False`, so the worker takes the grantless path | verified in code (step 5; was `pixel`/`sum`) |
| `danbooru_tagger` (tagmatch) | `none` | network lookups, `num_gpus = 0` | verified in config |
| `dotsocr` (doctr) | `pixel` / `sum` | variable-resolution VLM; image-token count (and the KV cache behind `max_new_tokens = 128`) scales with decoded pixels | verified in code (dtype/FA2 sites) |
| `easyocr` (doctr) | `pixel` / `max-times-count` | batched CRAFT path requires uniform dims and pays max-size × batch — the known OOM trap ([easyocr-batch-oom]); currently `enable_batching = false` stopgap | verified in code |
| `doctr` (doctr) | `item` / `count` | detection resizes to the arch's fixed canvas (`db_resnet50` = 1024²) and recognition to fixed 32×128 crops; docTR re-batches internally on its own constants (det 2, reco 128), so what scales with *our* batch is the preprocessed tensors it moves to the device in one go — ~6.3 MB per page (fixed) against ~24 kB per detected word crop, so text density is a margin-sized term at this group's 1536px slice, not an order of magnitude | verified in code (step 5) |
| `florence2` | `item` / `count` | processor resizes to fixed 768×768; generation budget fixed per task prompt | verified in code |
| `sentence_transformers` (textembed) | `token` / `max-times-count` | inputs pre-split at `max_seq_length`, then padded per batch to the longest member | verified in code |
| `jina-clip-api` (textembed, clip, tclip) | `none` | remote API | verified in config |
| `faster_whisper` (whisper) | `none` | CT2 processes 30 s windows sequentially; VRAM ≈ constant per model, no torch allocator to measure. Excluded from calibration v1 (as it is from `run_with_oom_retry`) | verified in code |
| `openclip` (clip, tclip) | `item` / `count` | fixed preprocess resolution per model (224/378/384px) | verified in code |
| `qwen3-vl-embedding` (clip) | `pixel` / `sum` | qwen-vl-utils variable-resolution path, capped at `MAX_PIXELS = 1800 × 32² = 1.84 MP` per image (≤1800 merged vision tokens); the vision tower packs variable-length patches rather than padding them, which is what makes `sum` right | verified in code (step 5) |
| `qwen3-vl-embedding` (tclip) | `token` / `max-times-count` | the *text* tower: the processor truncates at `MAX_LENGTH = 8192` and pads each batch to its longest member (`padding=True`, `padding_side='right'`), exactly like `sentence_transformers` | verified in code (step 5; was `item`/`count`) |
| `nemotron-embed-vl` (clip) | `pixel` / `sum` | native-aspect tiling at 512px, bounded by `max_input_tiles = 6` + thumbnail (~1.84 MP per item); genuinely batched through `run_with_oom_retry`. Postdates the original table | verified in code (step 5) |
| `clap` | `item` / `count` | ClapProcessor truncates/pads every clip to a fixed 10 s window at 48 kHz (480 000 samples → one 1000×64 mel tensor), in every shipped checkpoint's `preprocessor_config.json` | verified in code (step 5) |

Notes:

- `max-times-count` models benefit most from **bucketing**: packing sorts
  the pending window by per-item units and builds batches from
  similarly-sized neighbours, so one 8000×6000 scan doesn't tax 63
  thumbnails. This is what finally retires easyOCR's
  `enable_batching = false`. Safety never depends on bucketing —
  max×count pricing admits a mixed batch conservatively (one big scan →
  batch of 1–2) — it is purely the throughput win, and its depth comes
  from window sizing (below). The easyOCR acceptance test must run under
  realistic core pipelining, or it measures a depth that never occurs in
  production.
- For `pixel` units, "units" means decoded pixels *as submitted* (after
  input-spec slicing/downscale) — the same quantity
  `slice_settings.mode = "pixels"` already reasons about upstream.
- **Pixel pricing saturates on capped VLMs** (step-5 finding, **fixed in
  run2 by R7**). Every `pixel`-class model shipped has an internal ceiling —
  qwen3-vl 1.84 MP (`MAX_PIXELS = 1800 x 32^2`), nemotron 1.84 MP (6 tiles +
  thumbnail at 512px), easyOCR's CRAFT detector 6.55 MP (a 2560px longer
  side), dots_ocr its own downloaded processor cap — while the worker priced
  the raw submitted pixel count, which keeps rising past it. Above the ceiling
  a batch was *over*-priced (safe, smaller batches); the costs were that a fit
  learned mostly from above-ceiling items carries a slope that
  *under*-predicts a batch of small ones by up to the saturation factor, and
  that a single large item exhausts a whole window's budget on its own. Run1
  measured both: nemotron fitted **4.33x** the probe's slope, 58 of 110
  batches held one item, and easyOCR granted 23-94 GB against as little as
  1 986 MiB of real free memory (run1 report §4, Q3/W1 and F-B).

  The fix is the per-item cap this section previously deferred, now spelled
  **`metadata.cost.canvas_pixels`** (an area, so the name says what it is —
  the placeholder name `unit_cap_per_item` said only what it did to a price).
  The registry declares it per model; `panoptikon/src/inferio/cost.rs`
  resolves it into the model's `CostDimension`, reading it only for a `pixel`
  unit and never inheriting it across a unit change, the same scale-bound rule
  `seed_units` has. **Both sides then price the same quantity**: the
  dispatcher applies `min(raw_pixels, canvas_pixels)` to the header estimate
  it sizes windows and asks for grants with (`estimate_input_units`), and the
  grant carries the figure to the worker, which applies the same `min` after
  decode in `price_inputs`. Capping only one side would leave the window bound
  denominated in raw pixels and the batches inside it in capped ones, which is
  the shape F-B measured; and for a model running with `enable_batching =
  false` — the three `easyocr_*` ids — the worker takes the grantless path and
  applies no cap at all, so the host's is the only one there is.
  A model whose canvas lives in a processor downloaded with the weights
  rather than in the registry is covered by a documented fallback — the
  worker reads the loaded impl's own `max_pixels`/`canvas_pixels` attribute,
  floored at 512^2 so a misidentified attribute cannot *under*-price an item —
  and **reports what it resolved on its `load` response**, so the orchestrator
  can price that model's windows by it too. A registry declaration always
  wins: it is the statement a maintainer wrote, reviewed and can correct,
  where the reading is an attribute off an object graph nobody here controls.
  Absent everywhere = uncapped, exactly as before. Declaring a canvas changes
  what one *unit* of that model means, so it bumps `metadata.cost.epoch` like
  any other change to memory behaviour that moves no key component: a slope
  fitted against raw pixels would otherwise be applied to capped ones and
  under-predict, which over-admits. Wire and worker details:
  `docs/inferio-worker-protocol.md`, "Memory grants" and "Memory sensing".

  **A declared canvas obliges the impl** (run2 D1-b). The cap is a statement
  that the model's *batch tensor* never exceeds that area per item, and the
  price is only honest if the impl enforces it before it forms that tensor.
  Most `pixel` impls do so by construction — they resize or tile each input
  and flatten it to a patch sequence, so no raw per-item dimension survives —
  but an impl that **pads a batch to a common size** does not, and easyOCR's
  did not: `pad_images_to_same_size` padded to the largest member's *raw*
  dimensions while the price said 2560². The cap makes this worse before it
  makes it better, because it is the cap that flattens an 8.7 MP scan and a
  48 MP sheet to one price and so lets them share a bucket. Two changes close
  it: `inferio.impl.eocr` resizes every input onto the detector's own canvas
  before it pads, and `plan_batches` keeps **raw** pixels as a descending
  secondary key among equally-priced items, so a bucket stays as
  size-homogeneous as the corpus allows. A worker-side warning names any
  future impl that pads to a common size while declaring no canvas of its own
  and is handed a batch mixing raw sizes by more than 2×.

  The obligation stops at the tensor. easyOCR's recogniser crops boxes out of
  the page, but each crop is resized to a fixed `imgH × imgW` before it
  becomes a tensor, so its device memory does not scale with the page's
  resolution — and the impl therefore detects on the canvas-bounded batch,
  maps the boxes back, and recognises from the **raw** image, which is what
  keeps transcription quality on large scans identical to the unbatched path.
  The test for a new impl is whether an allocation grows with the input's
  area, not whether an array is large: bounding work that is already
  area-independent costs output quality and saves nothing the ledger prices.
- Backends without a free-memory query (MPS, CPU) degrade to no
  admission: seed-sized fixed batches plus the Package-1 backstop, the
  same class as `none`.
- A resident `none`-class worker with no torch allocator (faster_whisper /
  CT2) reports ~0 `memory_reserved`, so its real VRAM lands in the
  ledger's `external` term — margin-inflated but safe. This is the
  intended accounting, not phantom headroom, until CT2 footprint
  recording exists (see Open questions).

## Where each piece runs

The inference server (inferio) is independent of core, can be remote, and
can serve several cores; one host can run several workers (models ×
replicas) on one GPU. VRAM is therefore a shared resource with exactly one
component that sees all claimants — the Rust orchestrator — and sizing
must be centralized there, not computed independently per worker:

- **The orchestrator is the budget arbiter.** Per GPU (by GPU UUID) it
  keeps a ledger: the configured limits, each resident worker's recorded
  `base`, each in-flight window's outstanding **grant**, and the freshest
  external-usage sample. All sizing intelligence lives here: it fits the
  cost model from reported samples, owns persistence (atomic TOML rewrite
  of the local store, shipped-baseline loading), sizes dispatcher windows,
  attaches a memory grant to every window, and exposes state read-only
  over the API for UI/labels (`/api/inference/metadata` overlay, like
  `unavailable` today).
- **The worker is mechanism and sensor.** It is the only place that has
  torch, the allocator statistics, `mem_get_info`, and per-item unit
  counts after decode. A harness around `predict()` (sibling to
  `run_with_oom_retry`) packs the window into GPU batches within the
  grant (bucketed for `max-times-count`), applies the defensive clamp,
  measures every batch, and reports measurements plus a fresh device
  memory sample on the response frame.
- **Core keeps its opaque-ID worldview** — it learns nothing about VRAM,
  GPUs, or profiles. It forwards the user cap per request and sizes its
  *requests* by its own concerns (payload memory in flight, pipelining),
  not by guessing GPU batches. The orchestrator/worker re-slices whatever
  arrives.

Transport: **no new channels**. The worker protocol is strictly
request/response with one window in flight per worker, so grants (and
cost-fit snapshots, when they change) ride on request frames,
measurements and memory samples ride on response frames, and the load
response carries the base measurement. A worker-initiated query channel
("ask the scheduler for the current budget mid-window") was considered
and rejected: it complicates the protocol for a grow-direction freshness
win the grant model already bounds (see the staleness note below). One
addition ships in v1 and is compatible with that rejection: an
**orchestrator-initiated trim message** (see Reactive shrink below) — a
new message type on the existing request/response channel, same direction
as `load`; only worker-initiated queries were rejected.

Two keyspaces, deliberately different:

- **Cost profiles** are keyed by GPU *model* (`name` string) + environment
  tuple — a property of the silicon and software, shareable.
- **Budgets and budget settings** are keyed by GPU *instance* (GPU UUID,
  `GPU-…` from NVML/nvidia-smi/torch device properties; on ROCm the same
  `GPU-` prefix over a KFD `unique_id` or the GPU's PCI address). Two identical
  cards on one host share profiles but can carry different budget settings
  (e.g. the one driving the monitors gets a bigger margin). CUDA device
  index is **never** an identity — it is not stable across reboots or
  `CUDA_VISIBLE_DEVICES` changes.

**Every worker is pinned to exactly one GPU.** The spawn machinery
already supports pins (`config.replicas`/`config.devices` →
`CUDA_VISIBLE_DEVICES` per replica), but the default today is a single
*unpinned* replica that sees every device — impls then run on
`devices[0]`, so on a multi-GPU host attribution is ambiguous and card 0
silently gets everything. Under the ledger, ambiguity is unacceptable:
the orchestrator resolves an explicit pin for every worker at spawn,
written in the UUID form CUDA accepts directly
(`CUDA_VISIBLE_DEVICES=GPU-…`), so the pin shares the budget keyspace's
identity and device-index instability never enters. (ROCm shipped the
index form instead — `HIP_VISIBLE_DEVICES=<row index>`, where the row order
is the openable KFD nodes' order, i.e. what ROCr enumerates; HIP accepts no
UUIDs, so no index→UUID mapping exists or is needed, and the row order is
cross-checked at registration.
See `docs/rocm-batch-calibration-parity.md` D2.) Default placement is the
**highest-compute-capability GPU** (ties broken by VRAM total descending,
then the lowest index — an all-unknown-capability ROCm host would otherwise
let a first-enumerated small GPU outrank the big one), which is rough parity with
what an unpinned worker got before: torch's default device order is
`FASTEST_FIRST`, so "no pin, impls run on `devices[0]`" already meant the
fastest GPU rather than the first one on the bus. Headroom-based
placement across cards is a natural later upgrade once ledgers exist, not
v1. The impl-side multi-device path
(`get_device()` returning several devices) drops out of the supported
envelope: a worker sees exactly one GPU, and every report (base,
reserved, memory samples) lands on exactly one ledger.

## Dispatcher windows and the batch cap

The dispatcher's current effective-cap rule (max over the explicit
`max_batch` values in the window; else registry `default_batch_size`;
else server default) existed to reconcile "inferio doesn't know what is
safe" with heterogeneously-capped requests. Grant-based admission removes
that job, so the rule is **deleted, not adapted** — its OOM-recovery
rationale is obsolete once safety lives in the ledger.

Under auto:

- **Window size comes from the orchestrator's fitted model**, like the
  grant: a few GPU batches' worth of units (≈2–4× the current admitted
  batch estimate; seed-derived before calibration), additionally bounded
  by payload bytes (the transport frame limit — `MAX_FRAME_BYTES`, 2 GiB —
  is the hard wall). Windows
  deep enough to hold several batches are what give bucketing material
  and amortize the request/response round trip; the *bound* is what keeps
  work divisible across replicas (an unbounded drain would hand the whole
  queue to the first free replica) and keeps the failure blast radius
  small (a window is the unit of fallback and of fatal-error loss). There
  is no time bound anywhere: `predict` keeps its no-deadline semantics.
- **Dispatcher-side unit counts are estimates, and safety never depends
  on them.** Window sizing and grant pricing need per-item units before
  any worker has decoded anything: `pixel` models use image-header
  dimensions (parsed at dispatch, or forwarded by core, which already
  knows post-slicing dims); `token` models use a bytes-per-token
  heuristic (the dispatcher cannot tokenize); `max-times-count` window
  depth uses the sum-of-units approximation (true max×count is undefined
  before the worker buckets). Mis-estimates only mis-size windows — an
  over-estimate yields a larger grant still clamped by headroom, an
  under-estimate yields more GPU batches per window — because the worker
  packs within the grant using exact post-decode counts.
- **The user cap travels per request.** Windows are partitioned by cap
  value — capped jobs are the exception under auto, so mixed-cap queues
  are rare and the partition costs nothing — and the worker enforces the
  cap at pack time as an **item-count constraint**, never converted to
  units. A capped window is *also* bounded in items, at the same batch
  depth the unit budget uses: the cap makes the worker's batches small
  regardless of the budget, so an unbounded capped window would become
  thousands of one-item batches — one measurement and one driver query
  each, overflowing the telemetry ring and deferring the grant's
  re-evaluation for minutes.

### The unpriced path

`none`-class models, a host with no GPU inventory and a GPU outside the
enumeration get no admission handle and therefore no grant, and the worker
runs no packing harness: the frame the worker receives *is* the GPU batch.
Every frame — a merged window's as much as an oversized lone request's — is
bounded in items by `min(user cap, default_batch_size or default_max_batch)`.
Using the configured batch size rather than the calibration seed is a
deliberate deviation from "seed-sized fixed batches": a host with no
free-memory query has no VRAM budget to protect, and `none`-class models do
not scale with batch size, so seeding those hosts' batches would be a
throughput regression with no safety benefit. The impl's own OOM-halving loop
remains the backstop.

### The window settle

The dispatcher forms a window out of whatever is queued at the instant a
replica frees — and a replica frees when the replies go out, *before* a
closed-loop caller has released its permits and re-submitted. A caller of
depth `C` therefore leaves `C - W_k` queued behind window `W_k`, so
`W_{k+1} = C - W_k`: an involution, every value on a period-2 orbit and none
of them attracting. The largest window the system can form is `C - W_min`
rather than `C`, the mean is `C/2`, and half of all windows are the small
phase — which also halves the rate at which the ramp advances.

The fix is to wait, briefly and conditionally, for the refills the window
that just completed will provoke. The wait ends on the first of: the queue
reaching the unit budget, a quiet gap of 2 ms with no arrival, or 20 ms past
the moment the last window finished. A refill is a caller task already parked
on its unit semaphore with its bytes loaded, so its latency is a wake, a
multipart build and a send — sub-millisecond on a loopback self-call, one RTT
more from a gateway elsewhere; 2 ms covers that with margin. The 20 ms bound
is under 1 % of the seconds-long windows this applies to, and it bounds the
case a quiet gap alone does not: a caller trickling arrivals every 1 ms
forever.

This is not the batching timer the dispatcher section forbids. It never
applies to an idle model (it is armed only by a window that just completed),
never applies on the unpriced path (there is no unit budget to fall short
of), never applies when the queue already fills the window, and it ends as
soon as arrivals stop.

### The in-flight items figure

Core must not learn about VRAM, so the one number that crosses the boundary
is an item count: the window's unit target projected through the most recent
window's items-per-unit ratio (a seed ratio before the first window), times a
slack of 2, and bounded by the payload-byte wall converted through the same
window's bytes-per-item. The slack of 2 is the smallest value that lets the
next window be formed out of requests queued while the current one runs: at
exactly one window's worth in flight the queue is empty the instant a window
forms, so consecutive windows can never merge. The byte bound is applied
without the slack, because past the byte wall no amount of extra in-flight
work can make a window bigger.

When the ledger squeezes a grant, the figure and the next window's unit bound
both follow the **granted** budget's own window depth rather than the
anchor-derived target: publishing the target anyway asks the caller to keep
feeding windows sized for memory the GPU does not have, and the window then
runs for as long as it takes to chew through them at the squeezed batch size,
with no grant, no high-water sample and no re-pricing in between. Both
callers are needed: core clamps what it is told to a floor of 64 items, so
under a hard squeeze the window bound — which has no floor — is the only
thing keeping a window from running blind. The clamp is always applied to the
anchor-derived target and the grant in hand, never to a bound that already
carries an earlier window's clamp, so an unsqueezed grant restores the figure
on the very next window.

`queue_bound_windows` on `/health` counts the priced windows formed short of
the unit budget the ledger allowed. It is what separates "this model is
memory-bound" from "this model is starved": a ramp that is not advancing
while this counter climbs will not move for any amount of freed VRAM.

### Replicas, deaths and the OOM fallback

The dispatcher owns the model's replicas and serves them from one shared FIFO
queue; free replicas sit in a pool and each in-flight window is a task that
returns its replica when it completes. Pickup is FIFO by construction
(windows are queue prefixes); completion order across replicas may differ,
which is harmless because every request replies through its own oneshot.

Death policy: any replica failing fatally kills the whole model (degrading to
a smaller set is future work). Queued requests are failed, windows in flight
on other replicas are aborted, idle replicas get a ladder-less kill, and the
manager's death handler runs once under the load-generation guard. A death is
normally discovered by a request failing on the pipe, which leaves an *idle*
replica's death invisible — a model nobody predicts against reads nothing —
so the manager's sweeper ticks a liveness message that `try_wait`s every free
replica and takes the same path, minus the window settlement it has no window
for. An idle replica's death settles nothing on purpose: a death mid-window
is a synthetic memory negative on unified-memory devices, and a replica with
no window in flight can say nothing honest about a batch size.

A fatal failure settles as `WorkerDied` only when the worker actually stopped
answering; a torn-down stream the dispatcher itself caused by dropping a
request future (the user-cancel path) settles as `Aborted`, which teaches the
ledger nothing. The death is claimed rather than read, so one death settles
at most one window.

When a merged window fails with a per-request error the requests are retried
individually inside the same reservation, but if the failure was an
out-of-memory condition the retries carry a **halved** unit budget: the same
grant would let the worker's packer rebuild the batch size that just failed.
The MB reservation is untouched, since this window's reservation covers the
retries either way. Classifying that out-of-memory reads only the failure's
own message and traceback, never the worker's stderr tail: the tail is a ring
of whatever the worker logged over its recent life, including an
out-of-memory it caught, halved and recovered from requests ago, and letting
a stale line flip an unrelated failure into a negative sample would deflate
the model and halve its grants over a batch that never failed.

## Grant sizing and packing

Orchestrator, per GPU, when dispatching a window:

```
growth(w)    = max(0, reserved(w) − reserved_at_load(w))
footprint(w) = base(w) + growth(w)      # driver currency, ≥ base
charge(w)    = footprint(w) + max(0, Σ grants(w) − growth(w))
external  = max(0, total − free − Σ footprint(our workers))
limit     = min(total × cap_fraction,           # server lever, default off
                total − external × (1 + margin)) # desktop lever, default on
headroom  = limit − Σ charge(residents) − Σ load_reservations
grant     = min(headroom share, ramp step, slope × knee_units,
                slope × shape_ceiling_units,
                priced content of the window itself)
```

The `slope × knee_units` term is written on the MB side here and enforced
on the **unit** side in the implementation (`admitted_units`): post-fit the
two are the same constraint, since a grant's MB figure is `units × slope`,
and the unit-side form needs no fit to be in force — so a knee still binds
on a model that has not been fitted yet. The `shape_ceiling_units` term is
the same shape and is enforced in the same place (run2 S1, "Shape ceiling:
the third brake"): the size the impl's own kernels have said they cannot
execute at this corpus's shapes.

- **The ledger runs in one currency: driver MB.** A worker's charge is
  its `footprint` — process-level `base` (context + workspaces +
  weights) plus allocator pool growth since load. Charging allocator
  `reserved` alone would misclassify each resident's ~0.5 GB context and
  workspaces as *external* (margin-inflated) while `base` counts them
  again — a systematic double-count worth 1.5–2 GB across a few
  residents; charging `base` alone would hand a resident's retained pool
  out again to neighbours — releasing a grant returns nothing physically
  until `empty_cache()` — who then hit the defensive clamp forever.
  `footprint ≥ base` by construction: residency changes who has already
  paid the base, not whether it counts. Where NVML per-process works the
  orchestrator may substitute the exact per-PID figure (the same tier
  machinery as base measurement); `base + pool growth` is the WDDM-safe
  approximation.
- **A grant is dual-denominated**: an MB reservation (the ledger
  currency) and a unit budget (the packing currency). Post-fit the unit
  budget derives from the MB side via the slope; pre-fit there is no
  slope, so the unit budget is the ramp value (`seed_units × 2^k`) and
  the MB side is the contention share held while that step is measured.
  Without this the ramp is unit-shaped, the ledger is MB-shaped, and the
  conversion is undefined exactly when it is needed most.
- **A grant and the pool it grows are the same memory, charged once.** Post-fit
  a grant's MB figure is the envelope over `reserved_at_load` the window may
  reach — exactly what the footprint's growth term already counts once the pool
  has grown into it. Summing footprints and grants GPU-wide would double-charge
  every busy resident's working set: on a 6 GB card a model with a 2.4 GB working
  set would be charged 4.8 GB over its base, which declares the GPU full,
  collapses that model's own next share to the contention floor, and never
  recovers. One window is in flight per replica, so the honest charge is per
  replica: `footprint + max(0, Σ grants − pool growth)`.
- **Grants are reservations, not estimates.** Two replicas cannot claim
  the same headroom, so the concurrent-ramp race is structurally
  impossible rather than probabilistically mitigated. A grant is released
  when its response frame lands; a dying worker's grants are released
  with its aborted windows under the existing generation guard. A *hung*
  worker (stuck CUDA call) holds its grant indefinitely — deliberate:
  `predict` has no deadline by standing policy, the memory genuinely is
  unavailable, and the contention floors keep neighbours running at
  seed-batch throughput until the operator intervenes (drain + restart,
  the existing stuck-CUDA recovery).
- **A load in progress is a reservation too.** Loads are serialized by
  the manager's load lock, but dispatch is concurrent with loading:
  without a charge, windows granted during a multi-second load collide
  with the incoming weights. From load-start the ledger holds a
  `load_reservation` at the *expected* base (local profile → shipped
  profile → conservative constant), replaced by the measured value when
  the load response lands. This is also item 8's trigger arriving early:
  expected base exceeding headroom is the evict-before-load signal. That
  signal needs a *measured* GPU, so the load path first probes the host
  for the GPU's free memory when its reading is missing or stale — the
  staleness refresh only runs from a grant request, which needs a resident
  worker, and a GPU that has never had one would otherwise be priced as
  empty however full it is.
  One wrinkle: `dtype` is in the profile key, but dtype negotiation
  (Package 1) resolves *during* the load — on the first-ever load of a
  model on a GPU the orchestrator cannot know which dtype's profile to
  consult, and guessing fp16 when negotiation lands on fp32
  under-reserves ~2× for exactly the seconds the reservation exists to
  protect. When the negotiated dtype is unknown, reserve at the most
  conservative plausible dtype's base (fp32 profile if present, else the
  constant); the load response reports the actual dtype, and the
  orchestrator remembers the negotiated outcome per (model, GPU) for
  subsequent loads. Two distinct states share the word: "dtype not yet
  known" is the absent query (`None`, matches every profile, takes the
  conservative maximum), while the literal key value `"unstated"` is a
  worker's report that it neither selected a dtype nor could infer one
  from its weights; it matches only itself. (Spelled `"unknown"` before
  run2 change R11: a key component that reads as a failure invites a
  consumer to treat it as one, and the two states above are exactly what
  must not be confused. The rename moves the key, so rows written under
  the old spelling stop matching and are re-measured — deliberate, and
  cheap, because the sentinel was introduced during run1 and nothing has
  been released under it.)
- **External usage is derived, not margin-guessed, for our own
  processes.** Every worker reports `memory_reserved` per response (and
  `reserved_at_load` once, on the load response), so the orchestrator
  computes footprints and the margin multiplier applies only to
  genuinely external usage — sibling workers, contexts and workspaces
  included, are never margin-inflated. `external` is clamped at ≥ 0:
  `free` and the per-worker samples come from different moments, and
  sampling skew must never manufacture phantom headroom. When a replica
  leaves the GPU its footprint is credited back to the freshest free
  reading as it drops out of the sum — nothing samples a GPU *because*
  a worker left, so without the credit the departed replica's whole
  footprint would be reattributed to `external` and margin-inflated
  against the next model to load — and the adjusted reading is flagged
  for a live re-read on the next grant. Samples arrive
  only on response frames, so after a long idle gap the first window
  prices `external` from a stale sample; the shrink clamp makes that
  safe, and the orchestrator refreshes via NVML (the Package-1 probe
  machinery) when the freshest sample exceeds an age threshold — a
  single coherent snapshot (total/free/per-process in one read),
  preferred over stitched per-frame samples whenever it is fresh. In
  scope for v1, since the probe machinery already exists; an accuracy
  measure, not a safety requirement.
- **Free memory is reported per batch, not per window** (run2 change R5;
  finding T3). Samples arriving only on response frames made `external` a
  window-boundary quantity: run1 measured 0 host probes in 2.5 h, a freshest
  reading ageing to 166.9 s, a +30 GB external step taking 31.5 s to reach
  `/health`, and a 53 GB ten-second spike moving it by 2 MiB. The worker's
  defensive clamp already reads live free memory before **every** batch, so it
  reports that reading on the measurement (`free_mb`/`free_source`), and the
  orchestrator folds each one into the GPU under the same source-precedence,
  departed-worker-credit and currency rules a memory sample obeys. Within one
  response the readings apply in measurement order and the response-level
  sample last, matching the order the worker took them. `external_mb` then
  refreshes at response cadence rather than at the staleness timer, and a
  window that OOMed contributes its readings too — the reading describes the
  GPU, not the outcome.
- **Contention policy** when several models are hungry at once: demand
  first (queue depth; an idle model consumes no new grants, though it
  holds its pool until trimmed — see Reactive shrink), then split by
  calibrated appetite `slope × knee_units` — implemented as `slope ×
  min(ratchet anchor, knee)`, the same unit-side equivalence as the grant
  term above, so a knee-capped worker cannot claim a share of the GPU
  sized for a batch it will never be admitted for — falling back to `base`
  weighting before calibration, with a floor of one seed batch per worker
  so nothing starves to zero. When even the floors oversubscribe
  headroom they shrink pro-rata — grants are reservations and the ledger
  invariant is never violated — bottoming out at the one-item minimum at
  pack time.
- **Fit confidence widens margins automatically**: `residual_mb` (and
  non-local-profile status — any shipped or fallback-matched entry not
  yet locally confirmed, see Lookup) inflate that model's effective
  margin, clamped to a maximum. Both inflations are **additive
  increments** on the configured margin and it is their sum that is
  clamped, never the total: the user's own number survives whatever they
  set it to (including 0.9), and a configured margin of 0 — a headless
  box — still gets the unconfirmed-profile widening rather than
  multiplying it away. Safety never depends on a human reading a
  Desktop label; the future tab's "verified" badge is presentation on
  top of the same number.
- **Ramp**: until the fit has enough samples, grants ramp geometrically
  (seed, ×2 per clean window) instead of jumping to the predicted
  ceiling, measuring each step. A too-low seed costs a logarithmic number
  of windows, which is why seeds don't need per-GPU tuning. A step is earned
  only by a window that actually **produced** a high-water measurement, not by
  the mere absence of bad news: a model whose batches all run on a warm pool
  reports nothing about a bigger batch's cost, and doubling per window
  regardless would walk the budget to its ceiling on hope alone.
- **Extrapolation ratchet**: the ramp never ends by handing control to
  extrapolation. Even after the fit converges, a grant's unit budget
  never exceeds ~2× the largest *locally measured* clean high-water
  batch; the measured range extends itself geometrically under real
  load. The fitted model's job is pricing mixed compositions and
  re-evaluating against live free memory — never predicting far beyond
  evidence, which is exactly where nonlinear effects (allocator
  behaviour, attention memory, workspace growth) break linearity, and
  where WDDM gives no clean failure (see Backstop). The ratchet counts
  only local samples, so a fresh install ramps from seed even with a
  shipped profile: profiles govern pricing, `base` accounting, and the
  knee cap — not growth. The ratchet anchor **persists**: the local
  store records the largest locally measured clean high-water batch
  (see Calibration store), so a restart resumes from the measured range
  instead of re-ramping from seed — otherwise the "ramp cost is
  logarithmic and one-time" argument silently becomes "per restart" on
  desktops. A persisted anchor still enters every window through the
  defensive clamp against live free memory, and deflation state remains
  runtime-only. The anchor floors the ramp **exponent**, not merely the
  budget: a replica resuming at a surviving anchor runs its windows on an
  already-grown pool, which produces no high-water sample, so if its
  earned doublings had to walk back up to the anchor first they never
  would — the budget would pin at the anchor and the ratchet's own 2×
  ceiling would be unreachable.
- **Shape ceiling** (run2 S1): a batch size the impl's own kernels have said
  they cannot execute at this corpus's shapes, learned from a
  `clamped.reason = "index_limit"` report. A pure `min` on the unit budget
  beside the knee, and the one brake that also stops the ramp **exponent** —
  a window trimmed back to the ceiling is no evidence about a bigger batch and
  never can be. It is not a memory condition and never deflates anything, and
  it is runtime-only: it depends on this corpus's padded dims and on the canvas
  the window was priced under, so it travels in no profile and a restart
  re-learns it. See "Shape ceiling: the third brake".

Worker, per batch within its window:

- Pack up to the grant's unit budget (bucketed for `max-times-count`);
  a batch is never smaller than one item — a single item over budget
  goes through anyway (the backstop catches it if it truly cannot run;
  Package 1 already decided batch-1 OOM = item fails, job continues).
  Bucketed packing reorders items; the worker restores input order
  before replying, since the dispatcher splits outputs back per request
  by position.
- **Defensive clamp, shrink-only**: before each batch, check live
  `mem_get_info` and pack smaller if the world moved; never exceed the
  grant. Freshness is therefore per-batch in the shrink direction and
  per-window in the grow direction — staleness can only *under*-size
  (memory freed mid-window is not seen until the next window's grant), a
  throughput nibble bounded by window depth, never a safety issue.
- **Measurement**: the fit runs orchestrator-side in **reserved**
  currency — that is what the driver (and therefore the budget) sees;
  allocator fragmentation and library workspaces make `allocated` a
  systematic underestimate, not scatter. But reserved is an **envelope,
  not a per-batch delta**: the caching allocator never returns blocks
  between batches, so once the pool covers the working set a repeat
  batch grows reserved by zero, and a delta series drags the fitted
  slope toward zero — over-admission, the exact failure this design
  exists to prevent. Only **high-water batches** — those that grow the
  pool: every geometric ramp step, and regrowth after `empty_cache()` —
  contribute reserved samples, regressing `peak_reserved −
  reserved_at_load` against batch units with a **free intercept**:
  `base` is process-level driver currency the allocator never saw, so
  forcing the fit through it (or through zero) would bias the slope low
  — admission uses the slope; the intercept is diagnostic only.
  Warm-pool batches contribute the allocated
  transient (`peak_allocated − allocated_before`, which has no caching
  hysteresis) as the diagnostic floor and validation series.
  `empty_cache()` events are therefore calibration opportunities, not
  just hygiene. Robust two-parameter fit; retain scatter (sample count,
  residual) as confidence.
- **A batch is only priceable when the impl ran the batch it was given.**
  Several shipped impls sub-batch inside `predict` — `run_with_oom_retry`
  with an `initial_chunk_size`, florence2's chunk of 1, easyOCR's per-image
  loop while `enable_batching = false` — so the peaks the harness measures
  can describe a fraction of the units it packed. Reporting the packed
  figure anyway biases the fitted slope low by exactly that fraction, and a
  low slope is over-admission, the failure this whole design exists to
  prevent. So the harness reports `units` only when the executed GPU batch
  matches the planned one, omits it otherwise (an unpriced measurement never
  reaches the fit), and treats an absorbed halving inside the impl as a
  negative sample. An impl whose own batching is switched off is not granted
  at all — it is `none`-class for calibration until re-enabled, which is
  another reason easyOCR's `enable_batching = false` stopgap has to go.
- **Reactive shrink**: grants shrink as external usage rises, but freeing
  our tensors is not enough to give memory *back* — the allocator pool
  holds it — so when the grant falls materially below the pool's
  **releasable slack** (`memory_reserved() − memory_allocated()`, the
  blocks no live tensor sits in, which is all an `empty_cache()` can
  return), call `empty_cache()` between batches. Hysteresis: e.g. the
  grant below 80% of that slack for 2 consecutive windows. Slack, not
  `memory_reserved()`: the grant is an *incremental* activation
  reservation while the pool includes the weights, so comparing the two
  compares different quantities and is true on any calibrated model
  essentially always — the trigger would fire every other window and tear
  down pools with nothing spare in them. Against slack the rule is
  self-limiting too, since a release leaves none. Exact thresholds:
  implementation detail, tune empirically.
- **Trim for idle residents**: the reactive-shrink path only runs in
  workers that are receiving windows — an idle resident gets no frames,
  so its retained pool would squeeze its neighbours indefinitely. When
  the ledger sees a hungry worker constrained by an idle resident's
  pool slack (`reserved − reserved_at_load`, the growth term of its
  footprint), the orchestrator sends that resident a trim
  request; the worker calls `empty_cache()` and replies with a fresh
  memory sample. **Idle means "has held no grant for a few seconds"**, not
  "holds none at this instant": one window is in flight per replica, so a
  replica draining a queue is grantless between every pair of windows, and
  trimming it there would cost it a re-`cudaMalloc` of a working set it is
  about to need again — thousands of times a minute. Trim is not unload:
  it releases only pool slack —
  weights, live tensors, and the CUDA context stay, so the model remains
  resident at a cost of milliseconds plus re-`cudaMalloc` as the pool
  regrows — whereas unload (item-8 eviction) frees `base` too at full
  reload cost. Trim when budgets are tight; evict when even the bases
  don't fit.
- **Backstop**: `run_with_oom_retry` unchanged. An OOM despite admission
  is recorded as a negative sample (prediction was wrong or the world
  moved) and deflates that worker's grants; N consecutive clean windows
  restore them — deflation must be recoverable, or one external spike
  degrades a worker until respawn. Deflation is runtime state,
  deliberately not persisted across restarts. It may shrink a worker below its
  seed, down to a single unit: the seed is where the ramp *starts*, not a promise
  to a worker that just OOMed; the real floor is at pack time (a batch is never
  smaller than one item).
- **Deflation is bounded and repaid by time as well as by windows** (run2
  change R4; findings F4 / Q2 / B8). Run1 measured the counter as an uncapped
  debt register: 108 levels on a shipped model in one phase, **8 074 levels in
  148 s** in another, repaying at 7.04 levels/s and so charging 15.6 minutes at
  0.43× throughput for a two-minute fault. Two rules fix it, and a third was
  already true:
  - **Cap** at `ceil(log2(max(anchor, seed))) + 1`. That many halvings already
    take the budget to a single unit, so every level past it changes nothing
    about admission while still having to be repaid before the budget can
    move. The one spare level preserves the difference between "as deflated as
    it can be" and "one more negative just arrived".
  - **Repay one level per 30 s of wall time**, in addition to the
    three-clean-windows rule. Clean windows can only repay while windows are
    flowing, and the expensive case is the one where they are not: the fault
    storm deflates the replica, the queue drains, and nothing is left to earn
    the halvings back. 30 s is the idle-resident trim's debounce — the interval
    at which the machinery that *relieves* a tight GPU can act — so a level
    survives one full relief cycle before it is handed back. Against the cap
    the worst case is bounded: an 11-level replica is whole again in five and a
    half minutes.
  - **Cleared on respawn**, which holds by construction: the counter lives on
    the ledger's per-replica entry and the manager builds a fresh one. The
    (model, GPU) ratchet anchor, which is not per replica, survives.
- **What counts as an out-of-memory condition at all** (run2 change R3, host
  half; finding Q1/B11). Before run2 the host deflated on any error text
  containing the words "out of memory", and run1 measured that firing 15 times
  on a GPU with 96 GB free, from a shipped impl that worded an unrelated
  failure as "the caption cache is out of memory slots". Two paths reach the
  deflation gate and each has its own rule:
  - **With a measurement**, the worker states *how* it classified the failure
    (`oom_class.source`, protocol doc "Memory sensing"). `typed_exception` and
    `marker` are structural — the interpreter named the condition, or our own
    marker did after naming it — and deflate on their own. `message_pattern`
    is a reading of prose, and it is **vetoed** by `free_mb_at_failure`: if
    the worker's live reading at the instant of the failure showed at least
    the whole MB envelope the grant priced that window at, then no batch size
    the host could have chosen was the problem and halving the budget would
    cost throughput and fix nothing. The veto only ever refuses a deflation —
    an absent reading, or a memory-blind grant, leaves the classification
    standing, because a *missed* out-of-memory leaves the ledger
    over-admitting against a model that has just proved it cannot take the
    size.
  - **Without one** (the error frame: a `predict` that failed with nothing
    measured), the host mirrors the worker's classifier exactly: the
    `INFERENCE_OOM_*` markers, the closed list of allocator and driver
    spellings that never say "out of memory", the `defaultcpuallocator` pair,
    and the words `out of memory` **plus a device-API token as a whole word**
    (`cuda`, `hip`, `rocm`, `nvml`, `xpu`, `sycl`). The bare substring is
    gone; the scoped rule is what keeps `CUDA driver error: out of memory` and
    CTranslate2's `CUDA failed with error out of memory` from being lost to a
    closed list. Every rule is matched per **line** of the failure's own
    message and traceback — never its stderr tail — because a traceback names
    `torch/cuda/__init__.py` in its frames and `/` is a word boundary.
- **A negative sample deflates and is then discarded.** It never enters the fit
  and never advances the ratchet anchor. Its `peak_reserved` is whatever the
  allocator managed before it gave up — an *under*-statement of the batch's real
  cost — so fitting it drags the slope down, which is over-admission produced by
  the very signal meant to prevent it; and anchoring on it would enshrine the
  failing batch size as the measured-clean floor the ramp resumes at, so
  deflation could never take hold.
- **WDDM synthetic negative sample**: on Windows the OOM signal is
  unreliable by construction — driver sysmem fallback (default on since
  ~536) lets an over-budget allocation succeed by spilling to system
  RAM, so over-admission usually manifests as a silent throughput
  collapse, never an exception, and the OOM path above would simply not
  fire. The worker already times every batch for knee capture; a
  pool-growing batch whose units/sec craters far below the fitted
  throughput curve is therefore recorded as a synthetic negative sample
  feeding the same deflation path. **Pre-fit the comparator is the
  previous ramp step**: a ×2 units step whose units/sec drops by the
  collapse ratio relative to the prior step is a spill — without this
  the ramp, the riskiest phase (especially under a wrong shipped
  profile), would be exactly the window the signal cannot cover. No new
  machinery — it reuses the timing and the deflation mechanism. The
  comparison is only valid **upward**: a batch is compared to the previous
  pool-growing batch only when it is an upward-or-equal step in units, so a
  window's small tail batch — inherently slower per unit, since fixed
  per-call overhead is amortized over less work — is never mistaken for a
  spill. A flagged batch does not become the new comparator (a persistent
  spill must not normalise itself), but the comparator ages out after a run
  of non-comparable batches so a stale reference cannot flag forever.
  Collapse threshold and that run length: implementation detail, tune
  empirically. Corollary at
  batch 1: a single over-budget item "goes through anyway" and on WDDM
  it does not fail as it would under Package 1's batch-1-OOM rule — it
  silently runs slow, once. Accepted: `slice_settings` bounds decoded
  pixels upstream, and it is one item, not a regime. Documentation (and later the Desktop tab)
  should additionally recommend the driver's "Prefer No Sysmem
  Fallback" setting (NVIDIA control panel, driver ≥ 546; not settable
  programmatically) — with it, Windows regains a crisp OOM signal and
  the synthetic path becomes the fallback for default-configured
  machines rather than the primary signal.

The only timing assumption left: external usage doesn't swing by more
than the margin within one window. The backstop covers the exceptions.

## Base measurement

`base` is the worker's whole-**process** device footprint, not its
allocator footprint: the CUDA context (~300–600 MB) and cuDNN/cuBLAS
workspaces reduce free memory but never appear in allocator statistics,
and the ledger (and item-8 eviction) count residents in driver currency.
Undercounting each resident by half a GB, times several residents, is
phantom headroom. Measurement is tiered:

1. **NVML per-process** (`nvmlDeviceGetComputeRunningProcesses`, own
   PID's `usedGpuMemory`) — exact and pollution-free; reliable on Linux
   including the CUDA Docker image under the nvidia runtime
   (`nvidia-ml-py`, pure-Python dependency). NVML reports *host* PIDs, so a
   container without `--pid=host` never finds itself in the list and
   degrades to tier 2 (logged once).
2. Where NVML reports N/A — **Windows WDDM** — or where our PID is not in
   its list, fall back to the free-memory delta around load, used only when
   it is ≥ the allocated-delta. Below that the allocated-delta wins, and
   what gets reported is then the allocated-delta **plus a fixed context
   estimate** — the same formula as the implausible-reading fallback below,
   since one `base_method` value cannot name two different quantities. It
   is a one-shot sample: a reading implausibly
   larger than the *reserved* delta plus a context/workspace allowance
   means another process moved during the load window → fall back to
   allocated + a fixed context estimate. (Pool overshoot inside the load
   itself is legitimate, which is why the plausibility test is against
   reserved rather than allocated.) The free reading must come from the
   same source (NVML or `mem_get_info`) on both sides of the window — the
   two disagree by GBs on Windows — and the tier applies only to a process
   that demonstrably allocated on the device; one that never did reports
   no base at all rather than a base of 0.
3. The `max_memory_allocated` delta around load is always recorded as the
   floor.

On ROCm the first tier is DRM fdinfo rather than NVML — the process's own
`drm-resident-vram` for the GPU it is pinned to, `base_method = "fdinfo"`,
with the same shape of plausibility floor underneath it — and the free/total
reading behind tiers 2/3 comes from amdgpu sysfs
(`free_source = "amdgpu-sysfs"`, authoritative like NVML's). Details and the
tier order in `docs/rocm-batch-calibration-parity.md` D4.

`base_method` is recorded in the profile as provenance. Cross-platform
contamination is impossible by construction: `platform` is in the profile
key, so Linux bases (exact, with Linux-sized contexts) never overlay
Windows entries, whose WDDM contexts are genuinely different sizes.

## Budget configuration

Per-server defaults with per-GPU-instance overrides, in the inferio side of
the server config. Two composable limits; when both are set the admission
budget is the `min`:

```toml
[inference_local.vram]
# margin over other processes' usage; the desktop lever, default on.
# usable = total − other_used × (1 + margin)
# margin = 0.10

# hard ceiling as a fraction of total VRAM; the server lever, default off.
# cap_fraction = 0.90

[inference_local.vram.gpu."GPU-1a2b3c4d-....."]   # gpu UUID, per instance
# margin = 0.25          # e.g. the card driving the monitors
```

Defaults live in serde defaults per the config-authoring rules; the TOML
ships commented examples only. `margin` defaults on (0.10) everywhere —
on a headless server other-usage is ~0 so it costs nothing; server
operators who partition VRAM among services set `cap_fraction` and are
encouraged to leave `margin` alone.

### The reserve, and why an unset margin is not the same as `margin = 0.10`

Run2 change R5 (findings P5-2 / T4). As a pure fraction of external usage the
margin inverts its own intent on a busy GPU: `limit = total − external ×
(1 + margin)` reaches 0 once external passes `total / (1 + margin)`, and run1
measured `limit_mb` of 2 813 at 10 GB free and **0** at 4 GB free on a 97 GB
card. The last ~9.8 GB of every GPU was unusable, and below that grants went
memory-blind (`mb = 0`), which is the state that admits batches priced against
nothing. The margin exists so a desktop user's own variable VRAM use does not
spill into ours; withholding ten gigabytes is not that.

So the config's `margin` is an **option**, and absence is a distinct state:

```
reserve = ceil(external × margin)                          # margin configured
reserve = min(ceil(external × margin), 1024 MiB)           # margin unset
limit   = min(total × cap_fraction, total − external − reserve)
```

- A margin the user wrote down is honoured **verbatim and uncapped**, exactly
  as before — `total − external − ceil(external × margin)` is
  `total − ceil(external × (1 + margin))` to the MiB, for integer `external`.
  It is a statement about their machine and the ledger has no standing to
  overrule it.
- An **unset** margin takes the default fraction *and* a 1 GiB ceiling on what
  it may withhold. 1 GiB is the size of the thing being protected against — a
  browser tab compositing, a game loading a shader cache, a second CUDA
  process's context — and the worker's own per-batch defensive clamp, which
  re-reads live free memory before every batch, is what actually catches a
  bigger move.

Keeping the two distinguishable is also what makes the default *changeable*
later without overriding somebody's deliberate `margin = 0.10`, per the
config-authoring rules.

Two consequences worth stating. The confidence widening (unconfirmed profile,
fit scatter) multiplies `external` the same way the base margin does, so under
the default rule it is capped along with it: on a GPU with tens of GB of
external usage an unconfirmed profile buys no *extra* reserve. That is the
user's stated rule ("at most 1 GB is ever withheld") and the widening was
never the main protection — the ramp and the extrapolation ratchet both count
local samples only, and neither is affected. And `/health` reports the reserve
actually applied (`reserve_mb`) and which rule produced it (`reserve_rule`:
`user_margin` | `capped_default`), as does every `issued a memory grant` log
line, so which arithmetic a GPU is under is never a guess.

## Calibration store

### File format

Human-readable TOML, one array-of-tables; the same file format for shipped
baselines and locally generated data. Every `*_mb` quantity in the store (and
on the worker wire) is **MiB** — mebibytes, 1024², the unit `nvidia-smi
--format=nounits` and torch's memory statistics both speak — never decimal
megabytes.

```toml
schema = 1

[[profile]]
inference_id = "clip/ViT-H-14-378-quickgelu_dfn5b"
epoch        = 1                       # from model metadata; stale-epoch entries are ignored
gpu          = "NVIDIA GeForce RTX 5090"
platform     = "windows"               # windows | linux | macos
backend      = "cuda"                  # accelerator extra (cuda | rocm | mps | cpu)
torch        = "2.7.1+cu128"
dtype        = "fp16"                  # load precision actually in use;
                                       # "unstated" when the impl negotiates
                                       # none and its weights could not be
                                       # read (a value, not an omission:
                                       # an absent key component makes the
                                       # whole entry unkeyable). Spelled
                                       # "unknown" before run2 (R11)
dtype_method = "inferred"              # selected | attribute | inferred |
                                       # unstated: how that precision was
                                       # arrived at. Run2 (R11), additive and
                                       # ignored by matching — the key is
                                       # `dtype` whichever method produced it
unit         = "item"                  # cost dimension in force when measured;
aggregation  = "count"                 # part of the key (see below)

base_mb           = 4321               # load footprint (process-level, see Base measurement)
base_method       = "nvml"             # nvml | fdinfo | free_delta | alloc_delta
                                       # (fdinfo = NVML tier 1's ROCm twin,
                                       # docs/rocm-batch-calibration-parity.md D4)
slope_mb_per_unit = 0.79               # marginal cost in MiB per unit, fitted
                                       # on reserved deltas (same field name and
                                       # currency as the wire `fit` snapshot)
knee_units        = 512                # optional: throughput stopped improving here
samples           = 38
residual_mb       = 96                 # fit scatter → confidence / safety margin
measured_at       = "2026-07-30T00:00:00Z"
generator         = "panoptikon 0.1.8" # provenance

# Local-store-only fields (ignored when read from a shipped baseline —
# they carry local authority a foreign measurement cannot):
max_units_measured = 1024              # ratchet anchor: largest locally
                                       # measured clean high-water batch
local_samples      = 12                # local clean samples; also the
                                       # non-local-profile confirmation gate
knee_clean_windows = 7                 # run2 (R1d): clean windows already run
                                       # at knee_units, with memory to spare,
                                       # towards re-widening it
```

Key tuple for lookup: `(inference_id, epoch, gpu, unit, aggregation,
platform, backend, torch, dtype)`.

`unit`/`aggregation` are in the key rather than being decoration:
`slope_mb_per_unit`, `knee_units`, `max_units_measured` and the sample ring
are all counts of *that* unit combined *that* way, so an entry measured
under another dimension prices a different quantity by a factor nobody can
compute. `epoch` remains the deliberate lever a maintainer bumps when a
model is reclassified; matching on the dimension is the backstop for when
they forget. A mismatched row is ignored, never deleted — the same
treatment a stale epoch gets — and the merge rule agrees with the match
rule, so two rows that cannot answer the same query never fold into one.

**Lookup is a fallback hierarchy, not an exact match** on the full tuple:
exact torch string → same torch `major.minor` ignoring the local version
tag (`backend` already encodes the CUDA/ROCm family) → no match. The full
string stays in the file as provenance; `epoch` remains the deliberate
invalidation lever. Without the hierarchy, every torch patch bump would
orphan the entire shipped-baseline set, and volunteers on different
patch versions would produce disjoint, never-matching entries.

**Any profile not generated locally — shipped baselines included, even
on an exact tuple match — is used with a widened effective margin until
a few local clean samples confirm it.** Driver version is deliberately
not in the key, and `base` is driver-currency, so a foreign measurement
is a good prior, never ground truth; fallback-matching is just the
least-confident case of this one rule. The cost is a conservative first
few windows on a fresh install (confirmation is a sample-count gate,
and local samples accrue on every ramp step), largely masked by the
ramp, which governs growth regardless (see the extrapolation ratchet).

### Layering and lifecycle

- **Shipped baselines**: `python/inferio/config/calibration/*.toml`,
  beside the model registry, mtime-reloaded the same way, **not**
  user-seeded (per the CLIP-FP16 lesson: `python/inferio/config/` is not a
  user-owned surface). Populated over time from maintainers' and
  volunteers' generated files.
- **Local store**: one generated TOML in inferio's data directory, written
  by the orchestrator, overlays shipped entries (local wins on identical
  key). Deleting an entry (by hand or from a future Desktop surface)
  triggers recalibration — passively, on the next run.
- **Write policy**: the orchestrator updates a local entry (via the
  atomic rewrite) whenever the ratchet anchor advances or the fit
  meaningfully changes — not per batch. This is what makes the ratchet
  and the confirmation gate survive restarts; runtime-only state
  (deflation, ramp position within a step, outstanding grants) is
  deliberately never persisted. Because a shipped baseline's `samples`
  are the *generator's*, local confirmation always reads
  `local_samples` — a shipped entry confirms only by accruing a local
  overlay entry.
- **Calibration is never frozen**: the fit keeps ingesting qualifying
  samples for as long as the model runs — high-water batches from
  ratchet range extensions and post-`empty_cache()` regrowth (shrink and
  trim events are calibration opportunities), warm-pool transients as
  the validation series, throughput samples for the knee. To make
  continuous refitting survive restarts, the local entry also persists a
  **bounded ring of recent high-water samples** (`(units, reserved_mb)`
  pairs; local-only like the ratchet fields, stripped on
  baseline import) — a robust fit cannot be resumed from aggregates
  alone, and ring eviction doubles as recency aging: samples from a
  since-changed driver or allocator fall out instead of anchoring the
  fit forever. Ring size: implementation detail, a few dozen.
- **Merge, never replace**: two identical cards in one host share a single
  profile key (the keyspace is the GPU *model name*) but carry separate
  runtime state, so a write from one must not overwrite the other's wholesale
  — they would ratchet each other's persisted anchor back and forth on every
  window. An update is merged into the entry it lands on: the two monotone
  quantities (`max_units_measured`, `local_samples`) take the maximum, the
  incoming fit is kept only when it carries one, and an absent knee leaves
  the stored one alone unless the explicit withdrawal signal is set. Finer
  per-GPU provenance inside one entry buys nothing while sharing between
  hosts is a file copy.
- **A winner with no fit borrows one**: the write policy deliberately stores a
  local entry with no fit fields while the fit in force came from a shipped
  baseline, and that entry outranks the baseline (it holds this machine's
  anchor, ring and confirmation count). A lookup therefore takes the fit from
  the highest-ranked candidate that has one and everything else from the
  winner, and reports whether the fit itself was local — a borrowed shipped
  slope stays foreign for the confirmation gate.
- **A read failure is not an answer**: the local store is rewritten wholesale,
  so a write from a half that could not be read would truncate it to whatever
  the process happens to hold. A transient failure (a sharing violation while
  a scanner holds the file, a bad sector) therefore leaves the half unread and
  retried, and a pending write that still cannot read it stays pending rather
  than either dropping the update or overwriting the file. A missing or
  corrupt file is a real answer of "nothing" and is overwritten normally.
- **Sharing**: shipped baselines accrete from maintainers' and
  volunteers' local stores by copying the file (it is one
  human-readable TOML; the local-only fields are stripped or ignored on
  import). No mechanism beyond that in v1; an "export calibration"
  affordance belongs on the future Desktop tab's list.
- **Invalidation**: `epoch` is declared in model metadata
  (`metadata.cost.epoch`, default 1) and bumped when an impl's memory
  behaviour changes *without moving any key component* — a new attention
  backend, a preprocessing change, swapped weights under the same
  inference ID. It is per-model (per-ID override) and reaches every user
  through the shipped registry on upgrade. Changes that *do* move a key
  component need no bump: a default-dtype flip (the CLIP FP32→FP16 case)
  re-keys lookups the moment negotiation lands on the new dtype, so all
  old-dtype entries — local and shipped — stop matching automatically;
  torch upgrades likewise via the `torch` key. Stale entries are ignored,
  not deleted.

### Model metadata additions

```toml
[group.clip.metadata.cost]
unit        = "item"
aggregation = "count"
epoch       = 1
seed_units  = 8        # first-touch batch on unknown hardware
```

`seed_units` takes over the *safety* role of `default_batch_size` (the
target role disappears — auto is the target). Per-ID override where an ID
deviates from its group (`dots_ocr`, `easyocr_*`, `qwen3-vl-embedding-*`
all deviate from their groups' dimensions and need per-ID `cost` blocks).

Run2 adds one optional key, `canvas_pixels` — the model's per-item pixel
ceiling, used only by `pixel`-priced models (see the taxonomy notes and
`docs/inferio-worker-protocol.md`, "Memory grants"):

```toml
[group.clip.inference_ids.nemotron-embed-vl-1b-v2.metadata.cost]
unit          = "pixel"
aggregation   = "sum"
seed_units    = 2000000
canvas_pixels = 1835008   # (6 tiles + thumbnail) x 512^2
```

Both scale-bound keys — `seed_units` and `canvas_pixels` — stop being
inherited the moment an ID redeclares `unit`; every other cost key is
scale-free and inherits key by key.

## Batch size UX: auto everywhere, the number becomes a cap

Auto is the only mode; what varies is whether a **cap** is present.
The current single number splits three ways:

1. **User cap** (`Option`, default `None` = no cap) — renamed
   "max batch size" in every UI surface. Request-scoped: core forwards it
   on each inference request; the dispatcher partitions windows by cap
   value and the worker enforces it at pack time as an item-count
   constraint (see the dispatcher section). Inferio stores nothing per
   user/core, so one server serves differently-capped jobs from several
   cores concurrently. Capping only lowers; there is no override above
   the calibrated/knee ceiling.
2. **Core in-flight sizing** — no longer user-facing. Core sizes requests
   by request-level concerns (payload bytes in flight — the existing
   byte-budget pipelining — and keeping the server fed), not by the cap
   and not by guessing GPU batches.
3. **Calibration seed** — inferio-side, and *not* a single resolution
   order, because the seed and the profile answer different questions.
   The seed batch size is `metadata.cost.seed_units`, falling back to the
   global conservative constant; a profile never supplies it. What a
   profile supplies is where the ramp *starts from*: a **local** profile's
   ratchet anchor is restored and acts as a floor on the budget (and on
   the ramp exponent), so the first window after a restart resumes at the
   largest batch this machine has actually measured rather than at the
   seed. A **shipped** profile deliberately does not — it prices the
   window (slope, `base`, the knee cap) but confers no growth, so a fresh
   install still ramps from the seed even with a baseline present.

Schema/plumbing (verified): `CronJob.batch_size`, model-config
`default_batch_size`, and job-request `batch_size` are already
`Option<i64>` — `None` = auto with no schema migration.

Migration and surface changes:

- One-time migration nulls **both** the stored last-used defaults
  (`default_batch_size` in per-model system config) **and** cron rows'
  `batch_size` to `None` (= auto). The two are not symmetric — a default
  is only "last selected" and changes nothing until a user manually runs
  a job, while nulling a cron row silently changes what runs unattended
  on the user's machine — but auto is the better setting for the vast
  majority even in cron, re-selecting auto by hand is awkward, and the
  user base is small; the rare intentional cap is re-entered once. Cron
  rows themselves are preserved — only the batch number is cleared.

  Mechanics (both values live in the same per-index-DB `config.toml` —
  `job_settings[].default_batch_size` and `cron_jobs[].batch_size` are
  `SystemConfig` fields — so this is one file rewrite per index DB):

  - **Hook**: a Rust post-migration step for index databases, running
    wherever SQL migrations already run — the startup sweep
    (`migrate_all_databases_on_disk`, which enumerates every index-DB
    directory including ones the user never opens) *and* the per-DB
    open/create path (`migrate_databases_on_disk`). Covering both paths
    is what makes the guard airtight for databases created at runtime
    *after* the upgrade: they get stamped at creation (when nulling a
    default config is a no-op — `migrate_path` already knows `fresh`),
    so a cap the user enters later can never be wiped by a delayed
    first sweep.
  - **Stamp**: a named-row table in the index schema (the
    `maintenance_state` pattern), created empty by a normal sqlx
    migration; the Rust step checks it, and inserts the row only after
    the TOML rewrite succeeds. Config-then-stamp ordering is the
    crash-safe direction: a crash between the two re-runs the null on
    the next startup, which is harmless because no user interaction can
    intervene before that restart completes.
  - **Rewrite**: `SystemConfigStore` load → null the two fields → save,
    which goes through the comment-preserving `TomlDocument`
    patch path and the atomic write. A DB directory with no
    `config.toml` is skipped, not seeded (nothing to null). One
    verification for implementation: `patch_serialized` must *remove*
    a key whose value went `Some → None` — TOML has no null.
- Cron model config must accept and persist "auto" (`None`) — it already
  can; the UI must offer it.
- The Desktop "new database" wizard drops its batch-size control entirely.
- Scan page: "Auto" default, cap as an advanced field whose tooltip fits
  one sentence ("never more than N items at once").

## Rollout order

1. Cost-dimension metadata + the ledger/grant loop: universal
   worker→GPU pinning at spawn, grants and fit snapshots on request
   frames, measurements and memory samples on response frames, tiered
   base on the load response, load reservations (the
   expected-base-vs-headroom *check* rides along; the eviction response
   waits for item 8), packing + defensive clamp in the worker,
   dispatcher cap-rule removal, local store round-trip through the
   orchestrator. easyOCR re-batching (bucketed `max-times-count`)
   **under realistic core pipelining** is the acceptance test.
2. Budget config (margin + cap, per-UUID overrides) and the reactive
   shrink path with `empty_cache()` hysteresis, plus the idle-resident
   trim message.
3. Core/UI: auto/cap rename, wizard removal, the stamped one-time
   migration (last-used defaults + cron rows → auto), cron "auto" in the
   UI.
4. Throughput-knee capture — fitted in units/sec (heterogeneous batches
   make items/sec noisy for `sum` models) with a minimum-sample gate
   before the knee may cap grants; shipped-baseline directory wiring
   (format exists from step 1; shipping actual baselines can lag).
5. Impl-time verifications flagged in the taxonomy table (moondream
   bounds, doctr recognition variance, qwen3 pixel cap, CLAP window).
   **Done**: every flag resolved against the impls, the table rows and the
   registry's `metadata.cost` comments now carry the code evidence.
   Reclassifications: moondream → `none` (+ `enable_batching = False` on
   both impls), tclip's qwen3 ids → `token`/`max-times-count`. Both carry
   `epoch = 2`, and the cost dimension is now part of the profile key, so
   any profile measured under the old classification is ignored by two
   independent mechanisms.

### Remaining for the easyOCR acceptance test (step 1)

Step 5 deliberately did **not** flip `enable_batching`: window depth under
real core pipelining is what decides whether bucketing holds, and no unit
test can assert it. On real hardware, in order:

1. Remove `config.enable_batching = false` from the three `easyocr_*` ids
   in `python/inferio/config/inference.toml`.
2. Run a full OCR job (easyOCR *and* docTR) over a realistically mixed
   corpus — thumbnails through 8000×6000 scans — with core pipelining as
   users get it, not a hand-fed window.
3. Confirm the worker's bucketed `max-times-count` packing keeps batches
   size-homogeneous (batch logs), that no OOM or WDDM throughput collapse
   is recorded, and that throughput beats today's per-image fallback.
4. Only then delete the stopgap comments in the registry and this note.

Item 8 of the parent doc (evict residents pre-load using recorded `base`)
falls out of step 1's footprint data plus the existing generation-guarded
unload machinery; its *trigger* (a load reservation exceeding headroom)
already ships in step 1 — the eviction response slots in after step 2. Item 9 (self-test) pre-warms
profiles using the same harness on synthetic inputs — after step 1 it is a
script, not a subsystem.

## Open questions

- Desktop margin default: 0.10 pending real-world feel; revisit after
  dogfooding on the two-5090 host (asymmetric monitor load is the test).
- Tuning constants bundled as "implementation detail, tune empirically":
  `empty_cache()` hysteresis (deflate ratio, consecutive-window count),
  the clean-window count N that restores a deflated grant, the window
  depth multiplier (2–4×), the widened-margin factor for
  non-local profiles, the `residual_mb` margin-widening clamp, the
  squeeze threshold that triggers an idle-resident trim, the
  throughput-collapse ratio behind the WDDM synthetic negative sample,
  the non-local-profile confirmation sample count, and the
  extrapolation-ratchet factor (default 2×).
- Placement policy on multi-GPU hosts: v1 pins every worker but keeps
  today's placement (the fastest GPU by compute capability, or the
  registry's explicit `devices` pins). Headroom-based placement — put the next load on the
  card whose ledger has the most room — is the natural follow-up once
  ledgers exist.
- ROCm: **resolved** — see `docs/rocm-batch-calibration-parity.md`
  (designed and implemented 2026-07-31). Parity is sysfs-first rather than
  SMI-based: KFD topology + amdgpu counters replace the nvidia-smi probe
  and the external-usage refresh, GPU identity is the PCI BDF (with a
  `GPU-<unique_id>`/`GPU-BDF-…` key), pins are HIP device indices, and the
  one unverifiable assumption — enumeration order — is cross-checked at
  worker registration, which degrades to unpriced dispatch rather than
  mis-pricing. No AMD hardware was available: a field pass is still owed.

  **It was not free on CUDA**, and the full accounting is in that document's
  "What this changed on CUDA hosts". The two an operator can *observe*: the
  capability tie-break gained a VRAM-descending rung, so default placement
  moves on an equal-capability, unequal-VRAM host (the unpinned model
  relocates to the bigger GPU and re-measures its base there — a placement
  change, not a change to how anything is keyed); and the registration
  single-GPU fallback is a **new admission path live on CUDA today**,
  admitting the shipped torch-2.7.1 worker that reports a total but no
  address, gated by the total-VRAM check. The rest are fixes (load
  reservations no longer miss on abbreviated-UUID pins; a resolved pin is
  canonicalised so the prewarm pool and the ledger agree about GPU
  equality), additive wire fields, and new log lines.
- ~~Per-item unit ceilings for `pixel`-class VLMs~~ — **done in run2 (R7)**,
  as `metadata.cost.canvas_pixels` clamped into the worker's `price_inputs`
  (see the taxonomy notes).
- Whisper stays out of v1; if CT2 footprint recording is ever wanted it
  needs an NVML-based path (no torch allocator) and is Linux-reliable
  only.
