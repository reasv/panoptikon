# Run 5b (toobig, round 2) — `fix/too-big-model` at 73765a8f on the 5090 box

Round 1 (`verify/too-big-model`, `results/windows/run5-toobig`) verified 9e018083 and left five
defects. This round checks the four fixer commits on top of it — a075444d (floor predicate +
working set + refusal room + `refusable_base_mb`), 659a274e (`was_condemned` gates the cooldown),
fdbee4e8 (the 500 carries the load error), 73765a8f (design doc).

Host `A-DESKTOP`, Windows 11 (10.0.26200), 2 x RTX 5090 (sm_120, 32 607 MiB), driver 610.74,
torch 2.7.1+cu128. Clone `Q:\projects\panoptikon-pr27` fetched to 73765a8f from a bundle;
`cargo build --release -p panoptikon` 1 m 16 s, the one pre-existing `dead_code` warning on
`NofileRaise`. All legs on **GPU 1** (`server-C1win-g1.toml` + `env.C1win-g1`, ports
6382/6383/6379); the user's instance on 6339/6342/6343 untouched. 2026-09-16, 23:16-23:35 UTC.
The clone is back at 0d7f5671 and nothing is left running.

## Legs

`items/s = total_segments / inference_time`, the run4/run5 basis (jobs.json), **not** the
`throughput` line `analyze.py` prints, which is over a different denominator.

| leg | store | outcome | grants | blind (mb=0) | CUDA-OOM lines | verdicts | job | items/s |
|---|---|---|---|---|---|---|---|---|
| `T1-qwen8b-S2-seeded` | seeded sm_120 row, base 31752 | **load refused** | 0 | - | **0** | - | `failed`, 0 items, 4 s | - |
| `T2-qwen8b-S2-fresh` | empty | **condemned, then fatal** | 22 | 1 | **176** | 1 | `failed`, 22 failed items, 38 s | - |
| `T2b-qwen8b-cycles` | empty, 3 jobs in one server | condemned once, then **refused twice** | 22 | 1 | 200 | 1 | 3 x `failed` | - |
| `T3-wdvit-S2-seed` | run4 `seed-sm120-win.toml` | healthy | 6 | 0 | 0 | - | `completed` 2000/2000 | 51.69 |
| `T5-vith14-S4a8-fresh` | empty, hog leaves 8 192 MiB | healthy | 11 | 0 | 0 | - | `completed` 2000/2000 | 103.09 |
| `T5b` (repeat of T5) | as T5 | healthy | 13 | 0 | 0 | - | `completed` | 103.40 |
| `T5c` (T5 on **9e018083**) | as T5 | healthy | 45 | 0 | 0 | - | `completed` | 104.76 |

Round 1's T2 for comparison: 1 999 grants, 8 002 OOM lines, 2 000 failed items, 690 s, 0 verdicts.

## T1 - seeded refusal, and the sentence now reaches the job

```
ERROR panoptikon::inferio::http: failed to load model ... error=failed to load model
  clip/qwen3-vl-embedding-8b: model clip/qwen3-vl-embedding-8b needs about 31752 MiB on GPU
  GPU-c77d0f94-..., which has room for 31047 MiB; not loading it
```

external 1418 -> limit 31047; 31752 > 31047 refuses. Cooldown 2.0 s then 4.0 s, `/health`
`failures=2`, `last_error` = that sentence. `jobs.json.failure_reason` now:

```
Inference is unavailable: clip/qwen3-vl-embedding-8b is in a load-failure cooldown after 2
consecutive load failures; retry at ...; last error: model ... needs about 31752 MiB ... which
has room for 31047 MiB; not loading it
```

(Round 1: `"... inference request failed (500): Failed to load model"`.) 0 OOM lines, 0 items.

## T2 - the unseeded case: rule (3) fires

Load attempted, `base_mb=Some(31595) base_method="alloc_delta" reserved_at_load_mb=Some(31202)`.
One memory-blind window, then 21 priced one-item windows at `mb=403 room_mb=403` while the
deflation ladder walks `unit_budget` 1 000 000 -> 1 (deflation 1...19). The rule counts a window
only when `unit_budget <= 1`, and an out-of-memory window never *clears* the count, so the three
that condemned are the blind first window (room 0) and the last two (deflation 20 and 21) - 22
windows apart:

```
DEBUG ... issued a memory grant ... unit_budget=1 mb=403 room_mb=403 headroom_mb=15 deflation=20
WARN  panoptikon::inferio::ledger: this model cannot run a single item on this GPU; failing it
  instead of dispatching to it again model=clip/qwen3-vl-embedding-8b gpu=GPU-c77d0f94-...
  base_mb=31595 room_mb=32071 windows=3
WARN  panoptikon::inferio::manager: worker died fatally; dropping model from all caches
  model=clip/qwen3-vl-embedding-8b cooldown_secs=2.0
```

`/health.load_cooldowns`: `failures=1`, `window_secs=2`, `last_error` =

```
model clip/qwen3-vl-embedding-8b ran out of memory on GPU GPU-c77d0f94-... at a one-item batch 3
windows running: its base is 31595 MiB of the 32071 MiB this GPU can lend, and one item on top
of it did not fit
```

and `jobs.json.failure_reason` carries the same sentence inside the cooldown body. The queue
fails **once**: 22 items attempted, 22 failed, 176 CUDA-OOM lines against round 1's 8 002.

Cost of the fix's shape: the condemnation cannot fire before the deflation ladder has walked the
unit budget down to 1, which took **21 windows / 19 s** here. That is the 176 OOM lines.

## T2b - two full cycles: does it loop?

Three extraction jobs against one server, 20 s apart, nothing else on GPU 1.

| cycle | what happened | numbers |
|---|---|---|
| 1 | load admitted, condemned after 3 one-item windows | base **31238** (`free_delta`), failing window `room_mb=286`, limit 31654; cooldown armed **2 s** with the verdict sentence |
| 2 | reload **refused by size** | `needs about 31525 MiB ... room for 31491 MiB`; failures 2 then 3, cooldown 4 s -> **8 s** |
| 3 | refused again | `needs about 31525 ... room for 31474`; failures 4, cooldown **16 s** |

The remembered working set is exactly `base + room_at_failure + 1` = `31238 + 287` = **31525**,
and it held: the card was free (595-611 MiB by `nvidia-smi`, 1 014-1 030 MiB as the ledger's
`external`) and the reload was still refused. Job 2's `failure_reason` is the **new 500 body**:

```
Failed to load model: model clip/qwen3-vl-embedding-8b failed to load on all 1 inference
endpoints: inference request failed (500): Failed to load model: failed to load model
clip/qwen3-vl-embedding-8b: model ... needs about 31525 MiB ... which has room for 31491 MiB;
not loading it
```

**The margin is 34 MiB.** `room = 32607 - 1.1 x external`, so the refusal holds only while
`external >= 984 MiB`. GPU 1's idle residue moved between 595 and 1 030 MiB inside this session,
and in the `T2-qwen8b-S2-fresh` leg the same model measured base 31595 with a failing-window room
of 403 -> a remembered working set of **31999** against a limit of **32071**, i.e. it would have
been re-admitted. Whether the second cycle refuses or re-loads is decided by tens of MiB of
desktop noise, not by the model. See W1 below.

## T3, T5 - controls unchanged, and the throughput A/B

`T3` seeded wd-vit: 6 grants, ramp 128 -> 512, 0 OOM, 0 deflation, `grant_safety` PASS,
`calibration_learned` PASS, `job_outcome` PASS, 51.69 items/s (round 1 54.07, run4 55.20).

`T5` ViT-H-14 under a hog at `leave-free 8192`: 11 grants, ramp 4 -> 480, 0 memory-blind, 0 OOM,
0 collapse, `completed`, 103.09 items/s (round 1 103.2, run4 110.21). `ledger_invariant` WARN
(7 `limit_fell` samples, 0 `over_grant`) is the hog moving the limit under a held footprint, as
in run4.

`analyze.py`'s own `throughput` line reads 69.0/71.4 items/s for T5/T5b against 103 on the
jobs.json basis, which looked like a regression. It is not: **T5c re-ran the same leg on round
1's binary (9e018083)** and gives 71.4 / 104.76 on the two bases. Both legs and both bases agree
between the two binaries. The grant counts do differ (11-13 here, 45 on 9e018083, 43 in round 1)
at identical throughput - bigger windows, same work; nothing in the four commits touches pricing,
so this is window/queue timing.

## Round-2 findings

- **W1 (concern, not a defect).** The remembered working set is `base + the failing window's
  room + 1`, and **pre-fit** the comparand for "one item does not fit" is the model's whole
  `base` (`one_unit_appetite_mb_locked`'s no-slope fallback). A replica can therefore be
  condemned with tens of GB of room in hand, and what is remembered is then nearly twice the
  base. `the_remembered_working_set_climbs_until_it_refuses` (this round's probe): base 60 000 on
  a 100 000 card, 30 000 of room, condemned -> remembered 88 501 -> the emptied card admits the
  reload -> condemned again with more room -> 100 001 -> refused for the life of the process.
  The sequence converges upward in **two cycles**, each costing a full deflation ladder plus
  three OOM windows; the end state is "needs = this card's whole limit + 1", never cleared and
  never lowered (`remembered_working_sets` has no removal path). The same arithmetic is what
  makes the T2b margin 34 MiB. Bounding the practical risk: a false condemnation needs three
  counted one-item out-of-memory windows with no clean window between, and run4's whole pressure
  set (S4a, S4b, S4c, S4d) recorded **0 OOM negatives**, so nothing in the suite comes near it.
  The doc's "that bound is measured, so a card that frees up later clears it" is true for one
  more attempt only; after the second condemnation the bound is the card itself.
- **W2 (concern).** `was_condemned` is keyed on the **model**, over every GPU, and is never
  cleared. After one condemnation anywhere, every later fatal death of that id - a python crash,
  a poison request, a death on another card - is treated as a costed load failure and arms the
  ladder with that unrelated death's sentence
  (`a_condemnation_elsewhere_arms_the_cooldown_for_an_unrelated_death`, this round's probe).
- **W3 (concern).** The ladder escalates only while nothing succeeds. A cycle that re-loads
  successfully hits `state.cooldowns.clear(inference_id)` (manager.rs:1720), so a
  condemn -> reload -> condemn loop re-arms at 2 s every time. In T2b the ladder did escalate
  (2 -> 4 -> 8 -> 16 s) only because the reload was refused rather than admitted.
- **W4 (minor).** The new 500 body is the whole `anyhow` chain (truncated to `MAX_ERROR_BYTES`).
  For the refusal that is exactly the sentence; for other load failures it is whatever the worker
  said, which can carry absolute paths and tracebacks into a user-visible `failure_reason`.

## Gates

`cargo test -p panoptikon` on the branch: **1705 passed, 0 failed, 10 ignored** (580.9 s); with
this round's two probes added, **1707 passed, 0 failed** (573.2 s). `cargo fmt --all -- --check`:
clean for `panoptikon/` (the one `panoptikon-desktop/src-tauri/src/lib.rs` diff is pre-existing —
it is also there at db4102f2, which additionally has `ledger.rs` and `manager.rs` diffs that this
branch does not). `cargo clippy -p panoptikon --all-targets`: 8 warnings, all
`chunks_exact_to_as_chunks` in `media_tools/outro.rs`, none in any file this branch touches.
