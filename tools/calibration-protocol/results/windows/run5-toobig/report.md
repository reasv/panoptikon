# Run 5 (toobig) — verification of `fix/too-big-model` (9e018083) on the 5090 box

Host `A-DESKTOP`, Windows 11 (10.0.26200), 2 × RTX 5090 (sm_120, 32 607 MiB), driver 610.74,
torch 2.7.1+cu128, rustc 1.97.1. Clone `Q:\projects\panoptikon-pr27` fetched to **9e018083**
(`fix/too-big-model`) from a bundle; `cargo build --release -p panoptikon` **1 m 23 s**, one
pre-existing `dead_code` warning on `NofileRaise`. All four legs on **GPU 1**
(`server-C1win-g1.toml` + `env.C1win-g1`, ports 6382/6383/6379); the user's instance on
6339/6342/6343 untouched. 2026-09-16, 21:45–22:06 UTC.

Build note: the rustup shims in `C:\Users\moot\.cargo\bin` are **0 bytes** and `cargo.exe` cannot
be launched (`ResourceUnavailable`); `C:\Users\moot\.rustup\toolchains\stable-x86_64-pc-windows-msvc\bin\cargo.exe`
works. `rustup.exe` itself is fine.

## Legs

| leg | store | outcome | grants | memory-blind grants (mb=0) | OOM lines | unrunnable verdicts | job | items/s |
|---|---|---|---|---|---|---|---|---|
| `T1-qwen8b-S2-seeded` | `clip/qwen3-vl-embedding-8b` shipped sm_120 row re-keyed `platform="windows"` (base 31752) | **load refused** | 0 | – | **0** | – | `failed`, 0 completed, 0 items attempted, 4 s | – |
| `T2-qwen8b-S2-fresh` | empty (run4 W-A1 exactly) | **OOM storm, unchanged** | 1 999 | **1** | **8 002** | **0** | `failed`, 0 completed, 2 000 failed items, 690 s | 2.899 |
| `T3-wdvit-S2-seed` | run4 `seed-sm120-win.toml` | healthy | 6 | 0 | 0 | – | `completed` 2 000/2 000 | 54.07 (run4: 55.20) |
| `T5-vith14-S4a8-fresh` | empty, hog leaving 8 192 MiB free on GPU 1 | healthy | 43 | 0 | 0 | – | `completed` 2 000/2 000 | 103.2 (run4: 110.21) |

`items/s = total_segments / inference_time`, the run4 report's basis.

## T1 — the seeded case: the refusal works

```
ERROR panoptikon::inferio::http: failed to load model model=clip/qwen3-vl-embedding-8b
  error=failed to load model clip/qwen3-vl-embedding-8b: model clip/qwen3-vl-embedding-8b
  needs about 31752 MiB on GPU GPU-c77d0f94-a65c-d89c-7b73-16c446d242c4,
  which has room for 31159 MiB; not loading it
WARN  panoptikon::inferio::manager: load failed; refusing further loads of this model until
  the cooldown expires model=clip/qwen3-vl-embedding-8b failures=1 cooldown_secs=2.0
```

`/api/inference/health` at the end of the leg:

```json
{"inference_id": "clip/qwen3-vl-embedding-8b", "failures": 2,
 "last_error": "model clip/qwen3-vl-embedding-8b needs about 31752 MiB on GPU GPU-c77d0f94-...,
   which has room for 31159 MiB; not loading it",
 "retry_at": "2026-09-16T23:46:42.739525700+02:00", "retry_after_secs": 0, "window_secs": 4}
```

No worker was spawned (`health.models` empty), no OOM line, the job ended in 4 s against run4's
480 s cap. The cooldown ladder armed (2.0 s, then 4.0 s on the job's second attempt).

**The margin is thin.** `room_mb` here is `total − external − reserve` = `32607 − 1316 − 132`.
The refusal fires iff `31752 > 32607 − 1.1 × external`, i.e. iff **`external > 777 MiB`**. On an
idle GPU 1 (897 MiB of driver/desktop residue) it clears by 132 MiB; on a card with nothing at
all on it the same model would be admitted and behave as T2.

**The job record does not carry the sentence.** `jobs.json`:

```
"outcome": "failed", "failed": 1, "completed": 0, "failed_items": 0,
"failure_reason": "Failed to load model: model clip/qwen3-vl-embedding-8b failed to load on all
  1 inference endpoints: inference request failed (500): Failed to load model"
```

`panoptikon/src/inferio/http.rs:1025` and `:1130` return a fixed `"Failed to load model"` body,
so the base/room detail reaches the log and `/health` but never the job. (Pre-existing; `http.rs`
is untouched by this commit. The 503 cooldown response *does* carry `last_error`.)

## T2 — the unseeded case: rule (3) never fires

The load is attempted (no row, so no refusal — correct by design) and the worker is admitted with
`base_mb=Some(31150) base_method="free_delta" reserved_at_load_mb=Some(31202)`.

The pre-fit fix works exactly once:

```
unit_budget=1 mb=0 share_mb=0 room_mb=0 headroom_mb=0 external_mb=1317 pre_fit=true squeezed=true
```

(run4 had `unit_budget=2000000 mb=0` here.) Every one of the remaining **1 998** grants is priced
at 292–374 MiB:

```
unit_budget=1000000 mb=374 share_mb=374 room_mb=374 headroom_mb=0 deflation=1  squeezed=false
...
unit_budget=1       mb=292 share_mb=292 room_mb=292 headroom_mb=0 deflation=22 squeezed=false
```

Once the model is resident its own footprint is *ours*, so `external` drops to ~900 MiB and the
card reports a few hundred MiB of nominal share. `ledger.rs:4769` requires
`charge.mb == 0 && charge.unit_budget <= 1`; with `mb = 292` the first conjunct is false, so
`note_floor_oom_locked` never counts a window and `OOM_WINDOWS_AT_FLOOR` is unreachable.
`analyze.py`: *"1999 grants; … 1 were memory-blind (mb=0, B1)"* and *"1999 OOM negatives …
0 fatal worker deaths"*. 0 `cannot run a single item`, 0 `died fatally`, 0 cooldowns.

The job now ends `failed` with all 2 000 items failed rather than run4's `running` at the cap —
that improvement comes from the job-outcome work already merged in db4102f2, not from this commit.

Second-order: even if the rule had fired, the hand-off to the refusal would not hold here. The
measured base is **31 150** against **31 620** MiB of room, and the refusal needs `base > room`,
so the next request would spawn the same worker again.

## T3, T5 — no regression

`T3` (seeded wd-vit, S2): budget 2 → 64 → 128 → 256 → 512, peak 512 at 24 260 MiB, 0 OOM,
0 deflation, `grant_safety` PASS, `calibration_learned` PASS, 54.07 items/s against run4's 55.20.

`T5` (unseeded ViT-H-14 under a hog leaving 8 192 MiB, `floor_bound resolved_mb 8192`): first
window `unit_budget=2 mb=4796 pre_fit=true` — the documented pre-fit cap, `share.mb > 0`, so the
new one-unit rule does not bite — 43 grants, 0 memory-blind, 0 OOM, 0 collapse, job completed,
103.2 items/s against run4's 110.21.

## Files

`seed-qwen8b-win.toml` is the T1 seed (the shipped sm_120 `clip/qwen3-vl-embedding-8b` row with
`platform = "linux"` → `"windows"`, nothing else changed). Per leg: `legs.json`, `jobs.json`,
`verdicts.json`, `health-end.json`, `failures.json`, `calibration.after.toml` where one was
written, and `ledger-excerpt.log` (the grant/admission/refusal/cooldown lines).
`code/parent-probes.log` is the four new tests ported to the parent's API, failing at db4102f2.
