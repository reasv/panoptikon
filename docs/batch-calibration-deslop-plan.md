# Deslopping the batch-calibration branch, then finishing run2

Written 2026-09-04 after the user reviewed the PR size: +86 051 lines on
`claude/batch-calibration-coverage-db9ab9` against master, of which only
about 19 000 are product code (Rust 12 474, Python 6 572); the rest is
tests (30 500), comments (13 000), docs (11 400) and protocol tooling
(11 000). The user's instructions, verbatim in substance:

1. Make comments concise; remove them where redundant; fold very long
   comments that survive into markdown documentation (new or existing).
2. Then assess whether the code itself is bloated or verbose. "Shorten
   the code" is too dangerous an objective to hand an agent directly, so
   the assessment must come first and the edits must be targeted.
3. The remaining steps of the product task come after this phase.

Rules that apply throughout: no behaviour change in phase 1; every step
has a separate verifier; the full suites are the check (`cargo test -p
panoptikon` with the one known host artefact, `pytest tests` in
`python/`, `cargo fmt --check`, `cargo clippy --all-targets` with no new
warnings, `cargo test --bin panoptikon openapi` and the `ui` types
byte-identical unless a schema changes); explicit-path staging; commit
subjects ≤80 chars, no trailers; never push without being asked; never
touch the GPUs (the user's SGLang is up) or `~/docker`.

## Phase 1 — vocabulary and comments (no behaviour change)

### 1a. Rename "board" → "gpu" / "device"

"Board" was the design doc's invented word for the GPU a model is
admitted to (chosen because the ledger also prices APU/MPS/CPU-RAM
pools). Nobody reads it that way. Counts at the tip: 234 distinct
identifiers (1 378 uses) in Rust and the Python worker, 3 339 prose
uses; no wire, store, config or API key uses it (the only `"…board…"`
strings are the unrelated `pinboard` feature and one NVML log reason).

- Identifiers: `board` → `gpu` everywhere the thing is a discrete card
  (nearly everywhere: `board_key` → `gpu_key`, `resolve_board_key`,
  `BoardLog`, `UNRESOLVED_BOARD_ADMISSION_KEY`, per-board maps in the
  ledger/manager, health struct fields that are internal). Use `device`
  only where the code genuinely covers the unified-memory and CPU-RAM
  pools as well, and define once, in the ledger module doc and the
  design doc's first section: a device is the memory pool a model is
  admitted to; on a CUDA/ROCm host that is one GPU.
- Prose (comments, docs, tooling messages): "board" → "GPU" or "device"
  by the same rule. `analyze.py`/`healthrec.py` parse log fields by
  name: any renamed log field must be renamed in the parsers in the
  same commit, and `results/run1`/`run2` stay readable (the parsers
  accept both spellings for the old recordings, or the old recordings
  are re-analysed only with the old tool — decide and record).
- Health/OpenAPI: a renamed public field (`GpuBudgetHealth` is already
  "gpu"; check `LedgerWorkerHealth`, `/health` per-replica fields, the
  `nvml_board_unidentified` reason) changes `openapi.json` and the `ui`
  types; regenerate both in the same commit.
- One mechanical commit per crate/package, then a sweep commit for
  docs and tooling. Verifier: grep for the leftover word, suites green,
  `analyze.py` reproduces `verdicts.json` for two run2 legs.

### 1b. Comment concision

Target: Rust comment lines from 11 375 added (whole PR) down by 6 000 to
8 000; Python comments (1 700 added) proportionally; the same rules for
the tooling. What stays in a comment: what the item is or does, the
invariant it protects, the non-obvious constraint (a lock order, a
unit, a sentinel), and a pointer to the doc section that argues it.
What goes: derivations, measurements, history ("run1 found…",
"before run2…", commit hashes), restated brief text, arguments for the
chosen constant, duplicated statements of the same rule at several
sites, and any paragraph that reads as an essay. Constants keep one
line saying what they bound and a doc pointer; the derivation moves.

Where the long ones go, by subject:
- Ledger arithmetic, knee vetoes and expiry, deflation, reserve, shape
  ceiling, occupancy → `docs/batch-calibration-design.md` (extend the
  existing sections; it already argues most of it, so mostly delete).
- Manager locks and the cooldown ladder → `docs/inferio-worker-protocol.md`
  lifecycle section (already has the bullets; delete the module essay
  down to the lock order list and the no-deadlock argument, which stay).
- Client pool/gate/transport classification, the h2 stream limit, the
  body budget, the multipart buffering → new `docs/inferio-client-transport.md`
  (short: the constants, their derivation, the failure kinds table).
- Job re-queue, partial outcome, failures audit, unit budget deficit →
  `docs/failed-media-retry-design.md` (exists) and the jobs section of
  the design doc.
- Worker: OOM classification tiers, canvas, index ceiling, context
  probe → `docs/inferio-worker-protocol.md` (the wire sections exist).
- Tooling: `codemap.md` keeps pointers only; the check descriptions in
  `analyze.py` docstrings shrink to one paragraph each, the rest in
  `tools/calibration-protocol/README.md`.

Method: one Opus agent per large file (`ledger.rs`, `inferio_client.rs`,
`http.rs`, `manager.rs`, `jobs/extraction.rs`, `dispatch.rs`,
`worker.rs`, `calibration.rs`, `policy.rs`, `packing.py`, `memory.py`,
`eocr.py`, `analyze.py`), each with a line budget and the "where it
goes" table; the agent writes the folded prose into the named doc in
the same commit (so nothing is lost between commits); one verifier per
group diffs the non-comment tokens before/after (a script that strips
comments and compares) to prove no code changed, then runs the suites.
Report the before/after comment-line counts per file.

### 1c. Test concision

Added tests: 23 174 Rust inline + 7 398 Python. Rules: merge near-
duplicate per-rule tests into table-driven ones; delete tests whose
only assertion is a log string or a `Display` text (keep the ones that
pin a wire field, a status code, a store field, or a numeric rule);
keep every regression test that failed before a fix (they are listed
in the fix reports); keep the replay fixtures. Target 7 000 to 9 000
lines. Verifier: the set of remaining test names is a documented
mapping from the old set (merged/deleted/kept with a reason); suites
green; the fix-round regression tests listed by name in the verifier's
report.

### 1d. Documentation concision

`docs/batch-calibration-run1-report.md` (1 160 lines) and
`…-run2-report.md` (~2 500) keep verdict tables, per-scenario numbers,
findings and decisions tables, release notes, host restore and the
commit appendix; narrative and repeated evidence paths go. The plan
(`…-test-protocol.md`) keeps §0 status blocks but each becomes a
table. Target about 4 000 lines out. `codemap.md` unchanged.

Order: 1a, then 1b and 1c in parallel by file ownership, then 1d, then
one final integration pass (suites, clippy, openapi/ui, codemap
re-resolve, size table before/after committed into this file).

## Phase 1 result

Measured 2026-09-05 by the 1b/1c integration verifier, at the merge of the ten
concision branches (`63a8ad73`) plus this pass's three commits. All figures are
**added lines against the merge-base `7aa92b20`** (`git diff --numstat`, then
classified per file: a Rust line is "comment" if its first token is `//`, and
"test" if it is at or below the file's `#[cfg(test)] mod tests`; a Python line
is "comment" if it starts with `#` or is inside a module/class/function
docstring, and "test" if the file is under a `tests/` directory).

| added lines vs `7aa92b20` | `7de4cf99` before deslop | `64e1c405` after the rename | now, after 1b/1c | delta over 1b/1c |
|---|---:|---:|---:|---:|
| Rust code | 12 713 | 12 715 | 12 709 | −6 |
| Rust comments | 11 136 | 11 143 | **5 797** | **−5 346 (−48 %)** |
| Rust tests | 21 923 | 21 892 | **18 861** | **−3 031 (−14 %)** |
| Python code | 2 563 | 2 563 | 2 554 | −9 |
| Python comments | 2 800 | 2 800 | **1 056** | **−1 744 (−62 %)** |
| Python tests | 6 169 | 6 169 | **4 713** | **−1 456 (−24 %)** |
| tooling code | 5 146 | 5 151 | 5 151 | 0 |
| tooling comments | 1 545 | 1 554 | **805** | **−749 (−48 %)** |
| docs (`*.md`) | 12 548 | 12 562 | 13 612 | **+1 050** (the folded prose) |
| generated (`openapi.json`) | 1 249 | 1 249 | 1 266 | +17 |
| everything else | 3 503 | 3 503 | 3 503 | 0 |
| **`git diff --shortstat` insertions** | **86 255** | **86 264** | **74 200** | **−12 064** |

The −6 / −9 in the two code rows is a counting artefact, not an edit: the
classifier takes the *first* `#[cfg(test)]` block as the test boundary, and
`worker.rs`'s `pub(super) mod testing` sits above its `mod tests`, so a few
test-support lines change side. Production tokens are byte-identical: the
verifier ran `strip_compare.py` on every merged commit (0 FAIL) and the suites
are green.

Against the targets: §1b asked for Rust comments down by 6 000, and they came
down by 5 346 — 90 % of it, with the shortfall argued per group below. §1c
asked for 7 000 to 9 000 test lines out and 4 487 came out; the binding
constraint was the keep list (266 named regression tests, every one still
present or merged with its case intact) plus the "do not weaken an assertion"
rule, which together floor most files well above their ceiling.

After §1d (merge `df30c394`): `docs/batch-calibration-run1-report.md`
1 169 → 863  `…-run2-report.md` 2 113 → 1 383  `…-test-protocol.md`
2 055 → 1 329  the fixtures README 138 → 133 (2 501 lines out; every
verdict  finding  decision  release-note  host-restore and commit-appendix
row kept  every fenced command byte-identical  the BLOCKED markers intact).
The two report ceilings (600 / 1 200) were missed because the protected
tables alone are ~1 000 lines each. Added docs lines are now 11 996 
whole-PR insertions **72594** (from 86 255 before the deslop).

### What overshot its budget, by group

- **A (`ledger.rs`).** Comments 3 602 → 1 912 against 1 300: the production
  region holds 544 distinct comment blocks, and the rule that every `pub` item
  keeps a `///` line plus the invariant it protects is already ~1 100 lines at
  two lines a block, with ~90 invariant-bearing blocks needing four to six.
  Tests 8 490 → 7 771 against 5 200: 87 of the 168 survivors are named
  regression tests and the rest each pin a wire, store, `/health` or numeric
  rule, so the remaining ~35 lines per test is ledger setup that *is* the
  behaviour under test.
- **B (gpu/rocm/cpu/mps/cost/capability/accelerator_env).** Comments met every
  ceiling; the residue in the smaller files is `SAFETY` notes on `unsafe`
  blocks, `#[cfg]`-arm notes and one-line `pub`-field docs, all of which the
  rules keep. `rocm.rs` tests 951 → 698 against 620: the cases are
  fixture-driven filesystem probes, so each costs a `Fixture` setup that no
  table collapses; two shared builders folded the repeated node/render/PCI/GTT
  chains and no case was dropped.
- **C (manager/prewarm/registry/config/main/setup).** Every comment budget was
  met (manager.rs 1 002 → 418). `manager.rs` tests 2 568 → 1 787 against 1 700
  (+5 %): the mechanical saving is spent and what is left is one test per
  behaviour, each with real subprocess setup, so the next 90 lines would come
  out of coverage.
- **D (worker/calibration).** Both comment ceilings are ~15 % over.
  `worker.rs` 1 008 → 517 against 450 because 212 of the survivors are
  master-era comments the brief says not to trim — the branch-added share fell
  761 → 305 (−60 %). `calibration.rs` 556 → 259 against 220 is 51 documented
  items over 834 code lines; the next cut would have removed the sentence that
  states the invariant rather than the field.
- **E (dispatch/http).** `dispatch.rs` met both budgets exactly (comments 770 →
  320, tests 1 562 → 1 000). `http.rs` tests 1 739 → 1 302 against 1 150:
  every remaining test in the file is on the regression list, so nothing could
  be deleted, and the residue is fixture setup plus assertions on wire fields
  and status codes — the categories the brief keeps. Comment lines inside that
  test region went 384 → 158.
- **F (inferio_client/policy/rlimit/process_tree).** Comments met every budget
  (client 746 → 300). Client tests 1 346 → 851 against 850 — one line, a
  closing brace `rustfmt` will not join.
- **G (extraction, db/*, api/*).** `extraction.rs` tests 2 269 → 1 922 against
  1 700: nine regression tests had to keep their cases and the rest are
  migrated-database integration tests each needing its own fixture; `rustfmt`
  also explodes any array element over 60 characters, so table-driven forms
  had to be written as closure calls rather than wide tuples. `batch_auto.rs`
  257 vs 220 and `job_failures.rs` 169 vs 150 are fixture text
  (`CONFIG_WITH_CAPS`, the `data_log` rows) plus per-rule tests that cannot
  merge without losing a rule; `extraction_write.rs` comments are 43 vs 40.
- **H1 (`memory.py` + `test_memory.py`).** No overshoot: comments 1 407 → 500
  against 500, tests 2 750 → 1 749 against 1 750, 109 → 66 tests.
- **H2 (packing/eocr/utils/`__main__` + tests).** Comments met every budget
  (packing 702 → 279, eocr 391 → 179). `test_packing.py` 1 587 → 1 328 against
  1 000: the file covers eleven distinct mechanisms end to end and is 56 tests
  over ~1 195 lines of bodies plus ~130 of fakes — about 21 lines a test, most
  of it `run_window(...)` setup and wire-map assertions. Reaching 1 000 would
  have meant deleting assertions.
- **I (`tools/calibration-protocol`).** No overshoot: comments 1 533 → 805,
  every file at or under its ceiling (`ceiling_probe.py` 231 → 98 against 100),
  and `analyze.py` reproduces its verdicts byte-for-byte on four recorded run2
  legs.

### Phase 2 candidates the concision pass noticed but did not touch

- `ledger.rs` `tracing` message strings still carry run/change labels (`run2
  change R5`, `P5-5`, `finding Q1/B11`); `analyze.py` parses those lines, so
  rewording them is a behaviour change.
- `ledger.rs` is still 14 362 lines; the obvious split is its 7 771-line test
  module into `ledger/tests/`, a code move rather than a concision change.
- `capability.rs::output_with_timeout` returns `join().ok()?` in one arm, so a
  panicking drain thread silently turns a successful probe into `None`.
- `cpu.rs::ram_total_mb` / `ram_available_mb` take `roots` and then
  `let _ = roots;` on three of four platforms; a `#[cfg]`-split pair would be
  shorter.
- `GpuInfo::total_mb` is the only `pub` field in that struct with no `///`, so
  its OpenAPI schema entry has no description.
- `docs/inferio-client-transport.md`, named in §1b, was created under the name
  `docs/inferio-transport.md`; this file's §1b table still says the old name.
- `inferio::calibration::tests::writes_are_debounced_and_flushed_on_demand`
  races a 100 ms sleep against a 10 s debounce and flaked once under load;
  a paused tokio clock or an assertion on the store's `pending` flag would
  make it deterministic.
- `ui::tests::resolve_node_order` failed once in a full-suite run and passed
  alone and on the clean re-run; unrelated to this branch, worth a look if it
  recurs.
- `http.rs` and `worker.rs` had byte-identical `test_spawn_config` /
  `workspace_root` / `test_venv_python` helpers; http.rs now uses worker's, but
  other modules may still carry copies.
- `api/desktop.rs` has three `#[cfg(test)] mod <name>_tests` blocks and
  `db/extraction_write.rs` two, which `linecount.py` and `strip_compare.py`
  treat as production — any later agent editing those test bodies must know.
- `jobs/queue.rs` still carries 369 comment lines and 1 701 test lines; it was
  out of scope beyond plainly redundant lines.
- `memory.py`'s `# --- Base measurement ---` banner sits above the accelerator
  context section rather than above `_resolve_base`, ~300 lines further down.
- `memory.py::_free_total_mb` keeps two unreachable "belt and braces" early
  returns, and `fdinfo_own_vram_mb` / `amdgpu_free_total_mb` keep a guard and a
  `max(..., 0)` that are each redundant with a check below.
- `eocr.py:599` `unload()` → `clear_cache()` raises `ImportError:
  sys.meta_path is None` from `InferenceModel.__del__` at interpreter shutdown
  — printed as "Exception ignored in", but `__del__` calling into an importing
  helper is a real latent bug.
- `tests/inferio/impl/test_eocr_canvas.py` and `test_eocr_index_ceiling.py`
  duplicate ~60 lines of fixture; a shared `conftest.py` would remove it.
- `tools/calibration-protocol/fixtures/README.md` still carries history-shaped
  prose and a stale `ledger.rs:1821` pointer; `config/README.md` there was
  never read for slop. Both are 1d items.
- `README.md`'s `healthrec.py` section explains the `boards` → `vram` rename as
  history; it is load-bearing for reading `results/run1` but could become a
  one-line compatibility note in 1d.
- `manager.rs`'s module doc still says "Deviations from the Python semantics
  are noted inline"; after the pass there is no inline Python note left in the
  file, and the four deviations now live in
  `docs/inferio-worker-protocol.md` "Lifecycle and timeouts (orchestrator
  side)". The sentence should point there.

## Phase 2 — code bloat: assess first, then targeted edits

"Shorten the code" is not an objective any agent gets. Phase 2 is an
assessment that produces a candidate list for the user to choose from,
and only the chosen items are edited, each with a verifier.

Objective signals to gather (scripts, no judgement):
- Size by module before run1 (`7aa92b20`), at the run1 end
  (`0d6b36c5`) and now, code lines only.
- Functions over 80 lines and files over 3 000 lines (`ledger.rs` is
  16 800 with 9 000 of tests).
- Test-only hooks in production code: `*_for_test`, `#[cfg(test)]`
  items outside `mod tests`, `pub(crate)` surface used only by tests.
- Helpers with exactly one caller; wrapper functions that only rename.
- Duplicated blocks (a clone detector such as `jscpd` over `src/` and
  `python/`, or `cargo clippy -W clippy::cognitive_complexity` and
  `-W clippy::too_many_lines`).
- Defensive branches the tests prove unreachable (coverage with
  `cargo llvm-cov` if available; else the agent lists them by reading).
- Plumbing that exists only to log or to expose a health field.
- Dead code and unused dependencies (`cargo machete`/`udeps`,
  `vulture` for Python).

Then one Opus reviewer per large file writes a candidate table:
location, what it is, category from the list above, lines it would
save, the risk (what could change), and a recommendation. The
orchestrator merges the tables into one ranked list in this file. The
user picks. Each chosen item is one commit by one agent with a
verifier that proves behaviour is unchanged (suites, and where the
item touches a measured mechanism, the relevant recorded leg replays:
the knee replay tests, `analyze.py` on run2 legs). Nothing outside the
chosen list is touched.

## Phase 2 result — the ranked candidate list (user picks)

Four read-only reviewers (ledger/calibration/cost; gpu/manager/worker;
dispatch/http/client/jobs; Python worker and tooling) assessed every file the
branch touched against the objective signals (long functions, single-caller
helpers, test hooks in production, clones, dead code, clippy length and
complexity lints). Their full tables (130 rows with evidence per row) are in
the session scratchpad (`deslop-2-candidates-{R1,R2,R3,P1}.md`). The
headline: **the code is dense, not bloated**. Every function over 100 lines
was judged a *move* if split (the intermediate struct costs what the body
saves, and several carry ordering risk: grant settlement, the lease-before-
sleep order, the settle/request_grant sequence). What is genuinely removable
is duplication: about **600 lines**, of which ~90 are test-only. Nothing
below touches a wire field, a store column, a `/health` field, a config key,
a log line the analyzer parses, or a regression-listed assertion.

Tier A: no behavioural surface, a verifier is the existing suite. Tier B:
low risk, named regression tests cover it. Tier C: medium risk or a schema
change; the user decides. "Do" = the reviewer recommends it.

| # | id | file | what | lines | tier | rec |
|--:|---|---|---|--:|:-:|:-:|
| 1 | R2-mgr-1 + R2-pw-1 | manager.rs:2175, prewarm.rs:547 | third and second copies of `worker::testing`'s spawn helpers (test-only) | 90 | A | do |
| 2 | E1 | jobs/extraction.rs:1489-1645 | the transient item-failure sequence written three times in `process_item` | 55 | B | do |
| 3 | C1 | inferio_client.rs:433-561 + extraction.rs:191-297 | `GateState` and `UnitBudget` are the same resizable semaphore with a pending shrink, written twice; the two release paths differ | 45 | C | user |
| 4 | E2 | jobs/extraction.rs:1375-1601 | the verdict sequence (`record_item_failure` → `finalize_item` → match) twice | 25 | B | do |
| 5 | D2 | dispatch.rs:487-760 | the four-variant `DispatchMsg` drain written three times | 25 | B | do |
| 6 | L3 | ledger.rs:1887/5014/10090 | the 12-field `GpuLedger` literal three times; `#[derive(Default)]` | 25 | A | do |
| 7 | H1 | http.rs:1033, manager.rs | `clamp_detail` and `clamp_cooldown_error` are copies of `db::ledger::truncate_error` | 24 | A | do |
| 8 | R2-wk-1 | worker.rs:496-516 | 21 lines of prose in one WARN that the protocol doc already states; the `attribution=` token stays | 22 | A | do |
| 9 | P1-M1 | memory.py:2011-2029 | `measure_batch`'s except arm rebuilds the wire map; provably unreachable | 19 | A | do |
| 10 | L2 | ledger.rs | `transients` ring written per batch, only ever read as `.len()`; a saturating counter is wire-identical | 15 | A | do |
| 11 | E3 | jobs/extraction.rs:741/1182 | twin 12-field `DataLogUpdate` literals | 14 | B | do |
| 12 | L5 | ledger.rs `update_shape_ceiling` | `set`/`lowered` arms identical but for the label | 14 | A | do |
| 13 | R2-gpu-1 | gpu.rs:711-741 | `memory_query`'s Cpu/Mps arms, same 14-line shape | 14 | A | do |
| 14 | P1-P1 | packing.py:932-964 | two counters differing by one attribute name | 14 | A | do |
| 15 | L4 | ledger.rs | the calibration key spelled out six times | 12 | A | do |
| 16 | P1-E1 | eocr.py:510-531 | shapes list, `mag_ratio` and `detector_tensor_dims` computed twice | 12 | B | do |
| 17 | R2-main-1 | main.rs:753-769 | `serve_with_stream_limit` wrapper whose content is one constant | 12 | A | do |
| 18 | R2-mgr-3 | manager.rs:189-209 | two single-caller constructors for a two-field private struct | 12 | A | do |
| 19 | R2-wk-2 | worker.rs:738-752 | two prose variants of one visibility warning | 12 | A | do |
| 20 | P1-M3 | memory.py:1268-1292 | three early returns that return what the fall-through returns | 11 | A | do |
| 21 | L6 | ledger.rs `fit_knee` | buckets walked twice, same rates re-collected | 10 | B | do |
| 22 | C1 (cal) | calibration.rs | `matches_key`/`same_entry` restate the key tuple | 10 | A | do |
| 23 | K1 | cost.rs | identical 5-line group-unit resolution in two fns | 10 | A | do |
| 24 | D3 | dispatch.rs:592-604 | window units/items/bytes folded in one pass | 10 | A | do |
| 25 | R2-gpu-3 | gpu.rs:251-275 | two identical empty-inventory returns | 10 | A | do |
| 26 | R2-gpu-2, R2-cap-1, R2-acc-1 | gpu.rs:1094, capability.rs:190, accelerator_env.rs:142 | subsumed branch; identical drain threads (fixes the silent `None`); five copies of push-unless-set | 24 | A | do |
| 27 | C2, C3, E5, L7, L14 | inferio_client.rs, extraction.rs, ledger.rs | small folds (probe helper, memo rule, hoisted sentence, double `get`, double clone) | 30 | A | do |
| 28 | P1-M2, P1-T1, P1-T2, P1-S1, P1-E2, P1-M6/M7 | memory.py, analyze.py, vramrec.py, `__main__.py`, eocr.py | dead parameter, dead method, dead wrapper, four repeated local imports, unused import, two belt-and-braces clauses (the unified arm's `max()`es at memory.py:797 are load-bearing: leave them) | 24 | A | do |
| 29 | AJ1 | api/jobs.rs | `JobItemFailure` DTO twin of `db::job_failures::JobItemFailureRow`; needs an openapi + ui regeneration for a schema rename | 35 | C | user |
| 30 | L13 | ledger.rs | move `mod tests` (8 212 lines) to `ledger/tests.rs`; 0 lines saved, the file drops under 3 000; must land first or last | 0 | A | user |
| 31 | R2-rl-1, R2-rocm-1 | rlimit.rs:33-108, rocm.rs:86-133 | log-only plumbing (`NofileRaise`, `ProbeFailure`) that typed tests assert on; removing them removes seven tests | 70 | C | defer |
| 32 | E4 | jobs/extraction.rs | extract the 120-line outcome/logging tail; saves ~5 lines but makes the outcome policy testable without a DB | 5 | B | user |

Not recommended, with the reason: every long function (`ingest_locked` 466,
`run_extraction_job_inner` 477, `run_dispatcher` 330, `process_item` 310
after #2, `fit_knee`, `spawn_model`, `ensure_loaded`, `roundtrip`,
`predict`, `run_window` 179, `_serve` 238, the tooling `main`s); the `GpuLog`
enum and `emit` (log-only, but it buys the documented lock-hold-time
property); the `WindowSettled` twin WARN/DEBUG arms (the analyzer selects by
level and reads `reason`); the `_for_test` hooks and `ProbeStub` (all
`#[cfg(test)]`-gated, nothing ships); cross-script deduplication in the
protocol tooling (each tool is a single stdlib-only file by contract);
`CostUnit::AudioSecond` (an accepted registry value).

Correctness items found on the way, not deslop picks: `eocr.py` `unload()`
via `InferenceModel.__del__` raises `ImportError` at interpreter shutdown
(master-era `__del__`); `writes_are_debounced_and_flushed_on_demand` is a
timing race (flaked once under load); `GpuInfo::total_mb` is the one public
health field without an OpenAPI description.

Execution once the user picks: one commit per row (or per file for the
grouped rows), by one agent, with a separate verifier running the suites,
the named regression tests, and for rows touching the ledger or dispatcher
the recorded-ring replay tests and `analyze.py` on two run2 legs.

## Phase 3 — the remaining product-task steps (after deslopping)

State when this was written: run2's change set R1–R12 (R10′) and the
fix round (P1, P2, F1, S1/S1b, C2, C4, C5, D1-b, easyOCR int32 ceiling,
ledger shape ceiling, typed transport retry) are implemented, verified
and integrated; binary `34a591aa`, image `panoptikon:calib-cuda`
`0b2261f94c8f`; both branches pushed (core
`claude/batch-calibration-coverage-db9ab9`, ui `batch-calibration-ui`
at `8abf631`). Phases A, C, D1 (easyOCR), the ground-truth probes,
Phase A′ (S2/S3 wd-vit re-run) and S4a are measured and reported in
`docs/batch-calibration-run2-report.md`. SGLang is the user's and is
up on both GPUs; the remaining legs are blocked on idle GPUs, not
failed.

After deslopping, and only when the user says the GPUs are free
(never stop SGLang unasked), on a binary rebuilt from the deslopped
tip (rebuild both the release binary and `panoptikon:calib-cuda`; the
suites must be green first):

1. S4b, S4d, S4g (report §4.5 has the commands and seed stores).
2. Phase D: S8 pixmix (nemotron; compare the fit against the probe of
   the same image group, `results/run2/probes/summary.md`), S11-C4
   (Docker, the shipped `docker.toml` policy over h2c, sockets counted
   in the container), the easyOCR C7 shape-ceiling leg (needs ~93 GB
   free; expect the ceiling at 28 items ≈ 183 500 800 units).
3. Phase E: the 4 h S9 soak on both GPUs.
4. Finalise report §4.5–4.8 and the plan's §4/§5 from the runlogs; the
   commit appendix; `results/run2/README.md`.
5. Decisions still owed by the user (report §6 options table): the
   worker clamp double-counting our own allocator pool (44 % of a grant
   unused at 12 GB free); nemotron's aspect-ratio pricing spread (tile-
   based pricing); easyOCR `enable_batching = false` on the shipped
   registry (never learns); `KNEE_PLATEAU_BUCKETS = 2` (MobileCLIP fits
   no knee); the OCR fidelity decision (recognition from raw kept);
   an expiry probe for the shape ceiling; whether the run2 constants
   (512 streams, 4 GiB body budget, 64 lanes, gate 256–4096) become
   settings.
6. Release: sync the Nix UI pin (`scripts/sync-nix-ui-pin.py`) before
   any tag, per CLAUDE.md.
