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
