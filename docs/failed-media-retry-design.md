# Failed-media ledger and targeted retry

Status: **designed 2026-08-01, not implemented.** Supersedes the mechanism in
PR #25 (soft-fail corrupt media) while keeping its goals; incorporates the
visuals negative-cache phase-2 design (2026-07-21) by reference.

## Problem

A file the pipeline cannot process is handled today in one of three bad ways:

1. **Extraction, hard fail** (`prepare_item` error, PDF/HTML render failure,
   ffmpeg failure): the item stays selectable and is re-attempted on **every
   cron run, forever**. No record beyond a log line and `data_log.errors`
   (an integer).
2. **Filescan, hard fail** (hash, mime, metadata/ffprobe, image decode in
   `prepare_new_item`): the file is not indexed and is re-hashed / re-probed
   / re-decoded on **every scan, forever**. The only trace is
   `file_scans.errors` (an integer) and an in-memory `error_paths` vec that
   dies with the scan.
3. **Filescan, visuals** (thumbnail/frame generation): failure is swallowed,
   the item is indexed without visuals, and generation is re-attempted on
   every scan (the visuals negative-cache problem, phase 2 designed
   separately).

Additionally, a corrupt item inside a coalesced inference batch fails the
whole batch, and a job where every item fails is unconditionally treated as
an inference-server outage.

Nothing anywhere records *which* files failed or *why*, so failures are
invisible and unauditable.

## Requirements (user-stated, 2026-08-01)

1. **Parity**: must not be stricter than the actual inference pipeline; never
   block media that would have passed.
2. **No significant overhead** (CPU or memory) in jobs on the healthy path.
3. Jobs **recover smoothly** from a failed item and continue.
4. Failed files are **visible and auditable**: which files, what error.
5. **Error classes** are distinguished; transient errors are never treated as
   permanent.
6. No **repeated wasted work** on known-failed items.
7. **Targeted retry on new versions**: an intentional, shipped decision (not
   every version bump), targetable at least by file format (mime prefix) and
   ideally by error class/stage; targets independently settable.
8. Works for **filescan and extraction jobs equally**.
9. Works for **all file types**.
10. Special handling for **missing external dependencies** (pdfium, headless
    browser, ffmpeg): those items retry when the dependency appears.

## Design principles

**The arbiter principle.** Only the component that actually consumes the
media may classify it as bad input. The gateway never *pre-judges* media on
behalf of a downstream consumer with a different decoder:

- Still-image bytes are consumed by the Python workers (PIL with
  `LOAD_TRUNCATED_IMAGES = True`). The gateway keeps only the cheap
  header-parse check (parity with Python's `is_image_readable`, which
  deliberately accepts truncated files) and **never fully decodes still
  images** just to test them. This kills PR #25's full-decode gate: the Rust
  `image` crate is stricter than PIL, and full decode of every healthy image
  is duplicated CPU. A file is "corrupt for inference" only if the worker
  itself says so (see worker protocol below).
- GIF frames, video frames (ffmpeg), PDF renders (pdfium), HTML screenshots,
  and audio transcodes are produced **by the gateway** — there the gateway's
  own failure *is* the pipeline verdict.

**Ledger, not placeholder.** Failures are recorded in dedicated ledger
tables, not by abusing `item_data` placeholder rows. `is_placeholder = 1`
keeps its exact current meaning ("processed successfully, genuinely no
data"). This keeps user-facing `processed_by` semantics pure, gives failures
a natural home for class/message/attempt metadata, and makes retro-retry a
row deletion instead of surgery on `item_data`.

**Markers are advisory for correctness, authoritative for scheduling.** A
lost ledger row costs one re-attempt, never correctness. A present row only
suppresses scheduling; it never affects search results or data reads.

**Deterministic, no timers.** No exponential backoff, no wall-clock expiry.
Rows are cleared by exactly three events: the content changes (new sha256 ⇒
new item ⇒ FK cascade / path re-verify), a shipped retry directive matches
them, or a missing dependency appears. Between those events the skip is
free.

## Error taxonomy

| class | meaning | persisted? | cleared by |
|---|---|---|---|
| `transient` | I/O, stat, network mount, worker crash/restart, inference server down, DB busy | **never** | n/a — item simply fails this run and stays selectable (today's behavior) |
| `input` | the pipeline's own decoder/tool rejected the *payload* (decode error, ffmpeg non-zero exit on read-OK bytes, pdfium parse failure, worker-reported per-item decode error) | yes | content change, retry directive |
| `blocked` | a required external dependency is not installed (`pdfium`, `html-renderer`, `ffmpeg`) | yes, with `blocker` | dependency appears (automatic), retry directive |
| `resource` | the item individually exceeds resource limits (e.g. classified batch-1 OOM from the GPU-compat work, decode memory limit hit) | yes | retry directive (e.g. after hardware/limit change) |

**The I/O-vs-payload split** (from PR #25, kept): any failure where the
gateway did its own successful read and a decode of in-memory bytes failed
is unambiguously `input`. Any failure inside a tool that does its own file
I/O (ffmpeg/ffprobe/pdfium reading a path directly) is ambiguous — a
transient SMB hiccup and a corrupt file both surface as a non-zero exit.
These ambiguous stages get a **confirmation threshold** (below) instead of
being classified `transient`, so corruption is still caught (req 6) without
a single NAS blip permanently skipping a healthy file (req 5).

Spawn errors (`ENOENT` on ffmpeg etc.) are `blocked`, never `input` — the
error construction sites must distinguish spawn failure from non-zero exit.

### Confirmation threshold (`skip_after`)

Each ledger row stores `skip_after` (1 or 2), fixed at classification time:

- `skip_after = 1`: deterministic verdicts — decode of successfully-read
  bytes, worker-reported per-item input errors, `blocked`.
- `skip_after = 2`: ambiguous verdicts — external-tool failures where the
  tool did its own I/O (ffmpeg/ffprobe/pdfium/html-renderer non-zero exits).

An item is skipped only when `attempts >= skip_after`. `attempts` increments
at most once per job/scan run (rows record the last incrementing
`job_id`/scan id), so a threshold-2 item gets exactly one confirmation
re-attempt in a *later* run, then goes quiet. Worst case waste: 2 attempts,
ever, per (item, consumer).

## Storage

Three ledgers, each colocated with the data whose lifecycle it shares:

### 1. `item_extraction_errors` (index.db) — extraction failures

```sql
CREATE TABLE item_extraction_errors (
    id          INTEGER PRIMARY KEY,
    item_id     INTEGER NOT NULL REFERENCES items(id)   ON DELETE CASCADE,
    setter_id   INTEGER NOT NULL REFERENCES setters(id) ON DELETE CASCADE,
    stage       TEXT NOT NULL,   -- 'prepare' | 'inference'
    error_class TEXT NOT NULL,   -- 'input' | 'blocked' | 'resource'
    blocker     TEXT,            -- 'pdfium' | 'html-renderer' | 'ffmpeg', else NULL
    mime_type   TEXT NOT NULL,   -- denormalized items.type, for targeted directives
    error       TEXT NOT NULL,   -- human-readable message, for audit
    skip_after  INTEGER NOT NULL DEFAULT 1,
    attempts    INTEGER NOT NULL DEFAULT 1,
    last_job_id INTEGER,         -- last data_jobs.id that attempted (attempt dedup + audit)
    first_seen  TEXT NOT NULL,   -- ISO-8601, matching schema convention
    last_seen   TEXT NOT NULL,
    UNIQUE(item_id, setter_id)
);
CREATE INDEX idx_item_extraction_errors_setter
    ON item_extraction_errors(setter_id, attempts);
CREATE INDEX idx_item_extraction_errors_class
    ON item_extraction_errors(error_class, mime_type);
```

Keyed per **(item, setter)**, not per item: prepare paths differ by input
handler, worker tolerance differs by model, and the work query is per-setter
so the anti-join lines up naturally. Corrupt files are rare, so the
per-setter multiplication is noise; a new model legitimately gets its own
attempt.

Write is an upsert: same (item, setter) → `attempts + 1` (if
`last_job_id` differs), refresh `stage/error_class/error/last_seen`; a class
change refreshes the classification but does **not** reset `attempts` — a
pair whose verdict alternates between runs (`input` on one pass, `resource`
on the next) would otherwise never reach `skip_after` and would be retried
forever. Deleted on successful extraction of the item
by that setter (one `DELETE` in the success path of `process_item` — a
no-op for the 99.99% case; skip it unless the job saw the item come off a
retry directive, tracked by a cheap "ledger had rows for this setter at job
start" flag).

Lifecycle: FK cascade with `items` (content fix ⇒ new sha ⇒ new item; old
item orphaned ⇒ row gone) and with `setters` (data-deletion job for a model
clears its failure history too — correct, since deleting and re-running is
the manual "retry everything" gesture today).

### 2. `scan_errors` (index.db) — filescan pre-item failures

```sql
CREATE TABLE scan_errors (
    id          INTEGER PRIMARY KEY,
    path        TEXT NOT NULL UNIQUE,
    last_modified TEXT NOT NULL,  -- mtime at failure
    file_size   INTEGER NOT NULL, -- size at failure
    stage       TEXT NOT NULL,    -- 'hash' | 'mime' | 'metadata' | 'decode'
    error_class TEXT NOT NULL,    -- 'input' | 'blocked' | 'resource'
    blocker     TEXT,
    mime_type   TEXT,             -- best-effort (extension guess), may be NULL
    error       TEXT NOT NULL,
    skip_after  INTEGER NOT NULL DEFAULT 1,
    attempts    INTEGER NOT NULL DEFAULT 1,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL
);
```

Path-keyed because these failures happen before an item (or even a hash)
exists. `(last_modified, file_size)` is the retry key: the scan walker skips
a path only when the row is active (`attempts >= skip_after`) **and** mtime
and size match — a modified file always retries automatically. This is the
same mtime shortcut the scan already trusts for unchanged-file detection,
so it adds no new trust assumption.

Scan integration: at folder-scan start, load the (tiny) set of `scan_errors`
rows under the scanned roots into a map — no per-file query. Rows are
deleted when the path processes successfully, and swept when the walker
finishes a root without encountering the path (the in-memory map makes the
sweep a set difference over ledger rows only, never over all files).
Skipped-known-bad paths are added to the existing `error_paths`/
`excluded_paths` mechanism so they are not marked unavailable. Continuous
scan consults and writes the same table (it shares `process_file`).

`FileProcessError` mapping: `Io`/`Worker` → transient (never recorded);
`Filtered`/`Unchanged` → unchanged behavior; `Unsupported` is **split** —
it currently conflates "no mime type", "ffprobe failed", and "image decode
failed", which must become distinct stages with per-stage class and
`skip_after` (ffprobe → `input`, `skip_after 2`; missing mime → `input`,
`skip_after 1`; spawn ENOENT → `blocked`).

### 3. `visual_attempts` (storage.db) — visuals negative cache

The existing phase-2 design (2026-07-21) is adopted as-is — sha-keyed,
lifecycle-shared with `thumbnails`/`frames`, cleared on positive store,
invalidated by `*_PROCESS_VERSION` bumps — with two amendments to align it
with this design:

- add `item_mime_type TEXT NOT NULL` (precedent: `frames` stores it), so
  retry directives can target visuals by format;
- its `failed` outcome drops the exponential-backoff clause and uses the
  same deterministic model as everything else: `skip_after` + retry
  directives + version bumps. ("Never permanent: decoders improve" is now
  serviced by directives, not timers.)

## Work-query integration (extraction)

`build_job_pql` gains one internal filter alongside the existing
`NOT ProcessedBy`:

```
NOT FailedFor { setter, min_attempts_reached: true }
```

compiled as the same anti-join shape `ProcessedBy` uses (LEFT JOIN +
IS NULL), against `item_extraction_errors` with
`setter_id = ? AND attempts >= skip_after`. The table is tiny and
unique-indexed on `(item_id, setter_id)`; the added cost on a 90k-row work
query is negligible (req 2). The filter is a first-class PQL element, which
also makes it exposable to user searches later ("show me files that failed
CLIP") for free — but job code is its only consumer initially.

`HasUnprocessed` and every other user-facing filter are untouched.

## Recovery and job completion (extraction)

Adopted from PR #25 (its best part), reworked onto the ledger:

- `process_item` failures classify → transient failures behave exactly as
  today (count error, stay selectable); persistent classes upsert the ledger
  row and count as an **input-side error** in `JobCounters`
  (`input_errors`).
- A **failed ledger write is counted systemic**, never input-side, and the
  item task returns `Err` — a DB outage must not soft-complete as "all
  corrupt media". One short write retry is fine but optional; the writer
  actor already serializes writes.
- Job completion classifier (pure function, unit-tested):
  - some successes → normal completion (unchanged);
  - all attempted failed, all input-side → **complete with warning**
    ("N items failed on input media"), `data_log` finishes normally;
  - all attempted failed, any non-input → hard fail with the existing
    "check the inference server" error, log row left unfinished.
- `data_log` gains an `input_errors INTEGER NOT NULL DEFAULT 0` column
  (migration) so job history shows "errors: 12 (9 input)" — per-job audit
  without a join.
- Every item-failure log line carries `path`, `sha256`, `stage`,
  `error_class` (PR #25's logging improvement, kept).

### Batch isolation and the worker protocol (parity, req 1)

Two layers:

**Layer 1 — per-item typed errors from workers (the precise fix).** The
inferio predict protocol currently returns exactly one output per input or
fails the whole roundtrip (`worker.rs:433`). Extend the protocol (we own
it; explicitly changeable per the port's design constraints): an output slot
may be an error object `{error: {class: "input"|..., message}}` instead of a
payload. On the Python side, the natural seam is the shared deserialization
helper (`inferio/impl/utils.py` — where PIL opens the bytes): catch
`UnidentifiedImageError`/decode `OSError` per item *before* the batch is
assembled, exclude the item from the tensor batch, and emit an error slot.
This fixes batch poisoning at the root — a corrupt item can no longer take
healthy batch-mates down — and makes PIL-with-truncation-enabled the image
arbiter, satisfying parity by construction. Worker impls that decode
somewhere other than the shared helper need a one-time audit; any that
slip through are covered by layer 2.

**Granularity caveat — text-entity models.** For a text-entity model a
per-item worker verdict is really a verdict on one *data row* (one extracted
text segment), while both the ledger and the `failed_for` anti-join key on
`item_id` and therefore suppress at (item, setter) granularity. Persisting a
worker-reported `input` verdict for such a model would take every one of that
item's segments out of the work query because a single segment was bad, which
is not acceptable. So worker-reported input errors for text-entity models
stay **transient** (counted, retried, never persisted) unless and until the
ledger gains a nullable `data_id` and the anti-join learns to match on it.
Item-keyed suppression remains correct for the prepare stage, where the
failure is the item's media and there are no data rows to key on anyway.

**Layer 2 — isolation retry (the fallback).** When a whole batch predict
fails and the batch had more than one item, re-submit the items
individually, once. Healthy items complete (req 3); the item that fails
alone is classified by its own error — which, absent a typed worker error,
stays `transient` (counted, retried next run). No isolation result is ever
promoted to `input` by pattern-matching exception text: unclassified means
transient, so the system can never be stricter than the pipeline (req 1),
merely slower to learn.

Gateway-native prepare stages classify directly (they are the pipeline):

| stage | failure | class | skip_after |
|---|---|---|---|
| file read/stat (any handler) | io error | transient | — |
| image header parse (in-memory bytes) | parse error | input | 1 |
| GIF decode (in-memory bytes) | decode error | input | 1 |
| video/audio ffmpeg, ffprobe | non-zero exit | input | 2 |
| video/audio ffmpeg, ffprobe | spawn ENOENT | blocked(ffmpeg) | 1 |
| PDF render | pdfium unavailable | blocked(pdfium) | 1 |
| PDF render | load/render error | input | 2 |
| HTML screenshot | no browser | blocked(html-renderer) | 1 |
| HTML screenshot | launch/timeout/exit | input | 2 |
| worker typed error slot | per-item decode | input | 1 |
| batch-1 classified OOM (GPU-compat) | resource limit | resource | 1 |
| anything unclassified | — | transient | — |

Note the reclassification of pdfium/browser-missing from today's
"hard-fail, retry every run forever" to `blocked`: with the ledger, a
missing dependency stops burning time on every job (req 6) and self-heals
(next section) instead of relying on eternal retries.

## `blocked` auto-heal (req 10)

At extraction-job and scan start, collect the distinct `blocker` values
present in the relevant ledger (usually none — one indexed query), and
probe **only those** backends via the existing resolution paths
(`pdfium()`, `html_renderer()`, ffmpeg spawn probe). For each backend that
now binds, delete its `blocked` rows — those items become selectable in the
same run. Lazy per-blocker probing is deliberate (same reasoning as the
visual_attempts design): never load a library the run doesn't need.

Caveat to document for users: the backends are cached in `OnceLock`s, so
installing a dependency takes effect at the next gateway restart; the
ledger clears on the first job after that.

## Targeted retry directives (req 7)

A retry directive is an ordinary **data-only migration** — the intentional,
ships-with-a-release act of saying "we improved X; re-attempt matching
failures":

```sql
-- migrations/index/20261015120000_retry_image_decode_upgrade.sql
-- image crate 0.26: retry everything that failed image decode.
DELETE FROM item_extraction_errors
WHERE error_class = 'input'
  AND stage = 'prepare'
  AND mime_type LIKE 'image/%';
DELETE FROM scan_errors
WHERE error_class = 'input'
  AND stage = 'decode';
```

Why migrations and not new machinery:

- The desired semantics — *runs exactly once per database, at upgrade, on
  every database* (`migrate_all_databases_on_disk` sweeps them all) — is
  precisely what the migration runner already provides. An in-code rule
  ledger with its own applied-version marker would re-implement that
  bookkeeping for cosmetic benefit.
- Targets are just the `WHERE` clause: mime prefix, error class, stage,
  blocker, or `setter_id` via a subquery on `setters.name` — independently
  composable (req 7), no DSL to design.
- Deletion is safe by construction: worst case, a still-bad file re-fails
  once (twice for `skip_after 2`) and the row comes back. No rollback
  needed.
- Repo precedent for data-ops in migrations exists (`maintenance_state`
  seeding), and the "never edit an applied migration" house rule covers the
  discipline.

Conventions: filename `..._retry_<slug>.sql`, a comment stating what
changed and why the retry is warranted, and **never** a blanket
`DELETE FROM item_extraction_errors` without a class/stage/mime predicate.
`visual_attempts` retries ship as `migrations/storage/` directives (the
mime column exists for exactly this), on top of the coarser
`*_PROCESS_VERSION` bump which remains the "the generator itself changed"
lever.

The acknowledged inelegance — schema history interleaved with retry
decisions — is cosmetic; the alternative machinery is real complexity.

## Scan policy for undecodable images (un-fusing the image decode)

The scan already has a uniform two-tier policy: **metadata failure → not
indexed; visuals failure → indexed without visuals.** A broken PDF is
indexed without a thumbnail (`files.rs:1981` — "Renders nothing when pdfium
is unavailable or the PDF is broken"), a video whose frames ffmpeg cannot
extract is indexed without visuals (error swallowed at `files.rs:1544`),
and only an ffprobe *metadata* failure blocks a video from indexing.

Images are the one type that violates this rule, by accident of code
structure: `prepare_new_item` performs a single full decode
(`files.rs:1506-1518`) whose result serves both the metadata phase
(dimensions) and the visuals phase (thumbnail, blurhash). A pixel-level
decode failure — a visuals-grade problem — is thereby escalated to a
metadata-grade one, and the file is never indexed at all, silently, and
re-decoded on every subsequent scan (no `files` row means no mtime
shortcut). Fix: un-fuse the decode so images follow the same two tiers:

- **Metadata phase: header parse only** (`into_dimensions()`). If even the
  header cannot be parsed, hard-fail as today — such a file is garbage to
  every consumer (this is also parity with PIL `verify()` semantics, which
  accept truncated pixel data but not unparseable headers). The
  `scan_errors` row makes the failure auditable and stops the every-scan
  re-attempt. Header parsing yields dimensions for almost all pixel-corrupt
  files (a truncated JPEG has an intact header), so indexed dimensions
  remain populated for the interesting cases.
- **Visuals phase: full decode.** On failure, index the item without
  visuals — exactly the broken-PDF behavior — and record the decode error
  (`scan_errors` stage `decode`, class `input`). Extraction then proceeds
  normally and PIL renders the final verdict on processability.

"Accepting images we can't thumbnail" is not a new concession: most images
already have no stored thumbnail (`generate_thumbnail` returns `None` for
the served-directly class) and are served from the original file as the
normal path. A pixel-corrupt indexed image serves its original — truncated
JPEGs render progressively in browsers — and remains CLIP/tag-searchable
via the PIL-side extraction path. The worst case is a broken-image icon
for a genuinely damaged large file, versus today's silent invisibility
plus a wasted decode every scan. Blurhash loss is cosmetic.

Video/audio keep their existing metadata hard gate, now recorded: scan
ffprobe and extraction ffmpeg are the same toolchain, so the scan verdict
already is the pipeline verdict; the `scan_errors` row (skip_after 2)
stops the every-scan re-probe and makes the file visible to auditing.

This section is the only part of the design that changes what enters the
index (images previously rejected at pixel-decode now index without
visuals); it ships as its own phase.

## Audit surface (req 4)

- **API**: `GET /api/jobs/data/failures` (extraction ledger joined with
  `items`+`files` for path/sha256/mime, filterable by setter, class, stage,
  mime prefix; paginated) and `GET /api/jobs/scan/failures` (scan ledger).
  Counts endpoint for badges. OpenAPI regen for the UI `.d.ts`.
- **UI**: a "Failed files" card on the job-management page: table with path,
  setter, class, stage, error message, attempts, last seen. Not blocking
  for the core mechanism, but this is the requirement-4 deliverable; logs
  alone are not "auditable later".
- **Job history**: `data_log.input_errors` split, shown in the existing job
  log UI.

## Requirement mapping

| req | satisfied by |
|---|---|
| 1 parity | arbiter principle; header-check parity; typed worker errors; unclassified ⇒ transient; scan image-gate softened |
| 2 overhead | no full decode; anti-join on tiny indexed table; ledger writes only on failure; scan map preloaded per folder |
| 3 recovery | per-item tasks (existing) + batch isolation retry + worker per-item error slots |
| 4 audit | ledger rows with stage/class/message/timestamps; failures API + UI; `data_log.input_errors`; path+sha256 in every failure log |
| 5 classes | taxonomy; transient never persisted; ambiguous stages get `skip_after 2` instead of misclassification |
| 6 no rework | active rows excluded from work query / scan walk; blocked no longer retries eternally; ≤2 attempts worst case |
| 7 targeted retry | data-only retry-directive migrations; targets = SQL predicates over class/stage/mime/blocker/setter |
| 8 both job types | `item_extraction_errors` + `scan_errors` + `visual_attempts`, one taxonomy and one directive mechanism across all three |
| 9 all types | classification is stage-based (read/decode/tool/worker), not format-based; every handler routes through it |
| 10 blocked deps | `blocked` class + per-blocker lazy probe at job/scan start, auto-clearing |

## Disposition of PR #25

Kept (as concepts, reimplemented on the ledger): the I/O-vs-payload
classification split, the pure job-completion classifier with its
placeholder-write-failure-is-systemic rule (now ledger-write-failure), the
soft-complete-with-warning behavior, path+sha256 logging, and the general
test shapes. Dropped: the full-decode gate (parity + CPU), `input_media`
placeholders in `item_data` (replaced by the ledger), the 50ms sleep-retry.
The `ApiErrorKind` enum survives but grows the full taxonomy
(`Transient`/`Input`/`Blocked`/`Resource` + blocker payload) instead of one
`InputMedia` flag.

## Phasing

1. **Core ledger**: migrations (both tables + `data_log.input_errors` +
   `visual_attempts`), `ApiErrorKind` taxonomy, extraction classification +
   work-query anti-join + completion classifier, scan pre-item ledger,
   blocked auto-heal. Tests: classifier unit tests, ledger round-trip DB
   tests, skip/threshold behavior, blocked-clear behavior.
2. **Worker protocol**: per-item error slots (Rust protocol + `utils.py`
   seam + per-impl audit), batch isolation retry.
3. **Audit surface**: failures API, UI card, `.d.ts` regen.
4. **Visuals negative cache**: implement the amended visual_attempts design
   (it now shares the taxonomy and directive mechanism).
5. **Scan image-decode un-fusing**: header-parse metadata gate + soft
   visuals decode for images (separate phase; changes what enters the
   index).

Phases 1 is prerequisite for the rest; 2–5 are independent of each other.

## Non-goals

- **Mutable-source extractors** (saucenao, tagmatch): they break the
  immutable run-once model in a different way (results change over time,
  not the media). Deliberately out of scope here, as in the embedding
  redesign.
- Placeholder-expiry timers, wall-clock backoff, or any automatic retry not
  triggered by content change, dependency appearance, or a shipped
  directive.
- Changing `is_placeholder` semantics or the `item_data` schema.
- **Per-data-row (segment) failure granularity.** The ledger keys on
  (item, setter); a nullable `data_id` and a data-keyed anti-join are out of
  scope, which is why worker-reported input errors for text-entity models
  stay transient (see "Granularity caveat — text-entity models").
- ANN/vector-index work and the model-identity redesign (orthogonal; the
  ledger keys on `setters.id`, which survives that redesign's setter-string
  changes as long as setter rows keep existing).
