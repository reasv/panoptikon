# Failed-media ledger and targeted retry

Status: **designed and implemented 2026-08-01** (all five phases; the
*As implemented* notes throughout record where the code diverged from or
settled something this document left open). Supersedes the mechanism in
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
    -- as implemented: 'mime' | 'metadata' | 'header' | 'decode', and every
    -- value but 'decode' blocks indexing and therefore suppresses — see the
    -- scan-policy section.
    stage       TEXT NOT NULL,
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

Traversal, as implemented: directories whose name starts with `.` or equals
`__MACOSX` (case-insensitively) are never descended into **below a root** —
by the batch walk, the directory poller, and the continuous scan's event
admission alike. Everything under them is macOS sidecar litter under
ordinary-looking names, which the file-name rules cannot see. Content
previously indexed beneath such a directory is retired by the ordinary
unavailable-file flow (the walk no longer reaches it, so the next scan marks
it unavailable, and `remove_unavailable_files` deletes it if the user has
that on), and its ledger rows go the same way as any vanished path, through
the end-of-root sweep. Because that is a deletion the user never asked for
directly, each root logs how many junk directories it pruned when the count
is nonzero. A dot-named folder the user actually wants indexed is registered
as an included root in its own right: the root itself is exempt, only what
is *below* it is judged.

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

*As implemented.* A rowid table (the `error` column is payload, and WITHOUT
ROWID would spill every failed row to overflow pages), keyed
`PRIMARY KEY (item_sha256, kind)` with one extra index on
`(outcome, item_mime_type)` for the auto-heal probe and the retry directives.
`outcome` is the coarse three-value vocabulary above: a `resource` verdict
persists as `failed`, since the finer class is the two index-side ledgers'
job. The `blocked` auto-heal is one pass over both scan-side tables, sharing
the probe. Two paths deliberately write markers without consulting them,
because the pass has already run by the time the content hash exists: new
items (no marker can apply to a hash the index has never seen) and the
continuous scan (an event fires only when a file's mtime moved — and its pass
runs in a worker with no database handle, which is the real obstacle, not the
key). Every path that re-attempts visuals for content the index already has
goes through the batch scan's `maybe_dispatch_backfill`, which consults. The
`image_is_served_directly` predicate stays the cache for images with no
storable thumbnail — marking those would put a row in the table for the
majority of a library.

Three things the first implementation got wrong or left open, all settled in
the same step:

- **A suppressed image used to fall through to the blurhash.** Clearing
  `needs_thumb` on an active marker is not the end of the dispatcher: an image
  with no stored thumbnail and no blurhash still has work, and the blurhash's
  only remaining source is a full decode of the original — the exact decode
  the thumbnail marker's verdict already settled. The dispatcher now tracks
  *why* `needs_thumb` is false (stored / served-directly / marker-suppressed)
  and returns without dispatching when a marker-suppressed image's only
  remaining work is that re-decode. The `visuals_suppressed` stat and its log
  moved with it: they now count whole dispatches skipped, not marker hits.
- **Frame markers are written but not yet consulted.** The dispatcher's only
  questions are "is there a thumbnail" and "is there a blurhash", so a video
  with both and no frames is invisible to it; no frames-only backfill exists
  to consult `kind = 'frame'`. The markers are still written (they are what a
  future frames-only pass would read, and what the orphan sweep and the
  directives already act on), and the invariant is pinned by
  `a_video_missing_only_frames_is_not_dispatched` so the consult arrives with
  the pass that needs it.
- **The backfill path records the no-video-track nothing too.** Its early
  return decides "this video has no video track" from indexed metadata without
  starting ffmpeg, and used to return without recording anything — so every
  track-less video *already in an index* (which is every existing library:
  those items never went through the new-item recording path) was re-decided
  on every scan forever. It now writes the same `none` verdict for both kinds
  that `build_new_item_renditions` writes, which is honest there because the
  branch only runs when no frames are stored.
- **One attempt token per run, not per root.** `attempts` counts runs that saw
  the same conclusion and dedups on `last_scan_id`, but a run opens one
  `file_scans` row per root while markers are keyed by content: identical
  content under two roots was counted twice by a single run, confirming a
  `skip_after = 2` verdict that had only failed once. Every `visual_attempts`
  write of a run now carries the first root's scan id.

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

### The other half: failures with no verdict (run2, R2)

Everything above is about failures that *are* a verdict on the media, and the
rule that only those may be persisted is unchanged and load-bearing: a row in
`item_extraction_errors` makes the work query skip an item, so writing one for
a transient failure would suppress a perfectly good file.

Run1 measured what that leaves behind. A single inference worker death failed a
whole in-flight window — **1 542 items** — the items were transient failures so
nothing was recorded anywhere, `/api/jobs/data/failures` answered
`{"total": 0}`, and the job reported **completed** (findings F7 and Q8/T8). The
user could not tell that a fifth of the work had not happened.

Three changes, none of which touch the ledger's rule:

1. **Re-queue once on a worker death.** The inference surface now names that
   failure on the wire — `{"detail": {"kind": "worker_died", …}}` on the
   predict 500 (`inferio/http.rs`) — and the client turns it into a typed
   `InferenceFailure` the job matches on. The items in a died-on request were
   never attempted, so `run_item_inference` re-submits that item's work
   **once**; the next predict is what respawns the worker, so there is nothing
   to wait for. One retry per item per job, so a job of N items can cost at
   most 2N requests however many times the worker dies. A model that fails to
   *load* surfaces as a load failure, not a death, so this cannot spin.
2. **A per-job failure audit** (`data_job_failures`, `db/job_failures.rs`).
   One row per item a job attempted, could not finish, and has no verdict for:
   item, setter, stage, error text, whether its re-queue was spent, and when.
   **Nothing joins it** — it suppresses nothing, and the item is selected again
   on the next run exactly as before. Rows are written once at the end of the
   job (a death fails a window at a time, so a writer round trip each would put
   the burst on the critical path of the failure the job is recovering from),
   bounded at 10 000 per job in the *listing* while the counts stay exact, and
   pruned with the job history that explains them.
3. **A job outcome, recorded rather than inferred.** `data_log.outcome` is one
   of `completed`, `partial`, `failed`, `cancelled` (`''` on every row written
   before the column existed, which reads as "still running"), with
   `data_log.failure_reason` beside it, and the queue's `JobOutcomeStatus`
   gains the matching `partial`. **`completed` now means every item was done.**
   A job that ran to the end with unexplained failures is `partial`; one that
   stopped early is `failed`, and *its record is finalized on that path too* —
   a real `end_time`, the counters it reached and the reason, where run1
   measured `end_time == start_time` and `failed = 0`. Cancellation is stamped
   by a drop guard, which is the only code that knows when the job stopped.

A **load-failure cooldown** (`{"kind": "load_cooldown"}`, HTTP 503 with
`Retry-After`) is the one refusal that aborts the job outright rather than
failing items: it is a statement about the model for a stated window, so every
remaining item would get the same answer. The job's failure reason names the
model, the retry instant and the last error.

The failures endpoint serves all three: the ledger's verdicts, the per-job
failures, and the jobs behind them — see "Audit surface" below.

### Batch isolation and the worker protocol (parity, req 1)

Two layers:

**Layer 1 — per-item typed errors from workers (the precise fix).** The
inferio predict protocol currently returns exactly one output per input or
fails the whole roundtrip (`worker.rs:433`). Extend the protocol (we own
it; explicitly changeable per the port's design constraints): an output slot
may be an error object
`{"__error__": {"class": "input"|"transient", "message": ...}}` instead of a
payload (as implemented; wire format and compatibility rules in
`docs/inferio-worker-protocol.md`). On the Python side, the natural seam is
the shared deserialization helper (`inferio/impl/utils.py` — where PIL opens the bytes): catch
`UnidentifiedImageError`/decode `OSError` per item *before* the batch is
assembled, exclude the item from the tensor batch, and emit an error slot.
This fixes batch poisoning at the root — a corrupt item can no longer take
healthy batch-mates down — and makes PIL-with-truncation-enabled the image
arbiter, satisfying parity by construction. Worker impls that decode
somewhere other than the shared helper need a one-time audit; any that
slip through are covered by layer 2. (Audit outcome: every image impl now
routes through the shared seam. The audio impls — `clap`, `whisper` — do not:
they deserialize an npy buffer the *gateway* produced with ffmpeg, so a
failure there is a gateway-side verdict, not the worker's. `danbooru` and the
saucenao impls are the mutable-source extractors this design lists as a
non-goal, and `sentence_transformers` decodes nothing.)

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

*Where layer 2 actually applies (found while implementing).* Extraction never
puts two **items** in one predict call: each item task chunks its own work
units by the job's batch size and issues one request per chunk. Cross-item
merging happens one layer down, in the local orchestrator's dispatcher, which
already implements exactly this fallback (a merged window that fails with a
per-request `WorkerError` is retried request by request — `dispatch.rs`), so
an item never loses its batch-mates' work to a neighbour's bad file. What was
missing is the level below: one item's chunk of many units (video frames, PDF
pages). That is where the implemented isolation retry sits — a failed
multi-unit chunk re-submits its units one at a time, once, and a unit that
still fails alone fails the whole item transiently rather than writing partial
data (an unclassified failure leaves the item selectable, so it is reprocessed
in full next run).

*Partial typed failures.* **All-`input` partials proceed**: an item where only
some inputs came back as typed slots, *and every one of those slots is class
`input`*, is processed with the outputs that succeeded — the verdict has to be
about the item's media, and media that partially decodes is processable media.
A `transient` slot is not a verdict at all, so a single one anywhere in the
response (partial or not) fails the whole item transiently: proceeding would
mark the item processed and permanently lose the unit the worker asked us to
retry. Class is therefore decided before arity. Only an item whose inputs
*all* failed with class `input` earns a ledger row (stage `inference`,
`skip_after 1` — the worker decoded bytes it already held).

The excluded units of a proceeding item are logged, counted nowhere, and never
persisted, and that is deliberate rather than a gap: the item *is* processed,
so there is no ledger row to attach a unit-level failure to and no counter it
could increment without lying about the item's outcome — the log line (path,
sha256, input index) is the whole record. Note also that the surviving outputs
keep their *original* input positions, so a dropped frame leaves a gap in
`item_data.idx` instead of renumbering its successors.

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
| single input over the transport frame budget (pre-send) | resource limit | resource | 1 |
| anything unclassified | — | transient | — |

The frame-budget row (added 2026-08-01, from the CLAP whole-track field
failure): one input is the smallest unit a predict can be split into, so a
single input over `FRAME_INPUT_BYTES_BUDGET` can never be inferred on this
machine no matter how it is batched — a deterministic per-machine verdict,
settled in `prepare_item` before any predict is attempted. Without it the
refusal surfaced as a generic predict 500 (typing lost over the HTTP hop),
classified transient, and re-failed the job on every run. The same change
made the transport refusal itself non-fatal (WorkerError, worker stays
alive), raised the frame limit to 2 GiB, and made dispatcher batch
admission and sub-batch splitting byte-aware so merged windows can never
assemble an unencodable frame.

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

Backend cache lifetime is backend-specific. PDFium retains its process-lifetime
binding, while HTML caches only a successful executable that still exists and
never caches absence. Installing a compatible browser can therefore clear
`blocked(html-renderer)` rows on the next scan without restarting the gateway.

HTML is the deliberate exception to the ordinary metadata/visuals split. It is
default-off and has no useful non-visual extraction path, so a new HTML item is
not inserted unless its first screenshot succeeds. Missing browsers and
persistent render failures are recorded in `scan_errors` at the metadata stage;
already-indexed HTML items are not retroactively removed if the renderer
disappears.

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
  AND stage IN ('header', 'decode')
  AND mime_type LIKE 'image/%';
```

(Both image stages, because a decoder upgrade can change either verdict; only
the `header` half actually un-blocks a file, the `decode` half clears audit
rows whose files are already indexed. The first shipped directive,
`20260801140000_retry_image_decode_unfused.sql`, is the un-fusing's own.)

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

A directive targeting `header`, `metadata` or `decode` rows must **always**
constrain on a mime family (`mime_type LIKE 'image/%'`), never on
`stage`/`error_class` alone. The scan's post-failure content sniff records
what a file's leading bytes actually are when they contradict its name —
`application/applefile` for the AppleDouble resource forks macOS scatters
over a share, `text/html` for an error page served in place of the file —
and those rows sit in the same `(stage, error_class)` space as the honest
failures: an AppleDouble named `.png` is an `input` verdict at stage
`header`, exactly like a genuinely broken image. A predicate without the
mime family therefore resurrects rows that no decoder and no tool upgrade
can ever fix, and they fail again for the same reason they failed the first
time. The mime family is what makes the directive say what it means.

`stage = 'mime'` is the one exception, and it is structural: the guess
itself is what failed there, so those rows carry `mime_type = NULL` and
never a sniffed verdict either. A mime predicate would exclude every one of
them forever, so they are targeted by stage alone — which is safe, since
nothing else lands in that stage.

The sniffed mime is authoritative only for rows written from the sniff
onward. A row recorded before it existed still carries the extension guess,
and the suppression gate means a confirmed row is never re-processed on its
own, so it keeps that guess indefinitely. The correction rides on the
directives themselves: if a mime-constrained directive resets such a row,
its one re-attempt re-records it through the sniff and it is truthful from
then on. That self-correcting path is the accepted cost — a one-shot
backfill migration was considered and rejected, since no released build ever
wrote pre-sniff rows at scale.

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

*As implemented.* The header parse is `ImageReader::open` +
`with_guessed_format` + `into_dimensions` — the same check extraction makes
before handing bytes to a worker (`ensure_image_readable`), with one
difference: extraction sniffs a buffer, while the scan opens by path and so
seeds the format from the extension before sniffing. A file whose bytes no
sniff recognizes but whose extension is known therefore passes the scan gate
and can still be rejected by extraction. That is the permissive direction —
the file is indexed and the consumer decides — which is why the seed was left
alone. No limits are set **by the header parse**, because reading a header
allocates nothing worth a configurable ceiling; that is not the same as no
limits at all, and the distinction matters (see below). Dimensions were the *only* thing the metadata
phase ever took from the decoded image (no EXIF, no colour information), so
nothing moved to the visuals phase and nothing was dropped; an image indexed
through this path therefore always has width and height, and no nullable-dims
work is owed. The un-fusing also removed the `preloaded_image` parameter that
threaded the fused decode from `prepare_new_item` into the visuals generator,
which is what made the batch walker and the continuous scan (`process_file`)
two different code paths for images — they are now the same one, and the
continuous scan stopped decoding twice per image.

Three things this section did not anticipate:

- **The header needs its own stage.** `scan_errors.stage` is the
  discriminator for what a row *is*: `mime`, `metadata` and the new `header`
  are failures that kept the file out of the index, so they suppress the next
  scan's re-attempt; `decode` no longer is, so it is **audit-only** — it never
  suppresses (`stage_blocks_indexing`), and it is not spent by its own file's
  success either, since that file now succeeds on every scan. Only bytes that
  moved retire it (plus the sweep and the directives). Suppressing on a
  `decode` row would have been the real hazard: the walker's ledger check runs
  before the mtime shortcut, so a confirmed row would have taken an *indexed*
  file out of the walk for the rest of its life. Keeping both verdicts under
  one stage was rejected for the same reason — a directive can still target
  both with `stage IN ('header','decode')`, but the scheduler cannot. Rows
  written by the fused gate are cleared by a retry directive
  (`20260801140000_retry_image_decode_unfused.sql`), which is exactly the
  mechanism this document proposes for the case.
- **The blurhash re-decode escaped the marker.** Step 4's dispatcher consults
  the thumbnail marker only when a thumbnail is *wanted*, which for the
  majority of an image library it never is (served directly from the
  original). Their blurhash still costs the same full decode, and before the
  un-fusing that decode could not fail on an indexed file — so a corrupt
  served-directly image would have been re-opened and re-decoded on every scan
  forever, with nothing recording why. The dispatcher now consults the same
  marker on the blurhash-only path, and the backfill's blurhash fallback
  records its own decode failure as a thumbnail verdict (the decode *is* what
  a thumbnail pass would have done). This is the only marker-consult a healthy
  library ever pays for, and only for images that still owe a blurhash.
- **A header parse can fail on limits.** Setting no limits means the image
  crate's *defaults*, not none — including a 512 MiB allocation cap, which is
  stricter than the configurable `image_decode_memory_limit_mb` the decode
  runs under. A header declaring an absurd width (an IHDR of 200,000,000, and
  a single row is enough) therefore fails `into_dimensions` with
  `ImageError::Limits`, i.e. the metadata phase can reject a file over a
  budget rather than over its content. Both classifiers map
  `(Header, Limits)` to `resource`, the same as their decode arms: a verdict
  on this machine, settled at one attempt and clearable by a directive once
  the ceiling moves, never a claim that the file is corrupt.

Both `visual_attempts` and `scan_errors` are written for a failed image
decode, and they are not redundant: the marker is the **schedule** (it is what
the next scan reads, and what confirms at `skip_after`), the row is the
**audit record** (requirement 4 — the failures API and card read `scan_errors`
and know nothing about markers). The asymmetry with the other visuals
failures — a broken PDF gets a marker and no row — is deliberate: every other
type has always been indexed without visuals, so none of them is news, while
the image decode is the class this phase newly admits into the index. Widening
the audit surface to all visuals failures is a separate, easy decision (a
`visual_attempts` listing endpoint would cover all of them at once).

The row is written by **both** decode sites — the new-item path (shared by the
two walkers) and the backfill, whose thumbnail half and blurhash fallback are
the only decodes an already-indexed file ever gets. Writing it in one place was
the first attempt and was wrong in three ways: `attempts` would have frozen at
1 while the marker counted to its threshold and suppressed, leaving the audit
surface saying "1/2 · will retry" about a file nothing would retry; an image
that was indexed decodable and then rotted in place would never have got a row
at all, since it never revisits the new-item path; and neither would one whose
marker a generator-version bump retired. Both sites write, so `attempts` moves
in lockstep with the marker — 1 on the pass that indexed the file, 2 on the
backfill that confirms it — and once the marker suppresses, nothing decodes and
nothing writes. `skip_after` travels for the directives' benefit.

Only an actual **decode** failure writes a row. An encode failure on pixels
that decoded fine (the thumbnail encoder, on an image the crate read happily)
is a generator problem, not a verdict on the file, so it gets the
`visual_attempts` marker and nothing else — the same treatment as a PDF or an
HTML page that fails to render. The failing site says which it was, exactly as
it already names the kinds it invalidates; the mime type cannot tell them
apart.

Two more consequences worth stating: the thumbnail endpoint already serves any
image with no stored thumbnail from its original file, with no size gate
(`api/items.rs`, the `mime.starts_with("image")` fallback after the stored
lookup), so an indexed-without-visuals image needs nothing from the API side —
non-images keep the placeholder, which is the broken-PDF precedent. And the
class of file this admits — image-crate-rejected but PIL-processable — now
reaches extraction, where the header check and the PIL arbiter decide it as
the arbiter principle intends.

## Audit surface (req 4)

- **API**: `GET /api/jobs/data/failures` (extraction ledger joined with
  `items`+`files` for path/sha256/mime, filterable by setter, class, stage,
  mime prefix; paginated) and `GET /api/jobs/scan/failures` (scan ledger).
  Since run2 the extraction response carries **three** lists, each with its
  own total and all sharing the `limit`/`offset` window: `failures` (the
  ledger's verdicts, unchanged), `job_failures` (items a job could not finish
  and has no verdict for, with the same representative-path join), and
  `failed_jobs` (the `partial`/`failed`/`cancelled` job records, each with a
  real `end_time`, the unexplained-failure count, the segment count and the
  reason). `error_class` and `mime_prefix` describe a *verdict*, so a request
  carrying either answers with the two job-side lists empty and their totals
  zero rather than with an unfiltered approximation.
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

*As implemented.* All five shipped on 2026-08-01, in that order, as seven
commits (phase 1 split across three: taxonomy and tables, extraction, scan).
Phase 5 turned out not to be independent of phase 4 after all — un-fusing the
decode is what makes a decode failure possible on an *indexed* file, which is
what exposed the blurhash hole in the phase-4 dispatcher (above).

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
