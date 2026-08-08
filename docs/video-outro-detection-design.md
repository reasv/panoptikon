# Video outro detection (TikTok end cards)

Status: **implemented and validated** (2026-08-08, five commits ending at the
equivalence harness). Measurement and validation were done 2026-08-07 against
real data; the numbers below are measured, not estimated. Re-validated
2026-08-08 over 2,703 files including 2025–26 material (§4.1); review
resolutions (§7.1, §7.2), the API/PQL scope decision (§6.3) and the
first-scan backfill acceptance (§7) folded the same day. §12's equivalence
requirement was executed at implementation time: 878 files (821 unique)
sampled deterministically from `tiktok`/`camera` (positives) and
`screenshots`/`default`/`rustest`/`rustest2` (negatives) — **zero verdict
mismatches, zero K mismatches** (271 accepted, exact float equality),
identical rejection-reason histograms per DB, identical error classes
(3 files, 0.34%), zero acceptances in 578 negative rows, K clustering on
2.00/3.00/4.00 with 98.5% within ±0.04. Detector:
[`media_tools/outro.rs`](../panoptikon/src/media_tools/outro.rs); harness:
[`tools/outro-equivalence/`](../tools/outro-equivalence/).

## 1. Problem

TikTok appends an end card to videos exported through its own "Save video"
button: a solid dark field with the TikTok logo, the creator handle in a search
bar, and a loud sound. It is **appended**, never overlaid — the original content
is intact and the card sits strictly after it.

This is a problem for viewing (the sound is much louder than the content, and it
makes intended-loop videos unloopable), and a problem for indexing: frame
extraction samples 4 frames evenly across the whole video, so on a
TikTok-dominated database a large share of items have a card frame among their
sampled frames. Every downstream consumer — thumbnails, CLIP embeddings, tags,
OCR — is then partly describing a logo.

Both problems are solved by the same fact: **where the real content ends**.

Scope of this document: **backend detection and measurement only**, plus the
metadata that carries the result. Playback-side trimming is out of scope (see
§10).

## 2. What was measured

Sample: 342 files from the `tiktok` index DB, stratified across 2020–2024 by
`files.last_modified`, zero probe errors. Cross-checked against ~1,400 videos
from the `camera`, `default`, `images`, `screenshots`, `rustest`, `rustest2`
databases.

### 2.1 The card is not one thing — there are (at least) three generations

| K (card length) | share of carded files | design | observed years |
|---|---|---|---|
| **4.00s** | 88% | `TikTok` logo + `@handle` in a search bar | all |
| **2.00s** | 10% | "Discover more creators on TikTok" + avatar, logo at bottom | 2023+ only |
| 3.00s | 2% | as 4.00s but the logo fades in | scattered |

Within a generation K is *near*-exact. The original sample read as ±1 frame
of 30fps quantisation; re-validation over 716 accepted files (§4.1) sharpens
this: **97.2% land within ±0.04s** of 2/3/4, but the spread is one-sided — a
positive tail reaching +12 frames, including a tight 10-file group at exactly
+2 frames (4.0667) that may be a real minor variant rather than jitter.
Operationally irrelevant: the per-file *measured* K is what gets stored,
never the nominal generation value. The content→card transition is a
single-frame hard cut, no fade.

The 2.00s card is growing: 0% of 2021–22 saves, 11% of 2023, **22% of 2024**,
and 18.2% of all accepted positives in the 2026-08-08 re-validation (§4.1),
whose `camera` material runs through 2026-08-07. No successor generation has
appeared: detection holds through 2026 with no unknown card background
observed.

**Consequence: no single blind constant works.** 4.15s over-trims the 2.00s card
by 2.15s of real content; 2.15s leaves 2s of card plus the full bang on 88% of
files. A naive "−3s" is the worst of all: because the bang fires in the first
~10ms of the card, it delivers the entire transient on 88% of files while
appearing to trim.

### 2.2 The fingerprint

Every card frame, across all generations, has frame-median colour
**RGB(12, 13, 25)** exactly. Letterbox bars in content are pure black
RGB(0,0,0) and are cleanly separable. This is the primary signal, but it is
**not sufficient on its own** — see §3.

### 2.3 Audio (recorded for the playback work, not used by detection)

- Card audio peaks at **−1.7 dBFS** median (near full scale), near-identical
  across files — a fixed asset.
- Content median is **−20.4 dBFS**. Difference: **+17.5 dB median, +35 dB p90**.
- The bang **leads the first card frame** by 0–60ms (median 10ms), so a cut
  exactly at the video boundary still leaks the transient.
- The pre-bang silent gap does **not** generalise (median 0.02s).
- ~19% of 4.00s cards have no loud bang at all but still carry the visual card.

For playback trimming, the derived cut point is **K + 0.15s** (covers the 60ms
audio lead plus rAF jitter, costs 150ms of content).

### 2.4 Population

`camera` (the phone roll, 13,281 videos) is ~87% TikToks — confirmed because its
K values cluster on exactly 2.00/3.00/4.00, which a colour coincidence cannot
produce. This is the database where re-extraction will have the most impact.

## 3. Detection algorithm

Colour alone produces false positives: a dark-mode Discord screen recording has
median RGB(13,19,22), inside tolerance of the card's (12,13,25). Three
structural rules fix this, and each earned rejections in the adversarial set.

### 3.1 Stage 1 — cheap gate (one frame)

Any carded file ends *inside* the card, so its final frame is a card frame.

- Decode the **last frame only** (`-sseof -0.35`, take the final frame).
- Squash to a fixed **32×32**, ignoring aspect ratio — a median-colour test does
  not care about geometry. This removes the need for an `ffprobe` call.
- Promote if `max|median − (12,13,25)| ≤ 8`.

Measured promotion rate: `default` 0%, `rustest` 0%, `screenshots` 2.5%,
`images` 5.2%, `camera` 87.2%. Recall on known positives: **312/312 (100%)**.

The gate is a *rejector only*. It may be run loose — stage 2 is the arbiter, and
a loose gate is the cheap insurance against a new card generation whose
background differs by a few levels.

### 3.2 Stage 2 — boundary scan (tail decode)

Only for promoted files.

Decode the last **7s** at **30fps**, scaled to **48px wide** (aspect preserved,
`scale=48:-2`), `rgb24`.

Per frame `i` of `n`:

- `med[i]` = median RGB over the frame.
- `on_bg[i]` = `max|med[i] − (12,13,25)| ≤ 8`.
- `bgfrac[i]` = fraction of pixels within 12 levels (max channel) of `med[i]`.
- `card[i] = on_bg[i] && bgfrac[i] ≥ 0.45`.

`bgfrac` must stay this permissive: in **square (576×576) videos the logo and
search bar occupy a far larger share of the frame** than in 9:16, and a 0.80
threshold silently truncated the run and reported K=3.50 instead of 4.00. This
was a real bug found during measurement.

Terminal run, gap-tolerant (bridges the animated search-bar sweep, which
transiently fails `bgfrac`):

```
i = smallest index such that card[i] and mean(card[i:]) >= 0.90
run  = (n - i) / 30
lead = i / 30
```

Then, in order:

| rule | test | rejects |
|---|---|---|
| R0 | `run ≥ 1.0s` | no card present |
| **R1 boundary** | `lead ≥ 0.40s` | **uniformly dark videos** — a card is a *transition*, not a state. If the whole window matches, no boundary was found. This alone kills the dark-mode-capture class. |
| **R2 cap** | `run ≤ 5.0s` | anything longer than any observed card |
| **R3 layout** | `ink_rows ≤ 0.60` | UI chrome. On the final frame, `ink = max|px − (12,13,25)| > 25`; `ink_rows` = fraction of rows containing any ink. The card is a near-empty field with ink in a few central rows; UI spreads ink across nearly every row. A fraction, so aspect-robust. |

Result: `K = run` (seconds of outro measured from end of file).

### 3.3 Reference implementation (Python, for re-validation)

```python
W, FPS, TAIL_S = 48, 30, 7
CARD_BG = np.array([12, 13, 25])
TOL, BGFRAC_MIN = 8, 0.45
MIN_RUN_S, MIN_LEAD_S, K_CAP = 1.0, 0.40, 5.0
INK_DELTA, INK_ROWS_MAX = 25, 0.60

# ffmpeg -v error -sseof -7 -i PATH -vf "fps=30,scale=48:-2,format=rgb24" -f rawvideo -
a    = frames                      # (n, h, W, 3) int16
flat = a.reshape(n, h * W, 3)
med  = np.median(flat, 1)
on_bg= np.abs(med - CARD_BG).max(1) <= TOL
near = (np.abs(flat - med[:, None, :]).max(2) <= 12).mean(1)
card = on_bg & (near >= BGFRAC_MIN)

i = n
for j in range(n - 1, -1, -1):
    if card[j] and card[j:].mean() >= 0.90:
        i = j
run, lead = (n - i) / FPS, i / FPS

if run < MIN_RUN_S:  reject("no-run")
if lead < MIN_LEAD_S: reject("no-boundary")
if run > K_CAP:      reject("too-long")
ink = np.abs(a[-1] - CARD_BG).max(2) > INK_DELTA
if float(ink.any(1).mean()) > INK_ROWS_MAX: reject("layout")
accept(K=run)
```

### 3.4 Decode-layer traps (found during re-validation)

- **`scale=48:-2` height rounding.** ffmpeg rounds the derived height
  half-*up*; a language-default banker's rounding computes 68 where ffmpeg
  produces 70 (576×828 → 828·48/576 = 69.0), the rawvideo buffer fails to
  reshape, and the file is misclassified as a probe error. Match ffmpeg:
  `h_scaled = int(h·48/w / 2 + 0.5) · 2`.
- **`-sseof` is not guaranteed.** On 2 of 2,703 files ffmpeg ignored the tail
  seek and decoded the whole video. Harmless *only because* K is anchored to
  the end of the stream (§12) — but the decode loop must never assume
  ≤ 210 frames.
- **As implemented, the height formula is the fallback, not the primary.**
  Stored `items.width`/`height` are *coded* dimensions, while the filter
  graph sees the auto-rotated size — deriving the height from stored dims is
  wrong on every rotated phone capture, i.e. the `camera` population. The
  Rust detector therefore parses the scaled geometry off ffmpeg's own
  output-stream log line (byte-robust drain; `Stream #` lines inside the
  `Output #` block only), and uses the formula above only as a fallback
  corroborated against the received byte count (ambiguity → probe error,
  never a guess).

## 4. Validation

| set | what it is | n | flagged |
|---|---|---|---|
| **screenshots** | screen recordings — adversarial | 392 | **0** |
| **default** | general video | 395 | **0** |
| **rustest / rustest2** | general video | 246 | **0** |
| images | downloads incl. social reposts | 400 | 15 (3.8%) |
| tiktok | known positives | 342 | 327 (95.6%) |

**Zero false positives in 1,033 general and adversarial videos.**

The 15 `images` hits were individually verified as **genuine** TikTok cards
arriving by other routes (one is a TikTok reposted to X and downloaded from
there; another has a raw TikTok CDN filename `v12044gd0000d01rsgfog65hfo08ijp0.mp4`).
Detecting these is correct behaviour, not a false positive.

Rejection reasons on the adversarial set confirm each rule pays for itself:
`screenshots` produced 5 `no-boundary` (R1) and 1 `layout` (R3) rejections.

Non-carded files in the `tiktok` folder (~9%) are genuine: they carry the
roaming watermark but no card, and score run=0.00 — a decisive rejection, not a
threshold near-miss.

### 4.1 Re-validation (2026-08-08)

Full re-run of §3.3 over 2,703 files, including the recency coverage §11
originally lacked (`camera` holds 11,687 videos from 2025–26, through
2026-08-07):

- **0 flagged in 1,338** general/adversarial videos; `screenshots` reproduced
  the rejection fingerprint exactly (5 × R1, 1 × R3). R2 fired **zero** times
  in the entire run — retained as pure safety.
- `tiktok` 94.0% (n=299), `camera` uniform-random 92.0% (n=299). Detection
  *rises* with recency: 96.6% of 2025 files, 95.8% of 2026, 96.7% of the 30
  newest (all 2026-08-07).
- Every near-uniform 2025+ rejection sits on pure black RGB(0,0,0) — the
  separable letterbox/fade class of §2.2 — never on a new dark constant. The
  RGB(12,13,25) fingerprint is intact through 2026.
- `images` measured 1.0% vs the original 3.8%: the DB has since grown to
  4,000 videos, changing the denominator; all 5 hits remain the
  genuine-repost class (one carries TikTok's 576×1024 export geometry).
- Probe errors 0.37% (missing moov atom, no video stream, invalid color
  space, one non-square-SAR webm) — same classes and rate as originally
  observed.

## 5. Cost

| | ms |
|---|---|
| `ffmpeg` process spawn, doing nothing | **50** |
| stage 1 gate (1 frame), local file | 95 |
| stage 1 gate (1 frame), SMB file | 91 |
| stage 2 full tail scan, SMB | 191 |

Key facts for the implementer:

- **The cost is process spawn, not decoding, and not I/O.** SMB is not the
  bottleneck (91ms vs 95ms local). "Decode fewer frames" buys almost nothing;
  "spawn fewer processes" buys everything.
- The probe is **~85ms flat regardless of file size** — it only reads the tail.
- Against scan's existing full-file sha256, measured over 14 camera videos
  (48 MB total): sha256 7431ms, probe 1310ms → **+17.6%**. Per file: 12–27% for
  typical 0.5–3 MB TikToks, 5.2% for a 24 MB file.
- One-time backfill for `camera` (13,281 videos) ≈ 19 min single-threaded.
- Two-stage vs always-full on a general library: ~95ms vs 190ms average, ~2×.
- **Future optimisation:** an in-process decode (ffmpeg bindings) would remove
  the 50ms spawn per file. panoptikon currently shells out everywhere
  ([`media_tools.rs`](../panoptikon/src/media_tools.rs)), so a subprocess is
  consistent with existing architecture; this is not required for v1.

## 6. Data model

Two nullable columns on `items`, alongside `duration`/`width`/`height`. This is
an **immutable per-item property**: any content change yields a new sha256 and
therefore a new item, exactly as documented for `duration` at
[`image_frames.rs:501`](../panoptikon/src/jobs/extraction/input_handlers/image_frames.rs).
It is measured once and never revisited.

| column | type | meaning |
|---|---|---|
| `outro_kind` | TEXT NULL | `NULL` = never examined; `'none/1'` = examined, no outro; `'tiktok_card/1'` = outro found |
| `content_end_ms` | INTEGER NULL | where real content ends; set only when an outro was found |

Deliberately **generic naming** (`outro`, not `tiktok`): other platforms append
outros, and the frame-sampling fix wants the general concept.

### 6.1 Three states are required

`content_end_ms` alone cannot express "examined, nothing found" — NULL would be
ambiguous. `outro_kind` carries the state, and it is what makes the
"negatives are never re-examined" property actually hold.

### 6.2 Versioning lives in the kind value

The `/1` suffix is the **detector version**. A future detector selects rows
whose kind-version it does not recognise and re-runs only those. This gives
versioning with zero extra columns and zero future migration, and — importantly
— **negatives carry their detector version too**.

Rationale for not using a separate column: adding one later with a default of
`1` for existing rows is equivalent *only if it ships in the same release as the
first detection change*. Tweak a threshold before then and you have a mixed
population that cannot be identified retroactively. Since `ink_rows`, the K cap
and the generation list are all tuned on a single library, such a tweak is
likely, and it is exactly the kind of change that ships without anyone thinking
"migration". Encoding the version in a column that is being added anyway removes
the whole failure mode for free.

**Any change to detection behaviour must bump this version.**

### 6.3 API and PQL exposure (in scope for v1)

Both columns ship as standard item metadata, read-only, in the same release
as detection — settled 2026-08-08. Deferring exposure would save nothing
(this implementation round pays the `.d.ts` regen anyway) and cost a second
API-churn round later; and it is immediately useful for validating the
feature itself ("everything detected as `tiktok_card`, sorted by K" becomes
a PQL one-liner instead of a SQL probe).

Concretely, `outro_kind` and `content_end_ms` follow `duration` through
every seam it already passes:

- item metadata response ([`api/items.rs`](../panoptikon/src/api/items.rs))
  and search result columns + the column whitelist
  ([`api/search.rs`](../panoptikon/src/api/search.rs));
- PQL `Column` enums ([`pql/model.rs`](../panoptikon/src/pql/model.rs)),
  `MatchValues`/`MatchValue` and their mapping blocks
  ([`match_filter.rs`](../panoptikon/src/pql/builder/filters/match_filter.rs)),
  and the emptiness checks in
  [`preprocess.rs`](../panoptikon/src/pql/preprocess.rs).

Presence semantics: because the stored kind value carries the detector
version (§6.2), raw equality on `outro_kind` breaks silently on a version
bump. "Has an outro" is therefore `content_end_ms` non-null; kind-specific
queries prefix-match (`tiktok_card/`). The raw stored value is the only
served form — no stripping at the API boundary, no stored-vs-served
divergence.

## 7. Pipeline placement

Detection must run **before frame extraction**, so extraction can sample only
the pre-outro range.

- Runs at **file scan** time, for video items only, subordinate to
  `scan_video` (off when video scanning is off, regardless of its own setting).
- Existing items with `outro_kind IS NULL` are picked up by the next scan, the
  same way a missing thumbnail is. This backfills automatically with no
  migration and no separate job. It is real dispatcher work, not a free ride:
  today the dispatcher's only questions are "is there a thumbnail" and "is
  there a blurhash" — frames-only gaps are deliberately invisible, an
  invariant pinned by the `a_video_missing_only_frames_is_not_dispatched`
  test in [`jobs/files.rs`](../panoptikon/src/jobs/files.rs) (~line 6789).
  The outro backfill adds a new dispatch question ("video with
  `outro_kind IS NULL`?"), and that pinned test must evolve together with it,
  exactly as its own comment anticipates. The first post-upgrade scan
  absorbs the whole backfill — the probe pass *and* §7.1's thumbnail/frame
  regeneration for every newly positive item — in one go. Accepted as-is
  (settled 2026-08-08): no pacing, no per-scan cap; scan duration is not a
  design input. Probe wall-clock is attributed to the existing thumbgen
  phase (it is visuals work serving the clamp); no new timer phase exists.
  One known cost asymmetry: the *continuous* scan has no DB handle at probe
  time, so a file whose mtime moves is re-probed and its (identical) verdict
  rewritten — §6.1's "negatives are never re-examined" holds for the batch
  walker, and is cost-only, never wrong, for the watcher.
- Frame/thumbnail generation clamps its sampling window to
  `[0, content_end_ms)` when set.
  Call sites: [`jobs/files.rs:4415`](../panoptikon/src/jobs/files.rs)
  (`extract_video_frames`, scan-side thumbnails/frames) and
  [`image_frames.rs:507`](../panoptikon/src/jobs/extraction/input_handlers/image_frames.rs)
  (extraction-side). The clamp needs a decode bound (`-t`/`-to`) or
  truncation of the emitted frame list, not just a recomputed interval: both
  call sites sample with `fps=1/interval` over the whole file, so shrinking
  the interval alone still emits card frames past the boundary.

### 7.1 Re-extraction trigger

Replace existing thumbnails and frames **only when an item newly becomes
positive** — not on every scan, and not for negatives, where nothing changed and
the existing outputs are already correct.

Derived ML data (CLIP embeddings, tags, OCR) is **left untouched** — settled
2026-08-08. Existing outputs are valid, merely computed over frames that
included the card: better is possible, wrong they are not. **No
feature-specific re-index mechanism ships**, neither automatic nor as a
user-facing action — a dozen features like this one over a couple of years
would each accrete their own one-off machinery, and the principled fix (a
dependency-graph system stamping item_data with a commitment over its inputs)
is a separate, undecided project. Recovery is the standard path that already
exists: erase a model's data and re-run its extraction job, exercised
manually by whoever cares. On `camera` (~87% of 13,281 videos) that is a
deliberate, user-initiated act, never a scan side effect.

### 7.2 Probe failures: the visuals ledger

Probe failures are stored in **neither** `outro_kind` (the column only ever
holds genuine verdicts) **nor** left `NULL` (retry-forever is exactly the
pre-ledger disease the failed-media work cured). They go through the existing
negative cache `storage.visual_attempts`
([`db/visual_attempts.rs`](../panoptikon/src/db/visual_attempts.rs)) as a new
**kind** (`VisualKind::Outro`, kind string `"outro"`) — the schema explicitly
supports new kinds without rebuild. The ledger's vocabulary maps unchanged:

- ffmpeg fails to spawn → `blocked` with `blocker = 'ffmpeg'`; cleared by the
  existing scan-start auto-heal when the toolchain binds.
- ffmpeg runs and fails → `failed` with `skip_after = 2`: the probe does its
  own file I/O, so a broken file and a transient mount hiccup exit
  identically — the same two-strike ambiguity rule, for the same reason, that
  frame extraction already applies. One SMB blip mid-backfill must not
  permanently stamp thousands of files.
- Consult: a video with `outro_kind IS NULL` checks
  `visuals_suppressed(sha, Outro, OUTRO_DETECTOR_VERSION)` before probing.
- A successful verdict write deletes the marker, mirroring
  `store_thumbnails`; the index-side write and the `storage.` delete share
  one transaction (connections have both databases attached).
- The ledger's integer `version` column carries the detector version — the
  same number as §6.2's `/N` suffix. A detector bump then retires failure
  markers for free via the existing `version >= ?` consult (confirmation
  counts restart, per the ledger's own rule), while the unrecognised-suffix
  selection recovers the negatives: two populations, each recovered by
  machinery already built for it, no new invalidation code.

One acknowledged semantic stretch: `visual_attempts` is documented as
shadowing the *storage-side* positive caches ("deleting storage.db drops the
markers with the visuals"), but the outro verdict lives in index.db. The
advisory property holds in both mismatch directions — a marker orphaned by a
storage.db wipe costs one ~85ms re-probe; one surviving an index.db rebuild
correctly suppresses re-probing a file that would fail again; never a wrong
answer — and the alternative is a parallel index-side ledger, i.e. duplicated
machinery for what measured 0.37% of files (§4.1). Note the stretch where
`VisualKind::Outro` is defined.

## 8. Configuration

New field on `SystemConfig`
([`db/system_config.rs:82`](../panoptikon/src/db/system_config.rs)):

```rust
#[serde(default = "default_true")]
pub detect_outros: bool,
```

Per `CLAUDE.md`, per-DB `config.toml` is serialised from
`SystemConfig::default()` at creation and serde defaults fill absent keys at
load — so this is **functionally correct for existing databases with no
migration and no TOML edit**. Opt-out, enabled by default.

Justified by the data: 0 false positives in 1,033 general/adversarial videos,
and +17.6% over hashing.

Semantics:

- **Off** → future scans skip detection; consumers ignore the metadata.
  Already-stored values are retained.
- **On again** → items still `NULL` are picked up by the next scan naturally.

### 8.1 Known asymmetry: disabling is not fully reversible

Turning the setting off does **not** revert thumbnails and frames already
regenerated against the trimmed range — those stay trimmed until something else
forces re-extraction. This is accepted (trimmed thumbnails are better
regardless), but the UI description must not imply the switch undoes anything.

As implemented, "consumers ignore the metadata" is literal: with the switch
off, both the scan-side and extraction-side clamps stop reading the stored
boundary, so a *regeneration* while off produces untrimmed visuals — the
false-positive escape hatch. Note the hatch is scan-side only: extraction's
frame cache (`storage.frames`) is returned before the gate is consulted, and
§7.1's recovery path (erase `item_data`, re-run) does not clear that cache —
undoing a false positive takes a scan-side regeneration, which is the only
path that replaces `storage.frames`.

## 9. UI surface

A switch-card in **Scan Configuration**, alongside Image/Video/Audio/PDF/HTML
Files and Remove Unavailable Files, and identically in both wizard flows
(new DB and onboarding). The desktop-side config surface is
[`api/desktop.rs:72`](../panoptikon/src/api/desktop.rs).

Precedent for a non-file-type toggle living in that grid: **Remove Unavailable
Files** is already a behaviour toggle, not a file type.

**Label it for the action, not the file type.** Proposed:

> **TikTok Detection** — *Detect TikTok end cards so thumbnails and AI skip them.*

Do **not** label it "TikTok Files". The five neighbouring toggles all mean
"these files do not enter the database", and that mental model is strong enough
that "TikTok Files: off" would read as "do not index my TikToks". That misreads
in both directions: a user wanting to exclude TikToks turns it off and they are
still indexed, and a user who only wants detection off may avoid the switch for
fear of losing files.

## 10. Out of scope here

- **Playback trimming.** The gallery/pinboard clip mechanism
  ([`ui/lib/videoTrim.ts`](../ui/lib/videoTrim.ts)) is already rAF-driven and
  roughly frame-accurate; it only ever needed a correct number. That number
  is already served (§6.3), so the remaining work is UI-only. When it
  happens: cut at `content_end_ms − 150ms`, render the video **without** the
  native `loop` attribute (see the header comment in that file), and guard
  `duration <= K`.
- Detecting TikToks as such. The question answered here is *"does this file end
  with a known outro"*, which is decidable from pixels and is the only question
  the feature needs. A TikTok without a card is correctly ineligible — there is
  nothing to trim.

## 11. Known gaps and risks

- **Validated against one library.** Two clear generations plus a probable third
  (3.00s; 26 hits in `camera`, one confirmed visually as a fade-in variant).
- **New generations degrade gracefully.** A card with a different background
  fails the colour filter, the item gets `outro_kind = 'none/1'`, and nothing
  happens. Absent behaviour, never wrong behaviour. But note this means a new
  generation is *recorded as a negative* — which is precisely why §6.2's version
  bump is the mechanism that recovers those items.
- **Probe failures fail closed via the visuals ledger** (§7.2): consumers see
  no `content_end_ms` and behave as if no outro exists; the failure itself is
  recorded in `visual_attempts`, never in `outro_kind`. Measured failure rate
  0.37% (§4.1).
- **~~Corpus ends late 2024~~ — closed by the 2026-08-08 re-validation.**
  `camera` covers 2025–26 through 2026-08-07 with detection rates *rising*
  over time and no successor generation observed (§4.1). Still a single
  user's library.
- Thresholds `INK_ROWS_MAX`, `K_CAP` and `BGFRAC_MIN` are tuned on this data.
  Changing any of them is a detector-version bump (§6.2).

## 12. Reproduction

Measurement was done with throwaway Python driving `ffmpeg`/`ffprobe`; §3.3 is
the complete algorithm. To re-validate after a change, run the detector across
samples from `data/index/{tiktok,camera}` (positives) and
`data/index/{screenshots,default,rustest,rustest2}` (negatives) and confirm:
K clusters on discrete values, and the negative sets stay at zero.

Note when re-measuring: the `Z:` share is a network mount
(`\\192.168.1.16\z`) that the Bash sandbox cannot reach — drive `ffmpeg` from
PowerShell instead. Anchor K to the **end** of the file
(`K = (N − i) / fps`) so `-sseof` seek accuracy never enters the measurement.

The Rust implementation must be validated for **equivalence against the §3.3
Python reference** before shipping: run both over the same sample and require
identical verdicts and K values (the pql-equivalence discipline). Divergence
risk concentrates in exactly two places — median semantics (numpy's median
over an even-length axis averages the two middle values; the Rust side must
match) and the `scale=48:-2` height rounding of §3.4.

This was executed at implementation time (see Status) via
[`tools/outro-equivalence/`](../tools/outro-equivalence/): a faithful §3.3
Python reference plus a runner driving
`media_tools::outro_equivalence` — an `#[ignore]`d `cfg(test)` entry point,
the only form with access to the detector's `pub(crate)` surface in a
bin-only crate, adding nothing to the shipped binary. Two operational
requirements the harness enforces that this section originally did not
name: both engines must be pinned to the **same ffmpeg binary** (the
detector otherwise resolves the venv's static-ffmpeg — a different build
would be a divergence blamed on the algorithm), and the Python reference
must apply ffprobe's rotation side-data (coded dims vs auto-rotated filter
graph, §3.4) before computing the scaled height.
