# Thumbnail formats, grid-xs, display tier, transparency — implementation plan

Converged 2026-09-01 from a four-phase bake-off over the user's four real
corpora (scratchpad `bakeoff/report.md`: A = encoders, B = browser decode
speed, C = display-tier thresholds + transparency census, D = pure-Rust JPEG
encoder) and a backend survey. Ships before the next release: every change
here rewrites stored bytes, and after the release each such change costs every
user a library-wide regeneration.

Companion to `docs/grid-scroll-performance-implementation.md` (the tier
ladder, whose §2 rules — decide-from-indexed-metadata, never decode in the
dispatcher, write-once, the finality oracle — all stand).

## 1. What the measurements settled

| finding | consequence |
|---|---|
| Today's encoder (`image` crate JpegEncoder q85) is **baseline 4:4:4, standard Huffman** — the crate's "4:2:2" doc comment is stale (verified in source) | mozjpeg/WebP at 4:2:0 are *quality regressions* unless 4:4:4; every comparison below is at 4:4:4 |
| A byte contest between quality-unequal candidates picks the worse file 82% of the time | **no per-image byte contest**; format is a *policy*, chosen per content class |
| WebP decodes 2.2–2.7× slower than JPEG per megapixel (Phase B); the grid is decode-bound | **grid tiers stay JPEG** |
| Pure-Rust `jpeg-encoder` 0.7 (baseline, 4:4:4, optimized Huffman, mozjpeg's quant table 3) = **91% of today's bytes, SSIM above today, 0.85–0.98× encode time** — mozjpeg gets 81% but needs `nasm` on every build leg | **grid encoder = `jpeg-encoder`**; mozjpeg recorded as a future upgrade (§9) |
| WebP rendition of a **PNG** original: ~90% bytes saved from 2 MiB up, decode parity or better; of a **JPEG** original: 60% saved but **2.33× slower decode** (Phase C) | the *format* of a display rendition follows the source: lossless sources → WebP, JPEG sources → JPEG (a JPEG downscale is smaller AND faster than the original) |
| Only 2.3% of PNGs have a non-opaque pixel (50% carry an alpha *channel*); alpha costs +2.5% bytes in WebP | transparency is decided by **pixels, never the header**; cheap to honour |
| Short-side cap 2560 removes 32% more bytes for −0.0025 mean screen-fit SSIM; 2160 is the exact 4K 1:1 floor with no margin | **rendition cap 2560** (the *trigger* for storing one is 4096 short / 24 MP / a per-class byte bound) |
| `image-webp` encodes lossless only; `libwebp-sys` is vendored C built with `cc` alone | one new C dependency (§9); no `nasm`, no `cmake` |
| WebP cannot encode a side > 16383 px | JPEG fallback for those (tall strips) |
| Display ETag is `sha-thumb{idx}` — no version, served immutable; display regeneration has no trigger (geometry-only compare, `THUMBNAIL_PROCESS_VERSION` bump forbidden) | §5 and §6 |

## 2. The rules (all pure functions in `visual_tiers.rs`, decidable from indexed metadata)

**R1 Grid tiers (grid-m 1024, grid-s 512, grid-xs 256).** Format `image/jpeg` via
`jpeg-encoder`: q83, `SamplingFactor::F_1_1` (4:4:4 — the crate defaults to
4:2:0 below q90, so this is mandatory), `set_progressive(false)`,
`set_optimized_huffman_tables(true)` (off by default),
`QuantizationTableType::ImageMagick`. Serve-directly rule unchanged in shape
(short ≤ 1.25·tier, long ≤ 2.5·tier, ≤ 8 MiB), so grid-xs serves directly at
320/640. Exception R4.

**R2 Display tier.** Two separate questions, deliberately: *whether* a
rendition is stored, and *what shape* it has once it is.

- **Trigger — dimensions shared, bytes per source class:** store a rendition
  iff `short > 4096` OR `pixels > 24 MP` OR `bytes > B(class)`, where
  `B(lossless PNG/BMP/TIFF) = 2 MiB`, `B(jpeg) = 4 MiB`, `B(webp) = none` (bytes never trigger), and `B(animated) = 5 MiB`
  (R3). Otherwise serve the original. Bytes mean different things per
  format — a 5 MiB PNG is a modest picture, a 5 MiB JPEG a large efficient
  one — so one number cannot serve all of them; a 600 KiB 2400×3600 JPEG is
  already a picture the gallery uses as-is, and a "thumbnail" of it would cost
  a decode and bytes for nothing.

  Where the numbers come from: PNG 2 MiB is measured (Phase C: a WebP
  rendition saves 80–90% of bytes from the 1–2 MiB bucket up, sentinel hits
  only below 1 MiB). WebP sources measured "never by bytes" (under 50% saved,
  half tripping the sentinel). Animated 5 MiB is the user's judgement call
  for GIFs. JPEG 4 MiB is from Phase E (JPEG→JPEG renditions via the actual
  `jpeg-encoder` settings): a rendition of a JPEG original **decodes faster
  than the original** (0.56–0.68×) and saves 57–62% of bytes from the 2–4 MiB
  bucket up with ≤ 7.5% sentinel hits; below 2 MiB it stops paying (20%
  sentinel hits, 49% saved). 4 MiB was chosen over the data-supported 2 MiB
  to halve the regeneration and storage footprint on photo libraries (at
  2 MiB, 75% of the camera corpus's raw-served JPEGs would gain a rendition);
  it is one constant if that trade is ever revisited. Phase C's "never by
  bytes for JPEG" verdict was about *WebP* renditions of JPEGs, which decode
  2.33× slower; it does not apply to JPEG renditions. The 4096 short-side bound is today's constant, unchanged. The pixel
  bound drops from 32 MP — which was 4096² doubled for a 2:1 aspect, not a
  chosen number — to 24 MP; decimal MP with `>`, so a 6000×4000 camera JPEG
  is exactly 24,000,000 and serves raw unless its class byte bound fires.

- **Format follows the source class:** *lossless* (`image/png`, `image/bmp`,
  `image/tiff`, any non-JPEG/non-WebP still) → **WebP q90** — 11× smaller at
  decode parity or better; *jpeg* → **JPEG** (`jpeg-encoder`, q85, 4:4:4, same
  settings otherwise) — a WebP of a JPEG decodes 2.33× slower than the JPEG
  it replaces, a JPEG downscale is smaller AND faster; *webp* → WebP q90.
- **Geometry once stored:** short side capped at 2560 (fills a 4K monitor 1:1
  in either orientation with margin), then the 24 MP bound, whole image, no
  crop. `DISPLAY_MAX_SHORT_SIDE` (4096) stays the trigger; a new
  `DISPLAY_RENDITION_SHORT_SIDE = 2560` is the cap. `DISPLAY_MAX_FILE_SIZE`
  becomes a per-class table; `DISPLAY_MAX_PIXELS` becomes 24,000,000.
- **Keep-original sentinel** (display stills only — see the 2026-09-02 note
  in §10): if the encoded rendition is not ≤ 75% of the source bytes, store a
  sentinel row (empty bytes) on `thumbnails` meaning "the original is the
  rendition". This is what protects an efficient 6 MiB JPEG whose re-encode
  would save nothing. The row's `media_type` names the format the generator
  *attempted*, never the source's own mime type: the verdict is about that
  encoder, so recording which one reached it is what lets a later format flip
  (a policy edit, a transparency measurement) retry instead of freezing on it,
  and it is what makes every stored-versus-wanted comparison plain equality
  with no exception for either kind of row.
- **WebP size limit**: if either side of the rendition would exceed 16383,
  encode JPEG instead (same q85 settings).

**R3 Animated display loop.** For an animated image above the raw floor, the
`display` request serves the original unless the R2 trigger fires
(`bytes > 5 MiB` — the animated class's byte bound — OR `short > 4096` OR
`pixels > 24 MP`); then a stored H.264
loop: where the grid loop row is already **the whole picture at native
resolution** it *is* the display loop and no second encode happens; otherwise
a second row `tier = "loop-display"`, whole image, capped at 2560 short,
CRF 16, same encoder settings. The rule is the whole picture and not merely a
short side ≤ 1024, because a strip (aspect > 2) has its grid loop stored as a
**top crop** — right in a cover cell, wrong on a contain surface — so every
strip over the trigger gets its own `loop-display`. 60 s duration cap on every loop encode
(§4). **No sentinel:** a loop is stored whatever its byte count.

**R4 Transparency.** `items.has_transparency` (0/1, NULL = never examined),
measured from decoded pixels (any alpha < 255) in `build_image_renditions`,
the one chokepoint that owns the decoded image. When 1: **every** rendition of
that item (grid tiers, display, posters) is WebP with alpha — grid tiers at q85,
display at q90. When the R2 size limit bites, fall back to JPEG flattened as
today.

**R5 Per-DB policy** `thumbnail_formats: Vec<String>` in `SystemConfig`,
serde default `["jpeg", "webp"]`, edited as a multi-select. It *constrains*
R1–R4: with `webp` absent, every WebP verdict becomes JPEG (alpha flattened as
today); with `jpeg` absent, every JPEG verdict becomes WebP (grid tiers q85 —
the storage-constrained deployment, which knowingly pays Phase B's decode
cost). An empty list is treated as the default with a warning — never a commit
rejection (CLAUDE.md rule).

Every rule above is a function of `(mime, bytes, width, height, duration,
has_transparency, policy)` — nothing decodes to decide.

## 3. Storage

- Migration: `ALTER TABLE thumbnails ADD COLUMN media_type TEXT NOT NULL DEFAULT 'image/jpeg'`
  (metadata-only, mirrors `20260901093000`). `StoredImage` gains `media_type`;
  `store_thumbnails`, `get_thumbnail_image` and `get_thumbnail_geometry` carry
  it. `get_thumbnail_bytes` does not: it is the scan's own blob read (a
  blurhash source, a tier's derived picture) and has no response to name a
  type for. The serving path reads geometry and blob together through
  `get_thumbnail_image`. Sentinel rows = empty `thumbnail` blob.
- Migration (index): `ALTER TABLE items ADD COLUMN has_transparency INTEGER` +
  partial index `ON items(type) WHERE has_transparency IS NULL`, same shape as
  `20260824120000_item_rotation.sql` (half-open `type` ranges, never `LIKE`).
- `ThumbnailTier` gains `GridXs` (`"grid-xs"`, short side 256); `GRID` array,
  `tier_ladder` in `api/items.rs`, and the OpenAPI enum follow. Wire values
  are frozen once shipped.
- `TIER_PROCESS_VERSION` → 2 (encoder change = "a different JPEG quality", the
  documented bump case). **Decouple the loop**: new `LOOP_PROCESS_VERSION = 1`
  used by `visual_process_version(Loop)` and the loop rows, so the tier bump
  does not re-run ffmpeg over every animated item.

## 4. Scan / dispatcher

- **Expected-vs-stored media type joins the comparison.** `get_thumbnail_tier_geometry`
  and `get_thumbnail_geometry` return `media_type`; `wanted_tier_geometry` and
  `display_matches` compare `(idx, tier, w, h, media_type)`. This is what makes
  R4, R5 and the display switch regenerate exactly the rows that are wrong,
  per item, with no policy stamp — and it runs off the index.
- Items whose `has_transparency IS NULL` are *pending* (`item_transparency_pending`,
  cloned from `item_rotation_pending`). The examination rides on the pass
  that decodes anyway (`TierWork::Image`); a transparency-only pending item
  (everything else converged) gets a `TierWork::Image` with
  `replace_display: false` so it decodes once, records the fact write-once
  (`SET has_transparency = ?1 WHERE sha256 = ?2 AND has_transparency IS NULL`,
  via a new `IndexDbWriterMessage`), and rewrites renditions only if the fact
  changed the verdict. **The new field must be added to
  `PendingBackfillWork::any()`** — omitting it is exactly the Wave-5 rotation
  bug.
- The grid-xs addition alone makes every tiered image want a new row, so the
  library-wide pass that installs the new encoder, measures transparency and
  rewrites display renditions is **one decode per image**, not three.
- Loop cap: `WHOLE_ANIMATION.end_cs = 60 s`. Stored over-cap loops on
  pre-release libraries are left alone (nothing has shipped; a handful of items).
- Non-image renditions (video 2x2 / 1x1, audio covers, PDF pages, HTML shots)
  keep JPEG via the new encoder (q85, 4:4:4) — same media type as today, so
  they regenerate only through their own existing versions. Out of scope:
  making video stills WebP.

## 5. Serving (`api/items.rs`)

- Content-Type and filename extension from the row's `media_type` (rows 8 and
  the poster branch of the header table; `.jpg`/`.webp`).
- **Display ETag** becomes `"{sha}-thumb{idx}-{w}x{h}-{fmt}{still_suffix}"` —
  it changes whenever the bytes can. Pre-upgrade browsers hold the old
  immutable JPEG at the bare URL; the UI adds a one-time revision parameter
  (`r=2`) to display requests so the transition is deterministic rather than
  "until eviction".
- Sentinel row on `thumbnails` → `file_variant_response`, immutable (mirrors
  the loop's three-state read). `tier_fall_up_is_final` learns the new display
  plan.
- R3: a `display` request for an animated item consults the loop rows the way
  the grid request does. `still=true` at the display size answers the largest
  stored poster — `grid-m`, since the poster ladder has no display rung — for
  an animated item above the raw floor, and the original file only below it,
  where nothing is stored at all.
- `/api/client-config` publishes the animated class's **trigger** (never the
  2560 rendition cap): `display_loop_trigger: { max_bytes: 5 MiB,
  max_short_side: 4096, max_pixels: 24_000_000 } | null` (null under the same
  condition as `animated_floor: null`), always all three keys. Any `<img>` or
  canvas consumer of a bare/`display` thumbnail URL must send `still=true`
  when `exceedsDisplayLoopTrigger(item)` — the gallery large view is the ONLY
  surface that mounts a `<video>` on the bare URL. `still=true` at the
  display size always answers an image (poster, or the original for an
  under-bound item), never video, never 404.
- Downstream JPEG assumptions: `api/video.rs::materialize_thumbnail` writes the
  file with the extension of `media_type`; `api/pinboards.rs` compose/preview
  paths use the existing `sniff_image_media_type`. `build_stored_thumbnail_tiers`
  decodes WebP blobs via `image-webp` (already in tree).

## 6. UI

- `lib/thumbnailTier.ts`: `grid-xs` (short side 256) in the type, the short-side
  table and `tierForCellWidth` (cell device px ≤ 288 → grid-xs, with the same
  1.125 slack the other rungs use); `gridcells.test.mjs` extended. The grid
  must hand it the **binding edge** of the image box (auto layout's box is
  `cellWidth × 384 CSS px`, not square — the filmstrip's
  `STRIP_CARD_CSS_BINDING_EDGE` reasoning applies), or a 266 px-wide auto cell
  asks for grid-xs and upscales 1.5×.
- Gallery large view: animated item over `display_loop_trigger` → mount
  `<video muted loop autoplay playsinline>` on the bare thumbnail URL; else
  the `<img>` as today. Filmstrip unchanged (posters).
- Scan settings page (`components/scan/Config.tsx`): `thumbnail_formats`
  multi-select (`multiCombobox`), key added to `guiKnownKeys`; help text says a
  change regenerates renditions on the next scan and that the DB file shrinks
  only after the maintenance VACUUM.
- Display request URLs gain `r=2` (§5).
- A pinboard **pin** of an animated item over the display trigger is a static
  poster by contract: the compose and preview paths ask for a picture, so they
  send `still=true` and get one. Nothing on a board mounts a `<video>`.

## 7. Verification

Unit (`visual_tiers.rs` tests): R1–R5 tables incl. the 16383 fallback, the
5 MiB/2560 boundaries per class, the sentinel, the loop reuse rule, policy
constraint in both directions. Encoder tests: `jpeg-encoder` output is
baseline (`FFC0`), 4:4:4, and decodes with `image`; WebP alpha round-trips.

Scan convergence (`jobs/files/mod.rs` tests, the scan→mutate→scan→settle
shape): pre-upgrade JPEG display rendition of a PNG → regenerated as WebP once,
settles; transparency measured once, written once, rewrites renditions only
when the verdict changes; policy flip `["jpeg"]` regenerates WebP rows only;
tier bump regenerates static tiers and **not** loops; grid-xs backfill is one
decode per image (count decodes); loop cap truncates a 90 s GIF to 60 s.

Serving (`api/items.rs` tests): media type and extension from the row; the
new display ETag differs across formats/geometry; sentinel display row →
original, immutable; a legacy empty *loop* row → original, revalidating (§10,
the 2026-09-02 note); animated display over/under bound; fall-up finality with
the new plan.

Integration on the isolated stack with the Phase-C worst cases (the two
>16383 strips, transparent PNGs, a 14 MiB artwork PNG, a 6000×4000 JPEG, a
30 MB GIF): correct Content-Type per request, gallery load bytes before/after,
`tools/scroll-perf` unchanged at the default size, and grid-xs at the minimum
size. Build: `cargo build --release` on Windows, `cargo clippy` zero warnings,
and the Nix/Docker/CI packaging change reviewed for the vendored `libwebp-sys`
(§9) — it cannot be executed on this machine.

## 8. Process and order

Backend worktree branch `thumb-formats` off master; UI branch `thumb-formats`
off `rust-ui` (after `hover-animate` merges — both touch `thumbnailTier.ts`).
Waves: (1) rules + storage + encoders + dispatcher; (2) serving + client-config
+ per-DB policy + settings UI; (3) grid-xs UI + gallery video mount + `r=2`;
(4) packaging. Opus implementer → Opus adversarial verifier → coordinator
adjudication → fixer, per wave. Nothing pushed.

## 9. Recorded for later

- **mozjpeg**: another ~10% of grid-tier bytes (trellis + deringing) for a
  `nasm` dependency on the Windows/Linux x86_64 release legs, the Dockerfile
  and both Nix files. Not worth it today.
- A separate *animated* display bound justified by GIF render cost would need
  a per-frame decode trace; today static and animated share the 5 MiB bound.
- Video stills as WebP; PQL-rule-driven per-item format policy (the row-level
  `media_type` and expected-vs-stored comparison already leave room for it).

## 10. Outcome (2026-09-02)

Implemented and merged: backend `2b76ac7` (35 commits), UI `rust-ui@47ea696`
(23 commits), gitlink bump `a40a180`. Nothing pushed. Two adversarial rounds
per branch, an integration round and a final round on isolated stacks, then
five Fable architectural reviews applied (backend 17 commits, UI 15).

**Verified on real fixtures** (final round, 2,991 files incl. every Phase-C
worst case): zero ffmpeg spawns across eight rescans — a library-wide
transparency pass and a full static-tier regeneration over 37 animated items
left all 42 loop rows byte- and rowid-identical; the loop-sentinel migration
flipped exactly the five empty rows of a pre-review database and the next
scan wrote nothing; a `["jpeg"]` policy flip and restore returned the store
byte-identical; at the slider minimum the grid decodes 4× fewer megapixels
and transfers 4.1× fewer bytes, and at every other size the request stream is
identical to the pre-package build (the per-item orientation-aware tier
choice never asks for a larger rung than width-only did on the measured
corpora). 1171 backend tests, 20 UI suites under the new `npm test`.

**Corrections adopted after the reviews** (each with a test):
- Loop reuse now runs on every animated-ladder dispatch path, per row, with
  retained rows *named* (`TierPayload::Retained`) instead of copied through
  the worker — before this the real upgrade pass would have re-encoded every
  loop despite the unit test proving the tier bump did not.
- One sentinel convention on both tables: the row names the format the
  generator *tried* (loops: `video/mp4`); a sentinel is final only while that
  format is still the verdict. Migration `20260902130000` rewrites the
  pre-review loop sentinels.
- `JPEG_MAX_SIDE` (65535) joins `WEBP_MAX_SIDE`: a shape no container can hold
  serves the original by rule instead of failing as a file verdict.
- Display ETag = `{sha}-thumb{idx}-{w}x{h}-{fmt}-v{ver}[-still]`.
- The display rule is `display_shape` (geometry + trigger, what the serve side
  asks) composed with the policy (what the generator asks).
- Gallery large view: on a `<video>` error the fallback is the bare URL first
  (a keep-original sentinel answers the animating original), `still=true`
  second — `still=true` at the display size answers the ≤1024 poster for any
  animated item above the raw floor, never the original.
- The grid card chooses its tier per row from the box edge its picture's
  short side must cover under `object-cover` (`coverBindingEdge`), latched at
  mount; `smallCell` is derived in the card, not passed.
- One URL builder per element kind (`lib/thumbnailURL.ts`): a bare-URL
  builder that takes a sha, and a picture builder that *requires* the row and
  the trigger, so an `<img>` consumer cannot forget `still=true`.

**Traps recorded:** a hand-launched `next start` behind a scratch gateway does
its SSR fetches against the default API base (production) unless
`PANOPTIKON_API_URL` is set; Edge without
`--disable-features=CalculateNativeWinOcclusion` never issues a `<video>`
request while occluded (poster forever, no error — looks exactly like a broken
fallback); tailwind-merge does not know a custom `@utility`, so a shadcn
`<Button>` under one keeps its own `rounded-md`; editing a committed migration's
comment changes its checksum (self-healed, but a WARN on upgrade).

**User QA:** the format switch is invisible by design except: the gallery's
14 MiB PNGs now load as ~1 MiB WebP; transparent PNGs show the background
through in the grid and gallery instead of black; heavy GIFs open as loops
(sentinel ones as the animating original); the scan settings page gains
"Thumbnail Formats"; the first scan after upgrade regenerates every tier once
(one decode per image) and the DB file shrinks only after the maintenance
VACUUM. Grid-xs at the slider minimum is the visible perf change.

### 2026-09-02: a loop is never a keep-original sentinel

A change to R3, from evidence in the user's real library. It does not bump
`LOOP_PROCESS_VERSION`. The keep-original rule used to
apply to the H.264 loop as well: an encode that came out at or above its
source's bytes was written as an empty row, and the endpoint answered the
original file. That inverted the trigger that produced it. Two 8.4 MiB
2439x1080 5.5 s GIFs had empty `loop` **and** `loop-display` rows, so the
gallery embedded the raw 8.4 MiB GIF the 5 MiB display trigger exists to
avoid, and every grid tier down to `grid-xs` — a 140 px cell — answered the
same file, a 2.6-megapixel-per-frame software GIF decode. Library-wide: 34 of
644 grid loops and 2 of 3 display loops were sentinels. A loop exists for
*decode cost* and playback smoothness, not for bytes — hardware-decoded H.264
with `faststart` and range requests beats a software GIF decode at any byte
ratio — and the user's priority order is crispness > performance > storage >
bandwidth > scan time. So `loop_keeps_original` is deleted and
`build_animated_tiers` stores the encode unconditionally.

The rows already written are a legacy state the **scan repairs**, not a
verdict: `TierGeometry` now carries `has_bytes` (`length(thumbnail) > 0`, no
blob read), `rendition_row_matches` requires it, and an empty row is therefore
neither a match nor reusable — so the item is dispatched to the animated
ladder once, per row, with no version bump (which would re-encode all 644
loops to repair 36). The endpoint keeps its empty-blob branch as a defensive
path for the window before that scan, but it now revalidates instead of
answering immutably: a state that changes is not a verdict that settles.
Migration `20260902130000` is historical for the same reason; it is not
edited.
