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
- **Keep-original sentinel**: if the encoded rendition is not ≤ 75% of the
  source bytes, store a sentinel row (empty bytes, `media_type` = source mime)
  meaning "the original is the rendition" — the loop's existing convention,
  now on `thumbnails`. This is what protects an efficient 6 MiB JPEG whose
  re-encode would save nothing.
- **WebP size limit**: if either side of the rendition would exceed 16383,
  encode JPEG instead (same q85 settings).

**R3 Animated display loop.** For an animated image above the raw floor, the
`display` request serves the original unless the R2 trigger fires
(`bytes > 5 MiB` — the animated class's byte bound — OR `short > 4096` OR
`pixels > 24 MP`); then a stored H.264
loop: if the source short side ≤ 1024 the **existing grid loop row is the
display loop** (it is already native resolution — no second encode); else a
second row `tier = "loop-display"` capped at 2560 short, CRF 16, same encoder
settings otherwise. 60 s duration cap on every loop encode
(§4). Sentinel as today.

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
  `store_thumbnails`, `get_thumbnail_bytes`, `get_thumbnail_image`,
  `get_thumbnail_geometry` carry it. Sentinel rows = empty `thumbnail` blob.
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
  the grid request does; `still=true` keeps answering the poster.
- `/api/client-config` publishes `display_loop_bound = { bytes: 5 MiB, short: 2560 }`
  so the gallery large view decides `<video>` vs `<img>` from item metadata
  (no wasted request, no error latch).
- Downstream JPEG assumptions: `api/video.rs::materialize_thumbnail` writes the
  file with the extension of `media_type`; `api/pinboards.rs` compose/preview
  paths use the existing `sniff_image_media_type`. `build_stored_thumbnail_tiers`
  decodes WebP blobs via `image-webp` (already in tree).

## 6. UI

- `lib/thumbnailTier.ts`: `grid-xs` (short side 256) in the type, the short-side
  table and `tierForCellWidth` (cell device px ≤ 288 → grid-xs, with the same
  1.125 slack the other rungs use); `gridcells.test.mjs` extended.
- Gallery large view: animated item over `display_loop_bound` → mount
  `<video muted loop autoplay playsinline>` on the bare thumbnail URL; else
  the `<img>` as today. Filmstrip unchanged (posters).
- Scan settings page (`components/scan/Config.tsx`): `thumbnail_formats`
  multi-select (`multiCombobox`), key added to `guiKnownKeys`; help text says a
  change regenerates renditions on the next scan and that the DB file shrinks
  only after the maintenance VACUUM.
- Display request URLs gain `r=2` (§5).

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
original, immutable; animated display over/under bound; fall-up finality with
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
