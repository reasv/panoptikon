# Indexing display dimensions instead of coded dimensions

## 0. Problem

`items.width` / `items.height` hold the **coded** dimensions of every image
and video: the numbers in the file header, before any orientation metadata is
applied. Every consumer that matters — the browser, ffmpeg's video decoder,
ffmpeg's JPEG decoder — paints the **display** dimensions. On any EXIF-rotated
photo and any rotated phone video the two are transposed, so the index
disagrees with the picture for exactly the files a phone produces most.

Where it came from:

- Images: `image_header_dimensions` (`jobs/files.rs`) is
  `ImageReader::open(..).with_guessed_format()?.into_dimensions()` — a raw
  header read with no EXIF involvement. Nothing in the scan path ever looked
  at orientation.
- Video: the scan's ffprobe call asks for
  `stream=index,codec_type,codec_name,duration,width,height,tags` and no
  `side_data_list`, so a stream carrying a 90-degree display matrix reports
  its coded size.

The knowledge already existed downstream and was written twice as a
workaround: `media_tools/transcode/compose.rs` parses `side_data_list`
rotation in `parse_probe`, and `media_tools/outro.rs`'s `corroborated_height`
tries *both* orientations because "the stored dims are the coded ones while
the filter graph sees the auto-rotated ones ... on every rotated phone
capture". On the client, `ImageGallery.tsx` defends itself with
element-confirmed aspect while `GalleryPinBoard.tsx` trusts the indexed dims
as "the authoritative source" — so mosaic and uniform pinboard layouts give
rotated captures wrong-shaped cells.

## 1. What the fix must move together

Correcting the columns alone would make things *worse* in three places,
because today's wrongness is partly self-consistent. Everything below moves in
the same change.

### 1.1 Stored image thumbnails (would newly disagree)

`generate_thumbnail` resizes the `open_image_staged` decode and `encode_image`
re-encodes it through `to_rgb8()` + JPEG, which drops EXIF. A large rotated
photo's stored thumbnail is therefore **un-rotated pixels**, which today agree
with the coded dims. They are already wrong against the browser — the original
file is painted upright, the stored thumbnail sideways — but correcting the
dims without correcting the pixels would make the item disagree with itself.

So the visuals path applies orientation before resizing: thumbnail *and*
blurhash source.

Note the asymmetry this removes: an image small enough to be served directly
(`image_is_served_directly`) has no stored thumbnail and is painted upright by
the browser; a larger one was served sideways. That inconsistency goes away.

### 1.2 Video frames and video thumbnails (already display-oriented)

`extract_video_frames_into` runs a plain `-i` decode with no `-noautorotate`,
and ffmpeg autorotates video by default. Stored frames and the thumbnail grid
built from them are therefore **already** display-oriented and already
disagree with the coded columns. Correcting the columns fixes that
disagreement; nothing needs regenerating.

### 1.3 Compose (`POST /api/video/compose`) — measured, not assumed

The suspicion that compose "already auto-rotates on decode" is only two-thirds
true. Measured against the bundled toolchain (ffmpeg 7.1, and the same holds
on 8.x):

| source | ffprobe `width`/`height` | what ffmpeg's decode produces |
|---|---|---|
| rotated video (display matrix) | coded | **display** (autorotate) |
| EXIF-rotated JPEG | coded, no side data, no tag | **display** (mjpeg honours EXIF) |
| EXIF-rotated PNG / WebP / TIFF | coded | **coded** (no autorotate) |

`parse_probe` already swaps for the video row. The JPEG row is the trap: the
decode is display-sized but `probe_source` reports coded, and ffprobe exposes
the orientation nowhere — not in `side_data_list`, not in `stream_tags`. The
PNG/WebP/TIFF row is the opposite trap: the browser rotates those (Chrome
honours EXIF in all three) but ffmpeg does not, so the client's crop and the
decoded frame are in different spaces.

`clamped_src` is the site that suffers: the client computes its crop against
"the natural size the browser reported", so once the index holds display dims
the crop is display-space, and clamping it against a coded-space probe
silently shrinks it.

The fix makes `probe_source` report display dimensions for **every** source,
and makes the filter chain normalize the still formats ffmpeg does not
normalize itself, ahead of the crop. After it, one rule holds everywhere:
*crops are display-space, and the frame the crop sees is display-oriented.*

### 1.4 Extraction (would transpose the tile grid — and fed models sideways)

`load_base_frames` feeds image extraction the **original file bytes**, and of
the three slicing decisions taken from an image's dimensions, one is
asymmetric: `grid_for_pixels` derives `rows` from the height and `cols` from
the width, so dimensions in one space over pixels in the other would tile a
rotated image with rows and columns swapped.

Extraction moves to display space whole, the same rule as everywhere else
(§5 for what that changes about the models' input):

- The image branch reports the buffer's own **display** dimensions
  (`ensure_image_readable` now returns the header's numbers with the EXIF
  orientation applied) rather than falling back to the item's — read from the
  same bytes the models will see, so a file that changed on disk after
  indexing can never be sliced against a stale shape.
- The slicer's decode (`load_dynamic_image`) applies the orientation before
  cutting, so the grid and the pixels it cuts are in the same space. Slices
  are re-encoded without EXIF, which is also what keeps them from reaching
  the models sideways.
- A whole-file send (the common, unsliced case) keeps its EXIF, and inferio's
  one decode seam applies it (§5).

The video branch — where the item's full-resolution dimensions deliberately
govern how a downscaled stored frame is tiled — is untouched: stored frames
are EXIF-less display-oriented JPEGs, and `items.width`/`height` are display
dimensions now, so its fallback is already in the right space.

### 1.5 Outro detection (already defensive)

`corroborated_height` tries both orientations and refuses to guess when the
byte count does not single one out. It keeps working unchanged; only its
comment, which asserts the stored dims are coded, needs correcting.

## 2. Data model: `items.rotation`

Nothing in the index can distinguish "these dims are coded" from "these dims
are display" — unlike the animation backfill, whose `items.duration IS NULL`
was already a sentinel, there is no existing column to key a backfill on.
So one is added (migration `20260824120000_item_rotation.sql`):

| value | meaning |
|---|---|
| `NULL` | never examined (pre-upgrade item), dims may be coded |
| `0` | examined: no transform, or none this build can read |
| `90` / `180` / `270` | examined: clockwise quarter turns from coded to display |

Stored as quarter turns, not as the eight EXIF orientation codes: mirroring
does not change dimensions, and the one consumer that needs the full transform
(compose, section 1.3) reads it from the file rather than the index. The
column is a fact — like the codec columns and unlike `outro_kind` — so it is
unversioned.

`CREATE INDEX idx_items_rotation_pending ON items(type) WHERE rotation IS NULL`
serves the dispatch question and drains as the backfill runs. `type` leads for
the same reason as the codec index: `items.type` holds the whole mime string,
so images and videos are half-open range scans, never a `LIKE` prefix (which
SQLite cannot serve from an index under the default case-insensitive `LIKE`).

## 3. Scan: measuring orientation

**Images.** `image_header_dimensions` becomes `image_header_geometry`,
returning dimensions *and* `image::Orientation`. `into_dimensions()` consumes
the reader, so it uses `into_decoder()` and asks the decoder for its
`orientation()`. Still a header read: JPEG's implementation decodes headers
only, and PNG/WebP/TIFF read the EXIF chunk.

Coverage is whatever `image` 0.25.9 implements — JPEG, PNG (`eXIf`), WebP,
TIFF. GIF has no orientation concept and records `0`. AVIF and HEIC never
reach here at all: the crate has no decoder for either (the default `avif`
feature is encode-only; decoding is behind `avif-native`/dav1d, which this
build does not enable — measured, `into_decoder` returns `Unsupported`), so
both fail the indexing gate. The legacy Python scanner could not index them
either (PIL without plugins), so no library holds items of either format from
any version. Should AVIF decoding ever land, note that its orientation is the
`irot`/`imir` container properties — authoritative over any embedded EXIF per
the AVIF spec — which `orientation()` (EXIF only) would not surface; that
wiring is future work alongside the decoder itself. A decoder that reports
nothing records `0` — "examined, no transform this build can read" — which is
a verdict, not a guess, and matches what every consumer in this codebase does
with the same file.

An orientation read that *fails* is not allowed to reject a file the header
read accepted: it degrades to `NoTransforms`. The indexing gate stays exactly
where it was (docs/failed-media-retry-design.md, "Scan policy for undecodable
images").

**Video.** `side_data_list` joins the ffprobe `-show_entries`, and the
rotation is **negated** before it is normalized: ffprobe reports the display
matrix counter-clockwise, while `items.rotation` and EXIF are both clockwise.
Measured rather than assumed — a frame with a top-left marker muxed with
`-display_rotation 90` probes as `rotation: 90` and decodes with the marker
bottom-left (90 counter-clockwise, 270 clockwise), while `-display_rotation
-90` probes as `-90` and decodes top-right (90 clockwise), which is the common
portrait phone capture. Odd quarter turns transpose the recorded dimensions.

`compose::parse_probe` reads the same field through an `abs()` and is left
alone: it consumes only the transposition, on which both conventions agree.
A matrix that is not a quarter turn is dropped rather than rounded.

**Both** write `rotation` alongside the corrected dims on the `items` INSERT.

## 4. Backfill: the dispatcher's sixth question

Established pattern (`maybe_dispatch_backfill`, alongside the outro, codec and
animation questions): *"is this an image or video whose orientation nothing
has examined?"*

- Predicate: `type` in the image or video range AND `rotation IS NULL`.
  Answered from the index; stamped once and free on every scan after.
- The early return that skips dispatch when no thumb/blurhash/outro/codec/
  animation work exists also consults it.
- The worker measures orientation from the file — a header read for images, an
  ffprobe run for videos, both under the metadata timer, both identical to
  what the new-item path charges there. An image header this build
  *deterministically cannot parse* (a format with no decoder, a header the
  legacy scanner's more tolerant PIL admitted) stamps `0` — the column's own
  "examined: none this build can read" — because a retry would re-dispatch
  the item on every scan forever; only I/O and `Limits` failures stay
  retries, being the two that can change without the bytes changing.
- `handle_backfill` writes `rotation` and, for a transposing turn, swaps
  `width`/`height` — guarded on `rotation IS NULL` in the `UPDATE` itself so a
  re-run never transposes twice. **That guard is the whole safety story for
  this column: unlike every other backfill write, the swap is not idempotent,
  so it must happen exactly once.**
- A dead worker records nothing and is retried next scan, matching every other
  backfill question.

### 4.1 Replacing an image's stale visuals

An image with a non-identity orientation that already has a stored thumbnail
or blurhash has *wrong pixels* stored (section 1.1), so the backfill must
replace them, not just fix the numbers. This is exactly the case
`replace_visuals` was built for (docs/video-outro-detection-design.md section
7.1) and it is reused.

The dispatcher cannot ask the question — it does not know the orientation
before the worker reads it — so it passes down what it *does* know ("visuals
are stored for this item") and the worker decides:

1. read the orientation from the header (cheap, every image);
2. if it is the identity, stop — no decode, which is the overwhelming
   majority of every library;
3. otherwise, if visuals are stored, decode and regenerate them, and set
   `replace_visuals` so the store guards are bypassed for this item.

Videos need no such branch (section 1.2).

## 5. What the models see: the picture, everywhere

The taggers, OCR and CLIP used to see whatever pixels happened to reach them:
video frames arrived display-oriented (ffmpeg autorotates on extraction,
§1.2), while images arrived as raw file bytes that nothing oriented —
`load_base_frames` sends them whole and inferio's Pillow load applies no
EXIF. The same rotated capture embedded upright as a video and sideways as a
photo. There was never a single input regime to preserve.

So the rule the rest of this design applies — *pixels and dimensions move
together, in display space* — extends to inference input:

- **Whole-file sends** keep their EXIF, and `load_image_from_buffer` in
  `inferio/impl/utils.py` — the one decode every image payload passes through
  — applies it (`ImageOps.exif_transpose`), exactly like a browser. A broken
  EXIF chunk degrades to the un-oriented pixels rather than becoming an
  `input` verdict on an image that decoded fine; the OpenCV fallback stays
  un-oriented, since it only runs where Pillow decoded nothing at all.
- **Slices** are cut from an oriented decode in display space (§1.4) and
  re-encoded EXIF-less, so the transpose above is a no-op for them — applied
  exactly once, on whichever side decodes.
- **Query uploads** for search-by-image go through the same seam, which is
  what keeps a query embedding comparable with the index it searches: a
  rotated photo used as a query now embeds the same picture its indexed twin
  does.

What this deliberately does **not** do is reconcile history. An embedding
extracted before this change came from un-oriented image pixels and stays in
the index unmarked; extraction is run-once per (item, model), so nothing
re-runs it, and cleanly versioning "what the model saw" into the extraction's
identity is the model-identity redesign's job (docs/model-identity-design.md).
The alternative — keeping new extractions wrong so they match old ones — was
rejected outright: the old corpus was never internally consistent (see above),
and a fix that never ships to anyone is not a compatibility guarantee, it is
just the bug. Rotated images are a small minority of any library, and each
one's embeddings improve the next time its models run for any reason.

## 6. Deliberately out of scope

**A one-off reindex action.** Forbidden by standing project rule; section 4 is
the accepted mechanism — automatic, generic and invisible.
