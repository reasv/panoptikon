# Still-video parity for composed exports

## 0. Problem

The animated compose renders a non-playing video as `still { at_cs:
trim.start | 0 }` — the first frame for an untrimmed video — regardless of
what the board is actually showing. The static mosaic gets this right, and
the compose module's own header states the governing principle it is
violating: **"THE POINT IS THAT IT IS THE SAME PICTURE."** The correct
behavior, per the static path (`pinboardMosaic.ts` / `pinboardMedia.ts`):

- **Closed** (no `<video>` mounted; the board shows the item's generated
  thumbnail): the export must composite **the thumbnail** — it is,
  effectively, an image in every way.
- **Paused / ended** (a `<video>` is mounted, stopped): the export must
  composite **the frame the player is parked on** — the element's
  `currentTime` — not the trim start.

Playing videos (spans) are untouched: a span is deliberately keyed on the
pin's *trim*, not the moving playhead, so an unchanged board re-exports to
the same artifact. A paused still is different: its playhead IS the picture
on the board, exactly as the static mosaic draws it, and a new pause
position minting a new artifact is correct.

## 1. Paused / ended: the playhead still (UI only)

- `VideoStateProbe` / `videoStateOf` / `PinVideoState`
  (`ui/lib/pinboardMedia.ts`) gain `currentTime` (the element's, `null`
  when non-finite).
- `resolveItemTime` (`ui/lib/pinboardCompose.ts`), still branch: when a
  state exists (element mounted, not playing), the freeze timestamp is
  `state.currentTime`, clamped by the existing `stillCs` rule (an ended
  element's `currentTime == duration` lands on the last frame, which is
  what an ended player shows). `trim.start` remains only as the fallback
  when `currentTime` is unknown.
- No server change: `ItemTime::Still` already seeks.

## 2. Closed: composite the stored thumbnail (API + server + UI)

The thumbnail is a stored blob (`StoredImage { idx, width, height, bytes }`
in the index DB), keyed by item sha — it has no file path and no recorded
source timestamp, so it can be neither referenced as a file nor recreated
by a seek. The compose document must be able to say "this item's picture is
its thumbnail":

- **Schema**: `ComposeItem` gains `#[serde(default)] source: ItemSource`
  with `ItemSource::File` (default) | `ItemSource::Thumbnail`
  (`snake_case` on the wire). Admission validates: `source = thumbnail`
  requires `time.kind = image` (a thumbnail is a still image; any other
  combination is refused by name, e.g. `thumbnail_not_an_image`).
- **Hash contract**: the field serializes into `ResolvedCompose`'s items
  (declaration-order contract), so the pinned worked-example fixture
  changes and `TRANSCODER_VERSION` **bumps** — the documented rule for any
  shape change. (Every compose cache key changes; outputs are re-derived
  on demand, no stored verdicts exist for compositions.)
- **Materialization**: at submit time (the API layer, which has DB
  access), a thumbnail-source item's blob is fetched — **idx 0, the same
  image the pin displays**: the pin `<img>` hits the thumbnail endpoint
  with no `big` parameter, `big` defaults to *true*, and the endpoint's
  video rule is `big ? idx 0 : idx 1`, so what the board shows for a video
  is the 2×2 frame grid at idx 0, not the idx-1 single frame (composing
  idx 1 renders a quarter of the pin's picture — the original ship of this
  feature did exactly that) — and written to a per-job temp dir owned by
  the job spec (the webp bridge's TempDir
  pattern: lives until the run, including its `cache:` retry, finishes);
  its path rides in `sources` like any input. Thumbnails are JPEGs —
  every ffmpeg reads them; the ordinary `Image` chain freezes the first
  (only) frame. A missing thumbnail (never generated, suppressed visuals)
  is refused at submit with a named rejection — the client should not
  have asked (see the UI fallback below).
- **Probe**: skipped, `StreamInfo` synthesized from the stored dims
  (`video_index: 0`, `has_audio: false`) — the DB already knows them.

## 3. UI classification and geometry

`resolveItemTime` input distinguishes the two still cases by what the
board shows, which the builder already knows:

- `isVideo && state == null` (closed, thumbnail on screen) →
  `{ time: image, source: thumbnail }`.
- `isVideo && state != null && !playing` → `still { at: currentTime }`
  (§1), `source: file`.

**Source-rectangle space**: a compose item's `src` rect is measured in the
pixels of what is displayed. For a closed video that is the *thumbnail's*
pixel space (its `naturalWidth/Height` off the same `<img>` the board
renders — the static path's `resolvePinDraw` already reads exactly this),
NOT the index's video dimensions. The builder must mirror the static
compositor's source-space choice for these pins; same aspect, different
numbers, and the server clamps `src` against the real input dims either
way.

**Fallback**: a closed video whose thumbnail is missing (no stored blob,
or no `<img>` dims to measure against) falls back to today's behavior —
`still { at: trim.start | 0 }`, `source: file` — degraded, never refused,
and never a request the server must reject. The missing-blob case is
client-detectable from the same `<img>` the probe measures: the thumbnail
endpoint answers a missing thumbnail with the shipped placeholder as an
ordinary 200 at exactly 4096×4096, a size no real thumbnail can reach
(generation caps them at 1024), so those naturals read as "no thumbnail"
in the classification and take the fallback.

## 4. Testing

- UI (`scripts/compose.test.mjs`): paused state with `currentTime` →
  still at the playhead; ended (`currentTime == duration`) → clamped last
  frame; closed video → thumbnail-source image item with src in thumbnail
  space; closed with no thumbnail → the fallback; playing → span exactly
  as before (trim-keyed, no playhead).
- Server: admission — `thumbnail` + non-`image` time refused by name;
  missing-thumbnail submit refused by name; the pinned resolved-hash
  fixture updated alongside the `TRANSCODER_VERSION` bump. Golden
  (ffmpeg-gated, skip-not-fail): a thumbnail-source item composes the
  thumbnail's pixels (seeded blob), not the source file's frame 0 —
  distinct colors make the assertion decisive.
- Materialization lifetime: temp dir outlives the run including the
  `cache:` retry (the bridge's pinned pattern).

## 5. Out of scope

- Playing spans stay trim-keyed (deliberate; see §0).
- The static mosaic is already correct and untouched.
- Thumbnail-source items for non-video items (images already composite
  their own file; nothing shows a thumbnail for them at export-relevant
  sizes).
