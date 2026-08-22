# Animated images as looping clips in composed exports

## 0. Problem

The animated mosaic export (`POST /api/video/compose`) freezes animated
images — GIF, animated WebP, animated AVIF — to their first frame. The UI
classifies any non-`video/*` mime as `ItemTime::Image`
(`ui/lib/pinboardCompose.ts`, `resolveItemTime`), and the server's image
chain deliberately takes one decoded frame (`trim=end_frame=1`) and holds
it. That was a robustness decision against demuxer hangs, but it is
inconsistent: an animated image on the board *plays* in the browser and
should play in the export, looping to fill exactly like a video span does.

## 1. Design principles (unchanged, load-bearing)

- **Admission is arithmetic over the document.** The resolved compose doc is
  the cache key; target length (`resolve_target_cs`), loop construction
  (`span_loop`), and the memory guard (`check_loop_memory`) are all computed
  from client-supplied bounds with zero file probes before keying. This
  design does not change.
- **"The client always knows the duration"** is made true for animated
  images the same way it is true for videos: the *index* records it at scan
  time. No server-side probe at admission, no client-side decode.
- The existing frozen-frame `Image` path remains, as the fallback for:
  still images, animated images the index has not measured yet, animated
  containers the bundled ffmpeg cannot decode, and unparseable files.

## 2. Data model: `items.duration` sentinel semantics

`items.duration` (existing column, today `NULL` for every image):

| value  | meaning for `image/gif`, `image/webp`, `image/avif` |
|--------|------------------------------------------------------|
| `NULL` | never measured (pre-upgrade item, or file unreadable at scan) |
| `0.0`  | measured: not animated (< 2 frames), or structure unparseable |
| `> 0`  | measured: animated, total animation length in seconds |

Other image mimes stay `NULL` forever (the backfill question is gated to
the three mimes). Video/audio duration semantics are untouched. No schema
migration is needed — the column exists.

## 3. Scan: measuring animation length (pure Rust, no ffmpeg)

New module `panoptikon/src/media_tools/animation.rs`:
`pub(crate) fn animation_duration_seconds(path, mime) -> Option<f64>` —
returns `Some(seconds)` (0.0 = still/unparseable verdict) for the three
mimes, `None` for anything else. Header/structure parsing only, no pixel
decoding, no ffmpeg — index data must not depend on the bundled ffmpeg.

- **GIF**: walk the block structure (header + LSD + palettes, extension
  blocks, image descriptors + LZW sub-blocks, trailer). Sum Graphic Control
  Extension delays, one per rendered frame. Normalize a delay of ≤ 1 cs to
  10 cs (both ffmpeg's gif demuxer and browsers substitute ~100 ms for
  0/1-cs delays). Fewer than 2 image descriptors → 0.0.
- **WebP**: RIFF walk. `VP8X` animation flag unset → 0.0. Otherwise sum the
  24-bit millisecond durations of the `ANMF` chunks. Fewer than 2 `ANMF`
  chunks → 0.0.
- **AVIF**: ISOBMFF box walk. No `avis` brand in `ftyp` (major or
  compatible) → 0.0. Otherwise read `moov`/`mvhd` duration ÷ timescale.
  Missing `moov` → 0.0.
- Any structural error, truncation, or I/O error mid-parse → 0.0 (a
  *verdict*, so the file is never re-probed; it composes frozen, which is
  today's behavior). Only a failure to open/read the file at all keeps
  `NULL` so a later scan retries (matches the visuals-backfill philosophy).

Exactness is not critical: the compose graph tolerates a measured duration
that disagrees with ffmpeg's decode timing (§5, under-run).

**New-item path**: `extract_item_metadata` (`jobs/files.rs`), image branch —
after the header dimensions, for the three mimes set
`metadata.duration = Some(animation_duration_seconds(..))`.

## 4. Scan: backfilling existing libraries (hard requirement)

Existing libraries get durations automatically via the established pattern:
a **fifth dispatcher question** in `maybe_dispatch_backfill`
(`jobs/files.rs`), alongside the outro (third) and codec (fourth)
questions, which the code itself documents as "exactly what backfills an
existing library, with no migration and no separate job":

> "is this a gif/webp/avif whose animation length nothing has measured?"

- Predicate: mime ∈ {image/gif, image/webp, image/avif} AND
  `items.duration IS NULL`. Answered from the index; costs nothing once
  stamped.
- When yes: carry the work into the same backfill dispatch
  (`BackfillResult` gains an `animation: Option<f64>` field, measured in
  the worker via `animation_duration_seconds`). The early-return that skips
  dispatch when no thumb/blurhash/outro/codec work exists must also check
  the animation question.
- `handle_backfill` writes the measured value to `items.duration` (index-DB
  writer, `UPDATE items SET duration = ? WHERE sha256 = ?`, only when the
  stored value is still NULL).
- A dead worker records nothing (stays `NULL`, retried next scan), matching
  the visuals-backfill failure stance. A file offline this scan is picked
  up on a later one.

This respects the no-special-case-reindex rule: it is automatic, generic,
and invisible — no user-facing recovery action.

## 5. Server: composing an animated-image span

`ItemTime::Span` already does everything needed — `-ss/-to` input seeking,
`fps` resample, `span_loop` looping, the memory guard — and none of it is
video-container-specific. **No filtergraph or admission changes.** The
run-time `StreamInfo` probe already selects the content video stream
(covers the animated-AVIF cover-still trap).

**Under-run tolerance** (measured duration > actual decoded length): the
span chain produces fewer frames than claimed; the `overlay` filter's
default `eof_action=repeat` freezes that item's last frame while the base
(colour source trimmed to target) carries the output to its full length.
Degraded, never hung, never short. A test must pin this (compose a span
whose `end_cs` overshoots a two-frame GIF and assert output duration).

**ffmpeg 7.x trailing-frame truncation**: 7.1's `scale` filter drops the
last frame's *duration*, so the `fps` resample ends the stream at that
frame's start — every animated-image span loses its final frame's hold
time (a two-frame file degrades to today's frozen behavior). Fixed in
ffmpeg 8.0; the bundled production toolchain is 8.0.1 and unaffected, but a
user-supplied 7.x `ffmpeg =` override renders spans with the trailing
frame truncated. Degraded, never hung or short. The compose golden tests
probe the local toolchain for this (`scale_preserves_trailing_frame_
duration`) and skip only the frame-accurate colour assertions on
duration-dropping builds — meaning a dev box on 7.x proves length and
first-frame behavior only; the frame-accurate path needs an 8.0+ ffmpeg on
PATH.

**Degenerate spans**: a measured animation shorter than 5 ms rounds to
`span{0,0}`, which admission refuses for the whole document
(`span_not_a_clip`). The UI classification therefore falls back to `image`
when the rounded `end_cs` is 0 — a frozen frame is what a 4 ms animation
means anyway.

**Decoder capability probes** (`media_tools/transcode/hw.rs` style,
`OnceLock`-cached):

- GIF: assumed universally decodable, no probe.
- Animated WebP: **no mainline ffmpeg build decodes animated WebP as of
  8.0.1** (verified empirically against both the 7.1 dev build and the
  production static 8.0.1-essentials binary: the decoder reads the cover
  frame only, or answers "image data not found"). The compose path
  therefore *bridges* it — frames decoded in Rust by the `image` crate at
  execution time and substituted as an ffconcat script of PNGs, which every
  ffmpeg plays — so `image/webp` is on the capability list
  unconditionally, like GIF. The probe survives as the bridge's bypass: a
  toolchain that can decode the file natively (a future mainline release,
  or a user's `ffmpeg =` override with a patched build) is preferred
  automatically. See `docs/animated-webp-bridge-design.md`.
- Animated AVIF: works on the production toolchain (probe verified passing
  on static 8.0.1 — AV1 decoder plus `avis` demux). Same probe shape, tiny
  embedded two-frame fixture.

Probes decode their embedded (`include_bytes!`) two-frame fixture via
`ffprobe -count_frames`, requiring > 1 frame on the best video stream (the
max over streams, because animated AVIF's first stream is a one-frame cover
still; and the counts are parsed rather than the exit status, because
ffprobe exits 0 even on "image data not found").

Probe results surface to the client in the **limits payload** the UI
already fetches via `useVideoPresets` (the same response that carries
`max_animated_image_seconds`): a new field
`span_capable_image_mimes: string[]` — always `image/gif`, plus
`image/webp` / `image/avif` as their probes pass. OpenAPI spec regenerated
(`panoptikon/openapi.json` → `npm run gen:api` in `ui/`).

The server does **not** reject a span on an unsupported container at
admission (it doesn't know mimes and must stay probe-free); the capability
flag exists so a correct client never builds one. A stale/hostile client
that does gets an ffmpeg failure at run time — an error, not a hang,
because the span path's bounded `-to` never engages the infinite image-
demuxer loop the old `-loop 1` design feared.

## 6. UI: classification

`resolveItemTime` (`ui/lib/pinboardCompose.ts`) — the non-video branch
becomes:

```
not video/* :
  recorded duration > 0 AND mime ∈ limits.span_capable_image_mimes
      → span { start_cs: 0, end_cs: round(duration * 100) }
  otherwise → image
```

- The input already carries `duration` (from item metadata) and the mime;
  add the capability list to the classification input. `FALLBACK_LIMITS`
  (pre-fetch) carries an **empty** capability list — conservative: while
  limits are in flight, an animated image classifies as a frozen image
  rather than risking a span the server's ffmpeg can't decode.
- Animated images have no `<video>` element, no trim, no play/pause state:
  always the full `0..duration` span, never `still`. `audio` stays false
  (`carriesAudio` is already false for images).
- Everything downstream follows automatically: a GIF's span participates in
  `LongestLoopOnce` (a stills-plus-one-3s-GIF board renders 3 s with the
  GIF playing once; shorter animations loop to fill); the preset-hiding
  rule for the animated-image length cap now sees animated images.
- **Single-item animated export**: the gate that refuses non-videos ("is a
  still image…") must key on the *resolved time kind* (`image` → refuse,
  `span` → allow) so an animated GIF can be saved as a looping clip. The
  refusal copy for genuine stills is unchanged.

## 7. Edge cases

- **Single-frame GIF / still WebP / still AVIF**: measured 0.0 → `image`.
  Correct, and cheaper than today (no misleading mime-based hopes).
- **Corrupt animated file**: 0.0 verdict → frozen frame via the image
  chain, which fails fast on undecodable input (existing behavior).
- **Very long animation**: span longer than target plays once, trimmed
  (`span_loop` returns `None`) — no loop buffer, no memory-guard impact.
  Looping animations buffer `frames_for(span)` at destination resolution,
  identical to video spans; over-budget boards are refused by
  `check_loop_memory` exactly as for videos.
- **`TRANSCODER_VERSION`**: no bump. `ResolvedCompose`'s shape is
  unchanged; a span on a GIF hashes like any span. Old cached mosaics of
  boards whose GIFs were frozen stay valid for *that* document; the new
  classification produces a *different* document → different key.
- **Config**: nothing new in server TOML; capability probes are automatic.
- **Side effect on `/api/video/transcode`**: the three image mimes now
  carry a recorded duration, so clip-request validation (`start_cs` past
  the end; the animated-image length cap) newly binds for them. Not
  reachable from the current UI (images get no trim UI), and the cap
  binding on long GIFs is the correct reading of the limit.
- **Whole-file read at measurement**: the parsers read the file into
  memory (GIF/WebP durations live in per-frame structures that run to the
  end of the file). Accepted: these files are small in practice, the scan
  already streams full files for hashing, and the parse is once-per-item.

## 8. Testing

- `animation.rs` unit tests against **handcrafted byte fixtures** (no
  ffmpeg): multi-frame GIF w/ delays, zero-delay GIF normalization,
  single-frame GIF, truncated GIF; animated + still WebP; `avis` AVIF w/
  mvhd, static AVIF; garbage bytes → 0.0.
- Scan tests (`jobs/files.rs` suite): a new animated GIF is indexed with
  its duration; an existing NULL-duration GIF gets the fifth-question
  backfill and is stamped; a measured-still (0.0) item is not re-dispatched
  on rescan.
- Compose golden test (ffmpeg-gated like its siblings): a *span* item over
  a two-frame GIF renders both frames at their timestamps and loops to the
  target; the overshoot/under-run test from §5.
- Capability probe tests: answer is stable and skip-not-fail without
  ffmpeg (the `hw.rs` pattern).
- UI `scripts/compose.test.mjs`: duration > 0 + capable mime → span;
  duration 0/NULL → image; mime not in capability list → image; empty
  fallback list → image; single-item export gate on resolved kind.

## 9. Out of scope

- Trim/play/pause UI for animated images on the board.
- APNG (`image/apng` or mislabeled `image/png`) — rare; the classification
  rule extends naturally later.
- Audio from animated containers (none carry it).
- Retroactively re-keying cached mosaics.
