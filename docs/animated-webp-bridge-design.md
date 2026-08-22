# Animated-WebP compose bridge

## 0. Problem

`docs/animated-image-spans-design.md` made animated images compose as
looping spans, but animated WebP stayed dark: **no mainline ffmpeg build
through 8.0.1 decodes animated WebP**. ffmpeg's native WebP decoder is
still-image-only, and its libwebp integration is encoder-only — there is no
libwebp decoder wrapper at all. The capability probe correctly answers
`false` everywhere, so `image/webp` never joins `span_capable_image_mimes`
and animated WebPs compose frozen.

Worse, "frozen" may be generous: the native decoder produces **zero**
frames from an animated WebP (`Video: webp, none`), so even the existing
`Image` chain (`trim=end_frame=1`, needs one frame) plausibly fails the
whole board today. The implementation must establish empirically what an
animated-WebP `Image` item does on current toolchains and cover it.

We already ship a spec-complete decoder: the `image` crate (0.25.x) via
`image-webp` — pure Rust, full animation support (per-frame durations,
disposal/blend compositing), the same crate the scanner uses for
thumbnails.

## 1. Approach

At compose **execution** time (never admission — that stays probe-free
arithmetic), any input that is an animated WebP is *bridged*: decoded in
Rust, its frames written as PNGs plus an `ffconcat` script carrying each
frame's duration, and that script substituted as the ffmpeg input for the
item. ffmpeg reads PNG + the concat demuxer universally; the frames are
lossless RGBA, so alpha and fidelity are preserved end-to-end.

The seam is `run_compose` (`media_tools/transcode/run.rs:718`), which
already maps `spec.sources` paths into `ComposeSource { path, probe }`:

- **Detect**: the source file is an animated WebP (reuse the
  `media_tools/animation.rs` WebP walk — `duration > 0` is the test; it is
  a content sniff, not an index lookup, so it cannot drift from the file).
- **Skip when native decode exists**: if `animated_webp_decodable()` is
  true (a future ffmpeg, or a user's patched `ffmpeg =` override), pass
  the file through unbridged — native decode is preferred automatically.
- **Extract**: stream-decode frames (one frame in memory at a time:
  decode → write PNG → drop) into a job-scoped `tempfile::TempDir` that
  lives until the ffmpeg run (including its `cache:` retry) finishes.
  Frame files use generated names only (`frame-%05d.png`) — no
  user-controlled strings anywhere near the script.
- **Script**: `ffconcat version 1.0`, relative filenames (concat "safe"
  mode is satisfied; no Windows path-quoting hazards), one
  `file`/`duration` pair per frame. The last frame's full duration must
  survive — the concat demuxer has historically dropped the trailing
  `duration` for image entries; verify empirically and apply the standard
  repeat-the-last-file-entry workaround if the toolchain needs it. The
  golden test pins whichever behavior ships.
- **Substitute**: `ComposeSource.path` becomes the script path; the input
  gains `-f concat` ahead of its `-i` and **no time options at all**.
  Empirically (7.1 and 8.0.1 both), `-ss`/`-to` on a concat script of image
  entries land on *entry boundaries* — the seek snaps to the next entry, or
  to nothing at all past the last one, which fails the whole graph — so the
  item's timestamps are honoured by the extraction window instead (§2): the
  script *is* the seeked content. `probe_source` is **skipped** for bridged
  inputs; the `StreamInfo` is synthesized from the decode itself (canvas
  width/height, `video_index: 0`, `has_audio: false`, `duration_s` = the
  script's total) — strictly more reliable than ffprobe on a script.
- Everything downstream of the input is unchanged: the `fps` resample,
  `span_loop` (whose arithmetic over `end_cs - start_cs` the windowed
  script's total matches exactly, modulo the 1 ms floor), memory guard,
  overlay — a bridged input is just another video input to the graph.

## 2. Extraction bounds (hostile input is assumed)

The scanner's parser bounds structure walks; the bridge decodes pixels, so
it needs its own budget:

- **Write only what the item shows** (because there is no usable seek, §1):
  `Span` → exactly the frames overlapping `[start_cs, end_cs)`, the first
  and last entries' script durations trimmed to the window so the script's
  total is exactly `end_cs - start_cs` (a frame ending exactly at
  `start_cs` is before the window); `Still` → its covering frame alone
  (or the animation's last, for a timestamp past the end — the clamp
  semantics an ordinary video's seek gets); `Image` → the first frame only.
  Frames *before* a window are still **decoded** — a WebP frame may be a
  delta over the previous canvas, so the compositor cannot skip them —
  just never written.
- **Hard frame cap** (constant, 3600 ≈ 60 s × 60 fps) on **decoded**
  frames, written ⊆ decoded. A span already inside its window truncates
  like the documented under-run case — `overlay eof_action=repeat` holds
  the last written frame, output length is unaffected — but a budget that
  runs out before the item's timestamp is even reached (a still's `at_cs`,
  a span's `start_cs`) is an extraction *failure* (below): shipping a
  nearby frame as if it were the right one is silent wrongness.
- **Decode ceiling**: a 16384 px per-side dimension bound via
  `image::Limits`, checked against the declared canvas before anything is
  allocated — and deliberately a dimension bound *only*: `WebPDecoder`
  has no `set_limits` override, so an allocation cap set there would be
  silently discarded (a claim of protection, not protection). image-webp's
  own constructor refuses any canvas whose area overflows `u32`; the
  bridge's bound covers the band under that (sides past 16384 whose area
  still fits), because its output is up to a budget's worth of canvas-sized
  PNGs on disk, not one decode. Worst-case transient memory is therefore
  one 16384² RGBA frame (~1 GiB); depth is bounded by the frame budget. A
  refusal takes the extraction-failure fallback.
- **Zero/negative frame durations** (spec-legal in WebP) are floored to
  1 ms in both the script and the cumulative accounting, guaranteeing
  progress; the resulting drift from the scanned total is absorbed by the
  existing under-run/over-run tolerance.
- **Cancellation** is checked between frames (the compose job's existing
  `AtomicBool`).
- **Extraction failure** (file parses as animated but `image-webp` cannot
  decode it): log a warning naming the input, pass the original file
  through unbridged, and let ffmpeg fail fast — the pre-bridge behavior,
  an error, never a hang. Bridging must not invent a new way for a board
  to succeed silently wrong.

## 3. Capability and caching consequences

- `span_capable_image_mimes` (hw.rs): `image/webp` becomes unconditional,
  like `image/gif` — the bridge needs only PNG decode and the concat
  demuxer, which every ffmpeg has. `image/avif` keeps its probe. The
  `animated_webp_decodable()` probe stays, demoted to a bypass switch
  (§1).
- **No `TRANSCODER_VERSION` bump**: compositions never record failure
  verdicts (`records_failures` is Single-only, pool.rs:239), so there are
  no stale negative verdicts to orphan; and pre-bridge documents classified
  WebP items as `image`, so newly-classified spans hash to different cache
  keys anyway.
- **No OpenAPI shape change**: the capability list's type is unchanged;
  only its runtime contents grow.
- **UI**: no functional change (the list drives everything). Comments and
  the previous design doc's WebP paragraph must be updated to point here —
  `animated-image-spans-design.md` §5 currently documents WebP as dormant.

## 4. Scope boundaries

- Compose inputs only. The single-file `/api/video/transcode` path
  (playability ladder) still cannot serve animated WebP; unchanged.
- No AVIF/GIF bridging — native decode works and is preferred.
- No persistent frame cache: extraction is per compose run, and compose
  outputs themselves are cached, so re-extraction happens only for new
  documents containing the same WebP. Revisit only if profiling ever says
  otherwise.

## 5. Testing

- **Extractor units** (no ffmpeg): against the committed
  `fixtures/two-frame.webp` — 2 PNGs, correct 0.5 s durations, script
  content exact; `Image`-time extraction stops at one frame; the frame
  budget and the 1 ms floor as pure-function tests; a still WebP and a
  garbage file refuse to bridge.
- **Golden compose tests** (ffmpeg-gated, skip-not-fail, same
  `scale_preserves_trailing_frame_duration` gating as the GIF tests): a
  span over the two-frame WebP renders both frames at their timestamps and
  loops to a cap; an `Image`-time animated WebP composes its first frame
  (this may be a fix, not just a test — see §0); the overshoot/under-run
  case holds the last frame at full output length.
- **Trailing-duration pin**: the golden span test's timing assertions are
  the empirical answer to the concat last-`duration` question; whichever
  workaround ships is asserted, not assumed.
- **Probe synthesis unit**: `build_filtergraph` over a bridged
  `ComposeSource` sees the synthesized `StreamInfo`.
- **UI**: `compose.test.mjs` fixture already lists `image/webp` as
  capable; add nothing beyond comment accuracy unless behavior changed.
