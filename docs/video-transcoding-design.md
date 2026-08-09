# Backend video transcoding

Status: DESIGNED 2026-08-09, not implemented.

One backend system serving three product features plus one future plan:

1. **Playback compatibility** — playing videos the browser cannot decode
   (.mov, .mkv, HEVC-in-mp4) through the gallery player.
2. **Clip export** — downloading the trimmed portion of a video (user A/B
   trim, or the server-known TikTok outro cut), turning the A/B loop into a
   clipping tool.
3. **Pinboard renders** — saving a single transformed video (trim + crop +
   D4 orientation) and, later, **animated mosaics** of a whole board or
   selection.
4. *(Future, shapes decisions here)* **Share links** — signed public URLs
   whose targets must be plain, directly playable files so Discord/Matrix
   link previews can hotlink them.

## 1. Architecture: one artifact pipeline, no HLS

Every transcode produces a **plain finished file** (mp4/webm/webp) in a disk
cache; delivery is the ordinary HTTP file-serving machinery. There is no HLS,
no segmenter, no playlist state, no client media library.

Rationale (settled 2026-08-09):

- Features 2 and 3 are inherently transcode-to-completion — the output *is*
  a file. Share links are disqualified from HLS outright (link previews need
  a directly playable URL). HLS would be a second delivery system built
  exclusively for feature 1 — the most marginal feature by the user's own
  accounting (0.01% of the library, mostly short files).
- The entire UI media stack assumes plain same-origin `<video src>` + Range:
  trim enforcement (`lib/videoTrim.ts`), the rVFC end probe
  (`lib/videoEndProbe.ts`), outro arithmetic, `<a download>` rows (the
  `download` attribute is silently dropped cross-origin).
- The file endpoint (`api/items.rs` `item_file`) already has complete
  single-range / ETag / If-Range machinery to reuse. An artifact keyed by
  content hash + params gets `Cache-Control: immutable` *without* the
  mtime-drift caveat the original-file path carries.

**Playback of a not-yet-cached rendition = run the job to completion, show
queue position then progress, then mount the ordinary `<video>` against the
artifact URL.** Accepted UX: a few seconds of progress overlay on the play
affordance for the rare unplayable short file.

**Reserved upgrade path (not v1):** tee-streaming — encode fragmented MP4
(`-movflags frag_keyframe+empty_moov`), tee ffmpeg output to both the cache
file and a chunked HTTP response so playback starts as bytes arrive. Same
endpoint, no HLS. The artifact GET must therefore be shaped as "serve cached
artifact, or join/start the job" so this can be added without a new route.

## 2. Core module and job model

New `media_tools/transcode.rs` (+ submodules), following house conventions:
binary from `media_tools::ffmpeg()`, spawn failures via
`media_tools::spawn_error` (a missing toolchain is `Blocker::Ffmpeg`, never a
media verdict), non-zero exits are verdicts on the input, piped-child hygiene
copied from `outro.rs` (byte-wise stderr drain, abort-on-drop, no orphans;
`process_tree.rs` die-with-parent on long children).

**Not the job queue.** `JobQueueActor` is strictly serial process-wide;
transcodes must not block behind folder scans or vice versa. Transcoding gets
its own worker pool (shape precedent: the continuous-scan supervisor, which
also lives outside the queue):

- Bounded concurrency: semaphore, default 1–2 concurrent ffmpeg jobs
  (`[transcode] max_concurrent_jobs`, serde default). HW encoder sessions
  are also limited; the pool bound covers both.
- FIFO queue with **exposed queue position** — a queued client sees
  "Queued (#N)" before progress starts.
- **Dedup / join:** in-flight map keyed by cache key; concurrent requests for
  the same rendition attach to the one job (cf. `BatchDedup` in the queue).
- Progress parsed from `ffmpeg -progress pipe:` (`out_time_ms` vs known
  duration).
- Failure recording can use the `visual_attempts` ledger pattern: per the
  failed-media-retry rules, gateway-produced artifacts' failures *are*
  pipeline verdicts, so "this file cannot be transcoded" is negative-cacheable.

### Job events: SSE, scoped and short-lived

Job state is **pushed over SSE**, not polled. Decision rationale:

- axum has first-class SSE (`axum::response::sse`); server cost is trivial.
- Queue-position + percent-progress is genuinely push-shaped; polling is
  chattier and laggier for the same information.
- The one real cost is the HTTP/1.1 6-connections-per-origin limit (the
  gateway serves plain HTTP; browsers only speak h2 over TLS, and desktop
  HTTPS is designed-not-implemented — the grid already saturates those 6
  connections). This argues only against an *always-open* global stream, not
  against a stream held **only while a job is pending/running** and closed on
  terminal state. That is the contract here.
- The existing polled job/queue status page predates this and is expected to
  migrate to the same mechanism eventually; the event envelope below is
  deliberately generic so `jobs/queue.rs` can adopt it later.

Surface:

- `GET /api/video/jobs/{id}` — JSON snapshot (state, queue position,
  progress, error, artifact ref). Exists for late joiners and as the trivial
  fallback; SSE's first event is always the current snapshot.
- `GET /api/video/jobs/{id}/events` — SSE stream of
  `{state: "queued", position}` → `{state: "running", progress}` →
  `{state: "done", artifact}` | `{state: "failed", error}`, with keep-alive
  comments (relay/proxy buffering mitigation). Stream ends after a terminal
  event.

## 3. Artifact cache

- **Global, content-addressed, on disk.** One cache for the whole process —
  artifacts are keyed by source content hash, so index-DB membership is
  irrelevant. Location configurable: `[transcode] cache_dir`, default under
  the data folder (e.g. `data/transcode-cache`); serde default + commented
  example per config-authoring rules.
- **Key = `sha256` + params hash**, where the params hash covers: preset id
  and its *resolved* settings, trim bounds (centiseconds), transcoder
  version. Same key = same bytes forever → `immutable` caching.
- **Sidecar SQLite** in the cache dir (not per-index storage.db — videos are
  far too big for the BLOB pattern): rows track path, bytes, last_access,
  hit_count, `pinned`.
- **Eviction:** byte-budgeted LRU whose victim selection is weighted by hit
  count (the MFU nudge; keep it simple — skip recently-frequent entries,
  nothing fancier). Budget `[transcode] cache_size_mb` (serde default),
  runtime-resizable.
- **`pinned` is the share-link hook:** a future public link pins its
  artifact; pinned rows are never evicted; revoking the link unpins. This
  answers "what happens to an evicted video behind a permanent link" — it
  can't happen while the link lives.
- **Atomic writes:** the `resources.rs` pattern — encode into
  `.tmp-<pid>-…/`, marker, `rename`, stale-temp sweeper.
- **Admin surface mirrors the search span cache:** `GET` stats /
  `PUT` resize / `DELETE` clear on `/api/video/cache`.
- Clips and one-off renders go through the same cache — no second uncached
  path. Arbitrary trims mostly won't re-hit; they're small and eviction
  handles the churn.

## 4. Hardware acceleration

First ffmpeg-side HW awareness in the codebase (existing detection is all
ML-stack). `OnceLock` probe in `media_tools/`:

1. Parse `ffmpeg -encoders` for candidates (`h264_nvenc`, `hevc_nvenc`,
   `h264_qsv`, `h264_vaapi`, `h264_videotoolbox`, `h264_amf`).
2. **Validate** the winner with a tiny real encode (one black frame) —
   listed ≠ working; drivers lie constantly.
3. Silent fallback to `libx264` always exists.

Config: `[transcode] hwaccel = "auto" | "off" | "<name>"`, following the
`[jobs]` tool-override idiom.

**Open verification item:** whether the `static-ffmpeg` pip builds and the
Docker image's Debian ffmpeg actually ship nvenc/amf/qsv (Debian ffmpeg has
nvenc; static builds usually include nvenc, often lack vaapi/qsv). Determines
how real "GPU where available" is per deployment target. Verify before
implementation planning hardens.

## 5. Presets and the fast channel

Config model copies the `VectorQuantsConfig` tri-state: **absent → built-in
presets; explicit empty → none; user entries → merged by name, overriding
built-ins.** Lives in the *global* Settings TOML (`[transcode.profiles.<name>]`,
BTreeMap like rulesets) — encoders are a host property, not per-DB. All
shipped as serde defaults + commented examples, never live lines.

Preset fields: `label`, container, vcodec/acodec, quality mode (CRF or
bitrate), max resolution, fps cap, audio on/off, **`channel: "quality" |
"fast"`**, and surface tags (`playback`, `clip`, `mosaic`, …) controlling
where the UI offers it.

**The fast channel (user decision 2026-08-09):** every save surface offers a
default (quality) option and a **"fast"** option. Quality channel = software
x264 at a decent CRF (HW encoders are meaningfully worse per bit; export
quality is the point). Fast channel = HW encoder when the probe validated
one, else `libx264 -preset veryfast`. Playback-cache renditions always use
the fast channel (throwaway quality, latency matters).

Built-ins (initial set): `playback` (h264+aac mp4, cap 1080p, fast),
`clip` (quality) / `clip-fast`, `webp-anim`, `mosaic-mp4` (quality) /
`mosaic-mp4-fast`, `mosaic-webm`. Animated AVIF is deliberately deferred
(libaom animation encode is punishingly slow); webm (vp9+opus) is in — no
availability problem, just slower encodes than x264, which the channel model
already expresses.

`GET /api/video/presets` returns the resolved, policy-filtered list
(id + label + surface tags) — this is how **user-declared custom presets
appear in the UI dropdowns automatically**.

## 6. Codec detection (v1, part of the point)

ffprobe already returns `codec_name` per stream at scan time; it is parsed
and discarded (`#[allow(dead_code)]`, `jobs/files.rs:5435`). Persist it:

- Migration: `items.video_codec TEXT`, `items.audio_codec TEXT` (nullable),
  written by `extract_media_info` consumers at scan.
- Backfill: the `outro_kind` pattern — partial index on NULL, background
  backfill pass dispatching ffprobe-only probes for existing video items.
- Exposure: `/api/items/item` metadata + PQL selectable columns, same as
  `content_end_ms`.

Client playability becomes one function in `lib/` (today it is a hardcoded
`type === "video/mp4" || "video/webm"` duplicated in `ImageGallery.tsx` and
`GalleryPinBoard.tsx`) returning a tri-state:

- `playable` — mime + codecs pass `canPlayType` with a codecs string.
- `needs-transcode` — container/codec unplayable but transcodable (covers
  .mov/.mkv *and* HEVC-in-mp4, which today mounts a `<video>` and fails
  silently to a black frame; also audio-codec-only failures, e.g. AC-3).
- `unsupported` — no video stream / known-untranscodable.

The pinboard's `data-playable` CSS hook must keep working; `needs-transcode`
shows the play affordance (today an unplayable file shows *no play button at
all*).

## 7. API surface and policy

**Path prefix `/api/video/` — deliberately not under `/api/items/`.** Every
shipped `restricted_demo` ruleset grants `GET /api/items/`; a transcode
endpoint there would be silently inherited by public profiles (the
`pinboard_search` precedent: moved out of `/api/search/` for exactly this
reason).

- `POST /api/video/transcode` — create/join a job (body: id/id_type/index_db,
  preset, trim params or `cut=outro`, or the mosaic composition doc).
- `GET  /api/video/artifact` — serve a cached rendition (Range/ETag
  machinery reused from `item_file`; same `(id, id_type, index_db)` query
  conventions so `useSelectedDBs` plumbing and the sha-keyed probe cache
  behave).
- `GET  /api/video/jobs/{id}`, `GET /api/video/jobs/{id}/events` — §2.
- `GET  /api/video/presets` — §5.
- `GET/PUT/DELETE /api/video/cache` — §3.

POST-transcode and GET-artifact are **separately rule-able**: a public
policy can serve already-encoded artifacts (share-link future) while denying
new conversions ("no video conversions on the public view").

Capability: `pub video_transcode: bool` in `ClientCapabilities`, probed off
`POST /api/video/transcode` per the four-edit pattern; UI gates via
`clientConfig` with the `!== false` default-on convention. Per-policy limits
ride the free-form `[policies.client]` table (e.g. `transcode_presets`,
`max_transcode_resolution`) — profiles stay global, only exposure/limits are
per-policy.

## 8. Feature specifics

### Playback compat (gallery)

`needs-transcode` → play affordance shows queue position, then progress
(SSE), then the `<video>` mounts against `/api/video/artifact`. Transcoded
renditions have clean timestamps (no edit lists / audio priming), so the
browser-vs-ffprobe timeline gap the outro probe compensates for shrinks;
trim (`vt`) stays keyed by the *original* item sha throughout.

### Clip export

- **One action applies the effective trim** (user trim, or the outro default
  when outro-skip governs) — no separate button in the trim popover; the
  TikTok user never needs to know the trim feature exists.
- Client sends explicit `start`/`end` centiseconds (already in `vt`). The
  server **never parses** the pinboard `layout` blob or `vt` — UI owns those
  formats.
- **Outro path is distinct and server-side:** `cut=outro` → server reads
  `items.content_end_ms` itself. This is *more* accurate than any
  client-sent timestamp: ffmpeg cuts in the ffprobe timeline natively; the
  whole client probe/midpoint dance exists to compensate browser-timeline
  drift that server-side cutting never sees.
- Clips **re-encode** (exact cuts, smaller share-friendly files, one code
  path). Keyframe-aligned `-c copy` "instant lossless" is a possible later
  preset, not v1.
- **UI:** download button + chevron dropdown at the video's top-right — the
  S1 upper-right is free in the gallery (only `NativeControlsEscape` uses
  that slot, in the other mode). Menu rows: "Original file" (exists),
  "Clip (trimmed)" default + fast variants when a trim/outro cut is active,
  "Re-encode" default + fast for shrink-to-share without a trim; rows beyond
  Original come from `/api/video/presets` surface-tagged `clip`. On **pins**
  the top-right is occupied (Select/Find) — clip actions go in the existing
  kebab / context menu instead.

### Pinboard save + animated mosaic

Governing rule (already identified when this was explored and deferred in
`pinboard-gravity-mosaic-proportional-design.md`): **never port the client's
crop/orientation/fit math to Rust.** The client resolves everything into
ffmpeg-ready parameters — per item: source-pixel crop rect, transpose/flip
ops, dest rect, trim bounds, playing/muted flags — and POSTs that
composition document; the server is a dumb filtergraph assembler that never
learns pinboard semantics and never parses the `h` codec.

- **Single-item save** is the bridge feature: whole pipeline + geometry for
  one input.
- **Animated mosaic:** N-input filtergraph (trim/loop/scale/crop/transpose/
  overlay); shorter loops repeat; `amix` of unmuted tracks for mp4/webm
  outputs (muted state respected instead of an audio option; webp has no
  audio).
- **Capture semantics (user decision):** playing videos start from their
  trim start / file start, never the live playhead; stopped videos are
  stills — the equal-bounds freeze-frame trim encoding already carries the
  frame choice, so the client sends no playhead state at all.
- **Length policy:** default "longest loop completes once"; explicit length
  cap option; hard server-side cap for animated-image outputs (tunable,
  serde default), no cap needed for real video outputs.
- Formats: webp + mp4 + webm (§5). GIF excluded on principle.
- Client UX reuses `exportGuard` and the progress-toast pattern; long jobs
  ride the same SSE surface.

## 9. Phasing

1. **Core:** `media_tools/transcode` (HW probe, preset resolution, worker
   pool with queue-position + dedup + SSE), disk cache + eviction + admin
   endpoints, `/api/video/*` routes, `video_transcode` capability, config
   surface. Codec columns + backfill.
2. **Playback compat:** playability tri-state in `lib/`, queue/progress play
   UX, cached rendition playback.
3. **Clip export:** download chevron UI, trim-applying clip path,
   `cut=outro` server path.
4. **Pinboard:** single-item transformed save, then animated mosaic.

## 10. Open items

- Verify HW encoder availability in static-ffmpeg builds and the Docker
  Debian ffmpeg (§4) before implementation hardens.
- SSE through the relay: confirm the relay path doesn't buffer
  `text/event-stream` (keep-alives mitigate; snapshot endpoint is the
  fallback).
- Exact built-in preset parameter values (CRF, resolution caps, fps caps) —
  decide during implementation.
- Share-link integration details (token scheme, pinning lifecycle) — future
  design, only the `pinned` hook and the POST/GET policy split are settled
  here.
