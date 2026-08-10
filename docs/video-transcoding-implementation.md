# Video transcoding — implementation plan

Status: PLANNED 2026-08-09, not implemented. Companion to
`docs/video-transcoding-design.md` (the design is authoritative for *what*;
this doc is authoritative for *how*, and for the contract deltas in §0 that
were settled during planning). Four phases, each executable as one or more
independent sessions; step boundaries below are safe cut points.

## 0. Contract deltas settled during planning

These refine or deviate from the design doc; the design doc governs where
silent.

1. **Compositions get a sibling route `POST /api/video/compose`**, not a
   variant body of `/api/video/transcode`. Rationale: separately rule-able
   (a policy may allow single-file re-encodes but deny N-input composition,
   which is strictly heavier), and compositions are addressed by composition
   hash, not `(id, id_type)` — folding both into one endpoint forces a
   tagged-union body and two unrelated artifact query grammars. Both routes
   return the identical job envelope and share jobs/SSE/cache. The
   single-item pinboard save is `compose` with `items.len() == 1`.
2. **`GET /api/video/artifact` never starts jobs in v1.** Primary query form
   is `key=<cache_key>` (every `ArtifactRef` carries it); the resolvable form
   `(id, id_type, preset, start_cs, end_cs)` is also accepted so the
   "serve-or-join/start" shape stays reserved for tee-streaming, but a miss
   is a 404 JSON (optionally carrying a live job ref) — a browser `<a>`
   navigation can't show progress, holds a connection through the encode, and
   crawlers must not spawn ffmpeg via GET. POST is the sole job creator.
3. **`DELETE /api/video/jobs/{id}`** exists (cancel; queued → terminal
   immediately, running → flag + child kill). No auto-cancel on SSE
   disconnect (a near-done encode is worth finishing for cache warmth).
4. **Presets DTO**: `{ id, label, container, ext, channel, surfaces }` —
   `ext` added so clients never maintain a container→extension table.
   Resolved encoder settings are deliberately not exposed. The response
   envelope also carries the compose limits (`max_mosaic_inputs`, canvas
   caps, `max_animated_image_seconds`) so the client builder clamps against
   live config, not mirrored constants (v1 fallback: hardcode defaults and
   rely on 422 handling).
5. **Compose document time model**: the design's "playing flag, muted flag"
   is folded into a `time` enum — `span{start_cs,end_cs}` *is* playing,
   `still{at_cs}` / `image` are stopped — plus a single `audio: bool`
   (client sets `playing && !muted`). Removes contradictory field
   combinations the server would otherwise cross-validate.
6. **HW probe candidates v1**: `h264_nvenc, h264_amf, h264_qsv,
   h264_videotoolbox, h264_mf`. **VAAPI deferred** — it needs
   `-vaapi_device` + `format=nv12,hwupload` filter-chain restructuring
   unlike every other candidate; Linux Intel gets QSV, others fall back to
   `libx264 -preset veryfast`. Leave a `// VAAPI:` comment in `hw.rs`.
7. **Codec columns + backfill are implemented in the Phase 2 stream** (design
   §9 lists them under Phase 1; nothing in the core depends on them, ordering
   is free, and they ship with their consumer).
8. **`cut=outro` composes with `start_cs`, excludes `end_cs`**, and is
   resolved to explicit centiseconds at POST time (shares cache entries with
   identical explicit trims; re-detection mints a new key). The job/cache
   layer never sees `cut=outro`.
9. **The `detect_outros` gate also blocks `cut=outro`** (same 404 as "no
   outro" — the off switch means off for every client at once; the UI can't
   reach this path anyway).
10. **Server-side outro guard**: cut at `content_end_ms − 60 ms` (named
    constant cross-referencing `ui/lib/videoTrim.ts` `OUTRO_GUARD_MS`; the
    audio-bang lead applies to exported files exactly as to playback).

## 1. Phase 1 — core backend

### Verified environment facts

- axum 0.8: SSE at `axum::response::sse::{Sse, Event, KeepAlive}`, no
  feature flag. No `tokio-stream`/`async-stream` in the tree — build the SSE
  stream with `futures_util::stream::unfold` over a `watch::Receiver`.
- Policy layer: `needs_db_params` (policy.rs:446) matches all `/api/` paths
  and injects `index_db`/`user_data_db` — **zero policy.rs changes needed**;
  handlers that don't need DB params just don't deserialize them.
- `tokio-util` has only the `io` feature — no `CancellationToken`; use
  `Arc<AtomicBool>` + child kill.
- All deployment ffmpegs ≥ 6.1 → `-fpsmax` (≥ 5.1) is safe for fps caps.

### Files

Create: `panoptikon/src/media_tools/transcode/{mod,hw,presets,run,cache,pool}.rs`,
`panoptikon/src/api/video.rs`, `panoptikon/src/api/http_file.rs`,
`panoptikon/migrations/transcode_cache/20260809120000_init.sql`.
Modify: `config.rs`, `media_tools/mod.rs`, `api/mod.rs`, `api/items.rs`
(refactor), `api/client_config.rs`, `main.rs`, `openapi.rs`, `shutdown.rs`,
`openapi.json` (regen), all five `config/server/*.toml`.

### Key contracts

```rust
pub(crate) const TRANSCODER_VERSION: i64 = 1;   // bump on any output-bytes change

#[derive(Serialize)]                     // canonical_json in decl order; a
pub(crate) struct TranscodeParams {      // pinned-fixture test guards drift
    pub source_sha256: String,
    pub preset: ResolvedPreset,          // RESOLVED settings — profile edits re-key
    pub start_cs: Option<i64>,
    pub end_cs: Option<i64>,             // absent hashes as absent, never as 0/duration
    pub transcoder_version: i64,
}
// key = "<sha256>-<hex(sha256(canonical_json))[..32]>"; file "<key>.<ext>"

#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum TranscodeJobEvent {      // SSE payload AND snapshot body;
    Queued  { position: usize },         // generic so jobs/queue.rs can adopt it
    Running { progress: Option<f32> },
    Done    { artifact: ArtifactRef },   // ArtifactRef { key, mime_type, size_bytes, url }
    Failed  { error: String, cancelled: bool },
}
```

`ResolvedPreset`: `{ id, label(#[serde(skip)]), container(Mp4|Webm|Webp),
vcodec, acodec: Option, quality(Crf|BitrateKbps), max_height, fps_max,
channel(Quality|Fast), surfaces(#[serde(skip)]) }`. Config-side
`TranscodeProfileConfig` has every field optional so an override can patch
one field of a built-in; tri-state resolution per `VectorQuantsConfig`
(absent → built-ins; empty map → none; entries → merged by name; a novel
name must specify at least container + vcodec, validated at load).

Built-in presets (tunable in code, ship nothing live in TOML):

| id | container | v/a | quality | max_h | channel | surfaces |
|---|---|---|---|---|---|---|
| `playback` | mp4 | h264/aac | crf 23 | 1080 | fast | playback |
| `clip` | mp4 | h264/aac | crf 18 | — | quality | clip |
| `clip-fast` | mp4 | h264/aac | crf 23 | — | fast | clip |
| `webp-anim` | webp | libwebp_anim | q 75 | 720 | fast | clip, mosaic |
| `mosaic-mp4` | mp4 | h264/aac | crf 18 | — | quality | mosaic |
| `mosaic-mp4-fast` | mp4 | h264/aac | crf 23 | — | fast | mosaic |
| `mosaic-webm` | webm | vp9/opus | crf 32 | — | quality | mosaic |

Encoder tunings: quality = `libx264 -preset medium`; fast-sw = `libx264
-preset veryfast`; nvenc `-preset p4 -cq crf+3`; amf `-quality speed`; qsv
`-preset veryfast -global_quality crf`; mf defaults; webp
`-c:v libwebp_anim -q:v 75 -loop 0`.

**hw.rs**: parse `-encoders` (pure fn, fixture-tested) → validate the
candidate with a 1-black-frame lavfi encode under a 15 s timeout →
`OnceLock`. `[transcode] hwaccel = "auto"|"off"|"<name>"` (accept `nvenc`
and `h264_nvenc` spellings). Probe spawn failure → `None` → libx264.

**run.rs**: blocking (`spawn_blocking`); command shape
`ffmpeg -nostdin -hide_banner -nostats -v error [-ss start] -i src [-t dur]
-progress pipe:1 [-vf scale=-2:'min(ih,H)'] [-fpsmax N] -c:v … [-c:a …|-an]
[-movflags +faststart] -pix_fmt yuv420p -y tmp`. stdout = `-progress`
key=value blocks read line-wise; stderr drained byte-wise on its own thread
(outro.rs:704 loop, minus the geometry scanner); child gets
`detach_from_console` + `die_with_parent` + `JobGuard::assign` — generalize
`process_tree.rs` with `std::process::Command` variants (it currently
targets tokio's). Drop impl kills+waits. `EncodeError::{Spawn, Failed,
Cancelled}` — Spawn maps via `media_tools::spawn_error` (blocked, never a
verdict); Failed is the negative-cacheable verdict.

**pool.rs**: one ractor actor (`OnceCell` + `ensure_*`, the
`ContinuousScanSupervisor` shape). State: FIFO `queued: VecDeque<Uuid>`,
`jobs: HashMap<Uuid, JobEntry{snapshot, watch::Sender, cancel}>`,
`by_key: HashMap<String, Uuid>` (in-flight dedup/join), terminal ring
(15 min / 512 entries), `running` count vs `max_concurrent_jobs`.
Submit: cache hit → `CacheHit(ArtifactRef)` (bump hit stats); negative-cache
hit → job born `Failed` (uniform client flow); `by_key` hit → `Joined`;
else queue + dispatch. After every transition re-broadcast
`Queued{position}` to remaining queued watchers (1-based, queued only).
**Job weights (Phase 4 forward-compat): a compose job with
`items.len() > compose_light_threshold` dispatches only when `running == 0`
and blocks further dispatch until it finishes** — exclusive occupancy
without a second pool; FIFO order preserves queue-position semantics.
Progress casts throttled ~4/s. Shutdown wired into `shutdown.rs::run_cleanup`
after `queue::shutdown_job_queue()`.

**cache.rs**: standalone `SqlitePool` (WAL, busy_timeout 5 s, 4 conns) on
`<cache_dir>/cache.db`; sqlx migrator on `migrations/transcode_cache/`
(reuse `normalize_line_endings`; no alembic baggage). Atomic writes:
`.tmp-<pid>-<key>.<ext>` → fsync → rename → **then** DB row (orphan files
are sweepable; orphan rows would 404). Startup reconciliation: drop rows
without files, files without rows, one eviction pass; `.tmp` sweeper
(foreign pid, >24 h). Eviction after every commit and on resize:

```sql
SELECT key, file_name, size_bytes FROM artifacts
WHERE pinned = 0
  AND NOT (hit_count >= 8 AND last_access > :now_minus_7d)  -- MFU nudge
ORDER BY last_access ASC LIMIT 32;
-- empty but still over budget → rerun without the hot-skip clause
```

Never delete the just-written artifact; one oversized artifact may exceed
budget (warn). Negative cache: `transcode_failures` table in the same DB
(global + content-addressed, unlike per-DB `storage.visual_attempts`;
version bumps re-key and orphan old failure rows for free). Two-strike:
first `Failed` allows a retry; `attempts >= 2` short-circuits Submit.
Spawn/`Blocker::Ffmpeg` failures are never recorded.

Schema (`migrations/transcode_cache/20260809120000_init.sql`):

```sql
CREATE TABLE artifacts (
    key TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL, params_hash TEXT NOT NULL,
    preset TEXT NOT NULL, file_name TEXT NOT NULL, mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL, transcoder_version INTEGER NOT NULL,
    created_at TEXT NOT NULL, last_access TEXT NOT NULL,
    hit_count INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0            -- share-link hook, never evicted
);
CREATE INDEX idx_artifacts_evict  ON artifacts (pinned, last_access);
CREATE INDEX idx_artifacts_source ON artifacts (source_sha256);
CREATE TABLE transcode_failures (
    key TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL, preset TEXT NOT NULL,
    error TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 1,
    last_attempt TEXT NOT NULL, transcoder_version INTEGER NOT NULL
);
```

### Config (`[transcode]`, host-level like `[jobs]`)

`max_concurrent_jobs` (1), `cache_dir` (Option; empty-string normalized like
tool paths; default `<data_folder>/transcode-cache`), `cache_size_mb`
(8192), `cache_size_max_mb` (262144, PUT ceiling), `hwaccel` ("auto"),
`profiles: Option<BTreeMap<String, TranscodeProfileConfig>>`. Plus Phase 4
keys: `max_mosaic_inputs` (12), `max_mosaic_loop_mb` (512),
`max_animated_image_seconds` (30), `max_output_seconds` (300),
`compose_light_threshold` (2). On `Settings` + `RuntimeConfig` (deep job
code has no Settings handle); `validate_transcode()` in
`Settings::validate`. Shipped TOML: commented example block only (every key
is a tunable default), appended to all five `config/server/*.toml`,
including a commented `[transcode.profiles.small-share]` example.

### API (`api/video.rs`)

- `POST /api/video/transcode` — `{id, id_type, preset, start_cs?, end_cs?}`
  (+ `cut` reserved for Phase 3, rejected 400 in v1). `DbConnection<
  ReadOnlyNoUserData>` → resolve sha/path/duration → validate → submit.
  Response `{outcome: "hit"|"created"|"joined"|"known_failure",
  job?: TranscodeJobSnapshot, artifact?: ArtifactRef}`; 200 hit, 202 else.
- `GET /api/video/artifact` — `key=` or resolvable form; serve via
  `http_file::serve_file` with `ETag = "<key>"`, `Cache-Control: public,
  max-age=31536000, immutable` (content-addressed, no mtime caveat),
  `Content-Disposition: inline` + indexed filename (Latin-1-stripped, per
  the `item_file` convention — the `<a download>` attribute carries UTF-8).
- `GET /api/video/jobs/{id}` (snapshot), `DELETE` (cancel),
  `GET /api/video/jobs/{id}/events` — SSE, first event = current snapshot,
  keep-alive comments every 10 s, stream ends after a terminal event.
  Register in openapi with a plain 200 description (utoipa can't model SSE;
  the event schema is a component via the snapshot endpoint).
- `GET /api/video/presets` — resolved presets filtered by the policy's
  `[policies.client] transcode_presets` array if present; rows per §0.4.
- `GET/PUT/DELETE /api/video/cache` — stats/resize/clear, mirroring
  `api/search_cache.rs:805` (PUT rejects over `cache_size_max_mb`, resize is
  runtime-only; DELETE clears unpinned artifacts + failures).

Capability: `video_transcode` probed off `POST /api/video/transcode`
(client_config.rs four-edit pattern). Shipped `restricted_demo`
(default/docker/nixos; desktop configs have only allow_all) gains

```toml
{ methods = ["GET"], path = "/api/video/artifact" },
{ methods = ["GET"], path = "/api/video/presets" },
```

— no POST, no jobs routes. (Ruleset blocks are live/frozen config: only new
deployments receive this.)

### `api/http_file.rs` refactor (behavior-preserving)

Move from items.rs: `RangeOutcome`, `parse_range_header` (:551),
`if_none_match_matches`, `not_modified_response`, `open_file_with_timeout`,
`FILE_IO_TIMEOUT`, and the response-assembly half of `try_file_response`
(:743-835) as `serve_file(FileServeSpec, &HeaderMap)`. Item-specific ETag
composition/mtime-drift policy/candidate fallback stays in items.rs. Move
the range/etag unit tests along.

### Ordered steps (each compiles + tests in isolation)

1. Config surface → `cargo test config` (tri-state parse, validation
   rejections).
2. `http_file.rs` refactor → moved tests pass unchanged, openapi fixture
   does not drift.
3. `presets.rs` → resolution/merge/surface-filter unit tests.
4. `mod.rs` hashing + `hw.rs` → params-hash **stability fixture** (pinned
   hex; changing serialization requires a TRANSCODER_VERSION bump),
   `-encoders` parse fixture, gated `validate_encoder("libx264")` test.
5. `cache.rs` + migration → tempdir tests: commit/hit/eviction (budget,
   pinned, hot-skip + fallback), reconciliation, sweeper, two-strike.
6. `run.rs` → arg-builder + progress-parser unit tests; gated e2e on a
   `write_clip` lavfi fixture (progress monotone, output exists).
7. `pool.rs` → actor tests with an injected runner closure (FIFO/positions/
   dedup/cancel/retention); gated e2e: second submit of same params =
   CacheHit.
8. `api/video.rs` + main.rs + openapi.rs + client_config.rs + shutdown.rs →
   route-shadowing tests, oneshot handler tests, SSE first-event test,
   `UPDATE_OPENAPI_FIXTURE=1 cargo test openapi` then clean run.
9. Shipped ruleset additions + capability fixture test; manual curl smoke
   (POST → SSE follow → artifact HEAD shows Accept-Ranges + immutable).

## 2. Phase 2 — playback compat + codec detection

### Server

**A1. Migration** `migrations/index/20260809120000_item_codecs.sql`:
`ALTER TABLE items ADD COLUMN video_codec TEXT; ... audio_codec TEXT;` +
`CREATE INDEX idx_items_codec_pending ON items(type) WHERE video_codec IS
NULL;`. Sentinel convention (in-band, **unversioned** — codec_name is a
fact, not a detector verdict): `NULL` never probed; `'none'` probed, no
video stream; `'unknown'` stream exists but ffprobe gave no name; else
codec_name verbatim. Audio: `NULL` conflates no-stream/unprobed —
**accepted**; termination keys on `video_codec` alone. Backfill population
is scoped to `type >= 'video/' AND type < 'video0'` (audio files get codecs
at scan time going forward but are not backfilled).

**A2. Scan-path persistence**: drop `#[allow(dead_code)]` on
`FfprobeStream.codec_name` (jobs/files.rs:5435); carry codec through
`AudioTrack`/`VideoTrack`; `ItemScanMeta` (db/files.rs:13) gains both
fields; populate in `extract_item_metadata` (jobs/files.rs:3556) per the
sentinel rules (first audio stream's codec); add the two binds to the item
INSERT (db/files.rs:463); fix literal `ItemScanMeta` constructors in tests
(db/files.rs:814, jobs/files.rs:8431/8484/8563).

**A3. Backfill** — not a job; the scan dispatcher's "fourth question",
mirroring outro (jobs/files.rs:2099-2320): `item_codec_pending(conn, sha)`
+ `set_item_codecs(conn, sha, video, audio)` next to their outro twins in
db/files.rs; `codec_work` flag keeps dispatch alive exactly as `outro_work`
does; worker runs `extract_media_info` and returns a `CodecRecord` through
the task outcome; trackless items settle synchronously (`'none'`, no
spawn — the `outro_verdict_without_a_probe` shortcut at :3179). ffprobe
failure = transient, retried next scan; guard permanent corruption with
`visual_attempts` `VisualKind::Codec` if the variant addition is contained,
else document retry-per-scan (ffprobe is cheap; outro measured 0.37%
failures). **No coupling to `detect_outros`** — codecs are scan metadata
like width/height.

**A4. API exposure — confirmed no gate** (codecs are objective facts like
duration; `serve_outro_metadata` exists for a feature *toggle*):
`ItemRecord` + SELECT + `ItemRecordResponse` + `map_item_record`;
`SearchResult` + `map_search_result` + `is_known_column`
(api/search.rs:1885); PQL `Column`/`OrderByField` variants + builder
name/expr tables (items columns — not text-search columns); optional but
recommended match filters ("find all HEVC"). Regen + commit
`openapi.json`.

### UI

**B1.** `npm run gen:api`; add both fields to the hand-written
`SearchResult` (lib/types.d.ts) and to **both** select lists in
`lib/state/searchQuery/searchQuery.ts` (:438, :585 — element-for-element
identical, load-bearing for the result cache).

**B2. `lib/videoPlayability.ts`** — pure, injectable `canPlayType`,
node-tested (`scripts/playability.test.mjs`). Ladder: not video →
`unsupported`; `'none'`/0 tracks → `unsupported`; `NULL` codec → **today's
exact mime check** (mp4/webm playable; else needs-transcode if capability
on) so nothing regresses during the backfill window; codecs known → RFC
6381 probe against `item.type` verbatim (`video/quicktime` etc. — the
per-browser answer is the truth wanted); `probably|maybe` → playable; else
needs-transcode (capability on) / unsupported. Unmapped codecs →
needs-transcode: the server job is the arbiter of untranscodable, no
client blocklist. Audio veto: mapped-playable video + unplayable/unmapped
audio → needs-transcode; `audio_codec == NULL` → probe video-only.

Mapping table (module data): h264→`avc1.42E01E`, hevc→`hvc1.1.6.L93.B0`,
av1→`av01.0.04M.08`, vp8→`vp8`, vp9→`vp09.00.10.08` (mp4) / `vp9` (webm),
mpeg4→`mp4v.20.9`, wmv3→unmapped; aac→`mp4a.40.2`, opus→`opus`,
vorbis→`vorbis`, mp3→`mp4a.6B`, ac3→`ac-3`, eac3→`ec-3`, flac→`flac`,
pcm_*→unmapped. Representative h264/hevc profile strings are the standard
compromise (profiles aren't stored); recovery for the High-10-style miss:
on a `playable` verdict, listen for the mounted `<video>`'s `error` event
and downgrade that sha to needs-transcode in-session.

Replace the three duplicated checks: ImageGallery.tsx:406,
GalleryPinBoard.tsx:2996, and :3061 (autoplay-short-pins: **`playable`
only** — a board never auto-starts transcodes). `data-playable` set for
`!== "unsupported"` (globals.css:286 contract unchanged).

**B3. Capability**: `videoTranscodeEnabled: capabilities.video_transcode
!== false` in `deriveClientConfig`. Capability off → tri-state collapses to
`unsupported`: preserves today's .mov behavior, and *changes* HEVC-mp4 from
broken-black-frame play button to no play button (intended; note in commit
message). Native playability is never suppressed (HEVC on Safari/HW-Chrome
still probes playable).

**B4. `lib/videoTranscode.ts`** — module-level sha-keyed store +
`useSyncExternalStore` (the videoEndProbe shape; gallery + pin share one
job). `TranscodeState = idle|requesting|queued{position}|running{progress}|
done{artifactUrl}|failed{error}`. `start()`: POST (dedup per `sha:preset`)
→ terminal snapshot short-circuit → `EventSource` on the events route;
close **immediately** on terminal event (the server ends the stream; an
unclosed ES would auto-reconnect forever — and close-on-terminal is what
honors the HTTP/1.1 6-connection budget). One native reconnect allowed;
second error without an intervening message (relay-buffering case) → fall
back to 1 s polling of the snapshot endpoint. `failed` cached for the
session (server negative-caches; don't re-POST in a loop), reload retries.

Host wiring: effective playback URL = artifact URL when needs-transcode
and done, else `fileURL`; `<video>` still `key={item.sha256}`. Play press
on needs-transcode calls `start()`; progress rendered by extending
`MediaControls` (PlayButton.tsx) with an optional progress prop — "#N"
queued, percent running, error glyph — so gallery (:971) and pin (:3373)
both get it and the pin `--spacing` clamp still sizes it. Download row and
drag-start keep the original `fileURL`.

Trim/probe audit results: `videoTrim.ts` needs nothing (element +
metadata based; artifact's clean timestamps keep the midpoint/probe guards
valid — add a comment). `videoEndProbe.ts` needs two changes: feed it the
*effective* playback URL (its doc comment demands the bytes the player
mounts; today both hosts pass the original fileURL) and gate on
`playbackURL != null`. `vt`/`h`/probe cache stay keyed by original sha —
correct per design.

**B5. Fixtures**: extend server `write_clip` with `vcodec: Option<&str>`
(libx265 + `-tag:v hvc1`; skip-not-fail per the outro test convention).
Manual browser fixtures documented in the PR: lavfi testsrc2 → `hevc.mp4`
(libx265+aac) and `movfixture.mov` (h264+pcm_s16le — exercises the audio
veto).

Order: A1→A2→A3→A4(+fixture regen)→B1→B2→B3→B4; A-steps shippable without
UI; B2/B3 shippable before B4.

## 3. Phase 3 — clip export

### Server

**S1. Parameters** (on the Phase 1 POST): activate the reserved `cut`
field. Rules: `cut=outro` excludes `end_cs`, composes with `start_cs`
(§0.8); explicit bounds require `end_cs > start_cs + 2` (2 cs mirrors
`FREEZE_EPS = 0.02 s`, named constant with a cross-reference comment —
freeze-frame "clips" are stills, 422 pointing at the pinboard save);
resolution: item lookup → `get_item_content_end_ms` (db/files.rs:348;
`None` → 404 "no detected outro") → gate check
`serve_outro_metadata(index_db, …)` (§0.9) → apply the 60 ms guard (§0.10)
→ floor to cs → validate → hash as explicit bounds.

**S2. Invocation** (run.rs already handles it via `TranscodeParams` bounds;
verify the arg contract): `-ss` **before** `-i` (fast seek + exact because
we re-encode), `-t end−start` **after** `-i` (input `-ss` resets
timestamps; an absolute `-to` would misread), `-map 0:v:0 -map 0:a:0?`
(audio optional — absence must not fail), `-sn -dn`,
`-avoid_negative_ts make_zero` (harmless insurance, mp4/mov),
`+faststart` mp4-only. Seconds formatted as `{cs/100}.{cs%100:02}` — never
float centiseconds.

**S3. Artifact download semantics**: per §0.2 the GET never starts jobs;
Content-Disposition `inline` with the transcode filename (server-side:
first file's stem + `-clip` when trimmed else `-{preset_id}` + preset ext;
pathless → `{sha[..10]}-clip.{ext}`); the `<a download>` attribute carries
the authoritative UTF-8 name (house convention, ImageGallery.tsx:897).
IMPLEMENTED with one addition: because `ArtifactRef.url` is the `key=` form
— which knows neither the source's path nor whether the request was
trimmed — `ArtifactRef` gained a **`filename`** field carrying that same
server-computed name, on both the cache hit and the SSE `Done` event. The
`key=` GET keeps its hash-prefix disposition name (a key cannot know about
trims); the resolvable GET form is unchanged. **U6 is therefore a
pass-through**: hand `artifact.filename` to `<a download>` rather than
re-deriving a name client-side (§0.4 precedent).

**S4. Tests**: `cut=outro` resolution (guard math, NULL→404,
`write_detect_outros_config` gate flip both ways), exclusivity 422s,
params-hash trim-vs-untrimmed and outro-vs-explicit-equal keys, argv
snapshots (`-ss` placement, `-t` not `-to`, faststart per container).

### UI

**U1.** Capability + presets hook `lib/useVideoPresets.ts` (`$api`,
`enabled: config?.videoTranscodeEnabled === true` — strict true so a
loading config never fires a 403-able request; `staleTime: Infinity`;
filter by surface tag).

**U2. `lib/videoClip.ts`** — the shared POST→progress→download engine:
per-item busy guard (module-scope keyed Map + `useClipBusy(sha)`, the
`menuGuard` pattern — survives Radix unmounting menus on select);
progress-then-replace toast (the `useSelectionExport` pattern; verify
`toast().update` exists in components/ui/use-toast, else
dismiss-and-reissue); flow: guard → POST → cache-hit short-circuit → SSE
with poll fallback → `downloadURL(artifact.url, filename)` → receipt
toast → clear guard. Pure row-decision helper:

```ts
clipRequestFor(trim, effective, outroGoverns):
  outroGoverns → { start_cs?, cut: "outro" }        // ignore client-computed end
  empty effective → null                            // caller shows Re-encode rows
  freeze frame (≤ FREEZE_EPS) → null
  else → { start_cs?, end_cs? }                     // Math.round(s * 100)
```

node-tested (`scripts/clipRequest.test.mjs`, six-case table).

**U3. `VideoDownloadControl`** in VideoPlayerSurface.tsx: split button at
picture top-right — primary `<a download>` = original file (today's
behavior, one click), chevron opens a `SurfacePopover placement="below"`
(non-portalled — element-fullscreen invariant). Fades with
`controller.visible`; holds the surface via `setHold("menu", open)` +
unmount cleanup; stops pointer/mouse/click/dblclick so click-to-navigate
halves survive. Rows: "Original file" (`MenuItemLink`); then from presets
surface-tagged `clip`: effective trim non-empty → "Clip (trimmed)" /
"Clip (trimmed, fast)"; empty → "Re-encode" / "Re-encode (fast)"; custom
presets by their label. Transcode rows are **buttons** (`MenuItem`), never
links. Freeze-frame trim ⇒ treated as no trim (Re-encode rows — no
disabled ghosts). Capability off or presets empty ⇒ no chevron at all,
primary button only (the hide-don't-disable house rule).

**U4. Gallery mount** (ImageGallery.tsx ~:881): sibling overlay anchored to
`pictureBox` exactly like the S2 `NativeControlsEscape` box; inside
`playerHostRef` so it survives element fullscreen. All inputs (trim,
outroCut, outroSkip, effectiveTrim, dbs) already exist in the host. Keep
the kebab's "Download original" row (serves the mini tier; if the control
crowds narrow surfaces, gate the chevron to `size !== "mini"`).

**U5. Pins**: no new overlay (top-right is Select/Find). Thread
`effectiveTrim`, `outroGoverns`, `clipItem {sha256, path, mime}` into
`PinBoardCtx` (computed next to GalleryPinBoard.tsx:3105-3113); render
clip rows after the loop-verbs block (PinBoardContextMenu.tsx:439-457) as
Radix `ContextMenuItem`s, `disabled={useClipBusy(sha)}`, same
`clipRequestFor`/`exportClip` engine. Rows require resolved item + video
mime + presets.

**U6. Filename**: SUPERSEDED by S3 as implemented — the server sends
`ArtifactRef.filename`, so the client passes it through instead of deriving
one. The original plan (kept for the rule it encodes):
`transcodeFileName(path, sha, suffix, ext)` next to
`downloadFileName` (lib/utils.ts:31) — stem + `-clip` (trimmed) or
`-{presetId}` (re-encode) + preset `ext` (from the DTO, §0.4). No
timestamp stamp: artifacts are deterministic, re-download legitimately
overwrites.

**U7.** Tests + manual pass: trim → clip download frame-accurate; TikTok
with outro-skip and no user trim sends `cut=outro` (devtools); equal
bounds → Re-encode rows; policy-off → chevron gone; fullscreen menu opens
inside; pin rows disabled during a job; double-click joins.

Order: S1→S2→S3→S4 → U1→U2(+U6)→U3→U4 / U5 → U7.

## 4. Phase 4 — pinboard save + animated mosaic

### Server

**C1. `media_tools/transcode/compose.rs`** — document + validation.
Request: `{index_db?, canvas{w,h,background}, fps, output{preset,
length: longest_loop_once | cap{seconds}}, items[]}`; item:
`{sha256, src{x,y,w,h} (source px, PRE-orientation = sourceRect × natural
dims), transform{quarter_turns, flip_h}, dest{x,y,w,h} (output px),
time: span{start_cs,end_cs} | still{at_cs} | image, audio}` (§0.5).
`end_cs` **required** for spans (client always knows duration; target
length = pure arithmetic, no probing). Validation with named 422 reasons:
items 1..=`max_mosaic_inputs`; canvas even, ≤ `max_canvas_side`/area caps;
dest inside canvas; src clamped (not rejected) to probed stream bounds;
fps 1..=60 then preset cap; span end>start; target clamped to
`max_animated_image_seconds` (webp) / `max_output_seconds` (mp4/webm);
**admission-time memory guard**: `Σ loop_frames_i × dest_w × dest_h × 1.5B
≤ max_mosaic_loop_mb` (loop_frames = 0 for the longest span, 1 for
stills) — the estimate goes in the 422 message. Cache key =
`sha256(canonical_json(request) ‖ resolved_preset ‖ TRANSCODER_VERSION)`.

**C2. Filtergraph builder** — pure `build_filtergraph(...) -> FilterPlan
{inputs, filter_complex, output_args}`, golden-string-tested. D4 table
(display = flipH^f ∘ rotCW^q; crop in source space, so chain =
`crop → transpose* → hflip?`): q1→`transpose=1`, q2→`hflip,vflip`,
q3→`transpose=2`, +f appends `hflip` — no clever collapsing. **Loop
mechanism: the `loop` filter after scaling to dest size** (buffers
seg_frames at dest resolution — bounded, computable; the longest item
needs no loop filter at all). Rejected: `-stream_loop` (loops the file,
not the trimmed segment), repeated `-i` + concat (decoder count =
Σ loops). Per-item chains:

- span: `-ss start -to end -i f` → `setpts=PTS-STARTPTS, crop, <D4>,
  scale=dw:dh:flags=lanczos, fps=N, [loop=L:size=segframes,]
  trim=end=target, setpts=PTS-STARTPTS`
- still: `-ss at -i f` → `trim=end_frame=1, crop, <D4>, scale,
  loop=-1:size=1, setpts=N/(fps*TB), trim=end=target`
- image: `-loop 1 -framerate N -i f` → `crop, <D4>, scale,
  trim=end=target, setpts=PTS-STARTPTS`

Base `color=c=bg:s=WxH:r=fps:d=target[base]` → left-fold `overlay=x:y` in
item order → `format=yuv420p` (mp4/webm) / `yuva420p` (webp). Audio
(mp4/webm, skipped entirely → `-an` when no item has audio): per-item
`asetpts, [aloop,] atrim=end=target, aresample=async=1` →
`amix=inputs=M:duration=longest:dropout_transition=0` (default
normalization on; revisit with `alimiter` if too quiet). Single-item save
emits the simplified no-overlay graph (canvas = crop size, preset-capped).

**C3. Fixture e2e** (gated on `ffmpeg_available()`): two lavfi clips →
2-item compose (span cropped+rotated + still); ffprobe dims/duration,
pixel-assert colors inside dest rects and background outside (the
`corner_is_card` precedent); repeat webp (`nb_frames > 1`) and webm.

**C4. Route + pool**: `POST /api/video/compose` (join/dedup by doc hash,
same job envelope); heavy-compose exclusivity per Phase 1 job weights;
`artifact?key=` already exists (§0.2); no separate capability (the
`/api/video/` POST rules cover it).

### UI

**C5. Geometry reuse — the pixel-for-pixel guarantee.** Refactor A:
extract `resolvePinDraw(placement, naturalW, naturalH) -> {src, dest}`
into `lib/pinboardGeometry.ts` (pure, node-testable) from the four lines
`drawPin` (lib/pinboardPreview.ts:126-183) composes inline
(`orientedSize` → `computeRestGeometry` → `sourceRect`); move
`computeRestGeometry` out of CropView.tsx into the pure module (re-export
for existing consumers); `drawPin` calls it — **byte-identical draws,
`scripts/mosaic.test.mjs` is the regression net**. Refactor B: carry
`trim` through `parsePlacements` onto `PinPlacement` (parseHField already
returns it). `composeBoardMosaic` itself: no changes — the doc builder
reuses the same exported solve functions it is built from.

**C6. `lib/pinboardCompose.ts`** — `buildCompositionDoc(opts)` mirroring
`composeBoardMosaic`'s solve step-for-step (same liveScale/visibleRows/
solveAt/clamp loop, clamp bound = server limits from the presets
envelope), then per placement: metadata per unique sha (react-query cache
hit — every pin already queried it) → `resolvePinDraw` → integer px rects
→ `transform` = orient verbatim → `time` from trim + probed state.
Missing-dimension pins: skip + name them in the completion toast (never
silently render grey boxes). Stopped untrimmed video: `still{at_cs: 0}` —
accepts the thumbnail-vs-frame-0 mismatch, documented.

**C7. State capture**: `probePinVideoState(key)` in `lib/pinboardMedia.ts`
next to `findPinVideoFrame` (DOM probe via `data-pin-key`): playing =
`!paused && !ended && readyState >= HAVE_CURRENT_DATA`; muted from the
element. Resolution: mounted+playing → span (start = trim.start ?? 0,
end = trim.end ?? duration), `audio = playing && !muted`; paused or no
mounted video → still (equal-bounds trim carries the frame; else trim
start; else 0); non-video → image. No playhead state is ever sent.

**C8. Job flow + menus**: `lib/pinboardAnimatedExport.ts` structured like
`useMosaicExport` — `exportGuard` (one export at a time, canvas or
server), POST compose, SSE progress toast ("Queued (#N)" → "Rendering…
42%"), `downloadURL` on done, server's named 422 reason on failure
(the memory-guard message tells the user to shorten the cap). Menus:
`PinboardMosaicMenu.tsx` gains an animated section (rows from presets
surface-tagged `mosaic` — WebP/MP4/MP4 fast/WebM + custom presets
automatically; reuses extent/seamless prefs; length-policy radio rows
"Longest clip plays once" / "Cap at 5/15/30 s" persisted in
`pinboardMosaicPrefs`). `PinboardExportMenu.tsx`: 2+ selection → animated
rows over `only`-scoped doc; single video pin → the single-item
transformed save (canvas = crop, offered when the result is a span —
a stopped pin's still is already served better by the existing image
path). Gating: `videoTranscodeEnabled` AND at least one in-scope video
pin (metadata cache; DOM `data-playable` fallback) — a stills-only board
shows no animated rows.

Order: C1→C2→C3→C4 strictly; C5-C6 parallel with C1-C4; C7-C8 need C4.

## 5. Cross-phase notes

- Suggested session sequencing: Phase 1 steps 1–5 / 6–9 as two server
  sessions; Phase 2 A (server) and B (UI) as two; Phase 3 S then U;
  Phase 4 C1-C4 then C5-C8. Every boundary above leaves the tree green.
- `openapi.json` regen + `ui` `npm run gen:api` are the hand-off points
  between server and UI work in every phase.
- The relay SSE-buffering open item (design §10) is covered defensively
  either way: 10 s keep-alives server-side, poll fallback client-side.
- Deferred beyond this plan: VAAPI encoder support (§0.6), tee-streaming
  playback, share links (pinning lifecycle), animated AVIF.
