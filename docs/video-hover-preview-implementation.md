# Video hover previews in the grid and filmstrip

Implementation notes, 2026-09-03. Companion to
`docs/grid-hover-animate-implementation.md`, whose arming machinery (D6/D7:
pointermove-then-dwell, one hover-playing cell, cancel on leave/scroll) this
reuses unchanged, and to `docs/video-transcoding-design.md`, whose pool, cache
and preset table it extends by two rows.

Before this, a `video/*` item never moved in the grid or the filmstrip: it
showed a frame still (1x1 in small cells, 2x2 otherwise) with the play badge,
and the only hover behaviour was the small-cell frame→mosaic image swap.

The plan was drafted around a single transcode rung behind direct playback of
the original. Browser verification falsified the premise that made "just play
the original" free, and the byte-capped ladder in §2 is the answer that
replaced it. §1 records what did not change.

## 1. Settled decisions

| # | decision | value |
|---|---|---|
| V1 | Trigger | the existing hover arm (real pointermove, 200 ms dwell, not scroll-suspended). Never on intersection, never on scroll settle. At most one previewing cell; leave/switch cancels. |
| V2 | Rung 0 — direct playback | for a `playable` item (the codec/container ladder in `lib/videoPlayability.ts`, fed by the `video_codec`/`audio_codec` columns the search rows already carry) **whose file is at or under the byte cap**: a muted `<video preload="none" loop playsinline>` on the original file URL, Range-served. **Nothing is requested before the dwell fires**: until then the cell holds a plain `<img>` poster and no `<video>` element exists, so the grid as a whole issues zero video requests. On leave the element is unmounted and its request aborted (`abortVideo`). With the outro-skip preference on and a detected outro on the row, the element loops at the cut in the browser rather than through the end card (§8). |
| V2b | Rung 1 — preview trim | over the cap, for an item **longer than 16 s** whose estimated slice fits under the cap and whose codec is browser-playable and mp4-muxable: the new `preview-trim` preset — the source's own packets remuxed into a 16 s mp4, no decode and no encode. An item of 16 s or less has no shorter slice to offer, so it skips this rung entirely — unless its detected outro cuts it shorter, in which case the slice is measured over the cut (§8). §2, §3. |
| V3 | Rung 2 — preview transcode | for a `needs-transcode` item (and a `playable` one that downgraded on a decode error), and for anything the two cheaper rungs cannot serve: the `preview` preset — first 16 s, mp4/H.264, no audio, short side ≤ 480 (`max_height 480`), fps ≤ 30, Fast channel, CRF 26. Ends at the detected outro when that comes first (§8). |
| V3b | Rung 3 | nothing. The cell keeps its still. |
| V4 | Rapid hover switching | one preview job per client at a time. A job this client *created* is cancelled (`DELETE /api/video/jobs/{id}`) on leave or switch if not done; a job it *joined* is never cancelled (it is someone else's). A cancelled key is freed by the pool, so re-hover resubmits. Cache hits skip the queue. |
| V5 | Gate, layer 1 — policy (negative override, wins over everything) | `[policies.client] hover_preview = false` turns **all three** rungs off for that policy; absent = allowed. Rungs 1 and 2 are additionally impossible wherever the policy already denies `POST /api/video/transcode` (public `restricted_demo`), and each can be denied alone by omitting its preset from that policy's `transcode_presets` list. Both are existing machinery. Policy blocks are live lines that freeze (CLAUDE.md); the default stays "allowed" so no seeded config needs a new line. |
| V6 | Gate, layer 2 — server default for rung 2 | `[transcode] hover_preview = "auto" \| "on" \| "off"`, serde default `"auto"` = on only where the hardware H.264 encoder probe succeeded (`hw::fast_h264_encoder()`). Rungs 0 and 1 have no such gate: neither runs an encoder. |
| V6b | The cap | `[transcode] hover_preview_max_bytes`, serde default 16 MiB. One number governs the whole ladder; §2. |
| V7 | Gate, layer 3 — browser preference | `localStorage` key `panoptikon.hoverPreviewPref`, shape `{ direct?: boolean, transcode?: boolean }`, absent = follow the server. "Originals" = rungs 0+1 (**never re-encode**); "All" adds rung 2. Same `valueBox`/`useSyncExternalStore`/cross-tab pattern as `lib/state/animatePref.ts`. Never in the URL. |
| V8 | What `/api/client-config` publishes | top-level `hover_preview: { direct, trim, transcode, max_bytes } \| null` — the server's resolved answer for THIS policy. `null` = feature absent (old server). The UI never re-derives it. §5. |
| V9 | Surfaces | result grid cells (all sizes) and the gallery filmstrip. The gallery large view is unchanged (it already plays through the playability ladder + transcode). Pinboards and the peek layer mount nothing. |
| V10 | Badge | unchanged: a video's cell keeps the play badge; it is already faded by `group-hover` while the pointer is on the card, so a playing preview never shows a glyph. |
| V11 | Feedback while a job is pending | the play badge STAYS VISIBLE while a preview job is pending (its `group-hover` fade suppressed for that cell) and its circular edge becomes a progress ring that fills with the job's progress; while queued, an indeterminate sweep, with a caption from the existing `transcodeBadge` formatter ("Transcoding…" / "Queued #2"). Both disappear when the video starts playing. Only the previewing cell subscribes to the job state (`useTranscodeState`), so no other cell re-renders. Rung 0 shows no ring: the poster holds until the first decoded frame. A sticky failure shows nothing and never retries in the session. |
| V12 | The frame swap (replaces D9 whenever previews are on) | With previews on for the cell (rung ≠ none): a SMALL cell (1x1 base) never swaps to the 2x2; the video fades in over the 1x1 after the dwell. A LARGE cell (2x2 base) swaps to the 1x1 frame in the same moment as the hover zoom-out, and the video fades in over that 1x1 when it plays — the 1x1 is the waiting placeholder. Because the dwell alone is 200 ms the video is never earlier than the swap, so hover is one change and playback one fade-in, never three. With previews OFF today's behaviour is unchanged. On leave everything reverts in one commit. |

## 1b. The trigger setting (amended 2026-09-04, after user QA)

Resting the pointer anywhere on a card was the wrong trigger for a *video*.
An animated image already has a thumbnail-like loop, so playing it costs
nothing visible and reverses instantly; it can behave like a hover highlight.
A video preview is a process with a name on screen, and a cursor rests on a
card for a dozen reasons that are not "show me this" — a corner button, a
pause in scrolling, an item the user is not even looking at. The trigger
must ask for the same intent a process deserves, and the target for that
intent already exists: the play badge.

| # | decision | value |
|---|---|---|
| T1 | The setting | a browser preference, `localStorage` key `panoptikon.hoverPreviewTrigger`, values `"card"` \| `"button"`, absent = `"button"` (the default). Same box/`useSyncExternalStore`/cross-tab pattern as the other preferences. Never in the URL, never in client-config: the server has no say in where a pointer must rest. Applies to the result grid and the gallery filmstrip alike (they share `VideoHoverPicture`). The large viewer is unchanged. |
| T2 | `"card"` | today's behaviour, V1–V12 unamended: the arm fires after 200 ms of real pointer rest anywhere on the card. |
| T3 | `"button"` — the arm | the play badge is the target. The existing 200 ms arm applies to the badge (real pointermove onto it, then rest), and is followed by a **countdown**: the badge's ring — the same ring V11 uses for job progress — fills over ~700 ms. When it completes, the preview starts exactly as the card arm would have (rung ladder, requests, job). A **click** on the badge starts it at once, countdown skipped. |
| T4 | Leaving the badge BEFORE the start | aborts: the countdown stops, the ring drains back, nothing was requested. The badge stays a badge. |
| T5 | Leaving the badge AFTER the start | changes nothing: from the start onward the preview is committed to the **card**, exactly as under `"card"`. Leaving the card is what cancels or stops it (V4). Rationale: the user just spent most of a second aiming at a small target, or clicked; cancelling because the pointer drifted a few pixels would be the UI changing its mind. Once the video plays the badge is gone and the two modes are indistinguishable. |
| T6 | Badge visibility under `"button"` | the badge is the target, so it must not fade on card hover (V10's `group-hover` fade is off for video cells in this mode). On card hover it gains a touch of contrast and a pointer cursor; it disappears at the first playing frame, as V11 already says. Under `"card"` the fade is unchanged. |
| T7 | The frame swap under `"button"` (amends V12) | a LARGE cell (2x2 base) keeps its 2x2 on card hover and gets today's zoom-out (`object-contain`) like any image card; it swaps to the 1x1 frame **when the badge arm fires** (the start of the countdown), so the swap is the first feedback that a preview is coming, and the video fades over that 1x1 when it plays. Aborting the countdown (T4) reverts the swap. A SMALL cell (1x1 base) stays 1x1 throughout — no 2x2 swap in either mode, as V12 already says. |
| T8 | The popover | a "Start on: Card / Play button" segmented setting beside "Video previews", laid out like its neighbours (name, one sentence, full-width control, one sentence under it). The sentence above "Video previews" follows the trigger: "Play a video by resting the pointer on its card." vs "Play a video by resting on its play button, or clicking it." |
| T9 | Mixed policy per rung | rejected: rung 0 costs no server work but the byte cap exists for a reason, and a user cannot predict which rung a file takes. One rule per setting. |
| T10 | Cost when idle | as before: nothing is subscribed or requested per cell until an arm fires. The badge's hover contrast is a stylesheet rule, not state. |

Implementation notes (ui `preview-trigger`, merged 2026-09-04): the
preference lives in `lib/state/hoverPreviewTrigger.ts`, the countdown is a
pure state machine in `lib/previewCountdown.ts` (`idle | counting | draining
| started`, 700 ms fill, 150 ms drain, `previewFrameShown` is the T7 swap
rule), and `hooks/usePreviewTriggerArm.ts` drives it through the director's
own `armHoverPlay` on the badge element — so the badge inherits the
real-pointermove-then-dwell discipline rather than re-earning it. Four
readings worth knowing: (1) after the start, the badge ignores every arm
loss including a fast-scroll suspend — only the card's leave cancels, as T5
words it; (2) T6's "disappears at the first frame" is achieved by the badge
ceasing to be interactive at the start, which restores the ordinary hover
fade while a pending job's ring keeps suppressing it (V11), with no
playing-state channel from the video; (3) the hit target is the badge SVG's
square box, with the overlay around it still `pointer-events: none`, and the
badge stays `aria-hidden` (the keyboard path is the card's link); (4) the
`previewTrigger` prop defaults to `"card"` on both card components, so
surfaces that pass no capability are byte-identical to before. Tests:
`scripts/previewtrigger.test.mjs`.

## 2. The ladder, and the measurement that produced it

The plan's rung 0 carried no size bound, on the reasoning that
`preload="none"` plus `play()` fetches the moov atom and the first seconds
only. **That is not what browsers do.** Measured in Edge 152 on
`long-playable.mp4` (15.7 MB, 30 s, 1280x720), cold cache, one three-second
hover over loopback:

```
GET /api/items/item/file?…   Range: bytes=0-     -> 206
  +100 ms   2,719,744 bytes received
  +200 ms   7,823,360
  +1.6 s    9,928,704      (video.buffered.end = 19 s of 30)
  leave     9,928,704, no further growth
```

**9.9 MB — 63 % of the file — for one hover.** Chromium issues a single
open-ended range and buffers as far ahead as it likes; there is no "first
seconds only". Cancellation is correct (bytes stop the instant the pointer
leaves), but the pointer crosses a card every 200 ms in a grid, and the
deployment this is built for reads off an SMB-mounted NAS.

So one byte cap governs everything, and the **client** picks the rung while
the server publishes the inputs:

1. `size <= max_bytes` and the codec/container is browser-playable →
   **rung 0**, the original file.
2. else, if the estimated 16 s slice — `size * min(16, duration) / duration`,
   and an unknown duration is *not* eligible — is `<= max_bytes` and the codec
   is browser-playable and mp4-muxable → **rung 1**, `preview-trim`.
3. else, when the policy and server offer it → **rung 2**, `preview`.
4. else nothing.

**The trim rung is reachable only for items longer than 16 s.** For an item of
16 s or less, `min(16, duration)` is the whole duration, so the estimate is the
whole file — which step 1 has already established is *over* the cap. Such an
item therefore falls straight through step 2 to the encode rung. This is not a
special case in the code; it is what the formula says, and it is the right
answer: there is no shorter slice of a short file to serve, so the only way to
get it under the cap is to re-encode it. (Confirmed by the verifier at a
1 MiB cap.)

The cap is a *byte* bound, not a pixel one: what the browser pulls is bytes,
and rungs 1 and 2 both cost the server a job, so the question at every step is
"how much traffic does this hover cost?".

## 3. The presets

Both live on `Surface::Preview`, so neither appears in a clip or mosaic
dropdown, and both take the 16 s window as a **trim bound the client sends**
(`end_cs = 1600`) rather than as a preset field — an item at or under 16 s is
previewed whole under the same cache key it would have had anyway.

| preset | container | video | audio | caps | cost |
|---|---|---|---|---|---|
| `preview-trim` | mp4 | `vcodec = "copy"` — the source's packets remuxed | none | none | I/O only: no decode, no encode |
| `preview` | mp4 | h264, CRF 26, Fast channel | none | `max_height 480`, `fps_max 30` | one short encode, hardware where there is one |

**Stream copy** (`presets::STREAM_COPY_VCODEC`, ffmpeg's own `copy` spelling)
is the first preset shape in the codebase that does not encode:

- `run::resolve_encoder` short-circuits to `ENCODER_COPY` **before** either
  hardware probe — a copy has no encoder to pick, and asking would spawn
  ffmpeg twice to answer a settled question. The identity is host-independent,
  so a `preview-trim` artifact keeps one cache key across a hardware flip that
  re-keys every real encode.
- `run::build_args` emits `-c:v copy -an -movflags +faststart` with no rate
  control, no `-vf`, no `-fpsmax`, and **no `-pix_fmt`** (packets keep the
  format they were encoded in; naming one asks a filter chain that does not
  exist to convert them). The existing input-side `-ss` and `-t` carry the
  trim unchanged.
- **The cut is keyframe-aligned by nature.** A slice may run a little past
  `end_cs`, to the end of the GOP that straddles it, and a source whose first
  keyframe is late starts late. Both are acceptable for a hover preview and
  neither is worth a decode to fix.
- A codec that will not mux into mp4 (mpeg2, vp8) simply **fails the job** the
  ordinary way, and the client falls to the encode rung.

Config validation keeps the shape coherent: a copy profile that names a
crf/bitrate, a `max_height`, an `fps_max`, audio, or an animated-image
container is refused at load. Values it merely *inherits* from a built-in are
dropped instead — there is no way to clear an inherited crf, so
`[transcode.profiles.playback] vcodec = "copy"` would otherwise be
unexpressible. Compositions refuse a copy preset by name: a filtergraph
produces frames, not source packets.

`ResolvedPreset::quality` became an `Option` for this. It re-keyed nothing —
`Some(q)` and a bare `q` serialize identically, and a test pins that — so
**`TRANSCODER_VERSION` is unchanged**: `preview-trim` opens new key space
rather than orphaning old.

## 4. Settings

| key | default | governs |
|---|---|---|
| `[transcode] hover_preview` | `"auto"` | rung 2 only. `"auto"` = on where `hw::fast_h264_encoder()` validated one; `"on"`/`"off"` decide without touching the toolchain. |
| `[transcode] hover_preview_max_bytes` | `16777216` (16 MiB) | the whole ladder; §2. Validated `> 0`. |

Both are tunable defaults, so both ship in all five `config/server/*.toml` as
**commented examples only** (CLAUDE.md: a live line freezes for existing
users). `hover_preview` is parsed by `hw::parse_hover_preview` and rejected at
config load like `hwaccel`; it resolves through `hw::resolve_hover_preview`,
which takes the probe as a closure so `"on"`/`"off"` never run it and the
matrix is unit-testable without a toolchain.

**Startup warm.** `main` schedules one fire-and-forget `spawn_blocking` that
runs the probe when `hw::hover_preview_probe_warm_needed` says the setting is
`"auto"`. Nothing awaits it and bind/listen is not delayed. Without it the
first `/api/client-config` of the session — a request on every UI mount —
would pay for the encoder listing plus a validation encode. The handler keeps
its own `spawn_blocking` resolve as the fallback; by then it reads a warm
`OnceLock`.

**Queue position.** Preview jobs are `JobWeight::Light` like every other
single-file job and take their ordinary FIFO place behind gallery playback
transcodes and clip exports — at the default `max_concurrent_jobs = 1` a
hovered cell waits for whatever is already encoding, and says so through the
queue position the pool already reports. A lane that let a skimmed pointer
jump ahead of an export the user is waiting on would be the wrong trade; the
16 s cap and the one-preview-at-a-time rule keep the queue short instead.

## 5. `/api/client-config`

```json
"hover_preview": { "direct": true, "trim": true, "transcode": true,
                   "max_bytes": 16777216 }
```

Always present; `null` (never a missing key) on a build without the feature,
so a client reading "no answer" cannot mistake it for "allowed".

- `direct` = the policy's `hover_preview` key is not `false`.
- `trim` = `direct` ∧ this policy may `POST /api/video/transcode` ∧
  `preview-trim` survives its `transcode_presets` limit. **Not** gated on the
  hardware probe or on `[transcode] hover_preview`: there is no encoder in
  this rung to have an opinion about.
- `transcode` = `direct` ∧ the server default ∧ the POST capability ∧
  `preview` survives the limit.
- `max_bytes` = the setting, verbatim; the client does arithmetic with it.

The preset checks reuse `allowed_presets` — the same resolution the POST
enforces — so client-config can never promise a rendition the route would
refuse by name.

## 6. Client notes worth recording

The store (`lib/videoTranscode.ts`) distinguishes a job it **created** from
one it **joined**, because only the former may be `DELETE`d: cancelling a
joined job would cancel someone else's work. `createdOwnJob` accepts only the
literal `"created"` outcome, and the in-flight path re-checks it before a late
DELETE. A **generation number** guards the window while a POST is in flight —
a cell that leaves before its response arrives must not have the arriving job
adopted by whatever the pointer landed on next.

`lib/state/hoverPreviewPref.ts` writes both preference slots on every click,
so "Originals" on a server that denies the encode rung persists
`{direct: true, transcode: false}`. That is deliberate for the control's
shape, but it means a user who later gains the encode rung keeps the stored
"no" — see the S2 note in the verification report.

## 7. Verification

Backend: preset rows pinned field by field (both, including the copy shape and
the fps-cap set); the `hover_preview` resolution matrix against a stubbed
probe, with the probe asserted to run *only* for `"auto"`; the startup warm
decision pinned against that same resolution; copy-preset config validation
(crf, bitrate, `max_height`, `fps_max`, audio, image container, and the
inherit-drops-instead-of-failing case); the copy argument vector (`-c:v copy`,
`-an`, faststart, `-t` from `end_cs`, and the absence of every encoder option
including `-pix_fmt`); client-config field shape at each gate; and two
real-ffmpeg integration tests through the POST handler — `preview` producing a
≤16 s, ≤480 px, silent mp4, and `preview-trim` producing an artifact whose
**video packet sizes equal the source's**, which is what makes "no encoder
ran" a measurement rather than an inference.

Browser matrix (Edge 152, CDP input, isolated stack on ports 6401/6402, the
user's 3000/6339/6342/6343 untouched), all **PASS**:

1. scroll with a stationary cursor → nothing plays, zero requests of any kind
   at both cell sizes; a synthetic `pointerenter` at unchanged coordinates
   arms nothing.
2. move-then-dwell on a playable item → `<video>` mounts on the original URL
   and plays muted — *and* pulls the 9.9 MB that produced §2.
3. needs-transcode → exactly one POST, `end_cs` absent for a 10 s item and
   `1600` for a 30 s one, badge ring + caption, artifact plays; re-hover is a
   cache hit with zero requests.
4. skim across ten items → 10 POSTs, 10 DELETEs, every job settling as
   cancelled; no pile-up. A *joined* job is never DELETEd.
5. leave mid-job → cancelled, and a re-hover resubmits (not sticky).
6. the V12 swap in all four quadrants (small/large × previews on/off), plus
   the filmstrip and the extreme-aspect card.
7. the three gates — default, `[policies.client] hover_preview = false`, and
   `[transcode] hover_preview = "off"` — each disabling exactly the segments
   it should, in behaviour and not only in chrome. Under `"off"` the gateway
   log shows no probe line at all.
8. scroll bench at the minimum cell size: p50/p90 identical and network
   byte-identical (110 requests / 2374 KB) with previews on and off, across
   three alternating runs. The machinery costs nothing until a dwell.

Not observable on this hardware: a *progressively filling* determinate ring
(the encodes finish in about a second, and server-side work cannot be
network-throttled), and the decode-downgrade path, which is unit-tested
instead.

## 8. The outro cut (2026-09-05)

Reported after QA: a TikTok with a detected end card previewed *with* the
card. All three rungs ignored `content_end_ms` — rung 0 looped the whole
original natively, and rungs 1/2 asked for `end_cs: 1600` (or nothing at all
for an item of 16 s or less), so a 12 s clip with an 8 s cut was remuxed or
re-encoded whole and that artifact was cached. Meanwhile the gallery player,
the pinboard, the clip route and the composition export all already ended at
the cut (`docs/video-outro-skip-design.md`).

**One rule for every rung.** A preview ends at the earlier of the 16 s window
and the item's detected outro cut, when the viewer's outro-skip preference
(`videoPlayerOutroSkip`, the same global the players follow, default on) is
on and the row carries a *usable* outro. Otherwise every rung behaves byte for
byte as before. The preference is read once per surface (`ResultGrid`, the
filmstrip) and handed down as a boolean like the capability, never
subscribed per card.

**What "usable" means, on the client.** `previewOutroCut` in
`lib/videoPreview.ts` is the client's half of the server's
`usable_outro_cut_cs`: no `content_end_ms`, a boundary at or past the item's
duration (no card to cut), or a cut inside the guard-and-freeze band all
answer null, and null names no cut. Mirrored exactly, not approximately: a
recorded duration of zero or less counts as a real duration the boundary is
past (the server's `is_some_and`, unlike `outroCutPoint`'s "zero is
unknown"), and the server's own floored centisecond —
`floor((content_end_ms − 60) / 10)`, where the player rounds — must clear
`validate_bounds`' freeze guard (more than 2 cs), so a boundary of 85..89 ms
names no cut even though the player's rounding would give one; 90 ms is the
first that does. Only the *verdict* is the server's; the seconds value
returned is still the player's rounded cut. It is start-anchored — at plan time
there is no element to anchor against — and is used for two things only:
whether to name the cut, and the copy rung's size estimate. The server
measures the real one.

**Rungs 1 and 2: the outro is named, not measured.** The request gains
`cut: "outro"` beside the window: `{ preset, end_cs: 1600, cut: "outro" }`
for an item over 16 s, `{ preset, cut: "outro" }` for one inside it (the
omission rule of §6 is untouched). `POST /api/video/transcode` used to
reject `cut` together with `end_cs`; it now takes **the earlier of the two**
(`api/video.rs`). The 404 for an unusable outro stands, cap or no cap — the
client pre-check above makes it a stale-row race rather than a routine.

This is the property the user asked for, stated precisely: **the artifact's
key is the resolved end, so it moves exactly when the cut moves, and only
for the items whose preview changes.** An item cached today under `e1600`
whose cut is 7.94 s resolves to `e794` on its next hover and is regenerated
without the card; an item scanned for outros after it was first previewed,
or one on a database where detection is switched on later, lands on a new
key the same way. An item whose outro lies past 16 s resolves to `e1600` —
the same bytes it always had — and keeps its artifact; regenerating it would
spend an encode to produce the same file. The client's store key carries the
cut as its own segment (`sha:preview-trim:e1600:outro`) so a capped request
and a capped-and-cut one are different slots before the server has answered,
and flipping the preference lands the next hover on a fresh slot.

The client's cut segment carries the row's `content_end_ms`, not the bare
literal: `sha:preview-trim:e1600:outro8005`, `sha:preview:outro8005`. The
wire body is unchanged (`cut: "outro"`, resolved by the server); the key
says which boundary the slot was minted for, because a `done` slot is never
forgotten this session while the server resolves the literal against the
row it holds *now*. A boundary that changes in-session (re-detection, a
re-search after the detector ran) therefore reaches the server as a fresh
request instead of replaying the old artifact from the client's slot — and
the segment can never equal the clip route's `sha:<preset>:outro`
(`clipStoreKey`), with which the preview shares one store on the promise
that the two namespaces never meet. Without a cut there is no segment at
all, so an uncut key is byte-identical to the one it had before this
section.

**The copy rung's estimate** shrinks to the cut:
`size × min(16, cut, duration) / duration`. A 20 MB twelve-second TikTok
with an 8 s cut was over the cap whole and went straight to the encoder;
two thirds of it fits, so it now takes the stream copy.

**Rung 0 loops in the browser at the cut**, with the gallery player's own
mechanism: `useVideoTrim` in loop mode (a playhead crossing check that seeks
back to the start), fed `outroCutPoint` with the row's boundary, its indexed
duration and the element's own — the player's midpoint anchoring, within
about a tenth of a second of the card (`PreviewCutVideo` in
`VideoHoverPicture.tsx`). The player's frame-exact rVFC end probe is
deliberately **not** used: it mounts a second offscreen `<video>` and seeks
it to the end of the file, i.e. a second Range request of the tail on every
hover, against the byte cap's whole purpose. The native `loop` attribute
stays on; when the cut does not apply (durations disagreeing by a second or
more, which `outroCutPoint` reads as two different files) native loop wraps
at zero, which is the start anyway. Only the previewing cell mounts the hook
and its two duration listeners (`loadedmetadata` and `durationchange`).

**The stream copy gets the same browser-side cut.** Measured while
implementing: ffmpeg's output-side `-t` on a `-c:v copy` of a B-frame source
kept **two frames past the bound** (`-t 7.94` at 30 fps gave 241 frames,
8.03 s) — packet reordering, not the GOP-end overrun `run.rs` guesses at.
That is a frame or two of end card at every loop seam, so the copy
artifact's element gets the plan's start-anchored cut unrefined. The copy's
raw packet timestamps are the source's, but it is written with
`-avoid_negative_ts make_zero` and its edit list no longer compensates the
B-frame CTS offset, so on an engine that honours edit lists its frames
present about one reorder delay *later* than the source's (~66 ms at 30 fps
with three B-frames): the client's cut fires up to that much *early* — into
content, never into the card — which is the safe direction and why the
unrefined cut is acceptable. The artifact's own duration is no anchor — it
is the artifact's, not the file's. The re-encode rung is cut on the
frame and gets no browser-side cut. A copy of a long item whose cut lies past
the window has its cut beyond the artifact's end, where it never fires.

**Tests.** Server: the handler accepts the pair (`parse_cut` no longer sees
`end_cs`); through the handler, a cap
past the outro resolves to the outro and a cap inside it to the cap, each
under the key the bare bound would mint; an unusable outro is a 404 with or
without a cap; the openapi fixture regenerated. Client
(`scripts/videopreview.test.mjs`, `scripts/transcode.test.mjs`): the
pre-check against each of the server's refusals, request shape by preference
× duration × boundary, key segments and their distinctness, the shortened
slice, the ladder opening the copy rung through the cut, and that a host
passing nothing plans the ladder it always did.
