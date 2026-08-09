# Automatic outro skip at playback

Status: designed 2026-08-08, settled with the user over one design round;
implemented, then revised 2026-08-09 after the user validated it on real
TikToks (§1 end-anchored cut + 60 ms guard, §3 purpose-built glyph, §4
click-vs-drag contract — the three defects that round found).
Companion to [`video-outro-detection-design.md`](video-outro-detection-design.md)
(which produces the metadata this feature consumes) and
[`video-player-ui-design.md`](video-player-ui-design.md) (the player surface it
lives in). This is the "main feature" the detector was built for: TikTok
videos with a detected end card play as if the card were not there.

## 1. Core model: a default for the end bound

Outro skip is **not a second trim** and never becomes a user trim by itself.
It is a *default value for the trim end bound*, applied at playback time only:

```
effectiveStart = userTrim.start                      (outro skip never touches start)
effectiveEnd   = userTrim.end                        if the user set an end bound
               = cutPoint                            else, if skip is on and the item is eligible
               = null                                otherwise
```

- **`cutPoint` is END-anchored.** The card is appended at the *end* of the
  file, and the browser's timeline routinely disagrees with ffprobe's about
  where zero is (mp4 edit lists, audio priming ≈ 0.05–0.15 s). Anchoring the
  cut to the start converts that disagreement into cut error — field
  validation on real TikToks measured the cut at 11.14 s against a card that
  actually began at 11.40 s, 260 ms early. The card's *length* is the same
  number in either origin, so it is what travels between the two timelines:

  ```
  card     = serverDuration − content_end_ms/1000     (both ffprobe seconds)
  cutPoint = browserDuration − card − guard           (both browser seconds)
  ```

  `serverDuration` is the item's indexed `duration` field; `browserDuration`
  is the `<video>` element's own, known only from `loadedmetadata` onward.
  Rounded to centiseconds (the storage resolution of the trim codec).
- **Fallback**: with either duration unusable — null, NaN, *or zero or
  negative* (a stored duration of 0 is as unknown as none at all) —
  `cutPoint = content_end_ms/1000 − guard`, the start-anchored original. It
  is not an error path: the browser's duration arrives with the metadata, so
  the fallback is what the first moments of every playback use, and the cut
  *refines* when the metadata lands. That is safe because `useVideoTrim`
  re-runs its loop effect on an end change without yanking the playhead. The
  toggle button's existence *can* differ between the two paths when the two
  eligibility thresholds land on opposite sides of the cut — reachable only
  with a `content_end_ms` in the low hundreds of ms (a video barely longer
  than its card) plus an origin shift — and when it happens the state is
  internally consistent: button, cyan marker, tail dimming and the effective
  end all appear or vanish together.
- **Card sanity guard**: `card ≤ 0` (content ends at or past the file end)
  makes the item ineligible outright — not a fallback, since the numbers are
  already known to disagree. The other degenerate direction (a card as long
  as the whole browser timeline) needs no guard of its own: it computes a
  cut at or below `−guard`, which the eligibility floor already rejects.
- The guard is **60 ms**. It covers the card's audio bang, which leads the
  first card frame by up to 60 ms (detection design §2.3/§10). It was 150 ms
  in the first implementation and the field round cut it: 150 ms costs 4–5
  visible frames on *every* video to cover a lead whose median is 10 ms, and
  with the systematic offset now handled by the end anchor and a per-video
  escape hatch in the drag/step workflow (§4), there is nothing left for the
  extra 90 ms to absorb. Accepted trade-off: on a maximum-lead file about one
  rAF tick of bang transient can leak.
- **Eligibility**: item has `content_end_ms` non-null AND
  `cutPoint > FREEZE_EPS` (0.02 s, `lib/videoTrim.ts`). A cut point inside
  the freeze band means the item is treated as having no outro: `{start:
  null, end: 0.01}` is not a short video, it is `useVideoTrim`'s freeze
  branch showing frame 1 and pausing, with no user trim anywhere in sight.
- The effective range feeds the existing `useVideoTrim` hook **unchanged** —
  `{start: null, end}` already loops back to 0 on crossing the end, and
  `isEmptyTrim(effective)` (not the user trim) must govern the native `loop`
  attribute at both hosts.
- A user end bound wins even when it lies *past* the cut point: someone who
  deliberately trims into the outro is respected. There is no `min()`
  composition.
- A user trim with only a start point still skips the outro (start is
  orthogonal to the end default). This is deliberate: placing a loop start
  must not silently resurrect the end card.
- **Degenerate-range guard**: the outro default applies only when
  `cutPoint − effectiveStart > FREEZE_EPS` — the exact complement of
  `useVideoTrim`'s freeze test (`end − start ≤ FREEZE_EPS`), *not*
  `cutPoint > effectiveStart`. A user start at, past, or within one freeze
  band before the cut point suppresses the default for that video (plays out
  to the file end); a `start < end` range that is merely a centisecond wide
  would still hit the freeze branch and pause the video, which is never what
  a start-only trim means. Same threshold as the eligibility rule above,
  which is this predicate at `effectiveStart = 0`. User-set ends are exempt
  (they own the range; `trimWithBound` already keeps user bounds consistent
  with each other).

Because the outro end is a default and never *becomes* a user trim, every
state question dissolves: nothing is written to the URL (`vt`) or the
pinboard h-field, there is no "modified outro trim" state, and the toggle
button has exactly one meaning at all times (§3).

## 2. Where it applies

Both video players: the gallery (`ImageGallery.tsx`) and pinboard pins
(`GalleryPinBoard.tsx`), via their shared `VideoPlayerSurface`. The
composition is one shared helper (in `lib/videoTrim.ts`) called by both
hosts; hosts pass the outro cut point down to the surface for rendering.

Pin creation from the gallery does **not** stamp the outro end into the new
pin's h-field. The "trim flows with the act of creating a pin" bridge applies
to real user trims (`vt`) only; the pin's player applies the skip dynamically
from the item's own metadata.

## 3. The toggle button

- **Existence**: rendered only on eligible videos (§1). On any other video
  the button does not exist — no disabled ghost.
- **Meaning**: it is the browser-level preference, nothing else. One global
  setting, **default ON**, persisted in localStorage (follow the existing
  `persistVolume` pattern in the player state module), **shared between
  gallery and pinboard**. Toggling it on one video toggles it for all —
  it is a viewer preference like volume, not per-video or per-board state.
- **Placement**: in the S1 flat control row, right-hand group, immediately
  left of the trim button (it is trim-adjacent in meaning). It follows the
  same size-ladder rules as the trim button (absent at the mini tier).
- **Glyph**: a purpose-built inline SVG (`OutroSkipIcon`, next to its use
  site in `VideoPlayerSurface.tsx`), not a stock icon. It draws the feature —
  a solid rail, a tall vertical cut mark, and behind it a faded two-dash tail
  standing for the removed end card. The first implementation used lucide's
  `SkipForward` and field validation rejected it: an arrow-to-a-bar reads as
  an ordinary next-track button, which is a different verb. It follows
  lucide's drawing contract (24-unit box, `currentColor`, 2 px round-capped
  strokes, no fill) so it sits among lucide siblings as one of them, and is
  held to four strokes so it survives the 20 px it renders at.
- **Appearance**: glowing light blue when active (skip on and governing).
  Distinct hue from the user-trim blue-400 accent — use the same cyan as the
  outro end marker (§4). Hover shows an explanatory bubble in the existing
  black/80 style: "TikTok end card detected — skipped during playback.
  Click to disable."
- **Inert state**: while skip is on *and* a user *end* bound is set, the end
  override wins and the button renders dimmed, with the bubble explaining
  "Manual trim end overrides outro skip." It stays clickable — clicking still
  flips the global preference (harmless, takes effect wherever no end
  override exists). A user start bound alone does not dim it. The off state
  always wins the presentation: skip off shows the unlit off-copy even when a
  user end exists (dimming there would hide that the feature is off). Four
  bubble states total: governing, off, overridden-by-end, suppressed by the
  degenerate-start guard.
- Turning the toggle off simply removes the default: full video, native-style
  loop at the file end (via `isEmptyTrim` on the now-empty effective range).

## 4. Timeline (rail) rendering

The rail shows the *effective* range, with the end marker's color declaring
who owns it:

- Outro-derived end in effect → a **cyan end marker** at the cut point
  (distinct from the user-trim blue-400), and the rail segment beyond the cut
  is dimmed (e.g. white/20 vs the normal white/40) to read as "skipped".
  No band recolor; the trim band itself keeps its existing look, spanning the
  effective range (relevant when a user start is set: blue start marker, band
  to the cyan end marker).
- User-set end → the existing blue marker and handles, exactly as today.
  The skipped-tail dimming disappears (the user's range governs).

### Click vs. drag on a marker

Every marker on the rail — user start, user end, cyan outro — obeys one
gesture contract, decided by whether the gesture ever **moved** a value
(tracked as a `moved` flag in the drag state). The click/drag boundary is a
**3 px horizontal deadzone** (`DIRECTION_DEADZONE`, the same one a coincident
"pending" grab uses for direction resolution): a 1 px slip between press and
release is routine on a trackpad, and without the deadzone it would flip an
inspection click into its qualitative opposite — a committed bound, a rewind,
a history entry and a resume. Zero-delta and purely vertical pointermoves
fall out of the same test; once a gesture is a drag it tracks the pointer
freely, back through the grab position included.

- **Click** (release without movement, any marker, a still-pending grab
  included): **seek the playhead to the marker's value and stay paused.**
  Commits nothing, writes no history entry, and — unlike every other release
  path — does *not* resume even if the video was playing when grabbed
  (pointerdown already paused it; a playhead that immediately runs away is
  not parked). This is the *inspection* gesture, and it is what makes
  stepwise refinement possible: **click the cyan marker → frame-step with the
  popover's −1f/+1f or the gallery's `,`/`.` keys → commit with Set end or
  `O`.** Without it there was no way to park the playhead at the detected
  endpoint at all: the old contract committed on click and then rewound to
  the loop start.
- **Drag** (moved): **seed-on-drag**, resolved at release. From the first
  move the gesture owns the end — the cyan marker turns blue, the trim band
  spans it, the skipped-tail dimming stands down — and the release writes
  exactly **one** `onTrimChange`, at the position it ended on, populating
  `vt` (gallery) / the h-field (pinboard). The toggle button then dims per
  §3. End commits rewind to the loop start as "here's your loop" feedback,
  and the gesture resumes playback if it had paused it.

The seed lives in the **drag state**, never in the caller's trim: seeding on
pointerdown *and* committing on pointerup would push two history entries for
one gesture, against the one-entry-per-trim-edit rule of the `vt` param. So
the outro default becomes user-owned only by an actual drag of that specific
marker, or through the ordinary Set-end/`O` verb at a stepped playhead —
never by merely touching it.

Because the seed is uncommitted until release, a chorded second pointerdown
mid-gesture must be ignored (the stood-down outro marker leaves no end value
to reseed from), and a cancelled gesture (pointercancel / lost capture) must
clear the drag state without committing, resuming if it had paused. That
abort path also has to be **fenced off from a normal release**:
`lostpointercapture` fires right after `pointerup`, and re-running the abort
there would resume the video a deliberate click had just parked — so a
release raises a synchronous flag (a ref, not state, which React has not
committed yet) that the abort checks, lowered again at the next pointerdown.

Parking the playhead exactly *at* the end point is safe: `useVideoTrim`
clamps no seeks, its crossing test requires `!paused`, and its freeze branch
only fires when a freeze-width range *plays*. A later play() from there runs
out to the file end and wraps to the loop start — the hook's documented seek
semantics, unchanged.

### Boundaries that keep it unambiguous

- The trim popover reflects **user bounds only**. "Set end" never shows the
  outro value as an active bound; its ×-badge / shift-click-clear semantics
  are unchanged.
- No new keyboard shortcuts. Gallery `I`/`O` (set bounds) and shift-variants
  (clear) are untouched.

## 5. Clearing

Clearing a user bound removes *the user's override*; whatever default is
underneath resurfaces:

- Clear the end bound with skip on → the end falls back to the outro default:
  the marker snaps back to the cyan cut-point position and the toggle button
  un-dims/relights. The video still ends early — the visual transition is the
  explanation, so it must be legible.
- Clear with skip off (or on a normal video) → no end bound, full video.
- Full clear (both bounds) → start disappears entirely, end falls back as
  above.

"Watch the full video including the card" is the toggle's job, not clear's:
making clear mean "full video" would make the default reachable only by
toggling the preference off and on, and would make clear behave differently
on TikToks vs every other video.

In storage terms clear is unchanged: it removes the bound from `vt` / the
h-field; the outro default returns dynamically and is never written anywhere.

## 6. Feature-off gating is server-side

Requirement: when `detect_outros` is off, the entire playback feature is off —
even for outros already detected, the information is not used.

The UI must not learn the toggle from `/api/jobs/config` (an admin/jobs
surface; capability-scoped clients may lack it, and it is per-DB plumbing the
player has no other need for). Instead, the API stops serving the metadata:

- **When the request's index DB has `detect_outros` off, the API serves
  `content_end_ms` and `outro_kind` as null/absent** at both serving sites:
  - the item metadata endpoint (`api/items.rs`, `ItemRecordResponse`), and
  - search result rows (`api/search.rs` result mapping).
- The UI rule then collapses to the existence rule it already has (§3):
  button exists iff `content_end_ms` arrived. Automatically correct per-DB,
  for every client, with zero config plumbing in the player. This extends the
  detection design's §8 contract ("off ⇒ consumers ignore the metadata") to
  the API as a consumer.
- Config is resolved per-request through `SystemConfigStore::from_env()`, but
  the request pays for a stat, not a parse: the `detect_outros` bit is cached
  per index database against the stamp (modification time and length) of its
  `config.toml`, and re-read only when that stamp moves. A config save
  rewrites the file, so the next request after it sees a new stamp and
  re-reads — a change still takes effect immediately, without a TOML parse on
  every search. The read never creates the file (a GET must not write to the
  data folder) and fails open: an unreadable or malformed config logs a
  warning and serves the metadata rather than failing the request. That
  fail-open verdict is cached against the same stamp, so a broken file costs
  one read and one warning per revision of it, not per request, and still
  recovers on the next request after it is fixed. The one case that cannot be
  cached is a config whose modification time is unreadable — there is no stamp
  to expire against, so it degrades to a read per request and says so once.
- The nulling must be applied **after any result-cache read, at response
  mapping time**, so a config change takes effect immediately and cached rows
  never leak stale policy.
- **Deliberate asymmetry**: PQL *predicates* (`match` filters, `order_by`) on
  the outro columns stay functional with the toggle off. Querying is a query
  capability, not playback; gating the PQL builder is invasive and the
  toggle-off population deliberately opted out of a playback feature, not of
  their own data. Document the asymmetry in the API doc comments.

## 7. Data plumbing (UI)

- The gallery's search select does not currently request `content_end_ms` or
  `duration` — add **both** to both select lists in
  `lib/state/searchQuery/searchQuery.ts` (~lines 438–446 and ~583), in the
  identical position in each: the two request bodies have to hash the same
  for the sidebar-to-main swap to hit the result cache. `outro_kind` is not
  needed by the player ("has an outro" = `content_end_ms` non-null).
- Pinboard pins already receive both: their item fetch returns
  `ItemRecordResponse`, which always carries them.
- The **browser** duration comes from the `<video>` element and is owned by
  the *host*, not the rail: `useVideoDuration(videoRef, showVideo)` in
  `lib/videoTrim.ts`, whose value feeds `outroCutPoint` and is then handed
  down through `VideoPlayerSurface` to `VideoRail` as a prop (the rail's own
  `loadedmetadata`/`durationchange` listener was removed in the same move).
  One listener, one number — the rail's geometry and the cut point drawn on
  it must never come from two different answers. It is gated on `showVideo`
  because the element is created and destroyed under an unchanged ref
  identity, and it takes the item's **sha as a reset key**: it resets to
  `NaN` when inactive *or when the key changes*, so a departing item can
  never lend its duration to the arriving one. The key matters on the
  pinboard specifically — a pin swaps `src` in place under an unchanged ref
  and unchanged `showVideo` when the board reflows, and the load algorithm
  fires no `durationchange` when it empties the element, so nothing else
  bridges that gap.
- **Hand-built current-item payloads must carry both fields.** Three sites
  construct a `SearchResult`-shaped current-item object by hand rather than
  passing a search row through, and each must copy `content_end_ms` *and*
  `duration` or the feature silently disappears (or silently loses its end
  anchor) for that selection path: the pin corner select button
  (`SelectButton.tsx`), the pinboard double-click verb
  (`GalleryPinBoard.selectAsCurrentItem`), and the similarity-target open
  path (`itemSimilarity/similarityTarget.tsx`). Any future builder of that
  shape inherits the same requirement.
- Regenerate `ui/lib/panoptikon.d.ts` when the API's field *descriptions*
  change, not just the shape — openapi-typescript emits descriptions as
  JSDoc, so a comment-only server change still desyncs the file (it did for
  the Step 1 gating notes).

## 8. Out of scope

- Per-video or per-board skip overrides (the preference is global).
- Seeding the trim popover or `I`/`O` keys from the outro value.
- Multi-segment trims (see player design doc — deferred there too).
- Any change to detection, scan-side clamping, or stored metadata.
- Audio fade at the cut (hard cut, consistent with the loop mechanism).

## 9. Implementation plan

Two steps, each with its own adversarial verification round, plus the
field-feedback round that followed them (Step 3).

**Step 1 — server gating** (main repo, `master`):
`detect_outros`-off nulling at both serving sites (§6), config read
per-request, post-cache. Tests: the existing outro serving tests keep passing
(default is on); new tests prove both sites null the fields when off and that
PQL predicates still work. If API doc comments change, regenerate
`openapi.json` and run `cargo test -p panoptikon openapi` (targeted runs miss
the fixture test).

**Step 2 — UI** (ui repo, `rust-ui`): effective-trim helper + composition at
both hosts (`useVideoTrim` untouched; `loop` attr from effective emptiness),
localStorage preference, toggle button with active/dimmed states and bubbles,
cyan outro marker + skipped-tail dimming + seed-on-drag, select-list
additions. `tsc`, existing test scripts, and `next build` must stay green.

**Step 3 — field-feedback round** (ui repo, `rust-ui`, one commit): the
end-anchored cut point and its duration plumbing (§1, §7), the 60 ms guard,
the purpose-built toggle glyph (§3), and the click-vs-drag marker contract
(§4). Same verification bar as Step 2, with `scripts/outroskip.test.mjs`
rewritten for the new signature (§10).

Known repo state caution: `ui` has an unrelated uncommitted change
(`components/gallery/PinboardMenu.tsx`) that must not be swept into commits.

## 10. Testing notes

- Composition helper is pure — unit-test the table in §1 (user end past cut,
  start-only trim, ineligible item, toggle off) plus the cut point itself:
  end-anchored vs fallback (every way of not knowing a duration must still
  produce the fallback, never null), the card sanity guards, the 60 ms guard
  as an exact value, centisecond rounding, and the unchanged eligibility
  floor. `scripts/outroskip.test.mjs` (`npm run test:outroskip`), which is
  written to fail on a wrong anchor, a dropped fallback or a changed guard —
  including the field case as a fixture (11.29 s content end, 12.29 s server
  duration, 12.40 s browser duration ⇒ 11.34 s, 110 ms further out than the
  fallback).
- Gestures (manual — the rail's pointer code has no harness): on **each** of
  the cyan outro marker, a user end marker, a user start marker and a
  coincident (pending) stack, a click must seek the playhead there, commit
  nothing, and leave the video paused even when it was playing; and a drag
  of each must produce exactly **one** `onTrimChange` at the released
  position, with an end commit rewinding to the loop start and resuming if
  it had paused. A chorded second pointerdown mid-gesture changes nothing; a
  pointercancel mid-drag commits nothing and resumes; the
  `lostpointercapture` that follows a normal pointerup must be inert (in
  particular it must not resume after a click).
- Degenerate-range guard (§1): a user start at, past, or within `FREEZE_EPS`
  before the cut point must suppress the outro default (assert that whenever
  composition applies the default the composed range satisfies
  `end − start > FREEZE_EPS`, not merely `start < end`). The rail should then
  show no cyan marker for that video while that start bound exists.
- Server gating: config off ⇒ item endpoint and search select both serve
  null; cache warm/cold behavior identical.
