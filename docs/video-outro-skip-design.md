# Automatic outro skip at playback

Status: designed 2026-08-08, settled with the user over one design round.
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

- `cutPoint = (content_end_ms − 150) / 1000` seconds, rounded to centiseconds
  (the storage resolution of the trim codec). The −150 ms guard covers the
  card's audio bang, which leads the first card frame by up to 60 ms
  (detection design §2.3/§10).
- **Eligibility**: item has `content_end_ms` non-null AND `cutPoint > 0`.
  A degenerate cut point (≤ 0) means the item is treated as having no outro.
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
  `cutPoint > effectiveStart`. A user start placed at or past the cut point
  suppresses the default for that video (plays out to the file end) — without
  this, the composed `start ≥ end` range would hit `useVideoTrim`'s freeze
  branch and pause the video, which is never what a start-only trim means.
  User-set ends are exempt (they own the range; `trimWithBound` already
  keeps user bounds consistent with each other).

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
- **Appearance**: glowing light blue when active (skip on and governing).
  Distinct hue from the user-trim blue-400 accent — use the same cyan as the
  outro end marker (§4). Hover shows an explanatory bubble in the existing
  black/80 style: "TikTok end card detected — skipped during playback.
  Click to disable."
- **Inert state**: while a user *end* bound is set, the end override wins and
  the button renders dimmed, with the bubble explaining "Manual trim end
  overrides outro skip." It stays clickable — clicking still flips the global
  preference (harmless, takes effect wherever no end override exists). A user
  start bound alone does not dim it.
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

### Drag-to-adjust (seed on grab)

The cyan outro marker is **draggable**. Grabbing it seeds a user end bound at
its current position (the cut point) and from that instant it is an ordinary
user-trim edit: the marker turns blue mid-drag, the normal end-marker gesture
continues, and commit populates `vt` (gallery) / the h-field (pinboard). The
toggle button dims per §3. This gives "adjust the detected endpoint" without
any conversion ambiguity — the default becomes user-owned only on a physical
grab of that specific marker.

After the grab, frame-precision adjustment uses the existing affordances
(popover frame-step, gallery `,`/`.` keys) since the end is now a real user
bound. No outro-specific nudge controls.

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
- Config is read per-request via the existing `SystemConfigStore::from_env()`
  pattern (as the jobs handlers do). The nulling must be applied **after any
  result-cache read, at response mapping time**, so a config change takes
  effect immediately and cached rows never leak stale policy.
- **Deliberate asymmetry**: PQL *predicates* (`match` filters, `order_by`) on
  the outro columns stay functional with the toggle off. Querying is a query
  capability, not playback; gating the PQL builder is invasive and the
  toggle-off population deliberately opted out of a playback feature, not of
  their own data. Document the asymmetry in the API doc comments.

## 7. Data plumbing (UI)

- The gallery's search select does not currently request `content_end_ms` —
  add it to both select lists in `lib/state/searchQuery/searchQuery.ts`
  (~lines 438–446 and ~583). `outro_kind` is not needed by the player
  ("has an outro" = `content_end_ms` non-null).
- Pinboard pins already receive it: their item fetch returns
  `ItemRecordResponse`, which always carries the field.
- Regenerate `ui/lib/panoptikon.d.ts` only if the API schema shape changes
  (the gating changes served values, not the schema, so likely not).

## 8. Out of scope

- Per-video or per-board skip overrides (the preference is global).
- Seeding the trim popover or `I`/`O` keys from the outro value.
- Multi-segment trims (see player design doc — deferred there too).
- Any change to detection, scan-side clamping, or stored metadata.
- Audio fade at the cut (hard cut, consistent with the loop mechanism).

## 9. Implementation plan

Two steps, each with its own adversarial verification round.

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
cyan outro marker + skipped-tail dimming + seed-on-grab drag, select-list
additions. `tsc`, existing test scripts, and `next build` must stay green.

Known repo state caution: `ui` has an unrelated uncommitted change
(`components/gallery/PinboardMenu.tsx`) that must not be swept into commits.

## 10. Testing notes

- Composition helper is pure — unit-test the table in §1 (user end past cut,
  start-only trim, ineligible item, toggle off).
- Seed-on-grab: grabbing the cyan marker must produce exactly one user end
  bound at the cut point before any movement is applied (a click-without-move
  on the marker = end bound set at the cut point, board/URL populated).
- Degenerate-range guard (§1): a user start at or past the cut point must
  suppress the outro default (assert the composed range never has
  `start ≥ end` from composition alone). The rail should then show no cyan
  marker for that video while that start bound exists.
- Server gating: config off ⇒ item endpoint and search select both serve
  null; cache warm/cold behavior identical.
