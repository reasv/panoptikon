# Gallery video end action (loop / play once / auto-advance)

Status: designed 2026-08-09, not yet reviewed or implemented.
Companion to [`video-player-ui-design.md`](video-player-ui-design.md) (the
player surface the toggle lives in) and
[`video-outro-skip-design.md`](video-outro-skip-design.md) (whose effective
end bound is also this feature's end-of-playback signal).

A gallery-only, three-state preference for what happens when a video's
playback reaches its end — where "its end" is whatever already governs
playback today: the user trim end, the outro cut, or the file's natural end.

- **`loop`** (default): today's behavior, unchanged. The video loops —
  natively when the effective trim is empty, via `useVideoTrim`'s wrap
  otherwise.
- **`stop`** (play once): playback pauses at the end. Nothing advances.
- **`advance`**: the gallery moves to the next *video* in the search results,
  skipping non-videos, continuing across at most one page boundary at a time,
  and stopping when there is nothing left to play.

Explicitly rejected: a "loop the current page's videos" mode. No use case
found — the point of `advance` is to play through *search results*, and the
point of `loop` is to hold one video; a page is not a unit anyone asked to
hold.

The pinboard is deliberately untouched: pins are an arrangement, not a
sequence, so "advance" has no meaning there, and a board of parallel players
has nothing to do with play-once semantics either. The preference, the
button and the mechanism are gallery-only.

## 1. The preference

`GalleryEndAction = "loop" | "stop" | "advance"`, browser-level like the
outro-skip preference, persisted in localStorage (key
`galleryVideoEndAction`), default `loop`, any unrecognized stored value reads
as `loop`. Implemented with the same module-store +
`useSyncExternalStore` pattern as `useOutroSkipEnabled` in
`lib/videoPlayerState.ts` (SSR-safe snapshot, cross-tab `storage` sync for
free). Not a URL param: it is a viewer preference like volume, not a property
of the search or the item, and it must survive sessions.

## 2. End-of-playback mechanism

Every end-of-playback already funnels through exactly two places, and the
mode only has to touch those:

- **The native `loop` attribute** (gallery `<video>`): today
  `loop={isEmptyTrim(effectiveTrim)}`. Becomes
  `loop={mode === "loop" && isEmptyTrim(effectiveTrim)}` — in `stop` and
  `advance` the element must be allowed to fire `ended`.
- **`useVideoTrim`** (`lib/videoTrim.ts`) grows two options, defaulted so
  every existing caller (the pinboard) is byte-for-byte unchanged:

  ```
  loopAtEnd?: boolean        // default true = today's wrap semantics
  onEndReached?: () => void  // fired once per playback when the end is reached
  ```

  With `loopAtEnd: false`:
  - **Crossing the trim end** (the rAF/timeupdate checker): instead of
    `jumpToStart()`, snap `currentTime` to the end bound (park exactly at the
    marker — the same visual language as the click-park inspection gesture),
    `pause()`, then fire `onEndReached`.
  - **Native `ended`**: instead of wrap-and-play, leave the element where the
    browser parked it and fire `onEndReached`. This path must bind even when
    the trim is empty — the hook's current early-return on
    `start == null && end == null` only applies when `loopAtEnd` is true.
  - `onEndReached` is read through a latest-ref inside the hook so callback
    identity never rebinds the listeners; effect deps gain only `loopAtEnd`.

  Gallery wiring: `loop` → `loopAtEnd: true`; `stop` → `false`, no callback;
  `advance` → `false`, callback = the advance step (§3).

Details that keep the semantics coherent:

- **Once-per-playback guard**: a `fired` ref, set when either end path runs,
  cleared on the element's `play` event (and on effect rebind). Covers the
  corner where an end bound coincides with the file end and the crossing
  check and `ended` both fire in the same tick — two `advance` calls in one
  tick would still batch into one nuqs write, but the guard makes the
  contract explicit rather than accidental.
- **Replay after a natural end**: `play()` on an ended element seeks to 0
  per spec, which ignores a user trim start. In non-loop modes the `play`
  handler checks `video.ended` and jumps to the trim start first. Only
  `ended` gets this — a playhead *parked at* the trim end (stop-mode park,
  or the outro click-park gesture) deliberately keeps today's documented
  semantics: play from there runs out through the tail to the natural end
  (where the mode then acts). That preserves the outro-skip design's
  inspection workflow (park at the cut, play through the transition).
- **Freeze-frame trims** (`start === end`) never play, so they never end: in
  `advance` mode a freeze-frame video stalls the chain, by design — the user
  pinned that frame deliberately, and a dwell-timer would be a fourth mode
  hiding inside the third.
- **Mode changes mid-playback** just rebind the loop effect (same
  no-playhead-yank property the outro cut's refinement relies on). Switching
  `stop` → `advance` while parked at an end does not retroactively advance:
  both end events have already passed; the user's next `play()` runs the tail
  and the new mode acts at the natural end.

## 3. The advance step

Owned by `ImageGallery` (the component that has `items`, `page`,
`totalPages`, `setPage`), passed down to `GalleryImageLarge` as a callback,
wired as `onEndReached` only when the mode is `advance`.

The video predicate is the player's own: `type === "video/mp4" ||
type === "video/webm"` — hoisted from `GalleryImageLarge`'s inline
`isPlayable` into a shared helper so the two can never drift. Everything the
gallery cannot play (images, animations, unsupported video containers) is
"not a video" for this feature; that is precisely what makes the chain safe
to run unattended.

**Within the page**: scan `items` forward from `index + 1`; first playable
item wins → `setIndex(i, { history: "replace" })` (§4). The element is keyed
by sha, `showVideo` survives navigation, and `autoPlay` starts it — the
existing arrow-key navigation path, unchanged. The whole step no-ops while
`resultsAreStale`: an index chosen against rows the URL no longer names is
the exact mistake the `heldIndex` machinery exists to prevent.

**Page boundary**: no playable item in the rest of the page:

- `page >= totalPages` → do nothing. The last video stays parked at its end.
  ("Reaching the last video on the last page ends playback.")
- Otherwise → **fetch, then flip**. The URL must not move to a page whose
  rows are not yet in hand: flipping first would resolve the old (or a held)
  index against rows that arrive later, showing a wrong item for the whole
  fetch. `setPagePrefetch` already states the principle for the manual path
  ("warm the cache… so the flip and the results swap land in the same
  render"); the auto-turn extends it one step — the *landing index* is also
  chosen from the fetched rows before anything is written:

  1. Get the next page's rows under the exact key the live query will use —
     cache first (`getQueryData`; the prefetch below makes this the common
     case), else a `useFetchPageRows()` sibling of `usePrefetchPageState` in
     `lib/searchHooks.ts` (same `useSearchRequestFor` key identity,
     `fetchQuery` instead of `prefetchQuery` so the rows come back to the
     caller). A fetch failure ends the chain: the video stays parked,
     nothing is written. Serving possibly-stale cached rows is exactly as
     safe as every cached page-turn already is: the live query
     background-refetches, and the selection→index remap repositions if a
     row moved.
  2. Scan the rows: first playable at `k` → target `gi = k`; no playable →
     the chain is over — target `gi = 0` in windowed mode (the page still
     turns; the user lands at the top of the page that ended the session,
     never two pages out), **nothing at all in fullscreen** (see
     "Fullscreen continuity" below).
  3. Write `page` and `gi` **in one tick, through the raw setters, each with
     an explicit `{ history: "replace" }`** (and clear the grid scroll
     anchor the same way) — the `useCommitPageSize` write pattern, and for
     the same reasons: nuqs coalesces same-tick writes into one URL update
     but escalates the whole batch to `push` if any member asks for it, so
     the batch must not go through `useSearchPage`'s wrapper, whose internal
     default-push `setGi(0)` would both escalate the batch and clobber the
     target index.

  During the await the gallery simply keeps showing the ended video, parked
  and paused; after the flip, the one-commit throttle window is covered by
  the existing `heldIndex` hold. The first commit of the new page therefore
  already has `gi` on the first video (which autoplays, continuing the
  chain) — no intermediate item is ever displayed.

**Prefetch ahead of the turn**: the end-of-video fetch should be a cache
hit, not a network round-trip — a NAS-speed fetch between the last frame
and the next video is a visible hiccup. While `advance` mode is on and the
current item is the **last playable item of a non-final page**, an effect in
`ImageGallery` fires `usePrefetchPageState({ page: page + 1 })` once per
(page, sha) — the same warm-the-cache verb the manual turn uses, started
when that video becomes current instead of when it ends. Step 1's
cache-first read then lands on it. Guarded by the same `resultsAreStale`
gate as the scan; failures are ignored (the end-of-video fetch is the
retry). Not gated on `showVideo`: `advance` mode plus standing on the last
playable item is already the signal, and one page of rows is cheap.

**Supersession**: the await is a window the user can act in. A token ref
(the `useCommitPageSize` in-flight pattern) is captured before the fetch and
checked after it; manual navigation, gallery close, switching the mode off
`advance`, and a user-initiated `play` on the parked video all invalidate
it, and an invalidated turn writes nothing. A stale intent must never move
the gallery after the user has taken over.

### Fullscreen continuity

Requirement: an `advance` session running inside player fullscreen must
never fall out of it — not between videos, not across a page turn.

What the existing structure already gives us: the fullscreen element is the
gallery's `playerHostRef` wrapper, deliberately **not** keyed by item ("← /
→ must keep browsing inside it"), so the keyed `<video>` swap of a
within-page advance is the arrow-key path fullscreen already survives.

The tripwire is `useVideoPlayerSurface`'s exit rule: `!active && isFullscreen
→ exit()`, where `active` requires the current item to be a loaded, playable
video. **Any commit in which the current item is transiently not a playable
video closes fullscreen.** The advance design must therefore never produce
such a commit — which the fetch-then-flip turn guarantees: during the fetch
nothing changes; after the batched write the one-commit throttle window is
covered by `heldIndex` (still the old video, still playable); the first
fresh commit is already the landing video. The page-turn correction in §3 is
load-bearing for fullscreen, not just for flash avoidance.

The one place the player world genuinely ends is a videoless landing page:
writing `gi = 0` onto an image unmounts the host wrapper and force-exits
fullscreen mid-binge. So the end-of-chain behavior forks on
`player.isFullscreen` (the host passes the flag into the advance callback):

- **Windowed**: turn the page, land at index 0, chain over (§3 step 2).
- **Fullscreen**: suppress the turn entirely — the chain ends parked on the
  last video's end frame, fullscreen intact. This is the codebase's own
  rule ("nothing may leave the user in a fullscreen box with no video in
  it") outranking the land-where-it-ended convenience, which only has value
  outside fullscreen anyway (it positions the *grid*).

Incidentals that hold without work, verified against the layout: the search
loading spinner is a sibling of `playerHostRef`, so it cannot appear inside
fullscreen even on a slow turn (and the ahead-of-turn prefetch makes the
turn a cache hit regardless); error-skips swap video→video inside the
wrapper like any advance; and the `stop` / `loop` modes never unmount
anything, so they need no fullscreen consideration at all.

**Stopped is stopped**: when the chain ends (no more videos, last page, or
an error stop below) nothing rearms it except a real playback reaching a
real end. There is no retry timer and no watchdog.

### Broken videos

A playable-*typed* file can still fail — `MEDIA_ERR_DECODE` (HEVC in an mp4
on most Chromium installs, truncated files) or `MEDIA_ERR_SRC_NOT_SUPPORTED`.
Without handling, one broken file silently kills an unattended chain. So: in
`advance` mode the gallery host listens for the element's `error` event and
calls the same advance step, flagged as a non-playback advance.

That flag exists for one invariant: **an auto page-turn requires that some
video actually reached its end on the current page**. A `playedThisPage` ref
in `ImageGallery` is set by every end-of-playback advance (crossing/`ended`
imply real playback), never by error advances, and cleared on every page
change (auto or manual). The page-turn branch requires it. Consequence: error
skipping chains freely *within* a page, but a page of all-broken videos ends
the process at the page's edge instead of crawling the whole result set —
the "never two pages with no playback" rule, enforced at the only place it
can leak.

Not handled, accepted: a video that never fires `error` *or* `playing` (a
stalled NAS read) stalls the chain, exactly as it stalls a human; autoplay
rejection by the browser (unmuted autoplay before any user gesture on the
origin) parks the new video paused, ending the chain — same behavior the
arrow keys already have, and one click resumes it.

### Interactions that fall out correctly, for the record

- **Outro skip composes**: in `advance` mode an outro-skipping video advances
  at the cut point — the effective end *is* the end. Skip-outros-while-
  binging is the natural product of the two features, no special case.
- **A `vt` trim on some item**: sha-keyed and inert on other items; when the
  chain passes through that item it plays its trimmed range once and
  advances at the trim end. Its trim-start seek applies on load as today.
- **Pause blocks the chain**: a paused video fires nothing; user pause is a
  hold, resume continues, and `advance` never plays *for* a user who stopped.
- **Mid-gesture safety**: rail drags pause the video, so no advance can fire
  under a trim gesture.
- **Fullscreen**: covered by its own subsection above — the wrapper survives
  item swaps, and the two places it could break (a transient non-video
  commit, a videoless landing page) are designed out.
- **Hidden tab**: crossing detection already falls back to `timeupdate`,
  which keeps firing when hidden; the chain (and its page turns) works with
  the tab in the background — listening through results is a use case, not
  an accident.

## 4. History semantics

`gi` is a `history: push` param, one press = one entry, and the gallery
already suppresses arrow autorepeat specifically to protect the back button.
An unattended `advance` session would push dozens of entries an hour, so:
**every URL write on the auto-advance path uses `history: "replace"`** — the
within-page index writes and both members of the page-turn batch.

Plumbing: none on the manual path. Within-page advances call the `gi` setter
with per-call `{ history: "replace" }`; the auto page-turn uses the raw
`page`/`gi` setters directly (§3) precisely because the `setPage` prop chain
and `useSearchPage`'s wrapper are push-mode by design and stay that way.
Trap, inherited from `useCommitPageSize`: replace must be explicit on
*every* member of a same-tick batch — nuqs escalates the whole coalesced
write to `push` if any member asks for it.

Net effect: the history entry the user last created (the video where they
last acted) mutates in place as the session advances, and Back returns to
whatever preceded their last deliberate act. Manual navigation is untouched
and keeps pushing.

## 5. The toggle button

- **One cycling button, three states, three icons**, cycle order
  `loop → stop → advance → loop`. No dropdown: three states is the ceiling
  for a cycle button, and this is exactly three.
- **Gallery-only by construction**: `VideoPlayerSurface` gains an optional
  prop pair (`endAction` / `onEndActionChange`, or equivalent); the button
  renders only when provided, the same existence pattern as `outroCutPoint`
  and `download`. The pinboard passes nothing and is unchanged.
- **Placement**: the S1 right-hand group, leftmost — immediately left of the
  outro-skip button, so the row reads left-to-right as "what happens at the
  end → where the end is → edit the range" (end action, outro, trim).
  Follows the trim button's size ladder: absent at the mini tier (the
  gallery's surface only reaches mini in degenerate layouts; not worth a
  kebab row until someone misses it).
- **Icons** (lucide, matching the row's stock-glyph language):
  - `loop`: `Repeat`. Plain repeat, not `Repeat1` — there is no "repeat all"
    here for "repeat one" to contrast against.
  - `stop`: `ArrowRightToLine` — runs to the wall and stops.
  - `advance`: `ListVideo` — play through the list. `SkipForward` is
    rejected for the same reason the outro design rejected it: it reads as a
    next-track *action button*, not a mode.
  If field validation finds these too weak, a purpose-built glyph set in the
  `OutroSkipIcon` style is the escape hatch; start with stock.
- **Lit state**: unlit at the default (`loop`), lit (`active`) for `stop`
  and `advance` — the button glows when the player will do something
  non-default at the end. Not `aria-pressed` (it is not a toggle): the
  `title`/`aria-label` carries the state and the next state, e.g. "Loop this
  video — click: play once", "Play once, stop at the end — click:
  auto-advance", "Auto-advance to the next video — click: loop".
- No hover bubble and no keyboard shortcut in v1: the title suffices for a
  three-state cycle, and the shortcut namespace around the player is already
  dense.

## 6. Out of scope

- Any pinboard involvement.
- A page-looping mode (rejected above).
- Shuffle, or advancing by anything other than result order.
- Dwell timers for freeze frames or images ("slideshow" is a different
  feature with different controls).
- A stall watchdog for videos that neither play nor error.
- Persisting the mode per-search or in the URL.

## 7. Implementation plan

One UI-repo step (no server work: everything consumed already exists on
`SearchResult`).

1. `lib/videoPlayerState.ts`: the `GalleryEndAction` store (outro-skip
   pattern).
2. `lib/videoTrim.ts`: `loopAtEnd` + `onEndReached` on `useVideoTrim`,
   with the empty-trim `ended` binding, the park-at-end pause, the
   once-per-playback guard and the ended-replay trim-start fix. Pinboard
   call sites untouched (defaults).
3. `lib/searchHooks.ts`: `useFetchPageRows()` beside `usePrefetchPageState`
   (same key identity, `fetchQuery`).
4. `ImageGallery.tsx`: shared playable predicate; `advanceToNextVideo` with
   the fetch-then-flip page turn, supersession token, `playedThisPage` ref
   and its clears, the fullscreen fork, and the last-playable-item prefetch
   effect; `error` listener wiring; mode read + `loop` attr gate +
   `useVideoTrim` wiring in `GalleryImageLarge`; button props into the
   surface.
5. `VideoPlayerSurface.tsx`: the cycling button behind the optional prop.

Verification: `tsc`, `next build`, existing test scripts green. The
`useVideoTrim` changes are the risk concentration — the pinboard must be
demonstrably unchanged (defaults) and the loop mode byte-identical to today.

## 8. Testing notes

- **Hook semantics** (extend `scripts/outroskip.test.mjs`'s harness or
  manual): with `loopAtEnd: false` — crossing parks *at* the end bound and
  pauses; natural end fires the callback once; replay after `ended` starts
  at the trim start; replay from a parked end plays the tail out; the
  callback fires at most once per playback; `loopAtEnd: true` is
  byte-identical to today.
- **Advance walk** (manual): page of mixed media — every image is skipped;
  last video of the page turns exactly one page **and the first commit of
  the new page already shows its first video** (no other item of the new
  page is ever visible, even for a frame); a next page with no videos stops
  at its index 0; last video of the last page parks; a broken video (rename
  a text file to `.mp4` in a test dir) is skipped within the page but a page
  of only broken videos does not turn the page.
- **Supersession**: pressing ← / → (or closing the gallery, or clicking the
  parked video to replay) during the page-turn fetch wins — the turn writes
  nothing.
- **Prefetch**: standing on the last playable item of a page in `advance`
  mode fires exactly one prefetch of the next page (network tab); the later
  turn issues no second request; browsing the same item in `loop` mode
  fetches nothing.
- **Fullscreen**: an advance session started in fullscreen stays in
  fullscreen across N videos *and* a page turn; a videoless next page in
  fullscreen ends the chain parked (still fullscreen), while the same walk
  windowed turns the page and lands at index 0; an error-skip mid-chain
  does not exit fullscreen.
- **History**: an advance session of N videos with a page turn adds zero
  history entries; Back returns to the pre-session state; manual arrows
  still push one entry each. Verify the page-turn batch lands as **one**
  replace (no push escalation from a batch member).
- **Modes**: `stop` parks at trim end / outro cut / natural end (all three
  end kinds); toggling modes mid-playback neither yanks the playhead nor
  double-fires.
- **Pinboard regression**: pins loop exactly as before, with and without
  trims; no end-action button appears on any pin.
