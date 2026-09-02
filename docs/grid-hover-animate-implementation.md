# Grid hover-to-animate, badge rule, video 1x1/2x2, overlay scaling

Implementation plan, converged 2026-09-01. UI-only: every server-side piece it
needs already exists (`?still=true` posters, `big=false` single-frame video
thumbnails with their own grid tiers, immutable loop URLs, `animated_floor` in
`/api/client-config`). Zero backend changes.

Companion to `docs/grid-scroll-performance-implementation.md`, whose §2 rules
(tier latch at mount = the no-flash rule, per-cell URL subscriptions stay at
zero, the playback director owns every global listener) all remain in force.

## 1. Settled decisions

| # | decision | value |
|---|---|---|
| D1 | "Small cell" threshold — ONE shared constant | `SMALL_CELL_THRESHOLD_PX = 200` (CSS px cell width; tunable in QA) |
| D2 | Below the threshold, animated images default to | animate **on hover**; above: **always** (today's behaviour) |
| D3 | Preference storage | browser `localStorage`, **never the URL**. Shape `{ above?: boolean, below?: boolean }` (`true` = always). Absent slot = default. |
| D4 | Toggle semantics | the toggle in the cell-size popover shows the *effective* value for the **current** range and writes **only that range's slot** |
| D5 | `prefers-reduced-motion` | when the OS reports it, the *default* of both ranges is hover-only; an explicit slot always wins |
| D6 | Hover arming | `pointerenter` arms only if a real `pointermove` (coords changed) happened within the last 150 ms AND the director is not scroll-suspended; then a **200 ms dwell**; cancelled by `pointerleave`, scroll start, or suspend |
| D7 | Concurrency | at most ONE hover-playing cell; entering another stops the previous. On leave: pause + unmount the `<video>` (the poster `<img>` never left; the loop URL is immutable-cached, so re-hover is cheap) |
| D8 | Badge rule | badge = "this item moves, but is not moving right now". Shown on: video stills; an animated image's static poster (hover mode, not playing). Hidden on: a playing loop; a below-floor GIF animating natively in an `<img>`. The hover fade stays. |
| D9 | Videos | below the threshold request the single frame (`big=false`), above the 2x2 (today). **Plain `:hover`** at small sizes swaps to the 2x2 — with the card's cover→contain zoom-out, never as a second step after a dwell (user QA 2026-09-02) — latch-until-loaded (same pattern as the aspect>2 display swap). Applies to mounted cells immediately (not latched — see §6). |
| D10 | Gallery | **filmstrip thumbnails** hover-play the loop through the same director (they are grid-s posters today). The **large view is unchanged** (it already serves the original animated file). |
| D11 | Overlay scaling | grid host writes `--cell-px` on its container; overlay chrome geometry is `clamp()`/`calc()` of it: linear ramp from 240 px cells (scale 1.0, corner insets 8 px) down to 140 px (scale ≈0.72, insets 2 px). Acceptance: fanout fully open at the minimum size with no overlap with corner buttons; hit targets never under 18 px. `PlayableBadge` already self-scales (`cqmin`) — untouched. |

## 2. Package A — hover-to-animate

**A1 `lib/state/animatePref.ts` (new).** localStorage-backed box on the
existing `useSyncExternalStore` pattern (`lib/state/valueBox.ts`), key
`panoptikon.gridAnimatePref`. `effectiveAnimateMode(cellPx): 'always' | 'hover'`
= slot for `range(cellPx)` if set, else the default (D2, modified by D5 via
`matchMedia('(prefers-reduced-motion: reduce)')`). Cross-tab sync via the
`storage` event. Every read/write in try/catch; render correctly with no
stored value.

**A2 Toggle** in `components/GridCellSizeControl.tsx`: segmented
"Animate: Always / On hover" bound to the current range (D4), with a hint that
it applies to the current size range. Must NOT add a URL param and must NOT
touch save-as-default.

**A3 Director** (`lib/state/animatedPlayback.ts`): add one capture-phase
`pointermove` listener recording the timestamp of the last *real* move (coords
actually changed vs the previous event — Chromium re-dispatches enter/over when
content scrolls under a stationary cursor, which is the whole thing this
defends against). Export `armHoverPlay(el, onFire)` / cancel, implementing D6
and D7 on top of the existing scroll-suspend state. Extend `planPlayback` (or a
sibling pure function) so the arming rule is unit-testable. Standing cost when
nobody hovers: one listener that reads `clientX/Y` and writes a timestamp — no
layout reads, nothing per cell. In `hover` mode the director does not auto-play
on intersection.

**A4 Cells** (`components/SearchResultImage.tsx`, `AnimatedCellPicture`): plumb
`animateMode` through `CellFlagsContext` (a primitive, so the memoised flags
stay cheap; `lib/state/cellActions.ts`). In hover mode render the `still=true`
poster; on armed fire, mount the `<video muted loop playsinline
preload="none" poster={posterURL}>` layered over it and opacity-swap on
`playing`. Always mode stays byte-identical to today. Mode latches at mount.

**A5 Video cells**: D9. The `big` flag goes into the thumbnail URL helper
(`lib/utils.ts` `getFileURL`) — check how `still`/`size` are threaded and do
the same.

**A6 Badge**: replace the `isPlayableItem(item) && <PlayableBadge/>` sites in
`SearchResultImage.tsx` and `components/gallery/VirtualizedHorizontalScroll.tsx`
with a predicate implementing D8 (`lib/thumbnailTier.ts` already knows the
below-floor case via `animatedCellMode`). Document the rule where
`PlayableBadge.tsx`'s comment says "this expression is the single place".

**A7 Filmstrip** (`VirtualizedHorizontalScroll.tsx`): same arming + poster/loop
swap as A4. VERIFY (test, don't assume) that the director's capture-phase
`scroll` listener fires for the filmstrip's element scroll and suspends
arming while the strip is wheel-scrolled under the cursor.

## 3. Package B — overlay scaling

**B1** `app/search/ResultGrid.tsx` (or wherever the grid-level ResizeObserver
that drives tier selection lives): write `--cell-px` on the container via ref
in the same callback. No React state, no cell re-renders.

**B2** Overlay chrome in `CellActionsHost.tsx` / `SearchResultImage.tsx` and
the fanout component: D11. One `clamp()` ramp defined once (a CSS custom
property `--cell-chrome-scale` derived from `--cell-px`) and consumed by
button size, icon size, corner insets and fanout spacing. Keep
`focus-visible` rings intact.

## 4. Verification (adversarial round checklist)

Unit (`scripts/*.test.mjs`, `node --experimental-strip-types`): preference
resolution incl. reduced-motion and per-range writes; the arming rule as a pure
function; the badge predicate matrix; big/small selection vs threshold.

Browser, on an isolated stack with the animated fixture DB (stdtest has ZERO
animated items), driving **real CDP `Input.dispatchMouseEvent`** — synthetic DOM
events are a recorded trap:
1. scroll with a stationary cursor → nothing plays, nothing swaps;
2. move-then-dwell → plays after ~200 ms; badge hides while playing;
3. rapid skim across cells → only the last dwelt cell plays; no orphan videos;
4. leave / re-hover → poster stays painted, no flash;
5. toggle writes only the current range's slot (inspect localStorage); crossing
   the threshold via the slider flips the default; URL unchanged;
6. badge matrix (video still / poster / playing loop / below-floor gif);
7. filmstrip wheel-scroll under the cursor → nothing plays;
8. minimum cell size: fanout open, screenshot, no overlap, hit targets ≥ 18 px;
9. scroll bench (`tools/scroll-perf`) at the minimum cell size: no regression
   vs the merged baseline — the machinery must cost nothing un-hovered.

## 5. Process

One ui worktree (`Q:\projects\ui-hover-animate`, branch `hover-animate` off
`rust-ui`), Package A then B on the same branch. Opus implementer → Opus
adversarial verifier → coordinator adjudication → fixer. Commit messages ≤ 80
chars, one line, no trailers. Nothing is pushed.

## 6. Outcome (2026-09-02)

Merged into `rust-ui` after two adversarial browser rounds on an isolated
stack with an animated fixture library (real CDP input throughout). All §4
items pass; scroll bench flat with zero long tasks at the minimum size; idle
CPU profile 100% idle with the cursor parked; per-cell listeners unchanged on
static cards.

Deviations from §2/§3, all adopted:
- `animateMode` reaches cells as a latched **prop** (the `tier`/`animatedFloor`
  idiom), not via `CellFlagsContext` — the host publishing the flags has no
  access to the measured cell width, and `SearchResultImage` never subscribes
  to the flags by contract.
- One shared `components/LoopVideo.tsx` for grid and filmstrip; `AnimateMode`
  and `SMALL_CELL_THRESHOLD_PX` live in `lib/thumbnailTier.ts`.
- Extreme-aspect animated cards **never hover-arm**: their whole-image swap
  shows the original file, which animates natively, so a loop would be fetched
  only to be unmounted.
- Found and fixed en route: the director's scroll-velocity sampler read only
  `scrollTop`, so horizontal (filmstrip) pans could never trigger the
  fast-scroll suspend.
- Round-1 defect, fixed: arming was attempted only on `pointerenter`, which
  browsers dispatch BEFORE the `pointermove` that carried the cursor in, so a
  pointer that rested ≥150 ms before crossing a boundary was refused and never
  retried. Arming is now an explicit state machine: the entry's own coordinates
  count as the move (a scroll-induced re-dispatch carries identical
  coordinates, so D6 holds), and a refused or scroll-released root stays a
  pending candidate that the director's own capture-phase `pointermove` arms
  on the next real move inside it. Nothing arms on scroll settle alone.
- The badge over a *playing* hover-loop stays mounted and is hidden by
  `PlayableBadge`'s hover fade (hover-play implies `:hover`); unmounting would
  cost a per-cell state update per arm. The two are coupled — documented at
  `showsMotionBadge`.

User QA, 2026-09-02, two chrome fixes: (1) the two bottom buttons were not on
one baseline — the fanout's anchor was `bottom-3 left-1` against the details
button's `bottom-2 right-2` *before* this package, a 4 px stagger that the
"pixel-identical to today" criterion preserved and the ramp widened to ~6 px
at the smallest cells; the verifier's own screenshot showed it and its
geometry table listed both values, and nobody (coordinator included) read
alignment as a criterion because D11 never named it. Every corner anchor is
now the one `--cell-chrome-inset`. (2) The grid's file-action fanout moves to
bottom-right to match the filmstrip; the details button takes bottom-left.
(3) The small-cell video swap followed the loop's arming rule and dwell, so
the 2x2 arrived as a second change after the hover zoom-out; it now follows
`:hover` directly (mouseenter/leave on the group root, like the extreme-aspect
swap), so both changes are one moment.
(4) `animateMode` and `smallCell` were latched at mount like the tier, so the
Always / On hover toggle reached only cells mounted afterwards — it looked
inert until a refresh. They are live props now: the tier latch guards against
resize-driven flashes the user did not ask for; these two change only on a
deliberate act on the grid whose whole point is that the visible cells change.

## 7. Traps carried over

- React Compiler is ON: bare allocations hoist into dependency-free memo slots;
  `eslint-disable react-hooks` opts a whole function out; `ResultGrid` is
  `"use no memo"` (TanStack Virtual mutation). Effect declaration order in
  `ResultGrid.tsx` is load-bearing (see its bookkeeping map).
- Per-cell nuqs/`useSearchParams` subscriptions must stay at 0; a scroll-stop
  `top` write must still re-render zero cells.
- `PlayableBadge` fades on `group-hover`; the badge rule (D8) is about
  mounting, the fade is unchanged.
- Never launch `panoptikon.exe` without `--config` naming a scratch port; kill
  by exact PID only; ports 6342/6343/6339/3000 are off-limits.
