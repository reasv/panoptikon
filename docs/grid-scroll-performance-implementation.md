# Grid scroll performance: implementation plan

Written 2026-08-31, from the measurement session of the same day. Two tracks
executed together: the backend thumbnail tier ladder and the frontend render
efficiency work. Neither is optional; the measurements show both are required
to reach the target.

**Target**: scroll mode smooth enough to become the UX default — flat frame
times comparable to a native app, in both directions, at any session length.

## 1. Baseline and evidence (summary)

Measured with a CDP-driven Chromium instance (visible window,
background-throttling disabled), constant-velocity scroll at 4000 px/s,
maximized on a 4K display. Full session data in the investigation session;
the synthetic harness reproduces every number below.

Synthetic grid, realistic worst-case-policy images (12/33/100 MP JPEG +
16 MP PNG mix), cold scroll down then warm scroll up:

| Image source                    | direction | mean    | p90     | p99    | long tasks |
|---------------------------------|-----------|---------|---------|--------|------------|
| Originals (served directly)     | down/up   | 22/33ms | 8/42ms  | 872/554ms | 5.7/6.3s |
| 4096px stored thumbs (today)    | down/up   | 9/28ms  | 25/67ms | 42/258ms  | 0.5/3.6s |
| Tier M 1024, 5 cols             | down/up   | 4.2ms   | 4.2ms   | 4.3ms  | 0 |
| Tier S 512, 10 cols (4× cells)  | down/up   | 4.2ms   | 4.2ms   | 4.3ms  | 0 |

Key facts the plan builds on:

- The cost cliff is **decoded megapixels per cell**, not bytes or network.
  Chromium re-decodes on every cell remount; the immutable-cache work only
  killed transfers. Warm up-scroll is *worse* than cold down-scroll because
  cached bytes let all decodes fire simultaneously.
- `image_is_served_directly` (jobs/files.rs) serves any ≤5 MB file at
  original resolution regardless of dimensions (a 2.9 MB 100 MP JPEG goes to
  the grid raw), and stored thumbs cap the **long** side at 4096 (≈16 MP) —
  both inadequate, and long-side capping is backwards for `object-cover`
  cells (crispness is bound by the image's *short* side).
- GIFs are served as the original file always (api/items.rs `item_thumbnail`
  short-circuit); animated WebP >5 MB silently loses animation.
- The scan already fully decodes every image (blurhash), so tier generation
  is resize+encode on already-paid pixels.
- Frontend: ~19 nuqs hook instances + ~5 toast subscriptions per grid cell
  defeat `React.memo` from inside (every one is a `useSearchParams`
  subscriber); the derived-virtual-page `useState` in MultiSearchView
  re-renders the whole search page every `page_size` items scrolled; blurhash
  is decoded + PNG-encoded in pure JS per cell mount; sustained scrolling
  degrades monotonically (p90 8→34 ms over 40 s) from **accumulation, not
  scroll depth** — reproduced with zero image work (heap +64 MB/15 s, heavy
  GC), also present in pages mode with huge pages, so not the chunk store.

## 2. Settled design decisions (do not relitigate)

Converged with the user 2026-08-31:

- **Thumbnail ladder**: keep a 4096-class `display` tier (gallery quality +
  the browser-safety bound; rule change below). Add grid tiers capping the
  **short side**: `grid-m` = 1024, `grid-s` = 512. *(Superseded: a third rung
  `grid-xs` = 256, and both the encoder and the container are now
  `docs/thumbnail-format-implementation.md`'s R1/R4/R5 to decide.)*
- **Extreme aspect ratios (comic strips, webtoons) are cropped, not
  blurred.** These are real content in the target datasets and cluster in
  search results, so neither "let the long side run" (30 × 26 MP cells =
  the catastrophic regime again) nor a megapixel guard (crushes a webtoon's
  width to ~350px — blurry in the cell) is acceptable. The settled rule,
  derived from what a cover cell actually displays:
  - aspect ≤ 2: plain short-side resize (the whole image; no behavior
    change from a simple tier).
  - aspect > 2: the grid tier is a **crop** — short side ≤ tier, long side
    = 2× tier, cropped to match the CSS presentation. `object-top` means
    `object-position: 50% 0%`: tall images crop to the **top strip**, wide
    images to the **horizontally centered band**. The top alignment for
    tall images is a deliberate two-year-old product decision (portraits
    keep the face; center-cropping showed torsos) — the stored crop must
    preserve it exactly, and it is shared by every cover-top consumer
    (grid cells, gallery filmstrip, pinboard history previews). Full
    crispness in the default state; ≤2×TIER² pixels by construction, so no
    separate MP guard exists.
  - **Contain surfaces never receive crops**: any surface that displays
    with `object-contain` (similarity header, pinboard preview popover,
    gallery large view) shows the whole image, so for aspect > 2 items it
    must request the `display` tier — the same client-side aspect check
    the hover swap uses. (Aspect ≤ 2 grid tiers are whole-image, so those
    surfaces may use them freely.) The 80×80 pinboard drag ghost uses
    plain center `object-cover`; a top-crop shown there is accepted as a
    non-issue — out of scope.
  - The hover `object-contain` state (which shows the *whole* image, tiny)
    cannot use a cropped rendition: grid cells for aspect > 2 items swap
    their src to the `display` tier on hover — long-side-bounded, hence
    inherently few MP for a strip, and cover-vs-contain each get the
    rendition shaped for them. Normal-aspect items keep today's no-swap
    hover. Requires item dimensions in the search result payload (verify
    availability; add to enrichment if absent).
  - **Zero-cost-for-normal invariant**: the URL scheme is aspect-independent
    (every cell requests the same `size=` for its box; crop-vs-whole is the
    server's business), the aspect test is one comparison on row data (no
    hook, no measurement, `React.memo`-compatible), and the swap
    handlers/state are mounted ONLY on aspect > 2 cells — a hover swap
    re-renders that one memoized cell, never the grid. Normal-aspect cells
    keep today's CSS-only hover with no listeners and no state. Verifier
    rounds treat this as an invariant.
- **Serve-directly becomes dimension-first**, per requested tier: for grid
  tiers, serve the original iff its short side ≤ 1.25× the tier AND its
  long side ≤ 1.25× (2 × tier) AND bytes ≤ 8 MB. *(Adjudicated refinement,
  B1 verify round 1: the original clause was `aspect ≤ 2`, which refused
  every strip however small — a 1000×2100 image stored a grid-m "crop" of
  1000×2048, the whole picture 2% smaller, pure waste. `2 × tier` is the
  longest a stored tier is ever allowed to be, so bounding the long side
  against it with the same 1.25× slack measures the thing that actually
  costs — decoded pixels — instead of using aspect as a proxy for it.
  Genuine strips are unaffected: an 800×20000 webtoon is far past the
  bound at both tiers and still stores its top-strip crops. `grid_plans`
  skips exactly what this rule would serve directly, so the dispatcher,
  the generator and the endpoint stay one function.)*
- **Display tier rule is re-based on the short side**: serve the original
  iff short side ≤ 4096 AND total ≤ 32 MP AND bytes ≤ 24 MB; otherwise
  store a display thumb scaled by `min(4096/short_side, sqrt(32MP/(w*h)),
  1)`. *(Superseded: the pixel bound is 24 MP, the byte bound is per source
  class, and a stored rendition is capped at 2560 on the short side — see
  `docs/thumbnail-format-implementation.md` §2 R2.)* This drops the ≤5 MB-any-dimensions escape (the 100 MP hole) AND
  fixes an existing quality bug: today anything with long side > 4096 gets
  a 4096-fit thumb, so a 800×20000 webtoon renders 163px wide in the
  gallery. Under the new rule its original serves directly (16 MP, one
  image at a time — fine). Previously direct-served or long-side-crushed
  images gain correct display thumbs via backfill.
- **Backfill, not version bump**: a new scan backfill question ("which
  indexed images lack the tiers the current code would produce"), following
  the established pattern (animated-spans / display-dims questions). Do NOT
  bump `THUMBNAIL_PROCESS_VERSION` (that would regenerate video thumbs
  needlessly). Tier rows carry their own `TIER_PROCESS_VERSION`, independent
  of it in both directions: bump that — and only that — for a tier generator
  change the stored *geometry* cannot see (crop anchor, resampling filter,
  JPEG quality). A change that moves the dimensions needs no bump; the
  dispatcher's geometry comparison already catches it. Recovery remains
  generic erase+rerun; no one-off actions.
- **Animated thumbnails are video loops**: H.264 yuv420p, CRF ~18,
  `+faststart`, fps and duration preserved, ONE animated rendition per item
  (short side ≤ min(1024, source short side)) reused by both grid tiers.
  Applies to GIF and animated WebP (and AVIF when importing lands), except
  small-raw floor: original served raw iff ≤1 MB AND both dims ≤512.
  Static tier thumbs double as posters. Encoded-larger-than-original edge:
  keep the original. Client renders `<video muted loop playsinline
  preload="none">` styled identically to an image — **no `autoplay`**: a
  playback director plays only cells that are sufficiently visible and inside
  the concurrency cap (IntersectionObserver; pause off-screen and during fast
  scroll), and `preload="none"` is what makes that decision cover the network
  and not just the decode. Measured while `autoplay` was still on: every
  mounted loop fetched and fully buffered itself, 16 of 16 buffered with 14 of
  them off screen, ~4 MB nobody saw. Codec choice is settled: NOT AV1 (hardware decode sessions are
  finite at 20–30 streams; software H.264 is cheap, software AV1 is not).
  Known risk to quality-pass: yuv420 chroma fringing on pixel-art GIFs.
- **Tier selection is client-side by need**: request the smallest tier whose
  short side ≥ cell edge × DPR (with ~1.125 slack), i.e. `grid-s` when
  cellCSS × DPR ≤ 576, `grid-m` when ≤ 1152, else `display`. This is what
  keeps decoded MP per screenful constant as the size slider shrinks cells.
- **Size slider ships in this batch** (tier `grid-s` has no consumer without
  it). Design pre-settled in `docs/search-scroll-mode-design.md` §9: it
  controls explicit cell width (not column count), auto-vs-explicit is a
  hard switch, slider co-writes `page_size` by default with a lock toggle,
  batched replace-history writes.
- **Current cell sizes are the max**; no headroom sizing. Regenerating
  larger tiers later is an accepted escape hatch.

API contract (frozen now so tracks can proceed in parallel):

```
GET /api/items/item/thumbnail?id=…&id_type=sha256[&index_db=…]
    [&size=display|grid-m|grid-s]      default: display (back-compat)
    [&still=true]                      animated items: force the static tier
                                       image (poster) instead of the loop
```

- `size=grid-*` on an animated item with a stored loop returns `video/mp4`;
  `still=true` returns the static tier JPEG. Static items always return an
  image. Existing no-param behavior is unchanged (gallery untouched).
- Caching: URLs stay content-addressed; the same immutable/ETag rules apply
  per (id, size, still) variant, with one split settled in adjudication: a
  requested tier that has **no stored rendition** answers with a fall-up (the
  next larger tier, the display rendition, or the original file), and that
  response is immutable only when the serve rules say the tier will *never*
  exist. When the ladder says a tier should exist and the backfill has not
  written it yet, the fall-up is `public, no-cache` — otherwise a client that
  asked for `grid-m` during a library-wide upgrade pins the heavyweight bytes
  for a year and never sees the tier land. The ETag already changes when the
  tier arrives, so revalidation costs one 304.
- **Transition semantics** (URL versioning considered and REJECTED — the
  `size=` contract is frozen): B1 changes what the *default* path serves for
  two classes of item — the webtoon fix (an 800×20000 strip is no longer
  crushed to 163×4096) and items whose superseded stored rendition is
  dropped. Clients holding a year-long immutable entry for those default-path
  URLs keep serving the old bytes until natural eviction or a hard refresh.
  Accepted as a one-release transition: nothing is broken, only stale, and
  the new grid-tier URLs are unaffected because no cache has ever held them.
  **Release-notes reminder**: the release that ships B1 must say that
  previously-viewed extreme-aspect images may keep showing the old,
  long-side-crushed thumbnail in a warm browser cache until it is refreshed.
- Client decides `<img>` vs `<video>` from the result's `type` + `size`
  fields against the same floor constants; the floors are surfaced through
  `/api/client-config` so the two sides cannot drift.

## 3. Work breakdown

Every step lists: scope, primary files, exit criteria. File anchors are
as-of 2026-08-31; verify before editing.

### W0 — Harness hardening (small, first)

Promote the measurement harness (`cdp-scroll-bench.mjs` + synthetic grid
page + tier image generator) from session scratchpad into `tools/scroll-perf/`
with a README (launch flags for the instrumented browser, scenario matrix,
how to read output). It is the acceptance instrument for every other step.
Durable harness, so `tools/` is the right home per repo policy.

Exit: one command reproduces the baseline table against the synthetic page,
and one runs the stdtest matrix against a gateway on 6343.

### Backend track (main repo, `panoptikon/src`)

**B1 — Still-image tier ladder + dimension-first serving**

- Scan/generation: extend the visuals pass (`jobs/files.rs`,
  `build_new_item_renditions` / `generate_thumbnail` / `encode_image`) to
  produce `grid-m`/`grid-s` renditions by cascaded resize from the already
  decoded image (display → m → s). Short-side cap per §2; aspect > 2
  produces the presentation-matched crop (top strip / center band) instead
  of a whole-image resize. Note the crop must come from the pre-display
  decode when the display tier itself was MP-guard-scaled (never upscale a
  scaled intermediate).
  Storage schema in the storage DB grows a tier discriminator (migration in
  `db/migrations.rs`; mind the WITHOUT-ROWID payload rule — blob tables stay
  rowid tables).
- Serving: `api/items.rs` `item_thumbnail` gains `size`/`still` params and
  the per-tier dimension-first serve-directly rule; `image_is_served_directly`
  (jobs/files.rs:4910) reworked per §2 (the ≤5 MB escape dies). Keep the
  scanner-side predicate and the dispatcher's no-decode answer
  (`maybe_dispatch_backfill`, files.rs:2189-2208) consistent — that
  invariant ("decide from indexed metadata, never decode on rescan") must
  survive.
- Backfill: new scan question for missing tiers, wired like the existing
  backfill questions; must also catch previously direct-served >4096px
  images that now need a display thumb. It must early-out by mime family
  before touching storage (most files in a general-purpose library have no
  generator at all, and this runs per file per scan over SMB) and reuse the
  stat/dimensions the served-directly question already fetched.
- Animated items (GIF, and any image with a measured `items.duration > 0`)
  are excluded from the **still** ladder in both the dispatch question and
  the generator — B2 owns them. A static tier written for one now would be
  superseded the moment the loop pipeline lands, and in the window before
  it does the default path can flip an animated original to a static JPEG.
- Video items: their existing thumbs (frame grid + first frame) also get
  tier variants — they're the 3840×2160 stills the grid loads today.

Exit: cargo tests (including new coverage for the resize math, the crop
geometry — tall/top, wide/center, aspect exactly 2, tiny originals — the
serve rule truth table, and backfill idempotency — the scan's write-once
discipline per existing tests); a fixture gateway serves correct
content-type/dimensions per (size, still) combination; re-scan of an
already-scanned fixture DB generates tiers exactly once.

**B2 — Animated video-loop pipeline** (after B1 merges; same files)

- Generation: for GIF/animated-WebP above the raw floor, encode the H.264
  loop (ffmpeg is already a scan dependency; the animated-WebP decode bridge
  from the compose work is precedent for input handling) plus static tier
  posters. Store alongside tiers with an animated discriminator. The §2
  extreme-aspect crop rule applies to loops and posters the same way
  (crop filter in the encode).
- Serving: the `size=grid-*` → video/mp4, `still=true` → poster contract.
  The `image/gif` short-circuit in `item_thumbnail` (api/items.rs:457) is
  retired for tier requests; kept for the default/display path.
- **B2 MUST fold `still` into the ETag of the two file-serving branches**
  (the GIF short-circuit and the image fall-through) *before* `still`
  selects different bytes there. Those branches answer with
  `file_response`, whose validator is `sha256-size-mtime` — identical for
  the loop and the poster — so shipping the selection first hands a cached
  poster back for a loop request and vice versa. The stored-rendition
  branches already carry a `-still` suffix. TODO comments mark both sites.
- Floors into `/api/client-config`.
- Fixes the silent animation loss for >5 MB animated WebP as a side effect
  (loop exists; default path can keep serving what it serves today).

Exit: fixture GIFs (tiny raw-floor, large, long, pixel-art) round-trip with
correct types; visual quality pass on the pixel-art case (chroma fringing)
with CRF/scale adjustment if needed; encode-larger-than-original fallback
covered by test.

### Frontend track (`ui/`)

**F1 — Cell de-subscription** (biggest single win; touches every cell file)

Make `SearchResultImage` and everything it mounts render from props/context
computed once at grid level, so `React.memo` actually holds:

- Evict per-cell nuqs/useSearchParams: the in-cell `useSearchParams` +
  `imageLink` memo (SearchResultImage.tsx:45-55) — build gallery hrefs
  lazily at interaction time (or from a grid-level serialized base). The
  ~19 per-cell nuqs instances across BookmarkBtn, FileActionCluster,
  OpenFile/OpenFolder (`useFileOpenActions` ×2), OpenDetailsButton, and
  PinButton (`usePinBoard` = 10 nuqs hooks incl. a per-render `pinboard`
  stringify) get hoisted to one instance at grid level, with cells receiving
  stable callbacks/values. `OpenFileDetails.tsx`'s `usePaneRouting`
  commentary documents the intended pattern — PinButton reintroduced
  exactly what it engineered around.
- Toast: cells stop registering listeners (5×/cell today); use the imperative
  toast entry point from callbacks instead.
- Blurhash: module-level LRU keyed by hash string + hoist the CRC table out
  of `generatePng` (lib/state/blurHashDataURL.ts) — cheap insurance either
  way, decisive for the warm re-scroll path.
- Fold in the small paper cuts in the same files: the skeleton miss-path
  request-build+hashKey per cell per render (searchHooks.ts blockAt), the
  px-vs-rem breakpoint desync (GRID_BREAKPOINTS vs globals.css).
- Constraint: React Compiler is ON; ResultGrid keeps `"use no memo"`
  (TanStack Virtual), so do not rely on compiler memoization inside the
  grid — rely on `React.memo` + stable props. Honest deps only; no
  eslint-disable in new hooks (it opts the whole function out of the
  compiler).

Exit: instrumented render-count check — a scroll-stop `top` write re-renders
the header/scrubber but **zero cell bodies**; a chunk arrival re-renders
only cells whose row data changed. tsc + unit scripts + production build
green; all cell verbs (bookmark, pin, open, share, details) still work
against 6343.

**F2 — Derived-page scoping + top-of-tree render cost** (after F1; same
SearchPage.tsx region)

- Move the derived virtual page out of MultiSearchView's `useState`
  (`useDerivedVirtualPage`) into a narrow subscription (external store /
  `useSyncExternalStore`) consumed only by PageSelect and whatever header
  bit needs it. A virtual-page crossing must not re-render MultiSearchView,
  GridPanel, or the grid.
- Trim MultiSearchView's per-render body cost for the renders that remain:
  the double `JSON.stringify` in `useThrottledValue`, the `hashKey` calls,
  PageSelect's ~35 `URLSearchParams` copies per render (memoize by
  the inputs that actually change).
- Preserve scroll-mode invariants from `docs/search-scroll-mode-*.md`:
  `floor(top/k) = page−1`, rowsIdentity semantics, the dep-less ensureRange
  effect and its setWanted-returns-prev termination guarantee.

Exit: scrolling continuously at 4K/5-col with images blocked produces zero
frames >16 ms attributable to crossings (harness, CDP image blocking);
scrollmode unit tests green; mode-switch page-number invariant covered.

**F3 — Accumulation bisection** (investigation; read-only, runs parallel
with F1/F2)

Instrumented long-run measurements (the 40 s degradation reproduces on
stdtest) + CDP heap sampling to identify what grows: candidate classes are
per-mount listener/observer churn, React Query cache growth, GC debt from
per-render allocation, decoded-image/GPU-process state. Pages-mode-with-
huge-page also degrades (user report), so chunk machinery alone is excluded.
Deliverable: a findings write-up naming the growing object classes with
evidence, and proposed fixes. **Fixes land as F5 after adjudication.**

Exit: the degradation curve is attributed — re-running the 40 s scenario
with the suspected subsystem amputated flattens the curve.

**F4 — Tier consumption + size slider** (after F2 and B1's contract is
live on a fixture gateway)

- `getFileURL`/cell image URL builds carry `size=` chosen from cell CSS
  width × DPR per §2 thresholds. Per-surface audit, each requesting the
  tier its box needs: the gallery filmstrip
  (`VirtualizedHorizontalScroll`, ~240×320 CSS boxes) moves to **grid-s**
  — today it loads display-tier/original URLs, which is why it is only
  *relatively* smoother than the grid; similarity header and pinboard
  small pins likewise. Gallery large view stays on the default path.
- Extreme-aspect hover swap (§2): grid cells for aspect > 2 items switch
  src to the display tier while hovered (cover→contain), with a
  no-flash transition (keep the crop painted until the contain rendition
  is ready).
- **Contain surfaces and the hover swap request `?size=display`
  explicitly** for aspect > 2 items — the parameter spelled out, never the
  bare no-parameter URL that means the same thing. Two reasons, and the
  second is why it is stated here rather than left to taste: it keeps the
  aspect rule visible at every call site, and `?size=display` is a **new
  URL**, so it cannot be answered from a cache entry stamped before B1
  shipped. That is what busts the stale-webtoon class (see the
  release-notes reminder below) for exactly the items the bug was about.
- Size slider per design §9 (settled decisions there); scroll mode's fixed
  row height recomputed from explicit cell width; tier switch falls out of
  the URL builder. Breakpoint-auto remains the default.
- Blurhash/placeholder behavior at tier switch: changing tier changes the
  URL — must not flash-reload cells that are already displaying (keep the
  old image until the new loads, or only apply tier changes to newly
  mounted cells).

Exit: harness matrix on the synthetic page at slider sizes (full, half,
min) stays flat; grid on 6343 requests the expected tier per size
(network-log assertion); slider round-trips through URL + defaults stamping
consistent with the presentation-params doctrine.

Measured outcome (Wave-4 integration, 2026-09-01): met at every size except
the slider MINIMUM under the harness's 4000 px/s protocol, where 15 columns
× max velocity ≈ 280 cell mounts/s hits a steady-state mount-throughput
ceiling — p90 ~28 ms, zero long tasks, dead flat over 60 s (no
accumulation), image-independent (identical with images blocked), and flat
(p90 6.0) at a realistic 1500 px/s. Recorded as a known ceiling, not chased
in this batch; cell MOUNT cost is the lever if it ever matters.

Release-note item (alongside the stale-webtoon-cache note): on datasets
whose images sit inside the grid-m dimension bound (e.g. ~768×1092 anime
sets), `grid-m` serves the ORIGINAL directly — the ladder then optimizes
decoded pixels (30 vs ~976 MP/screenful) but not bytes (~800 KB originals
vs ~105 KB stored grid-s tiers). That is the settled §2 dimension-first
tradeoff; remote/NAS users who want small bytes at mid sizes should use the
smaller cell sizes, which request stored tiers.

**F5 — Accumulation fixes** (from F3 findings; scope defined at
adjudication)

Exit: 40 s sustained-scroll on stdtest: last-bucket p90 ≤ 1.25× first-bucket,
no heap growth trend with images blocked, and the fix is explainable (no
"it went away").

**F6 — Animated cells** (after B2 + F1)

- Cells render `<video muted loop playsinline disablepictureinpicture
  preload="none">` — deliberately **not** `autoplay`; see §2 — for animated
  types above the floor — decided from `type`+`duration` ("does
  it move") and `size`+`width`/`height` against the client-config floors ("is
  it above them"); all five fields are in the search payload — poster = still
  tier; styled indistinguishably from `<img>`.
- **The `<video>` → poster fallback is REQUIRED, not defensive polish.** A
  grid-tier request for an animated item above the floor answers with the
  item's *own* format rather than `video/mp4` in two cases the client cannot
  tell apart in advance, and a cell that only ever mounts `<video>` shows
  nothing in both:
  - **Backfill pending** — the loop has not been written yet. Transitional,
    and it costs one wasted fetch per animated cell for the length of the
    upgrade window; the response is `public, no-cache`, so it resolves as
    soon as the scan lands.
  - **Keep-the-original** — no H.264 encode of this source came out smaller
    than the source, so the settled §2 edge stores the loop's geometry with
    no bytes and the endpoint serves the file itself. **Permanent** for those
    items, and served immutable, so the fallback is the only thing that ever
    renders them.

  Handle it on the element's `error` event: swap to the `still=true` poster
  URL, which is a stored JPEG for every item above the floor. Do not gate on
  a probe request — that would double the request count for the common case
  to save one wasted fetch in the rare one.
- Concurrent-playback cap: play only sufficiently-visible cells
  (IntersectionObserver), pause during fast scroll and off-screen. Respect
  the existing browser-media-hygiene rule in verification (muted is
  intrinsic here).
- Grid/gallery interplay: gallery and pinboard keep their existing animated
  handling (spans machinery); this step is grid cells only.

Exit: a seeded fixture page of 30 animated cells scrolls within the tier-M
frame budget; play/pause cap observable; static fallback correct below the
floor; the poster fallback covered by a test that serves a non-video response
at a grid tier.

Reading the result: an animated page's frame times sit at ~11 ms where the
same page of stills sits at ~5.5 ms, and that gap is **not main-thread work**.
Playing loops drop the page's compositor cadence from ~180 Hz to ~90 Hz, so
~11 ms *is* the rAF interval — the frame is idle for most of it. Long tasks and
frames over 32 ms are the numbers that would mean jank, and both are zero on
both pages. Do not read the 11 ms as a regression.

### Final pass

**R — Fable architectural review, per subsystem** (only after all issues
from every verifier round are fixed and gates are green)

Not an adversarial bug hunt — an architecture and code-quality pass.
Subsystems, one Fable reviewer each, run in parallel:

1. Backend generation pipeline (files.rs visuals pass + backfill wiring)
2. Backend serving + storage schema (api/items.rs, db/storage.rs, migrations)
3. UI cells + grid (SearchResultImage, imageButtons, ResultGrid)
4. UI state/scroll machinery (searchHooks, SearchPage top, derived-page store)
5. Animated path end-to-end (encode → serve → video cells)

Each reviewer proposes structural improvements (boundaries, naming,
duplication, doc-comment fidelity, altitude); the coordinator adjudicates
which are worth the churn; Opus fixers apply the approved ones; full gates
re-run after each subsystem's batch.

## 4. Parallelization: waves and worktrees

The two repos (main + `ui` submodule) do not conflict at the file level, so
the tracks run concurrently without cross-repo coordination except the API
contract (frozen in §2) and the client-config floors.

```
Wave 0:  W0 harness → tools/scroll-perf            [main repo, direct]
Wave 1:  B1 stills+serving      [worktree: main repo,  branch wt-thumb-tiers]
         F1 cell de-subscription [worktree: ui,        branch wt-grid-cells]
         F3 accumulation bisection [read-only, no worktree]
Wave 2:  B2 animated pipeline   [same backend worktree, after B1 merges]
         F2 derived-page scoping [ui worktree, after F1 merges]
Wave 3:  F4 tiers+slider        [ui, needs B1 merged + F2 merged]
         F5 accumulation fixes  [ui and/or main, scope from F3 adjudication]
Wave 4:  F6 animated cells      [ui, needs B2 + F1]
         Integration verification (harness matrix + stdtest + fixture QA doc)
Wave 5:  R architectural reviews (5 parallel Fable reviewers) + approved fixes
```

Within a wave, units in different repos always run in parallel. F3 is
read-only and rides alongside anything. Merges to `rust-ui`/`master` (with
gitlink bump) happen at wave boundaries, by the coordinator, after gates.

## 5. Orchestration protocol

Roles per step (user-specified):

1. **Opus implementer**: receives the step spec (this doc section + file
   anchors + the settled decisions), implements on the assigned worktree
   branch, runs the step's gates, commits (≤80-char subjects, no trailers).
2. **Opus adversarial verifier**: fresh context; receives the spec and the
   diff; hunts for real defects — contract violations, invariant breaks
   (scroll-mode traps in §3/F2, scan write-once discipline, config-authoring
   rules), regressions, missing tests. Produces a findings list.
3. **Coordinator (Fable, this session)**: reviews every findings list
   personally — adjudicates each finding (real / intended / out of scope),
   chooses or overrides the fix approach, and decides whether the step
   needs another verify round before proceeding.
4. **Opus fixer**: applies the adjudicated fixes; verifier re-checks
   (round 2+); coordinator closes the step.

Standing constraints for every agent:

- All runtime testing against the stdtest-locked gateway on **6343** or a
  throwaway fixture gateway on an unused port — never 6342, never real DBs,
  never launch the production gateway.
- Port 3000 belongs to the user; dev servers only on free ports via
  launch.json entries.
- Commits only on the assigned worktree branch; never rewind shared
  branches; the coordinator performs merges and submodule gitlink bumps.
- Gates: backend = `cargo test` + zero-warning build; ui = `tsc`,
  `npm run build`, the scripts/*.test.mjs suites (scrollmode et al.);
  perf-relevant steps additionally run the W0 harness scenario named in
  their exit criteria.
- No pushes; the user pushes and performs final UX QA (build artifacts and
  a QA checklist are handed over per step where user-visible).

## 6. Out of scope / deferred

- AVIF importing (pipeline covers it when it lands).
- Policy-driven JIT thumbnails for public deployments (separate design).
- ANN/vector work, HTTP/2, and anything in `docs/client-performance-plan.md`
  not named here (P4/P5/P6 stay tracked there).
- Animate-on-hover-only as default (kept as a possible later policy toggle;
  always-animate remains the default UX).
- **A per-item animated-rendition field in the search payload.** The client
  currently transcribes `is_animated_image`, redoes the floor arithmetic from
  `/api/client-config`, and still guesses about a loop the backfill may not
  have written; one server-computed field per item ("static" / "still" /
  "loop") would collapse all three. The `<video>` → poster error latch stays
  regardless — it also covers the settled keep-the-original edge, which no
  payload field makes go away. Deferred because the search payload's shape is
  frozen for this release.
- **AVIF importing readiness.** The seam is already clean: `grid_ladder`
  answers `Unknown` (never a permanent verdict) for a container this build
  cannot demux, the capability table
  (`media_tools::animated_container_support`) has the AVIF row and the probe
  behind it, `animation::avif_animation_seconds` measures the duration with no
  ffmpeg at all, and the loop encoder's input preparation already refuses AVIF
  explicitly rather than by omission. What is missing is only the import path
  itself.
- **Lazy-mount cell overlay chrome.** Every grid cell mounts its overlay
  chrome — a `QueryObserver` plus Radix menus — invisibly, whether or not the
  pointer ever reaches it. At the measured mount ceiling (280 mounts/s, i.e.
  ~3.6 ms per mount on average) a mid-teens p90 at the smallest cell size is
  the estimate, so gating the overlay behind `pointerenter`/`focusin` is worth
  a look. Verify with the harness before building any of it: the estimate is
  arithmetic on an average, not a measurement of this component.
