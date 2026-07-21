# Changelog

All notable user-visible changes to Panoptikon are documented here. Release
sections are maintained newest first and are published verbatim as GitHub and
Desktop release notes.

## [Unreleased]

## [v0.1.7] - 2026-07-21

### Added

- **Search results are now cached.** The server keeps recent search results in
  memory, so revisiting a page, refreshing, or re-running a query answers
  instantly instead of re-executing it. Semantic search benefits most: the
  scan over your embeddings is paid once per query instead of once per page,
  and the UI now also has the server prefetch rows beyond the current page, so
  the next pages of a semantic search open instantly too.
  - Never stale: every write to a database invalidates its cached entries, so
    a cached answer is always identical to re-running the query.
  - Changing the page size or offset still hits the cache: rows are cached as
    contiguous spans, not exact pages.
  - Enabled by default with a 128 MB budget (`[search] cache_size_mb`; `0`
    disables). Desktop settings gained cache controls: the budget (applied
    live, no restart), usage and hit-rate stats, a clear button, and a disable
    toggle - use that one if you edit your databases outside Panoptikon, since
    the cache can't see those writes.
- **Random ordering is now stable and pageable.** Randomly ordered searches
  accept a `seed`: the same seed always produces the same shuffle, so pages no
  longer repeat or skip items, and refreshing, navigating back, or sharing a
  link reproduces exactly what you saw. Omitting the seed keeps the old
  fresh-shuffle-per-request behavior, and the response reports the seed used
  so clients can keep paging through the same shuffle.
- **Vector quantization (experimental).** Panoptikon can maintain compact
  binary copies of your embeddings (about 3% of their size) and use them for a
  fast first pass in semantic search, re-scoring the best candidates against
  the full-precision vectors so the results you see keep their exact ordering.
  Quant profiles are declared in the index database's `config.toml`, are built
  and kept up to date automatically (builds are resumable, and new embeddings
  are quantized as they are written), and can be managed from a new Vector
  Quantization card on the scan page. The vector search filters gained
  `index`, `variant`, and `k` arguments, with matching selectors in the search
  UI. So far the fast path only wins on the default combined-search query
  shape (up to ~3x there) and loses elsewhere, so no search uses quants unless
  you explicitly select a profile.
- **Desktop shows the address of the Search UI.** The setup wizard's last step
  and the Local Server section of Desktop settings now show the address Search
  is reachable at, with a copy button - so there is a way in even when asking
  the operating system to open a browser fails.

### Changed

- **Rescans of image-heavy libraries are much faster.** Whether an image needs
  a stored thumbnail is now decided from its already-indexed dimensions.
  Previously, every image small enough to be displayed as-is was fully decoded
  again on every rescan, only to conclude there was nothing to do.
- The scan page's per-model extracted-data counts are answered by a new
  database index instead of scanning the whole data table, so they load
  quickly and no longer tax large databases on every refresh.
- The search UI no longer runs searches in the background while the pinboard
  is maximized.

### Fixed

- **Continuous scanning no longer goes silently dead when file watching
  fails.** When the OS file watcher cannot start (typically inotify limits on
  large directory trees on Linux), Panoptikon now falls back to checking for
  changes every 60 seconds and says so in the scan page's status, instead of
  reporting a healthy watcher that watches nothing. Also fixed a Linux bug
  where the watcher re-triggered itself in a tight loop, pinning a CPU core.
- **Linux Desktop (AppImage): opening things in the browser or a file manager
  works again.** Every open-in-browser, open-file, and show-in-folder action
  across Desktop did nothing on Linux: programs it launched inherited library
  paths pointing into the AppImage's transient mount and died on startup, and
  the failure was swallowed rather than reported. Desktop now hands its
  children the host environment and says what went wrong when no launcher
  works.
- Changing the results-per-page setting no longer throws you back to the first
  page - the result you were looking at stays in view.

## [v0.1.6] - 2026-07-19

### Added

- **Pinboard layout control.** Pinboards grew a full layout-editing toolkit:
  - Select multiple pins (click, marquee, Ctrl-A) and act on the selection
    through a floating toolbar of layout verbs: send to a region, shuffle,
    center, auto-layout, crop toggle, and Clear Board.
  - Per-pin position and size locks, honored by every layout verb - locked
    pins stay put while the rest of the board reflows around them.
  - Hole targeting: carve a hole in the layout and send pins into it, with
    sticky carry and shift-drag placement.
  - An auto-layout toggle directly on the Pinboard tab, and the fullscreen
    board view gained a hover-reveal toolbar.
  - Copy a pin's file path from its context menu.
- **Pinboard tab in search results.** The grid view now has a Pinboard tab
  next to Results, so you can switch between a board and your search results
  without opening the gallery view.
- **Better new-board defaults.** New pinboards start with auto-layout and
  auto-crop enabled; manually dragging or resizing a pin switches auto-layout
  off for that board (a toast tells you when it happens). Board settings are
  now saved with the board, and saving them never creates a new board
  version.
- The page-size slider is now logarithmic and goes up to 10,000 results per
  page.

### Changed

- **Search-result image loading is much faster**, especially on large grids:
  - Thumbnails and files are served with proper HTTP caching (ETag/304
    revalidation, immutable caching for content-addressed thumbnail URLs),
    so re-scrolling or re-running a search no longer re-downloads images.
  - The gateway now pools read-only database connections instead of opening
    a fresh SQLite connection for every request.
  - Bookmark status is delivered inside search responses; the grid no longer
    issues one bookmark request per visible cell.
  - File-existence checks against slow network shares are bounded by a
    timeout instead of stalling requests.
- UI stack modernized: React 19.2, Next.js 16, Tailwind CSS 4.
- Server storage stack upgraded: sqlx 0.9 with bundled SQLite 3.51.3 and
  sqlite-vec 0.1.9.

### Fixed

- **Linux Desktop (AppImage): inference setup no longer fails.** The AppImage
  runtime's `PYTHONHOME`/`PYTHONPATH` environment leaked into the bundled
  server and its Python inference workers, breaking the managed Python
  environment. Desktop and the server now scrub these variables before
  spawning Python.
- A malformed line in a `.env` file no longer prevents startup - invalid
  lines are now skipped.
- The Desktop update window header now shows the actual Panoptikon logo.

## [v0.1.5] - 2026-07-16

### Added

- **Panoptikon Desktop (experimental)** - the first release of a new desktop
  distribution for Windows (x86-64), Linux (x86-64), and macOS (Apple
  Silicon). Desktop wraps the full Panoptikon Server in a tray application:
  download one installer, run it, and Panoptikon is set up and managed for
  you - no terminal, no manual configuration files. Search still opens in
  your default browser; the desktop windows are used for setup and control
  surfaces only.
  - A guided first-run setup wizard: pick the folders you want indexed,
    choose what to extract, and start your first scan.
  - Desktop settings windows for what previously required editing TOML by
    hand: server configuration (edited losslessly, your comments and
    formatting survive), ports, file-opening behavior, and starting
    Panoptikon automatically at login.
  - **Relay** is now built into Desktop, superseding the separate
    `panoptikon-relay` project: pair Desktop with a remote Panoptikon
    instance to open files and reveal them in your local file manager
    directly from the remote search UI.
  - A comprehensive update experience with persistent update awareness,
    curated release notes, reminders, and safer installation. Desktop, its
    control UI, Relay, and the bundled Server update as one signed unit.
  - Note: installers are not yet Authenticode- or Developer-ID-signed.
    Windows shows an unknown-publisher warning and macOS requires the
    standard Gatekeeper right-click-open approval on first launch.
- Inference model registries can now declare **external inputs** (API keys
  and similar deployment values) explicitly, with labels and descriptions.
  Desktop surfaces them in its configuration UI, and newly spawned inference
  workers pick up changed values without a server restart.

### Changed
- Routine policy and proxy request log lines were downgraded from INFO to
  DEBUG, making the default server logs substantially quieter.
