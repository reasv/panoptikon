# Changelog

All notable user-visible changes to Panoptikon are documented here. Release
sections are maintained newest first and are published verbatim as GitHub and
Desktop release notes.

## [Unreleased]

- **Transcoded videos and clips can be copied to the clipboard instead of
  downloaded.** The video download menus (the player's top-right split button
  and the pin context menu) gained a persistent "Copy, don't download" toggle,
  shown whenever copy-as-file is available. With it on, the download button
  copies the original, and every row — Original file, Web version, and the
  clip/re-encode rows — delivers the finished file to the clipboard through
  the same paths the share button already uses: the Server writes its own
  clipboard directly when you browse from the same machine (nothing is
  transferred — the file is already there), or an approved Panoptikon Desktop
  receives the bytes otherwise. The pasted file carries its proper name, not
  the cache's internal one.
- **You can now copy an indexed file itself to the clipboard, not just its
  path.** Gallery and pinboard items gained a share button whose primary action
  is Copy when Panoptikon can reach the file and Download when it cannot; the
  gallery also accepts Ctrl+C. The right-click menu always offers the other
  verb.
  - When the browser and the Panoptikon Server run on the same computer, the
    Server writes the clipboard directly.
  - When they do not, an approved Panoptikon Desktop does it. If the file sits
    in a folder you already mapped for Relay, Desktop copies that local file
    and nothing is transferred. Otherwise the browser sends the file to Desktop,
    which keeps it in a size-capped share cache (default 5 GB, adjustable — with
    a Clear button — under Integration → Relay). A file larger than that limit
    falls back to a download instead.
  - Desktop's file-opening settings gained a third action, "Copy to Clipboard",
    with the same System default / executable / shell-command choices as Open
    File and Show in Folder. The Server has matching `clipboard_program`,
    `clipboard_args`, and `clipboard_command` keys in its `[open]` section. In a
    clipboard shell command the placeholders are quoted for you, so the command
    must not add quotes of its own.
  - On Linux the built-in copy uses `wl-copy` or `xclip` and offers the file as
    `text/uri-list`, which browsers, chat clients, Dolphin, Thunar and PCManFM
    accept. GNOME-family file managers (Nautilus, Nemo, Caja) only paste from
    their own clipboard flavour and will not accept it; a custom clipboard
    command is the way around that.
  - Endpoints paired with Relay before this release gained Copy File without
    re-approval, alongside the Open File and Show in Folder permissions they
    already had.
- **Custom shell commands on Windows now keep the quotes you wrote.** A
  file-opening or folder-revealing command such as `mytool "{path}"` previously
  reached `cmd.exe` with escaped quotes and was then split on the spaces in the
  path, so it failed for any path containing a space. The command line is now
  passed through verbatim. Commands without quotes of their own are unaffected.
- **The Desktop startup splash now uses the correct dark-background logo.**
  Its white border and wordmark no longer depend on the embedded WebView's
  appearance reporting.
- **The Desktop setup wizard no longer gets stuck on the Schedule step.**
  Disabling automatic runs allows Continue immediately, and generated daily,
  hourly, or weekly schedules no longer wait for the next-run preview. Enabled
  custom cron expressions still require successful validation; invalid custom
  drafts are never saved when automatic runs are disabled.
- **Panoptikon Desktop now stays out of the macOS Dock while running in the
  background.** Its Dock and application-switcher entry appears while a native
  setup, settings, Relay, or update window is visible, then disappears when the
  last such window closes. The menu-bar icon remains available throughout.
- **macOS now has first-class Desktop development scripts.** Developers can run
  the isolated unpackaged app, build a dev app/DMG, and safely reset its local
  profile with shell equivalents of the existing Windows workflows.

## [v0.1.8] - 2026-08-03

### Added

- **Files that fail processing are now recorded, skipped, and healed.**
  Previously a broken file failed again on every job and every rescan,
  forever, with nothing to show for it. Now:
  - Failures are classified (bad input, missing dependency, resource limit,
    or transient) and recorded per file and model. Persistently failing
    files are skipped on later runs after at most two attempts.
  - The scan page gained a failed-files card: browse recorded failures,
    hover for the full error text, click to copy file paths. Job logs count
    input errors separately from real job failures.
  - One bad file no longer poisons its whole batch: inference workers
    report per-item errors, and an implicated item is retried alone so the
    rest of the batch survives.
  - A job whose only failures are bad input files completes with a warning
    instead of reporting failure.
  - Images whose full decode fails are still indexed (searchable by name,
    path, and metadata, just without thumbnails) - only files whose basic
    metadata cannot be read are excluded, matching how broken videos and
    PDFs already behaved. Broken visuals are remembered, so rescans stop
    re-decoding the same broken files every time.
  - Failures caused by a missing dependency (for example no usable pdfium)
    heal automatically once the dependency is available, and upgrades can
    ship targeted retries so files that failed due to a now-fixed bug are
    re-attempted - never a blanket retry of everything.
  - AppleDouble sidecar files and HTML error pages saved with a media
    extension are identified by content when they fail, so they are
    skipped after a single attempt.
- **Search inside your pinboards.**
  - A new Library tab in the grid view runs your current search against
    your pinboards: it lists the boards containing matching images, ordered
    by where their best match ranks in the results, with match counts.
  - A new sidebar filter restricts search results by pinboard membership:
    pinned to any board, pinned to specific boards, or not pinned anywhere -
    that last one is made for finding matches you haven't curated yet.
  - PQL gained an `in_pinboard` filter (composable with NOT), and access
    policies gained a `pinboard_search` read capability gating the new
    endpoint.
- **Pinboard pins can be rotated, flipped, and bulk-edited.**
  - Rotate 90° left/right and flip horizontally/vertically, per pin or for
    a whole selection. Crops are preserved exactly: the content you framed
    stays framed, rotated along with the image.
  - Compress Left/Right/Up shrink letterboxed pins in the chosen direction,
    cascading so the spacing between pins is preserved.
  - New removal verbs: Remove Selected (also bound to the Delete key),
    Remove All but Selected, and Remove Items Below Viewport. No
    confirmation dialogs - a toast offers undo, and the browser Back button
    restores the board.
  - The selection toolbar moves below the selection when it would otherwise
    cover the controls of pins at the top edge.
- **Pinboard layout modes and export.**
  - A gravity toggle per board: turn off the automatic upward packing so
    pins stay exactly where you place them. On gravity-off boards, layout
    tools resolve any overlaps they would create by nudging pins downward
    instead of repacking the whole board.
  - A proportional layout mode that scales the entire arrangement with the
    window width, so a board composed at one size keeps its proportions at
    any other - instead of fixed-size cells reflowing.
  - Export a board as a single JPEG mosaic image, either seamless (gaps
    closed up) or reproducing the board's visible extent as arranged.
  - A per-board toggle to show resize handles on every edge and corner of
    each pin, not just the bottom-right (boards with gravity on omit the
    top handles, which cannot work under automatic packing).
  - Newly pinned items are placed row by row into the first free spot near
    the bottom of the board, instead of always starting a new row.
  - Pinboard preview images are sharper (the stored master doubled to
    2048px), and a Refresh Preview action re-renders a board's preview on
    demand.
- **The pinboard library sorts by activity.** Boards you recently opened
  float to the top, followed by a blend of how often and how recently each
  board is used; a toggle restores the old last-updated order. There is
  also a new "Open maximized in new tabs" option, and board links opened in
  new tabs get clean URLs instead of inheriting the whole search state.
- **New embedding models**: Meta's PE-Core-L14-336 and PE-Core-bigG-14-448,
  SigLIP2 B/16-384, and NVIDIA's llama-nemotron-embed-vl-1b (a
  visual-document retrieval model, strongest on screenshots and documents;
  registered for image embedding with plain text queries). Model
  descriptions now state the measured VRAM each model needs.
- **Support for older NVIDIA GPUs.** Models that hardcoded bf16 now
  negotiate their dtype from the GPU's actual capability at load time, so
  pre-Ampere cards get fp32 instead of crashes or garbage output (an
  explicit dtype in your config still wins). Whisper picks a compute type
  the device supports. Models with a hard hardware floor (dots.ocr needs
  compute capability 8.0) are marked unavailable in the scan UI with the
  reason, and refuse to start instead of failing mid-job - they stay
  selectable so Delete Data still works after a hardware downgrade. GPU
  out-of-memory during a batch is retried with halved batches; an item
  that cannot fit even alone fails alone, without sinking the job.
- The AMD inference stack moved to ROCm 7.2 multi-arch torch builds, with
  the worker HIP environment derived from the actual install and MIOpen
  auto-tuning enabled.
- **Nix packaging**: a Nix flake and NixOS module under `contrib/`,
  installable from release tags.
- The server logs the resolved inference accelerator (cpu/cuda/rocm) and
  detected GPUs at startup, and a new `panoptikon accelerator` subcommand
  prints the same report on demand.

### Changed

- **Vector quantization moved from binary to int8 and is now on by
  default.** The experimental binary quantization from v0.1.7 is retired:
  quant profiles now store int8 codes (about a quarter of full-precision
  size) whose results are nearly indistinguishable from exact search
  (identical top-10, top-100 overlap above 96% in evaluation) while being
  measurably faster - composed default searches ran 1.4-1.8x faster end to
  end on real databases. Because the ordering now matches exact search,
  semantic searches use the quantized path automatically whenever the
  default profile's coverage is ready, falling back to exact scoring
  otherwise. Existing configs are migrated on upgrade and the quant table
  is rebuilt automatically. The `k` argument on vector filters is
  deprecated and ignored, and its control was removed from the search UI.
- **Database maintenance now runs once per batch of jobs.** Checkpoints,
  ANALYZE, vacuum checks, and the tag recount used to run after every
  single job; now jobs report what they changed and maintenance runs when
  a database's queue empties, as a visible, cancellable Database
  Maintenance job. Models also stay loaded across consecutive jobs that
  use them, and cron schedules from multiple databases are merged so jobs
  sharing a model run back to back instead of reloading it per database.
  Cancel Selected gained a skip-maintenance option, and a new Database
  Maintenance card on the scan page runs maintenance on demand.
- **Tag autocomplete is ranked and fast.** Suggestions are ordered by how
  many results each tag would actually return, using a per-tag count kept
  up to date by post-job maintenance - one-letter queries on a database
  with 22 million tag rows went from ~600 ms to ~2 ms, and the suggestions
  are the most useful matches instead of an arbitrary sample. The top-tags
  API endpoint, previously unusable on any sizeable database (10+
  minutes), now answers in ~150 ms.
- **CLIP image embedding defaults to FP16**: about half the VRAM and ~6x
  the throughput, with retrieval quality verified unchanged.
- **Scans now skip hidden and macOS junk directories.** Directories whose
  name starts with `.`, and `__MACOSX` directories, are no longer descended
  into below a scan root (a root you explicitly add still scans even if its
  own name starts with a dot). Content previously indexed under such
  directories is retired like any other removed file.
- Composed semantic searches evaluate exact distances once, in a
  materialized subquery, instead of repeatedly inside the result sorter,
  and several prefix lookups (file hashes, tag namespaces, scan roots) now
  use indexes instead of scanning.

### Fixed

- **Runaway WAL growth during large jobs.** A 1.2M-item tagging job was
  reported growing the index write-ahead log to 33 GB, with writes slowing
  to minutes as it grew. The extraction driver held a single job-long read
  snapshot that no checkpoint could pass; it now drains its work queue in
  chunks. Additionally, the WAL is capped and truncated by a checkpoint
  after each job.
- **Slow searches after upgrading.** Database migrations that change the
  schema now refresh the query planner's statistics; previously an
  upgraded database could keep pathological query plans (minutes-long
  full-text searches) until the next maintenance run.
- **Corrupt audio no longer sinks embedding jobs.** An audio stream whose
  metadata lies about its duration could decode to gigabytes, crash the
  inference worker, and fail its job on every run. Audio embedding models
  now cap decode duration (they only consume the first seconds anyway),
  batches are admitted by payload size with a raised 2 GiB transfer cap,
  and a file that still exceeds the limits lands in the failed-files
  ledger while the job completes.
- **Pinboard editing gestures.** Cropping the lowest pin no longer shears
  the image out of the crop window mid-drag; drag and resize gestures
  freeze the board's scroll range so small movements are no longer
  amplified by the board scrolling underneath the pointer; releasing a
  shrink glides smoothly instead of snapping; dragging or growing a pin
  past the bottom edge auto-scrolls the board; and layout verbs invoked
  while crop mode is open no longer commit overlapping layouts.
- **Bookmark and pinboard saves are more robust under concurrent writes.**
  Two regressions from the Rust port were fixed: saves could fail
  instantly instead of waiting their turn when another write was in
  flight, and saving user data briefly write-locked the whole index.
- File-scan jobs configured with bookmark or pinboard filters now return a
  clear error explaining those filters cannot apply to file scans, instead
  of failing with an internal "no such table" error.
- Tag autocomplete treats `_` and `%` in the typed query literally instead
  of as wildcards.
- Configured pdfium, HTML-renderer, and font paths that fail to resolve
  are reported with a warning at startup instead of being silently
  ignored.

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
