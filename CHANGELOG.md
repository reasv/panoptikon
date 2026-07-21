# Changelog

All notable user-visible changes to Panoptikon are documented here. Release
sections are maintained newest first and are published verbatim as GitHub and
Desktop release notes.

## [Unreleased]

### Added

- **Desktop shows the address of the Search UI.** The setup wizard's last step
  and the Local Server section of Desktop settings now show the address Search
  is reachable at, with a copy button. Every other way out of Desktop asks the
  operating system to open a browser for you; when that fails there was nothing
  on screen to tell you that Panoptikon is a web app you can simply visit.

### Fixed

- **Linux Desktop (AppImage): opening things in the browser or a file manager
  works again.** Every button that hands something to the desktop - Open
  Panoptikon in the tray menu and the settings window, the setup wizard's Open
  Search and Open Scan, release notes, the Open Folder buttons, Relay's
  open-file action, and Open File / Show in Folder in search results - did
  nothing at all on Linux. The AppImage launcher points the library and data
  paths at its own transient mount, and every program Panoptikon started
  inherited them and died before it could draw anything; the failure was also
  swallowed rather than reported. Panoptikon now hands its own environment to
  the desktop instead of the AppImage's, and says what went wrong when no
  launcher works.

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
