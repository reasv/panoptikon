# Changelog

All notable user-visible changes to Panoptikon are documented here. Release
sections are maintained newest first and are published verbatim as GitHub and
Desktop release notes.

## [Unreleased]

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
