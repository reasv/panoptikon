Panoptikon Repository Guide

Overview
- Panoptikon is one Rust binary (`panoptikon`) plus Python inference workers.
- The legacy Python server lives on the `python-legacy` branch, not here.
  A permanent worktree of that branch is mounted (gitignored) at
  `python-legacy/` for reference and equivalence testing.
- See `docs/architecture.md` for the charter: repository layout, configuration,
  Python environment management, and the release roadmap.

Components
- panoptikon-config (shared configuration editing crate): `panoptikon-config/`
  - Comment/order-preserving TOML diffs, environment-binding detection,
    line-preserving `.env` edits, and atomic file replacement.
- panoptikon (Rust server crate): `panoptikon/`
  - The single entrypoint: HTTP server, policy layer, full API, PQL search,
    job system, cron, file scanning, database migrations, the inference
    orchestrator, and supervision of the production web UI.
  - See `panoptikon/AGENTS.md` for engineering notes.
  - See `panoptikon/README.md` for setup and configuration.
- python (inference workers): `python/`
  - `python/inferio_worker/` — the worker harness (protocol v2), spawned by
    the orchestrator as `python -m inferio_worker`.
  - `python/inferio/` — impl classes (`impl/`) and the built-in model
    registry TOMLs (`config/`).
  - `python/pyproject.toml` + `python/uv.lock` — worker/inference deps only.
- ui (Next.js frontend): `ui/` (git submodule → panoptikon-ui)

When changes are made
- If you alter server behavior, update `panoptikon/AGENTS.md` and
  `panoptikon/README.md`.
- If you alter the worker protocol, update `docs/inferio-worker-protocol.md`.
