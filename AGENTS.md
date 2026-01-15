Panoptikon Repository Guide

Overview
- This repo contains the core Panoptikon services and UI.
- Each component has its own README and internal agent notes where applicable.

Components
- panoptikon (Python backend): `src/panoptikon`
  - Primary API service today (FastAPI).
  - See `src/panoptikon/README.md` for details.
- inferio (Inference server): `src/inferio`
  - Optional inference service used for GPU/remote inference.
- searchui (Next.js frontend): `src/searchui/panoptikon-ui`
  - Web UI and API docs rewrites for demo mode.
- gateway (Rust HTTP entrypoint): `gateway`
  - Reverse proxy + policy enforcement, and gradually adds local API routes.
  - See `gateway/AGENTS.md` for engineering notes.
  - See `gateway/README.md` for setup and configuration.

When changes are made
- If you alter gateway behavior, update `gateway/AGENTS.md` and `gateway/README.md`.
- If you alter Python backend behavior, update `src/panoptikon/README.md`.
