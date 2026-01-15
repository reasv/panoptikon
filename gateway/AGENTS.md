Panoptikon Gateway (Rust) - Agent Notes

Purpose
- This crate is the HTTP gateway for Panoptikon.
- It is the single entrypoint for UI + API traffic and will gradually reimplement API routes locally while keeping proxy support for remote instances.

Architecture (current)
- Router: Axum routes for `/api`, `/docs`, `/openapi.json`, `/api/inference/*`, and fallback to UI.
- Proxy: `gateway/src/proxy.rs` streams requests to upstreams with minimal rewriting (forwarded headers, URI swap).
- Policy layer: `gateway/src/policy.rs` enforces host-based policy selection, rulesets, DB param rewriting, and `/api/db` response filtering across both proxied and local handlers.
- Local API: `gateway/src/api/*.rs` implements `/api/db` and `/api/items/item/file` locally when `upstreams.api.local = true`.
- Config: `gateway/src/config.rs` loads TOML + env, validates policies/rulesets, default path `config/gateway/default.toml`.

Behavior (important)
- Policy selection by effective host (`Host`, optionally forwarded headers).
- Ruleset allowlisting applies to all API surface paths (`/api/*`, `/docs`, `/openapi.json`).
- DB param enforcement:
  - Enforces `index_db` and `user_data_db` for DB-aware routes.
  - Strips DB params for `/api/inference/*`, `/api/db`, and `/api/db/create`.
  - Applies allowlists + tenant prefix rewriting via `tenant_prefix_template`.
  - Applies `tenant_default` when present; defaults must be in allowlist.
  - Sanitizes DB names + usernames; unsafe usernames are hashed.
- `/api/db` response filtering:
  - Only allowed DBs are returned.
  - Tenant-prefixed DB names are stripped before returning.
  - `current` values reflect policy defaults (tenant-aware).
- Local DB access:
  - Local handlers use a shared extractor to read `index_db`/`user_data_db` query params.
  - Read-only connections attach `storage` and `user_data` databases, mirroring Python behavior.
- Streaming:
  - All responses are streamed except `/api/db`, which is buffered so it can be filtered.

Motivations (why it is built this way)
- Keep policy enforcement in one place (layer) so local handlers can mirror upstream behavior.
- Preserve proxy compatibility even as more routes are implemented locally.
- Support multi-tenant DB selection safely and consistently across all API calls.

Tests
- Most behavior is tested in `gateway/src/policy.rs` under `mod tests`.
- When adding policy/DB rules, add unit tests there using `axum::http::Request` to validate query rewriting and response filtering.
- When adding local routes, add focused tests for handler outputs plus policy layer behavior if it transforms responses.
- All tests must include a descriptive comment above each test explaining expected behavior and outcomes.

When you change behavior
- Update this file to document new behavior, config knobs, and any new routes or policy rules.
- Keep the "Behavior" section authoritative; if behavior changes, update it.
- If the policy layer or proxy flow changes, also update `gateway/README.md`.
- Keep DB connection/helpers/CRUD code inside `gateway/src/db/`.
