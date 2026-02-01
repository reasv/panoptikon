Panoptikon Gateway (Rust) - Agent Notes

Purpose

- This crate is the HTTP gateway for Panoptikon.
- It is the single entrypoint for UI + API traffic and will gradually reimplement API routes locally while keeping proxy support for remote instances.

Architecture (current)

- Router: Axum routes for `/api`, `/docs`, `/openapi.json`, `/api/inference/*`, and fallback to UI.
- Proxy: `gateway/src/proxy.rs` streams requests to upstreams with minimal rewriting (forwarded headers, URI swap).
- Policy layer: `gateway/src/policy.rs` enforces host-based policy selection, rulesets, DB param rewriting, and `/api/db` response filtering across both proxied and local handlers.
- Local API: `gateway/src/api/*.rs` implements `/api/db`, `/api/db/create` (only when `EXPERIMENTAL_RUST_DB_CREATION` is set), `/api/bookmarks/ns`, `/api/bookmarks/users`, `/api/bookmarks/ns/{namespace}`, `/api/bookmarks/ns/{namespace}/{sha256}`, `/api/bookmarks/item/{sha256}`, `/api/items/item`, `/api/items/item/file`, `/api/items/item/thumbnail`, `/api/items/item/text`, `/api/items/item/tags`, `/api/items/text/any`, `/api/open/file/{sha256}`, `/api/open/folder/{sha256}`, `/api/search/pql`, `/api/search/pql/build`, `/api/search/embeddings/cache`, `/api/search/tags`, `/api/search/tags/top`, and `/api/search/stats` locally when `upstreams.api.local = true`. `/openapi.json` is served locally when `upstreams.api.local = true`. `/api/jobs/*` is only local when `upstreams.api.local = true` and `EXPERIMENTAL_RUST_JOBS` is truthy.
- Config: `gateway/src/config.rs` loads TOML + env, validates policies/rulesets, default path `config/gateway/default.toml`.

Behavior (important)

- Policy selection by effective host (`Host`, optionally forwarded headers).
- Ruleset allowlisting applies to all API surface paths (`/api/*`, `/docs`, `/openapi.json`).
- `.env` is loaded at startup (if present) so env-based config can be set via dotenv files.
- On Windows, the gateway sets the executable stack size via linker flags
  (`/STACK:8388608` for MSVC, `--stack,8388608` for GNU) to avoid startup stack
  overflows on the default main thread stack; Tokio worker threads are also
  configured with 8MB stacks.
- `EXPERIMENTAL_RUST_DB_CREATION` is treated as truthy only for `1`, `true`, `yes`, or `on` (case-insensitive).
- `EXPERIMENTAL_RUST_DB_AUTO_MIGRATIONS` runs migrations across all on-disk DBs in `DATA_FOLDER` and baselines Python-created DBs when truthy.
- `EXPERIMENTAL_RUST_JOBS` gates local `/api/jobs/*` endpoints; when false, job routes are proxied to the Python backend.
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
  - User-data write handlers open a read-write connection so bookmarks can be updated.
  - Index DB write connections (writer actors) attach `storage` only; `user_data` is not attached for write transactions.
  - SQLite extensions (sqlite-vec) are registered via the bundled Rust crate.
- Index DB writer actors:
  - All Index DB writes are serialized through a per-DB writer actor.
  - Writers open short-lived write transactions and keep a cached connection with a 5-minute idle timeout.
  - A supervisor actor spawns writers on demand and runs 5-minute health checks (index/storage file existence + index/storage read-only ping).
- Job system:
  - `/api/jobs/*` is implemented locally only when `upstreams.api.local = true` and `EXPERIMENTAL_RUST_JOBS` is truthy.
  - A global `JobQueueActor` keeps an in-memory queue and running job state; a `JobRunnerActor` executes one job at a time.
  - File scan jobs (`folder_rescan`, `folder_update`) run through `FileScanService` and the index writer actor for writes.
  - File scan jobs honor `filescan_filter` (PQL `Match`) during stage-1/2 file filtering and apply `job_filters` entries that include `file_scan` after scans to delete files that violate the rules.
  - Queue status mirrors Python: running job is listed first with `running=true`, followed by queued jobs.
  - Queue cancel can target queued jobs and the running job (best-effort cancellation).
  - Manual cronjob trigger enqueues configured cron jobs from the system config.
  - System config parses `job_filters` and `filescan_filter` as PQL objects; invalid PQL in config fails to load (mirrors Python).
- Local DB migrations:
  - SQLx migrations live in `gateway/migrations/index`, `gateway/migrations/storage`, and `gateway/migrations/user_data`.
  - `db::migrations::migrate_databases` can create or update on-disk DBs and supports in-memory DBs for tests.
  - Existing Python-created DBs without `_sqlx_migrations` are baselined to the first migration so future migrations can apply.
- Local PQL search:
  - `/api/search/pql` compiles queries via the Rust PQL builder and executes them locally.
  - `/api/search/pql/build` returns the compiled SQL/params without executing.
  - Extra columns use the Rust alias map, and `check_path` results are validated with fallback file lookup.
  - When `check_path` is enabled for `entity = file` and no `partition_by`, missing paths are dropped without substitution (matching Python behavior).
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

PQL Rewrite (Rust, Planned)

- Goal: fully replace the Python PQL compiler with a Rust implementation that is behaviorally identical for both results and performance-critical SQL structure.
- Rollout: gated by an explicit experimental env flag; when enabled, Rust PQL is the only path (no proxy fallback, no shadow mode).
- OpenAPI: PQL types are annotated for OpenAPI generation from the start; when `upstreams.api.local = true`, `/openapi.json` is served from the Rust generator even though some endpoints are still proxied.
- Architecture:
  - Schema/AST: `serde` models mirror the Pydantic union shapes and field names (`and_`, `or_`, `not_`, filter fields).
  - Preprocess/validation: matches Python behavior exactly, including filter-specific mutations (e.g., `MatchText.filter_only`).
  - Builder: SeaQuery-based query builder replicates `QueryState`, CTE chaining, root CTE unwrapping, join ordering rules, `order_by` + `partition_by`, and extra-column handling.
  - Join tracking: filters record which base tables they already join so root CTE unwrapping does not introduce duplicate base-table joins (avoids ambiguous column errors).
  - Count queries: preserve count semantics (including partition-by counting and ignoring gt/lt cursor filters).
  - SQLite specifics: FTS5 `MATCH`, `snippet(...)`, and vector functions are emitted as raw SQL fragments where needed.
- Initial filter subset (fully working core):
  - `Match`, `MatchPath`, `MatchText`, `MatchTags`, `InBookmarks`, `ProcessedBy`, `HasUnprocessedData`.
  - Embedding filters (`SemanticTextSearch`, `SemanticImageSearch`, `SimilarTo`) are implemented and require async preprocessing with the inference API for embeddings + distance function overrides.
- Implementation status:
  - `Match` is implemented with KV joins + recursive operator handling (eq/neq/in/nin/gt/gte/lt/lte/startswith/endswith/contains, plus nested and/or/not).
  - `MatchPath` is implemented with FTS5 `MATCH`, `rank`-based `order_rank`, `row_n` windowing, and `gt`/`lt` cursor filtering.
  - `MatchText` is implemented with FTS5 `MATCH`, setter/language/confidence filters, snippet extraction, and `row_n` windowing for best snippet selection.
  - `MatchTags` is implemented with tag/name/namespace filters, setters, confidence thresholds, and exact/all-setter matching via HAVING clauses.
  - `InBookmarks` is implemented with user + namespace filtering (including sub-namespaces) and ordering by latest bookmark timestamp.
  - `ProcessedBy` is implemented with setter filtering over derived data per item/data row.
  - `HasUnprocessedData` is implemented with derived-data `NOT EXISTS` checks and placeholder filtering.
  - `SemanticTextSearch` is implemented with embeddings distance aggregation (MIN/MAX/AVG), optional source-text filters + weights, and per-entity join paths.
  - `SemanticImageSearch` is implemented with CLIP cross-modal support, source-text filters, and model distance-function overrides.
  - `SimilarTo` is implemented with an `unqemb` CTE, cross-modal constraints, and weighted distance aggregation when source-text weights are provided.
  - `preprocess_query_async` embeds queries via the inference upstream and loads model metadata for distance-function overrides; the sync preprocessor accepts base64 embeddings or prefilled `_embedding` fields.
  - Inference metadata is cached per inference base URL (5-minute TTL) to avoid repeated `/metadata` calls during preprocessing.
    - Callers that need fresh metadata can construct the client with caching disabled (`InferenceApiClient::from_settings_with_metadata_cache(..., false)`).
- Search-time embeddings are cached in-process with a global LRU keyed by `(model, kind, query)`; cache size is controlled by `search.embedding_cache_size` in gateway config.
  - `/api/search/embeddings/cache` provides cache stats and allows clearing the embedding cache.
  - Embedding decoding accepts `f16/f32/f64`, integer/boolean dtypes, and both C/Fortran order; non-float inputs are coerced to `f32` and 2-D arrays use the first row.
  - Inference predict calls (multipart uploads) bypass the retry middleware and use a raw reqwest client with manual retry logic because multipart bodies are not clonable.
  - Inference embed/metadata errors are sanitized in client responses; detailed error context is logged server-side.
  - `/api/search/pql` and `/api/search/pql/build` now use the Rust compiler; no Python PQL calls remain in the gateway.
  - Test strategy (results + performance invariants):
    - Use Python `/api/search/pql/build` as the reference compiler for fixtures during development when needed.
    - Validate result equivalence and ordering on a fixed SQLite fixture DB.
    - Per-filter unit tests build a full PQL query and execute it against in-memory test databases to ensure the generated SQL is valid for our schema.
    - Validate SQL structure without relying on byte-for-byte SQL equality:
      - Normalize SQL (whitespace/casing) and compare key structural properties (CTE ordering, join graph, selected columns).
      - Track query plans as a diagnostic signal; do not rely on plan output alone, but use it to spot regressions in join/index usage.
  - Maintain a golden fixture suite covering all implemented filters, text vs file entities, partitioning, and ordering edge cases.

Continuous File Scanning (Implemented)

- Scope: optional continuous file scanning per index DB, controlled by `continuous_filescan = true` in the per-DB TOML SystemConfig. This feature is not part of the job queue.
- Actor topology:
  - `ContinuousScanSupervisor` (singleton actor) maintains `index_db -> ActorRef<ContinuousScanActor>`.
  - One `ContinuousScanActor` per index DB when enabled in config.
  - A ractor factory (per DB) runs per-file processing workers; DB writes still go through the index DB writer actor (serialized).
- Startup + discovery:
  - On startup, supervisor enumerates DBs in `DATA_FOLDER`, loads each config, and spawns per-DB actors when enabled.
  - Supervisor watches `DATA_FOLDER/index` for FS changes to react to DB additions/removals and config edits.
  - The config update API notifies the supervisor directly on changes (fast-path).
- Pause/resume semantics (no job queue coupling, but reactive):
  - Continuous scan runs concurrently with data extraction jobs and file scans on other DBs.
  - It pauses when a `folder_rescan`/`folder_update` starts on the same DB.
  - Job runner pauses before starting a file scan job and resumes after completion (if still enabled).
- Epoch gating (write safety guarantee):
  - Each `ContinuousScanActor` tracks `epoch: u64`, `paused: bool`, and `paused_for_job`.
  - Every worker task is dispatched with the current epoch.
  - Before any DB write, the actor checks `paused == false` and `task_epoch == current_epoch`.
  - Pause increments `epoch` and sets `paused = true`, so stale results are dropped without writing.
  - This guarantees no file/folder writes after a file scan job starts, without blocking on in-flight tasks.
- File scan rows:
  - Each continuous run creates a new `file_scans` row (path sentinel `"<continuous>"`).
  - On pause/stop/config disable, close the row (`end_time = now`) and do not reuse it.
  - On resume, create a new row with a new `scan_id`.
  - On startup, any open continuous scan row is closed before creating a new one.
- Allowed operations:
  - Continuous scan can add new files/items and delete files it created in the current continuous session after verifying they are truly missing.
  - No content-update semantics: file content change == delete old file row + create new file row (new hash -> new item).
  - Items with no files are always deleted. If an item should survive, its file should not be deleted.
  - No mark-unavailable + sweep deletes; those remain full scan responsibilities.
- Deletion policy (conservative):
  - Never delete on fs event alone; always verify on disk.
  - Safe deletes:
    - Files created in the current continuous scan (`scan_id == current_scan_id`).
    - Duplicate files for an item (other file rows exist).
  - Otherwise, avoid destructive deletes and defer to full scan.
- Move handling:
  - Rename events (old_path -> new_path) are safe to apply because they do not delete items; treat them as a path update on the file row.
  - If a move appears as delete+create (no rename event), process it directly as delete+create.
- Cross-platform file watching:
  - Use `notify` with native backends (Windows/macOS/Linux).
  - Optional polling mode when `continuous_filescan_poll_interval_secs` is set (uses `notify::PollWatcher`).
  - Watcher overflow logs a warning (index_db + watched roots); no automatic recovery action.
  - For unreliable shares (SMB/NFS), add an explicit config opt-in to use `notify::PollWatcher` with a configurable interval (e.g., `continuous_filescan_poll_interval_secs`); default remains native watchers.
