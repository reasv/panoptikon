# Panoptikon Gateway (Rust)

This crate is the single HTTP entrypoint for Panoptikon. It forwards requests
to the Next.js UI or the Python API based on path, with streaming
request/response bodies, policy enforcement, structured logging, and optional
local API handling when `upstreams.api.local = true`.

## Where it fits

Panoptikon currently runs:

- Next.js frontend (dev server on `http://127.0.0.1:6339`)
- Python backend (FastAPI on `http://127.0.0.1:6342`)

The gateway sits in front of both, so the browser only talks to the Rust
process. This keeps the entrypoint stable as more native Rust endpoints are
added later.

## Routing

- `/api/*` goes to the Python backend, except local routes when enabled
- `/api/inference/*` goes to the first inference upstream (defaults to the API
  upstream) — or is served in-process by the Rust inferio orchestrator when
  `[inference_local].enabled = true` (see below)
- `/docs` goes to the Python backend unless `upstreams.api.local = true`
- `/redoc` goes to the Python backend unless `upstreams.api.local = true`
- `/openapi.json` goes to the Python backend unless `upstreams.api.local = true`
- everything else goes to the Next.js frontend

Paths, methods, headers, and bodies are forwarded as-is.

## Policy enforcement

Policies are selected by host, then optionally restrict API routes via reusable
rulesets. DB-aware API routes always receive explicit `index_db` and
`user_data_db` query parameters, and the gateway validates or rewrites them per
policy (including optional multi-tenant prefixing).
Local user-data write endpoints open a read-write connection to allow bookmark
updates while still reading from the index database.
When local endpoints need vector search, the gateway registers the sqlite-vec
extension via the bundled Rust crate.

Special handling:

- `/api/db` is filtered so only allowed DB names are returned; tenant-prefixed
  DBs are reported without the prefix, and `current` defaults reflect the policy.
- `/api/db/create` uses `new_index_db` and `new_user_data_db` with the same
  enforcement rules as normal DB parameters.
- `/api/inference/*` never receives DB query parameters.
- When `upstreams.api.local = true`, the gateway serves `/api/db`,
  `/api/db/create`,
  `/api/bookmarks/ns`, `/api/bookmarks/users`,
  `/api/bookmarks/ns/{namespace}`, `/api/bookmarks/ns/{namespace}/{sha256}`,
  `/api/bookmarks/item/{sha256}`, `/api/items/item`, `/api/items/item/file`,
  `/api/items/item/thumbnail`, `/api/items/item/text`, `/api/items/item/tags`,
  `/api/items/text/any`, `/api/open/file/{sha256}`, `/api/open/folder/{sha256}`,
  `/api/search/pql`, `/api/search/pql/build`,
  `/api/search/embeddings/cache`,
  `/api/search/tags`,
  `/api/search/tags/top`, and `/api/search/stats`
  locally using the same policy enforcement and filtering rules, and serves
  `/openapi.json`, `/docs`, and `/redoc` from the local OpenAPI generator.
  `/api/search/pql` compiles queries via the Rust PQL builder and executes them
  locally; `/api/search/pql/build` returns the compiled SQL/params without
  executing. The Rust PQL compiler (SeaQuery) mirrors the Python
  implementation, including embedding filters and async preprocessing that can
  call the inference upstream and parse `.npy`/JSON embeddings (including
  `f16/f32/f64`, integer/bool dtypes, and Fortran-ordered arrays). It caches
  inference metadata lookups for 5 minutes to reduce repeated metadata calls
  while applying distance-function overrides. Multipart inference predict calls
  bypass the retry middleware and use a raw reqwest client with manual retry
  logic because multipart bodies are not clonable. Inference errors are sanitized
  in client responses while detailed error context is logged. Search-time embeddings are cached
  in-process with a global LRU keyed by `(model, kind, query)`
  using `search.embedding_cache_size` from the gateway config. It tracks joined base tables to
  avoid duplicate joins when the root CTE is unwrapped. When `check_path` is
  enabled for `entity = file` with no `partition_by`, missing paths are dropped
  instead of substituting a different file (matching Python behavior).

## Database migrations

The gateway tracks three SQLite schemas (index, storage, user_data) using SQLx
migrations stored in `gateway/migrations/index`, `gateway/migrations/storage`,
and `gateway/migrations/user_data`. The initial migrations mirror the schema
dumps produced by the Python backend, with `BEGIN`/`COMMIT` stripped to avoid
nested transactions under SQLx.

## Index DB write actors

Index DB writes for long-running jobs are serialized through a per-index DB
writer actor, supervised by a registry actor that spawns writers on demand.
Writers keep a cached write connection with a 5-minute idle timeout, and the
supervisor runs 5-minute health checks that verify `index.db`/`storage.db`
existence plus a read-only ping against `index`/`storage` only.
Writer connections attach `index` + `storage` only; `user_data` is not attached
for write transactions.

## Job system

When `upstreams.api.local = true`, `/api/jobs/*` is implemented locally and
the cron scheduler runs in the gateway. Local mode means the gateway owns the
databases outright: do not run the Python server's cron (or the Python server
at all) against the same `DATA_FOLDER`, or extraction jobs will be scheduled
twice. A global job-queue actor holds the in-memory queue and running job
state, and a job-runner actor executes one job at a time. File scan jobs
(`folder_rescan`, `folder_update`) run through `FileScanService`, which writes
via the index DB writer actor. Queue status mirrors Python semantics (running
job first, then queued jobs), and queued/running jobs can be cancelled via the
jobs API.
File scan jobs honor the `filescan_filter` (PQL `Match`) during stage-1/2
filtering, and apply `job_filters` entries that include `file_scan` after
scans to delete files that violate those rules.
The system config now parses `job_filters`/`filescan_filter` as PQL objects;
invalid PQL in config will fail to load (matching Python behavior).

Continuous file scanning is independent of the job queue and is controlled per
index DB via the system config `[continuous_filescan]` section. A supervisor
actor spawns one continuous scan actor per enabled DB. Each actor creates a
`file_scans` row with path `"<continuous>"` while active, uses notify-based
watchers to react to filesystem changes, and writes through the index DB writer
actor. Continuous scanning pauses automatically when a full rescan or folder
update job starts on the same DB and resumes afterward. If
`[continuous_filescan].included_folders` is set, watcher roots are limited to
those paths; they must be under the DB’s global `included_folders` and not under
`excluded_folders`, otherwise continuous scanning is disabled for that DB until
fixed. To force polling (e.g., for unreliable shares), set
`[continuous_filescan].poll_interval_secs` to a number of seconds (uses
`notify::PollWatcher` instead of native watchers). There is no native watcher
exclude support, so continuous-scan excludes are not implemented.

## Local inference (inferio orchestrator)

With `[inference_local].enabled = true` the gateway serves `/api/inference/*`
itself: it parses the inference TOML registry, spawns Python worker processes
(`python -m inferio_worker`) on demand, manages the LRU/TTL model caches, and
batches concurrent predict requests at dispatch time. The HTTP surface is
wire-compatible with the Python inference server (multipart predict requests,
octet-stream / multipart-mixed / JSON responses, `/metadata` with mtime-gated
registry reload), so the web UI and all gateway-internal clients work
unchanged. The routes stay behind the policy layer, which strips DB query
parameters for inference paths just as it did for the proxy.

`GET /api/inference/health` (additive; no Python counterpart) reports
orchestrator observability: top-level `status` ("ok"/"shutting_down"),
`shutting_down`, `registry_ok`, `model_count`, and a `models` array with, per
loaded model, `inference_id`, `generation`, `cache_keys`, `replicas
{total, free}`, `queue_depth`, `in_flight_windows`, `last_effective_cap`
(null until the first window dispatches), `total_predict_requests`, and
`total_batches`, plus a `prewarm` section `{enabled, lazy, warm:
[{impl_class, state}]}` where `state` is `"warm"`, `"spawning"`, or
`"failed_prepare"`. When local inference is disabled the path proxies
upstream like any other inference route (a Python upstream 404s it).

### Prewarming

Process start and heavy library imports dominate model load latency, so the
orchestrator keeps one *prewarmed* worker per impl class: spawned,
handshaken, and with the impl's optional `prepare()` classmethod already run
(imports only — no weights, no VRAM). Loading any model of that class claims
the parked worker (skipping the slow part); the pool never expires by design
— it exists precisely for the moment after the loaded model's TTL has passed.
Three mechanisms fill the pool:

- **Eager** (gateway mode): at startup and every minute, the search-usable
  embedding models with data across your index DBs (the same selection as
  `preload_embedding_models`) are mapped to impl classes and kept warm, so
  the first embedding search after a restart is fast. Opt a DB out by
  setting `prewarm_embedding_models = false` in that DB's `config.toml`.
- **Lazy**: after any model load, one warm worker of its class is kept for
  next time. Extraction jobs opt out via the additive `prewarm=false` query
  param on load/predict (absent = true; old Python servers ignore it).
- **`always_warm`**: impl classes warmed unconditionally at startup — the
  only eager mechanism in the `inferio` subcommand, which scans no DBs.

When `upstreams.inference` is empty and local inference is enabled, the
gateway synthesizes a loopback entry pointing at itself so search, jobs, and
cron preload work with zero extra config. Explicitly configured endpoints are
left untouched — you can mix the local orchestrator with remote inference
servers; entry order still decides which endpoint serves search/metadata and
which ones take jobs.

```toml
[inference_local]
enabled = true
# All optional:
# python = ".venv/Scripts/python.exe"  # default: auto-detect the repo venv
# impl_dirs = ["src/inferio/impl", "inferio_custom"]
# config_dirs = ["src/inferio/config", "config/inference"]
# pythonpath = ["src"]
# default_max_batch = 32
# sweep_interval_secs = 10
# handshake_secs = 30
# load_secs = 600
# unload_grace_secs = 10
# terminate_grace_secs = 5
# port = 7777        # `inferio` subcommand listen port (default: server.port)

# [inference_local.prewarm]
# enabled = true          # master switch for the warm-worker pool
# lazy = true             # keep one warm worker per class after each load
# always_warm = []        # impl classes warmed unconditionally at startup
```

A machine that only lends its GPU can run the standalone service:

```bash
cargo run -p gateway -- inferio --config config\gateway\default.toml
```

This starts only `/api/inference/*` plus `GET /health` — no proxy, local API,
jobs, cron, or migrations. Bare `/health` serves the same health report as
`/api/inference/health` (which is also available here). Point other
panoptikon instances at it with an `[[upstreams.inference]]` entry.

On shutdown, local inference workers are stopped via the graceful
unload → terminate → kill ladder after the job queue stops and before the
index writers flush; workers are additionally covered by a kill-on-close Job
Object so a forced exit cannot leak processes.

To add migrations, use SQLx's CLI against the appropriate source directory:

```bash
sqlx migrate add --source gateway/migrations/index add_new_table
```

Programmatic creation and migration lives in `gateway/src/db/migrations.rs`
(`migrate_databases`) and supports both on-disk databases and shared
in-memory databases for tests.

When `upstreams.api.local = true` (and `READONLY` is not set), the gateway
runs migrations across every on-disk DB in `DATA_FOLDER` at startup, like the
Python server does. Existing Python-created DBs are baselined: the init
snapshot is recorded as applied without executing any SQL — the only write is
the `_sqlx_migrations` bookkeeping table. Baselining requires the DB to be at
the exact alembic head the snapshot was taken from; anything older (or a
non-empty DB with no `alembic_version` at all) makes startup fail with an
explicit error instead of silently assuming the wrong schema. Freshly created
DBs get the alembic head revision stamped into `alembic_version` so they
remain manageable by the Python server during the transition.

## Configuration

Configuration is TOML and/or environment variables only. The gateway loads
`.env` at startup when present, then reads `[cwd]/config/gateway/default.toml`.
Override the path with `--config` or the `GATEWAY_CONFIG_PATH` environment
variable.

Create `config/gateway/default.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080
trust_forwarded_headers = false

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
local = false

# Inference upstreams (first entry is used for search + metadata + /api/inference proxy).
# Omit entirely when [inference_local] is enabled: a loopback self entry is
# synthesized automatically.
[[upstreams.inference]]
base_url = "http://127.0.0.1:6342"
weight = 1.0
use_for_jobs = true

# Serve /api/inference/* in-process instead of proxying (Rust inferio
# orchestrator; see "Local inference" above).
[inference_local]
enabled = false

[search]
embedding_cache_size = 16

[rulesets.allow_all]
allow_all = true

[[policies]]
name = "desktop"
ruleset = "allow_all"

[policies.match]
hosts = ["localhost", "127.0.0.1", "panoptikon.local"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"
```

Environment variables override the file:

```bash
GATEWAY_CONFIG_PATH=config\gateway\default.toml
GATEWAY__SERVER_HOST=0.0.0.0
GATEWAY__SERVER_PORT=8080
GATEWAY__SERVER_TRUST_FORWARDED_HEADERS=false
GATEWAY__UPSTREAM_UI=http://127.0.0.1:6339
GATEWAY__UPSTREAM_API=http://127.0.0.1:6342
GATEWAY__UPSTREAM_API_LOCAL=false
GATEWAY__SEARCH__EMBEDDING_CACHE_SIZE=16
```

CLI override example:

```bash
cargo run -p gateway -- --config config\gateway\userconfig.toml
```

## Logging and shutdown

Log output goes to the console and, by default, to
`$DATA_FOLDER/panoptikon-gateway.log` (append; the name is distinct from the
Python server's `panoptikon.log` so the two processes never interleave one
file). `LOGS_FILE` overrides the path; setting it to an empty string disables
file logging. `LOGLEVEL` sets the default level (same variable the Python
server uses); `RUST_LOG` takes precedence when set and supports per-module
directives.

On SIGINT/SIGTERM (Ctrl-C, `docker stop`, systemd) the gateway shuts down
gracefully: it stops accepting connections, drains in-flight requests, stops
the cron scheduler and continuous scan actors, cancels the running job (same
path as `POST /api/jobs/cancel`), and flushes the index DB writers so every
queued write commits. Cleanup is bounded by a 10s grace period and a 20s hard
deadline; a second signal exits immediately. Anything cut off is a single
SQLite transaction, which rolls back on next open.

The nested style supported by the config crate also works:

```bash
GATEWAY__SERVER__HOST=0.0.0.0
GATEWAY__SERVER__PORT=8080
GATEWAY__SERVER__TRUST_FORWARDED_HEADERS=false
GATEWAY__UPSTREAMS__UI__BASE_URL=http://127.0.0.1:6339
GATEWAY__UPSTREAMS__API__BASE_URL=http://127.0.0.1:6342
GATEWAY__UPSTREAMS__API__LOCAL=false
GATEWAY__UPSTREAMS__INFERENCE__0__BASE_URL=http://127.0.0.1:6342
GATEWAY__UPSTREAMS__INFERENCE__0__WEIGHT=1.0
GATEWAY__UPSTREAMS__INFERENCE__0__USE_FOR_JOBS=true
GATEWAY__SEARCH__EMBEDDING_CACHE_SIZE=16
```

## Running locally

From the repo root:

```bash
cargo run -p gateway
```

Windows note: the workspace sets the executable stack size via
`.cargo/config.toml` (MSVC: `/STACK:8388608`, GNU: `--stack,8388608`) to avoid
startup stack overflows on the default Windows main-thread stack. Tokio worker
threads are still configured with 8MB stacks in code.

Then visit:

- `http://localhost:8080` for the UI
- `http://localhost:8080/api/...` for the API

The gateway logs method, path, chosen upstream, policy, DB rewrite action, and
response status via `tracing`.
