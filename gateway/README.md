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
- `/api/inference/*` goes to the inference upstream (defaults to the API upstream)
- `/docs` goes to the Python backend
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
  enforcement rules as normal DB parameters and is only available when
  `EXPERIMENTAL_RUST_DB_CREATION` is set to a truthy value (`1`, `true`, `yes`,
  or `on`, case-insensitive).
- `/api/inference/*` never receives DB query parameters.
- When `upstreams.api.local = true`, the gateway serves `/api/db`,
  `/api/db/create` (only when `EXPERIMENTAL_RUST_DB_CREATION` is truthy),
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
  `/openapi.json` from the local OpenAPI generator.
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

When `upstreams.api.local = true` and `EXPERIMENTAL_RUST_JOBS` is set to a
truthy value (`1`, `true`, `yes`, or `on`), `/api/jobs/*` is implemented
locally. A global job-queue actor holds the in-memory queue and running job
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
index DB via the system config (`continuous_filescan = true`). A supervisor
actor spawns one continuous scan actor per enabled DB. Each actor creates a
`file_scans` row with path `"<continuous>"` while active, uses notify-based
watchers to react to filesystem changes, and writes through the index DB writer
actor. Continuous scanning pauses automatically when a full rescan or folder
update job starts on the same DB and resumes afterward. To force polling (e.g.,
for unreliable shares), set `continuous_filescan_poll_interval_secs` to a
number of seconds (uses `notify::PollWatcher` instead of native watchers).

To add migrations, use SQLx's CLI against the appropriate source directory:

```bash
sqlx migrate add --source gateway/migrations/index add_new_table
```

Programmatic creation and migration lives in `gateway/src/db/migrations.rs`
(`migrate_databases`) and supports both on-disk databases and shared
in-memory databases for tests. Existing Python-created DBs that predate
SQLx migrations are baselined to the first migration so later migrations
can still apply.
Set `EXPERIMENTAL_RUST_DB_AUTO_MIGRATIONS` to a truthy value (`1`, `true`,
`yes`, or `on`) to run migrations across every on-disk DB in `DATA_FOLDER`
at startup, including baselining Python-created databases.

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

[upstreams.inference]
# Optional; defaults to the API upstream if omitted.
base_url = "http://127.0.0.1:6342"
local = false

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
GATEWAY__UPSTREAM_INFERENCE=http://127.0.0.1:6342
GATEWAY__UPSTREAM_INFERENCE_LOCAL=false
GATEWAY__SEARCH__EMBEDDING_CACHE_SIZE=16
```

CLI override example:

```bash
cargo run -p gateway -- --config config\gateway\userconfig.toml
```

The nested style supported by the config crate also works:

```bash
GATEWAY__SERVER__HOST=0.0.0.0
GATEWAY__SERVER__PORT=8080
GATEWAY__SERVER__TRUST_FORWARDED_HEADERS=false
GATEWAY__UPSTREAMS__UI__BASE_URL=http://127.0.0.1:6339
GATEWAY__UPSTREAMS__API__BASE_URL=http://127.0.0.1:6342
GATEWAY__UPSTREAMS__API__LOCAL=false
GATEWAY__UPSTREAMS__INFERENCE__BASE_URL=http://127.0.0.1:6342
GATEWAY__UPSTREAMS__INFERENCE__LOCAL=false
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
