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
- `/docs` and `/openapi.json` go to the Python backend
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
  `EXPERIMENTAL_RUST_DB_CREATION` is set.
- `/api/inference/*` never receives DB query parameters.
- When `upstreams.api.local = true`, the gateway serves `/api/db`,
  `/api/db/create` (only when `EXPERIMENTAL_RUST_DB_CREATION` is set),
  `/api/bookmarks/ns`, `/api/bookmarks/users`,
  `/api/bookmarks/ns/{namespace}`, `/api/bookmarks/ns/{namespace}/{sha256}`,
  `/api/bookmarks/item/{sha256}`, `/api/items/item`, `/api/items/item/file`,
  `/api/items/item/thumbnail`, `/api/items/item/text`, `/api/items/item/tags`,
  `/api/items/text/any`, `/api/search/pql`, `/api/search/tags`,
  `/api/search/tags/top`, and `/api/search/stats`
  locally using the same policy enforcement and filtering rules.
  `/api/search/pql` compiles queries via the upstream `/api/search/pql/build`
  response to apply extra column aliases and the `check_path` behavior.

## Database migrations

The gateway tracks three SQLite schemas (index, storage, user_data) using SQLx
migrations stored in `gateway/migrations/index`, `gateway/migrations/storage`,
and `gateway/migrations/user_data`. The initial migrations mirror the schema
dumps produced by the Python backend, with `BEGIN`/`COMMIT` stripped to avoid
nested transactions under SQLx.

To add migrations, use SQLx's CLI against the appropriate source directory:

```bash
sqlx migrate add --source gateway/migrations/index add_new_table
```

Programmatic creation and migration lives in `gateway/src/db/migrations.rs`
(`migrate_databases`) and supports both on-disk databases and shared
in-memory databases for tests. Existing Python-created DBs that predate
SQLx migrations are baselined to the first migration so later migrations
can still apply.

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
```

## Running locally

From the repo root:

```bash
cargo run -p gateway
```

Then visit:

- `http://localhost:8080` for the UI
- `http://localhost:8080/api/...` for the API

The gateway logs method, path, chosen upstream, policy, DB rewrite action, and
response status via `tracing`.
