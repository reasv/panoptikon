# Panoptikon Gateway (Rust)

This crate is the single HTTP entrypoint for Panoptikon. It is still a reverse
proxy only: it forwards requests to the Next.js UI or the Python API based on
path, with streaming request/response bodies, policy enforcement, and structured
logging.

## Where it fits

Panoptikon currently runs:

- Next.js frontend (dev server on `http://127.0.0.1:6339`)
- Python backend (FastAPI on `http://127.0.0.1:6342`)

The gateway sits in front of both, so the browser only talks to the Rust
process. This keeps the entrypoint stable as more native Rust endpoints are
added later.

## Routing

- `/api/*` goes to the Python backend
- `/docs` and `/openapi.json` go to the Python backend
- everything else goes to the Next.js frontend

Paths, methods, headers, and bodies are forwarded as-is.

## Policy enforcement

Policies are selected by host, then optionally restrict API routes via reusable
rulesets. DB-aware API routes always receive explicit `index_db` and
`user_data_db` query parameters, and the gateway validates or rewrites them per
policy (including optional multi-tenant templates).

## Configuration

Configuration is TOML and/or environment variables only. By default the
gateway reads `[cwd]/config/gateway/default.toml`. Override the path with
`--config` or the `GATEWAY_CONFIG_PATH` environment variable.

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

[rulesets.allow_all]
allow_all = true

[[policies]]
name = "desktop"
ruleset = "allow_all"

[policies.match]
hosts = ["localhost", "127.0.0.1", "panoptikon.local"]

[policies.defaults]
index_db = "default"
user_data_db = "default"

[policies.index_db]
allow = "*"

[policies.user_data_db]
allow = "*"
```

Environment variables override the file:

```bash
GATEWAY_CONFIG_PATH=Q:\projects\panoptikon\config\gateway\default.toml
GATEWAY__SERVER_HOST=0.0.0.0
GATEWAY__SERVER_PORT=8080
GATEWAY__SERVER_TRUST_FORWARDED_HEADERS=false
GATEWAY__UPSTREAM_UI=http://127.0.0.1:6339
GATEWAY__UPSTREAM_API=http://127.0.0.1:6342
```

CLI override example:

```bash
cargo run -p gateway -- --config Q:\projects\panoptikon\config\gateway\default.toml
```

The nested style supported by the config crate also works:

```bash
GATEWAY__SERVER__HOST=0.0.0.0
GATEWAY__SERVER__PORT=8080
GATEWAY__SERVER__TRUST_FORWARDED_HEADERS=false
GATEWAY__UPSTREAMS__UI__BASE_URL=http://127.0.0.1:6339
GATEWAY__UPSTREAMS__API__BASE_URL=http://127.0.0.1:6342
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
