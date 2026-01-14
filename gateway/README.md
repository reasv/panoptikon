# Panoptikon Gateway (Rust)

This crate is the single HTTP entrypoint for Panoptikon. Right now it is a
reverse proxy only: it forwards requests to the Next.js UI or the Python API
based on path, with streaming request/response bodies and structured logging.

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

## Configuration

Configuration is TOML and/or environment variables only.

Create `gateway/config.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080

[upstreams]
ui = "http://127.0.0.1:6339"
api = "http://127.0.0.1:6342"
```

Environment variables override the file:

```bash
GATEWAY__SERVER_HOST=0.0.0.0
GATEWAY__SERVER_PORT=8080
GATEWAY__UPSTREAM_UI=http://127.0.0.1:6339
GATEWAY__UPSTREAM_API=http://127.0.0.1:6342
```

If you prefer the config crate's nested style, these also work:

```bash
GATEWAY__SERVER__HOST=0.0.0.0
GATEWAY__SERVER__PORT=8080
GATEWAY__UPSTREAMS__UI=http://127.0.0.1:6339
GATEWAY__UPSTREAMS__API=http://127.0.0.1:6342
```

## Running locally

From the repo root:

```bash
cargo run -p gateway
```

Then visit:

- `http://localhost:8080` for the UI
- `http://localhost:8080/api/...` for the API

The gateway logs method, path, chosen upstream, and response status via
`tracing`.
