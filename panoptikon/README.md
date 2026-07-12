# Panoptikon server (Rust)

This crate is the single HTTP entrypoint for Panoptikon. It forwards requests
to the Next.js UI or the Python API based on path, with streaming
request/response bodies, policy enforcement, structured logging, and optional
local API handling when `upstreams.api.local = true`.

## Where it fits

Panoptikon currently runs:

- Next.js frontend (dev server on `http://127.0.0.1:6340`)
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

## Listener endpoints

The gateway can bind multiple listeners. `[server] host`/`port` is the
primary listener, always named `default`; extra named listeners are declared
as `[[server.endpoints]]` entries (`name`, `port`, and an optional `host`
defaulting to `server.host`). Every listener serves the identical routes —
the point of extra endpoints is policy routing: `[policies.match]` can match
on `endpoints = ["name", ...]`, and the endpoint a request arrived on is
determined by the TCP listener, so unlike `Host` matching it cannot be
influenced by request headers and works with plain local ports. A typical
use is a second loopback port whose policy defaults to (and is locked to) a
dedicated test DB — anything pointed at that port operates on the test DB
with no manual selection (`config/gateway/local.toml` ships this setup on
port 6343). All listeners are bound before serving starts; if any bind
fails, startup fails.

## Policy enforcement

Policies are selected by effective host and/or listener endpoint: in
`[policies.match]`, an empty/omitted `hosts` or `endpoints` list matches
anything, both non-empty means both must match, and at least one must be
non-empty. Policies are checked in config order and the first match wins, so
endpoint-scoped policies should be listed before broad host policies.
Policies then optionally restrict API routes via reusable
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

### Policy-scoped SSR tokens (`x-panoptikon-policy`)

The gateway injects `x-panoptikon-policy: <policy>.<expiry_unix>.<hmac_hex>`
into every request it proxies to the UI upstream, naming the policy the
policy layer matched for that request (HMAC-SHA256 over `<policy>.<expiry>`,
5-minute expiry). When the Next.js server renders a page it echoes the token
on its own API calls back into the gateway; at policy ingress a token that
parses, verifies (constant-time), is unexpired, and names a configured
policy selects that policy instead of listener/host matching — so SSR acts
with the authority of the browser request that triggered the render, not
with the authority of the UI server's network position.

Threat model, in short: the UI process holds no authority of its own — the
token is minted per request and expires quickly. A forged, tampered,
expired, or absent token is silently ignored (reason logged at debug:
`malformed`/`bad-hmac`/`expired`/`unknown-policy`) and selection falls back
to listener/host matching, so point the SSR's API base URL at the listener
whose policy is the most restricted one. Every request log line records the
selection mechanism (`selected_by = token` or `listener/host`) and the
policy name.

The HMAC key is a random 256 bits generated at gateway boot. `[server]
policy_token_key` (64 hex chars, env-templatable) pins it — a niche option
needed only when tokens minted by one gateway must verify on another.

### Ingress header hygiene

At the policy-layer choke point, inbound `x-panoptikon-*` headers are
stripped from client requests so gateway-internal metadata can only ever be
set by the gateway itself. Two exceptions:

- `x-panoptikon-policy` is verified first, then consumed — it never travels
  upstream or into local handlers, valid or not.
- `x-panoptikon-gateway-hops` is **preserved** with its inbound value
  intact: it counts how many panoptikon gateways a request has passed
  through and is the self-proxy loop guard (`MAX_PROXY_HOPS` in proxy.rs,
  added after the 2026-07-07 port-exhaustion incident). Legitimate
  gateway→gateway forwarding re-enters the policy layer on the next
  gateway, so stripping it there would reset the count on every hop and
  disable loop detection. A client sending a bogus value can only lower its
  own hop budget, never bypass the guard.

The `x-panoptikon-*` namespace is therefore gateway-reserved. In
particular, `[policies.identity] user_header` may not name a header in that
namespace: the strip runs before identity extraction, so such a header
would never be seen and tenant isolation would silently fall back to the
un-tenanted defaults — config load rejects it with a hard error. Policy
names are likewise validated at config load (`[a-zA-Z0-9._-]`, max 64
chars) so every name is embeddable in the token header.

### `GET /api/client-config`

Local API endpoint (`upstreams.api.local = true` only) answering "what may
this client do, and how should it behave?". In local-API mode it is exempt
from ruleset enforcement (built-in, not configurable): a client must always
be able to ask what it may do — it is how restricted UIs learn which
controls to hide. With a proxied API the route does not exist and no
exemption applies (the path is subject to the ruleset like any other
upstream API route). Responses are policy-scoped and carry
`Cache-Control: no-store`, so an intermediary cache can never serve one
audience's capabilities to another. Response:

```json
{
  "policy": "public_demo",
  "capabilities": {
    "search": true, "items": true, "bookmarks": true,
    "scan_jobs": false, "open_files": false, "db_create": false,
    "inference": false, "pinboards": false
  },
  "client": { "search_throttle_ms": 1500, "disable_backend_open": true }
}
```

`capabilities` are derived, not configured: each is one representative
probe from the real route list — `search` → `POST /api/search/pql`,
`items` → `GET /api/items/item`, `bookmarks` →
`PUT /api/bookmarks/ns/{ns}/{sha256}`, `scan_jobs` →
`POST /api/jobs/folders/rescan`, `open_files` →
`POST /api/open/file/{sha256}`, `db_create` → `POST /api/db/create`,
`inference` → `POST /api/inference/predict/{group}/{id}`, `pinboards` →
`POST /api/pinboards` — evaluated against the matched policy's ruleset with
the same rule-matching code enforcement uses. Under the shipped
`restricted_demo` ruleset that yields search/items/bookmarks true and
everything host-side false.

`client` is the policy's `[policies.client]` TOML table passed through
verbatim (default: empty object). The gateway attaches no semantics to it;
recognized-by-convention keys are `search_throttle_ms`,
`disable_backend_open`, and `home_redirect` (a string path the UI's root
page redirects to, e.g. `"/search"`; unset = no redirect). Env templating
applies inside it like everywhere else in the config file.

## Database migrations

The gateway tracks three SQLite schemas (index, storage, user_data) using SQLx
migrations stored in `panoptikon/migrations/index`, `panoptikon/migrations/storage`,
and `panoptikon/migrations/user_data`. The initial migrations mirror the schema
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
at all) against the same data folder, or extraction jobs will be scheduled
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
# python = "python/.venv/Scripts/python.exe"  # default: the managed venv
#                                     # (python/.venv), then the legacy root .venv
# impl_dirs = ["python/inferio/impl", "inferio_custom"]
# config_dirs = ["python/inferio/config", "config/inference"]
# pythonpath = ["python"]
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

# [inference_local.python_env]  # managed venv policy (`panoptikon setup`)
# accelerator = "auto"    # "auto" | "cuda" | "rocm" | "cpu"
# auto_setup = true       # run setup at startup when python/.venv is missing
```

### The managed Python environment (`panoptikon setup`)

```bash
panoptikon setup [--accelerator auto|cuda|rocm|cpu] [--force]
```

Creates or updates the managed inference venv at **`python/.venv`** (fixed
location, relative to the working directory like every other path; a
`bundled` binary running outside a source checkout manages
**`runtime/venv`** against its extracted source set instead — see
"Self-contained builds" below). The flow:

1. **uv discovery** — `uv` on PATH is used when `uv --version` works and the
   version is at least 0.6.13 (the verified floor for the current lockfile);
   otherwise a pinned standalone build (see `UV_VERSION` in `src/setup.rs`)
   is downloaded from the official astral-sh/uv GitHub release into
   `runtime/uv/<version>/` and reused on later runs. Downloads are verified
   against SHA-256 checksums pinned in the binary (from the release's
   `.sha256` companion files) before extraction.
2. **Accelerator selection** — `--accelerator` beats
   `[inference_local.python_env] accelerator` (default `"auto"`). Auto
   detection: macOS → default PyPI wheels (MPS on Apple Silicon); CUDA when
   `nvidia-smi` is on PATH, `System32\nvidia-smi.exe` exists (Windows), or
   `/proc/driver/nvidia` exists (Linux); ROCm on Linux when `/opt/rocm` or
   `rocm-smi` is found; otherwise CPU. The decision and its evidence are
   logged.
3. **Locked sync** — `uv venv --python 3.12` when the venv is missing (uv
   auto-fetches CPython), then `uv sync --locked --extra <variant>` in
   `python/` (`cuda` → the `cu128` extra; the committed `uv.lock` is
   authoritative and covers all variants). Child output streams to the log.

Re-running converges (idempotent, fast when nothing changed); `--force`
deletes the managed venv first. As a safety guard, setup refuses to operate
on any venv other than the active managed one (`python/.venv` in a source
checkout, `runtime/venv` in extracted bundled mode) — a user-configured
`[inference_local].python` interpreter is never touched.

After every successful sync, setup writes a completion sentinel
(`python/.venv/.panoptikon-setup-complete`, recording the installed extra
and the `uv.lock` hash). With `auto_setup = true` (the default) and no
explicit `python` configured, startup (gateway and `inferio` modes) re-runs
setup automatically when the environment doesn't exist yet, when the
sentinel is missing (an interrupted first sync leaves an interpreter but no
sentinel), or when it is stale (`uv.lock` changed, e.g. after a pull). A
legacy pre-restructure root `.venv` without a managed venv suppresses the
trigger — it keeps working and is never auto-managed. An explicit
`panoptikon setup` always runs regardless of the sentinel. Auto-setup runs
blocking before the orchestrator serves; on failure the server continues
with local inference unavailable.

Concurrent runs (e.g. a gateway and an `inferio` process starting together)
are serialized by an exclusive lock on `runtime/setup.lock`: the second
process logs that it is waiting, then no-ops if the first already converged
the environment. macOS x86_64 is unsupported (PyTorch publishes no wheels
for it and the lock excludes it) — setup fails fast there.

A machine that only lends its GPU can run the standalone service:

```bash
cargo run -p panoptikon -- inferio --config config\gateway\default.toml
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
sqlx migrate add --source panoptikon/migrations/index add_new_table
```

Programmatic creation and migration lives in `panoptikon/src/db/migrations.rs`
(`migrate_databases`) and supports both on-disk databases and shared
in-memory databases for tests.

When `upstreams.api.local = true` (and `readonly` is not enabled), the gateway
runs migrations across every on-disk DB in `data_folder` at startup, like the
Python server does. Existing Python-created DBs are baselined: the init
snapshot is recorded as applied without executing any SQL — the only write is
the `_sqlx_migrations` bookkeeping table. Baselining requires the DB to be at
the exact alembic head the snapshot was taken from; anything older (or a
non-empty DB with no `alembic_version` at all) makes startup fail with an
explicit error instead of silently assuming the wrong schema. Freshly created
DBs get the alembic head revision stamped into `alembic_version` so they
remain manageable by the Python server during the transition.

## Production UI

With `[upstreams.ui] local = true` the gateway also runs the production
Next.js UI from a panoptikon-ui checkout (the `ui/` submodule by default;
port of the legacy Python `searchui` router, minus the git clone/pull). In
normal server mode (not the `inferio` subcommand) a background task:

1. runs `npm install --include=dev` when `node_modules` is missing or
   `package.json` is newer than the last install (tracked via a
   `node_modules/.gateway-install-stamp` file written after each successful
   install, falling back to the `node_modules` mtime). npm is invoked as
   `npm-cli.js` under the resolved node binary when it can be found next to
   it, falling back to npm on PATH;
2. runs `next build` per the `build` policy. `"auto"` (default) builds when
   `.next/BUILD_ID` is missing or the checkout's HEAD differs from the commit
   recorded in `.next/.gateway-built-commit` by the last gateway-run build (a
   missing stamp counts as differing; if git fails the HEAD is unknown and
   only a missing BUILD_ID builds). `"always"` builds on every startup.
   `"never"` never builds — if no build exists, that is a fatal
   misconfiguration: an error is logged and the supervisor stops instead of
   restart-looping;
3. spawns `next start` bound to the host/port parsed from `base_url` (which
   must be a plain loopback `http://host:port` URL in this mode — the proxy
   keeps forwarding to it unchanged), restarting on unexpected exit with
   capped backoff (1s doubling to 30s, reset after 60s of stable uptime). If
   the port is already accepting connections before the spawn, a warning
   names the squatting host:port (usually a still-running Python-managed UI).

Gateway startup is not blocked: the proxy returns 502 until the UI is up, and
an info line with the final URL is logged once the port accepts connections.
Child stdout/stderr stream into the gateway log line-by-line. The children
are detached from the console signal path (new process group on Windows,
setsid on Unix), so a Ctrl-C hits only the gateway and the supervisor tears
the UI down in order; on Windows the child tree additionally sits under a
kill-on-close Job Object (like inferio workers). Graceful shutdown kills the
UI first — it is stateless.

`[upstreams.ui]` keys:

- `local` (default `false`): spawn and supervise the production UI server.
- `dir`: path to the panoptikon-ui checkout; required when `local = true`,
  relative paths resolve against the working directory.
- `node`: explicit node binary. Default resolution: the managed venv's
  nodejs-wheel node (the real binary inside the `nodejs_wheel` package under
  `python/.venv`, then the legacy root `.venv`, falling back to the venv's
  `Scripts/node.exe` / `bin/node` launcher stub), then `node` from PATH.
- `build` (default `"auto"`): `"auto"` | `"always"` | `"never"`.

`config/gateway/local.toml` is a ready-made local rust-only production
config (local API + local inference + local UI); `start.bat` / `start.sh` at
the repo root runs the release binary with it.

In a `bundled-ui` build, a missing checkout at `dir` falls back to the
embedded UI bundle instead of failing: it is extracted to
`runtime/ui/<version>/` and supervised as `node server.js` (see
"Self-contained builds" below). A checkout that *does* exist at `dir`
always wins over the embedded bundle.

## Self-contained builds (`bundled`, `bundled-ui`)

Plain builds read the source tree exactly as documented everywhere else in
this file. Release builds enable cargo features that embed the runtime
resources into the binary (docs/architecture.md "Self-contained releases"):

- **`bundled`** embeds the default configs (`config/gateway/default.toml`,
  `config/inference/example.toml`) and the Python source set
  (`python/inferio_worker`, `python/inferio`, `pyproject.toml`, `uv.lock` —
  no tests, no venv, no bytecode caches) as a compressed archive.
- **`bundled-ui`** (additive) embeds a production UI bundle. The build-time
  env var `PANOPTIKON_UI_BUNDLE` must point at a **fully assembled** Next.js
  standalone output directory — `server.js` + `node_modules` at the root
  with the build's `.next/static` (and `public/`, if any) copied in;
  the build fails with a clear error when it is unset or invalid. The
  panoptikon-ui repo produces such a directory when built with
  `BUILD_STANDALONE=true next build` (the env var opts `next.config.mjs`
  into `output: 'standalone'`; it is not on by default because `next start`
  — plain and gateway-managed — refuses to run with standalone output).
  Note the standalone server bakes the config's rewrites and their env vars
  (`PANOPTIKON_API_URL`, `RESTRICTED_MODE`, ...) in at build time; behind
  the gateway they are dormant anyway, since the gateway serves `/api`,
  `/docs` and `/openapi.json` itself and only forwards the rest to the UI.

First run of a `bundled` binary materializes what is missing — loudly, in
the log:

- No `config/gateway/default.toml` (and no `--config`/`GATEWAY_CONFIG_PATH`
  pointing elsewhere): the embedded default configs are written to
  `config/`. Each file is written only if absent and **never overwritten** —
  they are user-owned from then on.
- No `python/inferio_worker` dev tree: the embedded Python source set is
  extracted to `runtime/pysrc/<version>/` (version = the binary's crate
  version). Extraction is atomic (temp dir + rename) and marker-verified
  (`.panoptikon-extracted` records the archive hash), so a corrupted or
  partial extraction is redone on the next start. `panoptikon setup` then
  manages the venv at `runtime/venv` — outside the version-keyed dir, so a
  version bump re-extracts sources but keeps the venv; the setup sentinel's
  uv.lock hash triggers the re-sync.
- UI (`bundled-ui`, `[upstreams.ui] local = true`, and the configured `dir`
  absent): the embedded bundle is extracted to `runtime/ui/<version>/` and
  run as `node server.js` with the bind address in the `PORT`/`HOSTNAME`
  env vars (the standalone server is not `next start`). npm install / build
  staleness steps are skipped — the bundle is immutable. Node comes from
  the managed venv's nodejs-wheel as usual.

Resource resolution order everywhere: **explicit config > dev source tree
(when present) > extracted embedded set**. A bundled binary dropped into a
source checkout behaves exactly like a plain build.

Housekeeping: the extracted sets are version-keyed, so
`runtime/pysrc/<version>/` and `runtime/ui/<version>/` accumulate one
directory per binary version you have run. Old version dirs are never
removed automatically (an older binary may still be running against them) —
deleting the ones for versions you no longer run is always safe. Orphaned
`.tmp-*` extraction leftovers from crashed runs are swept automatically
once they are over a day old.

### `--root`

The global `--root <dir>` flag (default: the current working directory) is
the base for all relative path resolution — `data_folder`, `config/`, the
python tree, `runtime/`. It is implemented as a chdir at startup before
anything else runs, so every CWD-relative default (including the `.env`
auto-load) resolves under it — portable-app style:

```bash
panoptikon --root D:/panoptikon-home
panoptikon setup --root D:/panoptikon-home --accelerator cpu
```

## Configuration

All global configuration is TOML. The gateway reads
`[cwd]/config/gateway/default.toml`; override the path with `--config` or the
`GATEWAY_CONFIG_PATH` environment variable. Environment variables are **not**
a parallel configuration mechanism: they feed TOML values through templating
(below), plus a small keep-list of bootstrap/diagnostic variables.

### Env templating

String values in the settings file — and in every inference registry TOML —
may reference environment variables:

```toml
[logging]
level = "${LOGLEVEL:-INFO}"   # LOGLEVEL, or "INFO" when unset or empty

[open]
file_command = "player ${PLAYER_FLAGS:-} {path}"

# In a registry TOML: secrets reach impl constructors as config kwargs.
# [group.clip-api.inference_ids.jina-clip-v2]
# config.api_key = "${JINA_API_KEY}"
```

The forms follow the shell / docker-compose conventions:

- `${NAME}` — replaced with the variable's value; **hard error** at load
  time when unset (fail loudly for secrets), naming the file and variable.
  Set-but-empty is not an error: it yields the empty string, as in shell.
- `${NAME:-default}` — the literal default when the variable is unset **or
  set but empty** (so a `.env` line like `LOGLEVEL=` still gets the default).
- `${NAME-default}` — the default only when the variable is *unset*; a
  set-but-empty variable yields the empty string.
- `$${` — a literal `${`.
- Multiple placeholders per string are allowed; substitution is one pass
  (values are never re-expanded). `NAME` is `[A-Za-z_][A-Za-z0-9_]*`.

Substitution runs on *parsed* string values, so env values containing
backslashes or quotes (Windows paths) cannot corrupt the TOML. Only strings
can be templated — for numeric/boolean keys use the `GATEWAY__*` override
layer instead. Values arriving through the `GATEWAY__*` override layer are
**not** templated: overrides are applied after substitution and are taken
literally (they already come from the environment). Registry TOMLs are
re-substituted on every mtime-gated reload. Per-DB system configs
(`data/index/<name>/config.toml`) are **not** templated: the gateway writes
them back, which would destroy the templates.

The gateway auto-loads `.env` from the working directory at startup — that is
the intended way to populate the variables templating references, and child
processes (inference workers, the UI server) inherit them.

### Settings

Create `config/gateway/default.toml`:

```toml
# Top level (all optional):
# data_folder = "data"       # root for index DBs, user data, thumbnails, logs
# index_db = "default"       # default DB names when neither the request nor
# user_data_db = "default"   #   the matched policy picks one
# readonly = false           # strip write locks, skip startup migrations
# temp_dir = "data/tmp"      # extraction scratch space (literal default —
                             #   not derived from data_folder)

[logging]
# file = ""                  # default: <data_folder>/panoptikon-gateway.log;
                             #   explicit "" disables file logging
level = "${LOGLEVEL:-INFO}"  # RUST_LOG takes precedence when set

# [open]                     # custom /api/open commands; {path} {folder}
# file_command = "mpv {path}"          #   {filename} placeholders; "" = no-op
# folder_command = "explorer {folder}" # (was: show in file manager)

[server]
host = "0.0.0.0"
port = 8080
trust_forwarded_headers = false
# Extra named listeners (the primary above is always endpoint "default");
# policies can match on them via [policies.match] endpoints = [...]:
# [[server.endpoints]]
# name = "test"
# port = 8081
# # host = "127.0.0.1"  # default: server.host

[upstreams.ui]
base_url = "http://127.0.0.1:6340"
# Run the production UI from a checkout (see "Production UI" above):
# local = true
# dir = "ui"                  # the ui/ git submodule is the standard spot
# node = "C:/path/to/node.exe"  # default: repo venv's node, then PATH
# build = "auto"                # "auto" | "always" | "never"

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

[jobs]
# loader_concurrency = 8
# intermediate_data_budget_mb = 1024
# atomic_extraction_jobs = false  # delete (not fail) incomplete jobs at start

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

Registry/impl directory resolution for local inference is configured only by
`[inference_local].config_dirs` / `impl_dirs` (defaults:
`["python/inferio/config", "config/inference"]` and
`["python/inferio/impl", "inferio_custom"]`).

### Environment variables that remain

These are deliberately *not* TOML keys:

- `GATEWAY_CONFIG_PATH` — bootstrap: locates the config file, so it cannot
  live inside it. (`--config` wins over it.)
- `RUST_LOG` — standard tracing debug tool; overrides `[logging].level` when
  set and supports per-module directives.
- `GATEWAY__*` — generic config override layer (see below); works for every
  key, including the new ones (e.g. `GATEWAY__DATA_FOLDER`,
  `GATEWAY__LOGGING__LEVEL`, `GATEWAY__READONLY`).
- Variables the gateway *sets* on child processes (internal protocol):
  `INFERIO_WORKER`, `PYTHONIOENCODING`, `PYTHONPATH` (prepended),
  `CUDA_VISIBLE_DEVICES` (per-replica device pins), plus plain environment
  inheritance — workers and the UI server see the gateway's env (and thus
  `.env`), unfiltered.
- Dev/diagnostic overrides for bundled native dependencies: `PDFIUM_PATH`,
  `HTML_RENDERER_PATH`, `PANOPTIKON_FONT`; `PANOPTIKON_TEST_PYTHON` is
  test-only.

The former `DATA_FOLDER`, `INDEX_DB`, `USER_DATA_DB`, `READONLY`, `TEMP_DIR`,
`LOGLEVEL`, `LOGS_FILE`, `OPEN_FILE_COMMAND`, `SHOW_IN_FM_COMMAND`,
`ATOMIC_EXTRACTION_JOBS`, `INFERIO_CONFIG_DIR`, `BASE_INFERENCE_CONFIG_FOLDER`
and `INFERIO_CUSTOM_IMPL_PATH` env vars are no longer read: use the TOML keys
(templated from env if desired, e.g. `level = "${LOGLEVEL:-INFO}"`).

### GATEWAY__ overrides

Environment variables with the `GATEWAY__` prefix override the file:

```bash
GATEWAY_CONFIG_PATH=config\gateway\default.toml
GATEWAY__SERVER_HOST=0.0.0.0
GATEWAY__SERVER_PORT=8080
GATEWAY__SERVER_TRUST_FORWARDED_HEADERS=false
GATEWAY__UPSTREAM_UI=http://127.0.0.1:6340
GATEWAY__UPSTREAM_API=http://127.0.0.1:6342
GATEWAY__UPSTREAM_API_LOCAL=false
GATEWAY__SEARCH__EMBEDDING_CACHE_SIZE=16
```

CLI override example:

```bash
cargo run -p panoptikon -- --config config\gateway\userconfig.toml
```

## Logging and shutdown

Log output goes to the console and, by default, to
`<data_folder>/panoptikon-gateway.log` (append; the name is distinct from the
Python server's `panoptikon.log` so the two processes never interleave one
file). `[logging].file` overrides the path; setting it to an empty string
disables file logging. `[logging].level` sets the default level; `RUST_LOG`
takes precedence when set and supports per-module directives.

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
GATEWAY__UPSTREAMS__UI__BASE_URL=http://127.0.0.1:6340
GATEWAY__UPSTREAMS__API__BASE_URL=http://127.0.0.1:6342
GATEWAY__UPSTREAMS__API__LOCAL=false
GATEWAY__UPSTREAMS__INFERENCE__0__BASE_URL=http://127.0.0.1:6342
GATEWAY__UPSTREAMS__INFERENCE__0__WEIGHT=1.0
GATEWAY__UPSTREAMS__INFERENCE__0__USE_FOR_JOBS=true
GATEWAY__SEARCH__EMBEDDING_CACHE_SIZE=16
GATEWAY__DATA_FOLDER=data
GATEWAY__LOGGING__LEVEL=debug
```

## Running locally

From the repo root:

```bash
cargo run -p panoptikon
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
