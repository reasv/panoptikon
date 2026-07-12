# Panoptikon architecture (Rust era)

This document is the charter for the `rust` branch: how the project is organized
now that the Rust server is the one and only entrypoint, where the remaining
Python lives, and how self-contained binary releases work. It exists so these
decisions don't have to be re-derived.

## What Panoptikon is now

One Rust binary (`panoptikon`) that owns everything: the HTTP entrypoint and
policy layer, the full API, PQL search, the job system, cron, file scanning
(including continuous scanning), database migrations, the inference
orchestrator, and supervision of the production web UI. Python remains for
exactly one purpose: the inference worker processes that load and run AI
models, spawned by the orchestrator as `python -m inferio_worker` over a
msgpack stdio protocol.

Python may not be a permanent dependency. Several models we use have
Rust-native implementations, and others could be written. Nothing in the
architecture may assume Python is load-bearing beyond the worker boundary.

## Repository layout

```
panoptikon/
├── Cargo.toml            # workspace
├── panoptikon/           # the server crate (binary: panoptikon)
│   ├── migrations/       # sqlx migrations (index/storage/user_data), compiled in
│   └── src/
├── python/               # ALL Python, self-contained
│   ├── pyproject.toml    # worker + inference deps, accelerator extras
│   ├── uv.lock           # universal lock, all platforms/accelerators
│   ├── .venv/            # managed venv (created by `panoptikon setup`)
│   ├── inferio_worker/   # worker entrypoint (protocol v2)
│   └── inferio/          # impl classes + built-in registry TOMLs
├── ui/                   # git submodule → panoptikon-ui
├── config/
│   ├── gateway/          # server TOML config (default.toml, local.toml)
│   └── inference/        # user model-registry TOMLs (merged with built-ins)
├── inferio_custom/       # user impl classes (merged with built-ins)
├── data/                 # runtime data (databases, logs) — DATA_FOLDER
└── docs/
```

The Python server, searchui (runtime git-clone machinery), alembic, the Python
inferio HTTP server/orchestrator, and the install/start batch scripts of the
Python era are deleted. The old code survives on the `python-legacy` branch,
mounted as a permanent gitignored worktree at `python-legacy/` (see Roadmap).

## Configuration

TOML with env templating is THE configuration mechanism (see
`panoptikon/README.md` for the reference). `${VAR}` errors on unset,
`${VAR:-default}` covers unset-or-empty, `${VAR-default}` unset only, `$${`
escapes. Applied to the server config and every inference registry TOML —
secrets reach inference impls via inference-ID `config.*` kwargs, never via a
special env-passing channel. `.env` is auto-loaded as a convenience source for
template variables. Bootstrap/diagnostic env vars (`PANOPTIKON_CONFIG_PATH`,
`RUST_LOG`, `PANOPTIKON__*` overrides) are the documented exceptions.

### Policy-scoped SSR

The gateway stamps every UI-bound proxied request with a short-lived HMAC
token (`x-panoptikon-policy: <policy>.<expiry>.<hmac>`, key random per boot,
overridable via `[server] policy_token_key` for multi-gateway setups) naming
the policy that request matched. The Next.js server echoes the token on its
SSR API calls back into the gateway, where a verified token selects the
named policy ahead of listener/host matching — SSR renders with the
browser request's authority, not the UI server's network position. Invalid
or absent tokens fall back to normal selection, so the SSR's API base URL
should point at the most restricted listener. At the same choke point all
other inbound `x-panoptikon-*` headers are stripped (except
`x-panoptikon-hops`, the self-proxy loop guard, which must survive
gateway→gateway forwarding). `GET /api/client-config` — exempt from ruleset
enforcement by design — tells any client its matched policy, capability
booleans derived from the policy's ruleset via probe routes, and the
free-form `[policies.client]` table verbatim. Details in
`panoptikon/README.md`.

## Python environment management

Owned by the binary. `panoptikon setup` (also auto-triggered at startup when
`[inference_local]` is enabled, no interpreter is explicitly configured, and
the managed environment is missing or incomplete — completion is tracked by
a sentinel inside `python/.venv` recording the synced extra and the uv.lock
hash, so an interrupted first sync or a changed lock re-arms the trigger,
while a legacy root `.venv` alone suppresses it; concurrent runs serialize
on a `runtime/setup.lock` file lock):

1. Locates `uv` on PATH, else downloads a pinned, checksum-verified
   standalone `uv` into the managed root.
2. Detects the accelerator (`[inference_local.python_env] accelerator =
   "auto" | "cuda" | "rocm" | "cpu"`; auto = our detection, ported from the old
   install scripts — uv's `--torch-backend=auto` exists only in its pip
   interface, so we detect and pass the extra ourselves).
3. Creates `python/.venv` and runs a locked `uv sync --extra <accelerator>`.

`python/pyproject.toml` encodes the accelerator matrix with mutually exclusive
extras (`[tool.uv] conflicts`), explicit PyTorch indexes per extra
(`[tool.uv.sources]`), and macOS routed to default PyPI wheels via markers.
One universal `uv.lock` covers Windows/Linux × CUDA/CPU, macOS aarch64, and
(untested) ROCm. Because the accelerator variant is locked, `uv sync` can never
downgrade torch out from under a working environment again (a
`constraint-dependencies` pin keeps even an extra-less `uv sync` on the same
torch). `[tool.uv] environments` restricts the lock to the platforms we
actually target — Windows x86_64, Linux x86_64, macOS aarch64; Linux aarch64
is excluded because torch 2.7.1's pinned triton publishes no aarch64 wheels.
The extras spell the CUDA variant `cu128` (torch 2.7.1); `panoptikon setup`
maps `accelerator = "cuda"` to it, so a future CUDA bump is a pyproject +
setup.rs change, not a config change.

Escape hatch: `[inference_local].python` points at any interpreter the user
manages themselves. Never run `uv sync` against a user-managed venv.

## Self-contained releases

A release is one binary per platform. Everything else is embedded at build
time (cargo feature `bundled`, used by release CI; plain dev builds skip
embedding and read the source tree as usual) and materialized at first run:

- **Default configs** (server TOML, example inference TOML): written to
  `config/` only if absent — user-owned afterwards, never overwritten.
- **Built-in Python source set** (`inferio_worker`, `inferio` impls + built-in
  registry, `pyproject.toml` + `uv.lock`): extracted to the version-keyed,
  binary-owned `<root>/runtime/pysrc/<version>/` that users never edit
  (atomic temp-dir + rename, redone when the `.panoptikon-extracted` marker
  is missing or its archive hash mismatches). The managed venv lives at
  `<root>/runtime/venv` — outside the version-keyed dir, so version bumps
  re-extract sources but keep the venv; the setup sentinel's uv.lock hash
  drives the re-sync. User extensions live in `inferio_custom/` and
  `config/inference/` and merge as a set, exactly like the dev-layout
  `impl_dirs`/`config_dirs`.
- **UI production bundle** (Next.js standalone output, built in the same CI
  run as the binary and passed to cargo via the `PANOPTIKON_UI_BUNDLE` env
  var — additive feature `bundled-ui`): embedded compressed, extracted like
  the Python set to `<root>/runtime/ui/<version>/`, run as the standalone
  `node server.js` (PORT/HOSTNAME env vars, not `next start`) with the
  managed venv's Node. Install/build staleness checks are skipped — the
  bundle is immutable. Requires `output: 'standalone'` in the UI repo's
  next config — opt-in via `BUILD_STANDALONE=true next build` there, since
  `next start` refuses to run with standalone output. Embedding is deliberate: single CI build, no dependency on
  GitHub artifacts at runtime, and fully offline operation once `setup` has
  run — no phone-home, no git-pull deployment.

Resource resolution order everywhere: explicit config > dev source tree (when
present) > extracted embedded set. The root for all of this is `--root`
(default: CWD, portable-app style; implemented as a chdir at startup, before
`.env` loading and config resolution). A future installer distribution will
use platform dirs (AppData/XDG) through the same root abstraction.

## Technical debt register

- **Node.js comes from `nodejs-wheel`** (a pip package in the managed venv).
  We are borrowing a JS runtime from a Python packaging accident. It works,
  but it is debt: the UI server should eventually get a properly-managed Node
  (or no Node at all). Docker images will install Node natively rather than
  inherit this.
- **ROCm support is aspirational and has never been tested.**
- **`cudnn/` vendored dir** remains supported as a legacy fallback path in the
  worker's cuDNN setup.

## Roadmap

- **M1**: repository restructure per the layout above; crate and
  binary renamed `panoptikon`; `ui/` submodule; README updated (not
  rewritten) with the Rust flow as the installation path.
- **M2**: `panoptikon setup` + the locked accelerator matrix.
- **M3**: embedded resources + first-run extraction (done: `bundled` /
  `bundled-ui` features, `--root`) + release CI (pending; panoptikon-ui
  supports `output: 'standalone'` via `BUILD_STANDALONE=true next build`).
- **M4 — the swap** (done 2026-07-12): `master` was renamed `python-legacy`
  and the Rust branch became `master` (also the GitHub default). A permanent
  worktree of `python-legacy` is mounted in the repo folder (gitignored) so
  the old implementation stays available side-by-side — for reference and for
  the PQL equivalence suite that runs both implementations against the same
  database snapshot and diffs results.
- **M5 — Docker rework** (done 2026-07-12): one container, one process — the
  Rust binary built with `bundled,bundled-ui`, a native Node.js, and the
  managed venv provisioned at image build time (CPU wheels by default,
  `--build-arg ACCELERATOR=cuda` for the CUDA variant). Two listeners with
  endpoint-scoped policies (private admin 6342 allow-all, public 6339
  `restricted_demo`) replace the Python-era nginx + two-UI-service compose
  stack. Image: `ghcr.io/reasv/panoptikon` (linux/amd64 — the lock excludes
  linux/aarch64), built/smoke-tested/pushed by the release workflow's docker
  job; the distributable compose file is `deploy/docker-compose.yml`.
