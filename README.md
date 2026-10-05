<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/reasv/panoptikon/master/static/render/gh_banner_darkmode.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/reasv/panoptikon/master/static/render/gh_banner_lightmode.png">
  <img alt="Panoptikon" src="https://raw.githubusercontent.com/reasv/panoptikon/master/static/render/gh_banner_fallback.png">
</picture>

## State-of-the-Art, Local, Multimodal, Multimedia Search Engine

Panoptikon indexes your local files using state-of-the-art AI and machine learning models, making difficult-to-search media such as images and videos easily findable.

Combining OCR, Whisper Speech-to-Text, CLIP image embeddings, text embeddings, full-text search, automated tagging, and automated image captioning, Panoptikon is the _Swiss Army knife_ of local media indexing.

Panoptikon aims to be the `text-generation-webui` or `stable-diffusion-webui` of local search. It is fully customizable, allowing you to easily configure custom models for any of the supported tasks. It comes with a wealth of capable models available out of the box, and adding another one or updating to a newer fine-tune is never more than a few TOML configuration lines away.

As long as a model is supported by any of the built-in implementation classes (supporting, among others, OpenCLIP, Sentence Transformers, Faster Whisper, and Florence 2 via HF Transformers), you can simply add it to the inference server configuration by specifying the Hugging Face repo, and it will immediately be available for use.

Panoptikon is designed to keep index data produced by multiple different models (or different configurations of the same model) **side by side**, letting you choose which one(s) to use _at search time_. As such, Panoptikon is an excellent tool for comparing the real-world performance of different methods of data extraction or embedding models, and allows you to leverage their combined power instead of relying on the accuracy of only one.

For example, when searching with a given tag, you can pick multiple tagging models from a list and choose whether to match an item if at least one model has set the tag(s) you're searching for, or require that all of them have.

The intended use of Panoptikon is for power users and more technically minded enthusiasts to leverage more capable and/or custom-trained open-source models to index and search their files. Unlike tools such as Hydrus, Panoptikon will never copy, move, or otherwise touch your data. You only need to add your directories to the list of allowed paths and run the indexing jobs.

Panoptikon will build an index inside its own SQLite database, referencing the original source file paths. Files are kept track of by their hash, so there's no issue with renaming or moving them around after they've been indexed. You only need to make sure to re-run the file scan job after moving or renaming files to update the index with the new paths. It's also possible to configure Panoptikon to automatically re-scan directories at regular intervals through the cron job feature.

<a href="https://panoptikon.dev/search" target="_blank">
  <img alt="Panoptikon Screenshot" src="https://raw.githubusercontent.com/reasv/panoptikon/refs/heads/master/static/screenshot_1.jpg">
</a>

## Download

### Panoptikon Desktop — recommended for your computer

Desktop installs the complete application, runs without a terminal, keeps the
local Server healthy from a tray icon, opens search in your normal browser,
and updates Desktop, Relay, the control UI, and Server as one signed unit. A
manual first launch immediately shows preparation progress and copyable failure
diagnostics; preparation/readiness notifications can be clicked to continue
into guided library setup or Search. Start-at-login never opens a window on its
own.

| Platform | Download |
| --- | --- |
| **Windows** · x86-64 | [`Panoptikon-Desktop-windows-x86_64.exe`](https://github.com/reasv/panoptikon/releases/latest/download/Panoptikon-Desktop-windows-x86_64.exe) |
| **Linux** · x86-64 | [`Panoptikon-Desktop-linux-x86_64.AppImage`](https://github.com/reasv/panoptikon/releases/latest/download/Panoptikon-Desktop-linux-x86_64.AppImage) |
| **macOS** · Apple Silicon | [`Panoptikon-Desktop-macos-aarch64.dmg`](https://github.com/reasv/panoptikon/releases/latest/download/Panoptikon-Desktop-macos-aarch64.dmg) |

Windows and macOS builds are intentionally not code-signed/notarized in this
initial release, so the operating system may show an unknown-publisher warning.
Updater payloads are nevertheless signed with Panoptikon's dedicated Tauri
update key and verified before installation.

### Panoptikon Server — command line, servers, Docker, and portable use

The self-contained console binary preserves the existing foreground and
`--root` workflows. On Linux/macOS, mark it executable with `chmod +x`.

| Platform | Download |
| --- | --- |
| **Windows** · x86-64 | [`panoptikon-server-windows-x86_64.exe`](https://github.com/reasv/panoptikon/releases/latest/download/panoptikon-server-windows-x86_64.exe) |
| **Linux** · x86-64 | [`panoptikon-server-linux-x86_64`](https://github.com/reasv/panoptikon/releases/latest/download/panoptikon-server-linux-x86_64) |
| **macOS** · Apple Silicon | [`panoptikon-server-macos-aarch64`](https://github.com/reasv/panoptikon/releases/latest/download/panoptikon-server-macos-aarch64) |

Per-release changelogs and the separate Server/Desktop update manifests are on
the [releases page](https://github.com/reasv/panoptikon/releases).

## Rust rewrite

Panoptikon is implemented in Rust: a single native binary owns the HTTP
server, the full API, PQL search, the job system and cron scheduler, file
scanning (including continuous scanning), database migrations, the inference
orchestrator, and the production web UI. Python is used for exactly one
thing: the inference worker processes that load and run the AI models,
spawned by the server on demand.

The legacy Python implementation lives on the `python-legacy` branch and is
no longer developed.

### Warning

Panoptikon is designed to be used as a local service and is not intended to be exposed to the internet. It does not currently have any authentication features and exposes, among other things, an API that can be abused for remote code execution on your host machine. Panoptikon binds to localhost by default, and if you intend to expose it, you should add a reverse proxy with authentication such as HTTP Basic Auth or OAuth2 in front of it.

### Public Instance (panoptikon.dev)

The **only** deployment style we endorse for a public Panoptikon instance is the Docker setup (see the Docker section below): the container exposes a restricted public listener (blocking all dangerous APIs via the server's policy/ruleset system — see the `restricted_demo` ruleset shipped in the config) separately from the unrestricted private admin listener, with authentication added at your reverse proxy if needed.

A public demonstration instance runs at [panoptikon.dev](https://panoptikon.dev/search) for users to try Panoptikon before installing it locally. Certain features, such as the ability to open files and folders in the file manager, have been disabled in the public instance for security reasons.

Panoptikon is also not designed with high concurrency in mind, and the public instance may be slow or unresponsive at times if many users are accessing it simultaneously, especially when it comes to the inference server and related semantic search features. This is because requests to the inference server's prediction endpoint are not debounced, and the instant search box will make a request for every keystroke.

The public instance is meant for demonstration purposes only, to show the capabilities of Panoptikon to interested users. If you wanted to host a public Panoptikon instance for real-world use, it would be necessary to add authentication and rate limiting to the API, optimize the inference server for high concurrency, and possibly add a caching layer.

> 💡 Panoptikon's search API is not tightly coupled to the inference server. It is possible to implement a caching layer or a distributed queue system to handle inference requests more efficiently. Without modifying Panoptikon's source code, you could use a different inference server implementation that scales better, then simply pass the embeddings it outputs to Panoptikon's search API.

> ℹ️ The public instance currently contains a small subset of images from the [latentcat/animesfw](https://huggingface.co/datasets/latentcat/animesfw) dataset.

Although large parts of the API are disabled in the public instance, you can still consult the full API documentation at [panoptikon.dev/docs](https://panoptikon.dev/docs).

## Relay for remote Panoptikon instances

Relay is built into Panoptikon Desktop and enabled by default. It lets a remote
or containerized Panoptikon ask your computer to open a locally mounted copy of
an indexed file. Start pairing from the remote web UI, then approve the
origin-bound request and any initial path mappings in Desktop's dedicated
pairing window. The window stays open until the secure exchange finishes;
closing it cancels the unfinished request. If a later file has no mapping,
Desktop opens a dedicated mapping window and resumes the blocked action after
the mapping is saved. Existing mappings and revocation remain available in
Desktop Settings.
Credentials are generated once, stored only as salted hashes by Desktop, and
can be revoked at any time. Relay listens on loopback only and does not execute
user-configurable shell commands. Desktop can run in Relay-only mode with its
local Server disabled.

## REST API

Panoptikon exposes a REST API that can be used to interact with the search and bookmarking functionality programmatically, as well as to retrieve the indexed data, the actual files, and their associated metadata. Additionally, `inferio`, the inference server, exposes an API under `/api/inference` that can be used to run batch inference using the available models.

The API is documented in the OpenAPI format. The interactive documentation can be accessed at `/docs` when running Panoptikon, for example at `http://127.0.0.1:6342/docs` by default. Alternatively, ReDoc can be accessed at `/redoc`, for example at `http://127.0.0.1:6342/redoc` by default.

API endpoints support specifying the name of the `index` and `user_data` databases to use, regardless of the configured defaults, through the `index_db` and `user_data_db` query parameters. If not specified, the configured default databases are used.

## 🛠 Installation

Prebuilt binaries are planned; until they arrive you build from source.

### Nix / NixOS

Packaging lives under `contrib/` (flake, packages, NixOS module). See
[`contrib/package/nix/README.md`](contrib/package/nix/README.md).

**Install only tagged releases** (pin the flake input to a release tag, e.g.
`github:reasv/panoptikon/v0.1.8`). master is the development branch — not
stable, and running it can leave your databases in a state the next release
cannot migrate cleanly.

```bash
nix build .#panoptikon   # from a checkout (development)
# NixOS: import inputs.panoptikon.nixosModules.default and the overlay,
# with the input pinned to a release tag
```

### Prerequisites

- **Git**
- **A Rust toolchain** (stable, via [rustup](https://rustup.rs/))
- **On Linux: OpenSSL development headers** (`libssl-dev` on Debian/Ubuntu,
  `openssl-devel` on Fedora/RHEL, `openssl` on Arch) — `native-tls` builds
  `openssl-sys` against the system OpenSSL, and without them the build fails
  with "Could not find directory of OpenSSL installation"

That's it — you do **not** need to install Python, uv, or Node.js.
`panoptikon setup` (below) finds or downloads [uv](https://docs.astral.sh/uv/),
which in turn fetches Python 3.12 and every locked dependency; the web UI
runs on the Node.js runtime bundled inside that same environment.

### Setup

1. Clone the repository **with submodules** (the web UI lives in the `ui/`
   submodule):

   ```bash
   git clone --recurse-submodules https://github.com/reasv/panoptikon.git
   cd panoptikon
   ```

   (For an existing clone: `git submodule update --init`.)

2. Build the server:

   ```bash
   cargo build --release -p panoptikon
   ```

3. Create the Python inference environment:

   ```bash
   target/release/panoptikon setup
   ```

   This finds `uv` on PATH (or downloads a pinned copy into `runtime/uv/`),
   detects your accelerator, creates `python/.venv`, and installs the locked
   dependency set for it. Accelerator selection is automatic — **CUDA** when
   an NVIDIA driver is present, **ROCm** on Linux with ROCm 7.2.x (pytorch.org
   multi-arch `rocm7.2` wheels), **MPS** on Apple Silicon, otherwise **CPU**.
   (macOS always gets the default PyPI wheels either way — `mps` and `cpu`
   install exactly the same torch there; the difference is that `mps` runs and
   prices work on the Metal device, and an explicit `accelerator = "cpu"` is
   the one way to make an Apple Silicon host run unaccelerated.) Override it
   with `--accelerator cuda|rocm|mps|cpu` or pin it in the config
   (`[inference_local.python_env] accelerator`). `--force` recreates the
   venv from scratch; re-running without it is a fast no-op.

   The first CUDA install downloads several GB of PyTorch wheels — watch the
   log. You can also skip this step entirely: on first start the server runs
   setup automatically when the environment is missing (disable with
   `[inference_local.python_env] auto_setup = false`).

#### Manual/custom environments

If you'd rather manage the Python environment yourself, point
`[inference_local].python` at any interpreter — the server never runs uv
against a user-configured interpreter (or against anything but
`python/.venv`). For a DIY environment with the repo's locked versions, use
the accelerator extras in `python/pyproject.toml` directly:
`uv sync --locked --extra cu128` (or `cpu`/`rocm`) inside `python/`.

### cuDNN

The Whisper implementation (via [CTranslate2](https://github.com/OpenNMT/CTranslate2/))
needs cuDNN, which the venv's `nvidia-cudnn-cu12` wheel normally provides.
As a legacy fallback you can also unpack a cuDNN package from
[Nvidia](https://developer.download.nvidia.com/compute/cudnn/redist/cudnn/)
into the `cudnn/` directory at the repo root (with `bin`, `lib`, `include`
as direct subfolders).

## Running Panoptikon

On Linux and macOS, run:

```bash
./start.sh
```

For Windows, run:

```bash
.\start.bat
```

Both run the release binary with the canonical configuration at
`config/server/default.toml`: the server owns the databases, jobs, cron, and
inference, and serves everything — UI included — on
**http://127.0.0.1:6342**.

On first start it will:

- create the Python inference environment if it is missing (`panoptikon
  setup` runs automatically; see Installation),
- create and migrate the databases under `data/`,
- install dependencies and produce a production build of the web UI (this
  takes a few minutes the first time; watch the log),
- start prewarming inference workers in the background.

Then open http://127.0.0.1:6342.

### Coming from the Python version

- Your existing `data/` folder works as-is: on first start the server
  verifies your databases are at the expected schema version and adopts
  them. If they are older, run the Python version (`python-legacy` branch)
  once to bring them up to date first.
- **Back up your `data/` folder before switching.**
- **Never run the legacy Python server and this server against the same
  data folder at the same time** — both would schedule cron and extraction
  jobs.

### Remote inference

A machine that only lends its GPU can run the standalone inference service,
and other Panoptikon instances (gateways) send it their inference work:

```bash
target/release/panoptikon inferio
```

This serves only the inference API (`/api/inference/*`). Run the same
Panoptikon version on the gateway and the inference server.

**Inference server.** The shipped `config/server/default.toml` listens on
`127.0.0.1` and its policies only allow `localhost`, so a gateway on another
machine cannot connect, or is refused with a 403. Set these keys in its
existing `[server]` table:

```toml
host = "0.0.0.0"                 # or the LAN IP; 127.0.0.1 is unreachable from the LAN
port = 7777
trust_forwarded_headers = false
```

and append this policy at the end of the file (the shipped `localhost`
policy can stay):

```toml
[[policies]]
name = "lan"
ruleset = "allow_all"
[policies.match]
hosts = ["192.168.1.16"]         # the name/IP the gateway puts in its inference base_url
[policies.index_db]
default = "default"
allow = "*"
[policies.user_data_db]
default = "default"
allow = "*"
```

`hosts` matches the host the request was sent to, not the client's address.
To allow every request that reaches the server's listener instead, use
`endpoints = ["default"]` in place of `hosts`. `trust_forwarded_headers =
true` trusts `X-Forwarded-Host` from any client that can reach the port, so
only enable it behind a reverse proxy you control.

**Gateway.** Set `enabled = false` in its existing `[inference_local]`
table, and point it at the server:

```toml
[[upstreams.inference]]
base_url = "http://192.168.1.16:7777"
```

**TLS in front of the server.** A reverse proxy that offers HTTP/2 through
ALPN or only HTTP/1.1 both work. For a self-signed certificate or a private
CA, the gateway must trust the issuing CA:

- Linux: start the gateway with `SSL_CERT_FILE=/path/to/ca.pem`. The file
  must contain the issuing CA; it replaces only the CA bundle file, and the
  system certificate directory (such as `/etc/ssl/certs`) is still read.
- Windows: import the CA into the Windows certificate store (Trusted Root
  Certification Authorities). `SSL_CERT_FILE` has no effect.
- macOS: add the CA to the Keychain and mark it trusted. `SSL_CERT_FILE` has
  no effect.

The proxy must pass the `Host` the server's policy matches (Caddy does by
default; nginx needs `proxy_set_header Host $host;`).

A single inference request can be about 1 GiB (one very large input, such
as long audio, is sent alone and can be larger). Raise the proxy's body
limit accordingly, or items fail with 413 on every run: nginx's default
`client_max_body_size` is 1 MiB, so set `client_max_body_size 0;` (no
limit) or `2g`; Caddy has no limit unless a `request_body { max_size … }`
is set. A request can also wait minutes for its answer while the server is
busy, and nginx's 60 s `proxy_read_timeout` then answers 504: set
`proxy_read_timeout` and `proxy_send_timeout` to a large value such as
`1h`.

Over HTTP/1.1 every request in flight is a connection of its own: the
gateway opens up to 256, more when the server asks for more, up to 4096 or
(hard `nofile` limit - 256) / 2, whichever is lower (see "File descriptors"
below). A proxy in front must accept twice that many connections (nginx
counts both sides against `worker_connections`).

A server that stops answering (a frozen process) is noticed through the
proxy too: once a request has waited 30 s, the gateway checks the server's
`/api/inference/health`, and after two checks go unanswered (about 50 s) it
fails the waiting requests and the new ones until the server answers again.
A running job then ends `partial`, or `failed` if no item had succeeded;
either way its items are owed, and the next run retries them.

The check goes through the proxy on a connection of its own, over HTTP/2 or
HTTP/1.1 as the requests are. A proxy that caps its connections to the
server (HAProxy `maxconn`, nginx `max_conns`) can queue the check behind
predictions until it times out. A busy server is then taken for frozen when
no request from this gateway is answered during two checks in a row (about
20 s): with batches longer than that, or another gateway keeping the server
busy, raise the cap well above the requests in flight, or exempt
`/api/inference/health`.

See the configuration reference in
[`panoptikon/README.md`](panoptikon/README.md) for every
`[[upstreams.inference]]` key.

## First Steps

Open the home page of the web UI and follow the instructions to get started. You'll have to add directories to the list of allowed paths and then run the file scan job to index the files in those directories. Before being able to search, you'll also have to run data extraction jobs to extract text, tags, and other metadata from the files.

## Bookmarks

You can bookmark any search result by clicking on the bookmark button on each thumbnail. Bookmarks are stored in a separate database and can be accessed through the API, as well as through search.

To search in your bookmarks, open Advanced Search and enable the bookmarks filter, which will show you only the items you've bookmarked.

Bookmarks can belong to one or more groups, which are essentially tags that you can use to organize your bookmarks. You can create new groups by typing an arbitrary name in the Group field in Advanced Search and selecting it as the current group, then bookmarking an item.

## Adding More Models

See `config/inference/example.toml` for examples on how to add custom models from Hugging Face to Panoptikon.

## Configuration

All global configuration is TOML: the server reads the all-in-one
`config/server/default.toml` (override with `--config` or
`PANOPTIKON_CONFIG_PATH`). Environment variables are no longer a parallel
configuration mechanism: string values in the TOML (and in every inference
registry TOML) can reference environment variables with `${VAR}` /
`${VAR:-default}` templating, and a `.env` file in the repo root is still
auto-loaded as a convenient source for those variables (see `.env.example`).
Inference registry templates and declared worker external inputs are resolved
again before each new Python worker is spawned, so edits do not require a
Panoptikon restart. Desktop provides an Additional configuration UI backed by
its managed Server root `.env`.
Numeric and boolean keys can be templated too, as quoted whole-value
templates (e.g. `port = "${PORT:-6342}"` — coerced to the key's type at
load). The remaining real environment variables are bootstrap/diagnostic:
`PANOPTIKON_ROOT`, `PANOPTIKON_CONFIG_PATH` and `RUST_LOG`.

See [`panoptikon/README.md`](panoptikon/README.md) for the full configuration
reference: every key, the templating syntax, and policies and rulesets.

# Docker

The official image (`ghcr.io/reasv/panoptikon`, linux/amd64) packages
everything in one container: the Rust binary, a native Node.js for the web
UI, and the Python inference environment — no nginx, no separate UI services.
Three variants are published: a CPU image (`:latest`), a **CUDA image**
(`:latest-cuda`) for NVIDIA GPUs and a **ROCm image** (`:latest-rocm`) for
AMD GPUs — most users want a GPU one, see [GPU (CUDA)](#gpu-cuda) and
[GPU (AMD ROCm)](#gpu-amd-rocm) below. All include the optional PDF and HTML
renderers (bundled `libpdfium` and a headless Chrome).

You do **not** need to clone the repository. Download the compose file into
an empty directory and start it (this uses the CPU image):

```bash
curl -fsSLO https://raw.githubusercontent.com/reasv/panoptikon/master/deploy/docker-compose.yml
docker compose up -d
```

Then open http://localhost:6342. The container runs a single server process
with **two listeners**:

- **6342 — private admin** (full API): mapped to `127.0.0.1` only in the
  compose file. The API on this port can open files and trigger arbitrary
  command execution inside the container — **never** expose it to the
  internet or untrusted networks; reach it remotely via an SSH tunnel or
  VPN, or put an authenticating reverse proxy in front.
- **6339 — public restricted**: locked by an endpoint-scoped policy to the
  `restricted_demo` ruleset (search, item/thumbnail/file serving,
  bookmarks). This is the Rust equivalent of the Python-era "restricted
  mode" service. Still add authentication at a reverse proxy before
  exposing it publicly — it serves your indexed files.

To index your media, uncomment and edit the media bind mounts in the
compose file (e.g. `/path/to/pictures:/media/pictures:ro`), restart, then
add the container-side paths as allowed folders in the UI and run a file
scan. Databases, configuration, and the model cache live on named volumes
and survive image updates; the gateway config
(`config/server/docker.toml` on the config volume, seeded from the image
on first run) is user-owned — edit it and restart to reconfigure.

Since the server cannot open files on *your* machine from inside a
container, pair it with [Panoptikon Relay](https://github.com/reasv/panoptikon-relay)
on your client (see above).

**Running as another user.** The container runs as the image's `ubuntu` user
(uid 1000), which owns `/app` and everything on the three volumes.

- **root** (`--user 0`, `user: "0"`, or a host that only runs containers as
  root) works, with two differences. Models are downloaded to `/root/.cache`,
  so mount the cache volume there instead of `/home/ubuntu/.cache`. And what
  root writes to the volumes belongs to root: once that includes a database,
  a later start as the default user stops with an error naming it. Hand the
  volumes back first (with the cache volume at its default mount point):

  ```bash
  docker compose run --rm --user 0 --entrypoint chown panoptikon \
    -R ubuntu:ubuntu /app/data /app/config /home/ubuntu/.cache
  ```
- **Any other uid** (`--user 1234`) is not supported: the server has to write
  to `/app/runtime` inside the image, and stops at startup saying so.

The server uses `/app` whatever the working directory, through
`PANOPTIKON_ROOT` and `PANOPTIKON_CONFIG_PATH`: the image sets both in its
environment, and in `/etc/environment` for login sessions (SSH on a rented GPU
host). A shell with neither starts an empty root in its own directory and
reports no Python environment; pass both there:
`panoptikon --root /app --config /app/config/server/docker.toml accelerator`.

**File descriptors.** Local inference is served over loopback HTTP by the same
process that calls it, so each batch item in flight costs about two sockets;
container runtimes commonly start a process at a soft `nofile` limit of 1024.
The server raises its own soft limit to the hard limit at startup, which is
enough on Docker's defaults (hard limit 524 288) and needs no configuration.
If you deliberately run with a low **hard** limit, the server bounds how much
work it keeps in flight to fit — roughly `(hard_limit - 256) / 2` items — so
batches simply stop growing instead of failing; for full pipelining give it a
hard limit of at least ~8 500 (`ulimits: nofile:` in compose, `--ulimit
nofile=` for `docker run`), which is what the shipped 4096-item ceiling needs.

### GPU (CUDA)

For NVIDIA GPU inference — recommended, and what most users want — use the
published CUDA image (`ghcr.io/reasv/panoptikon:latest-cuda`) via its own
compose file. It needs an NVIDIA GPU with recent drivers and the
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/index.html);
no repo clone required:

```bash
curl -fsSLO https://raw.githubusercontent.com/reasv/panoptikon/master/deploy/docker-compose.cuda.yml
docker compose -f docker-compose.cuda.yml up -d
```

The CUDA compose passes the host GPU(s) into the container; everything else
(ports, volumes, media mounts) matches the CPU compose.

### GPU (AMD ROCm)

For AMD GPUs, use the published ROCm image
(`ghcr.io/reasv/panoptikon:latest-rocm`). It needs a Linux host with the
`amdgpu` kernel driver and a GPU that ROCm 7.2 supports; the image carries its
own ROCm libraries, so nothing ROCm needs to be installed on the host. The
container user must be in the host's `render` group, whose id differs between
distributions, so record it in a `.env` file next to the compose file first:

```bash
curl -fsSLO https://raw.githubusercontent.com/reasv/panoptikon/master/deploy/docker-compose.rocm.yml
echo "RENDER_GID=$(getent group render | cut -d: -f3)" > .env
docker compose -f docker-compose.rocm.yml up -d
```

The ROCm compose passes `/dev/kfd` and `/dev/dri` (every AMD GPU) into the
container; everything else matches the CPU compose.

**Building from source instead of pulling** — for development or local
changes — use the repo-root `docker-compose.yml`, which builds the image with
the `ACCELERATOR` build arg (`cuda` by default, `cpu` or `rocm` to override;
for `rocm`, swap its NVIDIA `deploy:` block for the ROCm compose's `devices:`
and `group_add:`):

```bash
git clone --recurse-submodules https://github.com/reasv/panoptikon.git
cd panoptikon
docker compose up -d --build              # ACCELERATOR=cpu for a CPU image
```

# License

Panoptikon is free software released under the [GNU Affero General Public License v3.0 or later](LICENSE) (AGPL-3.0-or-later).

You may use, modify, and redistribute it under the terms of that license. If you run a modified version of Panoptikon as a network service, the AGPL requires you to offer the modified source code to that service's users.

## Contributions

Panoptikon is written and copyrighted by a single author. Contributions are welcome, but are accepted only under the terms in [CONTRIBUTING.md](CONTRIBUTING.md): by submitting a contribution you assign its copyright to the maintainer and agree to its release under the AGPL. Read that file before opening a pull request.
