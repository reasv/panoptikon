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
| **Windows** · x86-64 | (coming soon) |
| **Linux** · x86-64 | (coming soon) |
| **macOS** · Apple Silicon | (coming soon) |

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

## This is the Rust implementation

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

Relay is built into Panoptikon Desktop. It lets a remote or containerized
Panoptikon ask your computer to open a locally mounted copy of an indexed file.
Enable Relay in Desktop, start pairing from the remote web UI, approve the
origin-bound request locally, then configure component-aware path mappings.
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

### Prerequisites

- **Git**
- **A Rust toolchain** (stable, via [rustup](https://rustup.rs/))

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
   an NVIDIA driver is present, **ROCm** on Linux with a ROCm install
   (untested), otherwise **CPU**; macOS always gets the standard PyPI wheels
   (which include MPS support on Apple Silicon). Override it with
   `--accelerator cuda|rocm|cpu` or pin it in the config
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

### Running inference on a separate machine

A machine that only lends its GPU can run the standalone inference service:

```bash
target/release/panoptikon inferio
```

This serves only the inference API (`/api/inference/*`). Point other
Panoptikon instances at it with an `[[upstreams.inference]]` entry in their
config — see the configuration reference in
[`panoptikon/README.md`](panoptikon/README.md).

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
Numeric and boolean keys can be templated too, as quoted whole-value
templates (e.g. `port = "${PORT:-6342}"` — coerced to the key's type at
load). The remaining real environment variables are bootstrap/diagnostic:
`PANOPTIKON_CONFIG_PATH` and `RUST_LOG`.

See [`panoptikon/README.md`](panoptikon/README.md) for the full configuration
reference: every key, the templating syntax, and policies and rulesets.

# Docker

The official image (`ghcr.io/reasv/panoptikon`, linux/amd64) packages
everything in one container: the Rust binary, a native Node.js for the web
UI, and the Python inference environment — no nginx, no separate UI services.
Two variants are published: a CPU image (`:latest`) and a **CUDA/GPU image**
(`:latest-cuda`) — most users want the GPU one, see [GPU (CUDA)](#gpu-cuda)
below. Both include the optional PDF and HTML renderers (bundled `libpdfium`
and a headless Chrome).

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

**Building from source instead of pulling** — for development or local
changes — use the repo-root `docker-compose.yml`, which builds the image with
the `ACCELERATOR` build arg (`cuda` by default, `cpu` to override):

```bash
git clone --recurse-submodules https://github.com/reasv/panoptikon.git
cd panoptikon
docker compose up -d --build              # ACCELERATOR=cpu for a CPU image
```

# License

Panoptikon is free software released under the [GNU Affero General Public License v3.0 or later](LICENSE) (AGPL-3.0-or-later).

You may use, modify, and redistribute it under the terms of that license. If you run a modified version of Panoptikon as a network service, the AGPL requires you to offer the modified source code to that service's users.
