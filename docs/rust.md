# Running the Rust version of Panoptikon

Panoptikon has been reimplemented in Rust. The new implementation is **feature-complete**: it serves the full API, the job system and cron scheduler, file scanning (including continuous scanning), PQL search, database migrations, and the inference orchestrator, and it builds and runs the production web UI for you. Python is still used for one thing, by design: the actual inference workers that load and run the AI models.

The Rust version is the future of Panoptikon - the Python implementation will be replaced by it soon. It is currently in a **testing phase**, so convenient prebuilt binaries and simple installers are not available yet and you have to build it from source as described below. Once the testing phase is over, the Rust version will be much easier to install and run than the Python version ever was: the plan is self-contained binary releases that manage their own Python inference environment, configuration, and UI.

If you just want the stable, documented path today, follow the normal installation instructions in the [main README](../README.md). If you want to run the future version, read on.

## Prerequisites

- **Git**
- **A Rust toolchain** (stable, via [rustup](https://rustup.rs/))
- **Python 3.12 and the platform requirements** from the normal install path - the Rust server spawns Python workers for inference, so the Python environment is still required. You do **not** need Node.js: the Python venv bundles it, and the Rust server uses that to build and run the web UI.

## Setup

1. Clone this repository, and clone the UI **as a sibling directory** next to it:

   ```bash
   git clone https://github.com/reasv/panoptikon.git
   git clone https://github.com/reasv/panoptikon-ui.git
   cd panoptikon
   ```

   (The default configuration expects the UI checkout at `../panoptikon-ui`; you can point it elsewhere with the `[upstreams.ui] dir` config key.)

2. Create the Python environment using the normal install script for your platform, exactly as described in the [main README](../README.md) - `install-nvidia.bat` / `install-cpu.bat` on Windows, `./install.sh` / `./install-amd.sh` on Linux. This creates the `.venv` the inference workers run from.

3. Build the server:

   ```bash
   cargo build --release -p gateway
   ```

## Running

From the repository root:

- **Windows:** `start-rust.bat`
- **Linux/macOS:** `./start-rust.sh`

Both run the release binary with the all-in-one configuration at [`config/gateway/local.toml`](../config/gateway/local.toml): the Rust server owns the databases, jobs, cron, and inference, and serves everything on **http://127.0.0.1:6342** (the same port the Python server used).

On first start it will:

- create and migrate the databases under `data/` (or adopt your existing ones - see below),
- install dependencies and produce a production build of the web UI (this takes a few minutes the first time; watch the log),
- start prewarming inference workers in the background.

Then open http://127.0.0.1:6342.

## Coming from the Python version

- Your existing `data/` folder works as-is: on first start the Rust server verifies your databases are at the expected schema version and adopts them. If they are older, run the Python version once to bring them up to date first.
- **Back up your `data/` folder before switching.** The adoption step is conservative and non-destructive, but you want the backup anyway.
- **Never run the Python server and the Rust server against the same data folder at the same time** - both would schedule cron and extraction jobs, duplicating work at best.
- Your `.env` file is still loaded, but global configuration now lives in TOML: config values can reference environment variables with `${VAR}` / `${VAR:-default}` templating. See the configuration reference in [`gateway/README.md`](../gateway/README.md) for the full key list and templating syntax.

## Odds and ends

- A machine that only lends its GPU can run the standalone inference service: `gateway inferio --config config/gateway/local.toml` serves only the inference API for other Panoptikon instances to use.
- Custom inference implementations still go in `inferio_custom/`, and custom model definitions in `config/inference/`, same as the Python version.
- Found a problem? Please open an issue - real-world testing is exactly what this phase is for.
