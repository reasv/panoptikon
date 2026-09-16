# run4-desktop — the Desktop app (Tauri) on Apple M3 Max, MPS

First calibration leg ever driven through `panoptikon-desktop`: the bundled
`.app`, its own `--root`, its own first-run managed venv, its own gateway.

| | |
|---|---|
| Host | MacBook Pro M3 Max, 128 GiB, macOS 26.6.2 (25G83), arm64 |
| Commit | `41b4fde1` (branch `run4-mac-desktop` on the Mac clone), `ui` at `ce7506a` |
| Toolchain | rustup cargo/rustc 1.97.1, node v22.2.0, npm 10.8.3, Xcode CLT at `/Applications/Xcode.app` |
| App | `Panoptikon Desktop Dev` 0.1.8, identifier `app.panoptikon.desktop.dev`, gateway 16342 / UI 16340 |
| Server root | `~/Library/Application Support/app.panoptikon.desktop.dev/server` |
| Wall clock | ~18:56–19:19 UTC 2026-09-16 (~23 min: 4 min build, ~13 min run) |

Launch method: `open -n "<…>/Panoptikon Desktop Dev.app"` from an ssh session
into the machine's live console GUI session. That is a real Launch Services
launch of the real bundle, with the real `env_clear` + `host_env` sidecar
spawn — no gap on the server-launch path. What was *not* exercised is the
Tauri UI itself (tray menu, setup window), because the tray cannot be clicked
over ssh; every check below went through the gateway the app started.

---

## Build verdict

**The Desktop app builds on this Mac.** The two failures seen are both
environmental/prerequisite, not code.

### B1 — a bare `cargo build -p panoptikon-desktop` cannot succeed (expected)

    cargo:rustc-env=TAURI_ENV_TARGET_TRIPLE=aarch64-apple-darwin
    resource path `binaries/panoptikon-aarch64-apple-darwin` doesn't exist
    warning: build failed, waiting for other jobs to finish...
    BARE_RC=101

(`cargo-desktop-bare.log`.) This is `tauri_build::build()` validating
`bundle.externalBin = ["binaries/panoptikon"]`
(`panoptikon-desktop/src-tauri/tauri.conf.json:31`) against the host triple.
The repository only carries `binaries/panoptikon-x86_64-unknown-linux-gnu`, so
the crate — and therefore `cargo build --workspace` — is unbuildable on any
host until the Server sidecar has been built and installed as
`panoptikon-<host-triple>`. That is what
`scripts/desktop-dev-macos-lib.sh:104` (`desktop_dev_build_sidecar`) does.
**This is the whole of the earlier "the workspace build fails on
panoptikon-desktop" note: neither a toolchain nor a code problem, just a build
step that must run first.** `cargo 1.97.1` is far above the edition-2024 floor
the script checks for (1.85).

### B2 — `scripts/build-desktop-dev.sh --release-desktop`: app yes, DMG no

Everything up to and including the signed app bundle succeeded (3 min 55 s,
`build.status`, `build-desktop.log`): npm ci, standalone Next.js UI build,
pinned PDFium staged (build 6899), release sidecar built and staged, Tauri
release build, ad-hoc signature applied.

    Bundling Panoptikon Desktop Dev_0.1.8_aarch64.dmg …
    Running bundle_dmg.sh
    failed to bundle project: error running bundle_dmg.sh: `failed to run …/bundle_dmg.sh`
    BUILD_RC=1

Cause, confirmed directly:

    $ osascript -e 'tell application "Finder" to get name of startup disk'
    33:37: execution error: Finder got an error: AppleEvent timed out. (-1712)

`bundle_dmg.sh` styles the mounted volume through Finder AppleScript, and an
ssh session cannot drive Finder. The `.app` is complete and correctly signed
(`adhoc,runtime`, `libpdfium.dylib` in `Contents/Frameworks`, both binaries in
`Contents/MacOS`), which is all this leg needed. A release cut from an
interactive session is unaffected; see finding F2 for the diagnosability
problem this exposes.

---

## The six checks

### 1. First-run `setup` creates the managed venv and converges — PASS

`panoptikon-desktop.log`, sidecar pid 37230:

| | |
|---|---|
| `started Panoptikon Server sidecar` | 19:06:18.173 |
| `running setup automatically` | 19:06:18.221 (reason: no Python inference environment) |
| uv 0.11.28 downloaded (21 MiB), checksum verified | 19:06:18.2 → 19:06:19.48 |
| `creating the managed venv (uv venv)` | 19:06:19.985 |
| `uv sync`: Resolved 138, Prepared 6, **Installed 91 packages in 1.00 s** | 19:06:20.0 → 19:06:22.23 |
| ffmpeg/ffprobe fetched (static-ffmpeg) | 19:06:24.688 |
| `Python inference environment is ready … extra="mps"` | 19:06:24.688 |

**6.47 s wall, end to end.** Managed path exactly as designed:
`…/app.panoptikon.desktop.dev/server/runtime/venv` (`paths.rs:31`
`server_root = local_data_dir/server`), 1.5 GiB on disk.

Wheels: **torch 2.7.1, torchvision 0.22.1, torchaudio 2.7.1**, the default
PyPI macOS/arm64 wheels — `accelerator selected requested=Auto accelerator=Mps
extra="mps" evidence="macOS: default PyPI wheels (MPS on Apple Silicon)"`.

Caveat on the 6.47 s: `~/.cache/uv` was already warm from earlier calibration
runs on this machine (only 6 of 97 packages were downloaded). This is a
correct *convergence* figure, not a cold-download figure. The uv binary itself
was fetched fresh, so the download/verify/extract path did run.

Idempotence: the second launch (19:17:38.121) skipped setup entirely and had
the gateway listening **58 ms** later.

### 2. Server up, MPS-priced device with a `base_method` — PASS

`gateway listening endpoint=default address=127.0.0.1:16342` at 19:06:24.722;
embedded UI up at 19:06:27.419; `h264_videotoolbox` validated.

`health-t0.json` — the device is priced as unified MPS memory from the start:

```
"gpus":  [{"index":0,"uuid":"GPU-MPS","name":"Apple M3 Max (128 GB)",
           "total_mb":98304,"unified_ram_mb":131072}]
"vram":  [{"limit_mb":98304,"reserve_rule":"capped_default","margin":0.1}]
```

98304 MB is the documented 75 % seed; once a worker reported the exact
recommended-max the total moved to **110100 MB** with `external_source:"mps"`,
`external_known:true`, `reserve_mb:1024`, `reserve_rule:"capped_default"`
(`health-end.json`).

`base_method` (`health-loaded.json`, a live worker):

```
"replicas_detail":[{"gpu_name":"Apple M3 Max (128 GB)","torch_version":"2.7.1",
  "base_mb":1384,"base_method":"mps","dtype":"fp32",
  "free_mb":108960,"total_mb":110100,"free_source":"mps"}]
```

and persisted for both models in `calibration.after.toml`
(`base_method = "mps"`, `backend = "mps"`, `platform = "macos"`,
`arch = "apple-m3"`).

### 3. 300 images + text files, tags job, clip job, search — PASS, 0 failed items

Corpus generated with
`corpus.py --tier ramp --scale 0.15` → 300 × 1024×1024 JPEG, plus 5 text
files. **The 5 `.txt` files were not indexable at all** — see finding F3;
re-authored as `.html` with `scan_html = true`, they indexed immediately.
Final scan: `total_available: 305, new_files: 5, errors: 0` (`folders2.json`).

| job | setter | segments | image/other | errors | input_errors | failed_items | wall | inference_time |
|---|---|---|---|---|---|---|---|---|
| 1 | `tags/wd-vit-tagger-v3` | 305 | 300 / 5 | 0 | 0 | **0** | 45 s | 10.945 s |
| 2 | `clip/apple_MobileCLIP-S1` | 305 | 300 / 5 | 0 | 0 | **0** | 32 s | 4.029 s |

`total_remaining: 0`, `outcome: "completed"` for both (`jobs-hist.json`).
`/api/jobs/data/failures` → `{"total":0,"job_failures_total":0,
"failed_jobs_total":0}` for every database used (`failures-*.json`).
Every queue entry (1–12) finished `completed`, `error: null`.

Searches (`search-*.json`):

* full PQL select → `count: 305`, first result served;
  `/api/items/item/thumbnail` → 200, 40 019 bytes.
* `match_tags` `["1girl"]`, setter `tags/wd-vit-tagger-v3`,
  `min_confidence 0.2` → **38 hits** — the tags job's output is queryable.
* `image_embeddings` with a *text* query ("a colourful abstract pattern")
  against `clip/apple_MobileCLIP-S1` → 200, 305 ranked results — the clip
  job's embeddings plus live text-side encoding both work on MPS.

### 4. Settings API round-trip; "max batch size" on auto does not override the ledger — PASS

Round-trip: `GET /api/jobs/config` → `PUT` verbatim → `GET` returns a
byte-identical config (`identical: true`, `events.json`). An explicit
per-model *auto* entry survives as auto — `null`, not coerced to a number:

```
[{"group_name":"tags","inference_id":"tags/wd-vit-tagger-v3",
  "default_batch_size":null,"default_threshold":null},
 {"group_name":"clip","inference_id":"clip/apple_MobileCLIP-S1",
  "default_batch_size":null,"default_threshold":null}]     preserved: true
```

and an explicit cap round-trips as `8`, then restores to `null`
(`events3.json`, `config-auto-get.json`, `config-final.json`).

Auto really leaves the ledger alone. The contrast leg (`cal2`, same model,
same corpus, `default_batch_size = 8`):

| | queue row `batch_size` | `data_log.batch_size` |
|---|---|---|
| auto (both jobs in `cal`) | `null` | **0** |
| capped (`cal2`) | `8` | **8** |

`0` is the "unset" sentinel (`extraction.rs:311-315`), and
`gpu_batch_cap(None) → None` (`extraction.rs:305-310`) means no ceiling is
forwarded to the inference API at all. Positive evidence that the *registry's*
`default_batch_size` also stayed out of it: the tags group declares
`default_batch_size: 64` (`metadata.json`), yet on auto the ledger measured
rungs **1, 2, 4, 8, 16, 32** from the bottom (`calibration.after.toml`
`sample_units`) — exactly the "core no longer invents a batch size" rule at
`extraction.rs:2578-2582`.

### 5. No policy 403 on the desktop profile (P1 desktop parity) — PASS

12 probes, six endpoints × `Host: 127.0.0.1` and `Host: localhost`
(`policy-probe.json`): `/api/client-config`, `/api/inference/health`,
`/api/jobs/queue`, `/api/jobs/config`, `/api/inference/metadata`, `/api/db`.
**All 200. Zero 403.** The seeded `config/server/desktop-dev.toml` carries the
`allow_all` ruleset with `[policies.client] desktop = true` and
`hosts = ["localhost","127.0.0.1"]`, and that is what the app ran under.

### 6. Quit reaps the workers — PASS (with finding F4)

Before quit (`ps-before-quit.txt`): shell 37228 → sidecar 37230 → **4 ×
`runtime/venv/bin/python -m inferio_worker`** (37574, 37676, 37678, 37706).

After `osascript … to quit` (`ps-after-quit.txt`): nothing.
`pgrep -f inferio | wc -l` → **0**. No stray `node`/`next-server` either.
The server log shows a clean 1.15 s teardown:

```
19:16:02.802  Desktop parent control channel closed; shutting down
19:16:02.804  UI server stopped
19:16:02.808  index DB writers drained writers=3
19:16:02.808-.821  Unloaded; exiting.  ×4  (wd_tagger ×2, openclip ×2)
19:16:03.187  wrote the local calibration store … profiles=2
19:16:03.187  local inference workers stopped / background work stopped cleanly
19:16:03.187  gateway stopped
```

Reproduced identically on the second launch (19:18:03). Workers are reaped —
but by the *fallback* path, not the supervised one; see F4.

---

## Ramp hold / held rung, as the desktop user would see it

All four ledger hold/release events for this leg (`panoptikon-server.log`):

| model | rung (units) | certified | knee_binds | reason logged |
|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | **16** | true | false | the rung is the top of a measured plateau |
| `tags/wd-vit-tagger-v3` | — | — | — | *the throughput ramp is free to grow again* |
| `tags/wd-vit-tagger-v3` | **64** | true | **true** | a knee caps the sizes a doubling would have to measure at |
| `clip/apple_MobileCLIP-S1` | **64** | **false** | false | the ring cannot certify this rung yet |

Settled calibration after the leg (`calibration.after.toml`, schema 3,
`arch = "apple-m3"`, `platform = "macos"`, `backend = "mps"`, `torch = "2.7.1"`,
`dtype = "fp32"`, `unit = "item"`):

| | `tags/wd-vit-tagger-v3` | `clip/apple_MobileCLIP-S1` |
|---|---|---|
| `base_mb` / `base_method` | 1384 / `mps` | 1160 / `mps` |
| `slope_mb_per_unit` | 46.375 | 6.2367 |
| `residual_mb` | 87.75 | 9.05 |
| `knee_units` | **3** (`knee_clean_windows = 7`) | none |
| `max_units_measured` | 32 | 64 |
| `samples` / `local_samples` | 7 / 40 | 8 / 19 |
| `sample_units` | 1,2,4,8,16,32,3 | 1,4,8,16,32,64,51,2 |

So the desktop user's first two jobs end with the tagger pinned to a knee at
3 units on this GPU and the clip model uncapped but not yet certified past 64
— consistent with the earlier MPS legs on this machine, and reached here with
no configuration at all beyond pressing Add Folder.

---

## Findings

**F1 — `panoptikon-desktop` is unbuildable without a pre-staged sidecar, and
the error does not say so.**
`panoptikon-desktop/src-tauri/tauri.conf.json:31` declares
`"externalBin": ["binaries/panoptikon"]`; only the linux-x86_64 sidecar is
committed. `tauri_build::build()` (`panoptikon-desktop/src-tauri/build.rs:17`)
then aborts with `resource path 'binaries/panoptikon-aarch64-apple-darwin'
doesn't exist`, which reads like a missing repository file rather than "run
`scripts/build-desktop-dev.sh` first". Consequence: plain `cargo build
--workspace`, `cargo test --workspace` and any editor/rust-analyzer check fail
on every host but linux-x86_64. Explains the pre-existing note. Low severity,
high nuisance; a one-line note in `docs/desktop-release.md` next to the macOS
script section, or a `build.rs` pre-check with a better message, would retire
it.

**F2 — the DMG bundling failure is undiagnosable from the build log.**
`scripts/build-desktop-dev.sh` ends with
`failed to bundle project: error running bundle_dmg.sh` and *no cause*: the
Tauri CLI discards the script's stderr. The actual cause here is that
`bundle_dmg.sh` drives Finder over Apple Events and no ssh session can
(`-1712 AppleEvent timed out`, reproduced directly). Anyone building over ssh
or in a headless CI-like shell gets a dead end. Worth documenting in
`docs/desktop-release.md` ("macOS development app, DMG, and clean-state
testing"), which currently lists only the Xcode-license prerequisite. Note the
`.app` is produced and signed before this step, so `--bundles app` is a
working escape hatch.

**F3 — plain text files can never be indexed.**
`panoptikon/src/jobs/files/mod.rs:6368` `build_extension_set` gates on
`scan_images`, `scan_video`, `scan_audio`, `scan_html` (`.html`/`.htm`) and
`scan_pdf` — there is no `.txt`/`.md` extension anywhere and no config flag
that would add one. A desktop user who points Panoptikon at a notes folder
gets `total_available: 0` with no error, no warning and nothing in the scan
history to explain it (verified: the 5 `.txt` files produced
`total_available: 300` against 305 files on disk; renaming them `.html` with
`scan_html = true` indexed all 5). Either the extension set should cover
plain text, or the scan history should report "N files skipped: unsupported
extension" so the silence ends.

**F4 — on macOS an Apple-Event quit bypasses the Desktop shell's own quit
path, losing the supervised stop and the log tail.**
`quit_inner` (`panoptikon-desktop/src-tauri/src/lib.rs:2344-2350`) is the
intended shutdown: shut the Relay down, `Supervisor::stop(&app, false)` —
which owns the shutdown deadline and the kill fallback at
`supervisor.rs:340-362` — then `app.exit(0)`. A quit sent as an Apple Event
(`osascript … to quit`, and therefore also ⌘Q and Dock → Quit) does not reach
it, and is not stopped by the
`RunEvent::ExitRequested { code: None } => api.prevent_exit()` arm at
`lib.rs:354-361` either: the process simply goes away. Evidence:

* The Desktop log stops mid-stream and never records the quit. Both launches:
  270 lines, last entry `19:17:38.738 … prewarmed worker parked`, nothing
  after. The `tracing_appender` `WorkerGuard` held as `RuntimeState._log_guard`
  (`lib.rs:32`, created in `init_logging`, `lib.rs:368`) is never dropped, so
  the buffer is never flushed.
* Sessions in this profile's older logs that ended through the tray's Quit
  item *do* end correctly, with `gateway stopped` followed by
  `Server sidecar exited code=Some(0)` (2026-08-12, 09-06, 09-10) — the
  contrast that isolates the path.
* The sidecar still shuts down gracefully, but because its parent pipe closed:
  `panoptikon::shutdown: Desktop parent control channel closed; shutting down`.

Net effect today is benign — every worker was reaped, twice, and the
calibration store was written — because the server's own fallback is solid.
What is lost is the shell's Relay shutdown, the supervised stop with its
deadline and kill fallback, and all shell-side logging of the exit, on what is
the ordinary way to quit a macOS app. Worth an `applicationShouldTerminate` /
`ExitRequested { code: Some(_) }` bridge into `quit_inner`, or at minimum an
explicit flush.

**F5 — minor API ergonomics met while driving the app.**
`POST /api/jobs/data/extraction` answers **202**, not 200, and rejects a bare
inference id with `{"detail":"Inference ID must be in group/id format"}` —
both fine, but note `tools/calibration-protocol/legs.py:812` posts
`inference_ids=<model>` from `--model`/`--models`, so a leg driven with a bare
id would 400. Separately, `PUT /api/inference/load/{group}/{id}`
(`panoptikon/src/inferio/http.rs:462`) has three required query parameters
with no defaults and surfaces them one at a time
(`missing field 'cache_key'` → `'lru_size'` → `'ttl_seconds'`), costing three
round-trips to discover.

---

## Artifacts

Everything in this directory came off the Mac. `corpus/` (300 JPEG + 5 HTML,
13.8 MiB) is excluded; regenerate it with
`tools/calibration-protocol/corpus.py --tier ramp --scale 0.15 --seed 20260903`
— `corpus-manifest.json` is the manifest it produced.

| file | what |
|---|---|
| `cargo-desktop-bare.log` | the bare `cargo build -p panoptikon-desktop` failure (B1) |
| `build-desktop.log`, `build.status`, `build-driver.sh` | the full `build-desktop-dev.sh --release-desktop` run (B2) |
| `panoptikon-desktop.log` | the Desktop shell's own log, both launches (F4: ends mid-stream) |
| `panoptikon-server.log` | the sidecar's log: setup, jobs, ramp holds, shutdown |
| `calibration.after.toml` | the calibration store the leg produced |
| `health-t0.json`, `health-loaded.json`, `health-end.json` | device pricing and `base_method` |
| `jobs-hist.json`, `jobs-cal2.json`, `failures-*.json` | job outcomes; `batch_size` 0 vs 8 |
| `search-pql.json`, `search-tags.json`, `search-semantic.json`, `thumb.bin` | the three searches and a served thumbnail |
| `policy-probe.json` | the 12 host/endpoint probes, all 200 |
| `config-*.json`, `events*.json` | settings round-trips and the timed event stream |
| `ps-before-quit.txt`, `ps-after-quit.txt` | worker reaping |
| `drive.py` … `drive4.py` | the drivers, in order |

The Mac was left clean: app quit, no processes of this leg running,
`caffeinate` killed, the pre-existing `app.panoptikon.desktop.dev` profile
restored from backup, the clone on `run4-mac-desktop` with a clean tree.
