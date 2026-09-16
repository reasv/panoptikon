# run4-deploy — the deployment paths the GPU campaign never ran

What happens to a **real user** on the day they upgrade to PR #27
(`claude/batch-calibration-coverage-db9ab9` @ `0d7f5671`) from what they run
today (`master` @ `7aa92b20`). Everything here is **CPU-only**: every gateway
was started with `CUDA_VISIBLE_DEVICES=""`, every Python environment is the
`cpu` extra (`torch 2.7.1+cpu`), and no container was given `--gpus`.

Host: 48 cores, 128 GiB RAM (`MemTotal 131737460 kB`, reported by the
orchestrator as `CPU (128 GB)`, `total_mb 128649`), Linux 6.12.73.
All state under `$TMPDIR=$HOME/tmp-run4-upgrade`. Binaries: `cargo build
--release -p panoptikon` in `/home/admin/projects/panoptikon-master` (master)
and in this worktree (branch).

| Scenario | Verdict |
|---|---|
| A — upgrade master → branch on a live data folder | **PASS with two defects** (D1 job history, D2 accelerator switch); downgrade is **one-way** (D4) |
| B — fresh branch install, no GPU | **PASS**; CPU admission path fully asserted; ramp brake verified (see §F) |
| C — hand-edited master-era server TOML + hand-written per-DB config | **PASS**, byte-for-byte round trip |
| D — Docker CPU image, config volume, cgroup limit, CUDA image without `--gpus` | **PASS with one known defect** (D5, B19 reconfirmed, 5.89x) |
| E — Nix | **SKIPPED** — `command -v nix` finds nothing on this host |
| F — CPU throughput-plateau ramp (added mid-run) | **PASS** — the knee lands on the measured plateau, the budget never chases RAM |

---

## Defects

**D1. Every pre-upgrade job in the history reads `outcome: "running"` forever.**
`panoptikon/src/db/extraction_log.rs:164-167` renders `data_log.outcome = ''`
as `'running'` unconditionally. The migration that added the column says the
opposite — `panoptikon/migrations/index/20260904120000_job_item_failures.sql`:
*"'' is the value every pre-existing row and every in-progress row carries;
the reader renders it from `completed` exactly as before, so no backfill is
needed"* — and so does `panoptikon/src/db/extraction_write.rs:34-37`. The
reader ignores `completed`, so a finished master-era job reports as running.
Measured in scenario A on a row master wrote (`completed: 1`, `status: 1`,
`end_time` set):

```
id 1  tags/wd-vit-tagger-v3  ... "completed": 1, "status": 1, "outcome": "running"
id 2  doctr/db_resnet50...   ... "completed": 1, "status": 1, "outcome": "completed"   <- written by the branch
```

Fix is one `CASE` arm (`WHEN data_log.outcome = '' AND data_log.completed = 1
THEN 'completed'`) or the backfill the migration says is unnecessary.

**D2. The upgrade re-detects the accelerator and can silently replace a
deliberate CPU install with `cu128`.** The branch changes `python/uv.lock`
(one added dependency, `nvidia-ml-py`), so `auto_setup_needed()` returns
`"uv.lock changed since setup last completed"` for **every existing user** and
startup runs a full `uv sync`. `panoptikon/src/setup.rs:1191-1200` calls
`run(..., SetupOptions { accelerator: None, .. })`, which re-resolves from the
host instead of from the sentinel's own `extra=` line
(`setup.rs:550 installed_accelerator()` is only used for *reporting*).
Observed verbatim on a data folder whose sentinel said `extra=cpu`:

```
INFO panoptikon::config: the managed Python environment is missing or incomplete;
     setup will run automatically at startup reason="uv.lock changed since setup last completed"
INFO panoptikon::setup: accelerator selected requested=Auto accelerator=Cuda extra="cu128"
     evidence="nvidia-smi on PATH"
INFO panoptikon::setup: syncing the locked environment (uv sync); the first run downloads
     several GB of packages and can take a while extra="cu128" wheels="cu128"
INFO panoptikon::setup: Downloading nvidia-cudnn-cu12 (693.2MiB) ...
```

(`logs/A-branch-autosetup.log`.) A user who chose CPU on a machine that has an
NVIDIA driver gets ~4 GB of CUDA wheels and a different torch build on first
boot after the upgrade, with no prompt. Failure is non-fatal (`setup.rs:1204`),
so the *risk* is bandwidth and a changed runtime, not a dead server.

**D3. Losing VRAM admission entirely is logged at DEBUG, while every
neighbouring cause of the same loss is logged at WARN.**
`panoptikon/src/inferio/ledger.rs:2135-2146` (`DispatchMismatch::NoGpu`) uses
`tracing::debug!`; the `PciMismatch` and `TotalDisagrees` arms immediately
above (`:2082`, `:2119`) use `tracing::warn!` for the same consequence
("dispatching this model without VRAM admission"). Reached by a **documented,
shipped configuration**: set `[inference_local] python` to a CPU-only venv on a
host that has an NVIDIA driver — which is exactly what
`tools/calibration-protocol/config/server-C1.toml:119` does. With an explicit
`python`, the managed-venv sentinel does not exist, so
`accelerator_report` falls through to `auto` detection and resolves **cuda**;
the inventory then lists the physical GPUs, the CPU-only worker matches none of
them, and the ledger dispatches with no admission at all. Measured:

```
INFO  panoptikon::accelerator_report: accelerator backend backend="cuda"
      backend_source=nvidia-smi on PATH devices=[2 x RTX PRO 6000]
INFO  panoptikon::inferio::gpu: CUDA_VISIBLE_DEVICES names devices by index ... visible_devices=-1 gpus=2
DEBUG panoptikon::inferio::ledger: the worker reports no GPU this GPU inventory lists;
      dispatching this model without VRAM admission model=tags/wd-vit-tagger-v3
      worker_uuid="<none>" worker_bdf="<none>" gpus=0
```

(`logs/E-explicit-python-noadmission.log`.) The whole feature of this PR is off
— zero grants, no ramp, no calibration row — and at the shipped log level the
operator sees nothing. Workaround: also set `[inference_local.python_env]
accelerator = "cpu"`, after which `backend_source=explicitly configured` and
the CPU device resolves normally. Two fixes worth considering: raise that arm
to `warn!`, and let `installed_accelerator()` read the sentinel of the
*configured* interpreter, not only of the managed venv.

**D4. The upgrade is one-way: master will not start again.** After the branch
has opened the data folder, master exits 1 on the first index DB in the startup
sweep — including DBs the user never opened:

```
Error: failed to migrate database data/index/default/index.db
Caused by:
    migration 20260903120000 was previously applied but is missing in the resolved migrations
```

(`logs/A-master-downgrade.log`.) This is inherent to adding SQL migrations, but
it is worth stating in release notes, because the only way back into the data is
`readonly = true` at the **top level** of the server TOML, which does start
(`logs/A-master-downgrade-readonly.log`) and serves reads.

**D5. In Docker the CPU device reads host RAM, not the container's cgroup
limit (run1 B19, reconfirmed on the branch).**
`panoptikon/src/inferio/cpu.rs:36-46` reads `/proc/meminfo`, which is not
namespaced. Under `mem_limit: 16g` / `memswap_limit: 16g`:

| | |
|---|---|
| device the ledger reports | `CPU (128 GB)`, `total_mb 128649` |
| budget after `cap_fraction 0.75` | `limit_mb 96486` |
| what the kernel enforces | `16384` (`/sys/fs/cgroup/memory.max = 17179869184`) |
| overcommit | **5.89x** (run1 measured 2.94x on a 64 GB host) |

---

## A — upgrade from master

```bash
# root: a bare-binary install laid out as the shipped tree
#   $TMPDIR/A/app/{config/server/default.toml, config/inference/, python/{inferio,inferio_worker,uv.lock,.venv}}
# the server TOML is master's shipped config/server/default.toml verbatim,
# with four user-style edits: [upstreams.ui] local = false (no UI checkout on
# this host) and the three listener ports moved to 17342/17343/17339.
cd $TMPDIR/A/app
CUDA_VISIBLE_DEVICES="" /home/admin/projects/panoptikon-master/target/release/panoptikon \
    --root $TMPDIR/A/app --disable-update-check
curl -X POST "$B/api/db/create?new_index_db=up&new_user_data_db=up"
curl -X PUT  --data @cfg.json "$B/api/jobs/config?index_db=up"   # folders + default_batch_size 7 + cron batch_size 5
curl -X POST "$B/api/jobs/folders/rescan?index_db=up"
curl -X POST "$B/api/jobs/data/extraction?index_db=up&inference_ids=tags/wd-vit-tagger-v3"
```

Master indexed 30 PNGs and tagged them (`batch_size 7`, 0 errors, 0 failed),
wrote `data/index/up/config.toml`, and created `_sqlx_migrations` with 17 rows.
`.txt` files are **not** indexed on any configuration — `build_extension_set`
(`panoptikon/src/jobs/files/mod.rs:6362-6388`) has no text extension — so the
four `.txt` files in the corpus were ignored, as the protocol's own README
already documents.

Then the branch binary on the *same* root:

- **Starts clean.** 0 `ERROR` lines. `no accelerator on this host; admitting
  batches against system RAM`.
- **Migrations run and ANALYZE follows** on all three index DBs
  (`default`, `up`, and a stray `cal` — see "Environment note").
- **The batch-auto migration fires exactly as designed**:
  `INFO panoptikon::db::batch_auto: batch sizes in the stored job settings and
  cron schedule were reset to auto index_db="up"`, and the only difference in
  `config.toml` is the two removed keys:

  ```diff
  -batch_size = 5          # [[cron_jobs]]
  -default_batch_size = 7  # [[job_settings]]
  ```

- **Per-DB config loads with the new keys at their serde defaults**: the branch
  adds no new `SystemConfig` keys (GET before/after is key-identical), and the
  cleared values read back as `null` (= auto).
- **Settings round-trip**: `GET /api/jobs/config` → `PUT` unchanged → **200**,
  file byte-identical; changing one unrelated setting (`detect_outros`) → **200**,
  file still parses (`tomllib`), value applied.
- **Search still works**: `match_path` returns the 30 items.
- **A new extraction job completes with 0 failed items**: `doctr/...mobilenet_v3_small`,
  30 segments, `errors 0`, `failed_items 0`, `outcome "completed"`, `batch_size 0` (auto).
- **The calibration store is written** at `data/inferio/calibration.toml`
  (`arch = "cpu"`, `base_method = "rss"`, `slope_mb_per_unit = 169.5`).
- **D1 shows up here** (pre-upgrade job row reads `running`).
- **D2 shows up here** (first boot wants a `cu128` re-sync).
- **Downgrade: D4** — master exits 1; only `readonly = true` gets back in.

## B — fresh install of the branch, no GPU

Root built the same way from the branch checkout, ports 17352/17353/17349,
shipped `config/server/default.toml` with only `local = false` and the ports
changed. 0 `ERROR` lines on first boot.

Four jobs, all on the CPU inference server, all clean:

| job | segments | errors | failed_items | outcome | inference s |
|---|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | 30 | 0 | 0 | completed | 6.4 |
| `doctr/db_resnet50_crnn_mobilenet_v3_small` | 30 | 0 | 0 | completed | 18.3 |
| `textembed/all-mpnet-base-v2` | 60 | 0 | 0 | completed | 22.3 |
| `clip/ViT-H-14-378-quickgelu_dfn5b` | 30 | 0 | 0 | completed | 38.9 |

**`/health` reports no GPU and no ledger error.** One synthetic device:

```json
"gpus": [{"index":0,"uuid":"CPU","name":"CPU (128 GB)","total_mb":128649,
          "compute_cap":null,"unified_ram_mb":128649}]
"vram": [{"gpu_uuid":"CPU","total_mb":128649,"limit_mb":85194,"reserve_mb":1024,
          "reserve_rule":"capped_default","margin":0.1,"cap_fraction":0.75,
          "external_known":true,"external_source":"ram","charges_mb":0,
          "grants_outstanding":0,"workers":[]}]
"load_cooldowns": []
```

**The CPU admission path, asserted field by field** (coordinator's item 1):

| assertion | measured | where |
|---|---|---|
| device key `CPU` | `"uuid": "CPU"` | `/api/inference/health`; `cpu.rs:23 DEVICE_KEY` |
| `free_source = "ram"` | `free_source  ram` / `87909 / 128649 MiB` | `selftest.py` with the gateway's own device selection (`logs/B-selftest-cpu.log`) |
| `base_method = "rss"` | `base_method rss`, `base_mb 618`; and `base_mb=Some(483) base_method="rss"` in the ledger's admit line | selftest + gateway trace |
| `cap_fraction 0.75` | `0.75`, `limit_mb = 0.75 x 128649 = 96486` on an empty device | `/health`; `cpu.rs:30 DEFAULT_CAP_FRACTION` |
| no `index_limit` / shape-ceiling clamp | `index_limit_events 0` (selftest); `grep -ci index_limit` and `shape_ceiling` over the whole trace log = **0**; every settled window `clamped=none clamped_samples=0 deflation=0` | `logs` |
| selftest verdict | `VERDICT: no degraded tiers` | `logs/B-selftest-cpu.log` |

Note on the instrument: `selftest.py` run **without** `INFERIO_DEVICE=cpu`
resolves `free_source nvml` and `base_method None` on this host and prints
`VERDICT: degraded` (`logs/B-selftest-nodevice.log`), because the worker's
"is this host CPU-priced?" answer comes from the orchestrator's env, not from
torch. The gateway always sets it, so this is a tooling footgun, not a product
defect — but it is the same root as D3.

**Restart and resume**: after `SIGTERM` and a restart, the four history rows
are intact and all read `completed`; a rescan picked up 12 new images and a
second `tags` job ran them (12 segments, 0 errors, completed). Semantic search
works on both new setters: `text_embeddings` (`all-mpnet-base-v2`) and
`image_embeddings` (`ViT-H-14-378`) each return 30 results, so the query-side
model loads work too.

### Log growth at the shipped level (coordinator's item 3)

No `RUST_LOG`, shipped `[logging] level = "${LOGLEVEL:-INFO}"`, file
`<data folder>/panoptikon.log`, **no rotation anywhere**:

| work | bytes added | per 1000 items |
|---|---|---|
| extraction, `clip/ViT-B-32_openai`, **2000 items** | 4 681 | **2.3 KB** |
| file rescan of 2000 unchanged files | 526 | 0.26 KB |
| extraction, 30 items (tags) | 1 995 | (fixed per-job cost dominates: ~2 KB/job) |
| extraction, 30 items (doctr) | 2 303 | — |
| extraction, 30 items (clip ViT-H-14) | 5 550 | — |

So the growth is dominated by a **fixed ~2-5 KB per job**, not by item count:
a user indexing a million items a day through a handful of jobs adds single-digit
MB/day. At `RUST_LOG=info,panoptikon::inferio=trace` the same 2000-item job wrote
**385 KB** (~190 KB/1000 items, 80x), which matters only for protocol runs.

## C — a user's hand-edited server TOML, and a hand-written per-DB config

Server TOML = **master's shipped `config/server/default.toml`** with the edits a
real user makes, and with no key the branch added (`[inference_local.vram]` is
absent entirely):

```toml
data_folder = "mydata"          # moved off the default
temp_dir = "mydata/tmp"
[logging] level = "DEBUG"       # replaced the ${LOGLEVEL:-INFO} bridge with a literal
[server] port = 17362           # and 17363 / 17359 for the other two listeners
[inference_local.prewarm] enabled = true, lazy = false, always_warm = ["wd_tagger"]
```

The branch honours every one of them — `logging to file path=mydata/panoptikon.log`,
the three listeners on the user's ports, DEBUG lines present, and
`prewarm: {"enabled":true,"lazy":false,"warm":[{"impl_class":"wd_tagger","state":"warm"}]}`
— with 0 `ERROR` lines, and defaults everything the file does not mention:

| branch-added key, absent from the file | value in force |
|---|---|
| `inference_local.vram.margin` | `0.1` |
| `inference_local.vram.cap_fraction` | `0.75` (the CPU device's shipped ceiling) |
| `inference_local.max_concurrent_loads` | 1 (default) |
| `transcode.hover_preview` / `hover_preview_max_bytes` | `"auto"` → `{"direct":true,"trim":true,"transcode":false,"max_bytes":16777216}` on `/api/client-config` |

**Per-DB config with comments and non-canonical key order.** A hand-written
`config.toml` (comments on several keys, `cron_schedule` first, `job_settings`
as an `[[array of tables]]`, `default_batch_size = 12`) was dropped into a DB
directory, the gateway restarted, `GET /api/jobs/config` returned every value
correctly, and `PUT` of that same body left the file **byte-identical** —
comments, ordering and the user's `default_batch_size = 12` all preserved.
(The batch-auto migration correctly did *not* clear it: the DB was created by
the branch, so it was stamped at creation, which is the documented rule.)

## D — Docker

```bash
docker build --build-arg ACCELERATOR=cpu  -t run4-panoptikon:cpu  .   # 3.50 GB
docker build --build-arg ACCELERATOR=cuda -t run4-panoptikon:cuda .   # 9.43 GB, ~5 min warm
docker compose -f docker-compose.run4.yml up -d      # copy of deploy/docker-compose.yml,
                                                     # run4- names, image above, ports 17442/17439
```

**Config volume seeding.** First start seeds `run4-panoptikon-config` from the
image: `/app/config/server/docker.toml` only (`config/inference/` is created
empty; the bundled first-run dump of `default.toml`/`example.toml` does not run
in the Docker layout, which is correct — `PANOPTIKON_CONFIG_PATH` points at
`docker.toml`). Editing a value in the volume and starting again:

```
# uncommented cap_fraction in [inference_local.vram]: 0.5
md5 before restart 4caa70e0...  md5 after restart 4caa70e0...   (not overwritten)
/health -> "cap_fraction": 0.5, "limit_mb": 64324  (= 0.5 x 128649)
```

So the volume is seeded once and the user's edit survives and takes effect.
(A deliberately broken edit — a duplicated `[inference_local.vram]` header —
produces a precise message and exit 1: `TOML parse error at line 278 ...
duplicate key 'vram' in table 'inference_local'`. With the shipped
`restart: unless-stopped` that becomes a restart loop, but the reason is in the
log.)

**One CPU job inside**: `tags/wd-vit-tagger-v3` over a read-only bind mount,
30 segments, 0 errors, `outcome "completed"`.

**Under a 16 GiB cgroup** (`docker-compose.run4-C6.yml`, a copy of the
protocol's C6 with run4 names):

- `memory.max = 17179869184`, `memory.swap.max = 0` — both accepted.
- **D5**: the device is `CPU (128 GB)`, `limit_mb 96486`, against a 16 GiB
  ceiling → **5.89x overcommit**.
- 13 x 1 GiB numpy hogs inside the container while the job ran: the kernel
  OOM-killed inside the cgroup (`memory.events: oom 2, oom_kill 2`,
  `State.OOMKilled = true`) and took the worker with it. The ledger did what it
  is designed to do:

  ```
  WARN ledger: this replica died while running a granted window on a GPU whose memory is
       the machine's own; recording it as a memory negative ... and halving the batch size
       the next replica of this model is admitted for
  WARN ledger: settled a granted window ... outcome="worker_died" reason="unified_device_death"
       deflation=1 clean_windows=0
  ```

  anchors `16 -> 8 -> 4`, **2 deaths, one respawn each** (no respawn storm; the
  new `load_failure_cooldown` ladder does not apply here — it covers failed
  *loads*, and `/health load_cooldowns` stayed `[]` throughout).
- **The job's own report is much better than run1's.** run1 (F7/T8/Q8) recorded
  a job that said `completed` after doing 121/180 with `failures {"total":0}`.
  The branch reports:

  ```
  outcome: "partial",  errors: 6,  failed_items: 6,  image_files: 174/180,
  failure_reason: "6 of 180 attempted items could not be processed and are still owed
                   (6 were re-queued after a predict that never reached a model)"
  /api/jobs/data/failures -> job_failures_total 6, failed_jobs_total 1, each owed item
                             listed with its path, stage ("inference") and error
  ```

  A second pass **with the hog still holding ~15 GB** finished the remaining 6
  items with 0 errors — run1 needed five passes. Convergence and, for the first
  time, an honest verdict.

**CUDA image with `--gpus` ABSENT** (the common user mistake): it **starts and
works**, degraded and loudly:

```
INFO accelerator_report: accelerator backend backend="cuda"
     backend_source=managed venv (setup sentinel) devices=[]
WARN accelerator_report: backend is cuda but no GPU name could be detected for stack 'nvidia'
WARN inferio::gpu: this host is configured for CUDA but nvidia-smi was not found on PATH;
     workers will not be pinned, batch sizes will not be calibrated and model availability
     will not be capability-filtered
```

`/health` shows `"gpus": []` and `"vram": []` — no device at all, so no
admission and no calibration — and a `tags` job over 30 images still ran to
`completed`, 0 errors, 8.0 s. This is the *right* behaviour, and the contrast
with **D3** is the point: here the loss of admission is a WARN, there it is a
DEBUG.

## E — Nix

`command -v nix` → nothing. **Skipped.**

## F — the CPU throughput plateau (does the batch chase RAM?)

Cold store, `RUST_LOG=info,panoptikon::inferio=trace`, the protocol's `ramp`
corpus (2 000 x 1024x1024 JPEG) in a fresh root, one model per job.
Device budget throughout: `limit_mb` ~96 486, `headroom_mb` ~71 000-74 000
(the box's own 53 GB of other usage is counted as `external_mb`).

### `tags/wd-vit-tagger-v3` — 2 000 items, 474.6 s inference, 0 errors

Per-batch throughput, taken from the worker's own batch boundaries:

| batch | n | median items/s | gain vs previous |
|---|---|---|---|
| 2 | 3 | 3.585 | — |
| 4 | 3 | 4.949 | **1.380x** |
| 7 | 151 | 4.767 | 0.963x |
| 8 | 6 | 4.860 | 1.020x |
| 15 | 45 | 3.864 | **0.795x** |
| 16 | 11 | 3.842 | 0.994x |

The rate stops improving at **4-8** and is 20% *worse* at 15-16.
The ramp went `1 -> 2 -> 4 -> 8 -> 16` and **held at 16**, 35 s and 6 windows
into the job:

```
18:30:45 INFO ledger: holding the throughput ramp at this rung: the ring cannot certify
         this rung yet model=tags/wd-vit-tagger-v3 gpu=CPU units=16 certified=false knee_binds=false
```

— i.e. **one doubling past the plateau**, inside the two-doubling gate, and it
never moved again for the remaining 7 minutes although 71 GB stayed free.
Then the knee fitted and was persisted:

```toml
base_mb = 481            base_method = "rss"     slope_mb_per_unit = 17.358
knee_units = 7           samples = 9             max_units_measured = 16
```

`knee_units = 7` is exactly the measured plateau. **Memory at the hold**:
worker RSS peaked at **1 377 MB** at rung 16 versus **1 085 MB** at the plateau
rung 7-8 (**1.27x**), and the granted charge was 177-261 MB — i.e. **1.4% of
the `cap_fraction x RAM` budget of 96 486 MB**. Nothing chased RAM.

**The knee binds on the next run.** A second `tags` job (smoke corpus, same
data folder) started at `unit_budget=7` instead of ramping, and said so:

```
INFO ledger: holding the throughput ramp at this rung: a knee caps the sizes a doubling
     would have to measure at ... units=32 certified=true knee_binds=true
INFO ledger: this model has run cleanly at its throughput knee for long enough ...
     widening the cap by one batch-size step
DEBUG ledger: fitted a throughput knee ... knee_units=7 previous=Some(15) observations=128
```

The widening probe re-tested 15 and refitted back to 7 on 128 observations.

### `clip/ViT-B-32_openai` — 2 000 items, 51.6 s inference, 0 errors

| rung | 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 |
|---|---|---|---|---|---|---|---|---|
| units/s | 8.8 | 6.9 | 9.1 | 10.1 | 11.6 | 13.0 | **14.3** | 12.9 |
| worker RSS peak (MB) | — | 1071 | 1088 | 1121 | 1186 | 1302 | 1525 | 1976 |

Plateau at 64 (32→64 = 1.098x, 64→128 = 0.90x). The ramp doubled to 128 and
**held at 256 with `certified=true`, "the rung is the top of a measured
plateau"**, and persisted `knee_units = 63`. Peak RSS **1 976 MB = 2.0% of the
budget**.

*Observation (not a defect):* every one of CLIP's 15 grants was `pre_fit=true`,
each charging the whole ~72 GB share, and the stored profile has `samples = 0`,
`slope_mb_per_unit = 0.0` — on the CPU device the RSS deltas never varied
enough to fit a cost model for this impl, so the model is never priced and the
device's entire budget stays charged to it for the length of the job. A second
model cannot be admitted concurrently while that runs. `tags` did fit
(`slope 17.36`, 9 samples), so this is impl-dependent, not universal.

### `textembed/all-MiniLM-L6-v2` — 4 000 text segments, 11.5 s inference, 0 errors

Cost dimension is `token` / `max-times-count`, so the rungs are token counts:
`50 -> 100 -> 177 -> 354 -> 688 -> 1314 -> 2583 -> 5166 -> 10296 -> 20592`,
units/s rising throughout (291 → ~6 700), held at 20 580 when the job ran out
of work. No knee — the job was too short to produce a plateau. Charge at the
top rung **776 MB**, worker RSS peak **950 MB** (1.0% of budget). Store:
`base_mb 219`, `base_method "rss"`, `slope 0.0367 MB/token`, 63 samples.

### Warm-up cost on a fresh install

`tags`: **6 windows / 35 s** to the hold, out of a 480 s job. `clip`: 8 windows
/ ~29 s out of 52 s. On a slow CPU that fraction is larger, but it is paid once
per model per data folder — the knee is persisted in
`<data folder>/inferio/calibration.toml` and binds from the next job onward
(verified above).

### Protocol legs

**`legs.py --scenario S14`** (CPU, the fixed config, `analyze.py --checks
failures,job_outcome,grant_safety,peak_fds`, `S14-verdicts.json`):

```
failures      PASS  0 OOM negatives, 0 throughput-collapse negatives,
                    0 unified-memory-device death negatives, 0 fatal worker deaths,
                    0 merged-window fallbacks
job_outcome   PASS  1 job record: 1 completed, 0 failed items, 0 errors;
                    queue outcomes all 'completed'
grant_safety  WARN  171 grants; 0 exceeded the headroom they were priced against;
                    0 memory-blind (mb=0) -- ORACLE CLAUSE NOT RUN: no grant joined a
                    vramrec sample within 1.5s (vramrec's oracle is NVML per-process;
                    a CPU-device grant has nothing for it to join, so this WARN is the
                    harness, not the ledger)
peak_fds      INFO  peak 59 descriptors over 1062 samples, soft limit 524288 (0%)
```

That leg's ramp held twice — first uncertified at 8, then
`a knee caps the sizes a doubling would have to measure at ... units=16
certified=true knee_binds=true` — and stored `knee_units = 3`,
`max_units_measured = 15`, `base_method = "rss"`, `backend = "cpu"`. The knee
is **3** there against **7** in the dedicated ramp above, on the same model and
host: that leg ran with worker DEBUG logging and alongside other work, and a
throughput knee is a measurement of the machine as it is at the time. Both are
at or below the plateau, which is the direction that matters.

**`legs.py --scenario S2`** was run twice. The **first** run is reported as an
incident below; the **second**, with `[inference_local] python` repointed at
the CPU venv, produced the D3 evidence rather than a CPU ramp (that is what D3
*is*), and was abandoned in favour of the hand-run measurement above, which
uses the shipped configuration and therefore the real CPU device. `analyze.py`
on the first leg returned `grant_safety PASS, failures PASS, job_outcome PASS,
ledger_invariant PASS, deflation_recovery PASS, idle_liveness PASS,
calibration_learned PASS, alloc_retries PASS`, with `oracle_agreement FAIL` and
`footprint_agreement INFO` — both of those compare against an NVML GPU oracle
and are meaningless for a CPU-priced device on a host whose GPUs belong to
another tenant.

---

## Environment notes (things that are about this host, not the product)

1. **Port collision with a sibling agent.** The first scenario-A gateway bound
   the shipped 6342/6343/6339 and immediately received `POST /api/db/create`
   and extraction jobs for an index DB named `cal` pointed at
   `tools/calibration-protocol/results/corpus/smoke` — another agent's protocol
   run, talking to my process. Moving to 6442 did not help (the same thing
   happened again); everything from then on ran on 1734x/1735x/1736x/1739x and
   17442/17459/17462/17472. The stray `cal` DB was left in place and served as
   a second master-written database for the migration sweep.

2. **One leg ran on a GPU before I caught it.** `legs.py --scenario S2` resolves
   `--config C1` to `tools/calibration-protocol/config/server-C1.toml`, which
   hard-pins `[inference_local] python` to the **main tree's CUDA venv**
   (`server-C1.toml:119`), overriding `--python`, and `legs.py`'s env-file
   parser drops a `KEY=` line with an empty value, so `CUDA_VISIBLE_DEVICES=`
   never reached the gateway. One `tags/wd-vit-tagger-v3` job over 2 180 images
   therefore ran on GPU 0 for ~90 s (18:12:29-18:14:00 UTC) before I stopped
   and repinned everything to the CPU venv. No other GPU work was started, and
   no container was ever given `--gpus`.

3. `.txt` files are not indexed by any configuration, so the "a few .txt files"
   part of the corpus contributed nothing; `textembed` was fed from the text
   rows the tagger and the OCR wrote, which is the documented route.
