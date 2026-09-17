# final-deploy — the deployment paths, re-run on the final tip

The `run4-deploy` pass (branch @ `0d7f5671`) found defects **D1–D5** and made
observation **F** about the CPU ramp. Those were then fixed. This is the same
set of scenarios re-run on the final tip of
`claude/batch-calibration-coverage-db9ab9` @ **`2133563f`**, with every fix
asserted directly.

Everything is **CPU-only**: every gateway was started with
`CUDA_VISIBLE_DEVICES=""` and every Python environment is the `cpu` extra
(`torch 2.7.1+cpu`), except the one gateway scenario **D3** deliberately runs
with the GPUs visible to the host process (never to the worker). No container
was given `--gpus`. Every container, volume, image and network created here
carries a `final-` prefix and was removed at the end.

Host: 48 cores, 128 GiB RAM (`MemTotal 131737460 kB`, reported by the
orchestrator as `CPU (128 GB)`, `total_mb 128649`), Linux 6.12.73, two RTX PRO
6000 Blackwell cards **belonging to another agent and never touched**. All
state under `$TMPDIR=$HOME/tmp-final-deploy`. Binaries: master @ `7aa92b20`
(`/home/admin/projects/panoptikon-master/target/release/panoptikon`) and a
`cargo build --release -p panoptikon` of this worktree. CPU venv:
`uv sync --locked --extra cpu --group test` in a copy of `python/` under the
worktree.

Wall clock: **1 h 22 min** (00:51:46 → 02:14 UTC, 2026-09-17).

---

## Verdicts vs run4-deploy

| Scenario | run4-deploy @ `0d7f5671` | final-deploy @ `2133563f` |
|---|---|---|
| **A** — upgrade master → branch | PASS with two defects (D1 job history, D2 accelerator switch); downgrade one-way (D4) | **PASS. D1 fixed, D2 fixed (both halves).** D4 unchanged and inherent |
| **B** — fresh branch install, no GPU | PASS; CPU admission path asserted; ramp brake verified (§F) | **PASS on every device-key, worker-env and job assertion. F's CLIP gap is fixed.** One ramp assertion fails as stated — see **N1** |
| **C** — hand-edited server TOML + hand-written per-DB config | PASS, byte-for-byte round trip | **PASS**, byte-for-byte round trip |
| **D** — Docker | PASS with D5 (5.89x overcommit); CUDA image without `--gpus` unpriced | **PASS. D5 fixed (1.00x). The no-`--gpus` CUDA image is now priced on the CPU device instead of having no device at all.** |
| **E** — Nix | SKIPPED (`command -v nix` finds nothing) | **SKIPPED** — same, `nix` is still not on this host |
| **F** — CPU throughput-plateau ramp | PASS — knee lands on the plateau, budget never chases RAM | **Split**: the budget still never chases RAM (RSS 1.0–5.4 % of the cap, PASS), but the wd-vit knee is no longer persisted — **N1** |
| **S14** leg (`analyze.py`) | failures PASS / job_outcome PASS / grant_safety WARN / peak_fds INFO | **identical** |
| **S2** leg (`analyze.py --checks all --learning`) | (run4's S2 was abandoned to GPU/D3 problems; not a clean comparand) | **PASS on failures, job_outcome, deflation_recovery, idle_liveness, persistence, calibration_learned**; grant_safety WARN (harness) and ledger_invariant WARN (3 `limit_fell` samples, 0 over-grants) |

**Fix status, one line each:**

| Fix under test | Outcome |
|---|---|
| D1 — outcome derivation incl. `cancelled` | **PASS** on `/api/jobs/data/history` *and* `/api/jobs/data/failures` |
| D2a — re-sync keeps the sentinel's extra | **PASS**: `requested=Cpu accelerator=Cpu extra="cpu" evidence="the extra the managed venv was synced for"`, no cu128 |
| D2b — a sentinel this platform cannot install re-probes | **PASS**: `WARN … re-probing the host installed=Mps`, then `requested=Auto accelerator=Cuda` — a re-probe, not a bail |
| D3 — warn-once / a CPU worker is placed on the CPU device | **PASS**: worker admitted on `gpu=CPU`, grants issued, store written, **zero** warnings; `/health` shows the CPU row carrying the replica and both GPU rows empty |
| D5 — cgroup bound on both sides (active+inactive file) | **PASS**: device `CPU (16 GB)`, `limit_mb 12288`; and under an in-container 10 GiB hog the free side follows too (`external_mb 9582`, `limit_mb 5843`) |
| F — CPU RSS basis with glibc pinning | **PASS**: `MALLOC_MMAP_THRESHOLD_=131072` and `MALLOC_TRIM_THRESHOLD_=131072` in the worker env; CLIP now **fits** (`slope 6.64 MB/unit`, `samples 9`) and its grants **leave** `pre_fit` |
| mixed devices | **PASS**: CPU row beside two CUDA rows on one host; the no-`--gpus` CUDA image gets a CPU device too |

---

## Defects and differences

**N1 (new — not present in run4). On the CPU device a fitted throughput knee
is widened away and then *withdrawn*, so no knee reaches the store.**
Reproduced on both wd-vit ramps on this tip, with the same shape:

```
01:21:03 DEBUG ledger: fitted a throughput knee … knee_units=7 previous=None observations=13
01:21:56 INFO  ledger: this model has run cleanly at its throughput knee for long enough,
         with memory to spare, that the knee is worth re-testing; widening the cap by one
         batch-size step. A knee is a brake, not a ceiling
   …repeatedly, for two minutes:
         DEBUG ledger: declining to read this model's throughput curve: the observations in
         one batch-size bucket disagree with each other by more than the knee's own decision
         band, so something outside this ledger was moving
01:23:52 INFO  ledger: this model's throughput knee has widened past the point where it could
         cap anything and has been withdrawn; the ramp and the extrapolation ratchet govern
         its batch size from here
01:23:59 INFO  ledger: the throughput ramp is free to grow again
```

(`logs/F-tags-ramp.log`; the S2 leg repeats it verbatim at 02:00:22 / 02:03:19,
`legs/S2`.) run4 persisted `knee_units = 7` and **held at 16** for the
remaining seven minutes. Here the ramp goes on to 32, then 64/81, then **128**,
and the stored profile has **no `knee_units` at all**
(`calibration/F-tags-minilm.toml`, `max_units_measured = 128`). So the literal
run4 assertion — *"the batch holds near the throughput plateau"* — **does not
hold on this tip**.

What it did *not* cost, measured:

* throughput **improved**: 2 000 items in **422.1 s** of inference here against
  run4's **474.6 s** for the same corpus and model;
* memory was never chased: peak worker RSS **5 178 MB** at rung 128 =
  **5.4 %** of the `cap_fraction x RAM` budget of 96 486 MB, and the largest
  granted charge was 4 542 MB = 4.7 %;
* the profile still binds on the next job — a second wd-vit job in the same
  data folder came up `seeded_from_store=true`, first grant already
  `pre_fit=false`, and jumped straight to the remaining 29 items instead of
  re-ramping from 1.

The trigger is the *decision-band* gate: this box runs another agent's
`inferio_worker` processes (three distinct foreign worker pids were sampled
during the wd-vit ramp, peaking at 8 905 MB RSS), so "something outside this
ledger was moving" is literally true and the curve never became readable
again. The ledger's documented response to an unreadable curve is to widen and
withdraw. **This is behaviour to confirm rather than a proven regression**: on
a contended host the knee is withdrawn and the brake is gone from the store,
and on this host that was the faster answer. It needs one run on a quiet
machine to decide which reading is right.

**D4 (unchanged, inherent). The upgrade is one-way: master will not start
again.** Verbatim as in run4:

```
Error: failed to migrate database data/index/default/index.db
Caused by:
    migration 20260903120000 was previously applied but is missing in the resolved migrations
```

(`logs/A-master-downgrade.log`, exit 1.) `readonly = true` at the **top level**
of the server TOML still gets back in and serves reads (master started and
`/api/search/stats` returned the 30 indexed files). Release-notes material.

**O1 (observation). A pre-upgrade `failed` row is re-stamped `cancelled` by the
next extraction job.** On first boot the branch derives log id 5 (`job_id
NULL`, `completed = 0`) as `outcome: "failed"` — correct, and that is the D1
fix working. The first extraction job the branch then runs executes the
incomplete-job cleanup (`extraction_write.rs`, `WHERE completed = 0 AND outcome
= ''`), which stamps the same row `cancelled`. Both words are defensible for an
interrupted job, but the two readings disagree across one job boundary for the
same row.

**O2 (observation). With an explicit `[inference_local] python`, a CPU-priced
profile is keyed `backend = "cuda"`.** The store env's `backend` is the
*host's* resolved accelerator (`http.rs:306`), so D3 and both `legs.py` legs
wrote `arch = "cpu"`, `gpu = "CPU (128 GB)"`, `torch = "2.7.1+cpu"` and
`backend = "cuda"` in the same profile. Self-consistent for that deployment,
but such a profile will never be shared with the same model's profile on a
managed CPU venv (`backend = "cpu"`), which is what the Docker CPU image and
the bare CPU install write.

**O3 (tooling). `corpus.py --tier smoke` fails on all 5 PDFs on this host.**
`AttributeError: 'tuple' object has no attribute 'convert'` (Pillow 10.4.0),
exit 1, manifest written with `errors: 5`. The 180 images and 15 text files are
fine, so the smoke legs run; the PDF tier of the corpus is unusable here.

**O4 (tooling). `legs.py --port N` moves the probe, not the gateway.** Its help
says "gateway port (default: read from the config)", but it only changes the
URL `legs.py` polls; the gateway still binds whatever the config declares.
A leg run with `--port 17912` against `server-C1.toml` bound 6342/6343/6339 and
then aborted with `gateway_never_answered` after four minutes. Worked around by
passing a copied config with the ports edited.

---

## A — upgrade from master

A bare-binary install laid out as the shipped tree
(`$TMPDIR/A/app/{config/server,config/inference,python/{inferio,inferio_worker,uv.lock,.venv}}`),
the server TOML being **master's shipped `config/server/default.toml`** with
four user-style edits: `[upstreams.ui] local = false` and the three listener
ports moved to 17842/17843/17839. The `.venv` is the CPU venv and its sentinel
says `extra=cpu` with **master's** `uv.lock` hash. The upgrade replaces the
binary, `python/{inferio,inferio_worker,pyproject.toml,uv.lock}` and
`config/inference/`, and keeps the data folder, the server TOML and the venv —
which is what a release tarball does.

Master (0 errors) indexed the 30-PNG corpus, wrote `data/index/up/config.toml`
with `default_batch_size 7` / cron `batch_size 5`, and produced five history
rows on purpose, one of each shape the reader has to render:

| log id | how it was made | `data_log.completed` | `data_jobs.completed` | expected |
|---|---|---|---|---|
| 1 | a clean `tags` job | 1 | 1 | `completed` |
| 2 | `POST /api/jobs/cancel` mid-job | 0 | -1 | `cancelled` |
| 3 | `SIGKILL` mid-job, then a later job's cleanup | 0 | -1 | `cancelled` |
| 4 | a clean `clip` job | 1 | 1 | `completed` |
| 5 | `SIGKILL` mid-job with `atomic_extraction_jobs = true`, whose cleanup **deletes** the `data_jobs` row | 0 | *(job_id NULL)* | `failed` |

(`atomic_extraction_jobs` was set to `true` only long enough to produce row 5,
then reverted; it is the only shipped path that produces a `job_id IS NULL`
row. Master's own history renders no `outcome` field at all.)

### D1 — the outcome derivation

First boot of the branch on that folder, **before any branch job ran**:

```
id 5  doctr/…mobilenet_v3_small  completed=0 status=None  OUTCOME='failed'
id 4  clip/ViT-B-32_openai       completed=1 status=1     OUTCOME='completed'
id 3  clip/ViT-B-32_openai       completed=0 status=-1    OUTCOME='cancelled'
id 2  doctr/…mobilenet_v3_small  completed=0 status=-1    OUTCOME='cancelled'
id 1  tags/wd-vit-tagger-v3      completed=1 status=1     OUTCOME='completed'
```

and `/api/jobs/data/failures` agrees, row for row
(`logs/A-branch-failures.json`): `failed_jobs_total 3`, log id 5 `"failed"`,
log ids 3 and 2 `"cancelled"`, `job_failures_total 0`. run4's single defect
here — *every* pre-upgrade row reading `running` forever — is gone. See **O1**
for what the next job then does to row 5.

### D2 — the accelerator re-sync

The branch's `uv.lock` differs from master's, so `auto_setup_needed()` fires
for every existing user, exactly as in run4. What follows is the fix:

```
INFO panoptikon::config: the managed Python environment is missing or incomplete;
     setup will run automatically at startup reason="uv.lock changed since setup last completed"
INFO panoptikon::setup: accelerator selected requested=Cpu accelerator=Cpu extra="cpu"
     evidence="the extra the managed venv was synced for"
INFO panoptikon::setup: syncing the locked environment (uv sync) extra="cpu" wheels="cpu"
INFO panoptikon::setup: Resolved 138 packages in 2ms
INFO panoptikon::setup: Uninstalled 5 packages in 39ms          (the `test` group)
INFO panoptikon::setup: Python inference environment is ready extra="cpu"
```

`grep -ci 'cu128\|Downloading nvidia'` over the whole boot log = **0**
(`logs/A-branch-autosetup.log`). The sentinel afterwards still says
`extra=cpu`, with the new lock hash. nvidia-smi is on PATH on this host, so
run4's `accelerator=Cuda extra="cu128" evidence="nvidia-smi on PATH"` and its
693 MiB cuDNN download are the pre-fix behaviour of this exact configuration.

**The `mps` half.** A copy of the same root with the sentinel edited to
`extra=mps` and a bogus lock hash, started with `UV_OFFLINE=1` so no bytes
could be fetched:

```
INFO  accelerator_report: accelerator backend backend="mps" backend_source=managed venv (setup sentinel)
WARN  setup: the managed venv was synced for an accelerator this platform cannot install;
      re-probing the host installed=Mps
INFO  setup: accelerator selected requested=Auto accelerator=Cuda extra="cu128"
      evidence="nvidia-smi on PATH"
INFO  setup: Installed 18 packages in 1.89s        (entirely from the uv cache)
```

(`logs/A-mps-sentinel-reprobe.log`.) It re-probes with a log line and does not
fail. A data folder carried from a Mac to this Linux box therefore boots.

### The rest of A

* **0 `ERROR` lines** on the first branch boot.
* **Migrations + ANALYZE** on both index DBs (`default`, `up`).
* **The batch-auto migration fires**: `batch sizes in the stored job settings
  and cron schedule were reset to auto index_db="up"`, and the only difference
  in `config.toml` is the two removed keys (`batch_size = 5`,
  `default_batch_size = 7`).
* **No new `SystemConfig` keys**: `GET /api/jobs/config` before and after is
  key-identical (set difference empty both ways); the cleared values read back
  as `null` (= auto).
* **Settings round-trip**: `GET` → `PUT` unchanged → 200, file md5 unchanged;
  one unrelated setting changed (`detect_outros`) → 200, file still parses
  (`tomllib`), value applied.
* **Search still works**: 30 items.
* **A new branch extraction job completes**: `doctr/…mobilenet_v3_small`, 29
  segments, `errors 0`, `failed_items 0`, `outcome "completed"`, `batch_size 0`
  (auto).
* **The calibration store is written** at `data/inferio/calibration.toml`
  (`arch = "cpu"`, `base_method = "rss"`, `gpu = "CPU (128 GB)"`).
* **Health**: one synthetic device, `uuid CPU`, `device_kind "cpu"`,
  `total_mb 128649`, `cap_fraction 0.75`, `external_source "ram"`,
  `ledger_error None`, `load_cooldowns []`.
* **Downgrade: D4**, above.

## B — fresh install of the branch, no GPU

Root built from the branch checkout, ports 17852/17853/17849, shipped
`config/server/default.toml` with only `local = false` and the ports changed.
0 `ERROR` lines on first boot.

Four jobs, all on the CPU inference server, all clean:

| job | segments | errors | failed_items | outcome | inference s | (run4) |
|---|---|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | 30 | 0 | 0 | completed | 8.0 | 6.4 |
| `doctr/db_resnet50_crnn_mobilenet_v3_small` | 30 | 0 | 0 | completed | 22.7 | 18.3 |
| `textembed/all-mpnet-base-v2` | 60 | 0 | 0 | completed | 21.0 | 22.3 |
| `clip/ViT-H-14-378-quickgelu_dfn5b` | 30 | 0 | 0 | completed | 39.0 | 38.9 |

`/health` reports one synthetic device and no ledger error:

```json
"gpus": [{"index":0,"uuid":"CPU","name":"CPU (128 GB)","total_mb":128649,
          "compute_cap":null,"unified_ram_mb":128649}]
"vram": [{"gpu_uuid":"CPU","gpu_name":"CPU (128 GB)","device_kind":"cpu",
          "total_mb":128649,"limit_mb":96486,"reserve_rule":"capped_default",
          "margin":0.1,"cap_fraction":0.75,"grants_outstanding":0,"workers":[]}]
"load_cooldowns": []
```

### The CPU admission path, asserted field by field

| assertion | measured | where |
|---|---|---|
| device key `CPU` | `"uuid": "CPU"` | `/api/inference/health`; `cpu.rs DEVICE_KEY` |
| the row says which kind of device it is | `"device_kind": "cpu"` | `/health` (new since run4) |
| `free_source = "ram"` | `free_source ram`, `86299 / 128649 MiB` | `selftest.py` with `INFERIO_DEVICE=cpu` (`logs/B-selftest-cpu.log`) |
| `base_method = "rss"` | `base_mb 621`, `base_method rss`; ledger `base_mb=Some(486) base_method="rss"` | selftest + gateway trace |
| `cap_fraction 0.75` | `0.75`, `limit_mb 96486 = 0.75 x 128649` on an empty device | `/health` |
| no `index_limit` / shape-ceiling clamp | `index_limit_events 0` | selftest |
| selftest verdict | `VERDICT: no degraded tiers` | `logs/B-selftest-cpu.log` |
| **glibc pinning reaches the worker** | `MALLOC_MMAP_THRESHOLD_=131072`, `MALLOC_TRIM_THRESHOLD_=131072`, `INFERIO_DEVICE=cpu`, `CUDA_VISIBLE_DEVICES=` in `/proc/<worker>/environ` | `logs/B-worker-env.txt` |

The worker was identified by parentage (`/proc/<pid>/stat` ppid == the
gateway's pid): this box runs another agent's `inferio_worker` processes and a
bare `pgrep -f inferio_worker` picks the wrong one.

**Restart and resume**: after `SIGTERM` and a restart, the four history rows
are intact and all read `completed`; a rescan picked up 12 new images and a
second `tags` job ran them (12 segments, 0 errors, completed).

**Search on both new setters** (`/api/search/pql`): `match_path` 30,
`text_embeddings` (`all-mpnet-base-v2`) 30, `image_embeddings`
(`ViT-H-14-378`) 30 — the query-side model loads work too.

### Log growth at the shipped level

No `RUST_LOG`, shipped `[logging] level = "${LOGLEVEL:-INFO}"`, file
`<data folder>/panoptikon.log`, no rotation:

| work | bytes added | per 1000 items |
|---|---|---|
| extraction, `clip/ViT-B-32_openai`, **2 042 items** | 6 061 | **3.0 KB** |
| file rescan that *added* 2 000 files | 1 603 | 0.80 KB |
| file rescan of 2 042 unchanged files | 1 165 | 0.57 KB |

Same shape as run4: a fixed few KB per job, single-digit MB/day for a user
indexing a million items through a handful of jobs.

### The CPU ramps

Cold store per model, `RUST_LOG=info,panoptikon::inferio=trace`, the 2 000-image
`ramp` corpus, one model per job, worker RSS sampled from `/proc` at 4 Hz and
**filtered to this gateway's own worker pids**. Rungs are grant→settle windows.

**`tags/wd-vit-tagger-v3` — 2 000 items, 422.1 s inference, 0 errors**

| rung | n | items/s | median grant MB | peak worker RSS MB |
|---|---|---|---|---|
| 1 | 1 | 0.891 | 88 087 | — |
| 2 | 1 | 0.940 | 86 418 | — |
| 4 | 1 | 1.651 | 86 753 | — |
| 7 | 12 | 1.679 | 263 | — |
| 8 | 1 | 1.650 | 457 | — |
| 15 | 12 | 1.659 | 550 | 1 316 |
| 16 | 3 | 1.335 | 594 | 1 350 |
| 32 | 2 | 1.159 | 1 168 | 1 919 |
| 64 | 1 | 1.658 | 2 330 | 3 007 |
| 81 | 1 | 6.039 | 2 864 | 3 077 |
| 128 | 2 | 2.503 | 4 542 | **5 178** |

(the RSS sampler was started after the ramp had passed rung 8, hence the
blanks.) The knee fitted at **7** — the same figure run4 measured — and was
then widened and withdrawn: **N1**. Peak RSS **5 178 MB = 5.4 %** of the
96 486 MB budget; largest charge 4 542 MB = **4.7 %**. Nothing chased RAM.

**`clip/ViT-B-32_openai` — 2 000 items, 61.0 s inference, 0 errors**

| rung | 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 256 |
|---|---|---|---|---|---|---|---|---|---|
| items/s | 1.65 | 3.89 | 3.97 | 4.57 | 6.19 | 8.10 | 7.78 | 12.39 | **24.15** |
| grant MB | 87 147 | 87 198 | 87 107 | 144 | 230 | 366 | 640 | 1 044 | 1 749 |
| worker RSS MB | 1 057 | 1 075 | 1 097 | 1 130 | 1 187 | 1 288 | 1 484 | 1 880 | **2 743** |

Throughput rises monotonically to 256, where the ramp held when the job ran
out of work (`holding the throughput ramp … units=256`). Peak RSS **2 743 MB =
2.8 %** of the budget.

**This is the F fix.** run4 recorded that *every* one of CLIP's 15 grants was
`pre_fit=true`, each charging the whole ~72 GB share, with a stored profile of
`samples = 0`, `slope_mb_per_unit = 0.0` — the model was never priced and the
device's whole budget stayed charged to it for the length of the job. On this
tip:

```toml
inference_id = "clip/ViT-B-32_openai"
base_mb = 848            base_method = "rss"
slope_mb_per_unit = 6.6413690476190474
samples = 9              residual_mb = 12.52
max_units_measured = 256
```

and the grants go `pre_fit=true` x3 (the first three windows) then
`pre_fit=false` x8 — visible in the grant row above as the charge dropping from
87 107 MB at rung 4 to **144 MB** at rung 8. A second model can now be admitted
concurrently.

**`textembed/all-MiniLM-L6-v2` — 4 000 text segments, 15.7 s inference, 0 errors**

Cost dimension is `token` / `max-times-count`, so the rungs are token counts:
`59 → 118 → 203 → 406 → 728 → 1 424 → 2 772 → 5 518 → 11 020 → 22 040`,
units/s rising throughout (125 → ~9 500), held at 22 032 when the job ran out
of work. No knee — the job is too short to produce a plateau, same as run4.
Charge at the top rung **496 MB**, peak worker RSS **954 MB = 1.0 %** of the
budget. Store: `base_mb 220`, `base_method "rss"`, `slope 0.01925 MB/token`,
**52 samples**; grants `pre_fit=true` x2 then `pre_fit=false` x15.

## C — a user's hand-edited server TOML, and a hand-written per-DB config

Server TOML = **master's shipped `config/server/default.toml`** with the edits
a real user makes, and with no key the branch added
(`[inference_local.vram]` absent entirely — `grep -c` = 0):

```toml
data_folder = "mydata"          # moved off the default
temp_dir = "mydata/tmp"
[logging] level = "DEBUG"       # replaced the ${LOGLEVEL:-INFO} bridge with a literal
[server] port = 17892           # and 17893 / 17889 for the other two listeners
[inference_local.prewarm] enabled = true, lazy = false, always_warm = ["wd_tagger"]
```

The branch honours every one — `logging to file path=mydata/panoptikon.log`,
the three listeners on the user's ports, 214 DEBUG lines in the first boot, and
`prewarm: {"enabled":true,"lazy":false,"warm":[{"impl_class":"wd_tagger","state":"warm"}]}`
— with **0 `ERROR` lines**, and defaults everything the file does not mention:

| branch-added key, absent from the file | value in force |
|---|---|
| `inference_local.vram.margin` | `0.1` |
| `inference_local.vram.cap_fraction` | `0.75` (`limit_mb 96486`) |
| `transcode.hover_preview` | `"auto"` → `{"direct":true,"trim":true,"transcode":false,"max_bytes":16777216}` on `/api/client-config` |

**Per-DB config with comments and non-canonical key order.** A hand-written
`config.toml` (comments on several keys, `cron_schedule` first, `job_settings`
as an `[[array of tables]]`, `thumbnail_formats = ["jpeg"]`,
`default_batch_size = 12`) was dropped into a DB directory and the gateway
restarted. `GET /api/jobs/config` returned every value correctly
(`cron_schedule "30 4 * * 1"`, `enable_cron_job true`, `scan_video false`,
`default_batch_size 12`), and `PUT` of that same body left the file
**byte-identical** — `diff` clean, md5 unchanged — comments, ordering and the
user's 12 all preserved. The batch-auto migration correctly did *not* clear
the 12: the DB was created by the branch, so it was stamped at creation.

## D — Docker

```bash
docker build --build-arg ACCELERATOR=cpu  -t final-panoptikon:cpu  .   # 3.50 GB
docker build --build-arg ACCELERATOR=cuda -t final-panoptikon:cuda .   # 9.42 GB
docker compose -f final-cpu.yml up -d       # copy of deploy/docker-compose.yml, ports 17942/17939
docker compose -f final-c6.yml  up -d       # + mem_limit/memswap_limit 16g, ports 17962/17959
docker compose -f final-cuda.yml up -d      # CUDA image, NO --gpus, ports 17972/17969
```

**Config volume seeding.** First start seeds `final-panoptikon-config` from the
image with exactly `server/docker.toml` and `inference/example.toml`. Then
`cap_fraction = 0.5` uncommented in `[inference_local.vram]` on the volume, and
a restart:

```
md5 before restart fb335a975932815635cfe07be4e37abc
md5 after  restart fb335a975932815635cfe07be4e37abc      (not overwritten)
/health -> "cap_fraction": 0.5, "limit_mb": 64324   (= 0.5 x 128649)
```

Seeded once; the user's edit survives and takes effect. A deliberately broken
edit (a duplicated `[inference_local.vram]` header) still produces a precise
message and a restart loop, with the reason in the log:

```
281 | [inference_local.vram]
    | ^
  invalid table header
  duplicate key `vram` in table `inference_local`
```

and removing the bad line brings it straight back up. One CPU job inside over a
read-only bind mount: `tags/wd-vit-tagger-v3`, 30 segments, 0 errors,
`outcome "completed"`.

### D5 — under a 16 GiB cgroup

`memory.max = 17179869184`, `memory.swap.max = 0`, both accepted.

| | run4-deploy | final-deploy |
|---|---|---|
| device the ledger reports | `CPU (128 GB)`, `total_mb 128649` | **`CPU (16 GB)`, `total_mb 16384`** |
| budget after `cap_fraction 0.75` | `limit_mb 96486` | **`limit_mb 12288`** |
| what the kernel enforces | 16 384 MiB | 16 384 MiB |
| overcommit | **5.89x** | **1.00x** |

The **free** side is bounded too, which is the other half of the fix. With an
8 GiB allocation held *inside* the container (`memory.current` 10.0 GB, of
which `active_file` 212 MiB, `inactive_file` 0 — the exact case the fix's
comment calls out):

```
external_mb 9582   external_source "ram"   external_sample_age_ms 1294
limit_mb 5843      headroom_mb 4711        charges_mb 1132
```

Before the fix that reading came from the host's non-namespaced
`/proc/meminfo` and the limit would not have moved at all.

The worker was **ADMITTED with no cross-check warning** — the container's whole
log for the leg contains **zero** `WARN` and zero `ERROR` lines — because the
worker now reads the same cgroup limit as the orchestrator:

```
DEBUG ledger: admitted a worker to a GPU's ledger model=tags/wd-vit-tagger-v3 gpu=CPU
      replica=2 base_mb=Some(497) base_method="rss" reserved_at_load_mb=Some(1170)
```

grants were issued (`charges_mb 12288`, `grants_outstanding 1` mid-job), the
job completed (180 segments, 0 errors, 0 failed items), and a store row was
written *keyed to the container's own device*:

```toml
inference_id = "tags/wd-vit-tagger-v3"
arch = "cpu"    gpu = "CPU (16 GB)"    backend = "cpu"    torch = "2.7.1+cpu"
base_mb = 497   base_method = "rss"    slope_mb_per_unit = 36.5
knee_units = 7  samples = 6            max_units_measured = 16
```

**Log growth per 1000 items, inside the container, at the shipped level**
(same 16 GiB container recreated without the protocol's `RUST_LOG` /
`LOGLEVEL` / `INFERIO_WORKER_LOG_LEVEL`; `docker logs` bytes, since the Docker
layout logs to stdout and writes no `panoptikon.log`):

| work | bytes added | per 1000 items |
|---|---|---|
| extraction, `clip/ViT-B-32_openai`, 2 000 items, shipped level | 7 811 | **3.9 KB** |
| the same container at `RUST_LOG=…inferio=trace` + worker DEBUG | ~591 KB total | ~260 KB |

### The CUDA image with `--gpus` ABSENT

Still starts, still loud — and no longer unpriced:

```
INFO accelerator_report: accelerator backend backend="cuda" backend_source=managed venv (setup sentinel) devices=[]
WARN accelerator_report: backend is cuda but no GPU name could be detected for stack 'nvidia'
WARN inferio::gpu: this host is configured for CUDA but nvidia-smi was not found on PATH;
     workers will not be pinned, batch sizes will not be calibrated and model availability
     will not be capability-filtered
INFO inferio::gpu: admitting batches from workers that run on the CPU against system RAM …
     uuid=CPU name=CPU (128 GB) total_mb=128649 default_cap_fraction=0.75 accelerators=0
```

run4 saw `"gpus": []` and `"vram": []` here — no device, no admission, no
calibration. Now `/health` carries the CPU device (`limit_mb 96486`), the
`tags` job over 30 images ran to `completed` in 7.6 s with 0 errors, and a
calibration profile was written (`arch = "cpu"`, `gpu = "CPU (128 GB)"`,
`backend = "cuda"`, `torch = "2.7.1+cu128"` — see **O2**). The container really
had no GPU: `which nvidia-smi` → nothing, `/dev/nvidia*` → nothing.

## D3 — the explicit-python-on-a-CUDA-host case

One gateway, started **without** the `CUDA_VISIBLE_DEVICES` mask so the host
process sees both cards, with `[inference_local] python` pointing at the CPU
venv and the managed venv deleted so nothing could fall back to it. This is
exactly the configuration run4 filed as **D3** (zero grants, no ramp, no
calibration row, and the loss logged at DEBUG).

`/health` on this tip, before any job — the CPU row **beside** the GPU rows:

```json
"gpus": [
 {"index":0,"uuid":"GPU-942d9e56-…","name":"NVIDIA RTX PRO 6000 …","total_mb":97887,"compute_cap":"12.0"},
 {"index":1,"uuid":"GPU-01c61d5b-…","name":"NVIDIA RTX PRO 6000 …","total_mb":97887,"compute_cap":"12.0"},
 {"index":0,"uuid":"CPU","name":"CPU (128 GB)","total_mb":128649,"unified_ram_mb":128649}]
```

```
INFO inferio::gpu: admitting batches from workers that run on the CPU … uuid=CPU
     name=CPU (128 GB) total_mb=128649 default_cap_fraction=0.75 accelerators=2
```

Mid-job (`clip/ViT-B-32_openai` over 30 images), the replica is on the CPU row
and **the GPU rows carry none**:

```
CPU   cpu   workers [{"inference_id":"clip/ViT-B-32_openai","footprint_mb":848,
                      "charge_mb":76889,"base_mb":848,"reserved_at_load_mb":1566,
                      "grants_outstanding":1,"grants_mb":76041, …}]
GPU-01c61d5b-…  cuda  workers []
GPU-942d9e56-…  cuda  workers []
```

The ledger's own line is `admitted a worker to a GPU's ledger … gpu=CPU`,
grants are issued and leave `pre_fit` at rung 8, both jobs completed with 0
errors, and a calibration profile was written. The whole gateway log contains
**one** `WARN` — `no headless browser found; HTML files will not be indexed` —
and **zero** occurrences of `without VRAM admission`, `names no device` or
`no GPU this GPU inventory lists`. D3 is fixed at its root: the worker is
placed on the device its load report names, so there is nothing left to warn
about.

## E — Nix

`command -v nix` → nothing. **Skipped**, as in run4.

## Protocol legs

**`legs.py --scenario S14`** (CPU, a copy of `server-C1.toml` with ports
17912/17913/17909, `--python` the CPU venv, a freshly generated `smoke`
corpus). Outcome `drained`, 180 items, the CI smoke assertions all green
(`pql 200/180`, `thumbnail 200`, `file 200`, both extra endpoints 200):

```
failures      PASS  0 OOM negatives, 0 throughput-collapse negatives,
                    0 unified-memory-device death negatives, 0 fatal worker deaths,
                    0 merged-window fallbacks
job_outcome   PASS  1 job record: 1 completed, 0 failed items, 0 errors;
                    queue outcomes all 'completed'
grant_safety  WARN  8 grants; 0 exceeded the headroom they were priced against;
                    0 memory-blind (mb=0) -- ORACLE CLAUSE NOT RUN: no grant joined a
                    vramrec sample within 1.5s (vramrec's oracle is NVML per-process;
                    a CPU-device grant has nothing for it to join, so this WARN is the
                    harness, not the ledger)
peak_fds      INFO  peak 53 descriptors over 199 samples, soft limit 524288 (0%)
```

Identical to run4's S14 verdicts. Store: `base_mb 483`, `base_method "rss"`,
`slope 41.5`, `samples 6`, `max_units_measured 16`.

**`legs.py --scenario S2`** (cold ramp, empty store, the 2 000-image `ramp`
corpus, CPU). Outcome `drained`, 2 000 items in 486 s, `analyze.py --checks all
--learning`:

```
failures             PASS   0 of everything
job_outcome          PASS   1 completed, 0 failed items, 0 errors
deflation_recovery   PASS   deflation never left 0 on any worker
idle_liveness        PASS   final grants_outstanding = 0 over the last 60 s
persistence          PASS   worst anchor-advance -> store-write delay 28.3 s [threshold 30 s]
calibration_learned  PASS   unit_budget 64 -> peak 64 (last 32, fit samples 9, knee 15);
                            1 profile in the store
throughput           INFO   4.115 items/s
ramp_progress        INFO   unit_budget 64 -> peak 64 (last 32, knee 15, low 2)
peak_fds             INFO   peak 59 descriptors over 1085 samples (0% of the limit)
grant_safety         WARN   44 grants; 0 exceeded their priced headroom; 0 memory-blind
                            -- ORACLE CLAUSE NOT RUN (the same CPU-device harness caveat)
ledger_invariant     WARN   strict: 3 of 1086 GPU-samples had charges + load reservations
                            > limit_mb (0 over_grant; 3 limit_fell -- the CPU device's limit
                            tracks host RAM, so it can drop under a footprint already held).
                            Our own residents: 0 of 44 grants exceeded their priced headroom
oracle_agreement / base_accuracy / footprint_agreement / slope_accuracy /
utilization / hog_tracking / alloc_retries:  SKIP (all need an NVML oracle or a
ceiling_probe, neither of which exists for a CPU-priced device)
```

This leg also shows **N1** (`fitted a throughput knee … knee_units=7` at
02:00:22, `…has been withdrawn` at 02:03:19, and no `knee_units` in the store).

---

## Environment notes

1. **The box is shared.** Another agent ran `inferio_worker` processes
   throughout: three foreign worker pids were sampled during the wd-vit ramp
   (peaks 8 905, 4 452 and 1 308 MB RSS). Every RSS figure in this report is
   filtered to this run's own worker pids by `/proc/<pid>/stat` parentage, and
   **N1**'s "something outside this ledger was moving" is that contention.
2. **Ports.** Everything here ran on 1784x/1785x/1786x/1787x/1788x/1789x/1790x,
   1791x and 1794x/1795x/1796x/1797x, never the shipped 6342/6343/6339 — except
   the one aborted S14 attempt that **O4** describes.
3. `.txt` files are not indexed on any configuration, so the four in the
   30-item corpus contributed nothing; `textembed` was fed from the
   `extracted_text` rows the tagger and the OCR wrote, which is the documented
   route.
4. **Cleanup.** `final-panoptikon:cpu`, `final-panoptikon:cuda`, the three
   compose projects and their nine volumes, and the `busybox` image pulled to
   read the config volume were all removed at the end of the run.
