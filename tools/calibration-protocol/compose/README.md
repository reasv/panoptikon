# Phase 6 compose files (C4, C5, C6)

Docker configurations for the batch-calibration test protocol
(`docs/batch-calibration-test-protocol.md` §3, scenarios S10, S11, S12,
S14). They are adaptations of the repo-root `docker-compose.yml`; every
line that deviates from it is marked `CALIB` and explained inline in the
file itself. This README covers what is *not* obvious from the files.

| File | Protocol row | Image | Distinguishing feature | Admin | Public |
|---|---|---|---|---|---|
| `docker-compose.C4.yml` | C4 | `panoptikon:calib-cuda` | shipped compose shape, **no** `pid: host` | 6442 | 6439 |
| `docker-compose.C5.yml` | C5 | `panoptikon:calib-cuda` | C4 **plus** `pid: host` | 6452 | 6449 |
| `docker-compose.C6.yml` | C6 | `panoptikon:calib-cpu` | no GPU, `mem_limit: 16g`, no swap | 6462 | 6459 |

All six host ports are bound to `127.0.0.1` only, and all six avoid the
bare-host gateway configurations C0–C3, which own 6342/6343/6339 and
6352/6353/6349. Container-side ports are untouched (6342 admin, 6339
public) so the image's baked `HEALTHCHECK` and the shipped
`config/server/docker.toml` keep working verbatim.

## Building the images

Both images are built by hand, once, from the **committed** `Dockerfile`
at the branch commit under test — the compose files carry no `build:`
block so that a stray `--build` cannot re-roll an image mid-phase:

```
docker build --build-arg ACCELERATOR=cuda -t panoptikon:calib-cuda .
docker build --build-arg ACCELERATOR=cpu  -t panoptikon:calib-cpu  .
```

They are two separate images on purpose. `panoptikon setup` keys the venv
to one accelerator (recorded in the `.panoptikon-setup-complete`
sentinel), so a single image switched between `cpu` and `cuda` would
re-sync — and clobber — the cu128 venv, which the protocol's C6 row
explicitly rules out.

## Logging: which env var reaches what

Three logging env vars are set on every configuration. They are not
redundant; they land in three different places.

- **`RUST_LOG=info,panoptikon::inferio=trace`** — honoured in the
  container, and it is the one that matters. `panoptikon/src/logging.rs`
  reads it straight from the process environment before consulting the
  config file, and a non-blank value **wins over `[logging].level`**
  (full `EnvFilter` directive syntax; it is deliberately not absorbed
  into the config file). Compose `environment:` is a plain process env,
  so nothing else has to be wired. Append
  `,panoptikon::db::batch_auto=debug` when a scenario also wants the
  auto-batch/migration lines (`codemap.md` §1.9).
- **`INFERIO_WORKER_LOG_LEVEL=DEBUG`** — read by the Python worker
  itself (`python/inferio_worker/__main__.py`, `logging.basicConfig`).
  The gateway spawns workers with the environment it inherited plus a
  few additive vars, so a compose-level setting reaches them. Worker
  stderr is forwarded line by line by the gateway and re-logged at
  **INFO** under `panoptikon::inferio::worker`, so those lines survive
  the `info` floor in `RUST_LOG` and show up in `docker logs`.
- **`LOGLEVEL=DEBUG`** — **not read by any Rust code.** It only reaches
  the gateway through env templating of the line
  `level = "${LOGLEVEL:-INFO}"` in `config/server/docker.toml`
  (`env_template.rs`), i.e. only as long as the config file on the
  config volume still contains that template. And since `RUST_LOG` is
  set, it is outranked anyway. It is kept purely as a fallback: if a
  scenario swaps in a server TOML and drops `RUST_LOG`, the run is still
  verbose rather than silently INFO.

Log destination: `docker logs <container>` only. `docker.toml` sets
`[logging] file = ""`, which disables file logging, so there is no
`panoptikon.log` on the data volume to collect.

## The config volume, and how to inject a server TOML

`/app/config` is a **named volume**. The image ships
`/app/config/server/docker.toml` and `/app/config/inference/example.toml`;
Docker seeds a named volume from the image's content the first time the
volume is mounted at a non-empty path, so the first `up` copies those two
files onto the volume and **never touches them again**. Editing the file
on the volume and restarting is therefore the normal reconfiguration
path — and, conversely, changing the shipped TOML in the repo has no
effect on a volume that already exists. `docker compose -f <file> down -v`
to start from a clean seed.

`PANOPTIKON_CONFIG_PATH=/app/config/server/docker.toml` is baked into the
image, so that file is what gets loaded.

To run a scenario against a modified server TOML (e.g. a copy carrying a
non-empty `[inference_local.vram]` block with `margin` / `cap_fraction`,
or a per-board `[inference_local.vram.gpu."GPU-…"]` override), there are
two ways, in order of preference:

1. **Bind-mount a file beside it and repoint the env var.** Leaves the
   seeded file intact, so the diff against the shipped default stays
   visible:

   ```yaml
   volumes:
     - ../config/server-C4-vram.toml:/app/config/server/calib.toml:ro
   environment:
     - PANOPTIKON_CONFIG_PATH=/app/config/server/calib.toml
   ```

2. **Bind-mount over the seeded file** — same effect, one less moving
   part, but the shipped file is then invisible for the run:

   ```yaml
   volumes:
     - ../config/server-C4-vram.toml:/app/config/server/docker.toml:ro
   ```

Either way, start from a copy of `config/server/docker.toml` (not
`default.toml`): the container needs `host = "0.0.0.0"`, the `public`
endpoint on 6339, `[upstreams.ui] dir = "no-checkout-use-embedded-bundle"`,
and the `[jobs]` ffmpeg/pdfium paths that only the docker profile sets. A
bind-mounted file is read-only to the API's config-write path, so use
option 1 (or drop `:ro`) if a scenario commits config through the UI.

## Corpus and fixtures

`../results/corpus` is bind-mounted read-only at `/media/corpus` in all
three files. Generate the tiers on the host first
(`corpus.py --tier smoke`, `--tier ramp`, …), then add
`/media/corpus/<tier>` as an allowed folder through the admin API and
scan. The container runs as uid 1000; the corpus is written by the same
uid on this host, so no permission juggling is needed.

The fixture impls (`oom`, `oom_second_batch`, `failbatch`, `dying`) that
S4b/S4c need are **not** installed by `install-fixtures.sh` inside a
container — that script writes into a checkout. Instead, uncomment the
two bind mounts already present in each compose file:

```yaml
- ../fixtures/impls:/app/inferio_custom:ro
- ../fixtures/registry/calibration-fixtures.toml:/app/config/inference/calibration-fixtures.toml:ro
```

Those are exactly the two default scan locations resolved against the
container's CWD (`/app`): `resources.rs::default_impl_dirs` ends in
`inferio_custom`, and `RegistryConfig::default`'s `config_dirs` ends in
`config/inference`. `inferio_custom` is in `.dockerignore`, so the
directory does not exist in the image and the bind mount creates it. The
second mount is *nested under* the `/app/config` named volume, which
Docker allows — mounts are applied shortest-destination-first, so the
volume goes down first and the file lands on top of it.

Use the **CUDA** fixture variants (`fixtures/impls/*_cuda_impl.py`) on
C4/C5: the torch-free originals report no `gpu_uuid`/`base_mb` and only
register through the single-visible-board fallback.

## Operating notes

- **Never `up` a GPU configuration while another agent holds the GPUs.**
  C4/C5 reserve `count: all`. Check `nvidia-smi` and coordinate first.
- `restart: "no"` everywhere. A crash, a respawn loop or an OOM kill is
  the result being measured; `unless-stopped` (what the shipped compose
  uses) would paper over exactly the behaviour S11/S12 are looking for.
- Volume names are explicit (`name: calib-c4-data`, …) rather than
  project-prefixed, so `docker volume rm calib-c6-data` is unambiguous
  and one configuration's state can be wiped without touching another's.
- S10's "Docker volume path" step needs a volume seeded by a
  **master-built** image before C4 is pointed at it: build a master image,
  `up` it once against `calib-c4-data`, `down`, then `up` C4.
- For a hog running on the **host** while a container runs (S11's
  `external_mb` check), nothing extra is needed: board-level NVML totals
  work without `--pid=host`. It is the *per-process* query that does not,
  which is the whole C4-vs-C5 difference.
