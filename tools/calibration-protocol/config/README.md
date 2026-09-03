# Server configs for the calibration protocol

One copy of the shipped `config/server/default.toml` per configuration id from
`docs/batch-calibration-test-protocol.md` §3, plus the environment each one
needs and a launcher. The shipped configs are never edited (CLAUDE.md: the
server TOMLs are seeded once and user-owned).

| file | configuration | ports (main/test/legacy, ui) | binary |
|---|---|---|---|
| `server-C1.toml` + `env.C1` | primary: PR branch, both GPUs visible | 6342 / 6343 / 6339, 6340 | branch `target/release/panoptikon` |
| `server-C0.toml` + `env.C0` | "before" baseline: master worktree | 6352 / 6353 / 6349, 6350 | `../../../panoptikon-master/target/release/panoptikon` |
| `server-C2.toml` + `env.C2` | C1 + `CUDA_VISIBLE_DEVICES=GPU-<uuid>` (UUID form) | 6362 / 6363 / 6359, 6360 | branch |
| `server-C3.toml` + `env.C3` | C1 + `CUDA_VISIBLE_DEVICES=1` (index form) | 6372 / 6373 / 6369, 6370 | branch |

C4–C7 are not here: C4/C5/C6 are Docker configurations (image build args and
compose overlays, Phase 6) and C7 is C1 plus a user registry file, so it uses
`server-C1.toml` unchanged.

## Running one

```
./run-gateway.sh C1 /abs/path/to/results/<run-id>/<scenario>
```

The second argument becomes `--root`, which panoptikon implements as a chdir
at startup, so the scenario owns its own `data/panoptikon.log`,
`data/inferio/calibration.toml`, `data/index/*` and `data/tmp`. That chdir is
also why each config pins `[inference_local]`'s `python`, `impl_dirs`,
`config_dirs` and `pythonpath` to absolute paths, and why `run-gateway.sh`
exports the checkout's `.env` itself instead of relying on the CWD auto-load.

The three deviations from the shipped default (ports, `[upstreams.ui] local =
false`, the absolute inference paths) are marked `CALIB` and explained inline
in each file. Everything else — including the empty `[inference_local.vram]`
table, so `margin` stays at its built-in 0.10 and `cap_fraction` stays off —
is byte-identical to `config/server/default.toml`.

Setting `python` also short-circuits the startup auto-setup
(`setup.rs::maybe_auto_setup` returns early when `python` is set). That is
deliberate: the venvs are synced by hand in Phase 0, the branch one **with**
the `test` group, and the server's own `uv sync --locked --extra cu128` would
uninstall it.

## Adding the fault-injection fixtures

See `../fixtures/README.md`. Either run `../fixtures/install-fixtures.sh`
(copies into `inferio_custom/` and `config/inference/`, which these configs
already point at) or extend `impl_dirs` / `config_dirs` here.
