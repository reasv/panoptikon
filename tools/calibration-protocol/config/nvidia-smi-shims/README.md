# `nvidia-smi` shims — scenario S13 (probe robustness)

Three drop-in replacements for `nvidia-smi`, used by
`docs/batch-calibration-test-protocol.md` §4 **S13** and finding **B13**. The
gateway locates the binary by walking `PATH` only
(`inferio/capability.rs: find_nvidia_smi`; on Windows also
`%SystemRoot%/System32`) — there is no configuration key for it — so every
S13 case is set up by giving the gateway a different `PATH`.

**Never put these on a PATH the user's own shell inherits.** Export `PATH`
for the gateway process alone.

| file | what it does | what it exercises |
|---|---|---|
| `slow-all` | sleeps 6 s, then execs the real binary | the 5 s **boot inventory** probe times out → whole inventory unknown, unpriced |
| `slow-memory` | sleeps 6 s on the `memory.free` query only | a GPU exists but its host refresh always times out → the load-path (T2) stall, the 10 s failure backoff, B13's thread pile-up question |
| `malformed` | answers instantly with an unparseable identity row | `parse_inventory`'s all-or-nothing rule → whole inventory unknown, unpriced |

Environment (all three): `SMI_SHIM_LOG` appends one line per invocation,
`SMI_SHIM_DELAY` overrides the 6 s, `SMI_SHIM_REAL` overrides
`/usr/bin/nvidia-smi`.

## Usage

```bash
T=tools/calibration-protocol
P=$T/config/nvidia-smi-shims
D=$T/results/<run>/<scenario>

# S13-2a / S13-2b / S13-3 — prepend one shim dir. Give the chosen shim the
# name `nvidia-smi` in a scratch directory (the files here are named after
# the case so all three can live in one directory in git).
mkdir -p /tmp/smi-shim && cp "$P/slow-memory" /tmp/smi-shim/nvidia-smi
env PATH="/tmp/smi-shim:$PATH" SMI_SHIM_LOG="$D/shim.log" \
  "$T/config/run-gateway.sh" C1 "$D/root"
```

## S13-1 — hiding `nvidia-smi` entirely

There is no shim for this: `find_nvidia_smi` must find nothing, so the
gateway needs a `PATH` in which no directory holds an `nvidia-smi`. A
non-executable or failing stub is **not** the same case — it makes the probe
*fail*, not *not exist*. On a host where only `/usr/bin` carries the binary,
build a scratch mirror of `/usr/bin` without it:

```bash
mkdir -p /tmp/nosmi/bin
for f in /usr/bin/*; do
  b=$(basename "$f"); [ "$b" = nvidia-smi ] && continue
  ln -sf "$f" "/tmp/nosmi/bin/$b"
done
env PATH="/tmp/nosmi/bin:/usr/local/sbin:/usr/local/bin:/opt/cuda/bin" \
  "$T/config/run-gateway.sh" C1 "$D/root"
```

Check first which directories actually carry it:

```bash
for d in $(echo "$PATH" | tr ':' ' '); do [ -x "$d/nvidia-smi" ] && echo "$d"; done
```

The managed interpreter is invoked by absolute path, so dropping `/usr/bin`
from the gateway's `PATH` does not break the worker; keep `LD_LIBRARY_PATH`
as `config/env.C1` sets it.
