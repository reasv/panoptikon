# run5-mixed (macOS, MPS + CPU) — verification of `fix/mixed-devices`

Host: M3 Max, 131 072 MiB unified RAM, macOS 26.6.2. Tree:
`verify/mixed-devices` (f4fb9768 = fix/mixed-devices merged with a99e8b54).
Config: `.mac/run5/server-C8-mac.toml` with `.mac/run5/registry-C8/`, which
pins `florence2/msft_large-caption` to `devices = ["cpu"]`. CLIP runs on
Metal. 152 Florence2 items + 160 CLIP items.

## Both devices are priced, each on its own backend

Final `/health` `vram[]`:

```
CPU      cpu  arch cpu       total 131072  ext 18569 ram  limit  98304  headroom 86586  fp 11718
  florence2/msft_large-caption  fp 11718  base 3383 (rss)  unit_budget 16  clean 20
                                fit slope 1216.0 MB/item, residual 4.0, 4 samples
GPU-MPS  mps  arch apple-m3   total 110100  ext 28437 mps  limit 101611  headroom 100450  fp 1161
  clip/apple_MobileCLIP-S1      fp 1161   base 1160 (mps)  unit_budget 128  clean 3
```

No OOM, no failed request, `memory_pressure` stayed at 97 % free and
`vm.swapusage` at 0.00 M for the whole run (`pressure.log`).

## The unified-memory accounting question

Physical RAM 131 072 MiB. Over the run (`health-stream.jsonl`, 40 samples):

| | CPU device | MPS device |
|---|---|---|
| `total_mb` | 131 072 (physical RAM) | 110 100 (`recommended_max_memory`) |
| `external_mb` | 17 691 – 22 786 | 28 437 |
| `footprints_mb` (ours) | 11 718 (Florence2 RSS) | 1 161 (CLIP Metal) |
| `external + ours` | 29 214 – 30 053 | 29 598 |
| `limit_mb` | 98 304 (0.75 cap) | 101 611 |

Both devices measure the **same** machine occupancy (~29.6–30.1 GiB) and
attribute it differently: the CPU replica's 11 718 MiB of RSS sits inside the
MPS row's `external_mb` (it is not netted as "ours" there), and CLIP's
1 161 MiB of Metal sits inside the CPU row's `external_mb`. So **no device
claims the other's bytes as its own** — each charges them as external, the
conservative direction, and `external_locked` subtracts only that device's own
footprints.

What is *not* bounded is the pair. Σ`limit_mb` = 199 915 MiB = **1.53x**
physical RAM, and Σ`headroom_mb` sat at 185 000–186 000 MiB against roughly
101 000 MiB actually free — **1.83x**. The two ledgers are independent: a grant
on one device is invisible to the other's headroom until that device's next
external refresh, so two large simultaneous grants can over-commit the machine.
On a discrete-GPU host this cannot happen (VRAM and RAM are separate pools);
it is specific to the unified host, and neither
`docs/unified-memory-admission.md` nor the design-doc section added in
9b87a20d discusses it.

Note also that the MPS row's `external_mb` froze at 28 437 from sample 6
onward: it is refreshed from MPS worker frames and from the MPS probe, and CLIP
was idle after its five requests, so the MPS device's view of the CPU replica's
growth to 11.7 GiB never arrived during the run.

## Florence2 first-request latency (no prewarm claim)

28.36 s wall for `load + one predict` from a cold gateway, of which the load
was ~19 s (`gateway.log`). A CPU-pinned replica can never claim a pooled worker
on a CUDA/ROCm host (its pin is `""`, the pool's is the default GPU), so that
import cost is paid on every cold load there.

## prewarm-claim/ — a defect

On a host with **no pin vocabulary** (MPS) the CPU pin resolves to `None`,
which equals the pool's pin, so the second load of a CPU-pinned model claims a
pooled worker that was spawned without `INFERIO_DEVICE=cpu` and the model runs
on Metal. Load 1 -> `CPU`, `base_method="rss"`, `gpu_arch="cpu"`; load 2 ->
`GPU-MPS`, `base_method="mps"`, `gpu_arch="apple-m3"`, 3 120 MiB. See
`prewarm-claim/health-load{1,2}.json` and the `claimed a prewarmed worker from
the pool impl_class="florence2"` line in `prewarm-claim/gateway.log`.
