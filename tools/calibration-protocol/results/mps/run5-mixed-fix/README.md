# run5-mixed-fix (macOS, MPS + CPU) — the two defects run5-mixed found, refixed

Host: M3 Max, 131 072 MiB unified RAM, macOS 26.6.2. Tree: `verify/mixed-devices`
at 5b921516 (64dc7c76 + the five commits of this fix). Same config as run5-mixed
(`.mac/run5/server-C8-mac.toml`, registry pins `florence2/msft_large-caption` to
`devices = ["cpu"]`); the pool-refusal leg adds one unpinned sibling id in the
same impl class (`.mac/run5/server-C8-pair.toml`).

## prewarm-claim/ — the cpu pin survives a reload

The run5-mixed defect: on a host with no pin vocabulary the `cpu` pin resolved
to `None`, which is also `default_pin()`, so the second load claimed a pooled
worker spawned without `INFERIO_DEVICE=cpu` and the model ran on Metal.

Load 1 and load 2 both land on the CPU device (`gateway.log`):

```
23:25:19  base_mb=3234 base_method="rss" gpu_name="CPU (128 GB)" gpu_arch="cpu"
23:25:19  admitted a worker to a GPU's ledger  gpu=CPU
23:25:40  base_mb=3235 base_method="rss" gpu_name="CPU (128 GB)" gpu_arch="cpu"
23:25:40  admitted a worker to a GPU's ledger  gpu=CPU
```

`replica loaded ... device=""` on both, and the log contains no `claimed a
prewarmed worker` line and no `warm` line at all: a cpu-pinned replica is no
longer claim-eligible, so `ModelManager::load` does not lazily warm the pool
for it either (`prewarm.warm` is `[]` in all three health snapshots). Against
run5-mixed, where load 2 came back `GPU-MPS` / `base_method="mps"` /
`gpu_arch="apple-m3"`.

## pool-refusal/ — and it is refused when the pool *does* hold one

The sharper case: an unpinned `florence2/msft_large-caption-metal` loads first
and lazily warms the pool on the default device, then the cpu-pinned id loads
with that worker parked.

```
23:28:40  msft_large-caption-metal  base_method="mps"  gpu_arch="apple-m3"  -> gpu=GPU-MPS
23:28:41  prewarmed worker parked impl_class=florence2 device="<unpinned>"
23:29:11  msft_large-caption        base_method="rss"  gpu_arch="cpu"       -> gpu=CPU
```

The parked worker is still `"state": "warm"` in `health-both.json` afterwards.

## The unified pair now charges both ways

`health-both.json`, with one replica on each device:

```
CPU      total 131072  ext 22374 ram  limit  98304  headroom  91953  charges 3231
GPU-MPS  total 110100  ext 18954 mps  limit 110100  headroom 103749  charges 3120
```

Each headroom is that device's limit less **both** replicas: 98 304 − 3 231 −
3 120 = 91 953 and 110 100 − 3 120 − 3 231 = 103 749. The Metal row's
`external_mb` no longer carries the CPU replica's RSS (18 954 against run5's
28 437, which included it). On either device
`headroom + Σ charges ≤ memsize − external`: 91 953 + 6 351 = 98 304 ≤ 108 698,
and 103 749 + 6 351 = 110 100 ≤ 112 118.

No OOM, no failed load; `memory_pressure` 96 % free and `vm.swapusage` 0.00 M
after both legs.
