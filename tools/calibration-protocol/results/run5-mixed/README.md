# run5-mixed (Linux, CUDA + CPU) — verification of `fix/mixed-devices`

Tree: `verify/mixed-devices` = `fix/mixed-devices` (b3d933fc) merged with the
branch tip a99e8b54. Binary: that tree's release build. Host: 2x RTX PRO 6000
Blackwell (sm_120), 128 649 MiB RAM.

Configs: `tools/calibration-protocol/config/server-C8.toml` (+ `registry-C8/`,
which pins `textembed/all-MiniLM-L6-v2` to `devices = ["cpu"]`) and
`server-C9.toml` (the same host pointed at a `+cpu` venv, **no** pin and no
`accelerator = "cpu"`).

| leg | what | result |
|---|---|---|
| `cuda-cpu` | tagger on the GPU, embedder pinned to `cpu`, both driven at once | three `/health` rows, one per device, each priced on its own backend; no once-warning |
| `empty-mask` | the same with `CUDA_VISIBLE_DEVICES=` | one device (`CPU`), both models on it, both priced |
| `cpu-venv` | `+cpu` venv, CUDA host, no pin at all (run4-deploy D3) | the worker names `cpu` from torch alone and is admitted on the CPU device, `rss` base, fitted |

`cuda-cpu` final `/health` `vram[]`:

```
CPU       cpu   arch cpu    total 128649  ext 44485 ram         limit 83140  fp 359   cap 0.75
  textembed/all-MiniLM-L6-v2  fp 359  base 229 (rss)  unit_budget 4224  clean 14  fit slope 0.0315 MB/token (4 samples)
GPU-01c6  cuda  arch sm_120  total  97887  ext  1568 nvidia-smi  limit 96162  fp 0     cap none
GPU-942d  cuda  arch sm_120  total  97887  ext  1668 nvml        limit 96052  fp 2144  cap none
  tags/wd-vit-tagger-v3       fp 2144 base 964        unit_budget 64    clean  9  fit slope 29.87 MB/item (5 samples)
```

The CPU replica's RSS is charged to the CPU device only: the GPU's `external`
comes from NVML/VRAM and never sees host RAM, so on a discrete-GPU host the two
devices do not interact. (On a **unified** host they do — see
`../mps/run5-mixed/README.md`.)

`empty-mask` (`CUDA_VISIBLE_DEVICES=`): `gpus[]` is exactly `[CPU]`, both
models land there, `external_source = ram`, the embedder fits (slope
0.0391 MB/token, 7 samples). One WARN per pin ignored, by design.

`cpu-venv`: `base_method = "rss"`, `gpu_arch = "cpu"`, admitted on `CPU` with
the CUDA devices idle beside it, and **no** "names no device at all" WARN —
this is the case the old `UnadmittedCpuWorker` escalation existed for.
