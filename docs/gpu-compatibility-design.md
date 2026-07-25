# GPU compatibility and inference resource limits — design

What actually restricts Panoptikon to newer NVIDIA hardware, and how the
inference stack should behave when a GPU is old, small, or both. Findings
verified 2026-07-25 against the dev venv (`torch 2.7.1+cu128`, Windows,
RTX 5090). Plan only — nothing here is implemented.

Two separate problems are covered because they surface as the same user
experience ("I tried a model and it died"):

1. **Architecture support** — which GPUs can run our kernels at all.
2. **Resource limits** — whether a model that *can* run also *fits*.

## Findings

### The CUDA wheel is not the constraint

The installed `cu128` wheel reports:

```
torch 2.7.1+cu128, CUDA 12.8
['sm_50','sm_60','sm_61','sm_70','sm_75','sm_80','sm_86','sm_90','sm_100','sm_120']
```

Kernels reach down to Maxwell, and cubins are forward-compatible within a
major version (an `sm_60` cubin runs on an `sm_61` device). Pascal
(GTX 10xx, sm_61) and Turing (GTX 16xx, sm_75) are therefore both covered
by PyTorch itself. A "no kernel image is available for execution on the
device" failure on such a card does **not** indicate a wheel/arch gap and
should not be diagnosed as one.

No ONNX Runtime is involved anywhere; the only non-torch CUDA component is
CTranslate2 (faster-whisper), plus the `nvidia-cublas-cu12` /
`nvidia-cudnn-cu12` wheels it needs.

### What actually gates old cards: hardcoded dtypes and attention kernels

| Site | Requires | Pascal (6.1) | Turing (7.5) |
|---|---|---|---|
| `python/inferio/impl/dotsocr.py:52` — `torch_dtype=torch.bfloat16` | sm_80+ | fails | fails |
| `python/inferio/impl/florence2.py:73` — `flash_attention_2` (opt-in, default off) | sm_80+ | fails if enabled | fails if enabled |
| `python/inferio/impl/whisper.py:42` — CT2 `compute_type="float16"` | CC ≥ 7.0 | fails | ok |
| `python/inferio/impl/ocr.py:49` — `.cuda().half()` (doctr) | fp16 | ok, slow | ok |
| `wd_tagger` / `clip` / doctr defaults (fp32) | — | ok | ok |

None of these are properties of the models; they are choices made at load
time without consulting the device.

**bf16 below Ampere is not a hard refusal.** `torch.cuda.is_bf16_supported()`
returns immediately when `device_properties.major >= 8`; below that it falls
through to `_check_bf16_tensor_supported()`, which merely allocates a bf16
tensor. So bf16 on sm_75 "works" for allocation and many elementwise ops,
while the heavy GEMMs have no tensor-core path (cuBLAS bf16 wants sm_80) and
either take an emulated path or error per-op. Not a configuration to ship.

**Florence-2 is not a heavy VLM and already runs on old cards.** It selects
fp16 on CUDA (not bf16) and `flash_attention` defaults to `False`, so it runs
on sdpa. Florence-2-large is 0.77B → ~1.5 GB of fp16 weights, which fits a
6 GB card with room for activations. This matters because Florence-2 in its
OCR configuration is currently the best OCR we ship: the dedicated
(non-VLM) models are workable but flaky, and our doctr integration does not
reconstruct reading order — it collects detected text without ordering the
words, acceptable for search but not ideal. `dots_ocr` is the genuinely
Ampere-gated model (bf16 + FA2 required, as its own description states).

**CT2 float16 is requested unconditionally.** CTranslate2 supports float16
only at CC ≥ 7.0, and faster-whisper is known to raise ("...do not support
efficient float16 computation") rather than degrade when the type is named
explicitly. *Unverified on hardware* — worth confirming on a 10-series card,
as it is the best candidate for a "kernels not found"-shaped report from a
Pascal user.

### The accelerator decision is one-shot and invisible

- `decide_accelerator` (`panoptikon/src/setup.rs:531`) probes for driver
  *presence* only: `nvidia-smi` on PATH, `%SystemRoot%\System32\nvidia-smi.exe`
  on Windows, `/proc/driver/nvidia` on Linux. It never reads compute
  capability. `on_path` does append `.exe` on Windows.
- The decision is made once, when the venv is created. `maybe_auto_setup`
  (`panoptikon/src/setup.rs:1074`) only re-runs when the environment is
  missing, incomplete (no sentinel), or the `uv.lock` hash changed, and it
  passes `accelerator: None`. A CPU venv built before a driver existed is
  never revisited.
- Nothing surfaces the result. `panoptikon-desktop/src-tauri/src/server_config.rs`
  exposes port, search cache and inference performance knobs — no accelerator
  field — so recovery means hand-editing `config/server/desktop.toml` and
  deleting `runtime/venv`. The only record of the choice is the
  `accelerator selected` log line (`panoptikon/src/setup.rs:212`), which
  carries the evidence string.
- The AppImage environment boundary is not implicated: `host_env` rewrites
  nothing unless `APPDIR` is set, and the sidecar's `env_clear` is gated on
  that being `Some` (`panoptikon-desktop/src-tauri/src/supervisor.rs:173`).

### Forward risk

CUDA 13 drops compute capability below 7.5. When PyTorch moves its default
wheels to cu13x, Pascal loses kernel coverage for real and will need a
pinned legacy extra (the last cu12 torch); Turing survives. Nothing to do
today beyond not designing the accelerator matrix as if `cu128` were
permanent.

### Incidental defects found while reading

- `python/inferio/impl/md_tagger.py:92` and
  `python/inferio/impl/md_captioner.py:75` compare a `torch.device` to the
  string `"cuda"`. Verified always `False`, so the `accelerate` `device_map`
  path is dead code.
- doctr reading order (above): the geometry is present in the predictor
  output, so grouping boxes into lines by vertical overlap and sorting
  left-to-right is a small self-contained fix.

## Plan — inference runtime

1. **Negotiate dtype instead of hardcoding it.** A `select_dtype()` beside
   `get_device()` in `python/inferio/impl/utils.py` that reads
   `get_device_capability()` and steps bf16 → fp16 → fp32 down to what the
   device has, plus a CT2 compute-type mapper (CC < 7.0 → float32/int8).
   Turns three hard failures into degraded-but-working.
2. **Capability guard in `get_device()`.** Drop devices whose capability is
   absent from `torch.cuda.get_arch_list()`. A no-op today; it is the safety
   net for the CUDA 13 transition and the correct guard for the genuine
   "no kernel image" class.
3. **Per-model capability floor** in `inference.toml` metadata (e.g. `dots_ocr`
   and FA2 variants at 8.0), so unavailable models are shown as unavailable
   rather than failing mid-job.
4. **OOM as a batch-level recoverable event.** Catch
   `torch.cuda.OutOfMemoryError` at the batch boundary, `empty_cache()`,
   halve, retry; at batch size 1 fall back to CPU for that item or mark the
   model unusable on this device and report it. This is the highest-value
   change: it converts "try and crash" into "try and it runs slower".
5. **Self-calibrating batch size**, persisted per (inference_id, GPU name,
   dtype): start at the configured default, back off on OOM, ratchet up
   after N clean batches. Retires the flat `default_batch_size = 64` without
   anyone needing to know the user's hardware.
6. **Budget batches by decoded pixels, not item count.** The failure scales
   with total area in flight, which is why easyocr has `enable_batching =
   false` as a stopgap. An area budget (plus an item cap) lets batching come
   back on, and the budget is another number the back-off loop learns. Input
   framing already thinks in these terms (`slice_settings.mode = "pixels"`);
   this moves the idea to batch admission.
7. **Record real footprint**: `max_memory_allocated()` deltas around load and
   around the first batch, keyed by model + dtype + GPU. After one run the
   footprint on *that* machine is known — enough for a preflight check
   against free VRAM and for honest UI labelling.
8. **VRAM-aware residency**: evict before a load when a recorded footprint
   will not fit alongside residents, instead of discovering it by OOM.
9. **Per-model-family GPU self-test** (tiny input, each family and dtype),
   runnable on demand. This is what turns "we do not know if it works on
   card X" into data, and is the artifact to hand a user testing old
   hardware.

Rationale for the shape of 4–8: VRAM use depends on model, dtype, attention
kernel, input resolution and batch composition simultaneously, and the model
set grows over time. A predictive table cannot be maintained and would be
wrong on the next model added. The lever is not prediction — it is making a
failed attempt cheap and letting each install accumulate its own measurements.

## Plan — setup and detection

- Probe **compute capability**, not just driver presence
  (`nvidia-smi --query-gpu=compute_cap`). Enables warning about too-old cards
  and choosing a wheel variant if one is ever needed.
- **Re-probe at startup**: when the installed venv is CPU but NVIDIA evidence
  now exists, offer a rebuild instead of silently staying on CPU forever.
- Keep `cu128` as the CUDA extra (it covers sm_50+). Add a pinned legacy
  extra only when torch moves to cu13x.
- A pending community PR adds **ROCm support and NixOS fixes** and touches
  this same decision path. Review it before rewriting `decide_accelerator`
  and build on whatever shape it lands in. ROCm remains listed as
  aspirational/untested in the tech-debt register.

## Plan — Desktop inference tab (UX)

A dedicated config tab for the inference environment, replacing the current
"edit TOML and delete a folder" recovery path:

- Detected GPUs with name and compute capability; the installed wheel
  variant; venv state.
- Switch accelerator / reinstall the Python venv from the UI.
- Select which GPUs participate in inference (drives `CUDA_VISIBLE_DEVICES`).
- Per-model compatibility: **verified** (ran here, ≈X GB), **untested here**,
  **requires newer GPU**, **known too large** — populated by the self-test and
  the recorded footprints.
- Governing principle: an old or small GPU results in *fewer available models
  and slower runs*, never a job that dies with a CUDA error. A model that
  fails to load on GPU retries once on CPU and is recorded visibly.

## Open questions

- **fp16 numerics for bf16-trained models.** bf16 carries fp32's exponent
  range; fp16 does not, so a bf16 → fp16 downgrade can produce inf/NaN. Which
  models tolerate it is empirical and per-model. The numerically safe
  fallback (fp32) roughly doubles footprint — a 2B-class VLM lands near 8 GB
  before activations, so on a 6 GB card the safe fallback does not fit and
  the fitting fallback needs validation. This is the real interaction between
  dtype and VRAM.
- Confirm CT2's actual behaviour on CC < 7.0 (raise vs fall back).
- Whether emulated bf16 on sm_75 is ever worth offering (probably not; prefer
  fp16 with validation).
- Florence-2 is an old VLM; newer, more efficient small VLMs may beat it for
  OCR. Worth an evaluation pass, given how much of our OCR quality rests on it.
