# GPU compatibility and inference resource limits — design

What actually restricts Panoptikon to newer NVIDIA hardware, and how the
inference stack should behave when a GPU is old, small, or both. Findings
verified 2026-07-25 against the dev venv (`torch 2.7.1+cu128`, Windows,
RTX 5090).

> **Status 2026-07-29**: runtime items 1–4 are IMPLEMENTED (dtype
> negotiation via `select_dtype`/`select_ct2_compute_type` in
> `python/inferio/impl/utils.py`, `get_device` arch-list guard, per-model
> `min_compute_capability` floor with an nvidia-smi probe +
> `/api/inference/metadata` overlay in `panoptikon/src/inferio/capability.rs`
> + job fail-fast + UI badges, and batch-level OOM halving via
> `run_with_oom_retry` with the classified `INFERENCE_OOM_BATCH_SIZE_1:`
> batch-1 error). The incidental md_tagger/md_captioner dead branches were
> deleted (accelerate was never a declared dependency, so the branch was
> doubly dead). Items 5–8 now have a concrete design —
> `batch-calibration-design.md` (2026-07-30), which supersedes their
> sketches below (cost-model calibration instead of learned batch sizes,
> auto-with-cap UX, per-GPU-instance VRAM budgets) — but are not
> implemented. Item 9, the setup-side re-probe, and the Desktop
> inference tab remain unimplemented. Still unverified on hardware: CT2's
> behaviour at CC < 7.0 (the mapper now asks
> `ctranslate2.get_supported_compute_types` instead of guessing) and
> bf16→fp32 stepping on sm_75.

> **Revised 2026-07-25** after PR #19 (host tool discovery + ROCm 7.2,
> merged at `492f87c`) landed on several of these surfaces. ROCm is no
> longer aspirational, the setup sentinel is now readable ground truth for
> the installed accelerator, and a post-setup probe hook exists. The
> detection probes themselves are unchanged, so the findings about them
> still hold. Changed points are marked inline.

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

The arch list above is a property of the `cu128` extra (torch 2.7.1), which
`cpu` shares. Since PR #19 the `rocm` extra is a different torch generation
entirely — 2.11.0 from the `rocm7.2` index — so "our torch" is no longer one
version, and any future statement about kernel coverage has to name the extra
it applies to.

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
  only re-runs when the environment is missing, incomplete (no sentinel), or
  the `uv.lock` hash changed, and it passes `accelerator: None`. A CPU venv
  built before a driver existed is never revisited. **Unchanged by PR #19**:
  the early-return in `setup::run` for a converged venv is now conditional,
  but its `options.accelerator.is_none()` arm is exactly the auto-setup path,
  and deliberately so — auto-setup must never silently swap a torch build the
  user synced on purpose.
- **Changed by PR #19 — the installed variant is now knowable.**
  `installed_accelerator()` reads the sentinel's `extra=` line, and
  `panoptikon setup --accelerator X --if-needed` re-syncs when the installed
  extra differs from the requested one (plain `panoptikon setup` always runs;
  `--if-needed` is the new opt-in skip). Switching accelerators from the CLI
  therefore no longer requires deleting the venv by hand.
- Desktop still cannot do any of that. It has no CLI surface,
  `panoptikon-desktop/src-tauri/src/server_config.rs` still exposes no
  accelerator field, and the config-only change a user can make is exactly the
  case auto-setup ignores — so deleting `runtime/venv` remains the Desktop-side
  recovery. The only record of the choice is the `accelerator selected` log
  line, which carries the evidence string.
- **Changed by PR #19 — `desktop.toml` now ships a live env bridge**,
  `accelerator = "${PANOPTIKON_ACCELERATOR:-auto}"`, added for packagers
  (e.g. a ROCm-targeted build). Per the config-authoring rules its fallback
  freezes for anyone who has already seeded the file, and releases up to
  v0.1.7 seeded the *commented* form — so field installs have a commented
  `accelerator` line, not the template.
- **Changed by PR #19 — a post-setup probe hook exists.**
  `accelerator_env::probe_after_setup(accelerator, interpreter)` runs after a
  successful sync and is documented as the extension point for post-sync
  validation (a HIP kernel probe for ROCm today, no-op for cpu/cuda).
- The AppImage environment boundary is not implicated: `host_env` rewrites
  nothing unless `APPDIR` is set, and the sidecar's `env_clear` is gated on
  that being `Some` (`panoptikon-desktop/src-tauri/src/supervisor.rs:173`).

### Forward risk

CUDA 13 drops compute capability below 7.5. When PyTorch moves its default
wheels to cu13x, Pascal loses kernel coverage for real and will need a
pinned legacy extra (the last cu12 torch); Turing survives. Nothing to do
today beyond not designing the accelerator matrix as if `cu128` were
permanent — and PR #19 already set the precedent, since the single universal
lock now spans two torch generations behind a `constraint-dependencies` range
(`torch>=2.7.1,<=2.11.0`). A legacy CUDA extra would be the same shape.

### Incidental defects found while reading

(Both re-checked at `492f87c` and still present.)

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
   hardware. The hook now exists — PR #19 added
   `accelerator_env::probe_after_setup` as the post-sync validation
   dispatcher — so this extends that rather than introducing a new one.

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
- **Desktop needs the CLI's new capability.** `--accelerator X --if-needed`
  already re-syncs a mismatched venv; Desktop should invoke that path (or the
  same code) from the planned inference tab instead of leaving "delete
  `runtime/venv`" as the only route.
- Build the re-probe on the merged pieces: `installed_accelerator()` for what
  is installed, `resolve_accelerator`/`effective_accelerator` for what the
  hardware now suggests, and surface the mismatch rather than acting on it
  silently.

ROCm (merged in PR #19, `492f87c`) is no longer aspirational: `rocm` maps to
torch 2.11.0 from the `rocm7.2` index, HIP/HSA worker env is injected only
when the *installed* accelerator is `rocm` (sentinel-derived, so a host with
`/opt/rocm` cannot poison a deliberately-`cuda` venv), MIOpen is forced to
FAST find, and EasyOCR is pinned to a single `cuda:0` device under HIP. It is
untested in CI by design. None of that touched the detection probes or the
dtype sites below, so the rest of this document stands.

## Plan — Desktop inference tab (UX)

A dedicated config tab for the inference environment, replacing the current
"edit TOML and delete a folder" recovery path:

- Detected GPUs with name and compute capability; the installed wheel
  variant; venv state.
- Switch accelerator / reinstall the Python venv from the UI.
- Select which GPUs participate in inference (drives `CUDA_VISIBLE_DEVICES`).
  Note the `PANOPTIKON_ACCELERATOR` bridge added in PR #19 is a packager
  affordance, not a user-facing control — it is set before first run, not
  changed afterwards.
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
