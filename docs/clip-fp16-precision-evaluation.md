# CLIP FP16 precision evaluation

**Date:** 2026-07-27
**Model:** `ViT-H-14-378-quickgelu` / `dfn5b` ([apple/DFN5B-CLIP-ViT-H-14-378](https://huggingface.co/apple/DFN5B-CLIP-ViT-H-14-378))
**Outcome:** FP16 adopted as the default for all `openclip` inference IDs.

## Why this was investigated

The default CLIP model for both the `clip` and `tclip` groups was loading in
FP32 and occupying ~4 GB of VRAM. `ClipModel.load()` called
`open_clip.create_model_and_transforms()` without a `precision` argument, and
open_clip's default is `precision='fp32'`. Nothing in the registry TOML set
`init_args`, so every open_clip model in the product ran FP32.

The model is 986,714,881 parameters (632.7M vision tower + 354.0M text tower),
counted from the checkpoint's own pickle metadata. At FP32 that is exactly the
3.95 GB observed both on disk and in VRAM.

It is the CLIP setter in 7 of 8 non-trivial index DBs, so any change here
affects nearly the whole corpus.

## Headline results

| | FP32 | FP16 | delta |
|---|---|---|---|
| Weights resident | 3829 MiB | 1982 MiB | −48% |
| Peak VRAM @ batch 64 | 7592 MiB | 3868 MiB | −49% |
| Encode throughput | 18.7 img/s | 114.2 img/s | **6.1x** |
| Model load (cold process) | 8.4–8.6 s | 7.2–7.3 s | ~1.2 s faster |
| Relevance retained vs FP32 | — | 0.9993 | — |
| Worst landing rank of a displaced top-10 result | — | **14** | — |

FP16 is *faster to load*, not slower. open_clip builds the model, converts it
to FP16 **on CPU**, and only then loads the checkpoint into it
(`factory.py` `create_model` → `_set_model_device_and_precision` precedes
`load_checkpoint`). Since `device` defaults to `'cpu'` and `ClipModel.load()`
does its own `.to(device)` afterwards, the PCIe transfer moves half the bytes
(1.35 s → 0.50 s). Disk read is unchanged — the checkpoint is FP32 on disk
either way. Peak VRAM never reaches 4 GB at any point during loading.

The 6.1x compute speedup is because FP32 matmuls do not use tensor cores.

> Measured on an RTX 5090 (Blackwell). **The memory numbers transfer to any
> card; the speed numbers do not.** Ampere's FP32↔FP16 tensor-core gap is
> narrower, and Ampere can recover part of the FP32 deficit via TF32
> (`torch.backends.cuda.matmul.allow_tf32`, off by default in PyTorch). Re-run
> the harness before quoting 6.1x on a 3090.

## The gotcha that invalidates the "one-line config fix"

`precision="fp16"` converts **weights only**. open_clip leaves input casting to
the caller — the same contract as the original CLIP reference implementation
(`model.encode_image(image.half())`). Passing FP32 pixels into an FP16 model
dies at the patch-embed conv:

```
RuntimeError: Input type (torch.cuda.FloatTensor) and weight type
(torch.cuda.HalfTensor) should be the same
```

The text tower is unaffected — `cast_dtype` is threaded through it — so only
the image path needs the cast. **A registry-TOML change alone is not
sufficient and will crash at the first image batch.**

## Where the default belongs

Not in the group config. Both `[group.clip]` and `[group.tclip]` contain
`qwen3-vl-embedding-*` inference IDs that override `impl_class`; a group-level
`config.precision` would be inherited by them and passed to
`Qwen3VLEmbedding.__init__`, which does not accept it (`TypeError`).

It lives in `ClipModel.__init__` as `precision: str = "fp16"`, overridable
per-inference-ID via `config.precision` on any `openclip` entry.

Note: the built-in registry dir (`python/inferio/config/` in dev,
`runtime/pysrc/<VERSION>/inferio/config/` when bundled) is **versioned app
payload, re-extracted per release** — not user-seeded state. It is the
user-facing `config/inference/` dir that is seeded once and frozen. The
CLAUDE.md config-freezing rule applies to the latter, not the former.

## Evaluation methodology

Harness: 3000 real corpus images sampled across `default` (1200, anime art),
`screenshots` (810), `images` (600), `camera` (390); 54 text queries spanning
character/appearance, scene composition, UI screenshots, text-in-image,
photographic, style, and deliberately vague prompts.

Three design decisions made the numbers interpretable:

**1. Preprocess once, encode many.** Preprocessed tensors were cached to a
memmap so every arm consumed bit-identical inputs. Removes decode/NAS/
preprocessing variance as a confound, and makes re-analysis free.

**2. A null arm.** `fp32b` is FP32 at batch 48 instead of 64 — identical math,
different kernel selection and batch boundaries. This is the noise floor from
GPU nondeterminism that already exists on every re-index. No arm can be called
"different" unless it exceeds this.

**3. A noise ladder.** Isotropic noise calibrated to target cosines
(σ = √((1/c²−1)/d)) applied to the FP32 embeddings, at c = 0.9992 … 0.90.
This proves the metrics can actually *detect* degradation — without it,
"no difference found" is unfalsifiable.

FP32 is the **incumbent reference, not ground truth.** Everything measures
"does switching change what you get versus what you have today."

## Retrieval results

| arm | top-1 | ov@10 | ov@20 | ov@50 | rel@10 | nDCG@10 | mean cos |
|---|---|---|---|---|---|---|---|
| fp32b (null) | 100% | 98.5% | 99.5% | 99.0% | 1.0000 | 0.99996 | 0.999929 |
| **fp16** | 94% | 96.1% | 95.6% | 96.2% | 0.9994 | 0.99937 | 0.998874 |
| bf16 | 93% | 94.8% | 95.4% | 95.8% | 0.9992 | 0.99922 | 0.998936 |
| noise@0.9992 | 98% | 95.6% | 96.0% | 96.7% | 0.9996 | 0.99963 | 0.999201 |
| noise@0.998 | 93% | 93.0% | 94.6% | 95.5% | 0.9988 | 0.99878 | 0.998003 |
| noise@0.99 | 83% | 87.0% | 88.7% | 89.7% | 0.9944 | 0.99402 | 0.989999 |
| noise@0.95 | 54% | 71.3% | 72.8% | 76.0% | 0.9679 | 0.96902 | 0.950091 |
| noise@0.90 | 44% | 54.8% | 62.6% | 64.7% | 0.9307 | 0.93075 | 0.900047 |

`rel@k` = relevance retained: z-scored score mass of the arm's chosen top-k
divided by FP32's ideal top-k. Z-scoring per query is necessary because raw
CLIP cosines sit in a compressed band (~0.05–0.35), which would flatter any
arm. `fp32b` scoring exactly 1.0000 is partly by construction (FP32 defines
the ideal set).

**Changing only the batch size costs 1.5% of top-10.** Any headline "95%
overlap" figure for FP16 is therefore not a pure precision measurement.

## "Do relevant results fall out?" — the decisive test

For every FP32 top-k item missing from the arm's top-k, where it actually
landed in that arm's ranking:

| arm | k | n dropped | median rank | p95 | worst rank | beyond 100 |
|---|---|---|---|---|---|---|
| fp32b | 10 | 8 | 10 | 10 | 10 | 0 |
| **fp16** | 10 | 21 | 11 | 13 | **14** | **0** |
| fp16 | 20 | 47 | 21 | 24 | 26 | 0 |
| fp16 | 50 | 102 | 52 | 66 | 75 | 0 |
| bf16 | 10 | 28 | 10 | 15 | 15 | 0 |
| noise@0.99 | 10 | 70 | 12 | 17 | 19 | 0 |
| noise@0.95 | 10 | 155 | 15 | 44 | 83 | 0 |
| noise@0.90 | 10 | 244 | 21 | 82 | **247** | **8** |

Across 540 top-10 slots (54 queries × 10), **every item FP16 displaced from
the top 10 landed between rank 11 and rank 14.** None fell below 14. The
noise@0.90 row confirms the metric detects real damage when present.

## "Were the swaps between near-ties?"

Cost of each swap in z-units, against how far apart FP32 already spaces
*consecutive* results (z[rank i] − z[rank i+1]):

| arm | k=10 swap gap | adjacent-rank gap | ratio |
|---|---|---|---|
| fp32b | 0.0082 | 0.1030 | 0.08 |
| **fp16** | 0.0452 | 0.1030 | **0.44** |
| bf16 | 0.0460 | 0.1030 | 0.45 |
| noise@0.99 | 0.1376 | 0.1030 | 1.34 |
| noise@0.95 | 0.3581 | 0.1030 | 3.48 |
| noise@0.90 | 0.4975 | 0.1030 | 4.83 |

A FP16 swap costs **0.44 of one rank step** — the images trading places are
closer together than any two adjacent results in FP32's own ranking. At k=50
the ratio reaches 1.34 (one rank step), because adjacent-rank spacing shrinks
deeper into the ranking while the swap cost stays flat.

## Image → image similarity (the CLIP-similarity feature)

600 anchors:

| arm | nn@10 | nn@20 | nn@50 | identical NN | beyond 100 |
|---|---|---|---|---|---|
| fp32b | 98.2% | 98.6% | 98.7% | 97.5% | 0 |
| **fp16** | 93.4% | 94.2% | 95.5% | 92.0% | 0 |
| bf16 | 92.4% | 93.0% | 94.6% | 89.7% | 0 |
| noise@0.99 | 91.8% | 92.5% | 93.6% | 89.2% | 0 |
| noise@0.90 | 70.9% | 73.5% | 76.8% | 63.0% | 12 |

This is the **most sensitive surface** — both sides of the comparison are
perturbed instead of one. Still zero catastrophic drops.

## FP16 beats bf16

FP16 wins on nearly every ranking metric: top-1 94% vs 93%, ov@10 96.1% vs
94.8%, rel@10 0.9994 vs 0.9992, identical-NN 92.0% vs 89.7%. Expected — FP16
has 10 mantissa bits vs bf16's 7, and bf16's wider exponent range buys nothing
at inference (zero NaNs and no overflow in either arm; FP32 norms were exactly
1.0000, FP16 spanned [0.9994, 1.0006], bf16 [0.9956, 1.0047]).

## Caveats

- **FP16 error is structured, not isotropic.** At its mean cosine of 0.998874,
  an isotropic-noise arm predicts ~95.5% nn@10; FP16 delivers 93.4%. Rounding
  error is correlated across dimensions, so it disturbs rankings slightly more
  than random noise of matched magnitude. Do not reason about precision impact
  from cosine similarity alone.
- **Corpus scale.** 3000 images is 3.5% of the 85k-image `default` index. More
  images means more near-ties competing for top-10 slots, so overlap
  percentages would likely sag somewhat at full scale. The rank-11-to-14
  landing result is the scale-robust finding.
- **Only ViT-H-378 was measured for quality.** The FP16 default applies to
  every `openclip` inference ID. `ViT-B-32/openai` and
  `apple/MobileCLIP-B-LT-OpenCLIP` (the timm-backed branch of
  `_set_model_device_and_precision`) were smoke-tested through `ClipModel` —
  they load, take the FP16 path, and return finite unit-norm embeddings — but
  their retrieval quality under FP16 was not evaluated.
- **Non-CUDA devices force FP32.** FP16 on CPU is slow and patchily supported.

## Migration impact

Existing indexed embeddings were produced in FP32. Newly embedded images will
be FP16. They coexist: mean cosine agreement is 0.9989, and a text query
embedded in FP16 searched against the existing FP32 image index gave **100%
top-10 overlap** with the all-FP32 baseline on every query tested. No
re-indexing is required.

## Reproducing

The harness was a one-time scratchpad artifact (`precbench/`: `prep.py` →
`encode.py <arm>` → `analyze.py`), not checked in. Structure if it is ever
needed again: sample paths from index DBs → preprocess once into a memmap →
encode one arm per process (fresh CUDA context) → analyze from saved
embeddings. Keeping encode and analysis separate matters: the metrics can be
redesigned without re-running the GPU work.
