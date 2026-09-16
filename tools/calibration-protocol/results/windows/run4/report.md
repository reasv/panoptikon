# Windows run 4 — per-implementation memory measurement, the shipped-baseline seeding scenario, and the regression pass

Host `A-DESKTOP`, Windows 11 (10.0.26200), 2 × NVIDIA GeForce RTX 5090 (sm_120, 32 607 MiB each),
driver 610.74, torch 2.7.1+cu128, rustc 1.97.1. Tip **0d7f5671** (`claude/batch-calibration-coverage-db9ab9`
with `origin/master` merged in on 2026-09-10), clone `Q:\projects\panoptikon-pr27`, ports 6382/6383/6379.
The user's own instance on `Q:\projects\panoptikon` (6339/6342/6343) ran throughout and was not touched.
2026-09-16, 17:30–21:00 UTC.

## 0. Build, venv, selftest

* `cargo build --release -p panoptikon` on the merged tip: **succeeded, 1 m 24 s**, one pre-existing
  `dead_code` warning on `NofileRaise`. **No D0** — §4.22's Windows build defect (`08825db3`, fixed by
  `637405b9`) stays fixed across the master merge.
* `uv sync --locked --extra cu128 --group test --directory python`: 8 packages moved, rc 0.
* `python -m pytest tools/calibration-protocol/tests -q`: **165 passed, 2 skipped**.
* `selftest.py` (`selftest.log`, `win-selftest.json`): unchanged from pass 2 and as the Per-OS table
  predicts —

  ```
  free_source  nvml     27137 / 32607 MiB      (torch tier reads 30577 / 32606: over-reports by the desktop)
  base_mb      840      base_method free_delta (nvml tier: "NVML lists no process with this pid")
  VERDICT: degraded - base:free_delta; oracle:no NVML per-process figure
  ```

  So `oracle_agreement` / `base_accuracy` / `footprint_agreement` SKIP by design on every leg below.

## 1. Per-implementation measurement (the main task)

Method, exactly §4.24's: one gateway per id, **fresh store**, `lru_size = 1`, driven by `loadgen.py`
against `POST /api/inference/predict` — except `clap`, whose `audio_tracks` handler only runs on the
job path, which used `legs.py --scenario S14 --scan-audio` over a freshly generated `audio` tier.
Each leg posts from **four concurrent slots with different `items=`** (e.g. 3 / 11 / 37 / 101) so the
coalesced windows land on many distinct unit counts: the ledger's fit ring holds one sample per
**distinct** `units` value (`ledger.rs:241`), so a single request size caps `samples` at the number of
ramp rungs. Corpora: `ramp` (2 000 × 1024², 1 MiB-pixel items) for the image ids, a freshly generated
`text` tier for the token-priced ids, `audio` for clap.

| id | impl | unit | Win slope | Lin sm_120 slope | ratio | Win base / method | Lin base | Win resid | Lin resid | Win samples | Win max_units | Lin max_units | leg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `clap/larger_clap_general` | `clap` | item | 34.5 | 34.5 | **1.00000** | 1208 / free_delta | 1326 | 0 | 0.25 | 8 | 32 | 28 | B4-clap-job |
| `clip/ViT-B-32_openai` | `openclip` | item | 1.168674699 | 1.171208818 | **0.99784** | 758 / free_delta | 868 | 0.361446 | 0.171209 | 30 | 127 | 256 | B4-clipA |
| `clip/nemotron-embed-vl-1b-v2` | `nemotron-embed-vl` | pixel | 0.0001587867737 | 0.00015890202 | **0.99927** | 3668 / free_delta | 3788 | 0.5 | 0.210692 | 7 | 8388608 | 67108864 | B4-nemotron |
| `clip/qwen3-vl-embedding-2b` | `qwen3-vl-embedding` | pixel | 0.0002603530884 | 0.0002600628397 | **1.00112** | 8661 / free_delta | 8780 | 1 | 0.869565 | 7 | 8388608 | 61865984 | B4-qwen2b |
| `doctr/db_resnet50_crnn_vgg16_bn` | `doctr` | item | 8.264935065 | 8 | **1.03312** | 737 / free_delta | 858 | 5.41039 | 0 | 9 | 64 | 128 | B4-doctr |
| `doctr/db_resnet50_crnn_vgg16_bn` | `doctr` | item | 8 | 8 | **1.00000** | 738 / free_delta | 858 | 2 | 0 | 37 | 128 | 128 | B4-doctr-r2 |
| `tclip/ViT-B-32_openai` | `openclip` | item | 0.9090909091 | 0.9107142857 | **0.99822** | 749 / free_delta | 868 | 0.454545 | 0.571429 | 51 | 255 | 192 | B4-tclipA |
| `tclip/qwen3-vl-embedding-2b` | `qwen3-vl-embedding` | token | 0.0968762505 | 0.08515642264 | **1.13763** | 8661 / free_delta | 8780 | 86.8063 | 48.4531 | 64 | 31806 | 63612 | B4-tqwen2b |
| `textembed/all-MiniLM-L6-v2` | `sentence_transformers` | token | 0.0188208399 | 0.01630226314 | **1.15449** | 520 / free_delta | 654 | 1.49942 | 5.18872 | 64 | 16128 | 49152 | B4-textembed-minilm |
| `textembed/all-mpnet-base-v2` | `sentence_transformers` | token | 0.07614533413 | 0.05106713517 | **1.49108** | 902 / free_delta | 1022 | 29.8621 | 25.5062 | 64 | 16128 | 49536 | B4-textembed-mpnet |
| `textembed/stella_en_400M_v5` | `sentence_transformers` | token | 0.09823885281 | not measured | - | 2117 / free_delta | - | 41.5369 | - | 64 | 12900 | - | B4-textembed-stella |

Per-id evidence: `baselines/<leg>/calibration.after.toml`, with the leg's `loadgen.log`, `leg.log` and
the `/api/inference/health` snapshots beside it. `B4-doctr` and `B4-doctr-r2` are the same id twice —
see the docTR note below.

### Verdict per implementation family

| Impl | ids measured here | same behaviour on Windows? | evidence |
|---|---|---|---|
| `openclip` | `clip/ViT-B-32_openai`, `tclip/ViT-B-32_openai` | **yes** | 0.99784 and 0.99822 of the Linux sm_120 slope, 30 and 51 fit samples, residuals 0.36 / 0.45 MiB. Adds a second size class to pass 2's `clip/apple_MobileCLIP-S1` (8.360 vs 8.371, 0.99864) |
| `doctr` | `doctr/db_resnet50_crnn_vgg16_bn` | **yes** | `B4-doctr-r2`: **8.0 exactly**, 37 samples, max_units 128 — the same 8.0 Linux measured and the same 8.0 pass 2 measured for `crnn_mobilenet_v3_small` |
| `clap` | `clap/larger_clap_general` | **yes** | **34.5 exactly, residual 0.0**, 8 samples over units 1–32; Linux 34.5 residual 0.25. Bit-identical slope |
| `qwen3-vl-embedding` (pixel side) | `clip/qwen3-vl-embedding-2b` | **yes** | 1.00112, residual 1.0 MiB on a 0.00026 MiB/pixel slope |
| `nemotron-embed-vl` | `clip/nemotron-embed-vl-1b-v2` | **yes** | 0.99927, residual 0.5 MiB |
| `qwen3-vl-embedding` (token side) | `tclip/qwen3-vl-embedding-2b` | **no** | **1.13763** (+13.8 %), residual 86.8 MiB against Linux's 48.5. Same weights, same impl, same leg design as the pixel side that matched — the unit, not the kernel, is what fails to travel |
| `sentence_transformers` | `textembed/all-MiniLM-L6-v2`, `all-mpnet-base-v2`, `stella_en_400M_v5` | **no** | **1.15449** and **1.49108**; `stella_en_400M_v5` has no Linux row at all to compare against (it was fixed in §4.22 after the sm_120 sweep, so it is *owed a Linux leg*, not measurable as equal here) |

**The split is by unit, not by implementation.** Every `item`- and `pixel`-priced id measured here is
within **0.3 %** of its Linux sm_120 row (five ids: 1.00000, 1.00000, 0.99784, 0.99822, 0.99927,
1.00112). Every `token`-priced id — `aggregation = "max-times-count"` — is **13–49 % high**, and that
includes `tclip/qwen3-vl-embedding-2b`, whose own pixel-side twin `clip/qwen3-vl-embedding-2b` matched
to 0.1 % in the leg that ran twenty minutes earlier on the same weights and the same impl class. A
token slope is a slope against `max(token_len) × count`, so it prices the batch's **padding**, and the
padding a batch carries depends on which requests coalesced — which on Windows is decided against a
`free_delta` base and a device-wide free reading that the desktop compositor moves. §4.23 and §4.24
already recorded token as the one unit that did not travel between sm_86 and sm_120 (mpnet 0.906,
MiniLM 1.021); this pass shows it does not travel across the OS either, on the same architecture.

**docTR, and why the same id appears twice.** `B4-doctr` (300 s, request sizes 3/11/37/101) fitted
**8.2649** with 9 samples and `max_units_measured` 64; `B4-doctr-r2` (600 s, sizes 5/19/61/151) fitted
**8.0 exactly** with 37 samples and `max_units_measured` 128. The difference is the lever arm: the fit
is Theil–Sen over (units, delta) pairs whose delta carries a ~390 MiB activation intercept, so a ring
that never reaches past 64 units divides that intercept's noise by a short baseline. The row to read is
`B4-doctr-r2`. The practical rule this gives for a Windows baseline leg: **the ring must reach at least
the `max_units_measured` the Linux row reached**, or the slope is not comparable. Three of the rows
above did not reach it (`clip/ViT-B-32_openai` 127 vs 256, both pixel ids 8 388 608 vs ~6.4e7, the
token ids ~1/3) — for the item and pixel ids the ratios came out inside 0.3 % anyway; for the token
ids, this is a second reason not to ship them, not an explanation that rescues them.

### Two more ids, measured as a by-product of §2

The fourteen seeding legs of §2 ran on **GPU 1** (no desktop on it) and each wrote its own store, so they
are fourteen more Windows fits of two ids, on the same tip:

| id | impl | Windows slopes measured (6 legs each) | Linux sm_120 | ratio range |
|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | `wd_tagger` | 29.857584, 29.858268, 29.859375, 29.860941, 29.861386, 29.862745 | 29.865328820116055 | **0.99974 – 0.99991** |
| `clip/ViT-H-14-quickgelu_dfn5b` | `openclip` | 10.327434, 10.328859, 10.331845 (×3), 10.333333 (×3), 10.336283 | 10.331845238095237 | **0.99957 – 1.00043** |

`tags` is re-confirmed on the merged tip, tighter than pass 2's single 29.9 (1.00116). `ViT-H-14` is a
**sixth `openclip` id** and the largest size class measured on Windows so far (base 2 469 MiB here against
the 97 GB card's 2 588 — the `free_delta` offset again, not a slope difference).

### Recommended `platform_copies` list

Ids whose **implementation family was measured equal on Windows** in this pass or pass 2, and so may
carry `metadata.cost.platform_copies` for the Windows key:

* `clap` — all four ids (`clap-htsat-unfused`, `larger_clap_general`, `larger_clap_music`,
  `larger_clap_music_and_speech`): slope measured bit-identical.
* `doctr` — the seven `db_resnet50_*` ids: 8.0 on both platforms, two ids measured (pass 2's
  `crnn_mobilenet_v3_small`, this pass's `crnn_vgg16_bn`). `dots_ocr` stays excluded (§4.23).
* `openclip` — the `clip/*` and `tclip/*` open_clip ids **whose unit is `item` or `pixel`**: three ids
  measured across three size classes (MobileCLIP-S1, ViT-B-32 image and text side).
* `qwen3-vl-embedding` and `nemotron-embed-vl` — **only the `clip/` (pixel) ids**.
* `tags` — the five `wd-*` ids, unchanged from §4.23's proposal (wd-vit re-confirmed below in §3).
* `florence2` — unchanged: pass 2 measured 382.571 vs 382.569, but §4.23's objection stands, the key
  cannot say "only while `config.flash_attention` is false".

**Do not copy:** every `token`-priced id — the two `tclip/qwen3-vl-embedding-*` ids, the
`tclip/PE-Core-*` token rows, and all of `textembed/*`. `textembed/stella_en_400M_v5` additionally has
no Linux measurement to copy from.

## 2. The shipped baseline on a consumer card — a big-card anchor against 32 GB

The shipped `python/inferio/config/calibration/sm_120-linux-cuda.toml` was measured on a **97 887 MiB**
RTX PRO 6000 of the same architecture as these 5090s, so its `base_mb`, `knee_units` and
`max_units_measured` are a big card's numbers. `platform` is part of the profile key, so on Windows only
ids with `metadata.cost.platform_copies` match — today that is `tags/wd-vit-tagger-v3` alone.

**What was rewritten.** `baselines/seed-sm120-win.toml` (committed) holds exactly three profiles lifted
verbatim out of the shipped file with **`platform = "linux"` → `platform = "windows"` and nothing else
changed**: `tags/wd-vit-tagger-v3` (base 964, slope 29.865328820116055, anchor 192),
`clip/ViT-H-14-quickgelu_dfn5b` (base 2588, slope 10.331845238095237, **knee 63**, anchor 192) and
`tclip/qwen3-vl-embedding-8b` (base **31752**, anchor 31806 — carried for completeness; no leg below
drives a `tclip` id). It is fed with `legs.py --seed-calibration`, i.e. as a **local** store. The
2026-09-07 ruling — any matching profile confers its anchor, local or shipped — is directly visible in
the load line, because `tags/wd-vit-tagger-v3`'s six legs exercise **both** routes with the same numbers:

```
A-wdvit-S2-fresh   seeded calibration from a stored profile  local=false fit_is_local=false confirms=false
                   slope_mb_per_unit=29.865328820116055 samples=5 max_units_measured=192   <- the SHIPPED row
A-wdvit-S2-seed    seeded calibration from a stored profile  local=true  fit_is_local=true  confirms=true
                   slope_mb_per_unit=29.865328820116055 samples=5 local_samples=0 max_units_measured=192
```

Same slope, same anchor 192, and — see the table — the same budget (512 / 256 / 165) and the same
items/s on both routes at every memory level. The local seed is a faithful stand-in for the shipped one.

All 14 legs ran on **GPU 1** (`CUDA_VISIBLE_DEVICES=GPU-c77d0f94-…`, config `server-C1win-g1.toml` +
`env.C1win-g1`), so the desktop's ~4.4 GB on GPU 0 does not enter. Hog: `hog.py --target gpu`
`leave-free`, forced with `legs.py --min-free-mb 12288` / `8192` — S4a's own fraction scales to 4 093 MiB
on this board, so the floor binds and the leg records `floor_bound {"resolved_mb": 12288 | 8192}`.
Measured holds: 18 944 MiB held / 12 343 free, and 22 912 held / 8 226 free.

| leg | free on the card | seeded? | conferred anchor / knee | 1st window (units, mb) | largest window (units, mb) vs share_mb / headroom_mb | deflation | OOM | collapse | over_grant | items | items/s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `A-wdvit-S2-fresh` | ~30 GB | shipped row (`platform_copies`) | 192 / – | 2, 75 MiB | **512**, 22 487 vs 29 971 / 18 717 | 0 | 0 | 0 | 0 | 2000/2000 | 55.19 |
| `A-wdvit-S2-seed` | ~30 GB | shipped + local seed | 192 / – | 1, 38 MiB | **512**, 22 405 vs 29 972 / 18 758 | 0 | 0 | 0 | 0 | 2000/2000 | 55.20 |
| `A-wdvit-S4a12-fresh` | 12 343 | shipped row | 192 / – | 2, 75 MiB | **256**, 9 550 vs 10 260 / 5 476 | 0 | 0 | 0 | 0 | 2000/2000 | 55.73 |
| `A-wdvit-S4a12-seed` | 12 343 | shipped + local seed | 192 / – | 1, 38 MiB | **256**, 9 468 vs 10 297 / 5 553 | 0 | 0 | 0 | 0 | 2000/2000 | 55.74 |
| `A-wdvit-S4a8-fresh` | 8 226 | shipped row | 192 / – | 1, 38 MiB | **165**, 6 103 vs 6 136 / 1 392 | 0 | 0 | 0 | 0 | 2000/2000 | 54.12 |
| `A-wdvit-S4a8-seed` | 8 226 | shipped + local seed | 192 / – | 3, 112 MiB | **165**, 6 228 vs 6 233 / 1 389 | 0 | 0 | 0 | 0 | 2000/2000 | 54.28 |
| `A-vith14-S2-fresh` | ~30 GB | **no** | – | 1, 29 004 MiB (pre-fit cap) | 32, 551 vs 29 001 / 28 713 | 0 | 0 | 0 | 0 | 2000/2000 | 108.61 |
| `A-vith14-S2-seed` | ~30 GB | local seed | 192 / **63** | 1, **13 MiB** | **127**, 1 755 vs 29 003 / 28 119 | 0 | 0 | 0 | 0 | 2000/2000 | 111.04 |
| `A-vith14-S4a12-fresh` | 12 343 | **no** | – | 1, 8 844 MiB (pre-fit cap) | 256, 4 791 vs 8 718 / 6 306 | 0 | 0 | 0 | 0 | 2000/2000 | 110.49 |
| `A-vith14-S4a12-seed` | 12 343 | local seed | 192 / 63 | 1, 13 MiB | **127**, 1 755 vs 8 720 / 7 836 | 0 | 0 | 0 | 0 | 2000/2000 | 111.16 |
| `A-vith14-S4a8-fresh` | 8 226 | **no** | – | 1, 4 748 MiB (pre-fit cap) | 247, 4 622 vs 4 622 / 2 210 | 0 | 0 | 0 | 0 | 2000/2000 | 110.21 |
| `A-vith14-S4a8-seed` | 8 226 | local seed | 192 / 63 | 1, 13 MiB | **127**, 1 755 vs 4 624 / 3 740 | 0 | 0 | 0 | 0 | 2000/2000 | 110.91 |
| `A-qwen8b-S2-fresh` | ~30 GB | **no** (no shipped row for this id) | – | **2 000 000 px, 0 MiB** | 2 000 000, 0 vs **0 / 0** | **22** | **5 018** | 0 | 0 | **0 done, 1 124 failed, 876 never reached** | 3.13 (all failures) |
| `A-qwen8b-S4a12-fresh` | 12 343 | **no** | – | **2 000 000 px, 0 MiB** | 2 000 000, 0 vs **0 / 0** | **22** | **6 254** | 0 | 0 | **0 done, 1 436 failed, 564 never reached** | 3.11 (all failures) |

### What this says about the claim under test

**A big-card anchor never set the budget on a small card — confirmed, on both routes.**

* `tags/wd-vit-tagger-v3` is seeded on all six legs — from the **shipped** baseline on the three
  "fresh" ones (it is the one allowlisted `platform_copies` id, so a fresh store is not an unseeded
  model) and from the **local** seed on the three "seed" ones. The conferred anchor is **192** in all
  six, and the two routes are indistinguishable in the result. The budget the card actually ran was
  **512 / 256 / 165** as free memory went 30 GB → 12 GB → 8 GB. The anchor was **exceeded** where memory
  allowed and **undercut** where it did not: it is a ratchet claim, not a budget. The MB charged tracks
  `share_mb` (22 487/29 971, 9 550/10 260, 6 103/6 136), i.e. the budget is re-derived from
  `share.mb / slope` every window.
* `clip/ViT-H-14-quickgelu_dfn5b` is the clean contrast, because no shipped Windows row exists for it.
  Seeded, the **first** window is priced at **13 MiB for 1 unit** — from the conferred slope — and the
  second opens at **63**, the conferred knee, reaching 127; unseeded, the first window is priced at the
  whole share (29 004 / 8 844 / 4 748 MiB for one unit, the documented pre-fit "cap, not a priced need")
  and the ramp crawls, taking 45 windows to reach 32 at native. **The seeded budget is identical (127,
  1 755 MiB) at 30 GB, 12 GB and 8 GB free** — it is the conferred *knee* that fixes it, not memory, and
  1 755 MiB fits inside the 4 624 MiB share at the tightest level with 3 740 MiB of headroom left.
* **Zero OOM, zero throughput collapse, zero `over_grant`, zero deflation and 2 000/2 000 items on all
  twelve legs that had a model which fits**, seeded or not, at every memory level.
* Cost of seeding: none, and a small gain. items/s seeded vs unseeded — wd-vit 55.20/55.19,
  55.74/55.73, 54.28/54.12; ViT-H-14 **111.04/108.61**, 111.16/110.49, 110.91/110.21.

### Finding W-A1 (new, blocking for large models on consumer cards) — a model that does not fit produces an OOM storm, not a refusal

`clip/qwen3-vl-embedding-8b` (~30.7 GiB of weights) on a 31.84 GiB visible board:

```
ledger: admitted a worker to a GPU's ledger model=clip/qwen3-vl-embedding-8b
        base_mb=Some(31595) base_method="alloc_delta" reserved_at_load_mb=Some(31202)
        seeded_from_store=false
ledger: issued a memory grant unit_budget=2000000 mb=0 canvas_pixels=1843200
        share_mb=0 room_mb=0 headroom_mb=0 pre_fit=true ramp_step=0 deflation=0 squeezed=true
worker: torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 134.00 MiB.
        GPU 0 has a total capacity of 31.84 GiB of which 0 bytes is free.
dispatch: merged batch of 2 requests failed, falling back to per-request prediction
```

The load is admitted with **no headroom at all** (`share_mb=0 room_mb=0 headroom_mb=0`), and the
**pre-fit branch then admits the registry seed's whole ramp step** — `seed_units = 2 000 000` pixels —
because pre-fit there is no slope to convert MB into units, so "the ramp value *is* the unit budget"
(`ledger.rs:4300-4316`), and the MB side is honestly charged 0. Deflation walks 0 → **22**
(2 000 000 → 1 unit) and every request still fails, because the weights alone leave nothing: the model
cannot run **one** image. The job then grinds to the 480 s cap — **1 124 and 1 436 failed items, 0
completed, 5 018 and 6 254 `torch.OutOfMemoryError` lines, no store row written** (`no_calibration_toml`),
no cooldown, no worker eviction, no refusal, and the job's own `outcome` stays `running` while `errors`
climbs. This is the opposite of the expected clean refusal.

Two sub-observations, both Windows-specific:

* Under the 12 GB hog the numbers do not reconcile on device memory alone: the hog held **18 944 MiB**
  (leaving 12 343 free) and the worker still reported `base_mb = 31595` with `reserved_at_load_mb =
  31202`, against a 32 607 MiB board whose recorded peak `used_mb` was **32 525**. 18 944 + 31 595 =
  50 539 MiB, so roughly 18 GB of that "allocation" was served out of host memory by the WDDM sysmem
  fallback. The driver's "Prefer No Sysmem Fallback" setting did not cover these processes.
* `base_method` came back **`alloc_delta`**, not this platform's usual `free_delta` — the free-delta tier
  cannot answer when the load consumed everything the free reading had.

## 3. Regression pass on the merged tip

Eighteen legs with `legs.py`, `--config tools/calibration-protocol/config/server-C1win.toml`
(both GPUs visible, GPU 0 with the desktop on it — pass 2's configuration), `--gpu-total-mb 32607`.
Each leg was scored with **its own `analyze_command`**, the one `legs.py` writes into `legs.json`, and
the matching pass-2 leg under `results/windows/*-final` was **re-scored with exactly the same flags on
the same (tip) `analyze.py`**, because `analyze.py` itself moved four commits between `24820452` and
this tip (`23e2bfb9`, `19435ead`, `9fec4640`, `c8daefba`). Comparing against the *stored* pass-2
`analyze.json` would have compared two analyzers, not two runs. Bold cells are the moves.

| leg | oracle agreement | base accuracy | footprint agreement | slope accuracy | grant safety | failures | deflation recovery | idle liveness | utilization | throughput | persistence | job outcome | ledger invariant | peak fds | hog tracking | ramp progress | calibration learned | alloc retries |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `r4-S1` | SKIP | SKIP | **SKIP→INFO** | - | PASS | PASS | - | - | - | - | - | PASS | **WARN→PASS** | INFO | - | - | - | - |
| `r4-S2` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | **PASS→INFO** | PASS | PASS | INFO | SKIP | INFO | PASS | **SKIP→PASS** |
| `r4-S3` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | PASS | PASS | PASS | INFO | SKIP | INFO | INFO | **SKIP→PASS** |
| `r4-S4a` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | **PASS→INFO** | PASS | WARN | INFO | INFO | INFO | INFO | **SKIP→PASS** |
| `r4-S4b` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | **PASS→INFO** | PASS | PASS | INFO | INFO | INFO | INFO | **SKIP→PASS** |
| `r4-S4c` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | **PASS→INFO** | PASS | PASS | INFO | INFO | INFO | INFO | **SKIP→PASS** |
| `r4-S4d` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | **PASS→INFO** | PASS | WARN | INFO | INFO | INFO | INFO | **SKIP→PASS** |
| `r4-S5-dies_on_load` | SKIP | SKIP | SKIP | WARN | SKIP | PASS | SKIP | PASS | WARN | INFO | WARN | FAIL | PASS | INFO | SKIP | SKIP | INFO | SKIP |
| `r4-S5-dying` | SKIP | SKIP | INFO | WARN | PASS | FAIL | PASS | PASS | SKIP | INFO | WARN | FAIL | PASS | INFO | SKIP | INFO | INFO | SKIP |
| `r4-S5-failbatch` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | PASS | PASS | WARN | INFO | SKIP | INFO | INFO | **SKIP→PASS** |
| `r4-S5-failbatch_oomtext` | SKIP | SKIP | INFO | SKIP | PASS | PASS | PASS | PASS | SKIP | INFO | PASS | PASS | WARN | INFO | SKIP | INFO | INFO | **SKIP→PASS** |
| `r4-S5-oom_second_batch` | SKIP | SKIP | **SKIP→INFO** | SKIP | PASS | WARN | **PASS→SKIP** | PASS | **SKIP→WARN** | INFO | **PASS→FAIL** | PASS | PASS | INFO | SKIP | **INFO→SKIP** | INFO | **SKIP→PASS** |
| `r4-S14-tags` | - | - | - | - | PASS | PASS | - | - | - | - | - | PASS | - | INFO | - | - | - | - |
| `r4-S14-clip` | - | - | - | - | PASS | PASS | - | - | - | - | - | PASS | - | INFO | - | - | - | - |
| `r4-S14-ocr` | - | - | - | - | PASS | PASS | - | - | - | - | - | PASS | - | INFO | - | - | - | - |
| `r4-S14-whisper` | - | - | - | - | SKIP | PASS | - | - | - | - | - | PASS | - | INFO | - | - | - | - |
| `r4-S14-florence2` | - | - | - | - | PASS | PASS | - | - | - | - | - | PASS | - | INFO | - | - | - | - |
| `r4-S14-textembed` | - | - | - | - | **SKIP→PASS** | PASS | - | - | - | - | - | PASS | - | INFO | - | - | - | - |


**No new FAIL.** The three FAIL cells (`r4-S5-dies_on_load` / `r4-S5-dying` `job_outcome`,
`r4-S5-dying` `failures`) are identical on both sides: they are the fault-injection fixtures scored with
`legs.py`'s own default flags, which do not pass `--expect-deaths` / `--expect-failed-items`, so a
fixture that is *supposed* to kill its worker reads as a failure. Same verdict in pass 2, same detail.

Moves, all of them explained and none of them a product regression:

| move | legs | why |
|---|---|---|
| `alloc_retries` SKIP → **PASS** | 9 legs | new instrument on this tip (`19435ead`, allocator retries per batch/window and in `/health`); pass-2 recordings predate the field. Values: 0 retries everywhere except S4c (1 of 10 windows) and S4d (2 of 124) |
| `ledger_invariant` WARN → **PASS** | `r4-S1` | 0 of 214 GPU-samples over limit, 0 `over_grant`, 0 `limit_fell`, where pass 2 had a `limit_fell` WARN |
| `grant_safety` SKIP → **PASS** | `r4-S14-textembed` | **T7 closes** — see below |
| `footprint_agreement` SKIP → INFO | `r4-S1`, `r4-S5-oom_second_batch` | report-only check; it now has joined samples to report on |
| `persistence` PASS → INFO | S2, S4a–S4d | `1 profile(s); no anchor_advanced update was queued during the recording`. On this tip `tags/wd-vit-tagger-v3` has a **shipped Windows row** (`platform_copies`) conferring anchor 192, so these legs open above where pass 2's empty store started and never advance an anchor *inside* the recording window. Nothing is unpersisted: each leg's `calibration.after.toml` carries its fit |
| `deflation_recovery` PASS → SKIP, `ramp_progress` INFO → SKIP, `utilization` SKIP → WARN, `persistence` PASS → **FAIL** | `r4-S5-oom_second_batch` only | `no workers in any health sample` — this fixture's job finished in **0.083 s** of inference (the same 0.087 s as pass 2) and the 0.5 s health poller caught no sample with a worker in it. The `persistence` FAIL is `worst anchor-advance → store-write delay **30.0 s [threshold 30 s]**`, i.e. exactly on the boundary, on a leg whose store write is the 30 s debounce firing after a job that lasted a tenth of a second. A recording artefact of a degenerate leg, not a behaviour change — the job itself is `completed`, 180/180, 0 errors, on both sides |

### T7 closes: `textembed` reached through the OCR chain on Windows

Pass 1 and pass 2 could only load `textembed` through `PUT /api/inference/load`; the extraction route
found `no items to process` (T7). The tip's `S14-textembed` scenario over a freshly generated `text`
tier runs the chain end to end on Windows for the first time:

```
job_start {"model": "doctr/db_resnet50_crnn_mobilenet_v3_small"}   -> job_end drained, 300/300, 0 errors
job_start {"model": "textembed/all-MiniLM-L6-v2", "tag": "-2"}     -> job_end drained
grant_safety PASS: 16 grants; 0 memory-blind
calibration.after.toml: textembed/all-MiniLM-L6-v2  token  0.014707999317444202  17 samples  max_units 12444
                        doctr/db_resnet50_crnn_mobilenet_v3_small  item  8.023863636363636  8 samples
```

Two things fall out of it. `doctr/db_resnet50_crnn_mobilenet_v3_small` fits **8.0239** here — a third
independent Windows confirmation of docTR's 8.0 (1.00298). And `textembed/all-MiniLM-L6-v2` fits
**0.014708** through the OCR chain against **0.018821** through the loadgen text route in §1 — the same
id, the same host, the same hour, **1.28× apart**, and straddling the Linux 0.016302 from either side
(0.902 and 1.154). A token slope is not even stable *within* one platform; that is the strongest single
argument against shipping a token-priced row as a `platform_copies` copy.

### Throughput and the ramp, against pass 2

| leg | run 4 items/s | pass 2 items/s | run 4 slope / anchor / knee | pass 2 slope / anchor / knee |
|---|---|---|---|---|
| S2 | **48.780** | 40.000 | 29.8594 / 512 / – | 29.9 / 64 / 3 |
| S3 | **48.780** | 39.216 | 29.8601 / 512 / – | 29.9 / 64 / 7 |
| S4a | **47.619** | 43.478 | 29.8571 / 55 / – | 29.8846 / 32 / – |
| S4b | **50.314** | 42.553 | 29.8594 / 512 / 255 | 29.8829 / 64 / 7 |
| S4c | **50.000** | 49.080 | 29.8594 / 587 / 255 | 29.8947 / 64 / 7 |
| S4d | **48.930** | 47.337 | 29.8593 / 521 / – | 29.8589 / 282 / – |

Every leg is faster, the ramp reaches **512–587** units where pass 2 stopped at 32–282, and the knees
pass 2 fitted at 3 and 7 units are gone (S2, S3 fit no knee at all; S4b/S4c hold at 255). That is
`fix/knee-race` (§4.25, `8135ec6e`) plus `fix/clamp-trap` (§4.22) landing on this platform: §4.22's
"wd-vit is CPU-bound on that desktop at ~40–50 items/s from 3 units up" reads differently now — the
low knees were the ring failing to certify a rung, and without them the same host runs 48–50 items/s.
The fitted slope also moved **towards** Linux: 29.857–29.860 here against pass 2's 29.9
(0.99973–0.99985 of 29.86533, where pass 2 was 1.00116).

## 4. Findings

| # | severity | finding |
|---|---|---|
| **W-A1** | high | A model whose weights do not leave room for one batch produces an **OOM storm, not a refusal**. `clip/qwen3-vl-embedding-8b` on a 32 607 MiB card: admitted with `share_mb=0 room_mb=0 headroom_mb=0`, the pre-fit branch grants the registry's whole `seed_units` (2 000 000 px) at `mb=0`, deflation walks to 22 (budget 1), and every request still `torch.OutOfMemoryError`s. 1 124 / 1 436 failed items, **0 completed**, 5 018 / 6 254 OOM lines, no cooldown, no eviction, no store row, job runs to the cap with `outcome: running`. Details and log lines in §2 |
| **W-A2** | medium | **Token-priced rows do not travel to Windows** — and are not stable within Windows. `textembed/all-MiniLM-L6-v2` 1.154× Linux via loadgen and 0.902× via the OCR chain (1.28× apart from each other); `all-mpnet-base-v2` 1.491×; `tclip/qwen3-vl-embedding-2b` 1.138× while its own pixel-side twin is 1.001×. `platform_copies` must not be granted to any `aggregation = "max-times-count"` id |
| **W-A3** | low, method | A Windows baseline leg whose fit ring never reaches the Linux row's `max_units_measured` is not comparable. `doctr/db_resnet50_crnn_vgg16_bn` fitted **8.2649** (ratio 1.033) from a ring that stopped at 64 units and **8.0 exactly** from one that reached 128. Drive the leg from several concurrent slots of different `items=` and long enough to reach the Linux anchor |
| **W-A4** | low, observation | On WDDM the sysmem fallback served roughly 18 GB of a "device" allocation: under a hog holding 18 944 MiB on a 32 607 MiB board the worker still reported `base_mb = 31595 / reserved_at_load_mb = 31202`, peak device `used_mb` 32 525. The driver's "Prefer No Sysmem Fallback" setting did not cover these processes. `base_method` also fell through `free_delta` to **`alloc_delta`** on that load |
| **W-A5** | none, closes a defect | **T7 is closed.** `textembed` is reachable from a corpus on Windows through `legs.py --scenario S14-textembed` over the `text` tier: 300 OCR'd pages → 17 fit samples for `textembed/all-MiniLM-L6-v2`, `grant_safety` SKIP → PASS |
| **W-A6** | none, recording nit | `r4-S14-textembed`'s `jobs.json` carries only the first of the chain's two jobs, though the leg log shows both `job_start`/`job_end drained` and the store carries both profiles. `legs.py` snapshots `/api/jobs/data/history` once and the second record had not landed |
| **W-A7** | none, marginal | `r4-S5-oom_second_batch` `persistence` FAILs at exactly the threshold (`30.0 s [threshold 30 s]`) on a leg whose job ran 0.083 s; the same leg's four other moves are all `no workers in any health sample`, the 0.5 s poller missing a job that short. Same job outcome as pass 2 (completed, 180/180, 0 errors) |

**Not reproduced / closed since pass 2:** D0 (the Windows build defect) — the merged tip builds in
1 m 24 s. The low knees of §4.22 (3 and 7 units) — gone, and throughput is 8–22 % higher for it.

## 5. Provenance

| artefact | what |
|---|---|
| `selftest.log`, `win-selftest.json` | §0 |
| `baselines/<leg>/` | §1 — `calibration.after.toml`, `loadgen.log`, `leg.log`, health snapshots. 10 legs |
| `seeding/seed-sm120-win.toml` | §2 — the three re-keyed shipped rows, verbatim but for `platform` |
| `seeding/seedsum.json` | §2 — per-leg grants, seeded profiles, deflation, OOM counts, job stats |
| `seeding/<leg>/` | §2 — `legs.json` (hog and `floor_bound`), `jobs.json`, `calibration.after.toml`, `leg.log`. 14 legs |
| `regression/<leg>/` | §3 — `verdicts.json`, `legs.json`, `calibration.after.toml`, `jobs.json`, `leg.log`. 18 legs |

Drivers used on the box, kept under the clone's untracked `.win/run4/`: `sweepleg.ps1` (one loadgen
baseline leg with a port guard), `seedsum.py`, `analyzeall.py`, and `gw.py` / `winjob.ps1` from pass 2.

## 6. Wall clock

| task | window (UTC) | elapsed |
|---|---|---|
| Clone update, `cargo build --release -p panoptikon`, `uv sync`, pytest, `selftest.py` | 17:30 – 17:41 | 11 min |
| §1 per-implementation sweep (10 loadgen legs + the clap job leg, incl. one aborted port collision and the docTR repeat) | 17:41 – 19:12 | 1 h 31 min |
| §2 shipped-baseline seeding scenario (14 legs, incl. a 16 GB model download) | 19:12 – 19:49 | 37 min |
| §3 regression pass (18 legs) | 19:49 – 20:35 | 46 min |
| analysis, copy-back, report | 20:35 – 21:00 | 25 min |
