# Measuring a model's cost dimension

How to fill in a new (or re-priced) model's `metadata.cost` block in
`python/inferio/config/inference.toml`. Runnable end to end by an agent on any
machine with a GPU; nothing here is specific to one host. What the keys mean:
`docs/batch-calibration-design.md`, "Model metadata additions".

## 1. Prerequisites

1. The dev venv: `python/.venv` (`uv sync` in `python/`), used below as `$V`.
2. The model's weights downloaded and the impl importable — probe once at
   `--batches 1` and fix load errors before measuring anything.
3. A corpus with a manifest, generated deterministically:

   ```bash
   V=python/.venv/bin/python; T=tools/calibration-protocol
   $V $T/corpus.py --tier ramp --scale 8 --out $T/results/corpus/ramp8
   ```

   Pick the tier whose items are the shape the model really receives, and probe
   **one group** of it (`--group`): a `pixel` slope is only comparable to
   another fit over the same group. `ramp`/`ramp8` (1024² JPEG) for image
   models, `text --group txt-1k` for text, `ocr --group scan-1240x1754` for
   OCR, `pixmix --group img-1mp` for tiled VLMs, `audio --group wav-30s` with
   `--mode audio-npy` for `whisper`/`clap` — and for `clap` also
   `--audio-sample-rate 48000`, matching that group's declared
   `input_spec.opts.sample_rate`: the flag defaults to the handler's 16 000,
   and nothing in the `.npy` carries a rate, so a mismatch is not raised
   anywhere — it just prices and embeds the wrong audio. `--list-tiers` has
   the rest.
4. A GPU with nothing else on it. Stop whatever else holds VRAM, confirm the
   board reads idle, and run one probe process per GPU — never two.

## 2. Probe

```bash
$V $T/ceiling_probe.py --model <group>/<inference_id> \
   --corpus $T/results/corpus/<tier>/manifest.json [--group <corpus group>] \
   --device 0 --batches 1,2,4,8,16,32,64,128,256,512 \
   --repeats 2 --warmup 1 --empty-cache-between-sizes \
   --out /tmp/<inference_id>.json
```

`--repeats 2` so one slow first batch cannot set the slope, `--warmup 1` to pay
lazy init outside the measurement, `--empty-cache-between-sizes` so each size
starts from a released allocator rather than the previous size's cached blocks
— without it `peak_reserved_mb` measures the pool, not the batch. Shorten the
ladder where a larger batch is meaningless; extend it past 512 in a second run
when nothing in the ladder failed and you need the real ceiling.

## 3. Read the result

Fields: `tools/calibration-protocol/README.md`, "The probe's output". Only rows
with `ran_whole_batch: true` count; where you read a ladder by hand — the bend,
the ceiling — take the **worst** of a size's two repeats.

- **Slope.** `fit.slope_mb_per_unit` (`fit.basis` reads `peak_allocated_mb`):
  Theil–Sen over `(units, peak_allocated_mb)` for every whole row, the
  estimator and the currency the ledger itself fits. Read it as measured. Do
  **not** seed from `fit_reserved`, the same fit over `delta_mb`: reserved is a
  caching high-water mark, 1.0–1.5× steeper per model and size, and the run2
  sweep reproduced `peak_allocated_mb` across runs to within 3 MiB and
  `delta_mb` not at all.
- **Linear or not.** `max |residual| / measured` over the ladder, under 5 % =
  linear. If it is not, say where it bends and take the stable marginal (the
  slope over the flat part) for the seed; an early step still seeds safely
  because the seed is small.
- **Ceiling.** The first batch that did not run whole, and *why*: `oom` with an
  `oom_class`, `absorbed_halvings > 0` (the impl swallowed an OOM), or
  `index_limit_events > 0` (a kernel's 32-bit element index, not memory — it
  can land with the whole GPU free). Record both.

## 4. Choose `seed_units`

The seed is the first-touch batch on unknown hardware, so it is a fixed budget
of first-touch growth, B = 2048 MiB:

```
seed_units = max(1, floor(B / slope))
```

rounded **down** to a round figure in the unit — `item`: the nearest lower of
1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256; `pixel`/`token`:
two significant digits — then capped at the largest whole batch **stated in the
id's own unit**: the `units` field of that batch record, never its item count.
Never above that cap, whatever the arithmetic says.

One worked example per unit, from the run2 sweep:

- `item`/`count` — `doctr/db_resnet50_parseq`, slope 8.0 MiB/item:
  `floor(2048 / 8.0)` = 256 → figure 256. Largest whole batch 128 items = **128
  units**, so the seed is capped to 128.
- `pixel`/`sum` — `clip/qwen3-vl-embedding-2b`, slope 0.000260 MiB/px:
  `floor(2048 / 0.000260079)` = 7 874 570 → 7 800 000 px. Its largest whole
  batch is 256 images of 1 048 576 px, and `sum` prices it Σ px = **268 435 456
  units**, not 256, so the cap does not bind.
- `token`/`max-times-count` — `textembed/all-MiniLM-L6-v2`, slope 0.0158922
  MiB/token: `floor(2048 / 0.0158922)` = 128 868 → 120 000 tokens. Largest
  whole batch 512 texts whose longest is 259 tokens, and `max-times-count`
  prices it max × count = 259 × 512 = **132 608 units**, so 120 000 fits under
  it. (A `pixel` id priced `max-times-count` reads the same way: max px ×
  count.)

If the derived value equals the id's group `seed_units`, write nothing: the
group default already says it. A `unit = "none"` model takes no seed.

## 5. Choose `canvas_pixels` (`pixel` models only)

Only a `pixel`-priced model has one: the largest number of decoded pixels one
input can cost it, whatever resolution it arrived at — the supremum, not a
typical value. Read it off the impl or its processor (a tile grid's
`max_tiles × tile²`, a `max_pixels`, a detector's `canvas_size²`) and record in
a comment where it comes from. Omit it when the canvas travels with the
weights: the worker reads the loaded impl's own attribute and reports it, which
stays version-correct where a guess here would override it.

**A declared canvas obliges the impl** to bound its own batch tensor to that
area per item before it pads — check that before declaring one. Declaring or
changing a canvas re-denominates what one unit *is*, so bump
`metadata.cost.epoch` in the same commit. Changing only `seed_units` does not:
the seed is where a ramp starts, not a price.

## 6. Where each key goes

`unit`, `aggregation`, `epoch`, `seed_units` and `canvas_pixels` all live under
`[group.G.metadata.cost]`, overlaid key by key by a `metadata.cost.<key>` line
on the id. Put a figure on the **group** when it is true of the whole group, on
the **id** when it deviates. `seed_units` and `canvas_pixels` are scale-bound:
an id that redeclares `unit` inherits neither and must state its own.

## 7. Record it, then test

In the PR, one table row per measured id — id, unit, slope (MiB/unit),
`B / slope`, seed chosen, group default, largest whole batch — plus the probe
JSON's path, the corpus group it was measured on, and the linear/nonlinear
verdict. A slope with no corpus group named cannot be checked later.

```bash
cargo test -p panoptikon --bin panoptikon inferio::cost
cargo test -p panoptikon --bin panoptikon resources
$V -m pytest tests -q            # from python/
```

`inferio::cost::shipped_registry_is_fully_classified` is the one that fails if
a new id has no valid cost declaration.
