# Outro-detector equivalence harness

Validation harness required by
[`docs/video-outro-detection-design.md`](../../docs/video-outro-detection-design.md)
§12: run the **Python reference** of §3.1 + §3.3 and the **Rust detector**
(`panoptikon/src/media_tools/outro.rs`) over the same sample of real files and
require **identical verdicts and identical K values**.

This is not CI. It is the pql-equivalence discipline applied once, against the
real library, before the detector is trusted.

## Pieces

| file | what it is |
|---|---|
| `reference.py` | the specification side — a straight numpy/subprocess transcription of the design's algorithm, written from the design and not from the Rust code |
| `run_equivalence.py` | sampling, both runs, comparison, report |
| `manifest.tsv`, `rust.jsonl`, `python.jsonl`, `report.json` | run outputs (gitignored-by-habit; regenerate rather than trust) |

The Rust side is driven by `media_tools::outro_equivalence`, an
`#[ignore]`d `#[cfg(test)]` test taking a file list through
`OUTRO_EQUIV_INPUT`/`OUTRO_EQUIV_OUTPUT`. `panoptikon` is a **bin-only crate**
(no `[lib]`), so a `cargo` example or a second `src/bin` entry would be a
separate crate root with no access to `outro`'s `pub(crate)` surface; a unit
test is the only form that reaches it, and `cfg(test)` keeps the shipped
binary untouched.

## Running it

Stdlib + numpy; no venv. **Run from PowerShell**, not the Bash sandbox: most
media lives on the `Z:` network mount (`\\192.168.1.16\z`), which the sandbox
cannot reach.

```powershell
python tools/outro-equivalence/run_equivalence.py --sample 150 --jobs 8
```

- Samples `ORDER BY sha256 LIMIT n` over each index DB's video items —
  deterministic, no RNG to seed — from `data/index/{tiktok,camera}`
  (positives) and `data/index/{screenshots,default,rustest,rustest2}`
  (negatives). Paths not on disk are skipped and counted.
- Opens every database **read-only** (`mode=ro`): they may be in use.
- Pins both engines to the same `ffmpeg`/`ffprobe` binaries
  (`OUTRO_EQUIV_FFMPEG`/`OUTRO_EQUIV_FFPROBE`). Without this the Rust side
  resolves ffmpeg through the managed venv's `static-ffmpeg` and a different
  build would be a divergence blamed on the algorithm.
- The Rust harness is built `--release`: the per-frame pixel loops are a
  debug build's worst case.

`--reuse-rust` / `--reuse-python` / `--skip-rust` re-compare existing results
without re-running an engine.

## The bar (§12)

- zero verdict mismatches (accept/reject *and* which rule rejected),
- zero K mismatches (exact float equality — both sides emit shortest
  round-trip doubles),
- negatives all rejected,
- positive K clustering on the discrete generation values 2.00 / 3.00 / 4.00.

### What the harness refuses to call agreement

A validation harness fails the wrong way when it passes on an absence, so
four things are checked before any verdict is compared:

- **Completeness.** Both result streams must cover the manifest exactly —
  no file missing, none unexpected. A file absent from *both* streams is the
  dangerous case: it looks like agreement over something neither engine ever
  measured, and it now fails the run. (The manifest may repeat a path —
  `rustest` is a subset of `rustest2` — so the expectation is the unique set,
  not the row count.)
- **Error class, not just error.** `spawn` (ffmpeg never ran) and `decode`
  (it ran and failed) take different routes through the visuals ledger
  (§7.2), so pairing one against the other is a divergence, not agreement.
- **The ffmpeg pin is falsifiable.** The Rust harness asserts the ffmpeg it
  resolved is the one it was handed (`install_runtime_for_tests` swallows an
  already-set `OnceLock`, so a pin can silently lose to an earlier
  resolution) and emits it as a header record; the runner rejects a report
  whose harness resolved something else.
- **`accept` and the K histogram are the agreed population only**, so the
  summary table cannot present one engine's numbers as the run's findings.
  `accepted_rust`/`accepted_python` keep both sides visible in `report.json`,
  and the table prints `rust/python` instead of a single figure when they
  differ.

`report.json` records provenance: the DB list, the resolved ffmpeg/ffprobe
paths, `ffmpeg -version`, and `git rev-parse HEAD`.

## The two named divergence risks

Both are handled explicitly and are what the harness is really testing:

- **Median semantics.** `np.median` averages the two middle values over an
  even-length axis; `median_u8` in the Rust side does the same and keeps the
  `.5` in `f64`. `h * 48` is always even, so the averaging branch is the only
  one that ever runs.
- **`scale=48:-2` height rounding.** ffmpeg rounds the derived height half-up
  to a multiple of two. Banker's rounding computes 68 where ffmpeg produces
  70 (576x828 → 69.0), the raw buffer fails to divide into frames, and a
  healthy file is recorded as a probe error.

A third, structural one is specific to the harness: the two sides learn the
frame height **differently**. Rust parses ffmpeg's own reported output
geometry off stderr (falling back to the item's stored dims only when nothing
was reported, and only when the byte count singles out an orientation); the
reference has no such channel and must call `ffprobe`. ffprobe reports
**coded** dimensions while the filter graph auto-rotates, so `reference.py`
reads the stream's rotation side-data and swaps w/h at |90°|/|270°|. A
disagreement here surfaces as a one-sided decode error, not as a wrong
verdict.
