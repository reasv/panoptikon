# Quant recall harness

Compares exact vector search against each ready quant profile on a live
gateway (docs/vector-index-design.md, Validation): the same queries run
under `index: "exact"` and `index: "quant"` + `variant`, and the harness
reports overlap@10/50/100, page-level membership equality, and timings.
The headline experiment is `binary` vs `binary-centered` on the real
default DB.

Stdlib-only Python; no venv needed.

## Usage

Start the gateway normally (inference must be up — queries are embedded
server-side), make sure the profiles show **Ready** on the scan page's
Vector quantization card, then:

```bash
python tools/quant-recall/run_recall.py \
    --api-url http://127.0.0.1:6342 \
    --index-db default \
    --queries tools/quant-recall/queries.txt \
    --json report.json
```

- `--queries`: one text query per line; a small built-in set is used when
  omitted. Use queries representative of real usage.
- `--similar N`: also compares `similar_to` for the top N hits of the
  first query (0 disables).
- Results use `cache: false`, so timings are honest first executions and
  the search cache is neither read nor polluted.

## Reading the numbers

- `overlap@N` = fraction of the exact top-N found in the quant top-N.
  sqlite-vec's rescore benchmarks measure 0.988 recall at oversample 8;
  with the default k=10000 and page-depth ≤ 100 the effective oversample
  is ≥ 100, so overlap@100 should be ≥ 0.99 for a healthy profile. A page
  where membership differs is a bug, not a tuning problem — membership is
  never supposed to change.
- Meaningful gaps between `binary` and `binary-centered` profiles on the
  same data are the expected outcome (centered should win on models with
  biased embedding dimensions, CLIP image embeddings included).
