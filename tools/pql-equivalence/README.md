# PQL equivalence suite

One-time validation harness that runs the same PQL queries through the
**legacy Python implementation** (imported in-process from the
`python-legacy/` worktree) and the **Rust server** (spawned with
`readonly = true`, queried over HTTP), against the same database snapshot,
and diffs counts, result rows, and ordering.

This is not CI — it exists to validate the Rust PQL port against real data
before retiring the Python code path for good.

## Setup

Requires the `python-legacy/` worktree mounted at the repo root
(`git worktree add python-legacy python-legacy`) and a release build of the
Rust binary (`cargo build --release`).

Create the suite venv (independent of the managed runtime venv — `uv pip`
here touches no lockfile):

```bash
uv venv tools/pql-equivalence/.venv --python 3.12
uv pip install -p tools/pql-equivalence/.venv \
    "sqlalchemy==2.0.39" "pydantic==2.10.6" "numpy==2.2.3" \
    "pillow==10.4.0" "alembic==1.15.1" "sqlite-vec>=0.1.6,<0.2"
```

Versions are pinned to what the legacy `uv.lock` shipped — newer SQLAlchemy
(2.0.51+) breaks the legacy query builder (`_DialectArgView` attribute
error). No FastAPI/torch needed: the driver stubs the legacy `inferio`
package's top-level `__init__` (which drags in the inference HTTP server)
and never triggers inline inference.

## Usage

```bash
tools/pql-equivalence/.venv/Scripts/python tools/pql-equivalence/run_suite.py \
    --data-folder /path/to/snapshot/data \
    --index-db default --user-data-db default
```

The data folder must have the standard layout:
`index/<db>/{index.db,storage.db}` and `user_data/<db>.db`.

**Work on a copy of your data folder, never the live one.** The suite itself
opens everything read-only (the Python side with `mode=ro`, the Rust server
with `readonly = true`, which also skips startup migrations), but:

- the snapshot must already be at the current Rust schema. If it isn't (or
  the user_data DB is missing), run once with `--prepare`, which boots the
  Rust server **writable** to migrate/create the DBs — this mutates the
  data folder, hence: copy.
- never point it at a data folder a live server is using.

Useful flags: `--only <substr>` to filter cases, `--port` (default 6345),
`--page-size` (default 100), `--rust-bin`, `--out report.json`,
`--float-rtol/--float-atol` for distance comparisons.

## What it covers

The corpus is parameterized by discovery queries against the snapshot
(top tags, setters per data type, embedding dimensions, a text word for FTS,
bookmark users/namespaces, sample sha256/path), so it adapts to whatever the
snapshot contains; cases whose prerequisites are missing are SKIPPED.

Cases cover: default/explicit queries, ordering (asc/desc, tie-break,
no-tie-break), paging, `page_size=0` (no limit), column projection,
count-only/results-only, `partition_by`, `entity=text`, `match` operators
(eq/in_/gt/lte) with and/or/not nesting, `match_path`, `match_text` (FTS,
ranked + snippet), `match_tags` (any/all/confidence/setters/namespaces),
`in_bookmarks`, `processed_by`, `has_data_unprocessed`, semantic text/image
search, `similar_to` (L2/COSINE), RRF hybrid ranking, sortable options
(`select_as`, `gt` cursor), and `check_path`.

The `ui_*` cases replicate the exact JSON shapes the production web UI
sends to `POST /api/search/pql` (`queryFromState` /
`sbSimilarityQueryFromState` in `ui/lib/state/searchQuery/searchQuery.ts`,
plus `FindButton`'s folder lookup), including its quirks: every filter arg
present at its nuqs url-state default (zeros, empty lists,
`select_snippet_as: ""`), `order: null` in `order_by`, an explicit
`partition_by: null`, extra url-state keys spread into `match_tags`
(`pos_match_all` & co.) and `src_text` (`raw_fts5_match`), `page_size: -1`
(FindButton, no limit), and the UI's fixed priorities/directions/RRF
constants (bookmarks/path/text 0, tags 50, semantic 60, anytext 100 with
rrf k=5/w=1 for path+text and k=10/w=0.5/0.7 for semantic text/image,
similarity 150). They cover: the default empty-`and_` search page (with
partition and ordering variants), mime/path prefix + exclusion `match`
filters, bookmark filtering/ordering/sub-namespaces, path/text FTS as
filter and rank source, `filter_only`, snippets, all four tag list slots
(pos/neg × any/all) plus confidence/setter/namespace variants and
`all_setters_required`, semantic text search with `src_text` variants
(length bounds, setters+languages, confidence weights), semantic image
search (plain and `clip_xmodal`), the "anytext" combined search as
single-filter and multi-filter RRF hybrids (up to path+text+two semantic
sources), its count-query twin, item similarity search-page mode
(CLIP COSINE, xmodal, text L2, `src_text` setter restriction), and the
similar-items sidebar queries (page_size 6, `count: false`, partition).

Two `ui_*` cases are regression reproducers for real divergences the suite
found in the Rust port, both since fixed: `ui_match_text_filter`
(`select_snippet_as: ""` — legacy treats the empty string as unset; Rust
used to gate on `is_some()` and computed/returned a spurious snippet under
the key `""`; empty aliases are now ignored everywhere, matching Python
truthiness) and `ui_semantic_text_conf_weight` (`src_text` confidence
weights emit `POW(...)`, which the bundled SQLite lacked — now enabled via
`LIBSQLITE3_FLAGS` in `.cargo/config.toml`; legacy additionally cross-joins
when only weights are set with no other src_text criteria, which the Rust
port fixed). Both must PASS.

Semantic queries never call inference: the driver generates a deterministic
pseudo-embedding per model (seeded by the model name, correct dimension) and
feeds the identical vector to both sides — as a base64 `.npy` string with
`embed: null` for Rust (`extract_embeddings`), and by injecting the raw f32
bytes into the validated filter model for Python (whose `embed` field cannot
be null on the wire; `set_validated(True)` skips its inference call).

`image_embeddings` and `similar_to` additionally resolve per-model
`distance_func` overrides from the inference server's metadata endpoint on
both sides. The suite runs without an inference server, so it starts a tiny
metadata stub (ephemeral port) that serves the discovered model groups/ids
with no `distance_func`, points the Rust gateway at it via
`[[upstreams.inference]]`, and patches the legacy
`get_distance_func_override` to return `None`. "No override" matches the
production config for every embedding model the corpus can discover; both
sides therefore use the query's declared distance function unchanged.

## Reading the output

- `PASS` — identical counts and rows (floats compared with tolerance).
- `ORDER_DIFF` — same rows, different order. Usually sort-tie ambiguity
  (SQL gives no stable order for equal keys); real only if it shows up on a
  case with a `file_id` tie-breaker.
- `COUNT_DIFF` / `RESULT_DIFF` — a real divergence; the report JSON has the
  first differing row from each side.
- `PY_ERROR` / `RUST_ERROR` / `BOTH_ERROR` — one side rejected or crashed on
  a query the other accepted (also a real finding unless the query itself is
  invalid).

Exit code is non-zero if any case is worse than ORDER_DIFF. Full details land
in `report.json` (gitignored).
