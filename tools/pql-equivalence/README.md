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

Semantic queries never call inference: the driver generates a deterministic
pseudo-embedding per model (seeded by the model name, correct dimension) and
feeds the identical vector to both sides — as a base64 `.npy` string with
`embed: null` for Rust (`extract_embeddings`), and by injecting the raw f32
bytes into the validated filter model for Python (whose `embed` field cannot
be null on the wire; `set_validated(True)` skips its inference call).

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
