# Client performance plan: search results view

Tracking document for the pre-release client-performance push, focused on the
search results view end to end, starting with the virtualized grid at large page
sizes (3 000–10 000 results). Compiled 2026-07-19 from a code investigation of
the gateway (`panoptikon/`), the UI (`ui/`), and the desktop wrapper
(`panoptikon-desktop/`).

Baseline symptom: with 4–5 columns and rapid scrolling, blurhashes render
instantly but real images trail far behind; a single column loads near-instantly.
Bookmark buttons fire one API request per grid cell. Rendering itself is not the
bottleneck.

## Root-cause summary

| # | Finding | Where |
|---|---------|-------|
| 1 | Gateway serves plain HTTP (no TLS) → browsers negotiate HTTP/1.1 → **~6 concurrent connections per origin** shared by all images, bookmark checks, and API calls. Explains "1 column instant, 5 columns queue". | `panoptikon/src/main.rs:544-582` (no TLS acceptor; hyper has h2 built but no ALPN) |
| 2 | **No `Cache-Control`, no `ETag`, no 304 handling** on any image response. Stored thumbnails carry zero validators; original files only `Last-Modified` (heuristic caching). Every re-scroll/re-search re-downloads bodies. | `panoptikon/src/api/items.rs:558-602` |
| 3 | **No SQLite connection pool**: every request opens a fresh connection, ATTACHes storage.db *and* user_data.db (user_data unused on the image path), and runs 2 PRAGMAs before the real query. | `panoptikon/src/db/connection.rs:263-365`, extractor at `:88-108` |
| 4 | **Blocking `Path::exists()` per metadata row on the tokio worker**, no timeout — a synchronous SMB stat per thumbnail request; a hung share can stall runtime workers. `file_response` also does a blocking `metadata()`. | `panoptikon/src/db/items.rs:241-249`, `panoptikon/src/api/items.rs:492-499` |
| 5 | **One `GET /api/bookmarks/ns/{ns}/{sha256}` per grid cell on mount**. Hover-reveal is CSS-only, so the query fires whether or not the button is ever shown. Virtualized cells recycle (overscan 3 rows) → continuous request stream competing for the same 6 connections. No batch status endpoint exists. | `ui/components/imageButtons.tsx:80-86`, `ui/components/SearchResultImage.tsx:108`, `ui/app/search/SearchPage.tsx:371-376` |
| 6 | Desktop opens the search UI in the **system browser** at `http://localhost:{port}` (by design — browser chrome is the point). Any transport change must work for a plain browser origin; the Tauri webview is not a lever. | `panoptikon-desktop/src-tauri/src/lib.rs:1020-1022`, `:1102-1124` |

Supporting facts for the design choices below:

- Image URLs are **content-addressed** (`/api/items/item/thumbnail?id=<sha256>&id_type=sha256`), so immutable caching is semantically correct. `ui/lib/utils.ts:8-18`.
- `sha256` is present in every search result by default (`panoptikon/src/pql/model.rs:452-459`, `panoptikon/src/api/search.rs:741`).
- `user_data.bookmarks` is **already ATTACHed on read-only search connections** (`connection.rs:100,329`), so post-query bookmark lookup needs no attach changes.
- Index-DB writes are fully serialized through one `IndexDbWriter` actor per DB (`panoptikon/src/db/index_writer.rs`); bookmark writes run directly in API handlers, unserialized. This asymmetry drives the cache design.
- No epoch/generation counter exists anywhere today; `PRAGMA data_version` is unused.
- `user_data.db` gets `journal_mode=WAL` only on its **first write connection** (`connection.rs:337-345`); a fresh DB may sit in rollback-journal mode until then.

---

## Optimizations

Ordered by priority. Status: `[ ]` open, `[x]` done, `[-]` rejected/deferred.

### P1 — Immutable caching on image endpoints

- [ ] **1.1 `Cache-Control: public, max-age=<large>, immutable` + `ETag` on thumbnail and file responses.**
  ETag = sha256 (+ thumbnail idx for the storage-blob path). Applies to
  `bytes_response` (`items.rs:586-602`) and `file_response` (`items.rs:484-584`).
  Content is addressed by hash in the URL, so `immutable` is correct, not a
  heuristic.
- [ ] **1.2 Conditional GET → 304.** Honor `If-None-Match` (and
  `If-Modified-Since` on the file path, which already emits `Last-Modified`).
  Today the server never returns 304; only `If-Range` is inspected
  (`items.rs:511-521`).
- [ ] **1.3 URL versioning escape hatch.** If thumbnail regeneration at
  different quality/size ever lands, bust caches by versioning the URL (e.g.
  `&v=`) rather than weakening `immutable`.

Expected effect: every second look at anything — scrolling back, re-running a
search, reopening the app — costs zero bytes. Largest win per line of code in
the whole plan.

### P2 — Per-request backend cost on the image path

- [ ] **2.1 Remove the blocking SMB stat from the thumbnail path.** Drop
  `Path::exists()` in `get_item_metadata` (`db/items.rs:241-249`) for image
  serving (a missing file fails at `open()` anyway, which must be handled
  regardless), or make it async + time-limited where existence filtering is
  genuinely needed (search `check_path` is a separate, opt-in concern).
- [ ] **2.2 Async/timeout hygiene for file IO.** `file_response`'s blocking
  `metadata()` (`items.rs:492-499`) and unbounded reads over SMB should not be
  able to stall tokio workers; add `spawn_blocking` or async equivalents and a
  sane timeout.
- [ ] **2.3 SQLite connection reuse.** Replace open-per-request with a reader
  pool (or cached connections) keyed by `(index_db, user_data_db)`, eliminating
  the per-request open + double ATTACH + PRAGMA ceremony
  (`connection.rs:263-365`).
- [ ] **2.4 Stop attaching `user_data` on paths that never read it** (the image
  endpoints attach it unconditionally today via `from_request_parts`,
  `connection.rs:100`). Falls out naturally of 2.3's pool design if pools are
  per-purpose.
- [-] **2.5 In-memory sha256 → location index.** Deferred, likely unnecessary:
  after 2.1–2.3 the remaining per-request DB cost is a microsecond point lookup.
  Revisit only if measurement after P1+P2 shows the metadata query itself
  matters (e.g. very large scale deployments). Trivially config-gatable if
  built.

### P3 — Bookmark status: server-side enrichment, kill per-cell GETs

Design decision (2026-07-19): include bookmark status **in the search response,
computed as post-query enrichment** — NOT as a JOIN/EXISTS in the compiled PQL
SQL, and NOT as a client-initiated second query (which cannot overlap with
search, since the client lacks the sha256s until search returns; at 10 000-item
pages the extra round trip is pure waste).

- [ ] **3.1 Opt-in search request flag** (e.g. `include_bookmarks` with
  namespace/user). After `run_compiled_query` returns rows
  (`search.rs:350-367`), run one
  `SELECT sha256 FROM user_data.bookmarks WHERE sha256 IN (...) AND user = ? AND namespace = ?`
  over the page's sha256s and stamp results — a `bookmarked` /
  `bookmark_namespaces` field on `SearchResult` (or the existing `extra` map,
  `search.rs:138-142`). `user_data` is already attached on search connections.
- [ ] **3.2 Client: seed a sha256-keyed bookmark store from the search
  response** and delete the per-cell `$api.useQuery` in `BookmarkBtn`
  (`imageButtons.tsx:80-86`). Buttons read from the shared store.
- [ ] **3.3 Mutations update the store locally** from the mutation result (the
  response already tells you the new state — no refetch for the clicked item).
  Keep the existing per-item endpoint (`GET /api/bookmarks/item/{sha256}`) for
  spot reconciliation if a surface ever needs it.
- [ ] **3.4 Ensure `user_data.db` is in WAL from creation**, not lazily on first
  write (`connection.rs:337-345`) — with every search reading the bookmarks
  table, a rollback-journal DB would reintroduce reader/writer contention.
- [-] **3.5 Batch status endpoint** (POST sha256 list → statuses). Nice-to-have
  for other surfaces; not load-bearing once 3.1 exists. Skip until something
  needs it.
- [-] **3.6 Hover-lazy fetching.** Rejected: pop-in / wrong-state-then-correct
  on hover is exactly the jank to avoid, and enrichment makes status free.
- [-] **3.7 Bookmark JOIN inside compiled search SQL.** Rejected: couples the
  most complex component to independently-mutating state and poisons the future
  search cache. Post-query enrichment gives identical wire shape without the
  coupling.

Why enrichment also future-proofs the cache: the search cache (P6) stores the
**un-enriched** result set; enrichment re-runs on every response, cache hit or
miss. Bookmark writes never invalidate cached searches; bookmark status in
responses is always fresh; the cache key never involves the user_data DB.

### P4 — Re-measure gate

- [ ] **4.1 Re-run the baseline test after P1–P3**: 4–5 column grid, page size
  3 000–10 000, rapid scroll + arbitrary jumps, network panel open. Compare
  first-view latency, repeat-view latency (should be ~all 304/memory-cache), and
  bookmark request count (should be 0 extra requests).
  P5 items proceed only if this still shows lane starvation.

### P5 — Transport & payload (contingent on P4)

- [-] **5.1 HTTP/2 via locally-trusted TLS.** Deferred, likely never for local:
  browsers require TLS for h2, which means installing a local CA (mkcert-style).
  Cross-platform reliability is poor: Windows workable (`certutil` + admin
  prompt); macOS needs admin + interactive keychain confirmation and breaks
  across OS updates; Linux is fragmented (Chromium/Firefox read NSS profile DBs,
  not the system store; Snap/Flatpak sandbox them; Firefox ignores OS stores
  everywhere without enterprise policy). Plus the optics/AV-flag cost of
  installing a root CA (name-constrained CA mitigates, doesn't erase).
  **Remote deployments should get h2 from a TLS-terminating reverse proxy
  instead — document this in deploy docs.**
- [-] **5.2 Second origin for images** (`127.0.0.1` + `localhost`, or a second
  port) to double the connection budget. Held in reserve; splits the browser
  cache by origin and complicates policy tokens. Only if 5.1 stays off the table
  and P4 still shows starvation.
- [ ] **5.3 JIT thumbnail system, policy-gated.** Wanted independently for
  internet-exposed instances (policy decides which endpoints serve thumbs vs
  originals). Generous quality/resolution — local UX must not degrade. For the
  local fast-scroll case this shrinks per-request lane occupancy, but note the
  honest scope: the **first** view still reads the full original off the NAS to
  generate the thumb; the win is every subsequent view (stored thumb served from
  local storage.db + browser cache). Nothing short of pre-generation (already
  ruled out: DB bloat, import slowdown, quality) removes the first-pass NAS
  read.
- [-] **5.4 Client-side image throttling / load-on-scroll-stop.** Rejected for
  the local default — it guarantees worse UX (the design goal is images
  resolving while they blow past). Possible future *public-instance* policy
  option for bandwidth saving only.

### P6 — Search cache (separate track, enabled by this work)

- [ ] **6.1 Epoch-based invalidation.** Introduce a monotonic epoch per index
  DB, bumped at the single chokepoint every index write already flows through —
  the `IndexDbWriter` actor (`index_writer.rs:252-292`) / job completion.
  (`PRAGMA data_version` is the ready-made alternative; currently unused.)
  100 % reliable because index writes are fully serialized; bookmarks stay out
  of the cached payload entirely (see P3), so their unserialized handler-driven
  writes never matter.
- [ ] **6.2 Multi-page eager caching** for full-table-scan searches: compute
  several pages server-side, cache, return the requested slice.

### P7 — Client rendering pass (minor; part of the broader end-to-end effort)

None of these are the current bottleneck; recorded for the later
reactivity/rendering pass over the search view.

- [ ] **7.1 Blurhash decoding cost.** Each cell PNG-encodes its blurhash in pure
  JS synchronously during render (`ui/lib/state/blurHashDataURL.ts`, memoized
  per hash). Consider a cheaper path (canvas decode, CSS-only approximation, or
  off-main-thread) if profiling shows it.
- [ ] **7.2 `SearchResultImage` re-executes every scroll frame** — the
  virtualized parent re-renders per frame (`"use no memo"`, inherent to
  tanstack-virtual v3) and passes non-referentially-stable props
  (`onImageClick`, `dbs`), defeating React Compiler memoization
  (`SearchPage.tsx:551-585`, `SearchResultImage.tsx`). Stabilize props /
  `React.memo` the card.
- [ ] **7.3 Image request cancellation on recycle** relies on the browser
  aborting detached `<img>` loads — no app-side guarantee (no
  AbortController/src-reset teardown). Note only; revisit if stale in-flight
  requests still visibly delay fresh ones after P1/P2.
- [ ] **7.4 Search-as-you-type reactivity audit** — the rest of the end-to-end
  scope (input → throttled query → state → render). Not yet investigated in
  depth; `useThrottledValue` + `searchThrottleMs` govern query issuance today.

---

## Measurement notes

Test rig: local gateway, files on SMB-mounted ZFS NAS (10G), page size
3 000–10 000, grid at 4–5 columns.

Per change, capture before/after:
- Network panel: request count (images, bookmarks), response codes (expect 304s
  after P1 on repeat views), queue/stall time per image (the HTTP/1.1 lane
  starvation signature), total transferred bytes.
- First-view fast-scroll: time from scroll-stop to all visible images rendered;
  same for arbitrary jump-to-offset.
- Repeat-view: same scroll path a second time (should approach zero network
  after P1).
- Server: per-request latency on `/api/items/item/thumbnail` (connection setup
  vs query vs IO), tokio worker stalls with NAS intentionally slowed if
  feasible.
