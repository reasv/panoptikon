# Pinboard activity ordering & clean new-tab links

Two related changes to how saved pinboards are found and opened:

1. A new default library ordering ("activity") that combines recency with a
   decaying frequency score, so unnamed boards the user keeps returning to
   are findable without scrolling — merely *opening* a board counts, not
   just saving it.
2. Library/history links opened in a new tab produce a clean, maximized
   board view instead of carrying the source tab's whole UI state.

Both live in the pinboard link/list surface and ship together.

## Part 1: activity ordering

### Recording opens

An "open" is recorded server-side in the `get_pinboard` handler
(`GET /api/pinboards/{pinboard_id}`). This endpoint fires on every path
that shows a board: the library's same-tab open, the `pbid`+`pbl` link
loader, and — via `PinboardMenu.tsx`'s always-on query (`enabled: pbid !=
null`, react-query default `staleTime: 0`) — every full page load,
refresh, session restore, and soft navigation of a tab with a board. No
client-side beacon is needed; the UI is not changed for recording.

Consequences accepted by design:

- A board sitting open in a background grid tab counts as "open" on each
  remount/refetch. The debounce absorbs this.
- Library hover previews do NOT hit this endpoint (they fetch version
  preview images), so browsing previews never counts.

### Debounce

An open only *does* anything when the stored `frecency_at` is older than
`W = 2 hours` (or NULL). Within the window, the GET does zero extra work:
the three activity columns ride the row fetch `get_pinboard` already
performs, and the handler just compares timestamps. This both limits
writes and encodes the semantic: "it was already open and I refreshed it"
is not a new visit, while a tab that stays open across restarts still
re-bumps every W at most.

There is deliberately **no in-memory cache**: the debounce read is free
(piggybacked on the existing SELECT) and the write is rare by definition.

### The write

On a qualifying open, the handler spawns a fire-and-forget task (the
response never waits):

- Opens its own short-lived write connection to the user_data DB
  (`get_pinboard` itself stays a pooled `ReadOnly` connection).
- Runs a guarded UPDATE:
  `SET last_seen = now, frecency = frecency * decay + 1, frecency_at = now
   WHERE id = ? AND user = ? AND frecency_at IS <value read>`
  with `decay = 2^(-(now - frecency_at)/Hf)` computed in Rust (NULL
  `frecency_at` → the stored `frecency` contributes 0). The guard makes
  concurrent racers collapse to one event.
- Is best-effort: on SQLITE_BUSY/lock contention with a concurrent save it
  logs at debug and gives up. Activity data is telemetry, not content.
- Requires the user-facing write transactions (pinboard saves, bookmarks)
  to use `BEGIN IMMEDIATE`: with a deferred BEGIN, a read pins the WAL
  snapshot and the background writer's commit would make the *save's*
  first write fail `SQLITE_BUSY_SNAPSHOT` (which bypasses the busy
  handler) — the contention asymmetry must run the other way, with the
  telemetry write as the loser. Because `BEGIN IMMEDIATE` write-locks every
  *writable* attached schema, `UserDataWrite` connections (and this
  writer's) open the index (`main`) and `storage` schemas read-only — via
  `mode=ro` URIs on a read-write connection, since a read-only connection
  cannot attach user_data read-write — confining the immediate lock to
  user_data so saves never contend with the index writer's transactions.
- Deliberately bypasses the `DbConnection` drop-time user-data epoch bump:
  `last_seen`/`frecency`/`frecency_at` can never affect any search result,
  so counted opens must not invalidate the search cache.

### Events from saves and creation

- `save_pinboard_version` (including `no_op` saves) sets `last_seen = now`
  **unconditionally** (a save is a deliberate act) alongside the existing
  `time_updated` bump, and applies the frecency increment **debounced by
  the same W** (an editing session with five saves is one visit). Runs
  inside the existing write transaction.
- `create_pinboard` seeds `last_seen = now`, `frecency_at = now`, and
  `frecency = SEED_NEW = 3.0`: a new board has zero accumulated score
  through no fault of its own but above-average importance to the user. 3
  outranks a weekly-habit board (F ≈ 2) for roughly four days of decay.
  "Save as new copy" goes through the same path and gets the same boost.
- Renames, deletes, and version deletes are **not** events. Every
  successful save *request* is one, including settings-only saves: a Save
  click is a deliberate act, and excluding it would be illusory anyway —
  the post-save react-query refetch hits `get_pinboard` and records the
  open regardless. The old "a settings-only save must not reorder the
  library" invariant survives where it matters: `set_flags` still never
  bumps `time_updated`, so the "Last saved" ordering never moves.

### Schema

New migration (plain `ALTER TABLE`, user_data schema):

```sql
ALTER TABLE pinboards ADD COLUMN last_seen INTEGER;
ALTER TABLE pinboards ADD COLUMN frecency REAL NOT NULL DEFAULT 0;
ALTER TABLE pinboards ADD COLUMN frecency_at INTEGER;
UPDATE pinboards SET last_seen = unixepoch(time_updated, 'utc');
CREATE INDEX idx_pinboards_last_seen ON pinboards(last_seen);
```

- The new timestamps are **unix-epoch integer seconds**, deviating from
  the table's localtime-text convention deliberately: the decay math needs
  real durations and localtime text is DST-ambiguous. (`time_added`/
  `time_updated` strings are localtime, hence the `'utc'` modifier to
  convert on backfill.)
- Backfilling `last_seen = time_updated` means: saves always maintain
  `last_seen`, so after migration `last_seen` alone IS the "last activity"
  recency key — no `max()` needed anywhere, and the smart order initially
  equals the current recency order (no jarring reshuffle at upgrade).

### Score and ordering

Computed in Rust at list time, over the rows `list_pinboards` fetches
(there is no pagination today, so this is exact):

```
score = B * 2^(-(now - last_seen) / Hr)  +  frecency * 2^(-(now - frecency_at) / Hf)
```

Ages are clamped to `>= 0` before the exponent: a stored timestamp in the
future (bad clock at write time) must read as "just now", not as
`decay > 1` — unclamped, an 80-years-ahead stamp scores `+inf` forever
with no self-heal path, since the debounce check also fails closed on a
backwards clock.

The final "activity" ordering is a hybrid with a bounded-pollution pinned
strip:

- **Section 1:** the top `R_PINNED = 5` boards by `last_seen` DESC —
  whatever was touched most recently is always right at the top,
  regardless of score.
- **Section 2:** all remaining boards by `score` DESC; ties broken by
  `last_seen` DESC, then `id` DESC.

Rationale: the strip guarantees just-opened/just-created boards are
immediately visible, which frees the recency-boost term `B` from having to
dominate frequency. With a small `B`, inspecting a few dozen boards
pollutes at most the R strip slots — the other opens land *below*
established habitual boards (F ≈ 4–10), not above. Frequency accumulates
(daily use converges to ~Hf/day-interval ≈ 10, weekly ≈ 2) and decays with
`Hf`, giving staying power that is not a recency proxy.

Constants (named consts in `db/pinboards.rs`, no config surface until one
provably feels wrong):

| const      | value | meaning                                    |
|------------|-------|--------------------------------------------|
| `W`        | 2 h   | debounce: min gap between counted events    |
| `Hf`       | 7 d   | frequency half-life                         |
| `SEED_NEW` | 3.0   | frecency seeded at board creation           |
| `R_PINNED` | 5     | recency-strip size (~one library row)       |
| `B`        | 4.0   | recency boost weight (section 2 tiebreaking)|
| `Hr`       | 6 h   | recency boost half-life                     |

`now` is a parameter of the db-layer functions so tests use fixed clocks.
Setting `R_PINNED = 0` degenerates to the pure-additive model — tuning by
feel later is a constants change.

### API and UI

- `GET /api/pinboards` gains `order=activity|updated` (default
  `activity`). `updated` is the existing `time_updated` DESC order. The
  order applies identically under FTS name search.
- `PinboardSummaryResponse` gains `last_seen` (nullable unix seconds) —
  for future "opened 2h ago" card display and debuggability. Requires
  regenerating `ui/lib/panoptikon.d.ts`.
- Library dialog: a small sort toggle ("Activity" / "Last saved"),
  persisted in localStorage, default Activity, passed as the query param.

### Scaling (future, documented only)

When the list endpoint someday paginates, exactness is preserved by
scoring a candidate union: top-K by `last_seen` DESC (indexed) ∪ top-K by
decayed frecency. The latter is indexable without query-time math because
decayed-frecency order is time-invariant and equals ordering by
`frecency_at + Hf * log2(frecency)` (computable in Rust at write time).
Not built now.

## Part 2: clean new-tab links

`pinboardOpenHref` (ui `lib/pinboardLinks.ts`) currently copies the whole
current query string, so middle-click/ctrl-click/right-click-new-tab
carries sidebar (`sb`/`sbt`), search state, everything. Invert the
construction: **build the href from scratch** in both modes.

Always seeded: `index_db`/`user_data_db` copied from the current params
(boards are per-DB objects and would not resolve otherwise), `pbid`,
`pbl`, `gpb=true` (a fresh tab has no gallery index; without the board tab
fronted the user would land on an empty grid — this forcing exists today
for the same reason).

- **Clean mode (default):** additionally force `gf=true`. Result: the new
  tab opens as a maximized board (`isPinboardMaximized`: `gf` + board
  present + `gpb`) over an otherwise-default view. No sidebar, no search,
  no gallery index.
- **Carry mode:** instead copy the current values of exactly the
  board-affecting view params — `gf` (maximized), `ghp` (board visible in
  gallery), `gt` (thumbnails). Sidebar and search state are still never
  carried.

The from-scratch build subsumes the existing `PINBOARD_DEFAULTABLE_KEYS`
stripping (board-scoped flags are simply never copied).

Scope and UI:

- Only the href changes: same-tab left-click keeps going through
  `loadBoard` into the current tab's context. Implementation must verify
  the library card and history rows intercept plain left-click
  (`preventDefault` → `openBoard`) so clean mode can never wipe the
  current tab's state via a same-tab navigation of the href.
- History rows use the same helper — version opens follow the setting.
- Checkbox in the library dialog, default **on**, persisted in
  localStorage: label **"Open maximized in new tabs"**, tooltip: "Boards
  opened in a new tab (middle-click, Ctrl-click) start maximized with
  nothing else open. When off, new tabs inherit your current view settings
  instead (never the sidebar or search)."

## Non-goals

- No pagination of the pinboard list; no candidate-union scoring yet.
- No config surface for the scoring constants.
- No "opened X ago" display on cards yet (only the API field).
- Mutable-source concerns and pinboard search integration are untouched:
  the new columns never affect search results (which is also why the
  activity write path must not bump the user-data epoch).
