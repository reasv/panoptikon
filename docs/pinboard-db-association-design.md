# Pinboard ↔ database association — design

Status: DESIGNED 2026-08-07 — rule and identity model user-resolved through
design review (three earlier match rules were rejected; the walkthroughs are
recorded below so they don't get re-proposed). Implementation review
2026-08-08 closed the remaining questions (cold-cache probe, PUT carry-through
semantics, transition/default answers — all folded in below). Not implemented.

## Problem

The pinboard library is not database-aware in any way. By design, user_data
contents are decoupled from any particular index DB — pins are sha256
addressed, and the same user_data DB can be attached to any index DB. Boards
carry their own stored previews, so every listing surface (library modal,
grid Library tab, sidebar board combobox) renders every board regardless of
which index DB is selected. Consequences:

- **Order/hygiene**: with the phone-photos DB selected, screenshot boards
  pollute the list — especially the activity-sorted library modal, where
  they disturb the order.
- **Broken opens**: opening a board whose items the current DB doesn't have
  renders broken image loads. (Every other surface degrades to empty on a
  DB switch; boards are the only thing that *breaks*.)
- The grid Library tab is partially immune (it intersects search results
  with board items), but a board sharing 1–2 incidental images with the
  current DB still leaks through.

The fix must preserve the decoupling: user_data must remain attachable to
any index DB, associations are **hints, never authority**, and there must be
a manual fix path for every automatic decision.

## Two principles (user-stated, they shape everything below)

1. **100% overlap is always valid.** If every item on a board exists in the
   current DB, the board may be treated as belonging to it — even if it was
   authored elsewhere. This is a principle the rule *does* apply in
   practice (clause (c) below).
2. **Partial overlap is not definable as a membership signal.** Rot is
   inevitable (files move, die, get modified), so low overlap doesn't mean
   "not mine"; and unrelated DBs share incidental images (camera roll
   containing drawings that also live in the drawings DB), so nonzero
   overlap doesn't mean "mine". Partial overlap is therefore never used for
   automatic membership — only for display ("38/40 here"), the
   stamp-on-save mistake guard, and human review in the backfill tool.

## Rejected: per-item DB hints

Storing a source DB per pinned element (which would let items from
*different* DBs render on one board) is rejected:

- Considerable URL cost — the full layout lives in the URL and every byte
  counts; a DB name per element doesn't fit the budget.
- It dirties the pin model: `in_pinboard`, the pinboard content search
  intersection (`api/search.rs::compose_pinboard_search`), and the mosaic
  export are all clean sha256 set operations today; per-item DB routing
  would thread a second dimension through all of them.

Everything below is board-level.

## Identity model

### Why a name alone can't be the record

Index DBs are identified by name only (`index_db` is a plain string param;
a DB is literally a folder, `data/index/<name>/index.db`). That is
deliberate: names are readable in URLs, work like folders UX-wise, and an
index DB must survive being deleted and remade from its TOML config. But
names are not unique across instances — `default` is the shipped default
name — and once user_data DBs sync between instances (planned), a
name-keyed association can never be disambiguated retroactively. Recording
richer identity now is the part that can't be retrofitted.

### Database UUID

Each database gets a one-row identity table, stamped by a migration
(`lower(hex(randomblob(16)))` — pure SQL, no new dependency):

- `migrations/index/…_database_identity.sql` — the identity clause (a) of
  the match rule reads this.
- `migrations/user_data/…_database_identity.sql` — **nothing reads this
  today.** It exists purely because future cross-instance sync of user_data
  needs an identity minted before files start traveling; stamping later
  can't distinguish copies.

Properties, stated plainly:

- A **folder rename** keeps the UUID → associations survive renames with no
  manual fixing (a win the name-only status quo doesn't have).
- **Delete-and-remake from TOML mints a fresh UUID.** This is inherent (the
  only durable home for the UUID is the DB file itself) and accepted; the
  name-fallback clause (b) exists to carry associations across it.

UUIDs never appear in API responses, URLs, or UI — they are server-side
matching keys only. Names remain the human-facing residual hint (e.g. to
label a broken link whose DB is gone).

### Instance UUID

One UUID in a plain file at the data dir root (`data_folder/instance_id`,
sibling of `index/` and `user_data/`), minted on first run. Deliberately
outside every database so it survives any DB delete/remake. It answers the
one question the stored `(db_uuid, name)` pair cannot: **who stamped this
association** — which is the only bit that differs between "my rebuilt
`default`" and "another instance's `default`" (both present as
dangling-UUID + matching-name).

Degradation: if the file can't be created (read-only deployment), clause
(b) simply never fires; (a) and (c) are unaffected.

## Association storage

New user_data migration:

```sql
CREATE TABLE pinboard_databases (
    pinboard_id   INTEGER NOT NULL,
    db_uuid       TEXT    NOT NULL,
    db_name       TEXT    NOT NULL,  -- name at stamp time, residual hint
    instance_uuid TEXT    NOT NULL,
    last_stamped  INTEGER NOT NULL,  -- unix seconds, same clock as activity
    PRIMARY KEY (pinboard_id, db_uuid)
) ;
```

- Rows are per (board, DB incarnation): a rebuilt DB stamps a second row
  with the same name and a new UUID. Harmless; the old row is what clause
  (b) matches until the manual editor or a re-stamp cleans it up.
- Board-level, never versioned. Stamping **must not bump
  `pinboards.time_updated`** — same rule as `set_flags` and the preview PUT
  (`db/pinboards.rs`): an association write is not a content change and
  must not reorder the library.
- Deleted explicitly in `delete_pinboard`, matching the existing
  no-FK-cascade style.
- `last_stamped` upserts on re-stamp; it is the "most recently stamped"
  order the opens-in link uses.

## The match rule

A board is **associated with the current index DB** iff any of:

> **(a)** a stamped row's `db_uuid` equals the current DB's identity UUID;
> **(b)** a stamped row has `instance_uuid` == this instance **and**
> `db_name` == the current `index_db` name **and** its `db_uuid` matches
> **no existing local index DB**;
> **(c)** `present_count == item_count` **and** `item_count > 0`
> (100% overlap, head version).

The filter (default-on, see UI) hides non-associated boards.

### Why each clause is shaped this way — rejected predecessors

This rule is the fourth iteration; the first three each failed a concrete
walkthrough. Recorded so they stay rejected:

1. *UUID only*: breaks every association on delete-and-remake from TOML —
   the exact lifecycle index DBs are required to survive. And since boards
   are effectively write-once (made once, rarely re-saved), save-time
   re-stamping does **not** heal this.
2. *UUID OR name (plain disjunction)*: name becomes a co-equal matcher, so
   any stamp naming `default` matches whenever `default` is selected —
   the status-quo collision, now enshrined.
3. *UUID OR (name AND uuid-dangling)*: fixes intra-instance rename-reuse,
   but still fails the **trivial case of one user_data DB shared by two
   instances that both use the name `default`**: instance B sees a
   dangling `uuid_A` + matching name and wrongly admits instance A's
   boards. Dangling+name-match describes both "my rebuilt DB" and "someone
   else's DB"; the stored pair cannot distinguish them.
4. *(current)* the instance UUID supplies the missing "who stamped it" bit.
   Walkthroughs:
   - **Two instances, both `default`, shared user_data**: B's rule sees
     `instance_A != instance_B` → (b) refused. Correct.
   - **Rebuild-from-TOML, same instance**: instance matches, old UUID
     dangles, name matches → (b) admits. Correct.
   - **Rename-reuse** ("photos"→"phone", new DB created as "photos"):
     stamps carry `uuid_P`, which still exists locally under "phone" → the
     not-claimed-elsewhere gate refuses (b); the board follows (a) to
     "phone" instead. Correct.

Residual failure modes, accepted:

- **Cloned data dir** (instance file copied wholesale): clones share an
  instance UUID; the collision returns. Degenerate; accepted.
- **Wiped data dir**: new instance UUID; rotted boards lose (b) across a
  subsequent rebuild until re-stamped (backfill tool / manual editor).
  Accepted — the stamps remain, only the fallback declines to fire.
- **Memory-only instance identity** (minting worked, persisting the file did
  not): stamps written during that run carry an instance UUID that exists
  nowhere on disk, so after a restart the instance mints a different one.
  Clause (a) is unaffected — the `db_uuid` is what it matches on — and only
  the cross-restart continuity of (b) is lost, the same degradation as an
  instance with no identity at all. Accepted; the alternative (refusing to
  stamp) would cost (a) as well.
- **Copied index DB** (folder copied, or `VACUUM INTO` of an index DB):
  the copy carries the original's `db_uuid`, so clause (a) matches *both*
  incarnations and the badge / opens-in link may point at the stale copy.
  Accepted — associations are hints, never authority, and the manual editor
  is the fix path. Step 2's local-UUID cache should log a warning when two
  local folders probe to the same UUID; that is the only place the
  duplication is visible.

Note (b)'s entire cargo is *rotted boards across a same-instance rebuild*:
intact boards re-admit through (c) after a rebuild of the same folders, no
name clause needed. If (b) ever proves troublesome, dropping it costs only
that case.

### Clause (c) mechanics — overlap is cheap here

`present_count` = distinct head-version sha256s that exist in the current
index DB. Pinboard handlers already run on a connection with both DBs
attached (the same fact the content search leans on), and `items.sha256` is
UNIQUE, so this is one indexed probe per pin across the library — thousands
of probes, not a search. It rides inside the existing `list_pinboards`
query as a correlated count next to `item_count`; no PQL, no second
round-trip. The `item_count > 0` guard exists because `0 == 0` would admit
every empty board everywhere. (Empty boards are impossible to construct via
the shipped UI — user-confirmed 2026-08-08 — so the guard is purely
defensive against hand-made API writes; no pre-existing empty boards need a
transition story.)

`present_count` is returned on every summary regardless of the filter — it
is also the display signal ("38/40 here") that tells rot apart from
foreignness.

### The local-UUID set (for (b)'s not-claimed-elsewhere gate)

The gate needs "the set of UUIDs of currently existing local index DBs".
Per-process cache, name → **probe result** (not name → UUID: the probe is
three-state, see below), re-validated against the folder listing per list
call (folders can be deleted out-of-band, so entries for vanished folders are
dropped — `load_db_info` already enumerates the folders).

**The cache must fill eagerly on miss, not only on normal DB opens** (review
finding 2026-08-08): the rename-reuse walkthrough only refuses (b) because
the gate *sees* `uuid_P` living under "phone" — after a gateway restart,
lazy-only fill leaves the cache cold until "phone" happens to be opened, and
(b) would wrongly admit in the window. So: on a list call, any existing
index folder absent from the cache gets a cheap read-only identity probe —
open the file read-only, SELECT the UUID, close. **Never through the normal
open path**: `migrate_path` runs migrations plus post-migration ANALYZE, and
a pinboard list must not trigger that on every local DB. Cost is bounded by
folder count and paid only on misses. The current DB's own UUID is a one-row
query on the already-attached connection.

The probe is **three-state**, and the difference is load-bearing for the gate
(`db::identity::DbIdentityProbe`):

- **Claims a UUID** — read successfully, identity row present.
- **Claims nothing** — read successfully, but there is no identity table
  (pre-upgrade, never opened), no row, or an unusable value. Also a path with
  no file at all. This is a real answer: the DB claims no UUID, so it cannot
  be the elsewhere-claimant that refuses (b).
- **Unknown** — the file exists but could not be interrogated (locked,
  corrupt, unreadable, read-only filesystem refusing the `-shm`). Not an
  answer: that DB might be holding exactly the stamped UUID. The gate must
  **fail closed** — an Unknown anywhere in the folder listing refuses the
  name-fallback clause (b) for stamps whose UUID is otherwise unaccounted
  for. Folding Unknown into "claims nothing" would let a momentarily locked
  DB hand its boards to a same-named one, which is precisely the collision
  the instance UUID exists to prevent.

Unknown results are **not cached** — a lock or a busy file is transient, so
the next list call re-probes. Only the two real answers are cached.

## Write points

- **Create** (`create_pinboard`): stamp the current index DB
  unconditionally — the strongest possible signal, and it covers every
  board made after this ships.
- **Save** (`save_pinboard_version`, including `no_op` settings-only
  saves — a save is a deliberate act, same reasoning as `touch_saved`):
  upsert the current DB's stamp **iff `present_count > 0`**. Zero overlap
  means the save happened under a mistakenly-selected DB; don't record it.
- **Not on open.** Opening a foreign board must not associate it.
- **No instance identity, no stamp.** If `instance_uuid()` reads `None`
  (read-only deployment, unreadable file), write **no row at all** — never a
  sentinel like `''`. A sentinel would compare equal across every
  identity-less instance, so (b) would match any same-named DB anywhere,
  which is exactly rejected rule 3 reintroduced through the back door. The
  cost of skipping the row is that clause (a) is not recorded either; that is
  the smaller loss, and clause (c) still admits intact boards.
- All stamps run inside the existing save transactions (`BEGIN IMMEDIATE`
  convention — the background activity writer makes deferred BEGINs a
  `SQLITE_BUSY_SNAPSHOT` trap; see the header in `api/pinboards.rs`).
- Because boards are write-once in practice, save-stamping is opportunistic
  gravy. The load-bearing paths are: create-stamping (future boards),
  clause (c) (intact boards), and the backfill tool + manual editor
  (existing rotted boards).

## API changes

All under the pinboards authorization domain (a policy denying pinboards
denies these).

- `GET /api/pinboards` — new query param `associated_only: bool`
  (default false; the client sends its stored preference). New per-board
  response fields:
  - `present_count: i64`
  - `associated: bool` (the full rule, server-computed)
  - `databases: [{ name, last_stamped, associated }]` — stamp rows with
    per-row verdicts ((a) or (b)); UUIDs stay server-side. The client uses
    this for the owning-DB badge and the opens-in link (most recently
    stamped row whose name resolves locally), and for the manual editor's
    current state.
- `POST /api/pinboards/search` — same `associated_only` param, applied as
  a post-filter over the matched boards (the grid Library tab must obey
  the same preference).
- `GET /api/pinboards/{id}` — same new fields on the detail response.
- `PUT /api/pinboards/{pinboard_id}/databases` — the manual editor: body
  is a list of DB names; the server replaces the board's rows
  (clear-then-set, like flags restore). Resolution semantics (settled
  2026-08-08 — naive clear-then-set can't keep an unresolvable name, since
  the server can't mint a row for it): a name matching an **existing
  stamped row** carries that row through verbatim; only genuinely new names
  must resolve to a local DB (`(db_uuid, db_name, instance_uuid)`), and an
  unknown-and-unresolvable name is a 400. Removing any stamped name —
  resolvable or not — is expressed by omitting it. Write-domain endpoint;
  does not bump `time_updated`.

## UI

- **Filter checkbox** — "Only boards from this database", default **on**,
  browser-stored in `pinboardLibraryPrefs` next to order/clean-links. One
  preference, three surfaces: library modal, grid Library tab, sidebar
  board combobox (the combobox inherits the shared preference, no toggle of
  its own). Default-on is confirmed including the transition window
  (2026-08-08): pre-existing rotted boards hide until the backfill runs,
  which is acceptable because the backfill will be run locally on the
  author's databases as part of rollout and no one else uses the library
  yet; fresh users have nothing to hide.
- **Foreign boards** (visible only with the filter off): card shows a
  badge with the owning DB name; the card's link href carries that DB's
  `index_db` — `pinboardOpenHref` already copies `DB_KEYS` into every
  board link, this overrides the value. **No silent DB switching**: the
  switch is explicit in the URL, works identically for middle-click/new
  tab, and is trivially reversible. Never overridden when the current DB
  is itself associated. No click-time query — the list response already
  carried everything.
- **Rot display**: `present_count`/`item_count` on cards when they differ
  ("38/40 here").
- **Manual editor** — card context menu → "Databases": checklist of local
  DBs (`/api/db`) plus any stamped-but-unresolvable names (shown with
  their name as the residual hint, removable). Non-optional: an
  automatic-only list can only grow, and renames, accidental stamps, and
  instance-identity resets all need somewhere to go.

## Backfill

**No shipped migration** — structurally impossible, not merely omitted: a
user_data migration runs with no index DBs attached and no way to
enumerate them, and the general case has no ground truth for which DB a
pre-existing board belongs to (principle 2). New users simply start with
no associations; (c) plus create-stamping covers them going forward.

For existing libraries (realistically: the author's ~100 boards), a
one-off script under `tools/` driving the API:

1. Enumerate index DBs (`/api/db`), fetch all boards, compute the
   board × DB overlap matrix.
2. Print proposed stamps (`board → DB, 34/40, 85%`) — **report-then-apply**
   (`--apply` flag), not a blind threshold, because no threshold is
   defensible in general and a human can eyeball 100 rows in a minute.
3. Apply via the `PUT …/databases` endpoint.

Boards at 100% overlap need no stamp at all (clause (c) admits them), so
the review list is only the rotted-but-mine boards — small.

Rollout plan (2026-08-08): the backfill is run locally against the author's
databases as part of shipping this feature — it is not left as a
someday-task, because the default-on filter's transition story depends on
it.

## Out of scope

- **Missing-item placeholder tiles** on the open board (rendering rot as a
  labeled placeholder instead of a broken `<img>`). Cosmetic — the item is
  gone and nothing conjures it — but worth doing separately so rot looks
  intentional rather than broken. Independent of everything here.
- Any use of partial overlap as automatic membership (principle 2).
- Cross-instance sync itself. When it lands, imported stamps should either
  be marked foreign-origin or checked against a local identity history —
  either is a filter-logic change over data this design already records;
  no migration will be needed. That future-proofing is the point of
  recording `(db_uuid, db_name, instance_uuid)` in full.

## Implementation-time decisions (settled 2026-08-08)

- Param/field names as written: `associated_only`, `databases`,
  `present_count`, `associated`. Badge = the owning DB name, plain.
- Sidebar combobox inherits the shared preference; no extra UI.
- Instance-id file: read (minting on first use if absent) lazily behind a
  `OnceCell`-style per-process cache.
- Relay's `instance_id` (api/relay.rs pairing records) is client-supplied
  per-pairing data, not a persistent local identity — checked 2026-08-08;
  no overlap with `data_folder/instance_id`, nothing to reuse.
