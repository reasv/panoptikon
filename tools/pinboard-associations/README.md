# Pinboard association backfill

One-off tool that gives **pre-existing** pinboards their database associations
(`docs/pinboard-db-association-design.md`, "Backfill"). It drives the HTTP API
of a **running** gateway — it never starts anything, and it only ever *adds*
associations.

There is deliberately no migration doing this: a user_data migration runs with
no index databases attached and no way to enumerate them, and no overlap
threshold is defensible as automatic membership. So the tool proposes and a
human reads the table.

Stdlib-only Python; no venv needed.

## When to run it

After upgrading the gateway to a build that has the association feature, once,
against your real databases. New boards stamp themselves on create, and intact
boards are admitted by the 100%-overlap clause with no stamp at all — the only
boards that need this are **rotted-but-mine** ones (files moved or died, so
overlap is 34/40 rather than 40/40), which are invisible under the default-on
"only boards from this database" filter until they are stamped.

## Usage

Start the gateway normally, then look before you leap:

```bash
# 1. dry run: prints the full table and the exact requests it would send
python tools/pinboard-associations/run_backfill.py --api-url http://127.0.0.1:6342

# 2. read the table, then write the proposals
python tools/pinboard-associations/run_backfill.py --api-url http://127.0.0.1:6342 --apply
```

Options: `--user` (pinboard owner, default `user` — the same default the API
uses; get it wrong and you will list a different user's, i.e. no, boards),
`--user-data-db` (defaults to the gateway's current one — associations live in
user_data, so this picks *which library* you are backfilling), `--threshold`
(default `0.5`), `--timeout`, `--self-test`.

No auth headers: the local gateway does not ask for any.

## Reading the table

One row per board, one column per index database. A cell is
`present/items pct` plus a marker:

- `S` — the board is already stamped for that database (clause (a)/(b)).
- `=` — 100% of its items are there, so it is already admitted by clause (c)
  and needs **no stamp**.
- `+` — proposed: this is what `--apply` would write.
- `-` — nothing of the board is in that database. `?` — that database's
  listing did not arrive (the tool warns and carries on), so the board's
  overlap there is *unknown*, never zero.

The `action` column also names any database the write will touch **besides**
the proposal ("also re-associates: drawings"). That is not cosmetic: the PUT
resolves every name in the payload, so carrying an existing stamp can re-point
it at whatever that name means locally today. Everything the write will do is
in the printout.

Sections, in print order:

- **PROPOSED STAMPS** — exactly one database has overlap ≥ threshold, below
  100%, and no stamp yet; the board is not already at home anywhere else; and
  every database was actually checked. These are written by `--apply`.
- **NEEDS REVIEW** — two or more plausible databases, a candidate on a board
  that is *already* associated somewhere, or a candidate on a board with a
  database that could not be checked. Never written unless you add
  `--include-ambiguous`, because partial overlap is not a membership signal:
  unrelated databases share incidental images. Decide these by hand (the card
  context menu → "Databases" does the same thing for one board).
- **ALREADY COVERED** — stamped, or 100% present somewhere. Nothing to do.
- **NO CANDIDATE** — best overlap is below the threshold. Lower `--threshold`
  to see them proposed, or leave them alone.
- **SKIPPED** — boards with no items (clause (c) guards on `item_count > 0`,
  and stamping an empty board buys nothing).

`--threshold` only controls what gets *proposed*. Every board is printed in
every run regardless. It must be below `1.0`: 100% overlap is clause (c), which
needs no stamp, so `1.0` could never propose anything.

## Safety properties

- **Dry-run by default.** `--apply` is required for any write, and the dry run
  prints each request line verbatim. One caveat by design: the dry run shows
  the payload built from the listing it just took, while a real write re-reads
  the board first (below), so the payload actually sent can pick up an
  association saved in between.
- **Never removes anything.** The `PUT .../databases` endpoint is
  *replace*-semantics, so every request carries the board's existing stamped
  names **plus** the addition — and those existing names are re-read fresh
  (one `GET /api/pinboards` per target database, taken at the start of that
  database's writes) rather than trusted from the opening listing, so a stamp
  saved from the UI mid-run is not silently dropped. If that re-read fails, the
  affected boards are **skipped** (reported, counted as failures) rather than
  written from stale data. Removing an association is the UI editor's job.
  (Sending an existing name also re-resolves it against the local database of
  that name, so a stale stamp from a rebuilt database gets re-pointed at the
  current one. That is the endpoint's design, and it is why the tool sends the
  name rather than trying to preserve rows by identity — it cannot see the
  UUIDs, which never leave the server. Every such effect is named in the
  printout.)
- **Leaves no other trace.** That re-read deliberately uses the list endpoint
  rather than `GET /api/pinboards/{id}`: the detail endpoint records an *open*,
  and its debounce is long enough that every board the tool touched would count
  as visited — a backfill would come out the other end having reshuffled an
  activity-sorted library. Stamps are the only thing this tool writes.
- **One request per (board, database).** A board proposed for two databases
  gets two writes. Each names only its own target among the proposals (plus
  everything already stored), so an unresolvable sibling cannot 400 a good
  write; and the response can only prove the association for the database the
  request ran against, which is what makes each write verifiable.
- **Writes are verified, not assumed.** A 200 is not proof: the endpoint's
  carry half always succeeds while the resolve half — the part that actually
  stamps — is skipped when the instance has no identity file or the target
  database has no identity row yet, which returns 200 with nothing added. The
  tool checks the response really lists the target as associated, and reports
  a failure naming the likely cause if not.
- **Per-board error handling.** A failed request is reported and the run
  continues to the next board. Dropped connections, read timeouts and non-JSON
  responses are handled the same way as API errors — nothing escapes to a
  traceback, so the closing summary always says how far the run got. Exit
  status is 0 for a clean run or dry run, 1 if anything failed or the gateway
  was unreachable, 2 for a bad `--threshold`.
- **Idempotent.** After a verified apply, the board carries a stamp whose
  identity is the database's own, so that database's next listing returns
  `databases[].associated = true` for it — which is exactly what the tool
  reads to decide "already stamped". A second run therefore finds the board
  under ALREADY COVERED and proposes nothing. (`--self-test` asserts this by
  replaying the post-apply listing.)
- **No proxy, ever.** The gateway is normally on localhost; an ambient
  `HTTP_PROXY` is explicitly disabled for these calls.

## Self-test

The proposal computation is a pure function over the fetched JSON, and
`--self-test` runs it against embedded fixtures — partial-overlap proposal,
100%-overlap no-action, already-stamped no-action, multi-database ambiguity,
zero-item boards, the carry-existing-names payload, side-effect disclosure, a
missing listing (which must demote a proposal to review), control characters in
a board name, the threshold guards, write verification, and the post-apply
idempotence replay.

It also runs the apply loop against a deliberately broken stub gateway it
spins up on localhost — dropped connection, non-JSON 200, carry-only 200,
unreadable pre-write re-read — and asserts the run survives all of them and
still reaches the last board. The same stub pins down the two payload rules:
a stamp that exists only in the fresh re-read must appear in the request body,
and a board with two proposals must produce two requests neither of which
names the other's target:

```bash
python tools/pinboard-associations/run_backfill.py --self-test
```

It needs no gateway of its own.
