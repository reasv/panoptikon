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
- `-` — nothing of the board is in that database. `?` — the board did not
  appear in that database's listing at all (should not happen; treated as
  "unknown", never as zero).

Sections, in print order:

- **PROPOSED STAMPS** — exactly one database has overlap ≥ threshold, below
  100%, and no stamp yet, and the board is not already at home anywhere else.
  These are written by `--apply`.
- **NEEDS REVIEW** — two or more plausible databases, or a candidate on a
  board that is *already* associated somewhere. Never written unless you add
  `--include-ambiguous`, because partial overlap is not a membership signal:
  unrelated databases share incidental images. Decide these by hand (the card
  context menu → "Databases" does the same thing for one board).
- **ALREADY COVERED** — stamped, or 100% present somewhere. Nothing to do.
- **NO CANDIDATE** — best overlap is below the threshold. Lower `--threshold`
  to see them proposed, or leave them alone.
- **SKIPPED** — boards with no items (clause (c) guards on `item_count > 0`,
  and stamping an empty board buys nothing).

`--threshold` only controls what gets *proposed*. Every board is printed in
every run regardless.

## Safety properties

- **Dry-run by default.** `--apply` is required for any write, and the dry run
  prints each request body verbatim, exactly as it would be sent.
- **Never removes anything.** The `PUT .../databases` endpoint is
  *replace*-semantics, so every request carries the board's existing stamped
  names **plus** the addition. Removing an association is the UI editor's job.
  (Sending an existing name also re-resolves it against the local database of
  that name, so a stale stamp from a rebuilt database gets re-pointed at the
  current one. That is the endpoint's design, and it is why the tool sends the
  name rather than trying to preserve rows by identity — it cannot see the
  UUIDs, which never leave the server.)
- **Per-board error handling.** A failed PUT is reported and the run
  continues; the exit status is non-zero if anything failed.
- **Idempotent.** After a successful apply, the board carries a stamp whose
  identity is the database's own, so that database's next listing returns
  `databases[].associated = true` for it — which is exactly what the tool
  reads to decide "already stamped". A second run therefore finds the board
  under ALREADY COVERED and proposes nothing. (`--self-test` asserts this by
  replaying the post-apply listing.)

## Self-test

The proposal computation is a pure function over the fetched JSON, and
`--self-test` runs it against embedded fixtures — partial-overlap proposal,
100%-overlap no-action, already-stamped no-action, multi-database ambiguity,
zero-item boards, the carry-existing-names payload, a missing listing, the
threshold, and the post-apply idempotence replay:

```bash
python tools/pinboard-associations/run_backfill.py --self-test
```

It needs no gateway.
