#!/usr/bin/env python3
"""Pinboard/database association backfill (docs/pinboard-db-association-design.md).

One-off tool for existing libraries: reads every board once per index database
from a *running* gateway, builds the board x database overlap matrix out of the
`present_count` the list endpoint already returns, prints the proposed stamps,
and (only with `--apply`) writes them through
`PUT /api/pinboards/{id}/databases`.

Report-then-apply by design: no overlap threshold is defensible as automatic
membership (principle 2 in the design doc), so the tool proposes and a human
reads the table. Boards at 100% overlap need no stamp at all - clause (c) of
the match rule admits them - so the review list is only the rotted-but-mine
boards.

Never removes an association: a proposal is always "existing stamped names +
the new one", because the PUT is replace-semantics. Stdlib only, ASCII output
(these consoles are frequently cp1252).

Usage:
    python tools/pinboard-associations/run_backfill.py --api-url http://127.0.0.1:6342
    python tools/pinboard-associations/run_backfill.py --api-url http://127.0.0.1:6342 --apply

See README.md.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

# Categories, in print order. The first two are the ones a human reads.
CAT_STAMP = "stamp"
CAT_REVIEW = "review"
CAT_COVERED = "covered"
CAT_NONE = "none"
CAT_EMPTY = "empty"
CATEGORY_ORDER = [CAT_STAMP, CAT_REVIEW, CAT_COVERED, CAT_NONE, CAT_EMPTY]
CATEGORY_TITLES = {
    CAT_STAMP: "PROPOSED STAMPS (applied with --apply)",
    CAT_REVIEW: "NEEDS REVIEW (applied only with --apply --include-ambiguous)",
    CAT_COVERED: "ALREADY COVERED (no stamp needed)",
    CAT_NONE: "NO CANDIDATE (nothing proposed)",
    CAT_EMPTY: "SKIPPED (no items)",
}


# ---------------------------------------------------------------------------
# HTTP


class ApiError(Exception):
    """A non-2xx response, with whatever detail the body could yield."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


def api_call(base: str, path: str, params: dict, body=None, method: str = "GET", timeout: float = 60.0):
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base.rstrip('/')}{path}" + (f"?{query}" if query else "")
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if data is not None else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as err:
        raise ApiError(err.code, error_detail(err)) from None
    except urllib.error.URLError as err:
        raise ApiError(0, f"{err.reason} ({url})") from None
    if not payload:
        return None
    return json.loads(payload)


def error_detail(err: urllib.error.HTTPError) -> str:
    """The `{"detail": ...}` body the server sends - defensively.

    The OpenAPI document declares no body for the editor's 400, so the shape is
    only what `api_error.rs` actually writes; anything else is reported raw.
    """
    try:
        raw = err.read().decode("utf-8", "replace").strip()
    except Exception:  # pragma: no cover - stream already consumed/closed
        return err.reason or "request failed"
    if not raw:
        return err.reason or "request failed"
    try:
        parsed = json.loads(raw)
    except ValueError:
        return raw[:400]
    if isinstance(parsed, dict) and isinstance(parsed.get("detail"), str):
        return parsed["detail"]
    return raw[:400]


def fetch_index_databases(base: str, timeout: float) -> list[str]:
    info = api_call(base, "/api/db", {}, timeout=timeout)
    index = (info or {}).get("index") or {}
    names = list(index.get("all") or [])
    if not names:
        raise ApiError(0, "/api/db returned no index databases")
    return names


def fetch_boards(base: str, index_db: str, user_data_db: str | None, user: str, timeout: float) -> list[dict]:
    params = {
        "index_db": index_db,
        "user_data_db": user_data_db,
        "user": user,
        # Ordering only affects the wire order; `updated` is stable across the
        # per-database passes, while `activity` shifts as boards are opened.
        "order": "updated",
        # The whole point is to see the boards this database does *not* admit.
        "associated_only": "false",
    }
    data = api_call(base, "/api/pinboards", params, timeout=timeout)
    return list((data or {}).get("pinboards") or [])


# ---------------------------------------------------------------------------
# Proposal computation (pure - this is what --self-test exercises)


@dataclass
class DbView:
    """One board as seen from one index database."""

    db: str
    present_count: int
    item_count: int
    stamped: bool  # a stored stamp resolves to this database (clause (a)/(b))
    seen: bool = True  # the board appeared in this database's listing

    @property
    def ratio(self) -> float:
        return self.present_count / self.item_count if self.item_count else 0.0

    @property
    def full(self) -> bool:
        return self.item_count > 0 and self.present_count >= self.item_count


@dataclass
class BoardPlan:
    board_id: int
    name: str
    item_count: int
    views: list[DbView]
    stamped_names: list[str]  # existing stamp names, stored order
    proposals: list[str] = field(default_factory=list)
    category: str = CAT_NONE
    reason: str = ""

    @property
    def payload_names(self) -> list[str]:
        """The PUT body: replace-semantics, so existing names travel along.

        Sending a stored name carries every row filed under it verbatim *and*
        re-resolves it against the local database of that name - both, by
        design. Only exact duplicates are collapsed; two spellings of one name
        are left alone because the server canonicalizes and the first requested
        resolution wins.
        """
        names: list[str] = []
        for name in list(self.stamped_names) + list(self.proposals):
            if name not in names:
                names.append(name)
        return names


def board_views(board_by_db: dict[str, dict], databases: list[str], item_count: int) -> list[DbView]:
    views: list[DbView] = []
    for db in databases:
        board = board_by_db.get(db)
        if board is None:
            views.append(DbView(db=db, present_count=0, item_count=item_count, stamped=False, seen=False))
            continue
        stamped = any(bool(row.get("associated")) for row in board.get("databases") or [])
        views.append(
            DbView(
                db=db,
                present_count=int(board.get("present_count") or 0),
                item_count=int(board.get("item_count") or 0),
                stamped=stamped,
            )
        )
    return views


def stamped_names_of(board_by_db: dict[str, dict], databases: list[str]) -> list[str]:
    """Stamped names as stored - identical in every listing, so the first
    listing that carried the board wins, and later ones only fill gaps."""
    names: list[str] = []
    for db in databases:
        board = board_by_db.get(db)
        if board is None:
            continue
        for row in board.get("databases") or []:
            name = row.get("name")
            if isinstance(name, str) and name not in names:
                names.append(name)
    return names


def plan_board(board_id: int, name: str, board_by_db: dict[str, dict], databases: list[str], threshold: float) -> BoardPlan:
    sample = next((board_by_db[db] for db in databases if db in board_by_db), {})
    item_count = int(sample.get("item_count") or 0)
    views = board_views(board_by_db, databases, item_count)
    plan = BoardPlan(
        board_id=board_id,
        name=name,
        item_count=item_count,
        views=views,
        stamped_names=stamped_names_of(board_by_db, databases),
    )

    if item_count <= 0:
        # Clause (c) guards on item_count > 0, and stamping an empty board buys
        # nothing: there is no content to find anywhere.
        plan.category = CAT_EMPTY
        plan.reason = "board has no items"
        return plan

    covered = [v.db for v in views if v.stamped or v.full]
    candidates = [v for v in views if v.seen and not v.stamped and not v.full and v.ratio >= threshold and v.present_count > 0]

    if not candidates:
        if covered:
            plan.category = CAT_COVERED
            plan.reason = "already associated with " + ", ".join(covered)
        else:
            best = max((v for v in views if v.seen), key=lambda v: v.ratio, default=None)
            plan.category = CAT_NONE
            plan.reason = (
                f"best overlap {best.present_count}/{best.item_count} in {best.db} is below the threshold"
                if best is not None and best.present_count
                else "no items present in any database"
            )
        return plan

    plan.proposals = [v.db for v in candidates]
    if len(candidates) > 1:
        plan.category = CAT_REVIEW
        plan.reason = "meaningful overlap with " + ", ".join(
            f"{v.db} ({v.present_count}/{v.item_count})" for v in candidates
        )
    elif covered:
        # It is already visible somewhere; adding a second home is a judgement
        # call, not a repair. Partial overlap is never automatic membership.
        plan.category = CAT_REVIEW
        plan.reason = f"candidate {candidates[0].db}, but already associated with " + ", ".join(covered)
    else:
        plan.category = CAT_STAMP
        plan.reason = f"{candidates[0].present_count}/{candidates[0].item_count} present in {candidates[0].db}"
    return plan


def compute_plans(listings: dict[str, list[dict]], databases: list[str], threshold: float) -> list[BoardPlan]:
    """Board plans from the per-database listings. Pure: no I/O.

    `listings` maps index database name -> the `pinboards` array that database's
    listing returned. Board ids are user_data-scoped, so they identify the same
    board across every listing.
    """
    by_board: dict[int, dict[str, dict]] = {}
    names: dict[int, str] = {}
    order: list[int] = []
    for db in databases:
        for board in listings.get(db) or []:
            board_id = int(board["id"])
            if board_id not in by_board:
                by_board[board_id] = {}
                order.append(board_id)
            by_board[board_id][db] = board
            if board.get("name") and board_id not in names:
                names[board_id] = str(board["name"])
    return [
        plan_board(board_id, names.get(board_id, ""), by_board[board_id], databases, threshold)
        for board_id in order
    ]


# ---------------------------------------------------------------------------
# Reporting


def cell(view: DbView) -> str:
    if not view.seen:
        return "?"
    if view.item_count <= 0 or view.present_count <= 0:
        return "-"
    pct = round(100 * view.ratio)
    mark = "S" if view.stamped else ("=" if view.full else " ")
    return f"{view.present_count}/{view.item_count} {pct}%{mark}"


def render_table(plans: list[BoardPlan], databases: list[str]) -> list[str]:
    headers = ["id", "board", "items"] + databases + ["stamps", "action"]
    rows: list[list[str]] = []
    for plan in plans:
        cells = []
        for view in plan.views:
            text = cell(view)
            if view.db in plan.proposals:
                text += " +"
            cells.append(text)
        rows.append(
            [
                str(plan.board_id),
                (plan.name or "(unnamed)")[:32],
                str(plan.item_count),
                *cells,
                ", ".join(plan.stamped_names) or "-",
                ("stamp " + ", ".join(plan.proposals)) if plan.proposals else plan.reason,
            ]
        )
    widths = [len(h) for h in headers]
    for row in rows:
        for i, text in enumerate(row):
            widths[i] = max(widths[i], len(text))
    lines = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip()]
    lines.append("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rows:
        lines.append("  ".join(text.ljust(widths[i]) for i, text in enumerate(row)).rstrip())
    return lines


def print_report(plans: list[BoardPlan], databases: list[str], threshold: float) -> None:
    print(f"{len(plans)} boards, {len(databases)} index databases, threshold {threshold:.2f}")
    print("cell = present/items pct;  S = stamped here,  = = 100% (clause c, no stamp needed),  + = proposed")
    for category in CATEGORY_ORDER:
        group = [plan for plan in plans if plan.category == category]
        if not group:
            continue
        print()
        print(f"== {CATEGORY_TITLES[category]} - {len(group)}")
        for line in render_table(group, databases):
            print(line)
        if category == CAT_REVIEW:
            print()
            for plan in group:
                print(f"  #{plan.board_id}: {plan.reason}")


# ---------------------------------------------------------------------------
# Apply


def apply_plans(base: str, plans: list[BoardPlan], user_data_db: str | None, user: str, timeout: float, dry: bool) -> int:
    failures = 0
    for plan in plans:
        names = plan.payload_names
        target = ", ".join(plan.proposals)
        print(f"PUT /api/pinboards/{plan.board_id}/databases  {json.dumps({'databases': names})}   (+{target})")
        if dry:
            continue
        try:
            # The board is written through the database it is being stamped
            # for: the first proposal resolves against the connection's own
            # index database even when its file probe comes back Unknown.
            result = api_call(
                base,
                f"/api/pinboards/{plan.board_id}/databases",
                {
                    "index_db": plan.proposals[0],
                    "user_data_db": user_data_db,
                    "user": user,
                },
                body={"databases": names},
                method="PUT",
                timeout=timeout,
            )
        except ApiError as err:
            # One board's failure must never abort the run.
            failures += 1
            print(f"  FAILED: {err}")
            continue
        stamped = ", ".join(row.get("name", "?") for row in (result or {}).get("databases") or [])
        print(f"  ok: associated={bool((result or {}).get('associated'))} databases=[{stamped}]")
    return failures


# ---------------------------------------------------------------------------
# Self-test


FIXTURE_DATABASES = ["photos", "drawings", "screenshots"]

# One board per case. `databases[].associated` is the server's per-row verdict
# ("this row is the database currently selected"), so a stamp shows up as
# associated only in its own database's listing.
FIXTURE_LISTINGS: dict[str, list[dict]] = {
    "photos": [
        # 1: rotted but mine - 34/40 here, nothing anywhere else. The case.
        {"id": 1, "name": "holiday", "item_count": 40, "present_count": 34, "associated": False, "databases": []},
        # 2: intact - clause (c) admits it with no stamp.
        {"id": 2, "name": "intact", "item_count": 10, "present_count": 10, "associated": True, "databases": []},
        # 3: already stamped for photos.
        {"id": 3, "name": "stamped", "item_count": 20, "present_count": 12, "associated": True,
         "databases": [{"name": "photos", "last_stamped": 100, "associated": True}]},
        # 4: ambiguous - high overlap in two databases.
        {"id": 4, "name": "ambiguous", "item_count": 10, "present_count": 8, "associated": False, "databases": []},
        # 5: empty board - clause (c)'s guard excludes it; stamping is pointless.
        {"id": 5, "name": "empty", "item_count": 0, "present_count": 0, "associated": False, "databases": []},
        # 6: incidental overlap only - below the threshold everywhere.
        {"id": 6, "name": "incidental", "item_count": 50, "present_count": 2, "associated": False, "databases": []},
        # 7: stamped for a retired database, and rotted-but-mine here. The PUT
        #    must carry "oldcam" through or the stamp is destroyed.
        {"id": 7, "name": "carried", "item_count": 10, "present_count": 9, "associated": False,
         "databases": [{"name": "oldcam", "last_stamped": 50, "associated": False}]},
    ],
    "drawings": [
        {"id": 1, "name": "holiday", "item_count": 40, "present_count": 0, "associated": False, "databases": []},
        {"id": 2, "name": "intact", "item_count": 10, "present_count": 1, "associated": False, "databases": []},
        {"id": 3, "name": "stamped", "item_count": 20, "present_count": 0, "associated": False,
         "databases": [{"name": "photos", "last_stamped": 100, "associated": False}]},
        {"id": 4, "name": "ambiguous", "item_count": 10, "present_count": 7, "associated": False, "databases": []},
        {"id": 5, "name": "empty", "item_count": 0, "present_count": 0, "associated": False, "databases": []},
        {"id": 6, "name": "incidental", "item_count": 50, "present_count": 3, "associated": False, "databases": []},
        {"id": 7, "name": "carried", "item_count": 10, "present_count": 0, "associated": False,
         "databases": [{"name": "oldcam", "last_stamped": 50, "associated": False}]},
    ],
    "screenshots": [
        {"id": 1, "name": "holiday", "item_count": 40, "present_count": 1, "associated": False, "databases": []},
        {"id": 2, "name": "intact", "item_count": 10, "present_count": 0, "associated": False, "databases": []},
        {"id": 3, "name": "stamped", "item_count": 20, "present_count": 0, "associated": False,
         "databases": [{"name": "photos", "last_stamped": 100, "associated": False}]},
        {"id": 4, "name": "ambiguous", "item_count": 10, "present_count": 0, "associated": False, "databases": []},
        {"id": 5, "name": "empty", "item_count": 0, "present_count": 0, "associated": False, "databases": []},
        {"id": 6, "name": "incidental", "item_count": 50, "present_count": 1, "associated": False, "databases": []},
        {"id": 7, "name": "carried", "item_count": 10, "present_count": 0, "associated": False,
         "databases": [{"name": "oldcam", "last_stamped": 50, "associated": False}]},
    ],
}


def self_test() -> int:
    failures: list[str] = []

    def check(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    plans = {p.board_id: p for p in compute_plans(FIXTURE_LISTINGS, FIXTURE_DATABASES, 0.5)}

    # Partial overlap in exactly one database is the proposal case.
    check(plans[1].category == CAT_STAMP, f"board 1 category {plans[1].category}")
    check(plans[1].proposals == ["photos"], f"board 1 proposals {plans[1].proposals}")
    check(plans[1].payload_names == ["photos"], f"board 1 payload {plans[1].payload_names}")

    # 100% overlap needs no stamp at all - clause (c) admits it.
    check(plans[2].category == CAT_COVERED, f"board 2 category {plans[2].category}")
    check(plans[2].proposals == [], f"board 2 proposals {plans[2].proposals}")

    # Already stamped: no new stamp, even though it is rotted (12/20).
    check(plans[3].category == CAT_COVERED, f"board 3 category {plans[3].category}")
    check(plans[3].proposals == [], f"board 3 proposals {plans[3].proposals}")

    # Meaningful overlap with two databases -> flagged, never auto-applied.
    check(plans[4].category == CAT_REVIEW, f"board 4 category {plans[4].category}")
    check(plans[4].proposals == ["photos", "drawings"], f"board 4 proposals {plans[4].proposals}")

    # Empty board: skipped.
    check(plans[5].category == CAT_EMPTY, f"board 5 category {plans[5].category}")
    check(plans[5].proposals == [], f"board 5 proposals {plans[5].proposals}")

    # Incidental overlap below the threshold: nothing proposed.
    check(plans[6].category == CAT_NONE, f"board 6 category {plans[6].category}")
    check(plans[6].proposals == [], f"board 6 proposals {plans[6].proposals}")

    # Replace semantics: the existing (unresolvable) name must ride along.
    check(plans[7].category == CAT_STAMP, f"board 7 category {plans[7].category}")
    check(plans[7].payload_names == ["oldcam", "photos"], f"board 7 payload {plans[7].payload_names}")

    # Idempotence: once applied, the stamp resolves in its own listing, so the
    # next run proposes nothing. Replay the fixture with board 1 stamped.
    applied = {db: [dict(b) for b in boards] for db, boards in FIXTURE_LISTINGS.items()}
    for db, boards in applied.items():
        for board in boards:
            if board["id"] == 1:
                board["databases"] = [{"name": "photos", "last_stamped": 200, "associated": db == "photos"}]
                board["associated"] = db == "photos"
    replay = {p.board_id: p for p in compute_plans(applied, FIXTURE_DATABASES, 0.5)}
    check(replay[1].category == CAT_COVERED, f"replayed board 1 category {replay[1].category}")
    check(replay[1].proposals == [], f"replayed board 1 proposals {replay[1].proposals}")
    check(
        [p.board_id for p in replay.values() if p.category == CAT_STAMP] == [7],
        "replay proposed something other than the untouched board 7",
    )

    # A board missing from one listing must not be read as zero overlap.
    partial = {db: list(boards) for db, boards in FIXTURE_LISTINGS.items()}
    partial["drawings"] = [b for b in partial["drawings"] if b["id"] != 1]
    missing = {p.board_id: p for p in compute_plans(partial, FIXTURE_DATABASES, 0.5)}
    check(any(not v.seen for v in missing[1].views), "missing listing not marked unseen")
    check(missing[1].proposals == ["photos"], f"board 1 proposals with a gap {missing[1].proposals}")

    # The threshold controls what is *proposed*, nothing else.
    strict = {p.board_id: p for p in compute_plans(FIXTURE_LISTINGS, FIXTURE_DATABASES, 0.9)}
    check(strict[1].category == CAT_NONE, f"board 1 at threshold 0.9 is {strict[1].category}")

    # The report must render for every category without blowing up.
    print_report(list(plans.values()), FIXTURE_DATABASES, 0.5)

    print()
    if failures:
        for message in failures:
            print(f"FAIL: {message}")
        print(f"self-test: {len(failures)} failure(s)")
        return 1
    print("self-test: all checks passed")
    return 0


# ---------------------------------------------------------------------------
# CLI


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api-url", default="http://localhost:6342", help="Gateway base URL (default: %(default)s)")
    p.add_argument("--user", default="user", help="Pinboard owner (default: %(default)s)")
    p.add_argument("--user-data-db", default=None, help="user_data database (default: the gateway's current one)")
    p.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Minimum overlap fraction for a database to be *proposed* (default: %(default)s). "
        "It never decides anything on its own - every board is printed either way.",
    )
    p.add_argument("--apply", action="store_true", help="Write the proposals (default: report only)")
    p.add_argument(
        "--include-ambiguous",
        action="store_true",
        help="With --apply, also write the boards flagged NEEDS REVIEW",
    )
    p.add_argument("--timeout", type=float, default=120.0, help="Per-request timeout in seconds")
    p.add_argument("--self-test", action="store_true", help="Run the built-in fixtures and exit")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        return self_test()
    if not 0.0 < args.threshold <= 1.0:
        print("--threshold must be in (0, 1]", file=sys.stderr)
        return 2

    try:
        databases = fetch_index_databases(args.api_url, args.timeout)
        listings = {
            db: fetch_boards(args.api_url, db, args.user_data_db, args.user, args.timeout) for db in databases
        }
    except ApiError as err:
        print(f"gateway request failed: {err}", file=sys.stderr)
        print("Is the gateway running at --api-url?", file=sys.stderr)
        return 1

    plans = compute_plans(listings, databases, args.threshold)
    if not plans:
        print(f"no pinboards for user {args.user!r}")
        return 0
    print_report(plans, databases, args.threshold)

    writable = [p for p in plans if p.category == CAT_STAMP]
    if args.include_ambiguous:
        writable += [p for p in plans if p.category == CAT_REVIEW]
    print()
    if not args.apply:
        print(f"dry run - {len(writable)} board(s) would be written. Requests that would be sent:")
        apply_plans(args.api_url, writable, args.user_data_db, args.user, args.timeout, dry=True)
        print()
        print("Re-run with --apply to write them.")
        return 0

    print(f"applying {len(writable)} board(s):")
    failures = apply_plans(args.api_url, writable, args.user_data_db, args.user, args.timeout, dry=False)
    print()
    print(f"done: {len(writable) - failures} written, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
