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
the new one", because the PUT is replace-semantics, and the existing names are
re-read from the server immediately before each write rather than trusted from
the opening snapshot. Stdlib only; stdout is reconfigured to UTF-8 with
replacement so board names survive a cp1252 console.

Usage:
    python tools/pinboard-associations/run_backfill.py --api-url http://127.0.0.1:6342
    python tools/pinboard-associations/run_backfill.py --api-url http://127.0.0.1:6342 --apply

See README.md.
"""

from __future__ import annotations

import argparse
import http.client
import json
import re
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


# The gateway is normally on localhost, and an ambient HTTP_PROXY (corporate
# laptops, VPN clients, mitmproxy left running) would otherwise capture these
# calls. An explicit empty ProxyHandler disables proxy lookup entirely.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


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
    # Everything below becomes an ApiError, never an escaping exception: the
    # apply loop catches ApiError per board, so a dropped connection or a read
    # timeout mid-run must not abort the remaining boards (and must not bury
    # the summary that says how far the run got).
    try:
        with OPENER.open(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as err:
        raise ApiError(err.code, error_detail(err)) from None
    except urllib.error.URLError as err:
        raise ApiError(0, f"{err.reason} ({url})") from None
    except (TimeoutError, http.client.HTTPException, OSError) as err:
        # RemoteDisconnected, IncompleteRead, socket timeouts on the *read*
        # (urlopen only wraps the connect), ECONNRESET while streaming.
        raise ApiError(0, f"{type(err).__name__}: {err} ({url})") from None
    if not payload:
        return None
    try:
        return json.loads(payload)
    except ValueError as err:
        # A 200 that is not JSON: a proxy's interstitial, an HTML error page
        # from something in front of the gateway, a truncated body.
        raise ApiError(0, f"response was not JSON ({err}) from {url}") from None


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


def stamped_names_on(board: dict) -> list[str]:
    names: list[str] = []
    for row in board.get("databases") or []:
        name = row.get("name")
        if isinstance(name, str) and name not in names:
            names.append(name)
    return names


def fetch_current_stamps(
    base: str, index_db: str, user_data_db: str | None, user: str, timeout: float
) -> dict[int, list[str]]:
    """Every board's stamped names *now*, keyed by board id, for one database.

    The PUT replaces, so the payload has to carry everything currently stored
    or the write deletes it, and the opening snapshot is not good enough: a
    save in the UI (or the editor, or another run) between the snapshot and the
    write can add a stamp that rebuilding from stale names would drop.

    Deliberately the **list** endpoint and not `GET /api/pinboards/{id}`: the
    detail handler records an open (`spawn_activity_write`), whose debounce is
    long enough that every board this tool touched would count as visited and
    the user's activity-sorted library would come back reordered. A backfill
    must leave no trace beyond the stamps it writes. The list endpoint carries
    the same `databases[]` on every summary and writes nothing.
    """
    return {
        int(board["id"]): stamped_names_on(board)
        for board in fetch_boards(base, index_db, user_data_db, user, timeout)
        if "id" in board
    }


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
    unseen = [v.db for v in views if not v.seen]
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
    elif unseen:
        # An unseen database is not a database with no overlap: its listing
        # never arrived. The single-candidate case is only unambiguous because
        # every other database was checked, so a gap costs it that status.
        plan.category = CAT_REVIEW
        plan.reason = (
            f"candidate {candidates[0].db} "
            f"({candidates[0].present_count}/{candidates[0].item_count}), but "
            + ", ".join(unseen)
            + " could not be checked"
        )
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

CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def clean(text: str) -> str:
    """Board and database names are user data and travel through JSON intact,
    so a name containing a newline or a tab would otherwise shear the table
    apart. Rendered cells only - never the values sent back to the server."""
    return CONTROL_CHARS.sub(" ", text)


def resolvable(name: str, databases: list[str]) -> str | None:
    """The live index database a payload name will resolve to, if any.

    Case-insensitive because the server canonicalizes a name to the folder's
    own spelling before comparing. Note the server's compare is
    `eq_ignore_ascii_case` while `casefold()` folds all of Unicode, so this can
    only ever over-disclose (name a database the server would in fact treat as
    distinct) - the safe direction for a disclosure whose whole job is to make
    sure nothing gets written unannounced.
    """
    for db in databases:
        if db == name:
            return db
    lowered = name.casefold()
    matches = [db for db in databases if db.casefold() == lowered]
    return matches[0] if len(matches) == 1 else None


def resolve_side_effects(names: list[str], proposals: list[str], databases: list[str]) -> list[str]:
    """Databases the write touches *besides* the proposals.

    A requested name does two things server-side: it carries every stored row
    filed under it, and it is also resolved locally, stamping the live database
    of that name afresh if no carried row already covers it. So sending an
    existing name can (re-)associate the board with whatever that name means
    here *now* - which after a delete-and-remake is a different database
    incarnation than the one originally stamped. The human reviewing the run
    must see that, not just the proposal.
    """
    effects: list[str] = []
    for name in names:
        if name in proposals:
            continue
        target = resolvable(name, databases)
        if target is not None and target not in effects and target not in proposals:
            effects.append(target)
    return effects


def cell(view: DbView) -> str:
    if not view.seen:
        return "?"
    if view.item_count <= 0 or view.present_count <= 0:
        return "-"
    pct = round(100 * view.ratio)
    mark = "S" if view.stamped else ("=" if view.full else " ")
    return f"{view.present_count}/{view.item_count} {pct}%{mark}"


def action_text(plan: BoardPlan, databases: list[str]) -> str:
    if not plan.proposals:
        return plan.reason
    text = "stamp " + ", ".join(plan.proposals)
    effects = resolve_side_effects(plan.payload_names, plan.proposals, databases)
    if effects:
        text += "; also re-associates: " + ", ".join(effects)
    return text


def render_table(plans: list[BoardPlan], databases: list[str]) -> list[str]:
    headers = ["id", "board", "items"] + [clean(db) for db in databases] + ["stamps", "action"]
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
                clean(plan.name or "(unnamed)")[:32],
                str(plan.item_count),
                *cells,
                clean(", ".join(plan.stamped_names)) or "-",
                clean(action_text(plan, databases)),
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
                print(f"  #{plan.board_id}: {clean(plan.reason)}")


# ---------------------------------------------------------------------------
# Apply


def merge_names(existing: list[str], additions: list[str]) -> list[str]:
    names = list(existing)
    for name in additions:
        if name not in names:
            names.append(name)
    return names


def describe_write(board_id: int, target: str, names: list[str], databases: list[str]) -> str:
    effects = resolve_side_effects(names, [target], databases)
    suffix = ("; also re-associates: " + ", ".join(effects)) if effects else ""
    return (
        f"PUT /api/pinboards/{board_id}/databases?index_db={target}"
        f"  {json.dumps({'databases': names})}   (+{target}{suffix})"
    )


def verify_write(result: dict | None, target: str) -> str | None:
    """None if the write took, else why it did not.

    A 200 is not proof: the endpoint's carry half always succeeds, while the
    resolve half - the part that actually stamps `target` - is skipped when the
    instance has no identity to sign with or the target database has no
    identity row yet. That combination returns 200 with the board's stamps
    unchanged, which would otherwise read as success and, worse, be
    indistinguishable from it on the next dry run.
    """
    rows = (result or {}).get("databases") or []
    for row in rows:
        name = row.get("name")
        if isinstance(name, str) and name.casefold() == target.casefold() and row.get("associated"):
            return None
    named = [row.get("name") for row in rows if isinstance(row.get("name"), str)]
    if any(name.casefold() == target.casefold() for name in named):
        return (
            f"the response lists {target} but does not count it as associated - "
            "the name was carried, nothing was stamped (no instance identity, "
            "or this database has no identity row yet: is it migrated?)"
        )
    return (
        f"the response does not mention {target} at all (stamps: "
        f"{', '.join(named) or 'none'}) - nothing was written"
    )


def apply_plans(
    base: str,
    plans: list[BoardPlan],
    databases: list[str],
    user_data_db: str | None,
    user: str,
    timeout: float,
    dry: bool,
) -> int:
    """One PUT per (board, proposal). Returns the number of failed boards.

    Per proposal rather than per board so every write is verifiable: the
    response's per-row `associated` is relative to the database the request ran
    against, so a single request could only ever prove its own `index_db`. Each
    request also names **only** the target among the proposals - a sibling
    proposal that turns out to be unresolvable would otherwise 400 the whole
    payload and take the good write down with it.
    """
    failures = 0
    # Fresh stamps per target database, fetched once at the first write to that
    # database and reused for the rest of the pass. One fetch is enough: within
    # this run the only writer is us, a board's own earlier writes are already
    # folded in through `applied`, and a write to one board cannot change
    # another's rows. An external save landing mid-pass is a race no amount of
    # re-reading closes - it is only ever narrowed - and the list endpoint is
    # the cheap, activity-free way to narrow it.  None = the fetch failed.
    fresh: dict[str, dict[int, list[str]] | None] = {}
    fresh_error: dict[str, ApiError] = {}

    def current_stamps(target: str) -> dict[int, list[str]] | None:
        if target not in fresh:
            try:
                fresh[target] = fetch_current_stamps(base, target, user_data_db, user, timeout)
            except ApiError as err:
                fresh[target] = None
                fresh_error[target] = err
        return fresh[target]

    for plan in plans:
        applied: list[str] = []
        for target in plan.proposals:
            if dry:
                names = merge_names(plan.stamped_names, applied + [target])
                print(describe_write(plan.board_id, target, names, databases))
                applied.append(target)
                continue
            # Re-read the stamps before writing: the PUT replaces, so anything
            # saved since the opening snapshot would be dropped if the payload
            # were rebuilt from it.
            stamps = current_stamps(target)
            if stamps is None:
                failures += 1
                print(f"PUT /api/pinboards/{plan.board_id}/databases: SKIPPED")
                print(f"  could not re-read the current stamps in {target}: {fresh_error[target]}")
                print("  not writing from the opening snapshot - that would risk dropping a stamp")
                break
            if plan.board_id not in stamps:
                failures += 1
                print(f"PUT /api/pinboards/{plan.board_id}/databases: SKIPPED")
                print(f"  the board is no longer in {target}'s listing (deleted since this run started?)")
                break
            names = merge_names(merge_names(stamps[plan.board_id], plan.stamped_names), applied + [target])
            print(describe_write(plan.board_id, target, names, databases))
            try:
                # Written through the database being stamped for: that name
                # resolves off the open connection even when its file probe
                # comes back Unknown.
                result = api_call(
                    base,
                    f"/api/pinboards/{plan.board_id}/databases",
                    {"index_db": target, "user_data_db": user_data_db, "user": user},
                    body={"databases": names},
                    method="PUT",
                    timeout=timeout,
                )
            except ApiError as err:
                # One board's failure must never abort the run.
                failures += 1
                print(f"  FAILED: {err}")
                break
            problem = verify_write(result, target)
            stamped = ", ".join(clean(str(row.get("name", "?"))) for row in (result or {}).get("databases") or [])
            if problem:
                failures += 1
                print(f"  FAILED: {problem}")
                print(f"  response: associated={bool((result or {}).get('associated'))} databases=[{stamped}]")
                break
            print(f"  ok: associated={bool((result or {}).get('associated'))} databases=[{stamped}]")
            applied.append(target)
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
        # 8: stamped under another spelling of a live database ("Drawings"),
        #    which the server would resolve case-insensitively - so the write
        #    for photos also re-associates drawings, and the run must say so.
        {"id": 8, "name": "restamp", "item_count": 10, "present_count": 9, "associated": False,
         "databases": [{"name": "Drawings", "last_stamped": 10, "associated": False}]},
        # 9: a name carrying control characters - user data, and it must not
        #    shear the table apart.
        {"id": 9, "name": "line1\nline2\ttab", "item_count": 4, "present_count": 3, "associated": False,
         "databases": []},
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
        {"id": 8, "name": "restamp", "item_count": 10, "present_count": 0, "associated": False,
         "databases": [{"name": "Drawings", "last_stamped": 10, "associated": False}]},
        {"id": 9, "name": "line1\nline2\ttab", "item_count": 4, "present_count": 0, "associated": False,
         "databases": []},
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
        {"id": 8, "name": "restamp", "item_count": 10, "present_count": 0, "associated": False,
         "databases": [{"name": "Drawings", "last_stamped": 10, "associated": False}]},
        {"id": 9, "name": "line1\nline2\ttab", "item_count": 4, "present_count": 0, "associated": False,
         "databases": []},
    ],
}


def transport_checks() -> list[str]:
    """The apply loop against a deliberately broken gateway.

    Everything here is a *transport* fault rather than an API error, which is
    the case that used to escape as a traceback and take the rest of the run
    (and the summary) with it. A local stub is the only way to produce a
    dropped connection or a non-JSON 200 honestly, so the self-test spins one.
    """
    import contextlib
    import http.server
    import io as _io
    import threading

    problems: list[str] = []
    # A stamp that exists only in the *fresh* listing, never in the plans'
    # snapshot: the payload must contain it, or the write would delete it.
    saved_meanwhile = "savedmeanwhile"
    sent: list[tuple[int, list[str]]] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):  # keep the self-test output clean
            pass

        def board_id(self) -> int:
            return int(self.path.split("/api/pinboards/")[1].split("/")[0].split("?")[0])

        def send_json(self, code: int, body) -> None:
            raw = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):  # the pre-write re-read, over the list endpoint
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if query.get("index_db") == ["locked"]:
                return self.send_json(500, {"detail": "database is locked"})
            self.send_json(
                200,
                {
                    "pinboards": [
                        {
                            "id": board,
                            "item_count": 10,
                            "present_count": 9,
                            "associated": False,
                            "databases": [
                                {"name": "oldcam", "associated": False},
                                {"name": saved_meanwhile, "associated": False},
                            ],
                        }
                        for board in (101, 102, 103, 104, 106, 107)
                    ]
                },
            )

        def do_PUT(self):
            board = self.board_id()
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
            sent.append((board, body["databases"]))
            if board == 102:  # connection dropped mid-request
                self.close_connection = True
                return
            if board == 103:  # a 200 that is not JSON
                raw = b"<html>proxy says no</html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                return self.wfile.write(raw)
            if board == 104:  # carried, but nothing stamped
                return self.send_json(200, {"associated": False, "databases": [{"name": "photos", "associated": False}]})
            written = query.get("index_db", [""])[0]
            self.send_json(
                200,
                {
                    "associated": True,
                    "databases": [{"name": name, "associated": name == written} for name in body["databases"]],
                },
            )

    try:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    except OSError as err:
        # Some sandboxes refuse listening sockets; that is not a tool defect,
        # but it must be said out loud rather than pass silently.
        return [f"could not bind the stub gateway, transport checks did not run: {err}"]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    def plan(board_id: int, *targets: str) -> BoardPlan:
        targets = targets or ("photos",)
        return BoardPlan(
            board_id=board_id,
            name=f"board {board_id}",
            item_count=10,
            views=[DbView(db=t, present_count=9, item_count=10, stamped=False) for t in targets],
            stamped_names=["oldcam"],
            proposals=list(targets),
            category=CAT_STAMP,
        )

    # 101 fine, 102 dropped connection, 103 non-JSON 200, 104 carry-only 200,
    # 105 aimed at a database whose listing cannot be read, 106 fine again -
    # that one is the point: the run must reach it. 107 has two proposals, to
    # pin down that a write names only its own target among them.
    plans = [
        plan(101), plan(102), plan(103), plan(104), plan(105, "locked"), plan(106),
        plan(107, "photos", "photos2"),
    ]
    captured = _io.StringIO()
    try:
        with contextlib.redirect_stdout(captured):
            failed = apply_plans(base, plans, ["photos", "photos2", "locked"], None, "user", 10.0, dry=False)
    except Exception as err:  # the whole point: nothing may escape
        return [f"apply_plans raised {type(err).__name__}: {err}"]
    finally:
        server.shutdown()
        server.server_close()
    out = captured.getvalue()

    if failed != 4:
        problems.append(f"expected 4 failed boards, got {failed}")
    if out.count("  ok: ") != 4:  # 101, 106, and both of 107's targets
        problems.append(f"expected 4 successful writes, got {out.count('  ok: ')}")
    if "/api/pinboards/106/databases" not in out:
        problems.append("the run did not reach the last board after the failures")
    if "SKIPPED" not in out:
        problems.append("an unreadable pre-write re-read did not skip the board")
    if "not JSON" not in out:
        problems.append("a non-JSON 200 was not reported as such")
    if "carried, nothing was stamped" not in out:
        problems.append("a carry-only 200 was not caught by verification")
    # The union is what makes the write non-destructive: a name known only to
    # the fresh listing has to survive into the payload.
    first = next((names for board, names in sent if board == 101), None)
    if first is None:
        problems.append("no write was recorded for the first board")
    elif saved_meanwhile not in first:
        problems.append(f"the payload dropped a stamp saved since the snapshot: {first}")
    elif not {"oldcam", "photos"} <= set(first):
        problems.append(f"the payload lost the snapshot's own names: {first}")
    if any(board == 105 for board, _ in sent):
        problems.append("a board was written even though its fresh listing could not be read")
    # Per-target isolation: the first of two proposals must not carry the
    # second, or one unresolvable name would 400 the write for a good one.
    multi = [names for board, names in sent if board == 107]
    if len(multi) != 2:
        problems.append(f"expected 2 writes for the two-proposal board, got {len(multi)}")
    else:
        if "photos2" in multi[0]:
            problems.append(f"the first write carried the sibling proposal: {multi[0]}")
        if "photos" not in multi[0]:
            problems.append(f"the first write did not name its own target: {multi[0]}")
        if not {"photos", "photos2"} <= set(multi[1]):
            problems.append(f"the second write lost the first target: {multi[1]}")
    return problems


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
        [p.board_id for p in replay.values() if p.category == CAT_STAMP] == [7, 8, 9],
        "the replay changed a board other than the one that was applied",
    )

    # Sending a stored name also re-resolves it, so a stamp under another
    # spelling of a live database is a second thing the write touches: the
    # report must disclose it rather than only naming the proposal.
    check(plans[8].category == CAT_STAMP, f"board 8 category {plans[8].category}")
    check(plans[8].payload_names == ["Drawings", "photos"], f"board 8 payload {plans[8].payload_names}")
    check(
        resolve_side_effects(plans[8].payload_names, plans[8].proposals, FIXTURE_DATABASES) == ["drawings"],
        "board 8 side effect not detected",
    )
    check(
        "also re-associates: drawings" in action_text(plans[8], FIXTURE_DATABASES),
        f"board 8 action text {action_text(plans[8], FIXTURE_DATABASES)!r}",
    )
    check(
        "also re-associates: drawings" in describe_write(8, "photos", plans[8].payload_names, FIXTURE_DATABASES),
        "board 8 write line does not disclose the side effect",
    )
    # A name that resolves to nothing local is carry-only: no side effect.
    check(
        resolve_side_effects(plans[7].payload_names, plans[7].proposals, FIXTURE_DATABASES) == [],
        "board 7 (retired database) reported a phantom side effect",
    )

    # Control characters in a board name must not shear the table.
    rendered = render_table([plans[9]], FIXTURE_DATABASES)
    check(all(CONTROL_CHARS.search(line) is None for line in rendered), "control characters reached the table")
    check(len({len(line.rstrip()) for line in rendered}) <= len(rendered), "table rows are not aligned")

    # A board missing from one listing must not be read as zero overlap - and
    # must lose its unambiguous status, because "exactly one candidate" only
    # means anything when every database was actually checked.
    partial = {db: list(boards) for db, boards in FIXTURE_LISTINGS.items()}
    partial["drawings"] = [b for b in partial["drawings"] if b["id"] != 1]
    missing = {p.board_id: p for p in compute_plans(partial, FIXTURE_DATABASES, 0.5)}
    check(any(not v.seen for v in missing[1].views), "missing listing not marked unseen")
    check(missing[1].proposals == ["photos"], f"board 1 proposals with a gap {missing[1].proposals}")
    check(missing[1].category == CAT_REVIEW, f"board 1 with a gap is {missing[1].category}, not review")
    check("drawings" in missing[1].reason, f"board 1 gap reason {missing[1].reason!r} does not name drawings")

    # The threshold controls what is *proposed*, nothing else, and 1.0 could
    # never propose anything (ratio 1.0 is clause (c), which needs no stamp).
    strict = {p.board_id: p for p in compute_plans(FIXTURE_LISTINGS, FIXTURE_DATABASES, 0.9)}
    check(strict[1].category == CAT_NONE, f"board 1 at threshold 0.9 is {strict[1].category}")
    check(threshold_error(0.5) is None, "0.5 rejected")
    check(threshold_error(1.0) is not None, "threshold 1.0 accepted")
    check(threshold_error(0.0) is not None, "threshold 0.0 accepted")

    # A 200 is not proof the stamp landed.
    ok = {"databases": [{"name": "photos", "associated": True}]}
    carried = {"databases": [{"name": "photos", "associated": False}]}
    nothing = {"databases": [{"name": "oldcam", "associated": False}]}
    check(verify_write(ok, "photos") is None, "a good write was reported as failed")
    check(verify_write(ok, "PHOTOS") is None, "verification is case-sensitive")
    check(verify_write(carried, "photos") is not None, "carry-only 200 passed verification")
    check(verify_write(nothing, "photos") is not None, "empty 200 passed verification")
    check(verify_write(None, "photos") is not None, "a bodyless 200 passed verification")

    failures.extend(transport_checks())

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


def threshold_error(value: float) -> str | None:
    """Why `--threshold` is unusable, if it is.

    1.0 is rejected rather than clamped: a ratio of 1.0 is full overlap, which
    clause (c) already admits with no stamp, so a threshold of 1.0 could never
    propose anything and would silently look like "nothing to do".
    """
    if not value > 0.0:
        return "--threshold must be greater than 0 (0 would propose every incidental overlap)"
    if value >= 1.0:
        return (
            "--threshold must be below 1.0: 100% overlap is clause (c), which needs no stamp, "
            "so 1.0 can never propose anything"
        )
    return None


def main() -> int:
    # Board and database names are user data; a cp1252 console would otherwise
    # raise on a name with a dash in it. Replacement keeps names readable
    # instead of forcing everything down to ASCII.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):  # already wrapped, or not a TextIO
            pass

    args = parse_args()
    if args.self_test:
        return self_test()
    problem = threshold_error(args.threshold)
    if problem:
        print(problem, file=sys.stderr)
        return 2

    try:
        databases = fetch_index_databases(args.api_url, args.timeout)
    except ApiError as err:
        print(f"gateway request failed: {err}", file=sys.stderr)
        print("Is the gateway running at --api-url?", file=sys.stderr)
        return 1

    # A listing that fails is a database left *unchecked*, not a database with
    # no overlap: its boards show `?` and any board that would have been an
    # unambiguous proposal is demoted to NEEDS REVIEW.
    listings: dict[str, list[dict]] = {}
    unreadable: list[str] = []
    for db in databases:
        try:
            listings[db] = fetch_boards(args.api_url, db, args.user_data_db, args.user, args.timeout)
        except ApiError as err:
            unreadable.append(db)
            print(f"WARNING: could not list boards for index database {db!r}: {err}", file=sys.stderr)
    if not listings:
        print("no index database could be listed; nothing to do", file=sys.stderr)
        return 1
    if unreadable:
        print(f"WARNING: {', '.join(unreadable)} could not be checked - affected boards are held for review")
        print()

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
        apply_plans(args.api_url, writable, databases, args.user_data_db, args.user, args.timeout, dry=True)
        print()
        print("(each write re-reads the board's stamps first, so the payload sent may pick up")
        print(" any association saved since this listing was taken)")
        print("Re-run with --apply to write them.")
        return 0

    print(f"applying {len(writable)} board(s):")
    failures = apply_plans(args.api_url, writable, databases, args.user_data_db, args.user, args.timeout, dry=False)
    print()
    print(f"done: {len(writable) - failures} written, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
