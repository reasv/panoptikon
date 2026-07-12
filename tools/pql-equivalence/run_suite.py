#!/usr/bin/env python3
"""PQL equivalence suite: legacy Python vs Rust, same DB snapshot.

Runs a corpus of PQL queries through the legacy Python implementation
(in-process, imported from the python-legacy worktree) and through the Rust
server (spawned with readonly=true, HTTP), against the same database
snapshot, then diffs counts, result rows, and ordering.

One-time validation, not CI. See README.md for setup and usage.
"""

from __future__ import annotations

import argparse
import base64
import http.server
import io
import json
import math
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EMB_MARKER = "__EMB__:"


# ---------------------------------------------------------------------------
# CLI


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-folder",
        required=True,
        type=Path,
        help="Snapshot data folder (contains index/<db>/ and user_data/)",
    )
    p.add_argument("--index-db", default="default")
    p.add_argument("--user-data-db", default="default")
    p.add_argument(
        "--rust-bin",
        type=Path,
        default=REPO_ROOT
        / "target"
        / "release"
        / ("panoptikon.exe" if sys.platform == "win32" else "panoptikon"),
    )
    p.add_argument(
        "--legacy-src",
        type=Path,
        default=REPO_ROOT / "python-legacy" / "src",
        help="src/ dir of the python-legacy worktree",
    )
    p.add_argument("--port", type=int, default=6345)
    p.add_argument(
        "--out", type=Path, default=Path(__file__).parent / "report.json"
    )
    p.add_argument(
        "--prepare",
        action="store_true",
        help="Boot the Rust server writable first to migrate the snapshot "
        "and create missing DBs (MUTATES the data folder), then run the "
        "suite readonly",
    )
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--float-rtol", type=float, default=1e-4)
    p.add_argument("--float-atol", type=float, default=1e-6)
    p.add_argument(
        "--only", default=None, help="Only run cases whose name contains this"
    )
    p.add_argument(
        "--timeout", type=float, default=600.0, help="Per-query timeout (s)"
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Rust gateway management


def write_config(
    scratch: Path,
    args: argparse.Namespace,
    readonly: bool,
    inference_url: str | None = None,
) -> Path:
    data_folder = args.data_folder.resolve().as_posix()
    inference_upstream = (
        f'\n[[upstreams.inference]]\nbase_url = "{inference_url}"\n'
        if inference_url
        else ""
    )
    cfg = f"""\
data_folder = "{data_folder}"
readonly = {"true" if readonly else "false"}

[server]
host = "127.0.0.1"
port = {args.port}

[upstreams.ui]
base_url = "http://127.0.0.1:{args.port + 1}"
local = false

[upstreams.api]
base_url = "http://127.0.0.1:{args.port}"
local = true
{inference_upstream}
[inference_local]
enabled = false

[inference_local.python_env]
auto_setup = false

[rulesets.allow_all]
allow_all = true

[[policies]]
name = "localhost"
ruleset = "allow_all"

[policies.match]
hosts = ["localhost", "127.0.0.1"]

[policies.index_db]
default = "{args.index_db}"
allow = "*"

[policies.user_data_db]
default = "{args.user_data_db}"
allow = "*"
"""
    path = scratch / ("suite-ro.toml" if readonly else "suite-rw.toml")
    path.write_text(cfg, encoding="utf-8")
    return path


class Gateway:
    def __init__(self, rust_bin: Path, config: Path, scratch: Path, port: int):
        self.port = port
        self.log_path = scratch / f"gateway-{config.stem}.log"
        self.log_file = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [str(rust_bin), "--config", str(config)],
            cwd=str(scratch),
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )

    def wait_ready(self, timeout: float = 90.0) -> None:
        deadline = time.monotonic() + timeout
        url = f"http://127.0.0.1:{self.port}/api/client-config"
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    if resp.status == 200:
                        return
            except (urllib.error.URLError, ConnectionError, OSError):
                pass
            time.sleep(0.5)
        self.stop()
        tail = ""
        try:
            tail = self.log_path.read_text(encoding="utf-8", errors="replace")
            tail = "\n".join(tail.splitlines()[-40:])
        except OSError:
            pass
        raise RuntimeError(
            f"Rust gateway did not become ready on port {self.port}.\n"
            f"Log tail ({self.log_path}):\n{tail}"
        )

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=15)
        self.log_file.close()


# ---------------------------------------------------------------------------
# Inference metadata stub
#
# image_embeddings and similar_to resolve per-model `distance_func` overrides
# from the inference server's metadata endpoint on both sides (Rust over
# HTTP, legacy Python via get_model_metadata -> HTTP). The suite runs with
# no inference server, so:
#   - the Rust gateway gets a [[upstreams.inference]] pointing at this stub,
#     which serves the discovered model groups/ids with NO distance_func --
#     the production config defines distance_func for none of the embedding
#     models the corpus can discover, so "no override" is production-faithful;
#   - the legacy side gets get_distance_func_override patched to return None
#     (its real implementation drags in panoptikon.data_extractors.models,
#     whose import chain needs packages absent from the suite venv, and would
#     then call the inference server anyway).
# Both sides therefore use the query's declared distance function unchanged.


def build_stub_metadata(d: dict) -> dict:
    meta: dict = {}
    for setter, _dim in d["text_emb"] + d["clip_emb"]:
        group, inference_id = setter.split("/", 1)
        entry = meta.setdefault(
            group, {"group_metadata": {}, "inference_ids": {}}
        )
        entry["inference_ids"][inference_id] = {}
    return meta


class _MetadataStubHandler(http.server.BaseHTTPRequestHandler):
    metadata: dict = {}

    def do_GET(self):  # noqa: N802
        if self.path.rstrip("/").endswith("/metadata"):
            body = json.dumps(self.metadata).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, *args):  # silence per-request stderr noise
        pass


def start_metadata_stub(metadata: dict) -> tuple[http.server.ThreadingHTTPServer, str]:
    handler = type(
        "MetadataStubHandler", (_MetadataStubHandler,), {"metadata": metadata}
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def install_legacy_distance_override_stub() -> None:
    from panoptikon.db.pql.filters.sortable import (
        image_embeddings,
        item_similarity,
        utils,
    )

    def _no_override(model_name):  # matches production config: no override
        return None

    utils.get_distance_func_override = _no_override
    item_similarity.get_distance_func_override = _no_override
    image_embeddings.get_distance_func_override = _no_override


def http_json(
    url: str, body: dict | None = None, timeout: float = 600.0
) -> dict:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST" if body is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"HTTP {e.code} from {url}: {detail}") from e


def prepare_databases(args: argparse.Namespace, scratch: Path) -> None:
    print("[prepare] booting the Rust server writable to migrate the snapshot")
    cfg = write_config(scratch, args, readonly=False)
    gw = Gateway(args.rust_bin, cfg, scratch, args.port)
    try:
        gw.wait_ready()
        qs = urllib.parse.urlencode(
            {
                "new_index_db": args.index_db,
                "new_user_data_db": args.user_data_db,
            }
        )
        http_json(
            f"http://127.0.0.1:{args.port}/api/db/create?{qs}", body={}
        )
        print("[prepare] databases created/migrated")
    finally:
        gw.stop()


# ---------------------------------------------------------------------------
# Legacy Python side


def install_inferio_stub(legacy_src: Path) -> None:
    """The legacy top-level inferio/__init__.py imports the FastAPI inference
    router (fastapi_utilities and friends), which the search path never uses.
    Register a bare package in its place so submodule imports like
    inferio.impl.utils still resolve to the real files without executing it."""
    import types

    if "inferio" not in sys.modules:
        pkg = types.ModuleType("inferio")
        pkg.__path__ = [str((legacy_src / "inferio").resolve())]
        sys.modules["inferio"] = pkg


def open_legacy_conn(args: argparse.Namespace) -> sqlite3.Connection:
    sys.path.insert(0, str(args.legacy_src.resolve()))
    install_inferio_stub(args.legacy_src)
    import sqlite_vec  # noqa: F401  (from the suite venv)

    data = args.data_folder.resolve()
    index_db = data / "index" / args.index_db / "index.db"
    storage_db = data / "index" / args.index_db / "storage.db"
    user_db = data / "user_data" / f"{args.user_data_db}.db"
    for f in (index_db, storage_db, user_db):
        if not f.exists():
            raise FileNotFoundError(
                f"{f} missing — pass --prepare to create/migrate the snapshot"
            )
    conn = sqlite3.connect(f"file:{index_db.as_posix()}?mode=ro", uri=True)
    conn.execute(
        f"ATTACH DATABASE 'file:{storage_db.as_posix()}?mode=ro' AS storage"
    )
    conn.execute(
        f"ATTACH DATABASE 'file:{user_db.as_posix()}?mode=ro' AS user_data"
    )
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA case_sensitive_like = ON")
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn


def inject_embeddings(node, b64_to_bytes: dict[str, bytes]) -> None:
    """Walk a validated legacy PQLQuery element tree and pre-set embeddings
    so preprocess never calls the inference server."""
    if node is None:
        return
    cls = type(node).__name__
    if cls == "AndOperator":
        for child in node.and_:
            inject_embeddings(child, b64_to_bytes)
    elif cls == "OrOperator":
        for child in node.or_:
            inject_embeddings(child, b64_to_bytes)
    elif cls == "NotOperator":
        inject_embeddings(node.not_, b64_to_bytes)
    elif cls in ("SemanticTextSearch", "SemanticImageSearch"):
        emb_args = (
            node.text_embeddings
            if cls == "SemanticTextSearch"
            else node.image_embeddings
        )
        raw = b64_to_bytes.get(emb_args.query)
        if raw is not None:
            emb_args._embedding = raw
            node.set_validated(True)


def run_python_query(
    conn: sqlite3.Connection, query: dict, b64_to_bytes: dict[str, bytes]
) -> dict:
    from panoptikon.db.pql.pql_model import PQLQuery
    from panoptikon.db.pql.search import search_pql

    q = PQLQuery.model_validate(query)
    inject_embeddings(q.query, b64_to_bytes)
    gen, count, _rm, _cm = search_pql(conn, q)
    results = [r.model_dump(mode="json", exclude_none=True) for r in gen]
    return {"count": count, "results": results}


# ---------------------------------------------------------------------------
# Rust side


def run_rust_query(args: argparse.Namespace, query: dict) -> dict:
    qs = urllib.parse.urlencode(
        {"index_db": args.index_db, "user_data_db": args.user_data_db}
    )
    return http_json(
        f"http://127.0.0.1:{args.port}/api/search/pql?{qs}",
        body=query,
        timeout=args.timeout,
    )


# ---------------------------------------------------------------------------
# Discovery: sample real values from the snapshot to parameterize the corpus


def discover(conn: sqlite3.Connection) -> dict:
    d: dict = {
        "tags": [],
        "tag_namespaces": [],
        "tag_setters": [],
        "text_setters": [],
        "text_word": None,
        "languages": [],
        "text_emb": [],  # [(setter, dim)]
        "clip_emb": [],  # [(setter, dim)]
        "types": [],
        "median_size": None,
        "sample_sha256": None,
        "path_fragment": None,
        "bookmark_users": [],
        "bookmark_namespaces": [],
        "similar_targets": {},  # setter -> sha256
    }

    def rows(sql: str, params=()):
        try:
            return conn.execute(sql, params).fetchall()
        except sqlite3.Error:
            return []

    d["tags"] = [
        r[1]
        for r in rows(
            "SELECT t.namespace, t.name, COUNT(*) c FROM tags t "
            "JOIN tags_items ti ON ti.tag_id = t.id "
            "GROUP BY t.id ORDER BY c DESC LIMIT 10"
        )
    ]
    d["tag_namespaces"] = [
        r[0] for r in rows("SELECT DISTINCT namespace FROM tags LIMIT 5")
    ]

    setters = rows(
        "SELECT DISTINCT s.name, d.data_type FROM setters s "
        "JOIN item_data d ON d.setter_id = s.id"
    )
    d["tag_setters"] = [s for s, t in setters if t == "tags"]
    d["text_setters"] = [s for s, t in setters if t == "text"]
    emb_setters = [(s, t) for s, t in setters if t in ("text-embedding", "clip")]
    for setter, dtype in emb_setters:
        r = rows(
            "SELECT length(e.embedding) FROM embeddings e "
            "JOIN item_data d ON d.id = e.id "
            "JOIN setters s ON s.id = d.setter_id WHERE s.name = ? LIMIT 1",
            (setter,),
        )
        if r:
            dim = r[0][0] // 4
            key = "text_emb" if dtype == "text-embedding" else "clip_emb"
            d[key].append((setter, dim))
        t = rows(
            "SELECT i.sha256 FROM items i "
            "JOIN item_data dd ON dd.item_id = i.id "
            "JOIN embeddings e ON e.id = dd.id "
            "JOIN setters s ON s.id = dd.setter_id "
            "WHERE s.name = ? LIMIT 1",
            (setter,),
        )
        if t:
            d["similar_targets"][setter] = t[0][0]

    d["types"] = [r[0] for r in rows("SELECT DISTINCT type FROM items LIMIT 5")]
    sizes = [
        r[0]
        for r in rows(
            "SELECT size FROM items WHERE size IS NOT NULL "
            "ORDER BY size LIMIT 1 OFFSET (SELECT COUNT(*)/2 FROM items)"
        )
    ]
    d["median_size"] = sizes[0] if sizes else None
    sample = rows("SELECT i.sha256, f.filename FROM items i JOIN files f ON f.item_id = i.id LIMIT 1")
    if sample:
        d["sample_sha256"] = sample[0][0]
        stem = Path(sample[0][1]).stem
        frag = "".join(c if c.isalnum() else " " for c in stem).split()
        d["path_fragment"] = frag[0] if frag else None

    for row in rows("SELECT text FROM extracted_text LIMIT 20"):
        words = [
            w
            for w in "".join(
                c if c.isalnum() else " " for c in (row[0] or "")
            ).split()
            if len(w) >= 4 and w.isascii()
        ]
        if words:
            d["text_word"] = words[0]
            break
    d["languages"] = [
        r[0]
        for r in rows(
            "SELECT DISTINCT language FROM extracted_text "
            "WHERE language IS NOT NULL LIMIT 3"
        )
    ]
    d["bookmark_users"] = [
        r[0] for r in rows("SELECT DISTINCT user FROM user_data.bookmarks LIMIT 3")
    ]
    d["bookmark_namespaces"] = [
        r[0]
        for r in rows(
            "SELECT DISTINCT namespace FROM user_data.bookmarks LIMIT 3"
        )
    ]
    return d


def make_embedding(key: str, dim: int) -> tuple[str, bytes]:
    """Deterministic pseudo-embedding for a marker key: (b64 npy, raw f32)."""
    import numpy as np

    seed = int.from_bytes(key.encode("utf-8")[:8].ljust(8, b"\0"), "little")
    rng = np.random.default_rng(seed)
    arr = rng.standard_normal(dim).astype(np.float32)
    arr /= np.linalg.norm(arr) or 1.0
    buf = io.BytesIO()
    np.save(buf, arr)
    return base64.b64encode(buf.getvalue()).decode("ascii"), arr.tobytes()


# ---------------------------------------------------------------------------
# Corpus


def build_corpus(d: dict, page_size: int) -> list[dict]:
    """Returns [{name, query, requires_missing (reason or None)}]."""

    TIEBREAK = {"order_by": "file_id", "order": "asc", "priority": 0}

    def base(query=None, **kw) -> dict:
        q = {
            "query": query,
            "order_by": [
                {"order_by": "last_modified", "order": "desc", "priority": 0},
                dict(TIEBREAK),
            ],
            "select": ["sha256", "path", "last_modified", "type"],
            "entity": "file",
            "page": 1,
            "page_size": page_size,
            "count": True,
            "results": True,
            "check_path": False,
        }
        q.update(kw)
        return q

    cases: list[dict] = []

    def case(name: str, query: dict | None, missing: str | None = None):
        cases.append(
            {"name": name, "query": query, "requires_missing": missing}
        )

    tag = d["tags"][0] if d["tags"] else None
    tag2 = d["tags"][1] if len(d["tags"]) > 1 else tag
    no_tags = None if tag else "no tags in snapshot"
    no_text = None if d["text_setters"] and d["text_word"] else "no extracted text"
    no_temb = None if d["text_emb"] else "no text embeddings"
    no_cemb = None if d["clip_emb"] else "no clip embeddings"
    no_bm = None if d["bookmark_users"] else "no bookmarks"

    # --- plain queries, ordering, paging, projection
    case("defaults_explicit", base())
    case(
        "order_path_asc",
        base(order_by=[{"order_by": "path", "order": "asc", "priority": 0}]),
    )
    case(
        "order_size_desc_tiebreak",
        base(
            order_by=[
                {"order_by": "size", "order": "desc", "priority": 0},
                dict(TIEBREAK),
            ]
        ),
    )
    case(
        "order_no_tiebreak",
        base(order_by=[{"order_by": "last_modified", "order": "desc", "priority": 0}]),
    )
    case(
        "select_all_file_columns",
        base(
            select=[
                "file_id", "sha256", "path", "filename", "last_modified",
                "item_id", "md5", "type", "size", "width", "height",
                "duration", "time_added", "audio_tracks", "video_tracks",
                "subtitle_tracks", "blurhash",
            ]
        ),
    )
    case("paging_page2", base(page=2, page_size=7))
    case("count_only", base(results=False))
    case("results_only", base(count=False))
    case("partition_item_id", base(partition_by=["item_id"]))
    case("partition_type", base(partition_by=["type"]))
    case("check_path_true", base(check_path=True))

    # --- entity: text
    case(
        "entity_text_basic",
        base(
            entity="text",
            select=[
                "sha256", "path", "type", "data_id", "text", "setter_name",
                "confidence", "language", "text_length",
            ],
            order_by=[
                {"order_by": "data_id", "order": "asc", "priority": 0},
                dict(TIEBREAK),
            ],
        ),
        no_text,
    )
    case(
        "entity_text_partition_item",
        base(
            entity="text",
            select=["sha256", "path", "data_id", "setter_name"],
            partition_by=["item_id"],
            order_by=[
                {"order_by": "data_id", "order": "asc", "priority": 0},
                dict(TIEBREAK),
            ],
        ),
        no_text,
    )

    # --- match (attribute filter)
    ftype = d["types"][0] if d["types"] else None
    case(
        "match_eq_type",
        base({"match": {"eq": {"type": ftype}}}) if ftype else None,
        None if ftype else "no item types",
    )
    case(
        "match_in_types",
        base({"match": {"in_": {"type": d["types"][:2]}}})
        if len(d["types"]) >= 2
        else None,
        None if len(d["types"]) >= 2 else "fewer than 2 item types",
    )
    case(
        "match_gt_size",
        base({"match": {"gt": {"size": d["median_size"]}}})
        if d["median_size"] is not None
        else None,
        None if d["median_size"] is not None else "no sizes",
    )
    case(
        "match_sha256_no_limit",
        base(
            {"match": {"eq": {"sha256": d["sample_sha256"]}}},
            page_size=0,
        )
        if d["sample_sha256"]
        else None,
        None if d["sample_sha256"] else "no items",
    )
    if ftype and d["median_size"] is not None:
        case(
            "boolean_nesting",
            base(
                {
                    "and_": [
                        {"match": {"gt": {"size": 0}}},
                        {
                            "or_": [
                                {"match": {"eq": {"type": ftype}}},
                                {
                                    "not_": {
                                        "match": {
                                            "lte": {"size": d["median_size"]}
                                        }
                                    }
                                },
                            ]
                        },
                    ]
                }
            ),
        )

    # --- match_path
    frag = d["path_fragment"]
    case(
        "match_path_fragment",
        base(
            {
                "match_path": {
                    "match": frag,
                    "filename_only": False,
                    "raw_fts5_match": False,
                }
            }
        )
        if frag
        else None,
        None if frag else "no path fragment",
    )
    case(
        "match_path_filename_only",
        base(
            {
                "match_path": {
                    "match": frag,
                    "filename_only": True,
                    "raw_fts5_match": False,
                }
            }
        )
        if frag
        else None,
        None if frag else "no path fragment",
    )

    # --- match_text (FTS)
    word = d["text_word"]
    case(
        "match_text_basic",
        base(
            {
                "match_text": {
                    "match": word,
                    "filter_only": False,
                    "setters": [],
                    "languages": [],
                    "raw_fts5_match": False,
                }
            }
        )
        if word
        else None,
        no_text,
    )
    case(
        "match_text_ranked_snippet",
        base(
            {
                "match_text": {
                    "match": word,
                    "filter_only": False,
                    "setters": d["text_setters"][:1],
                    "languages": [],
                    "raw_fts5_match": False,
                    "select_snippet_as": "snippet",
                },
                "order_by": True,
                "direction": "asc",
                "priority": 1,
                "row_n": False,
                "select_as": "rank",
            }
        )
        if word
        else None,
        no_text,
    )

    # --- match_tags
    case(
        "match_tags_single",
        base(
            {
                "match_tags": {
                    "tags": [tag],
                    "match_any": True,
                    "min_confidence": 0.0,
                    "setters": [],
                    "namespaces": [],
                    "all_setters_required": False,
                }
            }
        )
        if tag
        else None,
        no_tags,
    )
    case(
        "match_tags_all",
        base(
            {
                "match_tags": {
                    "tags": [tag, tag2],
                    "match_any": False,
                    "min_confidence": 0.0,
                    "setters": [],
                    "namespaces": [],
                    "all_setters_required": False,
                }
            }
        )
        if tag and tag2
        else None,
        no_tags,
    )
    case(
        "match_tags_confidence_setter_ns",
        base(
            {
                "match_tags": {
                    "tags": [tag],
                    "match_any": True,
                    "min_confidence": 0.5,
                    "setters": d["tag_setters"][:1],
                    "namespaces": d["tag_namespaces"][:1],
                    "all_setters_required": False,
                }
            }
        )
        if tag and d["tag_setters"] and d["tag_namespaces"]
        else None,
        no_tags,
    )
    case(
        "not_tags",
        base({"not_": {"match_tags": {"tags": [tag], "match_any": True}}})
        if tag
        else None,
        no_tags,
    )

    # --- bookmarks
    bm_user = d["bookmark_users"][0] if d["bookmark_users"] else None
    case(
        "in_bookmarks_basic",
        base(
            {
                "in_bookmarks": {
                    "filter": True,
                    "namespaces": [],
                    "sub_ns": False,
                    "user": bm_user,
                    "include_wildcard": True,
                }
            }
        )
        if bm_user
        else None,
        no_bm,
    )
    case(
        "in_bookmarks_namespaced",
        base(
            {
                "in_bookmarks": {
                    "filter": True,
                    "namespaces": d["bookmark_namespaces"][:1],
                    "sub_ns": True,
                    "user": bm_user,
                    "include_wildcard": True,
                }
            }
        )
        if bm_user and d["bookmark_namespaces"]
        else None,
        no_bm,
    )

    # --- processing state
    setter = (d["tag_setters"] or d["text_setters"] or [None])[0]
    case(
        "processed_by",
        base({"processed_by": setter}) if setter else None,
        None if setter else "no setters",
    )
    case(
        "has_data_unprocessed",
        base({"has_data_unprocessed": {"setter_name": setter, "data_types": []}})
        if setter
        else None,
        None if setter else "no setters",
    )

    # --- semantic search (deterministic injected embeddings, no inference)
    def semantic_case(name, kind, setter, dim, extra_args=None, missing=None):
        if missing:
            case(name, None, missing)
            return
        marker = f"{EMB_MARKER}{setter}"
        field = "text_embeddings" if kind == "text" else "image_embeddings"
        inner = {
            "query": marker,
            "model": setter,
            "distance_aggregation": "MIN",
        }
        inner.update(extra_args or {})
        case(
            name,
            base(
                {
                    field: inner,
                    "order_by": True,
                    "direction": "asc",
                    "priority": 1,
                    "row_n": False,
                    "select_as": "distance",
                },
                page_size=50,
                order_by=[dict(TIEBREAK)],
            ),
        )

    t_setter, t_dim = d["text_emb"][0] if d["text_emb"] else (None, None)
    c_setter, c_dim = d["clip_emb"][0] if d["clip_emb"] else (None, None)
    semantic_case(
        "semantic_text_min", "text", t_setter, t_dim, missing=no_temb
    )
    semantic_case(
        "semantic_text_avg",
        "text",
        t_setter,
        t_dim,
        {"distance_aggregation": "AVG"},
        missing=no_temb,
    )
    semantic_case(
        "semantic_image", "image", c_setter, c_dim, missing=no_cemb
    )
    semantic_case(
        "semantic_image_xmodal",
        "image",
        c_setter,
        c_dim,
        {"clip_xmodal": True},
        missing=no_cemb,
    )

    # --- similar_to (uses stored embeddings only)
    sim_setter = t_setter if t_setter in d["similar_targets"] else (
        c_setter if c_setter in d["similar_targets"] else None
    )
    if sim_setter:
        sim_kind_l2 = {
            "similar_to": {
                "target": d["similar_targets"][sim_setter],
                "model": sim_setter,
                "distance_function": "L2",
                "distance_aggregation": "MIN",
                "setters": [],
                "clip_xmodal": False,
                "xmodal_t2t": True,
                "xmodal_i2i": True,
            },
            "order_by": True,
            "direction": "asc",
            "priority": 1,
            "row_n": False,
            "select_as": "distance",
        }
        case(
            "similar_to_l2",
            base(sim_kind_l2, page_size=50, order_by=[dict(TIEBREAK)]),
        )
        sim_cos = json.loads(json.dumps(sim_kind_l2))
        sim_cos["similar_to"]["distance_function"] = "COSINE"
        case(
            "similar_to_cosine",
            base(sim_cos, page_size=50, order_by=[dict(TIEBREAK)]),
        )
    else:
        case("similar_to_l2", None, "no embeddings with a target item")
        case("similar_to_cosine", None, "no embeddings with a target item")

    # --- RRF hybrid ranking (regression: integer division bug class)
    if word and t_setter:
        case(
            "rrf_text_plus_semantic",
            base(
                {
                    "and_": [
                        {
                            "match_text": {
                                "match": word,
                                "filter_only": False,
                                "setters": [],
                                "languages": [],
                                "raw_fts5_match": False,
                            },
                            "order_by": True,
                            "direction": "desc",
                            "priority": 1,
                            "row_n": True,
                            "row_n_direction": "asc",
                            "rrf": {"k": 60, "weight": 1.0},
                        },
                        {
                            "text_embeddings": {
                                "query": f"{EMB_MARKER}{t_setter}",
                                "model": t_setter,
                                "distance_aggregation": "MIN",
                            },
                            "order_by": True,
                            "direction": "desc",
                            "priority": 1,
                            "row_n": True,
                            "row_n_direction": "asc",
                            "rrf": {"k": 60, "weight": 1.0},
                        },
                    ]
                },
                page_size=50,
                order_by=[dict(TIEBREAK)],
            ),
        )
    else:
        case(
            "rrf_text_plus_semantic",
            None,
            "needs both extracted text and text embeddings",
        )

    # --- sortable cursor (gt on rank output)
    case(
        "cursor_gt_confidence",
        base(
            {
                "match_tags": {
                    "tags": [tag],
                    "match_any": True,
                    "min_confidence": 0.0,
                    "setters": [],
                    "namespaces": [],
                    "all_setters_required": False,
                },
                "order_by": True,
                "direction": "desc",
                "priority": 1,
                "row_n": False,
                "select_as": "confidence_rank",
                "gt": 0.3,
            }
        )
        if tag
        else None,
        no_tags,
    )

    return cases


def collect_embeddings(cases: list[dict], d: dict) -> tuple[dict, dict]:
    """Scan corpus for EMB markers; return (marker->b64, b64->raw bytes)."""
    dims = {s: dim for s, dim in d["text_emb"] + d["clip_emb"]}
    marker_to_b64: dict[str, str] = {}
    b64_to_bytes: dict[str, bytes] = {}

    def scan(node):
        if isinstance(node, dict):
            for v in node.values():
                scan(v)
        elif isinstance(node, list):
            for v in node:
                scan(v)
        elif isinstance(node, str) and node.startswith(EMB_MARKER):
            key = node[len(EMB_MARKER):]
            if node not in marker_to_b64:
                b64, raw = make_embedding(key, dims[key])
                marker_to_b64[node] = b64
                b64_to_bytes[b64] = raw

    for c in cases:
        if c["query"]:
            scan(c["query"])
    return marker_to_b64, b64_to_bytes


def substitute_markers(node, marker_to_b64: dict, for_rust: bool):
    """Deep-copy query JSON, replacing EMB markers with base64 npy strings.
    For Rust, also set embed=null so the server decodes the embedding instead
    of calling inference. The Python side keeps the default embed args; the
    driver injects the raw embedding into the validated model instead."""
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            out[k] = substitute_markers(v, marker_to_b64, for_rust)
        if (
            for_rust
            and ("query" in out)
            and ("model" in out)
            and isinstance(out.get("query"), str)
            and out["query"] in marker_to_b64.values()
        ):
            out["embed"] = None
        return out
    if isinstance(node, list):
        return [substitute_markers(v, marker_to_b64, for_rust) for v in node]
    if isinstance(node, str) and node.startswith(EMB_MARKER):
        return marker_to_b64[node]
    return node


# ---------------------------------------------------------------------------
# Comparison


def strip_result(r: dict) -> dict:
    return {k: v for k, v in r.items() if v is not None}


def values_equal(a, b, rtol: float, atol: float) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=rtol, abs_tol=atol)
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a) != set(b):
            return False
        return all(values_equal(a[k], b[k], rtol, atol) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(values_equal(x, y, rtol, atol) for x, y in zip(a, b))
    return a == b


def canonical(r) -> str:
    def norm(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, float):
            return f"{v:.6g}"
        if isinstance(v, dict):
            return {k: norm(x) for k, x in sorted(v.items())}
        if isinstance(v, list):
            return [norm(x) for x in v]
        return v

    return json.dumps(norm(r), sort_keys=True)


def compare_case(
    query: dict, py: dict, rust: dict, rtol: float, atol: float
) -> tuple[str, str]:
    """Returns (status, detail)."""
    if query.get("count", True):
        if py["count"] != rust.get("count"):
            return (
                "COUNT_DIFF",
                f"python count={py['count']} rust count={rust.get('count')}",
            )
    py_rs = [strip_result(r) for r in py["results"]]
    rust_rs = [strip_result(r) for r in rust.get("results", [])]
    if len(py_rs) != len(rust_rs):
        return (
            "RESULT_DIFF",
            f"python returned {len(py_rs)} rows, rust {len(rust_rs)}",
        )
    mismatches = [
        i
        for i, (a, b) in enumerate(zip(py_rs, rust_rs))
        if not values_equal(a, b, rtol, atol)
    ]
    if not mismatches:
        return "PASS", ""
    # Same rows in a different order?
    if sorted(map(canonical, py_rs)) == sorted(map(canonical, rust_rs)):
        return (
            "ORDER_DIFF",
            f"{len(mismatches)} rows differ by position only "
            f"(first at index {mismatches[0]}) — likely sort-tie ordering",
        )
    i = mismatches[0]
    return (
        "RESULT_DIFF",
        f"{len(mismatches)} differing rows; first at index {i}:\n"
        f"  python: {json.dumps(py_rs[i], sort_keys=True)[:500]}\n"
        f"  rust:   {json.dumps(rust_rs[i], sort_keys=True)[:500]}",
    )


# ---------------------------------------------------------------------------
# Main


def main() -> int:
    args = parse_args()
    if not args.rust_bin.exists():
        print(f"error: rust binary not found: {args.rust_bin}", file=sys.stderr)
        return 2
    if not args.legacy_src.exists():
        print(
            f"error: legacy source not found: {args.legacy_src} "
            "(is the python-legacy worktree mounted?)",
            file=sys.stderr,
        )
        return 2

    scratch = Path(__file__).parent / ".scratch"
    scratch.mkdir(exist_ok=True)

    if args.prepare:
        prepare_databases(args, scratch)

    conn = open_legacy_conn(args)
    install_legacy_distance_override_stub()
    d = discover(conn)
    print(
        f"[discovery] tags={len(d['tags'])} tag_setters={d['tag_setters']} "
        f"text_setters={d['text_setters']} text_emb={d['text_emb']} "
        f"clip_emb={d['clip_emb']} types={d['types']} "
        f"bookmarks_users={d['bookmark_users']}"
    )

    cases = build_corpus(d, args.page_size)
    if args.only:
        cases = [c for c in cases if args.only in c["name"]]
    marker_to_b64, b64_to_bytes = collect_embeddings(cases, d)

    stub, stub_url = start_metadata_stub(build_stub_metadata(d))
    cfg = write_config(scratch, args, readonly=True, inference_url=stub_url)
    gw = Gateway(args.rust_bin, cfg, scratch, args.port)
    report = {"cases": [], "meta": {
        "data_folder": str(args.data_folder.resolve()),
        "index_db": args.index_db,
        "user_data_db": args.user_data_db,
        "discovery": {k: v for k, v in d.items() if k != "similar_targets"},
    }}
    tally: dict[str, int] = {}
    try:
        gw.wait_ready()
        for c in cases:
            name = c["name"]
            if c["requires_missing"]:
                status, detail = "SKIPPED", c["requires_missing"]
                py_ms = rust_ms = None
            else:
                py_q = substitute_markers(c["query"], marker_to_b64, False)
                rust_q = substitute_markers(c["query"], marker_to_b64, True)
                py = rust = None
                py_err = rust_err = None
                t0 = time.monotonic()
                try:
                    py = run_python_query(conn, py_q, b64_to_bytes)
                except Exception as e:  # noqa: BLE001
                    py_err = f"{type(e).__name__}: {e}"
                py_ms = round((time.monotonic() - t0) * 1000, 1)
                t0 = time.monotonic()
                try:
                    rust = run_rust_query(args, rust_q)
                except Exception as e:  # noqa: BLE001
                    rust_err = f"{type(e).__name__}: {e}"
                rust_ms = round((time.monotonic() - t0) * 1000, 1)
                if py_err and rust_err:
                    status, detail = (
                        "BOTH_ERROR",
                        f"python: {py_err}\nrust: {rust_err}",
                    )
                elif py_err:
                    status, detail = "PY_ERROR", py_err
                elif rust_err:
                    status, detail = "RUST_ERROR", rust_err
                else:
                    status, detail = compare_case(
                        c["query"], py, rust, args.float_rtol, args.float_atol
                    )
            tally[status] = tally.get(status, 0) + 1
            marker = {
                "PASS": "ok",
                "SKIPPED": "--",
                "ORDER_DIFF": "~~",
            }.get(status, "!!")
            timing = (
                f" (py {py_ms}ms / rust {rust_ms}ms)" if py_ms is not None else ""
            )
            print(f"[{marker}] {name}: {status}{timing}")
            if detail and status not in ("PASS", "SKIPPED"):
                print(f"     {detail.splitlines()[0]}")
            report["cases"].append(
                {
                    "name": name,
                    "status": status,
                    "detail": detail,
                    "python_ms": py_ms,
                    "rust_ms": rust_ms,
                    "query": c["query"],
                }
            )
    finally:
        gw.stop()
        stub.shutdown()
        conn.close()

    report["tally"] = tally
    args.out.write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )
    print(f"\nTally: {tally}")
    print(f"Report: {args.out}")
    bad = sum(
        v
        for k, v in tally.items()
        if k not in ("PASS", "SKIPPED", "ORDER_DIFF")
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
