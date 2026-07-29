#!/usr/bin/env python3
"""Sync ui-pin.json to the monorepo `ui` git submodule gitlink.

Source of truth: the monorepo `ui` gitlink (`git rev-parse :ui` / `HEAD:ui`).
This script never moves the submodule — maintainers bump `ui` manually, then
run this (or rely on git hooks).

Writes only:
  contrib/package/nix/panoptikon/ui-pin.json  — { rev, hash }

- rev  — panoptikon-ui commit
- hash — NAR SRI of that tree (same as `nix hash path --sri` / fetchFromGitHub)

npm deps are not pinned: the package uses importNpmLock on package-lock.json.

Hash computation is pure Python (no nix CLI).

**Hash materialization order:** try GitHub's `archive/{rev}.tar.gz` first
(same layout as `fetchFromGitHub` / CI with `submodules: false`). Offline
`git archive` of a local `ui/` checkout is only used when
``--allow-offline-hash`` is set (write or check). Without that flag, a
GitHub failure aborts so pins always match what `fetchFromGitHub` will fetch.
The gitlink must point at a commit already on panoptikon-ui for consumers.

Usage:
  scripts/sync-nix-ui-pin.py              # update pin (GitHub required)
  scripts/sync-nix-ui-pin.py --check      # exit 1 on rev/hash drift
  scripts/sync-nix-ui-pin.py --check --ref HEAD   # check committed tip
  scripts/sync-nix-ui-pin.py --allow-offline-hash # permit local git archive

Hooks (git config core.hooksPath scripts/git-hooks) — maintainers only:
  pre-commit   — nix fmt; full pin --check (sync+stage on mismatch)
  pre-push     — --check --ref HEAD (tip being pushed, not dirty worktree)
  post-commit  — safety net: follow-up pin commit if still drifted
  post-merge   — sync pin into working tree after pull; ask to commit

There is no npmDepsHash (importNpmLock). Pin is only rev + source NAR hash.

Flake consumers only need a tree where pin matches the gitlink; they do not
run these hooks. See contrib/package/nix/README.md ("Flake consumers").
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PIN_PATH = REPO / "contrib" / "package" / "nix" / "panoptikon" / "ui-pin.json"
UI_OWNER = "reasv"
UI_REPO = "panoptikon-ui"
PIN_KEYS = ("rev", "hash")


def run(
    args: list[str],
    *,
    check: bool = True,
    capture: bool = True,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd or REPO,
        check=check,
        text=True,
        capture_output=capture,
    )


def ui_submodule_rev(ref: str | None = None) -> str:
    """Rev recorded for the ui submodule.

    When *ref* is set (e.g. ``HEAD``), resolve ``{ref}:ui`` only — used by
    pre-push so the **committed tip** is validated, not a dirty worktree.

    Without *ref*, prefer the **index** gitlink (`git rev-parse :ui`) so
    pre-commit sees a staged submodule bump before it is committed. Do not
    use `git ls-tree :0` — that is not a valid tree-ish on current Git.
    """
    if ref:
        try:
            rev = run(["git", "rev-parse", f"{ref}:ui"]).stdout.strip()
        except subprocess.CalledProcessError as e:
            raise SystemExit(
                f"error: no ui gitlink at {ref} (git rev-parse {ref}:ui failed)"
            ) from e
        if re.fullmatch(r"[0-9a-f]{40}", rev):
            return rev
        raise SystemExit(f"error: {ref}:ui is not a 40-char commit: {rev!r}")

    for args in (
        ["git", "rev-parse", ":ui"],  # staged / index
        ["git", "rev-parse", ":0:ui"],
        ["git", "rev-parse", "HEAD:ui"],
    ):
        try:
            rev = run(args).stdout.strip()
        except subprocess.CalledProcessError:
            continue
        if re.fullmatch(r"[0-9a-f]{40}", rev):
            return rev

    # Fallback: ls-tree HEAD (same shape as gitlink)
    try:
        out = run(["git", "ls-tree", "HEAD", "ui"]).stdout.strip()
        m = re.match(r"160000\s+commit\s+([0-9a-f]{40})\tui$", out)
        if m:
            return m.group(1)
    except subprocess.CalledProcessError:
        pass

    ui_dir = REPO / "ui"
    if (ui_dir / "package.json").is_file() or (ui_dir / ".git").exists():
        try:
            return run(["git", "-C", str(ui_dir), "rev-parse", "HEAD"]).stdout.strip()
        except subprocess.CalledProcessError as e:
            raise SystemExit(f"error: cannot read ui HEAD: {e}") from e

    raise SystemExit(
        "error: cannot determine ui submodule rev; run: git submodule update --init ui"
    )


def pin_path_relative() -> str:
    return PIN_PATH.relative_to(REPO).as_posix()


def load_pin(ref: str | None = None) -> dict:
    """Load ui-pin.json from the worktree, or from *ref* (e.g. HEAD) via git show."""
    if ref:
        try:
            raw = run(["git", "show", f"{ref}:{pin_path_relative()}"]).stdout
        except subprocess.CalledProcessError as e:
            raise SystemExit(
                f"error: no {pin_path_relative()} at {ref} "
                f"(commit the pin with the ui gitlink)"
            ) from e
        data = json.loads(raw)
    else:
        if not PIN_PATH.is_file():
            return {}
        data = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"error: {PIN_PATH} is not a JSON object")
    return data


def write_pin(pin: dict) -> None:
    PIN_PATH.parent.mkdir(parents=True, exist_ok=True)
    PIN_PATH.write_text(json.dumps(pin, indent=2) + "\n", encoding="utf-8")


def pin_needs_write(current: dict, new_pin: dict) -> bool:
    """True if rev/hash differ or the file has extra/missing keys."""
    if set(current.keys()) != set(PIN_KEYS):
        return True
    return any(current.get(k) != new_pin.get(k) for k in PIN_KEYS)


# --- pure NAR SRI (matches `nix hash path --sri` / `nix-store --dump`) ---


def _nar_write_str(out: bytearray, data: bytes | str) -> None:
    if isinstance(data, str):
        data = data.encode("utf-8")
    out.extend(struct.pack("<Q", len(data)))
    out.extend(data)
    pad = (8 - (len(data) % 8)) % 8
    out.extend(b"\0" * pad)


def nar_serialize(path: Path) -> bytes:
    """Serialize *path* as a Nix archive (NAR), identical to `nix-store --dump`."""
    out = bytearray()

    def dump(p: Path) -> None:
        st = p.lstat()
        _nar_write_str(out, "(")
        if stat.S_ISREG(st.st_mode):
            _nar_write_str(out, "type")
            _nar_write_str(out, "regular")
            if st.st_mode & 0o111:
                _nar_write_str(out, "executable")
                _nar_write_str(out, "")
            _nar_write_str(out, "contents")
            _nar_write_str(out, p.read_bytes())
        elif stat.S_ISLNK(st.st_mode):
            _nar_write_str(out, "type")
            _nar_write_str(out, "symlink")
            _nar_write_str(out, "target")
            _nar_write_str(out, os.readlink(p))
        elif stat.S_ISDIR(st.st_mode):
            _nar_write_str(out, "type")
            _nar_write_str(out, "directory")
            for entry in sorted(p.iterdir(), key=lambda e: e.name.encode("utf-8")):
                _nar_write_str(out, "entry")
                _nar_write_str(out, "(")
                _nar_write_str(out, "name")
                _nar_write_str(out, entry.name)
                _nar_write_str(out, "node")
                dump(entry)
                _nar_write_str(out, ")")
        else:
            raise SystemExit(f"error: unsupported file type for NAR: {p}")
        _nar_write_str(out, ")")

    _nar_write_str(out, "nix-archive-1")
    dump(path)
    return bytes(out)


def nar_sri(path: Path) -> str:
    digest = hashlib.sha256(nar_serialize(path)).digest()
    return "sha256-" + base64.b64encode(digest).decode("ascii")


def _safe_tar_member_name(name: str) -> str:
    """Reject absolute paths and ``..`` components (path-traversal)."""
    # Normalize separators; tar members use forward slashes.
    norm = name.replace("\\", "/").lstrip("/")
    if not norm or norm == ".":
        raise SystemExit(f"error: unsafe empty tar member name: {name!r}")
    parts = norm.split("/")
    if any(p in ("", "..") for p in parts):
        raise SystemExit(f"error: unsafe tar member path: {name!r}")
    if os.path.isabs(name) or name.startswith("/") or (len(name) > 1 and name[1] == ":"):
        raise SystemExit(f"error: absolute tar member path: {name!r}")
    return norm


def _tar_extractall(tar: tarfile.TarFile, dest: Path) -> None:
    dest = dest.resolve()
    for member in tar.getmembers():
        member.name = _safe_tar_member_name(member.name)
        target = (dest / member.name).resolve()
        if not str(target).startswith(str(dest) + os.sep) and target != dest:
            raise SystemExit(f"error: tar member escapes dest: {member.name!r}")
    try:
        tar.extractall(dest, filter="data")
    except TypeError:
        # Python < 3.12: no filter=; members already name-sanitized above.
        tar.extractall(dest)


def _tar_extract(tar: tarfile.TarFile, member: tarfile.TarInfo, dest: Path) -> None:
    dest = dest.resolve()
    member.name = _safe_tar_member_name(member.name)
    target = (dest / member.name).resolve()
    if not str(target).startswith(str(dest) + os.sep) and target != dest:
        raise SystemExit(f"error: tar member escapes dest: {member.name!r}")
    try:
        tar.extract(member, path=dest, filter="data")
    except TypeError:
        tar.extract(member, path=dest)


def local_ui_matches(rev: str) -> Path | None:
    """Return local ui/ if it exists and HEAD == rev."""
    local = REPO / "ui"
    if not (local / "package.json").is_file() and not (local / ".git").exists():
        return None
    try:
        head = run(["git", "-C", str(local), "rev-parse", "HEAD"]).stdout.strip()
    except subprocess.CalledProcessError:
        return None
    if head != rev:
        return None
    return local


def export_git_tree(repo: Path, rev: str, dest: Path) -> None:
    """Export *rev* from *repo* into *dest* (no .git)."""
    dest.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["git", "-C", str(repo), "archive", "--format=tar", rev],
        check=True,
        capture_output=True,
    )
    with tarfile.open(fileobj=io.BytesIO(proc.stdout), mode="r:") as tar:
        _tar_extractall(tar, dest)


def download_github_tree(rev: str, dest: Path) -> None:
    """Fetch GitHub auto-archive; strip the top-level dir (fetchFromGitHub layout).

    Raises urllib.error.URLError / SystemExit on failure (caller may fall back).
    """
    url = f"https://github.com/{UI_OWNER}/{UI_REPO}/archive/{rev}.tar.gz"
    with urllib.request.urlopen(url, timeout=120) as resp:
        blob = resp.read()

    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        members = tar.getmembers()
        if not members:
            raise SystemExit(f"error: empty archive from {url}")
        top = members[0].name.split("/", 1)[0]
        prefix = top + "/"
        for m in members:
            name = m.name
            if name == top or name == top + "/":
                continue
            if not name.startswith(prefix):
                continue
            m.name = name[len(prefix) :]
            if not m.name:
                continue
            _tar_extract(tar, m, dest)


def materialize_ui_tree(rev: str, dest: Path, *, allow_offline: bool) -> None:
    """Materialize the UI tree at *rev* for NAR hashing.

    1. GitHub archive first (matches `fetchFromGitHub` / CI).
    2. Local `git archive` only when *allow_offline* (``--allow-offline-hash``).
    """
    try:
        download_github_tree(rev, dest)
        return
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        github_err = e

    if not allow_offline:
        raise SystemExit(
            f"error: cannot materialize UI at {rev}: GitHub download failed "
            f"({github_err}). Pin hashes must match fetchFromGitHub; re-run "
            f"with network, or pass --allow-offline-hash only for local "
            f"experimentation (do not push offline-only pins)."
        )

    local = local_ui_matches(rev)
    if local is not None:
        print(
            f"warning: GitHub archive for {rev[:12]} unavailable ({github_err}); "
            f"using local ui/ git archive — re-sync online before push "
            f"(offline hashes can diverge from fetchFromGitHub)",
            file=sys.stderr,
        )
        export_git_tree(local, rev, dest)
        return

    raise SystemExit(
        f"error: cannot materialize UI at {rev}: GitHub download failed "
        f"({github_err}); check out the ui submodule at that rev "
        f"(git submodule update --init ui) and re-run with "
        f"--allow-offline-hash if still offline"
    )


def compute_source_hash(rev: str, *, allow_offline: bool) -> str:
    with tempfile.TemporaryDirectory(prefix="panoptikon-ui-pin-") as tmp:
        root = Path(tmp) / "src"
        materialize_ui_tree(rev, root, allow_offline=allow_offline)
        if not (root / "package.json").is_file():
            raise SystemExit(
                f"error: materialized UI tree for {rev} has no package.json"
            )
        return nar_sri(root)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if ui-pin.json drifts from the ui submodule",
    )
    ap.add_argument(
        "--ref",
        metavar="REF",
        default=None,
        help="resolve ui gitlink and pin from this git ref (e.g. HEAD) "
        "instead of the worktree/index; for pre-push tip validation",
    )
    ap.add_argument(
        "--allow-offline-hash",
        action="store_true",
        help="allow local git archive when GitHub is unreachable "
        "(offline pins can diverge from fetchFromGitHub — do not push)",
    )
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    os.chdir(REPO)

    if args.ref and not args.check:
        raise SystemExit("error: --ref is only valid with --check")

    rev = ui_submodule_rev(args.ref)
    pin = load_pin(args.ref)
    allow_offline = bool(args.allow_offline_hash)

    if args.check:
        errors: list[str] = []
        if pin.get("rev") != rev:
            src = f"{args.ref}:" if args.ref else ""
            errors.append(
                f"{src}ui-pin.json rev={pin.get('rev')!r} != submodule {rev}"
            )
        if not pin.get("hash"):
            errors.append("ui-pin.json missing hash")
        extra = set(pin.keys()) - set(PIN_KEYS)
        if extra:
            errors.append(
                f"ui-pin.json has unexpected keys {sorted(extra)} "
                f"(expected only {list(PIN_KEYS)}; npm uses importNpmLock)"
            )

        # When checking a committed tip, refuse a dirty worktree that
        # disagrees with HEAD (would give a false sense that push is OK).
        if args.ref and not errors:
            try:
                wt_rev = ui_submodule_rev(None)
                wt_pin = load_pin(None)
            except SystemExit:
                wt_rev, wt_pin = None, {}
            if wt_rev and wt_rev != rev:
                errors.append(
                    f"worktree ui gitlink {wt_rev} != {args.ref}:ui {rev}; "
                    f"commit the submodule bump before push"
                )
            if wt_pin and (
                wt_pin.get("rev") != pin.get("rev")
                or wt_pin.get("hash") != pin.get("hash")
            ):
                errors.append(
                    f"worktree ui-pin.json differs from {args.ref}; "
                    f"commit the pin before push"
                )

        if not errors:
            if not args.quiet:
                print(f"checking source hash for {rev[:12]} ...")
            try:
                content_hash = compute_source_hash(rev, allow_offline=allow_offline)
            except SystemExit as e:
                errors.append(str(e).removeprefix("error: "))
            else:
                if pin.get("hash") != content_hash:
                    errors.append(
                        f"ui-pin.json hash={pin.get('hash')!r} != computed {content_hash!r}"
                    )

        if errors:
            for e in errors:
                print(f"error: {e}", file=sys.stderr)
            print("fix: scripts/sync-nix-ui-pin.py", file=sys.stderr)
            return 1
        if not args.quiet:
            where = f" @ {args.ref}" if args.ref else ""
            print(f"ok: ui-pin.json matches submodule {rev[:12]}{where}")
        return 0

    if not args.quiet:
        print(f"hashing UI tree at {rev[:12]} ...")
    content_hash = compute_source_hash(rev, allow_offline=allow_offline)
    new_pin = {"rev": rev, "hash": content_hash}

    if pin_needs_write(pin, new_pin):
        write_pin(new_pin)
        if not args.quiet:
            print(f"wrote {PIN_PATH.relative_to(REPO)}")
            if pin.get("rev") != rev:
                print(f"  rev -> {rev}")
            if pin.get("hash") != content_hash:
                print(f"  hash -> {content_hash}")
            extras = set(pin.keys()) - set(PIN_KEYS)
            if extras:
                print(f"  dropped keys: {sorted(extras)}")
    elif not args.quiet:
        print(f"pin already at {rev[:12]}")

    if not args.quiet:
        print(f"ok: ui -> {rev}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
