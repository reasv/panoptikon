#!/usr/bin/env python3
"""Validate and publish Panoptikon's structured CHANGELOG.md."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

TITLE = "# Changelog"
UNRELEASED = "## [Unreleased]"
RELEASE = re.compile(
    r"^## \[(?P<tag>v(?P<version>\d+\.\d+\.\d+))\] - (?P<date>\d{4}-\d{2}-\d{2})$"
)
ANY_H2 = re.compile(r"^## ")
SEMVER = re.compile(r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


class ChangelogError(ValueError):
    pass


@dataclass(frozen=True)
class Release:
    version: str
    tag: str
    date: str
    notes_markdown: str
    release_url: str | None = None


def _version_key(value: str) -> tuple[int, int, int]:
    match = SEMVER.fullmatch(value)
    if not match:
        raise ChangelogError(f"invalid release version: {value}")
    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))


def parse(text: str) -> tuple[str, list[Release]]:
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if sum(line == TITLE for line in lines) != 1 or not lines or lines[0] != TITLE:
        raise ChangelogError("CHANGELOG.md must begin with exactly one '# Changelog'")

    h2 = [index for index, line in enumerate(lines) if ANY_H2.match(line)]
    if not h2 or lines[h2[0]] != UNRELEASED:
        raise ChangelogError("the first H2 section must be '## [Unreleased]'")
    if sum(line == UNRELEASED for line in lines) != 1:
        raise ChangelogError("CHANGELOG.md must contain exactly one Unreleased section")

    releases: list[Release] = []
    seen: set[str] = set()
    for position, start in enumerate(h2):
        heading = lines[start]
        end = h2[position + 1] if position + 1 < len(h2) else len(lines)
        body = "\n".join(lines[start + 1 : end]).strip()
        if heading == UNRELEASED:
            unreleased = body
            continue
        match = RELEASE.fullmatch(heading)
        if not match:
            raise ChangelogError(f"malformed release heading: {heading}")
        tag = match.group("tag")
        if tag in seen:
            raise ChangelogError(f"duplicate release heading: {tag}")
        seen.add(tag)
        try:
            dt.date.fromisoformat(match.group("date"))
        except ValueError as error:
            raise ChangelogError(f"invalid release date in {heading}") from error
        if not body:
            raise ChangelogError(f"release section {tag} is empty")
        releases.append(
            Release(
                version=match.group("version"),
                tag=tag,
                date=match.group("date"),
                notes_markdown=body + "\n",
            )
        )

    keys = [_version_key(release.version) for release in releases]
    if keys != sorted(keys, reverse=True):
        raise ChangelogError("release sections must be ordered newest first")
    return unreleased, releases


def load(path: Path) -> tuple[str, list[Release]]:
    return parse(path.read_text(encoding="utf-8"))


def release_for_tag(releases: list[Release], tag: str) -> Release:
    matches = [release for release in releases if release.tag == tag]
    if len(matches) != 1:
        raise ChangelogError(f"expected exactly one non-empty changelog section for {tag}")
    return matches[0]


def write(path: Path | None, value: str) -> None:
    if path is None:
        sys.stdout.write(value)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate")
    extract = sub.add_parser("extract")
    extract.add_argument("--tag", required=True)
    extract.add_argument("--output", type=Path)
    feed = sub.add_parser("feed")
    feed.add_argument("--repository", required=True)
    feed.add_argument("--output", type=Path)
    feed.add_argument("--generated-at")
    args = parser.parse_args(argv)

    try:
        _, releases = load(args.changelog)
        if args.command == "validate":
            return 0
        if args.command == "extract":
            write(args.output, release_for_tag(releases, args.tag).notes_markdown)
            return 0
        generated_at = args.generated_at or dt.datetime.now(dt.UTC).replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z")
        payload = {
            "schema_version": 1,
            "generated_at": generated_at,
            "releases": [
                asdict(
                    Release(
                        **{
                            **asdict(release),
                            "release_url": (
                                f"https://github.com/{args.repository}/releases/tag/{release.tag}"
                            ),
                        }
                    )
                )
                for release in releases
            ],
        }
        write(args.output, json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
        return 0
    except (OSError, ChangelogError) as error:
        print(f"changelog error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
