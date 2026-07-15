import json
import tempfile
import unittest
from pathlib import Path

import changelog


VALID = """# Changelog

## [Unreleased]

Work in progress.

## [v1.2.0] - 2026-07-15

### Added

- New behavior.

## [v1.1.1] - 2026-06-01

### Fixed

- A bug.
"""


class ChangelogTests(unittest.TestCase):
    def test_parses_and_extracts_newest_first(self):
        unreleased, releases = changelog.parse(VALID)
        self.assertEqual(unreleased, "Work in progress.")
        self.assertEqual([release.tag for release in releases], ["v1.2.0", "v1.1.1"])
        self.assertEqual(
            changelog.release_for_tag(releases, "v1.1.1").notes_markdown,
            "### Fixed\n\n- A bug.\n",
        )

    def test_rejects_malformed_duplicate_empty_and_out_of_order_sections(self):
        invalid = [
            VALID.replace("## [v1.2.0] - 2026-07-15", "## v1.2.0"),
            VALID + "\n## [v1.2.0] - 2026-07-15\n\nAgain.\n",
            VALID.replace("### Added\n\n- New behavior.", ""),
            VALID.replace("v1.2.0", "v1.0.0"),
            VALID.replace("2026-07-15", "2026-02-30"),
        ]
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(changelog.ChangelogError):
                    changelog.parse(value)

    def test_feed_is_valid_utf8_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "CHANGELOG.md"
            output = root / "changelog.json"
            source.write_text(VALID.replace("A bug.", "A café bug."), encoding="utf-8")
            result = changelog.main(
                [
                    "--changelog",
                    str(source),
                    "feed",
                    "--repository",
                    "reasv/panoptikon",
                    "--generated-at",
                    "2026-07-15T12:00:00Z",
                    "--output",
                    str(output),
                ]
            )
            self.assertEqual(result, 0)
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["releases"][0]["version"], "1.2.0")
            self.assertIn("café", payload["releases"][1]["notes_markdown"])


if __name__ == "__main__":
    unittest.main()
