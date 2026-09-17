#!/usr/bin/env python3
"""runleg.py - legs.py with the corpus-name defect shimmed out.

`legs.py::corpus_complaint` (00f22c55) compares the manifest's stamped TIER
against `Scenario.corpus`. Two things are wrong with that:

1. `Scenario.corpus` is a *directory name* that may carry a scale suffix -
   S4b/S4c/S4d declare `ramp8`, and `corpus.py` has no `ramp8` tier - so
   those three legs cannot start ("corpus ... is the 'ramp' tier, this leg
   needs 'ramp8'", and the suggested `corpus.py --tier ramp8` is refused by
   argparse).
2. It ignores `--corpus`. An operator pointing a leg at a deliberately
   different tier - S5-oomimpl on `poison`, exactly as run4 ran it - is
   refused for using the flag as documented.

This wrapper leaves the repository untouched: it strips the trailing scale
digits before the comparison, and skips the tier clause (never the
generator-version clause) when `--corpus` was given explicitly. Everything
else is legs.py verbatim.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
TOOLS = HERE.parents[2]          # .../tools/calibration-protocol
sys.path.insert(0, str(TOOLS))

import legs  # noqa: E402

_orig = legs.corpus_complaint
_EXPLICIT = "--corpus" in sys.argv


def _patched(corpus, tier):
    base = tier.rstrip("0123456789") or tier
    if _EXPLICIT:
        # Keep the stamp check, drop the tier clause: the operator named
        # this directory on purpose.
        import json
        manifest = Path(corpus) / "manifest.json"
        if not manifest.is_file():
            return _orig(corpus, base)
        doc = json.loads(manifest.read_text(encoding="utf-8"))
        return _orig(corpus, str(doc.get("tier") or base))
    return _orig(corpus, base)


legs.corpus_complaint = _patched

if __name__ == "__main__":
    raise SystemExit(legs.main())
