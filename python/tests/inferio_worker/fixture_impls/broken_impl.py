"""Test fixture impl that fails at import time.

Sorts alphabetically before echo_impl.py, so discovery hits it first and
must tolerate the failure (warn and continue) without breaking discovery of
the other impls in the same dir.
"""

raise RuntimeError("broken_impl is intentionally broken at import time")
