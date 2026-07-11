"""Impl-class discovery for the worker harness.

Replicates the semantics of `inferio.utils.get_impl_classes()` without
importing `inferio`: load each `*.py` module in each impl dir, read its
`IMPL_CLASS` attribute, and match the class's `name()` against the
requested implementation name. Dirs are searched in the order given
(earlier dirs win, mirroring built-ins-over-custom precedence), files in
sorted order within a dir. A module that fails to import (or has a bogus
IMPL_CLASS) is logged and skipped so unrelated breakage never prevents
discovery of the requested class — same tolerance as `get_impl_classes`.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from typing import Sequence


def find_impl_class(
    impl_class_name: str,
    impl_dirs: Sequence[str],
    logger: logging.Logger,
) -> type:
    """Locate the impl class whose `name()` equals `impl_class_name`.

    Returns at the first match; raises LookupError if no module in any dir
    provides it.
    """
    searched: list[str] = []
    for dir_index, dir_str in enumerate(impl_dirs):
        impl_dir = Path(dir_str)
        searched.append(str(impl_dir))
        if not impl_dir.is_dir():
            logger.warning("Impl dir does not exist: %s", impl_dir)
            continue
        for pyfile in sorted(impl_dir.glob("*.py")):
            if pyfile.name == "__init__.py":
                continue
            impl = _load_impl_from_file(pyfile, dir_index, logger)
            if impl is None:
                continue
            impl_cls, impl_name = impl
            if impl_name == impl_class_name:
                logger.info(
                    "Resolved impl class %s to %s", impl_class_name, pyfile
                )
                return impl_cls
    raise LookupError(
        f"Implementation class {impl_class_name!r} not found in impl dirs: "
        f"{searched}"
    )


def _load_impl_from_file(
    pyfile: Path, dir_index: int, logger: logging.Logger
) -> tuple[type, str] | None:
    """Load one candidate module and return (IMPL_CLASS, name()) or None."""
    module_name = f"inferio_worker_impl_{dir_index}_{pyfile.stem}"
    spec = importlib.util.spec_from_file_location(module_name, pyfile)
    if spec is None or spec.loader is None:
        logger.warning("Could not create an import spec for %s", pyfile)
        return None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        # Mirror get_impl_classes tolerance: an unrelated broken module must
        # not break discovery of the requested class.
        sys.modules.pop(module_name, None)
        logger.warning(
            "Failed to import impl module %s: %s", pyfile, e, exc_info=True
        )
        return None
    impl_cls = getattr(mod, "IMPL_CLASS", None)
    if impl_cls is None:
        return None
    if not isinstance(impl_cls, type):
        logger.warning("Module %s does not have a valid IMPL_CLASS.", pyfile)
        return None
    name_attr = getattr(impl_cls, "name", None)
    if not callable(name_attr):
        logger.warning(
            "Implementation class %s.IMPL_CLASS does not have a name method. "
            "Skipping.",
            pyfile,
        )
        return None
    try:
        impl_name = impl_cls.name()
    except Exception as e:
        logger.warning(
            "Implementation class %s.IMPL_CLASS name() raised: %s. Skipping.",
            pyfile,
            e,
        )
        return None
    if not impl_name:
        logger.warning(
            "Implementation class %s.IMPL_CLASS returned an empty name. "
            "Skipping.",
            pyfile,
        )
        return None
    return impl_cls, impl_name
