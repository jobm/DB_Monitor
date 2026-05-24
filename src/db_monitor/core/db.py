"""Canonical core database namespace proxy.

During the namespace migration phase, this module delegates to the legacy
`app/extensions.py` runtime source while presenting stable imports via
`db_monitor.core.db`.
"""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_legacy_db = importlib.import_module("extensions")


def __getattr__(name: str):
    """Proxy attribute access to the legacy extensions module."""
    return getattr(_legacy_db, name)


def __dir__() -> list[str]:
    """Expose proxied DB attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_db)))
