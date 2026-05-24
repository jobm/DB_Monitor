"""Core database namespace proxy.

This module keeps compatibility with existing flat-module imports while
providing a stable package path for new code.
"""

from __future__ import annotations

import importlib

from _namespace_bridge import ensure_src_namespace_path

ensure_src_namespace_path()

_canonical_db = importlib.import_module("db_monitor.core.db")


def __getattr__(name: str):
    """Proxy attribute access to the canonical DB module."""
    return getattr(_canonical_db, name)


def __dir__() -> list[str]:
    """Expose canonical DB attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_canonical_db)))
