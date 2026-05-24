"""Core configuration namespace proxy.

This module keeps compatibility with existing flat-module imports while
providing a stable package path for new code.
"""

from __future__ import annotations

import importlib

from _namespace_bridge import ensure_src_namespace_path

ensure_src_namespace_path()

_canonical_config = importlib.import_module("db_monitor.core.config")


def __getattr__(name: str):
    """Proxy attribute access to the canonical core config module."""
    return getattr(_canonical_config, name)


def __dir__() -> list[str]:
    """Expose canonical config attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_canonical_config)))
