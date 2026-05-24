"""Core database namespace proxy.

This module keeps compatibility with existing flat-module imports while
providing a stable package path for new code.
"""

from __future__ import annotations

import extensions as _legacy_db


def __getattr__(name: str):
    """Proxy attribute access to the legacy DB extensions module."""
    return getattr(_legacy_db, name)


def __dir__() -> list[str]:
    """Expose legacy DB attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_db)))
