"""Core lifecycle namespace proxy.

This module keeps compatibility with existing flat-module imports while
providing a stable package path for new code.
"""

from __future__ import annotations

import lifecycle_manager as _legacy_lifecycle


def __getattr__(name: str):
    """Proxy attribute access to the legacy lifecycle module."""
    return getattr(_legacy_lifecycle, name)


def __dir__() -> list[str]:
    """Expose legacy lifecycle attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_lifecycle)))
