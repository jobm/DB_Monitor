"""Core configuration namespace proxy.

This module keeps compatibility with existing flat-module imports while
providing a stable package path for new code.
"""

from __future__ import annotations

import config as _legacy_config


def __getattr__(name: str):
    """Proxy attribute access to the legacy config module."""
    return getattr(_legacy_config, name)


def __dir__() -> list[str]:
    """Expose legacy config attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_config)))
