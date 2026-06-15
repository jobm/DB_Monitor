"""Core runtime facades for the legacy app runtime.

Submodules are loaded lazily on first attribute access, avoiding eager
import of the entire core tree (and in particular ``core.db`` →
``extensions``) when only ``core.config`` is needed.
"""

from __future__ import annotations

import importlib
from typing import Any

# Submodules that provide attributes at the ``core`` package level.
_SUBMODULES = (".config", ".db", ".lifecycle", ".models", ".responses")

# Lazily-built attribute → submodule mapping.
_ATTR_MAP: dict[str, str] = {}


def _ensure_attr_map() -> dict[str, str]:
    """Build the attribute → submodule mapping on first access."""
    if _ATTR_MAP:
        return _ATTR_MAP
    for submod_name in _SUBMODULES:
        submod = importlib.import_module(submod_name, __name__)
        for attr in dir(submod):
            if not attr.startswith("_"):
                _ATTR_MAP[attr] = submod_name
    return _ATTR_MAP


def __getattr__(name: str) -> Any:
    if name.startswith("_"):
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )

    attr_map = _ensure_attr_map()
    mod_name = attr_map.get(name)
    if mod_name is not None:
        mod = importlib.import_module(mod_name, __name__)
        return getattr(mod, name)

    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def __dir__() -> list[str]:
    return sorted(_ensure_attr_map())
