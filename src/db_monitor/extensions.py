"""Canonical top-level database extensions namespace proxy.

During the phased migration, this module delegates to the legacy
`app/extensions.py` implementation while exposing imports under `db_monitor`.
"""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_legacy_extensions = importlib.import_module("extensions")


def __getattr__(name: str):
    """Proxy attribute access to the legacy extensions module."""
    return getattr(_legacy_extensions, name)


def __dir__() -> list[str]:
    """Expose proxied DB extension attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_extensions)))
