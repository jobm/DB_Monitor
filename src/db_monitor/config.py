"""Canonical top-level config namespace proxy.

During the phased migration, this module delegates to the legacy
`app/config.py` implementation while exposing imports under `db_monitor`.
"""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_legacy_config = importlib.import_module("config")


def __getattr__(name: str):
    """Proxy attribute access to the legacy config module."""
    return getattr(_legacy_config, name)


def __dir__() -> list[str]:
    """Expose proxied config attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_config)))
