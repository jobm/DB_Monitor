"""Canonical lifespan namespace proxy."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_legacy_module = importlib.import_module("lifespan")


def __getattr__(name: str):
    """Proxy attribute access to the legacy module."""
    return getattr(_legacy_module, name)


def __dir__() -> list[str]:
    """Expose proxied module attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_module)))
