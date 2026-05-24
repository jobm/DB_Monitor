"""Canonical auth namespace proxy.

This module exposes authentication helpers under the package namespace while
runtime behavior still delegates to legacy app modules during migration.
"""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_legacy_auth = importlib.import_module("auth")


def __getattr__(name: str):
    """Proxy attribute access to the legacy auth module."""
    return getattr(_legacy_auth, name)


def __dir__() -> list[str]:
    """Expose proxied auth attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_legacy_auth)))
