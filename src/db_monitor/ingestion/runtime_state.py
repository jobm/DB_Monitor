"""Canonical ingestion runtime-state exports."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_runtime_state = importlib.import_module("consumer.runtime_state")


def __getattr__(name: str):
    """Proxy runtime-state attribute access to consumer runtime module."""
    return getattr(_runtime_state, name)


def __dir__() -> list[str]:
    """Expose proxied runtime-state attributes for introspection tools."""
    return sorted(set(globals()) | set(dir(_runtime_state)))
