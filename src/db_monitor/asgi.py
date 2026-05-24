"""ASGI compatibility export for package-based server startup."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path


def _load_legacy_app():
    """Load and return the FastAPI app from the legacy module."""
    ensure_legacy_path()
    legacy_main = importlib.import_module("main")
    return legacy_main.app


app = _load_legacy_app()
