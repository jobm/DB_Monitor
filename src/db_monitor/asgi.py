"""ASGI application entrypoint.

Re-exports the application instance from the canonical :mod:`db_monitor.main`
module for compatibility with ASGI servers (uvicorn, hypercorn, etc.).
"""

from __future__ import annotations

from db_monitor.main import app

__all__ = ["app"]

