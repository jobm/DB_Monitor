"""Canonical ASGI application module.

This module exposes the package-native ASGI app reference for future runtime
and deployment commands while preserving legacy compatibility today.
"""

from __future__ import annotations

from db_monitor.asgi import app

__all__ = ["app"]
