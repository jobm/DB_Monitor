"""Unified application configuration with lazy loading from environment.

Usage::

    from db_monitor.core.config import settings

    print(settings.env)               # was APP_ENV
    print(settings.postgres_url)      # was POSTGRES_URL
    print(settings.db.pool_size)      # was DB_POOL_SIZE
    print(settings.auth.jwt_secret)   # was JWT_SECRET
"""

from __future__ import annotations

from functools import cache

from dotenv import load_dotenv

from db_monitor.core.config.dataclasses import AppSettings

load_dotenv()


@cache
def _get_settings() -> AppSettings:
    """Return the lazily loaded, validated settings singleton."""
    return AppSettings.from_env()


settings = _get_settings()
"""Application-wide configuration singleton. Loaded once on first access."""


__all__ = [
    "AppSettings",
    "_get_settings",
    "settings",
]
