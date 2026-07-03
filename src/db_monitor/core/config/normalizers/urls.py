"""URL and value normalization helpers."""

from __future__ import annotations

import os

from db_monitor.core.config_loader import config_error


def normalize_postgres_url(url: str) -> str:
    """Ensure the DSN uses the ``postgresql+asyncpg://`` scheme."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    config_error(
        "Invalid POSTGRES_URL format.",
        hint="Use a PostgreSQL DSN starting with 'postgresql+asyncpg://'.",
    )
    return url  # unreachable


def default_allow_bootstrap() -> bool:
    """Return the safe default for ALLOW_BOOTSTRAP based on the environment."""
    env = os.getenv("APP_ENV", "development").lower()
    raw = os.getenv("ALLOW_BOOTSTRAP")
    if raw is not None:
        return raw.lower() == "true"
    return env not in {"production", "prod"}
