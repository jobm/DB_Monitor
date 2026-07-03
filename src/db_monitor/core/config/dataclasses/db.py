"""Database connection-pool settings for the monitor's PostgreSQL database."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from db_monitor.core.config_loader import get_env_or_file
from db_monitor.core.config.normalizers import normalize_postgres_url


@dataclass
class DatabaseSettings:
    """Connection-pool tuning for the monitor's own PostgreSQL database.

    Every field maps to a ``DB_*`` / ``POSTGRES_*`` environment variable.
    """

    postgres_url: str = field(
        default_factory=lambda: normalize_postgres_url(
            get_env_or_file(
                "POSTGRES_URL",
                "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres",
            )
            or "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres",
        )
    )
    """Monitor database DSN. Default: local postgres on port 5437."""
    sqlalchemy_echo: bool = field(
        default_factory=lambda: os.getenv("SQLALCHEMY_ECHO", "false").lower()
        == "true"
    )
    """Enable SQLAlchemy echo logging. Default: false."""
    pool_size: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_SIZE", "10"))
    )
    """Persistent pool size. Default: 10."""
    max_overflow: int = field(
        default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "20"))
    )
    """Max overflow connections beyond pool_size. Default: 20."""
    pool_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30"))
    )
    """Seconds to wait for a connection from the pool. Default: 30."""
    pool_recycle_seconds: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800"))
    )
    """Recycle connections after this many seconds. Default: 1800 (30 min)."""
    pool_pre_ping: bool = field(
        default_factory=lambda: os.getenv("DB_POOL_PRE_PING", "true").lower()
        == "true"
    )
    """Test connections before checkout. Default: true."""
