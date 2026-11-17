"""Backplane configuration: cross-replica pub/sub for WebSocket fanout.

Supports both Postgres LISTEN/NOTIFY and Redis Pub/Sub backends.
The Redis backend is recommended for high-scale deployments (5000+ ev/s)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from db_monitor.core.config_loader import get_env_or_file


@dataclass
class BackplaneSettings:
    """Cross-replica pub/sub backplane for WebSocket and schema-cache fanout."""

    backend: str = field(
        default_factory=lambda: os.getenv("WS_BACKPLANE_BACKEND", "postgres").lower()
    )
    """Backend type: ``postgres`` (LISTEN/NOTIFY) or ``redis`` (Pub/Sub).
    Default: postgres."""

    redis_url: str | None = field(
        default_factory=lambda: get_env_or_file("REDIS_URL")
    )
    """Redis connection URL (required when WS_BACKPLANE_BACKEND=redis).
    Example: ``redis://redis:6379/0``. Supports ``REDIS_URL_FILE``."""

    redis_channel_prefix: str = field(
        default_factory=lambda: os.getenv(
            "REDIS_BACKPLANE_CHANNEL_PREFIX", "db_monitor"
        )
    )
    """Prefix for Redis Pub/Sub channel names. Default: db_monitor."""
