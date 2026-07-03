"""Authentication, JWT, and API key lifecycle settings."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from db_monitor.core.config_loader import get_env_or_file
from db_monitor.core.config.normalizers import default_allow_bootstrap


@dataclass
class AuthSettings:
    """Authentication, JWT, and API key lifecycle settings."""

    jwt_secret: str = field(
        default_factory=lambda: get_env_or_file(
            "JWT_SECRET", "dev-insecure-secret-change-me-please-rotate"
        )
        or "dev-insecure-secret-change-me-please-rotate"
    )
    """JWT signing secret (supports _FILE variant)."""
    jwt_secret_next: str | None = field(
        default_factory=lambda: get_env_or_file("JWT_SECRET_NEXT")
    )
    """Secondary JWT secret for rotation overlap (supports _FILE variant)."""
    jwt_algorithm: str = field(
        default_factory=lambda: os.getenv("JWT_ALGORITHM", "HS256")
    )
    """JWT signing algorithm. Default: HS256."""
    access_token_ttl_minutes: int = field(
        default_factory=lambda: int(os.getenv("ACCESS_TOKEN_TTL_MINUTES", "15"))
    )
    """Bearer token time-to-live in minutes. Default: 15."""
    ws_session_token_ttl_seconds: int = field(
        default_factory=lambda: int(
            os.getenv("WS_SESSION_TOKEN_TTL_SECONDS", "60")
        )
    )
    """WebSocket session token TTL in seconds. Default: 60."""
    api_key_default_ttl_days: int = field(
        default_factory=lambda: int(os.getenv("API_KEY_DEFAULT_TTL_DAYS", "90"))
    )
    """Default API key expiration in days. Default: 90."""
    allow_bootstrap: bool = field(
        default_factory=lambda: default_allow_bootstrap()
    )
    """Allow the /auth/bootstrap endpoint. Disabled in production by default."""
    ws_backplane_enabled: bool = field(
        default_factory=lambda: os.getenv("WS_BACKPLANE_ENABLED", "true").lower()
        == "true"
    )
    """Enable Postgres LISTEN/NOTIFY backplane for WebSocket fanout. Default: true."""
    ws_backplane_channel: str = field(
        default_factory=lambda: os.getenv(
            "WS_BACKPLANE_CHANNEL", "db_monitor_ws_events"
        )
    )
    """Postgres NOTIFY channel name for WebSocket backplane."""
