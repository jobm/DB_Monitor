"""Outbound webhook delivery configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from db_monitor.core.config_loader import get_env_or_file


@dataclass
class WebhookSettings:
    """Outbound webhook delivery configuration."""

    urls: list[str] = field(default_factory=list)
    """Webhook destination URLs (parsed from WEBHOOK_URLS CSV)."""
    timeout_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("WEBHOOK_TIMEOUT_SECONDS", "5.0")
        )
    )
    """Per-request webhook timeout in seconds. Default: 5.0."""
    shared_secret: str | None = field(
        default_factory=lambda: get_env_or_file("WEBHOOK_SHARED_SECRET")
    )
    """HMAC-SHA256 signing secret for webhook payloads (supports _FILE variant)."""
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))
    )
    """Max delivery retries per webhook. Default: 3."""
    retry_backoff_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("WEBHOOK_RETRY_BACKOFF_SECONDS", "0.5")
        )
    )
    """Base retry backoff in seconds (exponential). Default: 0.5."""
    circuit_breaker_threshold: int = field(
        default_factory=lambda: int(
            os.getenv("WEBHOOK_CIRCUIT_BREAKER_THRESHOLD", "5")
        )
    )
    """Consecutive failures before circuit opens. Default: 5."""
    circuit_breaker_recovery_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS", "30.0")
        )
    )
    """Seconds before a tripped circuit breaker resets. Default: 30.0."""
