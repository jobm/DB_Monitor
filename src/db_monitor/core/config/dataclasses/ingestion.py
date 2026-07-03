"""CDC ingestion quota enforcement and bulk-write behavior."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class IngestionSettings:
    """CDC ingestion quota enforcement and bulk-write behavior."""

    quota_enabled: bool = field(
        default_factory=lambda: os.getenv("INGESTION_QUOTA_ENABLED", "false").lower()
        == "true"
    )
    """Enable per-source and per-tenant ingestion quotas. Default: false."""
    quota_window_seconds: int = field(
        default_factory=lambda: int(
            os.getenv("INGESTION_QUOTA_WINDOW_SECONDS", "60")
        )
    )
    """Quota evaluation window in seconds. Default: 60."""
    source_default_events_per_window: int = field(
        default_factory=lambda: int(
            os.getenv("INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW", "0")
        )
    )
    """Default event limit per source per window. 0=unlimited. Default: 0."""
    tenant_default_events_per_window: int = field(
        default_factory=lambda: int(
            os.getenv("INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW", "0")
        )
    )
    """Default event limit per tenant per window. 0=unlimited. Default: 0."""
    quota_mode: str = field(
        default_factory=lambda: os.getenv(
            "INGESTION_QUOTA_MODE", "throttle"
        ).strip().lower()
    )
    """Quota enforcement mode: ``throttle`` or ``drop``. Default: throttle."""
    quota_max_throttle_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("INGESTION_QUOTA_MAX_THROTTLE_SECONDS", "5.0")
        )
    )
    """Max throttle sleep per event in seconds. Default: 5.0."""
    bulk_write_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "INGESTION_BULK_WRITE_ENABLED", "true"
        ).lower()
        == "true"
    )
    """Enable bulk (batch) database writes for ingestion. Default: true."""

    # -- Computed (populated after construction via AppSettings.from_env) --
    source_quotas: dict[str, int] = field(default_factory=dict)
    """Per-source quota overrides parsed from INGESTION_SOURCE_QUOTAS JSON."""
    tenant_quotas: dict[str, int] = field(default_factory=dict)
    """Per-tenant quota overrides parsed from INGESTION_TENANT_QUOTAS JSON."""
