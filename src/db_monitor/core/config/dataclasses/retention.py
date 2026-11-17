"""Data-retention policies for events, column changes, DLQ, and audit logs."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class RetentionSettings:
    """Data-retention policies for events, column changes, DLQ, and audit logs."""

    cleanup_enabled: bool = field(
        default_factory=lambda: os.getenv("RETENTION_CLEANUP_ENABLED", "false").lower()
        == "true"
    )
    """Enable periodic retention cleanup. Default: false."""
    cleanup_interval_seconds: int = field(
        default_factory=lambda: int(
            os.getenv("RETENTION_CLEANUP_INTERVAL_SECONDS", "3600")
        )
    )
    """Seconds between cleanup cycles. Default: 3600 (1 hour)."""
    cleanup_batch_size: int = field(
        default_factory=lambda: int(
            os.getenv("RETENTION_CLEANUP_BATCH_SIZE", "1000")
        )
    )
    """Rows to delete per batch during cleanup. Default: 1000."""
    cleanup_max_batches_per_table: int = field(
        default_factory=lambda: int(
            os.getenv("RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE", "10")
        )
    )
    """Max batches per table per cycle. Default: 10."""
    archive_before_delete: bool = field(
        default_factory=lambda: os.getenv(
            "RETENTION_ARCHIVE_BEFORE_DELETE", "false"
        ).lower()
        == "true"
    )
    """Archive rows to disk before deleting them. Default: false."""
    archive_dir: str = field(
        default_factory=lambda: os.getenv(
            "RETENTION_ARCHIVE_DIR", "retention-archive"
        )
    )
    """Directory for retention archives. Default: retention-archive."""
    event_retention_days: int = field(
        default_factory=lambda: int(os.getenv("EVENT_RETENTION_DAYS", "30"))
    )
    """Days to retain events. Default: 30."""
    column_changes_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("COLUMN_CHANGES_RETENTION_DAYS", "30")
        )
    )
    """Days to retain column changes. Default: 30."""
    dead_letter_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("DEAD_LETTER_RETENTION_DAYS", "30")
        )
    )
    """Days to retain DLQ records. Default: 30."""
    api_audit_log_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("API_AUDIT_LOG_RETENTION_DAYS", "90")
        )
    )
    """Days to retain API audit logs. Default: 90."""
    spill_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("SPILL_RETENTION_DAYS", "30")
        )
    )
    """Days to retain replayed audit log spill rows. Default: 30."""
