"""Tracked schema and data migrations for DB Monitor.

The app no longer mutates schema implicitly on every startup.
Instead, startup either applies tracked migrations in
development-style environments or validates that all known
migrations have already been applied in production-style
environments.
"""

from __future__ import annotations

from db_monitor.core.migrations.core import (
    Migration,
    apply_migrations,
    get_pending_migrations,
    validate_migrations,
)
from db_monitor.core.migrations.legacy import (
    ensure_event_ingestion_columns,
    migrate_legacy_kafka_events,
)
from db_monitor.core.migrations.partition_ops import (
    create_monthly_partitions,
    drop_old_partitions,
)
from db_monitor.core.migrations.partitioning import (
    _apply_event_partitioning,
)
from db_monitor.core.migrations.recovery_migration import (
    _apply_broker_recovery_metadata,
)
from db_monitor.core.migrations.schema import (
    _apply_base_schema,
    _apply_bigint_kafka_offsets,
    _apply_event_ingestion_columns,
    _apply_events_unique_constraint,
    _apply_legacy_event_backfill,
    _apply_row_identity_columns,
)
from db_monitor.core.migrations.auth import (
    _apply_api_key_lifecycle_columns,
    _apply_customer_control_plane_state,
    _apply_customer_jwt_secret_state,
)
from db_monitor.core.migrations.infra import (
    _apply_api_audit_log_spill,
    _apply_cursor_pagination_indexes,
    _apply_ingestion_quota_windows,
    _apply_recovery_tables,
)

MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version="0001_base_schema",
        description="Create the baseline DB Monitor schema",
        apply=_apply_base_schema,
    ),
    Migration(
        version="0002_event_ingestion_columns",
        description="Add Kafka position columns for idempotent ingestion",
        apply=_apply_event_ingestion_columns,
    ),
    Migration(
        version="0003_legacy_event_backfill",
        description="Backfill structured events from the legacy raw table",
        apply=_apply_legacy_event_backfill,
    ),
    Migration(
        version="0004_api_key_lifecycle_columns",
        description="Add API key expiration and revocation lifecycle columns",
        apply=_apply_api_key_lifecycle_columns,
    ),
    Migration(
        version="0005_recovery_tables",
        description="Add persistent checkpoints and DLQ recovery tables",
        apply=_apply_recovery_tables,
    ),
    Migration(
        version="0006_bigint_kafka_offsets",
        description="Widen Kafka offset columns to bigint",
        apply=_apply_bigint_kafka_offsets,
    ),
    Migration(
        version="0007_row_identity_columns",
        description="Persist row identity for row-scoped audit history",
        apply=_apply_row_identity_columns,
    ),
    Migration(
        version="0008_broker_recovery_metadata",
        description=(
            "Add broker-neutral checkpoint and dead-letter metadata"
        ),
        apply=_apply_broker_recovery_metadata,
    ),
    Migration(
        version="0009_customer_jwt_secret_state",
        description="Persist customer JWT secret lifecycle state",
        apply=_apply_customer_jwt_secret_state,
    ),
    Migration(
        version="0010_customer_control_plane_state",
        description="Persist customer lifecycle and job orchestration state",
        apply=_apply_customer_control_plane_state,
    ),
    Migration(
        version="0011_ingestion_quota_windows",
        description="Persist shared ingestion quota windows",
        apply=_apply_ingestion_quota_windows,
    ),
    Migration(
        version="0012_api_audit_log_spill",
        description="Create durable spill table for audit log entries",
        apply=_apply_api_audit_log_spill,
    ),
    Migration(
        version="0013_event_partitioning",
        description=(
            "Convert events and column_changes to time-range "
            "partitioned tables for efficient retention"
        ),
        apply=_apply_event_partitioning,
    ),
    Migration(
        version="0014_cursor_pagination_indexes",
        description="Add cursor-optimized composite indexes for large scans",
        apply=_apply_cursor_pagination_indexes,
    ),
    Migration(
        version="0015_events_unique_constraint",
        description=(
            "Replace unique index with unique constraint on events "
            "so ON CONFLICT works on the partitioned table"
        ),
        apply=_apply_events_unique_constraint,
    ),
)

CURRENT_SCHEMA_VERSION = MIGRATIONS[-1].version

__all__ = [
    "Migration",
    "MIGRATIONS",
    "CURRENT_SCHEMA_VERSION",
    "apply_migrations",
    "get_pending_migrations",
    "validate_migrations",
    "ensure_event_ingestion_columns",
    "migrate_legacy_kafka_events",
    "create_monthly_partitions",
    "drop_old_partitions",
]
