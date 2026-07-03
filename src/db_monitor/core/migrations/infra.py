"""Infrastructure migration steps — recovery tables, cursor indexes, quota windows, audit spill."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def _apply_recovery_tables(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create checkpoint and persistent DLQ tables for recovery workflows."""
    del session_factory
    statements = [
        """
        CREATE TABLE IF NOT EXISTS consumer_checkpoints (
            id SERIAL PRIMARY KEY,
            consumer_group VARCHAR(128) NOT NULL,
            kafka_topic VARCHAR(256) NOT NULL,
            kafka_partition INTEGER NOT NULL,
            kafka_offset BIGINT NOT NULL,
            last_event_time TIMESTAMPTZ NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_consumer_checkpoints_group_topic_partition "
            "ON consumer_checkpoints "
            "(consumer_group, kafka_topic, kafka_partition)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_consumer_checkpoints_updated_at "
            "ON consumer_checkpoints (updated_at)"
        ),
        """
        CREATE TABLE IF NOT EXISTS dead_letter_events (
            id SERIAL PRIMARY KEY,
            service_name VARCHAR(128) NULL,
            kafka_topic VARCHAR(256) NOT NULL,
            kafka_partition INTEGER NOT NULL,
            kafka_offset BIGINT NOT NULL,
            operation VARCHAR(16) NULL,
            raw_payload TEXT NOT NULL,
            error_message TEXT NOT NULL,
            failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_replayed BOOLEAN NOT NULL DEFAULT FALSE,
            replayed_at TIMESTAMPTZ NULL,
            replay_error TEXT NULL
        )
        """,
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_dead_letter_events_kafka_position "
            "ON dead_letter_events "
            "(kafka_topic, kafka_partition, kafka_offset)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_dead_letter_events_failed_at "
            "ON dead_letter_events (failed_at)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_dead_letter_events_is_replayed "
            "ON dead_letter_events (is_replayed)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_cursor_pagination_indexes(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create cursor-optimized indexes for large event and change scans."""
    del session_factory
    statements = [
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_events_cursor_service_type_time "
            "ON events (service_name, event_type, capture_time, id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_events_cursor_source_table_time "
            "ON events (source_table_id, capture_time, id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_changes_cursor_table_time "
            "ON column_changes (table_id, changed_at, id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_changes_cursor_column_time "
            "ON column_changes (column_id, changed_at, id)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_ingestion_quota_windows(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create shared quota window storage for replica-consistent limits."""
    del session_factory
    await connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS ingestion_quota_windows (
                id SERIAL PRIMARY KEY,
                dimension VARCHAR(32) NOT NULL,
                identity VARCHAR(256) NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )
    statements = [
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_ingestion_quota_windows_dimension_identity_window_start "
            "ON ingestion_quota_windows "
            "(dimension, identity, window_start)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_ingestion_quota_windows_dimension_identity "
            "ON ingestion_quota_windows (dimension, identity)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_api_audit_log_spill(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create the persistent spill table for audit logs."""
    del session_factory
    await connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS api_audit_log_spill (
                id SERIAL PRIMARY KEY,
                entry JSONB NOT NULL,
                error_message TEXT NULL,
                spilled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )
    await connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_api_audit_log_spill_spilled_at "
            "ON api_audit_log_spill (spilled_at)"
        )
    )
