"""Schema and events migration steps — base tables, ingestion columns, backfill, offsets, row identity, unique constraint."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from db_monitor.core.models import Base
from db_monitor.core.migrations.legacy import migrate_legacy_kafka_events


async def _apply_base_schema(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create the current model-defined schema for fresh databases."""
    del session_factory
    await connection.run_sync(Base.metadata.create_all)


async def _apply_event_ingestion_columns(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Ensure idempotent-ingestion columns exist on legacy events tables."""
    del session_factory
    statements = [
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_topic VARCHAR(256)",
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_partition INTEGER",
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_offset BIGINT",
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_events_kafka_position "
            "ON events (kafka_topic, kafka_partition, kafka_offset)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_legacy_event_backfill(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Backfill structured events from the legacy raw event table."""
    del connection
    await migrate_legacy_kafka_events(session_factory)


async def _apply_bigint_kafka_offsets(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Widen Kafka offset columns so large offsets do not overflow int32."""
    del session_factory
    statements = [
        (
            "ALTER TABLE events ALTER COLUMN kafka_offset "
            "TYPE BIGINT USING kafka_offset::BIGINT"
        ),
        (
            "ALTER TABLE consumer_checkpoints ALTER COLUMN kafka_offset "
            "TYPE BIGINT USING kafka_offset::BIGINT"
        ),
        (
            "ALTER TABLE dead_letter_events ALTER COLUMN kafka_offset "
            "TYPE BIGINT USING kafka_offset::BIGINT"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_row_identity_columns(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Persist row identity on events and column changes for record history."""
    del session_factory
    statements = [
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS row_identity JSONB",
        (
            "CREATE INDEX IF NOT EXISTS ix_events_row_identity "
            "ON events USING GIN (row_identity)"
        ),
        (
            "ALTER TABLE column_changes ADD COLUMN IF NOT EXISTS "
            "row_identity JSONB"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_changes_row_identity "
            "ON column_changes USING GIN (row_identity)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_events_unique_constraint(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Replace unique index with unique constraint on partitioned events.

    PostgreSQL requires a UNIQUE CONSTRAINT (not merely a unique index)
    for INSERT ... ON CONFLICT to work on partitioned tables.  Databases
    that ran migration 0013 before this fix have a unique *index*
    instead of a constraint, which causes ON CONFLICT to fail at
    runtime with ``InvalidColumnReferenceError``.
    """
    del session_factory
    # Check whether the table is actually partitioned.
    partitioned = await connection.execute(
        text(
            """
            SELECT 1 FROM pg_partitioned_table
            WHERE partrelid = 'events'::regclass
            """
        )
    )
    if partitioned.fetchone() is None:
        return

    # Check if a UNIQUE CONSTRAINT (not just an index) already exists.
    constraint_exists = await connection.execute(
        text(
            """
            SELECT 1 FROM pg_constraint
            WHERE conname = 'ux_events_kafka_position'
              AND conrelid = 'events'::regclass
            """
        )
    )
    if constraint_exists.fetchone() is not None:
        return

    # Drop the existing unique index so we can recreate it as a
    # constraint with the same name.
    await connection.execute(
        text("DROP INDEX IF EXISTS ux_events_kafka_position")
    )
    await connection.execute(
        text(
            """
            ALTER TABLE events
            ADD CONSTRAINT ux_events_kafka_position
            UNIQUE (kafka_topic, kafka_partition, kafka_offset,
                    capture_time)
            """
        )
    )
