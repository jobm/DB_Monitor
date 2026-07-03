"""Event partitioning migration — convert events and column_changes to partitioned tables."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def _apply_event_partitioning(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Convert events and column_changes to time-range partitioned tables.

    Partitioning ensures that retention cleanup can DROP entire partitions
    instead of issuing expensive DELETE statements on billion-row tables.
    This migration is idempotent — it skips tables that are already
    partitioned.
    """
    del session_factory

    # Drop FK from column_changes → events upfront so events_old can
    # always be dropped without dependent-object errors.
    await connection.execute(
        text(
            """
            ALTER TABLE column_changes
            DROP CONSTRAINT IF EXISTS column_changes_event_id_fkey
            """
        )
    )

    # ── events ──────────────────────────────────────────────────────
    partitioned = await connection.execute(
        text(
            """
            SELECT 1 FROM pg_partitioned_table
            WHERE partrelid = 'events'::regclass
            """
        )
    )
    if partitioned.fetchone() is None:
        # Drop the UNIQUE CONSTRAINT that migration 0001 created via
        # Base.metadata.create_all (UniqueConstraint on KafkaEvent).
        await connection.execute(
            text(
                """
                ALTER TABLE events
                DROP CONSTRAINT IF EXISTS ux_events_kafka_position
                """
            )
        )
        await connection.execute(
            text(
                """
                ALTER TABLE events RENAME TO events_old
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE TABLE events (
                    id SERIAL,
                    event_type VARCHAR(64) NOT NULL,
                    event_time TIMESTAMPTZ NOT NULL,
                    user_id VARCHAR(128),
                    service_name VARCHAR(128),
                    kafka_topic VARCHAR(256),
                    kafka_partition INTEGER,
                    kafka_offset BIGINT,
                    source_table_id INTEGER,
                    operation VARCHAR(16),
                    row_identity JSONB,
                    event_data JSONB,
                    raw_payload TEXT NOT NULL,
                    capture_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
                ) PARTITION BY RANGE (capture_time)
                """
            )
        )
        # NOTE: We intentionally do NOT add the UNIQUE CONSTRAINT here.
        # Migration 0015 (_apply_events_unique_constraint) handles adding
        # the constraint to the partitioned table.  Keeping it in 0013
        # caused DuplicateTableError on fresh databases because the
        # constraint name is schema-scoped and may collide with the
        # backing index from Base.metadata.create_all.
        await connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_events_type_time
                ON events (event_type, event_time)
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_events_source_table_time
                ON events (source_table_id, event_time)
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_events_row_identity
                ON events USING GIN (row_identity)
                """
            )
        )
        # Create a default partition to hold legacy rows
        await connection.execute(
            text(
                """
                CREATE TABLE events_default PARTITION OF events DEFAULT
                """
            )
        )
        # Migrate rows from old table into default partition
        await connection.execute(
            text(
                """
                INSERT INTO events
                SELECT * FROM events_old
                """
            )
        )
        # Drop the legacy table and its indexes
        await connection.execute(
            text("DROP TABLE IF EXISTS events_old")
        )

    # ── column_changes ──────────────────────────────────────────────
    partitioned = await connection.execute(
        text(
            """
            SELECT 1 FROM pg_partitioned_table
            WHERE partrelid = 'column_changes'::regclass
            """
        )
    )
    if partitioned.fetchone() is None:
        await connection.execute(
            text(
                """
                ALTER TABLE column_changes RENAME TO column_changes_old
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE TABLE column_changes (
                    id SERIAL,
                    event_id INTEGER,
                    table_id INTEGER,
                    column_id INTEGER,
                    operation VARCHAR(16),
                    row_identity JSONB,
                    old_value JSONB,
                    new_value JSONB,
                    changed_at TIMESTAMPTZ NOT NULL
                ) PARTITION BY RANGE (changed_at)
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_column_changes_event_id
                ON column_changes (event_id)
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_column_changes_table_id
                ON column_changes (table_id)
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_column_changes_changed_at
                ON column_changes (changed_at)
                """
            )
        )
        await connection.execute(
            text(
                """
                CREATE TABLE column_changes_default
                PARTITION OF column_changes DEFAULT
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO column_changes
                SELECT * FROM column_changes_old
                """
            )
        )
        await connection.execute(
            text("DROP TABLE IF EXISTS column_changes_old")
        )
    # NOTE: We intentionally do not recreate the FK from
    # column_changes → events.  PostgreSQL requires FKs referencing
    # partitioned tables to include all partition-key columns, but
    # column_changes has no capture_time.  In practice partition DROPs
    # (used by retention cleanup) do not trigger FK cascades, so the
    # FK provides no operational benefit in a partitioned layout.
