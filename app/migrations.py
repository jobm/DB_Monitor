"""Tracked schema and data migrations for DB Monitor.

The app no longer mutates schema implicitly on every startup.
Instead, startup either applies tracked migrations in
development-style environments or validates that all known
migrations have already been applied in production-style
environments.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from core.models import Base, KafkaEvent, KafkaEventLegacy
from event_parser import parse_event_payload
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

SCHEMA_MIGRATIONS_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    """Single tracked migration step."""

    version: str
    description: str
    apply: Callable[[AsyncConnection, Callable], Any]


async def _ensure_migration_table(connection: AsyncConnection) -> None:
    """Create the migration tracking table when it is missing."""
    await connection.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
                version VARCHAR(64) PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )


async def _get_applied_versions(connection: AsyncConnection) -> set[str]:
    """Return the set of tracked migration versions already applied."""
    result = await connection.execute(
        text(f"SELECT version FROM {SCHEMA_MIGRATIONS_TABLE} ORDER BY version")
    )
    return {row[0] for row in result.fetchall()}


async def _record_migration(
    connection: AsyncConnection,
    migration: Migration,
) -> None:
    """Persist a successful migration application in the tracking table."""
    await connection.execute(
        text(
            f"""
            INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (version, description)
            VALUES (:version, :description)
            ON CONFLICT (version) DO NOTHING
            """
        ),
        {
            "version": migration.version,
            "description": migration.description,
        },
    )


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


async def _apply_api_key_lifecycle_columns(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Add API key expiration and revocation lifecycle columns."""
    del session_factory
    statements = [
        "ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
        "ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ",
        (
            "CREATE INDEX IF NOT EXISTS ix_api_keys_expires_at "
            "ON api_keys (expires_at)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_api_keys_revoked_at "
            "ON api_keys (revoked_at)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


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


async def _apply_broker_recovery_metadata(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Add broker-neutral recovery metadata for checkpoints and DLQ rows."""
    del session_factory
    statements = [
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_kind VARCHAR(32)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_destination VARCHAR(256)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_substream VARCHAR(128)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_position VARCHAR(128)"
        ),
        (
            "UPDATE consumer_checkpoints "
            "SET broker_kind = COALESCE(broker_kind, 'kafka'), "
            "broker_destination = COALESCE(broker_destination, kafka_topic), "
            "broker_substream = COALESCE("
            "broker_substream, kafka_partition::TEXT, ''), "
            "broker_position = COALESCE("
            "broker_position, kafka_offset::TEXT)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_kind SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_destination SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_substream SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_position SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN kafka_topic DROP NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN kafka_partition DROP NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN kafka_offset DROP NOT NULL"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_consumer_checkpoints_group_broker_stream "
            "ON consumer_checkpoints "
            "(consumer_group, broker_kind, broker_destination, "
            "broker_substream)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_kind VARCHAR(32)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_destination VARCHAR(256)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_substream VARCHAR(128)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_position VARCHAR(128)"
        ),
        (
            "UPDATE dead_letter_events "
            "SET broker_kind = COALESCE(broker_kind, 'kafka'), "
            "broker_destination = COALESCE(broker_destination, kafka_topic), "
            "broker_substream = COALESCE("
            "broker_substream, kafka_partition::TEXT, ''), "
            "broker_position = COALESCE("
            "broker_position, kafka_offset::TEXT)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_kind SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_destination SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_substream SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_position SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN kafka_topic DROP NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN kafka_partition DROP NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN kafka_offset DROP NOT NULL"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_dead_letter_events_broker_position "
            "ON dead_letter_events "
            "(broker_kind, broker_destination, broker_substream, "
            "broker_position)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_customer_jwt_secret_state(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create durable customer JWT secret lifecycle storage."""
    del session_factory
    await connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS customer_jwt_secret_state (
                customer_id VARCHAR(128) PRIMARY KEY,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )


async def _apply_customer_control_plane_state(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create durable customer lifecycle and job state tables."""
    del session_factory
    statements = [
        """
        CREATE TABLE IF NOT EXISTS customer_lifecycle_state (
            customer_id VARCHAR(128) PRIMARY KEY,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_provision_jobs (
            job_id VARCHAR(64) PRIMARY KEY,
            customer_id VARCHAR(128) NOT NULL,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_customer_provision_jobs_customer_id "
            "ON customer_provision_jobs (customer_id)"
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
        # Recreate indexes (partition key required in unique indexes)
        await connection.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_events_kafka_position
                ON events (kafka_topic, kafka_partition, kafka_offset,
                           capture_time)
                """
            )
        )
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
        # Drop the legacy table and its indexes so the partitioned table
        # can claim the canonical index names.
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

    # Recreate FK from column_changes → events unconditionally once
    # both tables have been partitioned.
    fk_exists = await connection.execute(
        text(
            """
            SELECT 1 FROM pg_constraint
            WHERE conname = 'column_changes_event_id_fkey'
            """
        )
    )
    if fk_exists.fetchone() is None:
        await connection.execute(
            text(
                """
                ALTER TABLE column_changes
                ADD CONSTRAINT column_changes_event_id_fkey
                FOREIGN KEY (event_id)
                REFERENCES events (id)
                ON DELETE CASCADE
                """
            )
        )


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
)

CURRENT_SCHEMA_VERSION = MIGRATIONS[-1].version

# ── Advisory lock for concurrent migration safety ────────────────────
#
# Uses PostgreSQL advisory lock to ensure only one process can execute
# migrations at a time. The lock ID is derived from a well-known constant
# so all replicas contend on the same lock.

_MIGRATION_LOCK_ID = 7273646  # "dbmonitor" on a phone keypad


async def _acquire_migration_lock(
    connection: AsyncConnection,
) -> bool:
    """Try to acquire the migration advisory lock. Returns True on success."""
    result = await connection.execute(
        text("SELECT pg_try_advisory_lock(:lock_id)"),
        {"lock_id": _MIGRATION_LOCK_ID},
    )
    return bool(result.scalar())


async def _release_migration_lock(
    connection: AsyncConnection,
) -> None:
    """Release the migration advisory lock."""
    await connection.execute(
        text("SELECT pg_advisory_unlock(:lock_id)"),
        {"lock_id": _MIGRATION_LOCK_ID},
    )


async def get_pending_migrations(engine: AsyncEngine) -> list[Migration]:
    """Return the ordered list of migrations that have not been applied."""
    async with engine.begin() as connection:
        await _ensure_migration_table(connection)
        applied_versions = await _get_applied_versions(connection)

    return [
        migration
        for migration in MIGRATIONS
        if migration.version not in applied_versions
    ]


async def apply_migrations(
    session_factory: Callable,
    engine: AsyncEngine,
) -> list[str]:
    """Apply all pending migrations and return their versions.

    Acquires a PostgreSQL advisory lock before running any migration
    steps. When the lock is held by another process this function logs a
    clear message and raises ``RuntimeError`` so callers can retry or
    exit cleanly.
    """
    applied_versions: list[str] = []

    async with engine.connect() as connection:
        await _ensure_migration_table(connection)
        await connection.commit()

        if not await _acquire_migration_lock(connection):
            raise RuntimeError(
                "Migration lock is held by another process. "
                "Only one actor can run schema changes at a time. "
                "Retry once the other migration completes."
            )
        await connection.commit()

        try:
            async with connection.begin():
                pending = [
                    m
                    for m in MIGRATIONS
                    if m.version
                    not in await _get_applied_versions(connection)
                ]

                for migration in pending:
                    await migration.apply(connection, session_factory)
                    await _record_migration(connection, migration)
                    applied_versions.append(migration.version)
        finally:
            await _release_migration_lock(connection)

    return applied_versions


async def validate_migrations(engine: AsyncEngine) -> None:
    """Raise when the database is missing required tracked migrations."""
    pending = await get_pending_migrations(engine)
    if pending:
        pending_versions = ", ".join(
            migration.version for migration in pending
        )
        raise RuntimeError(
            "Database schema is not up to date. "
            "Run the migration command before starting the app. "
            f"Pending migrations: {pending_versions}"
        )


async def ensure_event_ingestion_columns(session_factory: Callable) -> None:
    """Backward-compatible helper for targeted legacy ingestion upgrades."""

    statements = [
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_topic VARCHAR(256)",
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_partition INTEGER",
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_offset BIGINT",
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_events_kafka_position "
            "ON events (kafka_topic, kafka_partition, kafka_offset)"
        ),
    ]

    async with session_factory() as session:
        for statement in statements:
            await session.execute(text(statement))
        await session.commit()


async def migrate_legacy_kafka_events(
    session_factory: Callable,
    batch_size: int = 500,
) -> int:
    """Migrate rows from legacy `kafka_events` into structured `events`.

    This is safe to run multiple times:
    - Inserts use `ON CONFLICT DO NOTHING` on primary key `id`.

    Returns:
        Number of rows inserted into `events`.
    """

    inserted_total = 0

    async with session_factory() as session:
        try:
            stream = await session.stream_scalars(
                select(KafkaEventLegacy).order_by(KafkaEventLegacy.id)
            )
        except ProgrammingError as exc:
            if 'relation "kafka_events" does not exist' in str(exc):
                return 0
            raise

        batch: list[dict[str, Any]] = []
        async for legacy in stream:
            parsed = parse_event_payload(legacy.value)
            batch.append(
                {
                    "id": legacy.id,
                    "event_type": parsed.event_type,
                    "event_time": parsed.event_time,
                    "user_id": parsed.user_id,
                    "service_name": parsed.service_name,
                    "event_data": parsed.event_data,
                    "raw_payload": parsed.raw_payload,
                }
            )

            if len(batch) >= batch_size:
                stmt = (
                    pg_insert(KafkaEvent)
                    .values(batch)
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                result = await session.execute(stmt)
                inserted_total += int(result.rowcount or 0)
                batch = []

        if batch:
            stmt = (
                pg_insert(KafkaEvent)
                .values(batch)
                .on_conflict_do_nothing(index_elements=["id"])
            )
            result = await session.execute(stmt)
            inserted_total += int(result.rowcount or 0)

        await session.commit()

    return inserted_total


async def create_monthly_partitions(
    connection: AsyncConnection,
    table_name: str,
    partition_column: str,
    months_ahead: int = 3,
) -> list[str]:
    """Create monthly range partitions for the upcoming months.

    Idempotent — skips partitions that already exist. Returns the names
    of newly created partitions.
    """
    if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
        raise ValueError(
            f"Invalid table_name for partition DDL: {table_name!r}"
        )

    now = datetime.now(timezone.utc)
    created: list[str] = []

    for offset in range(months_ahead):
        month_start = _month_start(now, offset)
        month_end = _month_start(now, offset + 1)
        partition_name = (
            f"{table_name}_{month_start.strftime('%Y%m')}"
        )

        exists = await connection.execute(
            text(
                """
                SELECT 1 FROM pg_class
                WHERE relname = :partition_name
                """
            ),
            {"partition_name": partition_name},
        )
        if exists.fetchone() is not None:
            continue

        await connection.execute(
            text(
                f"""
                CREATE TABLE {partition_name}
                PARTITION OF {table_name}
                FOR VALUES FROM (
                '{month_start.isoformat()}') TO
                ('{month_end.isoformat()}')
                """
            )
        )
        created.append(partition_name)

    return created


def _month_start(reference: datetime, months_ahead: int) -> datetime:
    """Return the first instant of a month offset from ``reference``."""
    total_months = reference.year * 12 + (reference.month - 1) + months_ahead
    year = total_months // 12
    month = total_months % 12 + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


async def drop_old_partitions(
    connection: AsyncConnection,
    table_name: str,
    partition_column: str,
    older_than: datetime,
) -> list[str]:
    """Drop range partitions whose upper bound is before ``older_than``.

    Only drops partitions that are entirely before the cutoff — does not
    touch the DEFAULT partition. Returns the names of dropped partitions.
    """
    if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
        raise ValueError(
            f"Invalid table_name for partition DDL: {table_name!r}"
        )

    # Find child partitions older than the cutoff
    rows = await connection.execute(
        text(
            """
            SELECT c.relname AS partition_name,
                   pg_get_expr(c.relpartbound, c.oid) AS bound
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = :table_name
              AND c.relispartition = TRUE
              AND c.relname != :default_name
            """
        ),
        {
            "table_name": table_name,
            "default_name": f"{table_name}_default",
        },
    )

    dropped: list[str] = []
    for partition_name, bound_expr in rows.fetchall():
        if bound_expr is None:
            continue
        # bound_expr is e.g. "FOR VALUES FROM ('2026-01-01')
        # TO ('2026-02-01')"  — extract the upper bound
        upper_str = _extract_upper_bound(bound_expr)
        if upper_str is None:
            continue
        try:
            upper = datetime.fromisoformat(upper_str)
        except ValueError:
            continue

        if upper <= older_than:
            await connection.execute(
                text(f"DROP TABLE IF EXISTS {partition_name}")
            )
            dropped.append(partition_name)
            logger.info(
                "Dropped old partition %s (upper bound %s <= cutoff %s)",
                partition_name,
                upper.isoformat(),
                older_than.isoformat(),
            )

    return dropped


def _extract_upper_bound(bound_expr: str) -> str | None:
    """Extract the upper bound value from a PG partition bound expression."""
    match = re.search(r"TO\s*\(\s*'([^']+)'\s*\)", bound_expr)
    if match:
        return match.group(1)
    return None
