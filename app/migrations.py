"""Tracked schema and data migrations for DB Monitor.

The app no longer mutates schema implicitly on every startup.
Instead, startup either applies tracked migrations in
development-style environments or validates that all known
migrations have already been applied in production-style
environments.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from core.models import Base, KafkaEvent, KafkaEventLegacy
from event_parser import parse_event_payload
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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
        "ALTER TABLE events ADD COLUMN IF NOT EXISTS kafka_offset INTEGER",
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
            "CREATE INDEX IF NOT EXISTS ix_customer_provision_jobs_customer_id "
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
            "CREATE INDEX IF NOT EXISTS ix_api_audit_log_spill_spilled_at ON api_audit_log_spill (spilled_at)"
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
)

CURRENT_SCHEMA_VERSION = MIGRATIONS[-1].version


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
    """Apply all pending migrations and return their versions."""
    applied_versions: list[str] = []

    for migration in await get_pending_migrations(engine):
        async with engine.begin() as connection:
            await _ensure_migration_table(connection)
            await migration.apply(connection, session_factory)
            await _record_migration(connection, migration)
        applied_versions.append(migration.version)

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
        stream = await session.stream_scalars(
            select(KafkaEventLegacy).order_by(KafkaEventLegacy.id)
        )

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
