"""Schema and events migration steps — base tables, ingestion columns, backfill, offsets, row identity, unique constraint."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from db_monitor.core.migrations.legacy import migrate_legacy_kafka_events


async def _apply_base_schema(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create the full DB Monitor schema using explicit DDL statements.

    This replaces the previous ``Base.metadata.create_all`` so that
    every DDL operation is versioned and traceable.  Tables are created
    in dependency order so foreign-key constraints can be declared
    inline.
    """
    del session_factory

    statements = [
        # ── Tables without FK dependencies ──────────────────────────────
        # kafka_events (KafkaEventLegacy)
        """
        CREATE TABLE IF NOT EXISTS kafka_events (
            id SERIAL PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        # api_keys (ApiKey)
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            id SERIAL PRIMARY KEY,
            key_hash VARCHAR(255) NOT NULL,
            owner_name VARCHAR(128) NOT NULL,
            role VARCHAR(32) NOT NULL DEFAULT 'viewer',
            is_active BOOLEAN DEFAULT TRUE,
            expires_at TIMESTAMPTZ,
            revoked_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # monitored_tables (MonitoredTable)
        """
        CREATE TABLE IF NOT EXISTS monitored_tables (
            id SERIAL PRIMARY KEY,
            service_name VARCHAR(128) NOT NULL,
            database_name VARCHAR(128) NOT NULL,
            table_name VARCHAR(128) NOT NULL,
            topic_name VARCHAR(256) NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # customer_jwt_secret_state (CustomerJWTSecretState)
        """
        CREATE TABLE IF NOT EXISTS customer_jwt_secret_state (
            customer_id VARCHAR(128) PRIMARY KEY,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # customer_lifecycle_state (CustomerLifecycleState)
        """
        CREATE TABLE IF NOT EXISTS customer_lifecycle_state (
            customer_id VARCHAR(128) PRIMARY KEY,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # customer_provision_jobs (CustomerProvisionJob)
        """
        CREATE TABLE IF NOT EXISTS customer_provision_jobs (
            job_id VARCHAR(64) PRIMARY KEY,
            customer_id VARCHAR(128) NOT NULL,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # ingestion_quota_windows (IngestionQuotaWindow)
        """
        CREATE TABLE IF NOT EXISTS ingestion_quota_windows (
            id SERIAL PRIMARY KEY,
            dimension VARCHAR(32) NOT NULL,
            identity VARCHAR(256) NOT NULL,
            window_start TIMESTAMPTZ NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # consumer_checkpoints (ConsumerCheckpoint)
        """
        CREATE TABLE IF NOT EXISTS consumer_checkpoints (
            id SERIAL PRIMARY KEY,
            consumer_group VARCHAR(128) NOT NULL,
            broker_kind VARCHAR(32) NOT NULL,
            broker_destination VARCHAR(256) NOT NULL,
            broker_substream VARCHAR(128) NOT NULL DEFAULT '',
            broker_position VARCHAR(128) NOT NULL,
            kafka_topic VARCHAR(256),
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            last_event_time TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # dead_letter_events (DeadLetterEvent)
        """
        CREATE TABLE IF NOT EXISTS dead_letter_events (
            id SERIAL PRIMARY KEY,
            service_name VARCHAR(128),
            broker_kind VARCHAR(32) NOT NULL,
            broker_destination VARCHAR(256) NOT NULL,
            broker_substream VARCHAR(128) NOT NULL DEFAULT '',
            broker_position VARCHAR(128) NOT NULL,
            kafka_topic VARCHAR(256),
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            operation VARCHAR(16),
            raw_payload TEXT NOT NULL,
            error_message TEXT NOT NULL,
            failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_replayed BOOLEAN NOT NULL DEFAULT FALSE,
            replayed_at TIMESTAMPTZ,
            replay_error TEXT
        )
        """,
        # api_audit_log_spill (ApiAuditLogSpill) — includes replay columns
        """
        CREATE TABLE IF NOT EXISTS api_audit_log_spill (
            id SERIAL PRIMARY KEY,
            entry JSONB NOT NULL,
            error_message TEXT,
            spilled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            replayed BOOLEAN NOT NULL DEFAULT FALSE,
            replayed_by VARCHAR(128),
            replayed_at TIMESTAMPTZ
        )
        """,

        # ── Tables with FK dependencies ─────────────────────────────────
        # events (KafkaEvent) — FK to monitored_tables
        """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(64) NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            user_id VARCHAR(128),
            service_name VARCHAR(128),
            kafka_topic VARCHAR(256),
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            source_table_id INTEGER REFERENCES monitored_tables(id),
            operation VARCHAR(16),
            row_identity JSONB,
            event_data JSONB,
            raw_payload TEXT NOT NULL,
            capture_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # monitored_columns (MonitoredColumn) — FK to monitored_tables
        """
        CREATE TABLE IF NOT EXISTS monitored_columns (
            id SERIAL PRIMARY KEY,
            table_id INTEGER NOT NULL REFERENCES monitored_tables(id) ON DELETE CASCADE,
            column_name VARCHAR(128) NOT NULL,
            data_type VARCHAR(64) NOT NULL,
            is_primary_key BOOLEAN DEFAULT FALSE,
            is_nullable BOOLEAN DEFAULT TRUE,
            audit_enabled BOOLEAN DEFAULT TRUE
        )
        """,
        # api_audit_logs (ApiAuditLog) — FK to api_keys
        """
        CREATE TABLE IF NOT EXISTS api_audit_logs (
            id SERIAL PRIMARY KEY,
            api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
            endpoint VARCHAR(255) NOT NULL,
            method VARCHAR(16) NOT NULL,
            status_code INTEGER NOT NULL,
            ip_address VARCHAR(64),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # column_changes (ColumnChange) — FKs to events, monitored_tables, monitored_columns
        """
        CREATE TABLE IF NOT EXISTS column_changes (
            id SERIAL PRIMARY KEY,
            event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            table_id INTEGER NOT NULL REFERENCES monitored_tables(id) ON DELETE CASCADE,
            column_id INTEGER NOT NULL REFERENCES monitored_columns(id) ON DELETE CASCADE,
            operation VARCHAR(16) NOT NULL,
            row_identity JSONB,
            old_value JSONB,
            new_value JSONB,
            changed_at TIMESTAMPTZ NOT NULL
        )
        """,

        # ── Indexes and constraints ─────────────────────────────────────
        # monitored_tables unique index
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_monitored_tables_service_db_table
            ON monitored_tables (service_name, database_name, table_name)
        """,
        # monitored_columns indexes
        """
        CREATE INDEX IF NOT EXISTS ix_monitored_columns_table_id
            ON monitored_columns (table_id)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_monitored_columns_table_column
            ON monitored_columns (table_id, column_name)
        """,
        # api_keys indexes
        """
        CREATE INDEX IF NOT EXISTS ix_api_keys_expires_at
            ON api_keys (expires_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_api_keys_revoked_at
            ON api_keys (revoked_at)
        """,
        # events indexes (all the ones defined in the ORM model)
        """
        CREATE INDEX IF NOT EXISTS ix_events_type_time
            ON events (event_type, event_time)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_events_source_table_time
            ON events (source_table_id, event_time)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_events_cursor_service_type_time
            ON events (service_name, event_type, capture_time, id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_events_cursor_source_table_time
            ON events (source_table_id, capture_time, id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_events_row_identity
            ON events USING GIN (row_identity)
        """,
        # events unique constraint (replaced by migration 0015 on partitioned tables)
        # NOTE: Wrapped in a DO block because PostgreSQL does not support
        # ``ALTER TABLE ADD CONSTRAINT IF NOT EXISTS``.
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ux_events_kafka_position'
                  AND conrelid = 'events'::regclass
            ) THEN
                ALTER TABLE events ADD CONSTRAINT ux_events_kafka_position
                    UNIQUE (kafka_topic, kafka_partition, kafka_offset, capture_time);
            END IF;
        END;
        $$
        """,
        # customer_provision_jobs index
        """
        CREATE INDEX IF NOT EXISTS ix_customer_provision_jobs_customer_id
            ON customer_provision_jobs (customer_id)
        """,
        # ingestion_quota_windows indexes
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ingestion_quota_windows_dimension_identity_window_start
            ON ingestion_quota_windows (dimension, identity, window_start)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_ingestion_quota_windows_dimension_identity
            ON ingestion_quota_windows (dimension, identity)
        """,
        # consumer_checkpoints indexes
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_consumer_checkpoints_group_broker_stream
            ON consumer_checkpoints (consumer_group, broker_kind, broker_destination, broker_substream)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_consumer_checkpoints_group_topic_partition
            ON consumer_checkpoints (consumer_group, kafka_topic, kafka_partition)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_consumer_checkpoints_updated_at
            ON consumer_checkpoints (updated_at)
        """,
        # dead_letter_events indexes
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_dead_letter_events_broker_position
            ON dead_letter_events (broker_kind, broker_destination, broker_substream, broker_position)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_dead_letter_events_kafka_position
            ON dead_letter_events (kafka_topic, kafka_partition, kafka_offset)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_dead_letter_events_failed_at
            ON dead_letter_events (failed_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_dead_letter_events_is_replayed
            ON dead_letter_events (is_replayed)
        """,
        # api_audit_log_spill index
        """
        CREATE INDEX IF NOT EXISTS ix_api_audit_log_spill_spilled_at
            ON api_audit_log_spill (spilled_at)
        """,
        # api_audit_logs indexes
        """
        CREATE INDEX IF NOT EXISTS ix_api_audit_logs_api_key_time
            ON api_audit_logs (api_key_id, timestamp)
        """,
        # column_changes indexes (from the ORM model)
        """
        CREATE INDEX IF NOT EXISTS ix_changes_table_col_time
            ON column_changes (table_id, column_id, changed_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_changes_cursor_table_time
            ON column_changes (table_id, changed_at, id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_changes_cursor_column_time
            ON column_changes (column_id, changed_at, id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_changes_row_identity
            ON column_changes USING GIN (row_identity)
        """,
    ]
    for statement in statements:
        await connection.execute(text(statement))


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
