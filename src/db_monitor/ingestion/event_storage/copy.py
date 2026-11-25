"""PostgreSQL COPY-based bulk insertion for event batches.

The consumer writes events one batch per transaction. A multi-row
``INSERT ... ON CONFLICT`` still serializes every row through the extended
query protocol, which dominated the batch write at ~100 events. This module
streams the batch with ``COPY`` into a per-transaction staging table and then
performs a single ``INSERT ... SELECT ... ON CONFLICT DO NOTHING`` so
duplicate Kafka positions are still skipped and inserted ids are returned.

The staging table is created with ``ON COMMIT DROP`` so it disappears with
the caller's transaction and never leaks between batches.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.pg_copy import asyncpg_connection, jsonb

EventKey = tuple[str | None, int | None, int | None]

_STAGING_TABLE = "events_batch_staging"

_EVENT_COLUMNS: tuple[str, ...] = (
    "event_type",
    "event_time",
    "user_id",
    "service_name",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "source_table_id",
    "operation",
    "row_identity",
    "event_data",
    "raw_payload",
    "capture_time",
    "ingested_at",
)

# Staging columns are deliberately nullable and defaultless: the batch always
# supplies every value, and avoiding ``LIKE events`` keeps the sequence
# attached to ``events.id`` from being advanced by staging rows.
_CREATE_STAGING_SQL = f"""
CREATE TEMP TABLE {_STAGING_TABLE} (
    event_type text,
    event_time timestamptz,
    user_id text,
    service_name text,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    source_table_id integer,
    operation text,
    row_identity jsonb,
    event_data jsonb,
    raw_payload text,
    capture_time timestamptz,
    ingested_at timestamptz
) ON COMMIT DROP
"""

_INSERT_FROM_STAGING_SQL = f"""
INSERT INTO events ({", ".join(_EVENT_COLUMNS)})
SELECT {", ".join(_EVENT_COLUMNS)}
FROM {_STAGING_TABLE}
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset, capture_time)
DO NOTHING
RETURNING id, kafka_topic, kafka_partition, kafka_offset
"""


def _event_record(event_obj: KafkaEvent, now: datetime) -> tuple[Any, ...]:
    """Flatten one event into staging-column order."""
    return (
        event_obj.event_type,
        event_obj.event_time,
        event_obj.user_id,
        event_obj.service_name,
        event_obj.kafka_topic,
        event_obj.kafka_partition,
        event_obj.kafka_offset,
        event_obj.source_table_id,
        event_obj.operation,
        jsonb(event_obj.row_identity),
        jsonb(event_obj.event_data),
        event_obj.raw_payload,
        (
            event_obj.event_time
            if event_obj.event_time is not None
            else now
        ),
        now,
    )


async def copy_events_batch(
    session: AsyncSession,
    events: list[KafkaEvent],
) -> dict[EventKey, int | None]:
    """COPY ``events`` into staging and insert the batch with deduplication.

    Returns a mapping of Kafka position to the id of each newly inserted
    event. Positions already present in ``events`` are absent from the
    result, matching ``ON CONFLICT DO NOTHING`` semantics.
    """
    if not events:
        return {}

    now = datetime.now(timezone.utc)
    records = [_event_record(event_obj, now) for event_obj in events]

    await session.execute(text(_CREATE_STAGING_SQL))
    driver_connection = await asyncpg_connection(session)
    await driver_connection.copy_records_to_table(
        _STAGING_TABLE,
        records=records,
        columns=list(_EVENT_COLUMNS),
    )

    result = await session.execute(text(_INSERT_FROM_STAGING_SQL))
    inserted: dict[EventKey, int | None] = {}
    for row in result.all():
        inserted[
            (row.kafka_topic, row.kafka_partition, row.kafka_offset)
        ] = row.id
    return inserted
