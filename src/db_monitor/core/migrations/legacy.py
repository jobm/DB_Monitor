"""Legacy migration helpers for backward-compatible upgrades."""

from __future__ import annotations

import logging
from typing import Any

from db_monitor.core.models import KafkaEvent, KafkaEventLegacy
from db_monitor.ingestion.event_parser import parse_event_payload
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import ProgrammingError

logger = logging.getLogger(__name__)


async def ensure_event_ingestion_columns(session_factory: Any) -> None:
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
    session_factory: Any,
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
