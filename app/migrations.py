"""Best-effort data migrations for DB Monitor.

This project currently uses `Base.metadata.create_all()` on startup rather than a
formal migration framework. Until Alembic is introduced, we keep migrations here
and run them during `init_db()`.

High Priority TODO #1 requires a migration path from the legacy `kafka_events`
(raw text) table to the new structured `events` table.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from event_parser import parse_event_payload
from models import KafkaEvent, KafkaEventLegacy


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
