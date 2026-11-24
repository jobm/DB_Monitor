"""Consumer checkpoint persistence and snapshot queries."""

from __future__ import annotations

from collections.abc import Callable

from db_monitor.core.config import settings
from db_monitor.core.models import ConsumerCheckpoint, KafkaEvent
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.recovery.broker import (
    _build_broker_location,
    _serialize_broker_fields,
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import func


def _checkpoint_rows_for_events(
    events: list[KafkaEvent],
    consumer_group: str = settings.broker.kafka_consumer_group,
    broker_kind: str = "kafka",
) -> list[dict[str, object]]:
    """Return highest committed broker positions for a processed batch."""
    checkpoints: dict[tuple[str, str, str], dict[str, object]] = {}
    for event in events:
        location = _build_broker_location(event, broker_kind)
        if location is None:
            continue

        key = (
            str(location["broker_destination"]),
            str(location["broker_substream"]),
            str(location["broker_kind"]),
        )
        existing = checkpoints.get(key)
        if existing is None or int(
            str(location["broker_position"])
        ) > int(str(existing["broker_position"])):
            checkpoints[key] = {
                "consumer_group": consumer_group,
                **location,
                "last_event_time": event.event_time,
            }

    return list(checkpoints.values())


async def persist_consumer_checkpoints(
    events: list[KafkaEvent],
    consumer_group: str = settings.broker.kafka_consumer_group,
    broker_kind: str = "kafka",
    session_factory: Callable = AsyncSessionLocal,
) -> None:
    """Persist the latest committed broker positions per stream."""
    checkpoint_rows = _checkpoint_rows_for_events(
        events,
        consumer_group,
        broker_kind,
    )
    if not checkpoint_rows:
        return

    async with session_factory() as session:
        async with session.begin():
            insert_stmt = pg_insert(ConsumerCheckpoint).values(checkpoint_rows)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "consumer_group",
                    "broker_kind",
                    "broker_destination",
                    "broker_substream",
                ],
                set_={
                    "broker_position": insert_stmt.excluded.broker_position,
                    "kafka_topic": insert_stmt.excluded.kafka_topic,
                    "kafka_partition": insert_stmt.excluded.kafka_partition,
                    "kafka_offset": insert_stmt.excluded.kafka_offset,
                    "last_event_time": insert_stmt.excluded.last_event_time,
                    "updated_at": func.now(),
                },
            )
            await session.execute(upsert_stmt)


async def list_consumer_checkpoints_snapshot(
    session_factory: Callable = AsyncSessionLocal,
) -> list[dict[str, object]]:
    """Return the persisted checkpoint snapshot for operators."""
    async with session_factory() as session:
        result = await session.execute(
            select(ConsumerCheckpoint).order_by(
                ConsumerCheckpoint.consumer_group.asc(),
                ConsumerCheckpoint.broker_kind.asc(),
                ConsumerCheckpoint.broker_destination.asc(),
                ConsumerCheckpoint.broker_substream.asc(),
            )
        )
        checkpoints = result.scalars().all()

    return [
        {
            "consumer_group": checkpoint.consumer_group,
            **_serialize_broker_fields(
                broker_kind=checkpoint.broker_kind,
                broker_destination=checkpoint.broker_destination,
                broker_substream=checkpoint.broker_substream,
                broker_position=checkpoint.broker_position,
                kafka_topic=checkpoint.kafka_topic,
                kafka_partition=checkpoint.kafka_partition,
                kafka_offset=checkpoint.kafka_offset,
            ),
            "last_event_time": (
                checkpoint.last_event_time.isoformat()
                if checkpoint.last_event_time
                else None
            ),
            "updated_at": (
                checkpoint.updated_at.isoformat()
                if checkpoint.updated_at
                else None
            ),
        }
        for checkpoint in checkpoints
    ]
