"""Consumer checkpoint and DLQ persistence helpers."""

from __future__ import annotations

from collections.abc import Callable

from config import KAFKA_CONSUMER_GROUP
from extensions import AsyncSessionLocal
from metrics import dlq_records_pending
from models import ConsumerCheckpoint, DeadLetterEvent, KafkaEvent
from consumer.runtime_state import consumer_runtimes
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert


def _build_broker_location(
    event: KafkaEvent,
    broker_kind: str,
) -> dict[str, object] | None:
    """Build broker-neutral recovery metadata for one consumed event."""
    if event.kafka_topic is None or event.kafka_offset is None:
        return None

    if broker_kind == "kafka":
        if event.kafka_partition is None:
            return None
        broker_substream = str(event.kafka_partition)
        kafka_topic = event.kafka_topic
        kafka_partition = event.kafka_partition
        kafka_offset = event.kafka_offset
    else:
        broker_substream = ""
        kafka_topic = None
        kafka_partition = None
        kafka_offset = None

    return {
        "broker_kind": broker_kind,
        "broker_destination": event.kafka_topic,
        "broker_substream": broker_substream,
        "broker_position": str(event.kafka_offset),
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }


def _legacy_event_location(
    broker_kind: str,
    broker_destination: str,
    broker_substream: str,
    broker_position: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> tuple[str, int | None, int | None]:
    """Build legacy Kafka location fields from broker metadata."""
    if kafka_topic is not None:
        return kafka_topic, kafka_partition, kafka_offset

    try:
        position = int(broker_position)
    except (TypeError, ValueError):
        position = None

    if broker_kind == "kafka":
        try:
            partition = int(broker_substream)
        except (TypeError, ValueError):
            partition = None
    else:
        partition = 0

    return broker_destination, partition, position


def _serialize_broker_fields(
    *,
    broker_kind: str,
    broker_destination: str,
    broker_substream: str,
    broker_position: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> dict[str, object]:
    """Render broker metadata plus Kafka compatibility aliases."""
    return {
        "broker_kind": broker_kind,
        "broker_destination": broker_destination,
        "broker_substream": broker_substream or None,
        "broker_position": broker_position,
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }


def _checkpoint_rows_for_events(
    events: list[KafkaEvent],
    consumer_group: str = KAFKA_CONSUMER_GROUP,
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
        if existing is None or int(location["broker_position"]) > int(
            existing["broker_position"]
        ):
            checkpoints[key] = {
                "consumer_group": consumer_group,
                **location,
                "last_event_time": event.event_time,
            }

    return list(checkpoints.values())


async def refresh_dead_letter_backlog_metric(
    session_factory: Callable = AsyncSessionLocal,
) -> int:
    """Refresh the pending DLQ gauge from persisted recovery records."""
    async with session_factory() as session:
        result = await session.execute(
            select(func.count())
            .select_from(DeadLetterEvent)
            .where(DeadLetterEvent.is_replayed.is_(False))
        )
        pending_count = int(result.scalar_one() or 0)

    dlq_records_pending.set(pending_count)
    for runtime in consumer_runtimes.values():
        runtime["dlq_messages_total"] = pending_count
    return pending_count


async def persist_consumer_checkpoints(
    events: list[KafkaEvent],
    consumer_group: str = KAFKA_CONSUMER_GROUP,
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


async def _persist_dead_letter_event(
    event: KafkaEvent,
    raw_payload: str,
    error: str,
    broker_kind: str = "kafka",
    session_factory: Callable = AsyncSessionLocal,
) -> int:
    """Store one failed message in the persistent DLQ table."""
    location = _build_broker_location(event, broker_kind)
    if location is None:
        raise ValueError("Dead-letter event is missing broker location.")

    async with session_factory() as session:
        async with session.begin():
            insert_stmt = pg_insert(DeadLetterEvent).values(
                service_name=event.service_name,
                **location,
                operation=event.operation,
                raw_payload=raw_payload,
                error_message=error,
                is_replayed=False,
                replayed_at=None,
                replay_error=None,
            )
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "broker_kind",
                    "broker_destination",
                    "broker_substream",
                    "broker_position",
                ],
                set_={
                    "service_name": insert_stmt.excluded.service_name,
                    "kafka_topic": insert_stmt.excluded.kafka_topic,
                    "kafka_partition": insert_stmt.excluded.kafka_partition,
                    "kafka_offset": insert_stmt.excluded.kafka_offset,
                    "operation": insert_stmt.excluded.operation,
                    "raw_payload": insert_stmt.excluded.raw_payload,
                    "error_message": insert_stmt.excluded.error_message,
                    "failed_at": func.now(),
                    "is_replayed": False,
                    "replayed_at": None,
                    "replay_error": None,
                },
            ).returning(DeadLetterEvent.id)
            result = await session.execute(upsert_stmt)
            return int(result.scalar_one())


async def list_dead_letter_events(
    limit: int = 100,
    include_replayed: bool = False,
    session_factory: Callable = AsyncSessionLocal,
) -> list[dict[str, object]]:
    """Return persisted DLQ records for operator inspection."""
    async with session_factory() as session:
        statement = select(DeadLetterEvent)
        if not include_replayed:
            statement = statement.where(DeadLetterEvent.is_replayed.is_(False))
        statement = statement.order_by(DeadLetterEvent.failed_at.desc()).limit(
            limit
        )
        result = await session.execute(statement)
        events = result.scalars().all()

    return [
        {
            "id": event.id,
            "service_name": event.service_name,
            **_serialize_broker_fields(
                broker_kind=event.broker_kind,
                broker_destination=event.broker_destination,
                broker_substream=event.broker_substream,
                broker_position=event.broker_position,
                kafka_topic=event.kafka_topic,
                kafka_partition=event.kafka_partition,
                kafka_offset=event.kafka_offset,
            ),
            "operation": event.operation,
            "error_message": event.error_message,
            "failed_at": (
                event.failed_at.isoformat() if event.failed_at else None
            ),
            "is_replayed": event.is_replayed,
            "replayed_at": (
                event.replayed_at.isoformat() if event.replayed_at else None
            ),
            "replay_error": event.replay_error,
            "raw_payload": event.raw_payload,
        }
        for event in events
    ]
