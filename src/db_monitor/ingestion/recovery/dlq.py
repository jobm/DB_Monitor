"""Dead-letter queue persistence helpers and metric refresh."""

from __future__ import annotations

from collections.abc import Callable

from db_monitor.core.models import DeadLetterEvent, KafkaEvent
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.metrics import dlq_records_pending
from db_monitor.ingestion.recovery.broker import (
    _build_broker_location,
    _serialize_broker_fields,
)
from db_monitor.ingestion.runtime_state import consumer_runtimes
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert


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
