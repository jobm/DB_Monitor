"""Dead-letter queue persistence, replay, and backlog tracking."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone

from db_monitor.core.models import DeadLetterEvent, KafkaEvent
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.brokers import DeadLetterPublisher
from db_monitor.ingestion.event_parser import parse_event_payload
from db_monitor.ingestion.event_pipeline import event_pipeline
from db_monitor.ingestion.recovery import (
    _persist_dead_letter_event,
    _legacy_event_location,
    _get_canonical_service_name,
    refresh_dead_letter_backlog_metric,
)
from db_monitor.ingestion.runtime_state import consumer_runtime
from db_monitor.schema import extract_operation
from db_monitor.ingestion.event_storage import _store_event_graph_with_retries
from db_monitor.ingestion.broadcast import _broadcast_event
from db_monitor.metrics import dlq_messages_total
from db_monitor.tracing import start_span
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

DLQ_TOPIC = "db-monitor-dlq"


async def send_to_dlq(
    dlq_publisher: DeadLetterPublisher | None,
    event: KafkaEvent,
    error: str,
    raw_message: bytes | None = None,
    dlq_destination: str = DLQ_TOPIC,
    broker_kind: str = "kafka",
) -> bool:
    """Persist and optionally forward a failed message to the DLQ topic."""
    raw_payload = (
        raw_message.decode("utf-8")
        if raw_message is not None
        else event.raw_payload
    )
    if raw_payload is None:
        raw_payload = ""
    try:
        dlq_record_id = await _persist_dead_letter_event(
            event,
            raw_payload,
            error,
            broker_kind=broker_kind,
        )
    except Exception as exc:
        logger.exception("Failed to persist dead-letter event: %s", exc)
        return False

    if dlq_publisher:
        try:
            await dlq_publisher.publish_dead_letter(
                raw_payload=raw_payload,
                source_destination=event.kafka_topic,
                source_partition=event.kafka_partition,
                source_offset=event.kafka_offset,
                error=error,
                dlq_record_id=dlq_record_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to forward message to DLQ destination %s: %s",
                dlq_destination,
                exc,
            )

    dlq_messages_total.inc()
    await refresh_dead_letter_backlog_metric()
    logger.info(
        (
            "Message persisted to DLQ record %s for destination=%s "
            "partition=%s offset=%s"
        ),
        dlq_record_id,
        event.kafka_topic,
        event.kafka_partition,
        event.kafka_offset,
    )
    return True


def _build_replay_event(dlq_event: DeadLetterEvent) -> KafkaEvent:
    """Reconstruct a KafkaEvent from a persisted DLQ record."""
    parsed = parse_event_payload(dlq_event.raw_payload or "")
    operation = extract_operation(parsed.event_data or {})
    destination, partition, offset = _legacy_event_location(
        dlq_event.broker_kind or "",
        dlq_event.broker_destination or "",
        dlq_event.broker_substream or "",
        dlq_event.broker_position or "",
        dlq_event.kafka_topic,
        dlq_event.kafka_partition,
        dlq_event.kafka_offset,
    )
    return KafkaEvent(
        event_type=parsed.event_type,
        event_time=parsed.event_time,
        user_id=parsed.user_id,
        service_name=_get_canonical_service_name(
            destination,
            dlq_event.service_name,
        ),
        kafka_topic=destination,
        kafka_partition=partition,
        kafka_offset=offset,
        event_data=parsed.event_data,
        raw_payload=parsed.raw_payload,
        operation=operation,
    )


async def replay_dead_letter_event_record(
    dlq_event_id: int,
    session_factory: Callable[[], AsyncSession] = (
        AsyncSessionLocal
    ),
) -> dict[str, object]:
    """Replay a persisted DLQ record through the normal ingestion path."""
    with start_span(
        "dlq.replay.record",
        attributes={"db_monitor.dlq_event_id": dlq_event_id},
    ):
        async with session_factory() as session:  # type: ignore[type-var,attr-defined]
            result = await session.execute(
                select(DeadLetterEvent).where(
                    DeadLetterEvent.id == dlq_event_id
                )
            )
            dlq_event = result.scalar_one_or_none()

        if dlq_event is None:
            raise LookupError("DLQ event not found")

        replay_event = _build_replay_event(dlq_event)
        inserted = False
        replay_status = "skipped"

        try:
            if event_pipeline.should_process(replay_event):
                replay_event = event_pipeline.transform(replay_event)
                inserted = await _store_event_graph_with_retries(
                    session_factory,
                    replay_event,
                    max_retries=5,
                )
                replay_status = "replayed" if inserted else "duplicate"
                if inserted:
                    await _broadcast_event(replay_event)

            async with session_factory() as session:  # type: ignore[type-var,attr-defined]
                async with session.begin():
                    result = await session.execute(
                        select(DeadLetterEvent).where(
                            DeadLetterEvent.id == dlq_event_id
                        )
                    )
                    replay_row = result.scalar_one()
                    replay_row.is_replayed = True
                    replay_row.replayed_at = datetime.now(timezone.utc)
                    replay_row.replay_error = None

            consumer_runtime["dlq_messages_total"] = max(
                await refresh_dead_letter_backlog_metric(session_factory),
                0,
            )
            return {
                "dlq_event_id": dlq_event_id,
                "status": replay_status,
                "inserted": inserted,
                "event_id": replay_event.id,
                "broker_kind": dlq_event.broker_kind,
                "broker_destination": dlq_event.broker_destination,
                "broker_substream": (
                    dlq_event.broker_substream or None
                ),
                "broker_position": dlq_event.broker_position,
                "kafka_topic": replay_event.kafka_topic,
                "kafka_partition": replay_event.kafka_partition,
                "kafka_offset": replay_event.kafka_offset,
            }
        except Exception as exc:
            async with session_factory() as session:  # type: ignore[type-var,attr-defined]
                async with session.begin():
                    result = await session.execute(
                        select(DeadLetterEvent).where(
                            DeadLetterEvent.id == dlq_event_id
                        )
                    )
                    failed_row = result.scalar_one_or_none()
                    if failed_row is not None:
                        failed_row.replay_error = str(exc)
            raise


async def replay_dead_letter_event_records(
    limit: int = 100,
    include_replayed: bool = False,
    session_factory: Callable[[], AsyncSession] = (
        AsyncSessionLocal
    ),
) -> dict[str, object]:
    """Replay multiple persisted DLQ records through ingestion."""
    async with session_factory() as session:  # type: ignore[type-var,attr-defined]
        statement = select(DeadLetterEvent.id)
        if not include_replayed:
            statement = statement.where(
                DeadLetterEvent.is_replayed.is_(False)
            )
        statement = statement.order_by(
            DeadLetterEvent.failed_at.asc()
        ).limit(limit)
        result = await session.execute(statement)
        dlq_event_ids = list(result.scalars().all())

    results: list[dict[str, object]] = []
    replayed_count = 0
    duplicate_count = 0
    skipped_count = 0
    failed_count = 0

    for dlq_event_id in dlq_event_ids:
        if dlq_event_id is None:
            continue
        try:
            replay_result = await replay_dead_letter_event_record(
                dlq_event_id,
                session_factory=session_factory,
            )
            results.append(replay_result)

            status = replay_result.get("status")
            if status == "replayed":
                replayed_count += 1
            elif status == "duplicate":
                duplicate_count += 1
            else:
                skipped_count += 1
        except Exception as exc:
            failed_count += 1
            results.append(
                {
                    "dlq_event_id": dlq_event_id,
                    "status": "failed",
                    "error": str(exc),
                }
            )

    return {
        "results": results,
        "count": len(results),
        "requested_limit": limit,
        "include_replayed": include_replayed,
        "replayed_count": replayed_count,
        "duplicate_count": duplicate_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
    }
