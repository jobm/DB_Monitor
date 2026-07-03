"""Per-event processing logic — individual and batch-individual fallback."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from db_monitor.core.config import settings
from db_monitor.core.models import KafkaEvent
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.brokers import BrokerMessage, DeadLetterPublisher
from db_monitor.ingestion.broadcast import _broadcast_event
from db_monitor.ingestion.dlq import DLQ_TOPIC, send_to_dlq
from db_monitor.ingestion.event_storage import _store_event_graph_with_retries
from db_monitor.ingestion.recovery import persist_consumer_checkpoints
from db_monitor.ingestion.runtime_state import (
    _record_committed_offsets,
    _record_successful_commit,
)
from db_monitor.metrics import (
    events_failed_total,
    events_processed_total,
    kafka_commit_duration_seconds,
)
from db_monitor.tracing import start_span

logger = logging.getLogger(__name__)


async def _process_batch_individually(
    batch: list[tuple[KafkaEvent, str, BrokerMessage]],
    dlq_publisher: DeadLetterPublisher | None,
    dlq_destination: str,
    enable_dlq: bool,
    broker_kind: str,
) -> bool:
    """Process one batch with per-event persistence fallback logic."""
    for event, raw_data, _message in batch:
        try:
            inserted = await _store_event_graph_with_retries(
                AsyncSessionLocal,
                event,
                max_retries=5,
            )
            if not inserted:
                logger.info(
                    (
                        "Skipping duplicate Kafka event topic=%s "
                        "partition=%s offset=%s"
                    ),
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset,
                )
                continue

            await _broadcast_event(event)

            events_processed_total.labels(
                service=event.service_name or "unknown",
                operation=event.operation or "unknown",
            ).inc()
        except Exception as exc:
            logger.error("Failed to process event after retries: %s", exc)
            events_failed_total.labels(
                service=event.service_name or "unknown",
                error_type=type(exc).__name__,
            ).inc()
            if enable_dlq:
                handled = await send_to_dlq(
                    dlq_publisher,
                    event,
                    str(exc),
                    raw_data.encode(),
                    dlq_destination=dlq_destination,
                    broker_kind=broker_kind,
                )
                if not handled:
                    logger.error(
                        (
                            "Skipping commit because DLQ persistence "
                            "failed for %s/%s/%s"
                        ),
                        event.kafka_topic,
                        event.kafka_partition,
                        event.kafka_offset,
                    )
                    return False
            else:
                logger.error(
                    "Skipping commit because DLQ is disabled for %s/%s/%s",
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset,
                )
                return False

    return True


async def _process_single_event(
    event: KafkaEvent,
    ack_target: Callable[[], Awaitable[None]] | object,
    producer: DeadLetterPublisher | None = None,
    dlq_publisher: DeadLetterPublisher | None = None,
    dlq_destination: str = DLQ_TOPIC,
    enable_dlq: bool = True,
    raw_data: str = "",
    runtime: dict[str, object] | None = None,
    cluster_name: str = "default",
    consumer_group: str = settings.broker.kafka_consumer_group,
    broker_kind: str = "kafka",
) -> bool:
    """Process a single event."""
    from db_monitor.ingestion.runtime_state import consumer_runtime as _runtime_default

    if runtime is None:
        runtime = _runtime_default

    if dlq_publisher is None:
        dlq_publisher = producer

    if callable(ack_target):
        ack_callback = ack_target
        nack_callback = None
    elif hasattr(ack_target, "ack_callback") and callable(
        ack_target.ack_callback
    ):
        ack_callback = ack_target.ack_callback
        nack_callback = getattr(ack_target, "nack_callback", None)
    elif hasattr(ack_target, "commit") and callable(ack_target.commit):
        ack_callback = ack_target.commit
        nack_callback = None
    else:
        raise TypeError("ack_target must be callable or expose commit().")

    try:
        with start_span(
            "kafka.process.event",
            attributes={
                "messaging.destination.name": event.kafka_topic,
                "messaging.kafka.partition": event.kafka_partition,
                "messaging.kafka.offset": event.kafka_offset,
                "db_monitor.service": event.service_name,
                "db_monitor.operation": event.operation,
            },
        ):
            inserted = await _store_event_graph_with_retries(
                AsyncSessionLocal,
                event,
                max_retries=5,
            )
            if not inserted:
                logger.info(
                    (
                        "Skipping duplicate Kafka event topic=%s "
                        "partition=%s offset=%s"
                    ),
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset,
                )
            else:
                await _broadcast_event(event)

                events_processed_total.labels(
                    service=event.service_name or "unknown",
                    operation=event.operation or "unknown",
                ).inc()
    except Exception as exc:
        logger.error("Failed to process event after retries: %s", exc)
        events_failed_total.labels(
            service=event.service_name or "unknown",
            error_type=type(exc).__name__,
        ).inc()
        if enable_dlq:
            handled = await send_to_dlq(
                dlq_publisher,
                event,
                str(exc),
                raw_data.encode(),
                dlq_destination=dlq_destination,
                broker_kind=broker_kind,
            )
            if not handled:
                if nack_callback is not None:
                    await nack_callback()
                return False
        else:
            if nack_callback is not None:
                await nack_callback()
            return False

    try:
        with kafka_commit_duration_seconds.time():
            await ack_callback()
        await persist_consumer_checkpoints(
            [event],
            consumer_group=consumer_group,
            broker_kind=broker_kind,
        )
        _record_committed_offsets(runtime, cluster_name, [event])
        _record_successful_commit(runtime)
    except Exception as exc:
        if nack_callback is not None:
            await nack_callback()
        logger.exception("Failed to acknowledge broker message: %s", exc)
        return False
    return True
