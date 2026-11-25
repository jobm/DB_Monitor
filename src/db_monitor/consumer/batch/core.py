"""Batch processing orchestration — bulk path with per-event fallback."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from db_monitor.core.config import settings
from db_monitor.core.models import KafkaEvent
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.brokers import BrokerMessage, DeadLetterPublisher
from db_monitor.ingestion.broadcast import _broadcast_event
from db_monitor.ingestion.event_storage import _store_batch_graph_with_retries
from db_monitor.ingestion.recovery import persist_consumer_checkpoints
from db_monitor.ingestion.runtime_state import (
    _record_committed_offsets,
    _record_successful_commit,
)
from db_monitor.metrics import (
    events_duplicate_skipped_total,
    events_processed_total,
    kafka_commit_duration_seconds,
)
from db_monitor.tracing import start_span
from db_monitor.ws_manager import ws_manager
from db_monitor.consumer.batch.processing import _process_batch_individually
from db_monitor.consumer.checkpoint import _requeue_broker_messages

logger = logging.getLogger(__name__)


async def _process_batch(
    batch: list[tuple[KafkaEvent, str, BrokerMessage]],
    ack_callback: Callable[[list[BrokerMessage]], Awaitable[None]],
    dlq_publisher: DeadLetterPublisher | None,
    dlq_destination: str,
    enable_dlq: bool,
    runtime: dict[str, object],
    cluster_name: str = "default",
    consumer_group: str = settings.broker.kafka_consumer_group,
    broker_kind: str = "kafka",
) -> bool:
    """Process a batch of events."""
    with start_span(
        "kafka.process.batch",
        attributes={
            "db_monitor.batch_size": len(batch),
            "db_monitor.dlq_enabled": enable_dlq,
        },
    ):
        if settings.ingestion.bulk_write_enabled:
            try:
                inserted_keys = await _store_batch_graph_with_retries(
                    AsyncSessionLocal,
                    [event for event, _raw_data, _message in batch],
                    max_retries=5,
                )
            except Exception as exc:
                logger.warning(
                    "Bulk batch persistence failed, falling back to "
                    "per-event writes: %s",
                    exc,
                )
                processed = await _process_batch_individually(
                    batch,
                    dlq_publisher,
                    dlq_destination,
                    enable_dlq,
                    broker_kind,
                )
                if not processed:
                    batch_messages = [
                        message for _event, _raw_data, message in batch
                    ]
                    await _requeue_broker_messages(batch_messages)
                    return False
            else:
                published_event_ids: list[int] = []
                for event, _raw_data, _message in batch:
                    event_key = (
                        event.kafka_topic,
                        event.kafka_partition,
                        event.kafka_offset,
                    )
                    if event_key not in inserted_keys:
                        logger.info(
                            (
                                "Skipping duplicate Kafka event topic=%s "
                                "partition=%s offset=%s"
                            ),
                            event.kafka_topic,
                            event.kafka_partition,
                            event.kafka_offset,
                        )
                        events_duplicate_skipped_total.labels(
                            service=event.service_name or "unknown",
                        ).inc()
                        continue

                    await _broadcast_event(
                        event,
                        publish_backplane=False,
                    )
                    if event.id is not None:
                        published_event_ids.append(event.id)
                    events_processed_total.labels(
                        service=event.service_name or "unknown",
                        operation=event.operation or "unknown",
                    ).inc()

                # One backplane round trip per batch instead of one per event.
                await ws_manager.publish_event_notifications(
                    published_event_ids
                )
        else:
            processed = await _process_batch_individually(
                batch,
                dlq_publisher,
                dlq_destination,
                enable_dlq,
                broker_kind,
            )
            if not processed:
                batch_messages = [
                    message for _event, _raw_data, message in batch
                ]
                await _requeue_broker_messages(batch_messages)
                return False

    try:
        with kafka_commit_duration_seconds.time():
            batch_messages = [
                message for _event, _raw_data, message in batch
            ]
            await ack_callback(batch_messages)
        await persist_consumer_checkpoints(
            [event for event, _raw_data, _message in batch],
            consumer_group=consumer_group,
            broker_kind=broker_kind,
        )
        _record_committed_offsets(
            runtime,
            cluster_name,
            [event for event, _raw_data, _message in batch],
        )
        _record_successful_commit(runtime)
    except Exception as exc:
        batch_messages = [message for _event, _raw_data, message in batch]
        await _requeue_broker_messages(batch_messages)
        logger.exception("Failed to acknowledge broker messages: %s", exc)
        return False
    return True
