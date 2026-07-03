"""Consumer entry-point task that orchestrates connection, message loop, and shutdown."""

from __future__ import annotations

import asyncio
import logging

from db_monitor.core.config import settings
from db_monitor.ingestion.brokers import (
    DeadLetterPublisher,
    build_dead_letter_publisher,
)
from db_monitor.ingestion.dlq import DLQ_TOPIC
from db_monitor.ingestion.recovery import refresh_dead_letter_backlog_metric
from db_monitor.ingestion.runtime_state import (
    CircuitBreaker,
    _get_circuit_breaker,
    _get_consumer_runtime,
    cluster_circuit_breakers,
    consumer_runtime,
    consumer_runtimes,
    get_consumer_health as runtime_get_consumer_health,
)
from db_monitor.metrics import dlq_records_pending
from db_monitor.consumer.connection import (
    DEFAULT_TOPICS,
    _connect_consumer,
    _init_runtime_state,
    _resolve_destinations,
    _start_dlq_publisher,
)
from db_monitor.consumer.loop import _message_loop

logger = logging.getLogger(__name__)


def get_consumer_health() -> dict[str, object]:
    """Expose aggregate consumer health for API and readiness checks."""
    return runtime_get_consumer_health()


async def consumer_task(
    cluster_name: str = "default",
    broker_kind: str = "kafka",
    bootstrap_servers: str | list[str] | None = None,
    consumer_group: str | None = None,
    topics: list[str] | None = None,
    topic_partitions: dict[str, list[int]] | None = None,
    connection_url: str | None = None,
    queue_names: list[str] | None = None,
    prefetch_count: int = 100,
    dlq_destination: str = DLQ_TOPIC,
    enable_dlq: bool = True,
    enable_batch: bool = True,
    batch_size: int = 100,
):
    """Consume messages from the configured broker with resilience features.

    Features:
    - Multi-destination subscription
    - Manual ack/commit
    - Exponential backoff retry
    - Dead-letter queue
    - Circuit breaker
    - Batch processing
    """
    runtime = _get_consumer_runtime(cluster_name)
    breaker = _get_circuit_breaker(cluster_name)
    if topics is None:
        topics = DEFAULT_TOPICS
    if queue_names is None:
        queue_names = []
    bootstrap_servers = bootstrap_servers or settings.broker.kafka_broker
    consumer_group = consumer_group or settings.broker.kafka_consumer_group
    destinations = _resolve_destinations(topics, topic_partitions, queue_names)

    _init_runtime_state(runtime, cluster_name, broker_kind, destinations)

    try:
        await refresh_dead_letter_backlog_metric()
    except Exception as exc:
        logger.warning(
            "Failed to refresh dead-letter backlog metric on startup for cluster %s: %s",
            cluster_name,
            exc,
        )

    try:
        consumer = await _connect_consumer(
            cluster_name,
            broker_kind,
            bootstrap_servers=bootstrap_servers,
            consumer_group=consumer_group,
            topics=topics,
            topic_partitions=topic_partitions,
            connection_url=connection_url,
            queue_names=queue_names,
            prefetch_count=prefetch_count,
            runtime=runtime,
        )
    except Exception:
        runtime["running"] = False
        return

    dlq_publisher: DeadLetterPublisher | None = None
    if enable_dlq:
        dlq_publisher = build_dead_letter_publisher(
            broker_kind=broker_kind,
            bootstrap_servers=bootstrap_servers,
            connection_url=connection_url,
            dlq_destination=dlq_destination,
        )
        try:
            await _start_dlq_publisher(dlq_publisher)
        except Exception:
            logger.exception("Failed to start DLQ publisher for cluster '%s'", cluster_name)
            await consumer.stop()
            runtime["running"] = False
            return

    try:
        await _message_loop(
            consumer,
            cluster_name=cluster_name,
            broker_kind=broker_kind,
            consumer_group=consumer_group,
            dlq_publisher=dlq_publisher,
            dlq_destination=dlq_destination,
            enable_dlq=enable_dlq,
            enable_batch=enable_batch,
            batch_size=batch_size,
            runtime=runtime,
            breaker=breaker,
        )
    except asyncio.CancelledError:
        logger.info("Consumer task received cancellation signal (outer)")
        raise
    finally:
        logger.info("Stopping %s consumer '%s'", broker_kind, cluster_name)
        runtime["running"] = False
        runtime["connected"] = False
        await consumer.stop()
        if dlq_publisher:
            await dlq_publisher.stop()
