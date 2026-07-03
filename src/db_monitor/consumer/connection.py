"""Broker connection management with retry and backoff."""

from __future__ import annotations

import asyncio
import logging

from db_monitor.core.config import settings
from db_monitor.ingestion.brokers import DeadLetterPublisher, build_message_consumer

logger = logging.getLogger(__name__)

DEFAULT_TOPICS = [settings.broker.kafka_topic]
MAX_CONNECTION_RETRIES = 60
CONNECTION_RETRY_DELAY = 5.0


def _resolve_destinations(
    topics: list[str] | None,
    topic_partitions: dict[str, list[int]] | None,
    queue_names: list[str] | None,
) -> list[str]:
    """Resolve the list of destination names for the consumer."""
    if topic_partitions is not None:
        return sorted(topic_partitions.keys())
    return list(topics or DEFAULT_TOPICS)


def _init_runtime_state(
    runtime: dict[str, object],
    cluster_name: str,
    broker_kind: str,
    destinations: list[str],
) -> None:
    """Initialize the runtime state dict for a consumer cluster."""
    runtime.update(
        {
            "running": True,
            "connected": False,
            "topics": destinations,
            "broker_kind": broker_kind,
            "last_error": None,
            "last_message_at": None,
            "last_commit_at": None,
            "lag_by_partition": {},
            "lag_total": 0,
            "dlq_messages_total": 0,
        }
    )


async def _start_dlq_publisher(
    dlq_publisher: DeadLetterPublisher,
) -> None:
    """Start a DLQ publisher with retry logic."""
    for dlq_attempt in range(30):
        try:
            await dlq_publisher.start()
            return
        except Exception as dlq_exc:
            if dlq_attempt == 29:
                raise
            logger.warning(
                "DLQ publisher not ready (attempt %d/30): %s",
                dlq_attempt + 1,
                dlq_exc,
            )
            await asyncio.sleep(2)


async def _connect_consumer(
    cluster_name: str,
    broker_kind: str,
    *,
    bootstrap_servers: str | list[str],
    consumer_group: str,
    topics: list[str],
    topic_partitions: dict[str, list[int]] | None,
    connection_url: str | None,
    queue_names: list[str],
    prefetch_count: int,
    runtime: dict[str, object],
) -> object:
    """Build and connect a consumer with exponential backoff retry."""
    consumer = build_message_consumer(
        broker_kind=broker_kind,
        bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group,
        topics=topics,
        topic_partitions=topic_partitions,
        connection_url=connection_url,
        queue_names=queue_names,
        prefetch_count=prefetch_count,
    )

    connected = False
    retry_count = 0
    while not connected:
        try:
            await consumer.start()
            connected = True
            runtime["connected"] = True
            runtime["last_error"] = None
            _log_consumer_started(cluster_name, broker_kind, topics, queue_names, topic_partitions)
        except Exception as e:
            retry_count += 1
            runtime["last_error"] = str(e)
            if retry_count >= MAX_CONNECTION_RETRIES:
                logger.error(
                    "Failed to connect to broker cluster '%s' after "
                    "%d attempts: %s. Exiting consumer.",
                    cluster_name,
                    MAX_CONNECTION_RETRIES,
                    e,
                )
                runtime["running"] = False
                raise
            delay = min(
                CONNECTION_RETRY_DELAY * (2 ** min(retry_count - 1, 5)),
                60,
            )
            logger.warning(
                "Broker cluster '%s' connection attempt %d/%d failed: %s. "
                "Retrying in %.1fs...",
                cluster_name,
                retry_count,
                MAX_CONNECTION_RETRIES,
                e,
                delay,
            )
            await asyncio.sleep(delay)

    return consumer


def _log_consumer_started(
    cluster_name: str,
    broker_kind: str,
    topics: list[str],
    queue_names: list[str],
    topic_partitions: dict[str, list[int]] | None,
) -> None:
    """Log consumer connection success with appropriate detail."""
    if broker_kind == "rabbitmq":
        logger.info(
            "RabbitMQ consumer '%s' started, subscribed to %s",
            cluster_name,
            queue_names,
        )
    elif topic_partitions is None:
        logger.info(
            "Kafka consumer '%s' started, subscribed to %s",
            cluster_name,
            topics,
        )
    else:
        logger.info(
            "Kafka consumer '%s' started with partition assignments %s",
            cluster_name,
            topic_partitions,
        )
