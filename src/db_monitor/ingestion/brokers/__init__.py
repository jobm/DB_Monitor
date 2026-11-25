"""Broker adapters for Kafka and RabbitMQ ingestion."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

__all__ = [
    "BrokerMessage",
    "DeadLetterPublisher",
    "build_message_consumer",
    "build_dead_letter_publisher",
]


@dataclass
class BrokerMessage:
    """Normalized consumed message for the ingestion pipeline."""

    destination: str
    partition: int
    offset: int
    value: bytes
    lag: int | None
    ack_callback: Callable[[], Awaitable[None]]
    nack_callback: Callable[[], Awaitable[None]] | None = None


class MessageConsumerAdapter(Protocol):
    """Minimal consume/ack interface for supported brokers."""

    async def start(self) -> None:
        """Start the adapter connection and subscriptions."""

    async def stop(self) -> None:
        """Stop the adapter and release all resources."""

    async def ack_batch(self, messages: list[BrokerMessage]) -> None:
        """Acknowledge one processed batch of broker messages."""

    async def lag_snapshot(self) -> dict[tuple[str, int], int]:
        """Return current lag per ``(destination, partition)`` if supported."""

    def __aiter__(self) -> Any:
        """Iterate consumed broker messages."""


class DeadLetterPublisher(Protocol):
    """Broker-specific dead-letter forwarding interface."""

    async def start(self) -> None:
        """Start any underlying publisher resources."""

    async def stop(self) -> None:
        """Stop any underlying publisher resources."""

    async def publish_dead_letter(
        self,
        *,
        raw_payload: str,
        source_destination: str | None,
        source_partition: int | None,
        source_offset: int | None,
        error: str,
        dlq_record_id: int,
    ) -> None:
        """Forward one dead-letter payload through the active broker."""


from db_monitor.ingestion.brokers.kafka import (
    KafkaConsumerAdapter,
    KafkaDeadLetterPublisher,
)
from db_monitor.ingestion.brokers.rabbitmq import (
    RabbitMQConsumerAdapter,
    RabbitMQDeadLetterPublisher,
)


def build_message_consumer(
    *,
    broker_kind: str,
    bootstrap_servers: str | list[str] | None,
    consumer_group: str,
    topics: list[str],
    topic_partitions: dict[str, list[int]] | None,
    connection_url: str | None,
    queue_names: list[str] | None,
    prefetch_count: int,
) -> MessageConsumerAdapter:
    """Build the configured broker consumer adapter."""
    if broker_kind == "rabbitmq":
        if not connection_url:
            raise ValueError("RabbitMQ consumers require a connection_url.")
        if not queue_names:
            raise ValueError("RabbitMQ consumers require queue_names.")
        return RabbitMQConsumerAdapter(
            connection_url=connection_url,
            queue_names=queue_names,
            prefetch_count=prefetch_count,
        )

    return KafkaConsumerAdapter(
        bootstrap_servers=bootstrap_servers or [],
        consumer_group=consumer_group,
        topics=topics,
        topic_partitions=topic_partitions,
    )


def build_dead_letter_publisher(
    *,
    broker_kind: str,
    bootstrap_servers: str | list[str] | None,
    connection_url: str | None,
    dlq_destination: str,
) -> DeadLetterPublisher:
    """Build the configured dead-letter publisher."""
    if broker_kind == "rabbitmq":
        if not connection_url:
            raise ValueError("RabbitMQ DLQ requires a connection_url.")
        return RabbitMQDeadLetterPublisher(
            connection_url=connection_url,
            dlq_destination=dlq_destination,
        )

    return KafkaDeadLetterPublisher(
        bootstrap_servers=bootstrap_servers or [],
        dlq_destination=dlq_destination,
    )
