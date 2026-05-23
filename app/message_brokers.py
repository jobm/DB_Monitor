"""Broker adapters for Kafka and RabbitMQ ingestion."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from config import KAFKA_SECURITY_PROTOCOL, KAFKA_SSL_CONTEXT

logger = logging.getLogger(__name__)


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


class KafkaConsumerAdapter:
    """Kafka-backed message consumer adapter."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        consumer_group: str,
        topics: list[str],
        topic_partitions: dict[str, list[int]] | None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group = consumer_group
        self._topics = list(topics)
        self._topic_partitions = topic_partitions
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        consumer_kwargs = {
            "bootstrap_servers": self._bootstrap_servers,
            "group_id": self._consumer_group,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "ssl_context": KAFKA_SSL_CONTEXT,
        }
        if self._topic_partitions is None:
            self._consumer = AIOKafkaConsumer(*self._topics, **consumer_kwargs)
        else:
            self._consumer = AIOKafkaConsumer(**consumer_kwargs)

        await self._consumer.start()
        if self._topic_partitions is not None:
            assignments = [
                TopicPartition(topic_name, partition)
                for topic_name, partitions in self._topic_partitions.items()
                for partition in partitions
            ]
            self._consumer.assign(assignments)

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def ack_batch(self, messages: list[BrokerMessage]) -> None:
        del messages
        if self._consumer is not None:
            await self._consumer.commit()

    def __aiter__(self):
        return self._iterate_messages()

    async def _iterate_messages(self):
        assert self._consumer is not None
        async for raw_message in self._consumer:
            topic_partition = TopicPartition(
                raw_message.topic,
                raw_message.partition,
            )
            highwater = self._consumer.highwater(topic_partition)
            lag = None
            if highwater is not None:
                lag = max(highwater - raw_message.offset - 1, 0)
            yield BrokerMessage(
                destination=raw_message.topic,
                partition=raw_message.partition,
                offset=raw_message.offset,
                value=raw_message.value,
                lag=lag,
                ack_callback=self._consumer.commit,
                nack_callback=None,
            )


class KafkaDeadLetterPublisher:
    """Kafka-backed DLQ publisher."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        dlq_destination: str,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._dlq_destination = dlq_destination
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            ssl_context=KAFKA_SSL_CONTEXT,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

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
        assert self._producer is not None
        await self._producer.send_and_wait(
            self._dlq_destination,
            value=raw_payload.encode(),
            key=None,
            headers=[
                ("error", error.encode()),
                (
                    "source-topic",
                    (source_destination or "").encode(),
                ),
                (
                    "source-partition",
                    str(source_partition).encode(),
                ),
                (
                    "source-offset",
                    str(source_offset).encode(),
                ),
                ("dlq-record-id", str(dlq_record_id).encode()),
            ],
        )


class RabbitMQConsumerAdapter:
    """RabbitMQ-backed message consumer adapter."""

    _STOP_SENTINEL = object()

    def __init__(
        self,
        *,
        connection_url: str,
        queue_names: list[str],
        prefetch_count: int,
    ) -> None:
        self._connection_url = connection_url
        self._queue_names = list(queue_names)
        self._prefetch_count = prefetch_count
        self._connection = None
        self._channel = None
        self._consumer_tags: list[tuple[Any, str]] = []
        self._message_queue: asyncio.Queue[BrokerMessage | object] = (
            asyncio.Queue()
        )

    async def start(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(
            self._connection_url
        )
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch_count)

        for queue_name in self._queue_names:
            queue = await self._channel.declare_queue(
                queue_name,
                durable=True,
            )
            consumer_tag = await queue.consume(
                self._build_consumer_callback(queue_name)
            )
            self._consumer_tags.append((queue, consumer_tag))

    async def stop(self) -> None:
        for queue, consumer_tag in self._consumer_tags:
            try:
                await queue.cancel(consumer_tag)
            except Exception:
                pass
        self._consumer_tags.clear()
        await self._message_queue.put(self._STOP_SENTINEL)
        if self._channel is not None:
            await self._channel.close()
        if self._connection is not None:
            await self._connection.close()

    async def ack_batch(self, messages: list[BrokerMessage]) -> None:
        for message in messages:
            await message.ack_callback()

    def __aiter__(self):
        return self._iterate_messages()

    async def _iterate_messages(self):
        while True:
            message = await self._message_queue.get()
            if message is self._STOP_SENTINEL:
                break
            yield message

    def _build_consumer_callback(
        self,
        queue_name: str,
    ) -> Callable[[Any], Awaitable[None]]:
        async def callback(message: Any) -> None:
            async def nack_callback() -> None:
                await message.nack(requeue=True)

            await self._message_queue.put(
                BrokerMessage(
                    destination=queue_name,
                    partition=0,
                    offset=int(message.delivery_tag or 0),
                    value=message.body,
                    lag=None,
                    ack_callback=message.ack,
                    nack_callback=nack_callback,
                )
            )

        return callback


class RabbitMQDeadLetterPublisher:
    """RabbitMQ-backed DLQ publisher."""

    def __init__(
        self,
        *,
        connection_url: str,
        dlq_destination: str,
    ) -> None:
        self._connection_url = connection_url
        self._dlq_destination = dlq_destination
        self._connection = None
        self._channel = None

    async def start(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(
            self._connection_url
        )
        self._channel = await self._connection.channel()
        await self._channel.declare_queue(
            self._dlq_destination,
            durable=True,
        )

    async def stop(self) -> None:
        if self._channel is not None:
            await self._channel.close()
        if self._connection is not None:
            await self._connection.close()

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
        import aio_pika

        assert self._channel is not None
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                body=raw_payload.encode(),
                headers={
                    "error": error,
                    "source-topic": source_destination or "",
                    "source-partition": str(source_partition),
                    "source-offset": str(source_offset),
                    "dlq-record-id": str(dlq_record_id),
                },
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self._dlq_destination,
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
