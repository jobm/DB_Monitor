"""Kafka-backed consumer adapter and dead-letter publisher."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from db_monitor.core.config import settings
from db_monitor.ingestion.brokers import BrokerMessage


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
            "security_protocol": settings.broker.kafka_security_protocol,
            "ssl_context": settings.broker.kafka_ssl_context,
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
            security_protocol=settings.broker.kafka_security_protocol,
            ssl_context=settings.broker.kafka_ssl_context,
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
