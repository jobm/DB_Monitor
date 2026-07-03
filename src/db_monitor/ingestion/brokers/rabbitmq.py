"""RabbitMQ-backed consumer adapter and dead-letter publisher."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from db_monitor.ingestion.brokers import BrokerMessage


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
