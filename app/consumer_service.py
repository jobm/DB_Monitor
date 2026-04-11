# Kafka consumer logic for DB Monitor Server
import asyncio
import json
import logging
import random
from collections.abc import Callable
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_SECURITY_PROTOCOL, KAFKA_SSL_CONTEXT
from ws_manager import ws_manager
from event_parser import parse_event_payload
from event_pipeline import event_pipeline
from extensions import AsyncSessionLocal
from metrics import events_consumed_total, events_processed_total, events_failed_total
from models import KafkaEvent
from schema_discovery import SchemaDiscovery, extract_operation
from change_processor import ChangeProcessor
from sqlalchemy.exc import OperationalError, DBAPIError, SQLAlchemyError

logger = logging.getLogger(__name__)

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)

DLQ_TOPIC = "db-monitor-dlq"
DEFAULT_TOPICS = [KAFKA_TOPIC]
MAX_CONNECTION_RETRIES = 60
CONNECTION_RETRY_DELAY = 5.0


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning("Circuit breaker opened due to repeated failures")

    def record_success(self):
        self.failure_count = 0
        self.state = "closed"

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        if (
            asyncio.get_event_loop().time() - self.last_failure_time
            > self.recovery_timeout
        ):
            self.state = "half-open"
            logger.info("Circuit breaker half-open, allowing test request")
            return True
        return False


circuit_breaker = CircuitBreaker()


async def _write_event_with_retries(
    session_factory: Callable,
    event_obj: KafkaEvent,
    max_retries: int = 5,
) -> None:
    """Persist a single event with retry on transient DB errors."""
    base_delay = 1.0
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            async with session_factory() as session:
                async with session.begin():
                    session.add(event_obj)
            return
        except (OperationalError, DBAPIError) as exc:
            wait = base_delay * (2 ** (attempt - 1)) + random.random() * 0.1
            logger.warning(
                "Transient DB error on attempt %d/%d: %s. Retrying in %.2fs",
                attempt,
                max_retries,
                exc,
                wait,
            )
            await asyncio.sleep(wait)
            last_exc = exc
            continue
        except SQLAlchemyError as exc:
            logger.error("Non-retryable DB error when writing event: %s", exc)
            raise
    logger.error("Exhausted DB write retries (%d). Raising last exception", max_retries)
    if last_exc is None:
        raise RuntimeError("DB write retries exhausted")
    raise last_exc


async def send_to_dlq(
    producer: Optional[AIOKafkaProducer], raw_message: bytes, error: str
):
    """Send failed message to dead-letter queue."""
    if producer:
        try:
            await producer.send_and_wait(
                DLQ_TOPIC,
                value=raw_message,
                key=None,
                headers=[("error", error.encode())],
            )
            logger.info("Message sent to DLQ")
        except Exception as e:
            logger.error("Failed to send to DLQ: %s", e)


async def consumer_task(
    topics: list[str] = None,
    enable_dlq: bool = True,
    enable_batch: bool = True,
    batch_size: int = 100,
):
    """Consume messages from Kafka with resilience features.

    Features:
    - Multi-topic subscription
    - Manual offset commit
    - Exponential backoff retry
    - Dead-letter queue
    - Circuit breaker
    - Batch processing
    """
    if topics is None:
        topics = DEFAULT_TOPICS

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        group_id="fastapi-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        ssl_context=KAFKA_SSL_CONTEXT,
    )

    producer = None
    if enable_dlq:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            ssl_context=KAFKA_SSL_CONTEXT,
        )
        await producer.start()

    batch: list[tuple] = []
    batch_timeout = 5.0
    last_batch_time: float = 0

    connected = False
    retry_count = 0
    while not connected:
        try:
            await consumer.start()
            connected = True
            logger.info("Kafka consumer started, subscribed to %s", topics)
        except Exception as e:
            retry_count += 1
            if retry_count >= MAX_CONNECTION_RETRIES:
                logger.error(
                    "Failed to connect to Kafka after %d attempts: %s. Exiting consumer.",
                    MAX_CONNECTION_RETRIES,
                    e,
                )
                if producer:
                    await producer.stop()
                return
            delay = min(CONNECTION_RETRY_DELAY * (2 ** min(retry_count - 1, 5)), 60)
            logger.warning(
                "Kafka connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                retry_count,
                MAX_CONNECTION_RETRIES,
                e,
                delay,
            )
            await asyncio.sleep(delay)

    try:
        async for msg in consumer:
            if not circuit_breaker.can_execute():
                logger.warning("Circuit breaker open, skipping message processing")
                await asyncio.sleep(1.0)
                continue

            try:
                data = msg.value.decode("utf-8")
                logger.debug(
                    "Received message topic=%s partition=%d offset=%d",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )

                parsed = parse_event_payload(data)
                operation = extract_operation(parsed.event_data or {})

                event = KafkaEvent(
                    event_type=parsed.event_type,
                    event_time=parsed.event_time,
                    user_id=parsed.user_id,
                    service_name=parsed.service_name or msg.topic,
                    event_data=parsed.event_data,
                    raw_payload=parsed.raw_payload,
                    operation=operation,
                )

                events_consumed_total.labels(
                    service=event.service_name or "unknown",
                    operation=event.operation or "unknown",
                ).inc()

                if not event_pipeline.should_process(event):
                    logger.debug(
                        "Event %s dropped by pipeline rules.", parsed.event_type
                    )
                    circuit_breaker.record_success()
                    continue

                event = event_pipeline.transform(event)

                if enable_batch:
                    if not batch:
                        last_batch_time = asyncio.get_event_loop().time()
                    batch.append((event, data))
                    batch_elapsed = asyncio.get_event_loop().time() - last_batch_time
                    if len(batch) >= batch_size or batch_elapsed >= batch_timeout:
                        await _process_batch(batch, consumer, producer, enable_dlq)
                        batch = []
                else:
                    await _process_single_event(
                        event, consumer, producer, enable_dlq, data
                    )

                circuit_breaker.record_success()

            except asyncio.CancelledError:
                logger.info("Consumer task received cancellation signal")
                raise
            except Exception as e:
                logger.exception("Error processing message: %s", e)
                circuit_breaker.record_failure()

        if batch:
            await _process_batch(batch, consumer, producer, enable_dlq)

    except asyncio.CancelledError:
        logger.info("Consumer task received cancellation signal (outer)")
        raise
    finally:
        logger.info("Stopping Kafka consumer")
        await consumer.stop()
        if producer:
            await producer.stop()


async def _process_batch(
    batch: list[tuple],
    consumer: AIOKafkaConsumer,
    producer: Optional[AIOKafkaProducer],
    enable_dlq: bool,
):
    """Process a batch of events."""
    for event, raw_data in batch:
        try:
            await _write_event_with_retries(AsyncSessionLocal, event, max_retries=5)
            table_id = await schema_discovery.process_event(event)
            if table_id:
                event.source_table_id = table_id
                async with AsyncSessionLocal() as session:
                    async with session.begin():
                        session.add(event)
                        await session.flush()
                await change_processor.process_event(event)
            await _broadcast_event(event)

            events_processed_total.labels(
                service=event.service_name or "unknown",
                operation=event.operation or "unknown",
            ).inc()
        except Exception as exc:
            logger.error("Failed to process event after retries: %s", exc)
            events_failed_total.labels(
                service=event.service_name or "unknown", error_type=type(exc).__name__
            ).inc()
            if enable_dlq:
                await send_to_dlq(producer, raw_data.encode(), str(exc))

    try:
        await consumer.commit()
    except Exception as exc:
        logger.exception("Failed to commit Kafka offsets: %s", exc)


def _event_to_ws_payload(event: KafkaEvent) -> dict:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "event_time": event.event_time.isoformat() if event.event_time else None,
        "user_id": event.user_id,
        "service_name": event.service_name,
        "operation": event.operation,
        "source_table_id": event.source_table_id,
        "event_data": event.event_data,
    }


async def _broadcast_event(event: KafkaEvent) -> None:
    try:
        await ws_manager.broadcast(
            {
                "type": "new_event",
                "event": _event_to_ws_payload(event),
            }
        )
    except Exception as e:
        logger.warning("Failed to broadcast event via WebSocket: %s", e)


async def _process_single_event(
    event: KafkaEvent,
    consumer: AIOKafkaConsumer,
    producer: Optional[AIOKafkaProducer],
    enable_dlq: bool,
    raw_data: str,
):
    """Process a single event."""
    try:
        await _write_event_with_retries(AsyncSessionLocal, event, max_retries=5)
        table_id = await schema_discovery.process_event(event)
        if table_id:
            event.source_table_id = table_id
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    session.add(event)
            await change_processor.process_event(event)
        await _broadcast_event(event)

        events_processed_total.labels(
            service=event.service_name or "unknown",
            operation=event.operation or "unknown",
        ).inc()
    except Exception as exc:
        logger.error("Failed to process event after retries: %s", exc)
        events_failed_total.labels(
            service=event.service_name or "unknown", error_type=type(exc).__name__
        ).inc()
        if enable_dlq:
            await send_to_dlq(producer, raw_data.encode(), str(exc))
        return

    try:
        await consumer.commit()
    except Exception as exc:
        logger.exception("Failed to commit Kafka offsets: %s", exc)
