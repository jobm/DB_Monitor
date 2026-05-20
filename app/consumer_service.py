# Kafka consumer logic for DB Monitor Server
import asyncio
import logging
import random
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from config import (
    KAFKA_BROKER,
    KAFKA_CONSUMER_GROUP,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SSL_CONTEXT,
    KAFKA_TOPIC,
)
from ws_manager import ws_manager
from event_parser import parse_event_payload
from event_pipeline import event_pipeline
from extensions import AsyncSessionLocal
from metrics import (
    consumer_last_successful_commit_timestamp_seconds,
    consumer_lag,
    circuit_breaker_state,
    db_write_duration_seconds,
    dlq_records_pending,
    dlq_messages_total,
    events_consumed_total,
    events_failed_total,
    events_processed_total,
    kafka_commit_duration_seconds,
)
from models import ConsumerCheckpoint, DeadLetterEvent, KafkaEvent
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
CONSUMER_METRIC_NAME = "kafka_consumer"

consumer_runtime: dict[str, object] = {
    "running": False,
    "connected": False,
    "topics": [],
    "last_error": None,
    "last_message_at": None,
    "last_commit_at": None,
    "lag_by_partition": {},
    "lag_total": 0,
    "dlq_messages_total": 0,
}


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"
        self._update_metric()

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self._update_metric()
            logger.warning("Circuit breaker opened due to repeated failures")

    def record_success(self):
        self.failure_count = 0
        self.state = "closed"
        self._update_metric()

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        if (
            asyncio.get_event_loop().time() - self.last_failure_time
            > self.recovery_timeout
        ):
            self.state = "half-open"
            self._update_metric()
            logger.info("Circuit breaker half-open, allowing test request")
            return True
        return False

    def _update_metric(self):
        state_value = {
            "closed": 0,
            "open": 1,
            "half-open": 2,
        }.get(self.state, 0)
        circuit_breaker_state.labels(
            breaker=CONSUMER_METRIC_NAME
        ).set(state_value)


circuit_breaker = CircuitBreaker()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp_to_age_seconds(value: Optional[str]) -> Optional[float]:
    """Return the age of an ISO 8601 timestamp in seconds."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0)


def _update_runtime_lag(
    consumer: AIOKafkaConsumer,
    topic: str,
    partition: int,
    offset: int,
) -> None:
    """Update current lag snapshots for readiness and metrics."""
    topic_partition = TopicPartition(topic, partition)
    highwater = consumer.highwater(topic_partition)
    if highwater is None:
        return

    lag_value = max(highwater - offset - 1, 0)
    lag_key = f"{topic}:{partition}"
    lag_by_partition = dict(consumer_runtime.get("lag_by_partition") or {})
    lag_by_partition[lag_key] = lag_value
    consumer_runtime["lag_by_partition"] = lag_by_partition
    consumer_runtime["lag_total"] = sum(lag_by_partition.values())
    consumer_lag.labels(topic=topic, partition=str(partition)).set(lag_value)


def _record_successful_commit() -> None:
    """Record the latest successful Kafka commit time."""
    timestamp = datetime.now(timezone.utc)
    consumer_runtime["last_commit_at"] = timestamp.isoformat()
    consumer_last_successful_commit_timestamp_seconds.set(
        timestamp.timestamp()
    )


def _get_canonical_service_name(
    topic: str,
    parsed_service_name: Optional[str],
) -> str:
    """Normalize service names so APIs and TUI filters use one stable value."""
    topic_prefix = topic.split(".", 1)[0] if topic else ""
    if topic_prefix:
        return topic_prefix
    if parsed_service_name:
        return parsed_service_name
    return "unknown"


def get_consumer_health() -> dict[str, object]:
    status = (
        "healthy"
        if consumer_runtime["running"] and consumer_runtime["connected"]
        else "unhealthy"
    )
    return {
        "status": status,
        "running": consumer_runtime["running"],
        "connected": consumer_runtime["connected"],
        "topics": consumer_runtime["topics"],
        "last_error": consumer_runtime["last_error"],
        "last_message_at": consumer_runtime["last_message_at"],
        "last_commit_at": consumer_runtime["last_commit_at"],
        "last_commit_age_seconds": _timestamp_to_age_seconds(
            consumer_runtime["last_commit_at"]
        ),
        "lag_by_partition": consumer_runtime["lag_by_partition"],
        "lag_total": consumer_runtime["lag_total"],
        "dlq_messages_total": consumer_runtime["dlq_messages_total"],
        "circuit_breaker_state": circuit_breaker.state,
    }


async def refresh_dead_letter_backlog_metric(
    session_factory: Callable = AsyncSessionLocal,
) -> int:
    """Refresh the pending DLQ gauge from persisted recovery records."""
    async with session_factory() as session:
        result = await session.execute(
            select(func.count())
            .select_from(DeadLetterEvent)
            .where(DeadLetterEvent.is_replayed.is_(False))
        )
        pending_count = int(result.scalar_one() or 0)

    dlq_records_pending.set(pending_count)
    consumer_runtime["dlq_messages_total"] = pending_count
    return pending_count


def _checkpoint_rows_for_events(
    events: list[KafkaEvent],
) -> list[dict[str, object]]:
    """Return highest committed offsets per topic partition for a batch."""
    checkpoints: dict[tuple[str, int], dict[str, object]] = {}
    for event in events:
        if (
            event.kafka_topic is None
            or event.kafka_partition is None
            or event.kafka_offset is None
        ):
            continue

        key = (event.kafka_topic, event.kafka_partition)
        existing = checkpoints.get(key)
        if (
            existing is None
            or event.kafka_offset > int(existing["kafka_offset"])
        ):
            checkpoints[key] = {
                "consumer_group": KAFKA_CONSUMER_GROUP,
                "kafka_topic": event.kafka_topic,
                "kafka_partition": event.kafka_partition,
                "kafka_offset": event.kafka_offset,
                "last_event_time": event.event_time,
            }

    return list(checkpoints.values())


async def persist_consumer_checkpoints(
    events: list[KafkaEvent],
    session_factory: Callable = AsyncSessionLocal,
) -> None:
    """Persist the latest committed Kafka offsets for each topic partition."""
    checkpoint_rows = _checkpoint_rows_for_events(events)
    if not checkpoint_rows:
        return

    async with session_factory() as session:
        async with session.begin():
            insert_stmt = pg_insert(ConsumerCheckpoint).values(checkpoint_rows)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "consumer_group",
                    "kafka_topic",
                    "kafka_partition",
                ],
                set_={
                    "kafka_offset": insert_stmt.excluded.kafka_offset,
                    "last_event_time": insert_stmt.excluded.last_event_time,
                    "updated_at": func.now(),
                },
            )
            await session.execute(upsert_stmt)


async def list_consumer_checkpoints_snapshot(
    session_factory: Callable = AsyncSessionLocal,
) -> list[dict[str, object]]:
    """Return the persisted checkpoint snapshot for operators."""
    async with session_factory() as session:
        result = await session.execute(
            select(ConsumerCheckpoint).order_by(
                ConsumerCheckpoint.consumer_group.asc(),
                ConsumerCheckpoint.kafka_topic.asc(),
                ConsumerCheckpoint.kafka_partition.asc(),
            )
        )
        checkpoints = result.scalars().all()

    return [
        {
            "consumer_group": checkpoint.consumer_group,
            "kafka_topic": checkpoint.kafka_topic,
            "kafka_partition": checkpoint.kafka_partition,
            "kafka_offset": checkpoint.kafka_offset,
            "last_event_time": (
                checkpoint.last_event_time.isoformat()
                if checkpoint.last_event_time
                else None
            ),
            "updated_at": (
                checkpoint.updated_at.isoformat()
                if checkpoint.updated_at
                else None
            ),
        }
        for checkpoint in checkpoints
    ]


async def _persist_dead_letter_event(
    event: KafkaEvent,
    raw_payload: str,
    error: str,
    session_factory: Callable = AsyncSessionLocal,
) -> int:
    """Store the failed message in the persistent DLQ table."""
    async with session_factory() as session:
        async with session.begin():
            insert_stmt = pg_insert(DeadLetterEvent).values(
                service_name=event.service_name,
                kafka_topic=event.kafka_topic,
                kafka_partition=event.kafka_partition,
                kafka_offset=event.kafka_offset,
                operation=event.operation,
                raw_payload=raw_payload,
                error_message=error,
                is_replayed=False,
                replayed_at=None,
                replay_error=None,
            )
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "kafka_topic",
                    "kafka_partition",
                    "kafka_offset",
                ],
                set_={
                    "service_name": insert_stmt.excluded.service_name,
                    "operation": insert_stmt.excluded.operation,
                    "raw_payload": insert_stmt.excluded.raw_payload,
                    "error_message": insert_stmt.excluded.error_message,
                    "failed_at": func.now(),
                    "is_replayed": False,
                    "replayed_at": None,
                    "replay_error": None,
                },
            ).returning(DeadLetterEvent.id)
            result = await session.execute(upsert_stmt)
            return int(result.scalar_one())


async def list_dead_letter_events(
    limit: int = 100,
    include_replayed: bool = False,
    session_factory: Callable = AsyncSessionLocal,
) -> list[dict[str, object]]:
    """Return persisted DLQ records for operator inspection."""
    async with session_factory() as session:
        statement = select(DeadLetterEvent)
        if not include_replayed:
            statement = statement.where(DeadLetterEvent.is_replayed.is_(False))
        statement = statement.order_by(
            DeadLetterEvent.failed_at.desc()
        ).limit(limit)
        result = await session.execute(statement)
        events = result.scalars().all()

    return [
        {
            "id": event.id,
            "service_name": event.service_name,
            "kafka_topic": event.kafka_topic,
            "kafka_partition": event.kafka_partition,
            "kafka_offset": event.kafka_offset,
            "operation": event.operation,
            "error_message": event.error_message,
            "failed_at": (
                event.failed_at.isoformat() if event.failed_at else None
            ),
            "is_replayed": event.is_replayed,
            "replayed_at": (
                event.replayed_at.isoformat() if event.replayed_at else None
            ),
            "replay_error": event.replay_error,
            "raw_payload": event.raw_payload,
        }
        for event in events
    ]


def _build_replay_event(dlq_event: DeadLetterEvent) -> KafkaEvent:
    """Reconstruct a KafkaEvent from a persisted DLQ record."""
    parsed = parse_event_payload(dlq_event.raw_payload)
    operation = extract_operation(parsed.event_data or {})
    return KafkaEvent(
        event_type=parsed.event_type,
        event_time=parsed.event_time,
        user_id=parsed.user_id,
        service_name=_get_canonical_service_name(
            dlq_event.kafka_topic,
            dlq_event.service_name,
        ),
        kafka_topic=dlq_event.kafka_topic,
        kafka_partition=dlq_event.kafka_partition,
        kafka_offset=dlq_event.kafka_offset,
        event_data=parsed.event_data,
        raw_payload=parsed.raw_payload,
        operation=operation,
    )


async def replay_dead_letter_event_record(
    dlq_event_id: int,
    session_factory: Callable = AsyncSessionLocal,
) -> dict[str, object]:
    """Replay a persisted DLQ record through the normal ingestion path."""
    async with session_factory() as session:
        result = await session.execute(
            select(DeadLetterEvent).where(DeadLetterEvent.id == dlq_event_id)
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

        async with session_factory() as session:
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
            "kafka_topic": replay_event.kafka_topic,
            "kafka_partition": replay_event.kafka_partition,
            "kafka_offset": replay_event.kafka_offset,
        }
    except Exception as exc:
        async with session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(DeadLetterEvent).where(
                        DeadLetterEvent.id == dlq_event_id
                    )
                )
                replay_row = result.scalar_one_or_none()
                if replay_row is not None:
                    replay_row.replay_error = str(exc)
        raise


async def _store_event_graph_with_retries(
    session_factory: Callable,
    event_obj: KafkaEvent,
    max_retries: int = 5,
) -> bool:
    """Persist an event and its derived records with transaction retries."""
    base_delay = 1.0
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            with db_write_duration_seconds.labels(
                operation=event_obj.operation or "unknown"
            ).time():
                async with session_factory() as session:
                    async with session.begin():
                        table_id = await schema_discovery.process_event(
                            event_obj,
                            session=session,
                        )
                        if table_id:
                            event_obj.source_table_id = table_id

                        inserted = await _insert_event_record(
                            session,
                            event_obj,
                        )
                        if inserted and table_id:
                            await change_processor.process_event(
                                event_obj,
                                session=session,
                            )
            return inserted
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
    logger.error(
        "Exhausted DB write retries (%d). Raising last exception",
        max_retries,
    )
    if last_exc is None:
        raise RuntimeError("DB write retries exhausted")
    raise last_exc


async def _insert_event_record(session, event_obj: KafkaEvent) -> bool:
    """Insert an event once, keyed by Kafka topic/partition/offset."""
    insert_stmt = (
        pg_insert(KafkaEvent)
        .values(
            event_type=event_obj.event_type,
            event_time=event_obj.event_time,
            user_id=event_obj.user_id,
            service_name=event_obj.service_name,
            kafka_topic=event_obj.kafka_topic,
            kafka_partition=event_obj.kafka_partition,
            kafka_offset=event_obj.kafka_offset,
            source_table_id=event_obj.source_table_id,
            operation=event_obj.operation,
            event_data=event_obj.event_data,
            raw_payload=event_obj.raw_payload,
        )
        .on_conflict_do_nothing(
            index_elements=["kafka_topic", "kafka_partition", "kafka_offset"]
        )
        .returning(KafkaEvent.id)
    )
    result = await session.execute(insert_stmt)
    inserted_id = result.scalar_one_or_none()
    if inserted_id is None:
        return False

    event_obj.id = inserted_id
    return True


async def send_to_dlq(
    producer: Optional[AIOKafkaProducer],
    event: KafkaEvent,
    error: str,
    raw_message: bytes | None = None,
) -> bool:
    """Persist and optionally forward a failed message to the DLQ topic."""
    raw_payload = (
        raw_message.decode("utf-8")
        if raw_message is not None
        else event.raw_payload
    )
    try:
        dlq_record_id = await _persist_dead_letter_event(
            event,
            raw_payload,
            error,
        )
    except Exception as exc:
        logger.exception("Failed to persist dead-letter event: %s", exc)
        return False

    if producer:
        try:
            await producer.send_and_wait(
                DLQ_TOPIC,
                value=raw_payload.encode(),
                key=None,
                headers=[
                    ("error", error.encode()),
                    ("source-topic", (event.kafka_topic or "").encode()),
                    (
                        "source-partition",
                        str(event.kafka_partition).encode(),
                    ),
                    ("source-offset", str(event.kafka_offset).encode()),
                    ("dlq-record-id", str(dlq_record_id).encode()),
                ],
            )
        except Exception as exc:
            logger.error("Failed to forward message to DLQ topic: %s", exc)

    dlq_messages_total.inc()
    await refresh_dead_letter_backlog_metric()
    logger.info(
        (
            "Message persisted to DLQ record %s for topic=%s "
            "partition=%s offset=%s"
        ),
        dlq_record_id,
        event.kafka_topic,
        event.kafka_partition,
        event.kafka_offset,
    )
    return True


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

    consumer_runtime.update(
        {
            "running": True,
            "connected": False,
            "topics": list(topics),
            "last_error": None,
            "last_message_at": None,
            "last_commit_at": None,
            "lag_by_partition": {},
            "lag_total": 0,
            "dlq_messages_total": 0,
        }
    )
    try:
        await refresh_dead_letter_backlog_metric()
    except Exception as exc:
        logger.warning(
            "Failed to refresh dead-letter backlog metric on startup: %s",
            exc,
        )

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_CONSUMER_GROUP,
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
            consumer_runtime["connected"] = True
            consumer_runtime["last_error"] = None
            logger.info("Kafka consumer started, subscribed to %s", topics)
        except Exception as e:
            retry_count += 1
            consumer_runtime["last_error"] = str(e)
            if retry_count >= MAX_CONNECTION_RETRIES:
                logger.error(
                    (
                        "Failed to connect to Kafka after %d attempts: %s. "
                        "Exiting consumer."
                    ),
                    MAX_CONNECTION_RETRIES,
                    e,
                )
                if producer:
                    await producer.stop()
                consumer_runtime["running"] = False
                return
            delay = min(
                CONNECTION_RETRY_DELAY * (2 ** min(retry_count - 1, 5)),
                60,
            )
            logger.warning(
                (
                    "Kafka connection attempt %d/%d failed: %s. "
                    "Retrying in %.1fs..."
                ),
                retry_count,
                MAX_CONNECTION_RETRIES,
                e,
                delay,
            )
            await asyncio.sleep(delay)

    try:
        async for msg in consumer:
            if not circuit_breaker.can_execute():
                logger.warning(
                    "Circuit breaker open, skipping message processing"
                )
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
                consumer_runtime["last_message_at"] = _utc_now_iso()
                _update_runtime_lag(
                    consumer,
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )

                event = KafkaEvent(
                    event_type=parsed.event_type,
                    event_time=parsed.event_time,
                    user_id=parsed.user_id,
                    service_name=_get_canonical_service_name(
                        msg.topic,
                        parsed.service_name,
                    ),
                    kafka_topic=msg.topic,
                    kafka_partition=msg.partition,
                    kafka_offset=msg.offset,
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
                        "Event %s dropped by pipeline rules.",
                        parsed.event_type,
                    )
                    circuit_breaker.record_success()
                    continue

                event = event_pipeline.transform(event)

                if enable_batch:
                    if not batch:
                        last_batch_time = asyncio.get_event_loop().time()
                    batch.append((event, data))
                    batch_elapsed = (
                        asyncio.get_event_loop().time() - last_batch_time
                    )
                    if (
                        len(batch) >= batch_size
                        or batch_elapsed >= batch_timeout
                    ):
                        await _process_batch(
                            batch,
                            consumer,
                            producer,
                            enable_dlq,
                        )
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
        consumer_runtime["running"] = False
        consumer_runtime["connected"] = False
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
                    producer,
                    event,
                    str(exc),
                    raw_data.encode(),
                )
                if not handled:
                    logger.error(
                        (
                            "Skipping commit because DLQ persistence failed "
                            "for %s/%s/%s"
                        ),
                        event.kafka_topic,
                        event.kafka_partition,
                        event.kafka_offset,
                    )
                    return
            else:
                logger.error(
                    "Skipping commit because DLQ is disabled for %s/%s/%s",
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset,
                )
                return

    try:
        with kafka_commit_duration_seconds.time():
            await consumer.commit()
        await persist_consumer_checkpoints(
            [event for event, _raw_data in batch],
        )
        _record_successful_commit()
    except Exception as exc:
        logger.exception("Failed to commit Kafka offsets: %s", exc)


def _event_to_ws_payload(event: KafkaEvent) -> dict:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "event_time": (
            event.event_time.isoformat() if event.event_time else None
        ),
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
            },
            event_id=event.id,
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
            return

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
                producer,
                event,
                str(exc),
                raw_data.encode(),
            )
            if not handled:
                return
        else:
            return

    try:
        with kafka_commit_duration_seconds.time():
            await consumer.commit()
        await persist_consumer_checkpoints([event])
        _record_successful_commit()
    except Exception as exc:
        logger.exception("Failed to commit Kafka offsets: %s", exc)
