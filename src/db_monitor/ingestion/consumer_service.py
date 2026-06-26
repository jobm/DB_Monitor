# Broker consumer logic for DB Monitor Server
import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Optional

from db_monitor.core.config import settings
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.brokers import (
    BrokerMessage,
    DeadLetterPublisher,
    build_dead_letter_publisher,
    build_message_consumer,
)
from db_monitor.ingestion.broadcast import _broadcast_event
from db_monitor.ingestion.dlq import DLQ_TOPIC, send_to_dlq
from db_monitor.ingestion.event_parser import parse_event_payload
from db_monitor.ingestion.event_pipeline import event_pipeline
from db_monitor.ingestion.event_storage import _store_batch_graph_with_retries, _store_event_graph_with_retries
from db_monitor.ingestion.quota import quota_limiter
from db_monitor.ingestion.recovery import (
    _get_canonical_service_name,
    persist_consumer_checkpoints,
    refresh_dead_letter_backlog_metric,
)
from db_monitor.ingestion.runtime_state import (
    CircuitBreaker as RuntimeCircuitBreaker,
    ConsumerStopRequested,
    _get_circuit_breaker,
    _get_consumer_runtime,
    _record_committed_offsets,
    _record_successful_commit,
    _update_runtime_lag,
    _utc_now_iso,
    cluster_circuit_breakers as runtime_cluster_circuit_breakers,
    consumer_runtime,
    consumer_runtimes as runtime_consumer_runtimes,
    get_consumer_health as runtime_get_consumer_health,
)
from db_monitor.ingestion.schema import extract_operation
from db_monitor.metrics import (
    events_consumed_total,
    events_duplicate_skipped_total,
    events_failed_total,
    events_processed_total,
    ingestion_quota_dropped_total,
    ingestion_quota_sleep_seconds,
    ingestion_quota_throttled_total,
    kafka_commit_duration_seconds,
    dlq_records_pending as dlq_records_pending_metric,
)
from db_monitor.core.models import KafkaEvent
from db_monitor.tracing import start_span

logger = logging.getLogger(__name__)

DEFAULT_TOPICS = [settings.broker.kafka_topic]
MAX_CONNECTION_RETRIES = 60
CONNECTION_RETRY_DELAY = 5.0


CircuitBreaker = RuntimeCircuitBreaker
cluster_circuit_breakers = runtime_cluster_circuit_breakers
consumer_runtimes = runtime_consumer_runtimes
dlq_records_pending = dlq_records_pending_metric


def get_consumer_health() -> dict[str, object]:
    """Expose aggregate consumer health for API and readiness checks."""
    return runtime_get_consumer_health()


def _fallback_event_from_message(
    msg: BrokerMessage,
    raw_payload: str,
) -> KafkaEvent:
    """Build a minimal event shell when normal parsing fails."""
    return KafkaEvent(
        event_type="unparsed",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name=_get_canonical_service_name(msg.destination, None),
        kafka_topic=msg.destination,
        kafka_partition=msg.partition,
        kafka_offset=msg.offset,
        event_data=None,
        raw_payload=raw_payload,
        operation="UNKNOWN",
    )


async def _handle_message_processing_error(
    *,
    msg: BrokerMessage,
    raw_payload: str,
    exc: Exception,
    dlq_publisher: DeadLetterPublisher | None,
    dlq_destination: str,
    enable_dlq: bool,
    broker_kind: str,
) -> bool:
    """Persist a failed message to DLQ.

    Decide whether processing can continue after a message-level failure.

    Returns True when it is safe to continue consuming. Returns False when the
    consumer should stop to avoid committing offsets for unpersisted failures.
    """
    fallback_event = _fallback_event_from_message(msg, raw_payload)
    events_failed_total.labels(
        service=fallback_event.service_name or "unknown",
        error_type=type(exc).__name__,
    ).inc()

    if not enable_dlq:
        logger.error(
            (
                "Stopping consumer after message failure because DLQ is "
                "disabled for %s/%s/%s"
            ),
            msg.destination,
            msg.partition,
            msg.offset,
        )
        return False

    handled = await send_to_dlq(
        dlq_publisher,
        fallback_event,
        str(exc),
        raw_payload.encode(),
        dlq_destination=dlq_destination,
        broker_kind=broker_kind,
    )
    if not handled:
        logger.error(
            (
                "Stopping consumer because DLQ persistence failed for "
                "%s/%s/%s"
            ),
            msg.destination,
            msg.partition,
            msg.offset,
        )
        return False

    return True


async def _requeue_broker_messages(messages: list[BrokerMessage]) -> None:
    """Request broker redelivery for unacked messages when supported."""
    for message in messages:
        nack_callback = getattr(message, "nack_callback", None)
        if nack_callback is None:
            continue
        try:
            await nack_callback()
        except Exception as exc:
            logger.warning(
                "Failed to requeue broker message %s/%s/%s: %s",
                message.destination,
                message.partition,
                message.offset,
                exc,
            )


async def _ack_message_and_record_progress(
    *,
    msg: BrokerMessage,
    event: KafkaEvent,
    runtime: dict[str, object],
    cluster_name: str,
    consumer_group: str,
    broker_kind: str,
) -> bool:
    """Ack one broker message and persist checkpoint progress."""
    try:
        with kafka_commit_duration_seconds.time():
            await msg.ack_callback()
        await persist_consumer_checkpoints(
            [event],
            consumer_group=consumer_group,
            broker_kind=broker_kind,
        )
        _record_committed_offsets(runtime, cluster_name, [event])
        _record_successful_commit(runtime)
    except Exception as exc:
        nack_callback = getattr(msg, "nack_callback", None)
        if nack_callback is not None:
            await nack_callback()
        logger.exception(
            "Failed to acknowledge broker message %s/%s/%s: %s",
            msg.destination,
            msg.partition,
            msg.offset,
            exc,
        )
        return False

    return True


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
    if runtime is None:
        runtime = consumer_runtime

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

                    await _broadcast_event(event)
                    events_processed_total.labels(
                        service=event.service_name or "unknown",
                        operation=event.operation or "unknown",
                    ).inc()
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
    if topic_partitions is not None:
        topics = sorted(topic_partitions.keys())
    if broker_kind == "rabbitmq":
        destinations = list(queue_names)
    else:
        destinations = list(topics)

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
    try:
        await refresh_dead_letter_backlog_metric()
    except Exception as exc:
        logger.warning(
            (
                "Failed to refresh dead-letter backlog metric on "
                "startup for cluster %s: %s"
            ),
            cluster_name,
            exc,
        )

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

    dlq_publisher = None
    if enable_dlq:
        dlq_publisher = build_dead_letter_publisher(
            broker_kind=broker_kind,
            bootstrap_servers=bootstrap_servers,
            connection_url=connection_url,
            dlq_destination=dlq_destination,
        )
        for dlq_attempt in range(30):
            try:
                await dlq_publisher.start()
                break
            except Exception as dlq_exc:
                if dlq_attempt == 29:
                    raise
                logger.warning(
                    "DLQ publisher not ready (attempt %d/30): %s",
                    dlq_attempt + 1,
                    dlq_exc,
                )
                await asyncio.sleep(2)

    batch: list[tuple[KafkaEvent, str, BrokerMessage]] = []
    batch_timeout = 5.0
    last_batch_time: float = 0

    connected = False
    retry_count = 0
    while not connected:
        try:
            await consumer.start()
            if topic_partitions is not None:
                logger.info(
                    "Broker consumer '%s' using explicit topic assignments %s",
                    cluster_name,
                    topic_partitions,
                )
            connected = True
            runtime["connected"] = True
            runtime["last_error"] = None
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
                    (
                        "Kafka consumer '%s' started with partition "
                        "assignments %s"
                    ),
                    cluster_name,
                    topic_partitions,
                )
        except Exception as e:
            retry_count += 1
            runtime["last_error"] = str(e)
            if retry_count >= MAX_CONNECTION_RETRIES:
                logger.error(
                    (
                        "Failed to connect to Kafka cluster '%s' after "
                        "%d attempts: %s. "
                        "Exiting consumer."
                    ),
                    cluster_name,
                    MAX_CONNECTION_RETRIES,
                    e,
                )
                if dlq_publisher:
                    await dlq_publisher.stop()
                runtime["running"] = False
                return
            delay = min(
                CONNECTION_RETRY_DELAY * (2 ** min(retry_count - 1, 5)),
                60,
            )
            logger.warning(
                (
                    "Kafka cluster '%s' connection attempt %d/%d failed: %s. "
                    "Retrying in %.1fs..."
                ),
                cluster_name,
                retry_count,
                MAX_CONNECTION_RETRIES,
                e,
                delay,
            )
            await asyncio.sleep(delay)

    try:
        async for msg in consumer:
            if not breaker.can_execute():
                logger.warning(
                    (
                        "Circuit breaker open for cluster '%s', "
                        "skipping message processing"
                    ),
                    cluster_name,
                )
                await asyncio.sleep(1.0)
                continue

            current_event: KafkaEvent | None = None
            try:
                data = msg.value.decode("utf-8")
                logger.debug(
                    "Received message destination=%s partition=%d offset=%d",
                    msg.destination,
                    msg.partition,
                    msg.offset,
                )

                parsed = parse_event_payload(data)
                operation = extract_operation(parsed.event_data or {})
                runtime["last_message_at"] = _utc_now_iso()
                _update_runtime_lag(
                    runtime,
                    cluster_name,
                    msg.destination,
                    msg.partition,
                    msg.offset,
                    msg.lag,
                )

                event = KafkaEvent(
                    event_type=parsed.event_type,
                    event_time=parsed.event_time,
                    user_id=parsed.user_id,
                    service_name=_get_canonical_service_name(
                        msg.destination,
                        parsed.service_name,
                    ),
                    kafka_topic=msg.destination,
                    kafka_partition=msg.partition,
                    kafka_offset=msg.offset,
                    event_data=parsed.event_data,
                    raw_payload=parsed.raw_payload,
                    operation=operation,
                )
                current_event = event

                events_consumed_total.labels(
                    service=event.service_name or "unknown",
                    operation=event.operation or "unknown",
                ).inc()

                with start_span(
                    "kafka.consumer.message",
                    attributes={
                        "db_monitor.cluster": cluster_name,
                        "messaging.system": broker_kind,
                        "messaging.destination.name": msg.destination,
                        "messaging.destination.partition.id": msg.partition,
                        "messaging.message.id": msg.offset,
                    },
                ):
                    if not event_pipeline.should_process(event):
                        logger.debug(
                            "Event %s dropped by pipeline rules.",
                            parsed.event_type,
                        )
                        acknowledged = await _ack_message_and_record_progress(
                            msg=msg,
                            event=event,
                            runtime=runtime,
                            cluster_name=cluster_name,
                            consumer_group=consumer_group,
                            broker_kind=broker_kind,
                        )
                        if not acknowledged:
                            raise ConsumerStopRequested(
                                (
                                    "Filtered message could not be "
                                    "acknowledged."
                                )
                            )
                        breaker.record_success()
                        continue

                    event = event_pipeline.transform(event)

                    if settings.ingestion.quota_enabled:
                        quota_decision = await quota_limiter.apply(event)
                        if quota_decision.throttled:
                            ingestion_quota_throttled_total.labels(
                                dimension=quota_decision.dimension
                                or "unknown",
                                identity=quota_decision.key or "unknown",
                            ).inc()
                            ingestion_quota_sleep_seconds.observe(
                                quota_decision.sleep_seconds
                            )

                        if not quota_decision.allowed:
                            ingestion_quota_dropped_total.labels(
                                dimension=quota_decision.dimension
                                or "unknown",
                                identity=quota_decision.key or "unknown",
                            ).inc()
                            logger.warning(
                                (
                                    "Dropping event due to quota limit "
                                    "dimension=%s identity=%s limit=%s"
                                ),
                                quota_decision.dimension,
                                quota_decision.key,
                                quota_decision.limit,
                            )
                            acknowledged = await (
                                _ack_message_and_record_progress(
                                    msg=msg,
                                    event=event,
                                    runtime=runtime,
                                    cluster_name=cluster_name,
                                    consumer_group=consumer_group,
                                    broker_kind=broker_kind,
                                )
                            )
                            if not acknowledged:
                                raise ConsumerStopRequested(
                                    (
                                        "Quota-dropped message could not be "
                                        "acknowledged."
                                    )
                                )
                            breaker.record_success()
                            continue

                    if enable_batch:
                        if not batch:
                            last_batch_time = asyncio.get_event_loop().time()
                        batch.append((event, data, msg))
                        batch_elapsed = (
                            asyncio.get_event_loop().time() - last_batch_time
                        )
                        if (
                            len(batch) >= batch_size
                            or batch_elapsed >= batch_timeout
                        ):
                            batch_processed = await _process_batch(
                                batch,
                                consumer.ack_batch,
                                dlq_publisher,
                                dlq_destination,
                                enable_dlq,
                                runtime,
                                cluster_name,
                                consumer_group,
                                broker_kind,
                            )
                            if not batch_processed:
                                raise ConsumerStopRequested(
                                    "Batch processing aborted before ack."
                                )
                            batch = []
                    else:
                        processed = await _process_single_event(
                            event,
                            msg,
                            dlq_publisher=dlq_publisher,
                            dlq_destination=dlq_destination,
                            enable_dlq=enable_dlq,
                            raw_data=data,
                            runtime=runtime,
                            cluster_name=cluster_name,
                            consumer_group=consumer_group,
                            broker_kind=broker_kind,
                        )
                        if not processed:
                            raise ConsumerStopRequested(
                                "Message processing aborted before ack."
                            )

                breaker.record_success()

            except asyncio.CancelledError:
                logger.info("Consumer task received cancellation signal")
                raise
            except ConsumerStopRequested:
                breaker.record_failure()
                raise
            except Exception as e:
                logger.exception("Error processing message: %s", e)
                raw_payload = msg.value.decode("utf-8", errors="replace")
                should_continue = await _handle_message_processing_error(
                    msg=msg,
                    raw_payload=raw_payload,
                    exc=e,
                    dlq_publisher=dlq_publisher,
                    dlq_destination=dlq_destination,
                    enable_dlq=enable_dlq,
                    broker_kind=broker_kind,
                )
                breaker.record_failure()
                if not should_continue:
                    raise

                fallback_event = (
                    current_event
                    if current_event is not None
                    else _fallback_event_from_message(msg, raw_payload)
                )
                acknowledged = await _ack_message_and_record_progress(
                    msg=msg,
                    event=fallback_event,
                    runtime=runtime,
                    cluster_name=cluster_name,
                    consumer_group=consumer_group,
                    broker_kind=broker_kind,
                )
                if not acknowledged:
                    raise ConsumerStopRequested(
                        (
                            "DLQ-handled message could not be acknowledged."
                        )
                    )

        if batch:
            batch_processed = await _process_batch(
                batch,
                consumer.ack_batch,
                dlq_publisher,
                dlq_destination,
                enable_dlq,
                runtime,
                cluster_name,
                consumer_group,
                broker_kind,
            )
            if not batch_processed:
                raise ConsumerStopRequested(
                    "Final batch processing aborted before ack."
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
