"""Consumer message processing loop."""

from __future__ import annotations

import asyncio
import logging

from db_monitor.core.config import settings
from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.brokers import DeadLetterPublisher
from db_monitor.ingestion.event_parser import parse_event_payload
from db_monitor.ingestion.event_pipeline import event_pipeline
from db_monitor.ingestion.quota import QuotaDecision, quota_limiter
from db_monitor.ingestion.recovery import _get_canonical_service_name
from db_monitor.ingestion.runtime_state import (
    CircuitBreaker as RuntimeCircuitBreaker,
    ConsumerStopRequested,
    _update_runtime_lag,
    _utc_now_iso,
)
from db_monitor.metrics import (
    events_consumed_total,
    ingestion_quota_dropped_total,
    ingestion_quota_sleep_seconds,
    ingestion_quota_throttled_total,
)
from db_monitor.schema import extract_operation
from db_monitor.tracing import start_span
from db_monitor.consumer.batch import _fallback_event_from_message, _process_batch, _process_single_event
from db_monitor.consumer.checkpoint import _ack_message_and_record_progress
from db_monitor.consumer.dlq import _handle_message_processing_error
from db_monitor.consumer.handlers import _handle_filtered_event, _handle_quota_dropped_event

logger = logging.getLogger(__name__)

AnyConsumer = object  # Union of KafkaConsumerAdapter | RabbitMQConsumerAdapter


async def _message_loop(
    consumer: AnyConsumer,
    *,
    cluster_name: str,
    broker_kind: str,
    consumer_group: str,
    dlq_publisher: DeadLetterPublisher | None,
    dlq_destination: str,
    enable_dlq: bool,
    enable_batch: bool,
    batch_size: int,
    runtime: dict[str, object],
    breaker: RuntimeCircuitBreaker,
) -> None:
    """Process messages from the consumer in a loop with batching support."""
    batch: list[tuple[KafkaEvent, str, object]] = []
    batch_timeout = 5.0
    last_batch_time: float = 0.0

    async for msg in consumer:  # type: ignore[union-attr,type-var]
        if not breaker.can_execute():
            logger.warning(
                "Circuit breaker open for cluster '%s', skipping message processing",
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
                    await _handle_filtered_event(
                        event=event,
                        msg=msg,
                        runtime=runtime,
                        cluster_name=cluster_name,
                        consumer_group=consumer_group,
                        broker_kind=broker_kind,
                        breaker=breaker,
                    )
                    continue

                event = event_pipeline.transform(event)

                if settings.ingestion.quota_enabled:
                    quota_decision = await quota_limiter.apply(event)
                    if quota_decision.throttled:
                        ingestion_quota_throttled_total.labels(
                            dimension=quota_decision.dimension or "unknown",
                            identity=quota_decision.key or "unknown",
                        ).inc()
                        ingestion_quota_sleep_seconds.observe(
                            quota_decision.sleep_seconds
                        )

                    if not quota_decision.allowed:
                        await _handle_quota_dropped_event(
                            event=event,
                            msg=msg,
                            quota_decision=quota_decision,
                            runtime=runtime,
                            cluster_name=cluster_name,
                            consumer_group=consumer_group,
                            broker_kind=broker_kind,
                            breaker=breaker,
                        )
                        continue

                if enable_batch:
                    if not batch:
                        last_batch_time = asyncio.get_event_loop().time()
                    batch.append((event, data, msg))
                    batch_elapsed = asyncio.get_event_loop().time() - last_batch_time
                    if len(batch) >= batch_size or batch_elapsed >= batch_timeout:
                        batch_processed = await _process_batch(
                            batch,
                            consumer.ack_batch,  # type: ignore[union-attr]
                            dlq_publisher,
                            dlq_destination,
                            enable_dlq,
                            runtime,
                            cluster_name,
                            consumer_group,
                            broker_kind,
                        )
                        if not batch_processed:
                            raise ConsumerStopRequested("Batch processing aborted before ack.")
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
                    "DLQ-handled message could not be acknowledged."
                )

    # Flush remaining batch on graceful consumer exit
    if batch:
        batch_processed = await _process_batch(
            batch,
            consumer.ack_batch,  # type: ignore[union-attr]
            dlq_publisher,
            dlq_destination,
            enable_dlq,
            runtime,
            cluster_name,
            consumer_group,
            broker_kind,
        )
        if not batch_processed:
            raise ConsumerStopRequested("Final batch processing aborted before ack.")
