"""Event handlers for filtered and quota-dropped messages."""

from __future__ import annotations

import logging

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.quota import QuotaDecision
from db_monitor.ingestion.runtime_state import (
    CircuitBreaker as RuntimeCircuitBreaker,
    ConsumerStopRequested,
)
from db_monitor.metrics import ingestion_quota_dropped_total
from db_monitor.consumer.checkpoint import _ack_message_and_record_progress

logger = logging.getLogger(__name__)


async def _handle_filtered_event(
    *,
    event: KafkaEvent,
    msg: object,
    runtime: dict[str, object],
    cluster_name: str,
    consumer_group: str,
    broker_kind: str,
    breaker: RuntimeCircuitBreaker,
) -> None:
    """Ack and record progress for a filtered (dropped by pipeline) event."""
    logger.debug("Event %s dropped by pipeline rules.", event.event_type)
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
            "Filtered message could not be acknowledged."
        )
    breaker.record_success()


async def _handle_quota_dropped_event(
    *,
    event: KafkaEvent,
    msg: object,
    quota_decision: QuotaDecision,
    runtime: dict[str, object],
    cluster_name: str,
    consumer_group: str,
    broker_kind: str,
    breaker: RuntimeCircuitBreaker,
) -> None:
    """Ack and record progress for a quota-dropped event."""
    ingestion_quota_dropped_total.labels(
        dimension=quota_decision.dimension or "unknown",
        identity=quota_decision.key or "unknown",
    ).inc()
    logger.warning(
        "Dropping event due to quota limit "
        "dimension=%s identity=%s limit=%s",
        quota_decision.dimension,
        quota_decision.key,
        quota_decision.limit,
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
            "Quota-dropped message could not be acknowledged."
        )
    breaker.record_success()
