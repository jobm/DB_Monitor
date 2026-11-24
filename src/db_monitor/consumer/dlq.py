"""Dead-letter queue routing for failed broker messages."""

from __future__ import annotations

import logging

from db_monitor.consumer.batch import _fallback_event_from_message
from db_monitor.ingestion.brokers import BrokerMessage, DeadLetterPublisher
from db_monitor.ingestion.dlq import send_to_dlq
from db_monitor.metrics import events_failed_total

logger = logging.getLogger(__name__)


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
