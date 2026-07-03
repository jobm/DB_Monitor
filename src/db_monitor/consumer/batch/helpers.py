"""Fallback event construction helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.brokers import BrokerMessage
from db_monitor.ingestion.recovery import _get_canonical_service_name


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
