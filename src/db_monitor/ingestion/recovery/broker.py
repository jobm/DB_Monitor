"""Broker-neutral location helpers for consumer checkpointing and DLQ."""

from __future__ import annotations

from typing import Optional

from db_monitor.core.models import KafkaEvent


def _build_broker_location(
    event: KafkaEvent,
    broker_kind: str,
) -> dict[str, object] | None:
    """Build broker-neutral recovery metadata for one consumed event."""
    if event.kafka_topic is None or event.kafka_offset is None:
        return None

    if broker_kind == "kafka":
        if event.kafka_partition is None:
            return None
        broker_substream = str(event.kafka_partition)
        kafka_topic = event.kafka_topic
        kafka_partition = event.kafka_partition
        kafka_offset = event.kafka_offset
    else:
        broker_substream = ""
        kafka_topic = None
        kafka_partition = None
        kafka_offset = None

    return {
        "broker_kind": broker_kind,
        "broker_destination": event.kafka_topic,
        "broker_substream": broker_substream,
        "broker_position": str(event.kafka_offset),
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }


def _legacy_event_location(
    broker_kind: str,
    broker_destination: str,
    broker_substream: str,
    broker_position: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> tuple[str, int | None, int | None]:
    """Build legacy Kafka location fields from broker metadata."""
    if kafka_topic is not None:
        return kafka_topic, kafka_partition, kafka_offset

    try:
        position = int(broker_position)
    except (TypeError, ValueError):
        position = None

    if broker_kind == "kafka":
        try:
            partition = int(broker_substream)
        except (TypeError, ValueError):
            partition = None
    else:
        partition = 0

    return broker_destination, partition, position


def _serialize_broker_fields(
    *,
    broker_kind: str,
    broker_destination: str,
    broker_substream: str,
    broker_position: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> dict[str, object]:
    """Render broker metadata plus Kafka compatibility aliases."""
    return {
        "broker_kind": broker_kind,
        "broker_destination": broker_destination,
        "broker_substream": broker_substream or None,
        "broker_position": broker_position,
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }


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
