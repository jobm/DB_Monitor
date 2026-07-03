"""Offset checkpoint management for broker consumers."""

from __future__ import annotations

import logging

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.brokers import BrokerMessage
from db_monitor.ingestion.recovery import persist_consumer_checkpoints
from db_monitor.ingestion.runtime_state import (
    _record_committed_offsets,
    _record_successful_commit,
)
from db_monitor.metrics import kafka_commit_duration_seconds

logger = logging.getLogger(__name__)


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
