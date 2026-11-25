"""WebSocket broadcast and webhook delivery for consumed events."""

from __future__ import annotations

import json
from typing import cast
import logging

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.recovery import _persist_dead_letter_event
from db_monitor.tracing import start_span
from db_monitor.webhooks import webhook_notifier
from db_monitor.ws_manager import ws_manager

logger = logging.getLogger(__name__)


def _event_to_ws_payload(event: KafkaEvent) -> dict:
    """Serialize a KafkaEvent into a WebSocket-friendly dict."""
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
        "row_identity": event.row_identity,
        "event_data": event.event_data,
    }


async def _broadcast_event(
    event: KafkaEvent,
    *,
    publish_backplane: bool = True,
) -> None:
    """Broadcast an event via WebSocket and deliver via webhooks.

    Failed webhook deliveries are persisted as dead-letter events.
    Batch callers suppress the per-event backplane notification and publish
    once for the batch instead.
    """
    payload = {
        "type": "new_event",
        "event": _event_to_ws_payload(event),
    }
    try:
        with start_span(
            "websocket.broadcast.event",
            attributes={
                "db_monitor.event_id": event.id,
                "db_monitor.service": event.service_name,
                "db_monitor.operation": event.operation,
            },
        ):
            await ws_manager.broadcast(
                payload,
                event_id=event.id,
                tenant_id=event.service_name,
                publish_backplane=publish_backplane,
            )
    except Exception as e:
        logger.warning("Failed to broadcast event via WebSocket: %s", e)

    try:
        failed_deliveries = await webhook_notifier.send_message(
            cast(dict[str, object], payload)
        )
    except Exception as exc:
        logger.warning("Failed to deliver event via webhook: %s", exc)
        return

    if not failed_deliveries:
        return

    raw_payload = event.raw_payload or json.dumps(
        payload,
        separators=(",", ":"),
    )
    for delivery in failed_deliveries:
        try:
            await _persist_dead_letter_event(
                event,
                raw_payload,
                (
                    "Webhook delivery to "
                    f"{delivery['url']} failed: {delivery['error']}"
                ),
                broker_kind="webhook",
            )
        except Exception as exc:
            logger.warning(
                "Failed to persist webhook DLQ record for %s: %s",
                delivery.get("url"),
                exc,
            )
