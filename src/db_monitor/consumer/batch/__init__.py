"""Batch and single-event processing for broker consumers."""

from __future__ import annotations

from db_monitor.consumer.batch.core import _process_batch
from db_monitor.consumer.batch.helpers import _fallback_event_from_message
from db_monitor.consumer.batch.processing import (
    _process_batch_individually,
    _process_single_event,
)

# Re-export imported names so tests can monkeypatch them via
# from db_monitor.consumer import batch as consumer_batch
# monkeypatch.setattr(consumer_batch, "send_to_dlq", ...)
from db_monitor.ingestion.broadcast import _broadcast_event
from db_monitor.ingestion.dlq import send_to_dlq
from db_monitor.ingestion.event_storage import (
    _store_batch_graph_with_retries,
    _store_event_graph_with_retries,
)
from db_monitor.ingestion.recovery import persist_consumer_checkpoints
from db_monitor.ingestion.runtime_state import _record_successful_commit
from db_monitor.metrics import (
    events_duplicate_skipped_total,
    events_processed_total,
    kafka_commit_duration_seconds,
)

__all__ = [
    "_broadcast_event",
    "_fallback_event_from_message",
    "_process_batch",
    "_process_batch_individually",
    "_process_single_event",
    "_record_successful_commit",
    "_store_batch_graph_with_retries",
    "_store_event_graph_with_retries",
    "events_duplicate_skipped_total",
    "events_processed_total",
    "kafka_commit_duration_seconds",
    "persist_consumer_checkpoints",
    "send_to_dlq",
]
