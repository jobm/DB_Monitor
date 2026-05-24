"""Canonical ingestion consumer exports."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_consumer_service = importlib.import_module("consumer_service")

consumer_task = _consumer_service.consumer_task
get_consumer_health = _consumer_service.get_consumer_health
list_consumer_checkpoints_snapshot = (
    _consumer_service.list_consumer_checkpoints_snapshot
)
list_dead_letter_events = _consumer_service.list_dead_letter_events
replay_dead_letter_event_record = (
    _consumer_service.replay_dead_letter_event_record
)
replay_dead_letter_event_records = (
    _consumer_service.replay_dead_letter_event_records
)

__all__ = [
    "consumer_task",
    "get_consumer_health",
    "list_consumer_checkpoints_snapshot",
    "list_dead_letter_events",
    "replay_dead_letter_event_record",
    "replay_dead_letter_event_records",
]
