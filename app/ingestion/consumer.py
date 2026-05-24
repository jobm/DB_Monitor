"""Compatibility wrapper for the consumer service."""

from __future__ import annotations

import consumer_service as _consumer_service

DLQ_TOPIC = _consumer_service.DLQ_TOPIC
consumer_task = _consumer_service.consumer_task
get_consumer_health = _consumer_service.get_consumer_health
list_consumer_checkpoints_snapshot = (
    _consumer_service.list_consumer_checkpoints_snapshot
)
list_dead_letter_events = _consumer_service.list_dead_letter_events
persist_consumer_checkpoints = _consumer_service.persist_consumer_checkpoints
refresh_dead_letter_backlog_metric = (
    _consumer_service.refresh_dead_letter_backlog_metric
)
replay_dead_letter_event_record = (
    _consumer_service.replay_dead_letter_event_record
)
replay_dead_letter_event_records = (
    _consumer_service.replay_dead_letter_event_records
)
send_to_dlq = _consumer_service.send_to_dlq
