"""Consumer checkpoint and DLQ persistence helpers — domain-split sub-package.

All functions are re-exported here so that existing imports from
``db_monitor.ingestion.recovery`` continue to work unchanged.
"""

from db_monitor.ingestion.recovery.broker import (
    _build_broker_location,
    _get_canonical_service_name,
    _legacy_event_location,
    _serialize_broker_fields,
)
from db_monitor.ingestion.recovery.checkpoints import (
    _checkpoint_rows_for_events,
    list_consumer_checkpoints_snapshot,
    persist_consumer_checkpoints,
)
from db_monitor.ingestion.recovery.dlq import (
    _persist_dead_letter_event,
    list_dead_letter_events,
    refresh_dead_letter_backlog_metric,
)

__all__ = [
    "_build_broker_location",
    "_checkpoint_rows_for_events",
    "_get_canonical_service_name",
    "_legacy_event_location",
    "_persist_dead_letter_event",
    "_serialize_broker_fields",
    "list_consumer_checkpoints_snapshot",
    "list_dead_letter_events",
    "persist_consumer_checkpoints",
    "refresh_dead_letter_backlog_metric",
]
