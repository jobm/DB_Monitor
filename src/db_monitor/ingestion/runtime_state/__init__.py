"""Consumer runtime state and resilience helpers — domain-split sub-package.

All names are re-exported so existing imports from
``db_monitor.ingestion.runtime_state`` continue to work unchanged.
"""

from db_monitor.ingestion.runtime_state.breaker import (
    CONSUMER_METRIC_NAME,
    CircuitBreaker,
    ConsumerStopRequested,
    circuit_breaker,
    cluster_circuit_breakers,
    _get_circuit_breaker,
)
from db_monitor.ingestion.runtime_state.metrics import (
    get_consumer_health,
    _record_committed_offsets,
    _record_successful_commit,
    _update_runtime_lag,
)
from db_monitor.ingestion.runtime_state.state import (
    consumer_runtime,
    consumer_runtimes,
    _get_consumer_runtime,
    _new_consumer_runtime_state,
    _timestamp_to_age_seconds,
    _utc_now_iso,
)

__all__ = [
    "CONSUMER_METRIC_NAME",
    "CircuitBreaker",
    "ConsumerStopRequested",
    "circuit_breaker",
    "cluster_circuit_breakers",
    "consumer_runtime",
    "consumer_runtimes",
    "get_consumer_health",
    "_get_circuit_breaker",
    "_get_consumer_runtime",
    "_new_consumer_runtime_state",
    "_record_committed_offsets",
    "_record_successful_commit",
    "_timestamp_to_age_seconds",
    "_update_runtime_lag",
    "_utc_now_iso",
]
