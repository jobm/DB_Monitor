"""Consumer runtime state and resilience helpers.

This module owns mutable consumer runtime snapshots, circuit-breaker
instances, and health aggregation logic so ingestion orchestration can
focus on message processing.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from metrics import (
    circuit_breaker_state,
    consumer_commit_lag,
    consumer_committed_offset,
    consumer_current_offset,
    consumer_lag,
    consumer_last_successful_commit_timestamp_seconds,
)
from models import KafkaEvent

logger = logging.getLogger(__name__)

CONSUMER_METRIC_NAME = "broker_consumer"


def _new_consumer_runtime_state() -> dict[str, object]:
    """Build a clean runtime state structure for a broker consumer."""
    return {
        "running": False,
        "connected": False,
        "topics": [],
        "broker_kind": "kafka",
        "last_error": None,
        "last_message_at": None,
        "last_commit_at": None,
        "lag_by_partition": {},
        "highwater_by_partition": {},
        "lag_total": 0,
        "dlq_messages_total": 0,
    }


class ConsumerStopRequested(RuntimeError):
    """Raised when the consumer must stop to avoid losing messages."""


consumer_runtime: dict[str, object] = _new_consumer_runtime_state()
consumer_runtimes: dict[str, dict[str, object]] = {"default": consumer_runtime}


class CircuitBreaker:
    """Track repeated failures and gate downstream execution attempts."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        metric_name: str = CONSUMER_METRIC_NAME,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"
        self.metric_name = metric_name
        self._update_metric()

    def record_failure(self) -> None:
        """Record a failed operation and open the breaker on threshold."""
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self._update_metric()
            logger.warning("Circuit breaker opened due to repeated failures")

    def record_success(self) -> None:
        """Reset breaker state after a successful operation."""
        self.failure_count = 0
        self.state = "closed"
        self._update_metric()

    def can_execute(self) -> bool:
        """Return whether processing is currently allowed."""
        if self.state == "closed":
            return True
        if (
            asyncio.get_event_loop().time() - self.last_failure_time
            > self.recovery_timeout
        ):
            self.state = "half-open"
            self._update_metric()
            logger.info("Circuit breaker half-open, allowing test request")
            return True
        return False

    def _update_metric(self) -> None:
        """Publish breaker state transitions to metrics."""
        state_value = {
            "closed": 0,
            "open": 1,
            "half-open": 2,
        }.get(self.state, 0)
        circuit_breaker_state.labels(breaker=self.metric_name).set(
            state_value
        )


circuit_breaker = CircuitBreaker()
cluster_circuit_breakers: dict[str, CircuitBreaker] = {
    "default": circuit_breaker,
}


def _get_consumer_runtime(cluster_name: str) -> dict[str, object]:
    """Return the mutable runtime state for a cluster consumer."""
    if cluster_name == "default":
        return consumer_runtime

    runtime = consumer_runtimes.get(cluster_name)
    if runtime is None:
        runtime = _new_consumer_runtime_state()
        consumer_runtimes[cluster_name] = runtime
    return runtime


def _get_circuit_breaker(cluster_name: str) -> CircuitBreaker:
    """Return the circuit breaker assigned to a cluster consumer."""
    breaker = cluster_circuit_breakers.get(cluster_name)
    if breaker is None:
        breaker = CircuitBreaker(
            metric_name=f"{CONSUMER_METRIC_NAME}:{cluster_name}"
        )
        cluster_circuit_breakers[cluster_name] = breaker
    return breaker


def _utc_now_iso() -> str:
    """Render the current UTC timestamp as ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


def _timestamp_to_age_seconds(value: Optional[str]) -> Optional[float]:
    """Return the age of an ISO 8601 timestamp in seconds."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0)


def _update_runtime_lag(
    runtime: dict[str, object],
    cluster_name: str,
    destination: str,
    partition: int,
    offset: int,
    lag_value: int | None,
) -> None:
    """Update current lag snapshots for readiness and metrics."""
    if lag_value is None:
        return

    lag_key = f"{cluster_name}:{destination}:{partition}"
    lag_by_partition = dict(runtime.get("lag_by_partition") or {})
    lag_by_partition[lag_key] = lag_value
    runtime["lag_by_partition"] = lag_by_partition
    highwater_by_partition = dict(runtime.get("highwater_by_partition") or {})
    highwater_by_partition[lag_key] = offset + lag_value + 1
    runtime["highwater_by_partition"] = highwater_by_partition
    runtime["lag_total"] = sum(lag_by_partition.values())
    consumer_lag.labels(
        cluster=cluster_name,
        topic=destination,
        partition=str(partition),
    ).set(lag_value)
    consumer_current_offset.labels(
        cluster=cluster_name,
        topic=destination,
        partition=str(partition),
    ).set(offset)


def _record_committed_offsets(
    runtime: dict[str, object],
    cluster_name: str,
    events: list[KafkaEvent],
) -> None:
    """Update offset and commit-lag gauges from the committed batch."""
    highwater_by_partition = dict(runtime.get("highwater_by_partition") or {})
    for event in events:
        if event.kafka_topic is None or event.kafka_partition is None:
            continue
        if event.kafka_offset is None:
            continue

        partition_label = str(event.kafka_partition)
        consumer_committed_offset.labels(
            cluster=cluster_name,
            topic=event.kafka_topic,
            partition=partition_label,
        ).set(event.kafka_offset)

        lag_key = f"{cluster_name}:{event.kafka_topic}:{event.kafka_partition}"
        highwater = highwater_by_partition.get(lag_key)
        if highwater is None:
            continue

        commit_lag = max(int(highwater) - int(event.kafka_offset) - 1, 0)
        consumer_commit_lag.labels(
            cluster=cluster_name,
            topic=event.kafka_topic,
            partition=partition_label,
        ).set(commit_lag)


def _record_successful_commit(runtime: dict[str, object]) -> None:
    """Record the latest successful broker commit time."""
    timestamp = datetime.now(timezone.utc)
    runtime["last_commit_at"] = timestamp.isoformat()
    consumer_last_successful_commit_timestamp_seconds.set(
        timestamp.timestamp()
    )


def get_consumer_health() -> dict[str, object]:
    """Aggregate runtime snapshots from all configured broker clusters."""
    runtimes = {
        cluster_name: runtime
        for cluster_name, runtime in consumer_runtimes.items()
    }
    cluster_count = len(runtimes)
    connected = cluster_count > 0 and all(
        bool(runtime["connected"]) for runtime in runtimes.values()
    )
    running = cluster_count > 0 and all(
        bool(runtime["running"]) for runtime in runtimes.values()
    )
    lag_by_partition: dict[str, int] = {}
    topics: list[str] = []
    errors: list[str] = []
    last_message_at_values: list[str] = []
    last_commit_at_values: list[str] = []
    breaker_states: list[str] = []
    clusters_payload: dict[str, dict[str, object]] = {}

    for cluster_name, runtime in runtimes.items():
        cluster_topics = list(runtime.get("topics") or [])
        topics.extend(cluster_topics)
        lag_by_partition.update(dict(runtime.get("lag_by_partition") or {}))
        if runtime.get("last_error"):
            errors.append(f"{cluster_name}: {runtime['last_error']}")
        if runtime.get("last_message_at"):
            last_message_at_values.append(str(runtime["last_message_at"]))
        if runtime.get("last_commit_at"):
            last_commit_at_values.append(str(runtime["last_commit_at"]))

        breaker_state = _get_circuit_breaker(cluster_name).state
        breaker_states.append(breaker_state)
        clusters_payload[cluster_name] = {
            "running": runtime["running"],
            "connected": runtime["connected"],
            "broker_kind": runtime.get("broker_kind", "kafka"),
            "topics": cluster_topics,
            "last_error": runtime["last_error"],
            "last_message_at": runtime["last_message_at"],
            "last_commit_at": runtime["last_commit_at"],
            "last_commit_age_seconds": _timestamp_to_age_seconds(
                runtime["last_commit_at"]
            ),
            "lag_by_partition": dict(runtime.get("lag_by_partition") or {}),
            "lag_total": runtime["lag_total"],
            "dlq_messages_total": runtime["dlq_messages_total"],
            "circuit_breaker_state": breaker_state,
        }

    if any(state == "open" for state in breaker_states):
        aggregate_breaker_state = "open"
    elif any(state == "half-open" for state in breaker_states):
        aggregate_breaker_state = "half-open"
    else:
        aggregate_breaker_state = "closed"

    status = "healthy" if running and connected else "unhealthy"
    return {
        "status": status,
        "running": running,
        "connected": connected,
        "topics": sorted(dict.fromkeys(topics)),
        "last_error": "; ".join(errors) if errors else None,
        "last_message_at": (
            max(last_message_at_values)
            if last_message_at_values
            else None
        ),
        "last_commit_at": (
            max(last_commit_at_values)
            if last_commit_at_values
            else None
        ),
        "last_commit_age_seconds": _timestamp_to_age_seconds(
            max(last_commit_at_values) if last_commit_at_values else None
        ),
        "lag_by_partition": lag_by_partition,
        "lag_total": sum(
            int(runtime.get("lag_total") or 0)
            for runtime in runtimes.values()
        ),
        "dlq_messages_total": max(
            [
                int(runtime.get("dlq_messages_total") or 0)
                for runtime in runtimes.values()
            ]
            or [0]
        ),
        "circuit_breaker_state": aggregate_breaker_state,
        "clusters": clusters_payload,
    }
