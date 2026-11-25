"""Consumer runtime metric updates and health aggregation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from db_monitor.core.models import KafkaEvent
from db_monitor.metrics import (
    consumer_commit_lag,
    consumer_committed_offset,
    consumer_current_offset,
    consumer_lag,
    consumer_last_successful_commit_timestamp_seconds,
)
from db_monitor.ingestion.runtime_state.breaker import _get_circuit_breaker
from db_monitor.ingestion.runtime_state.state import (
    consumer_runtimes,
    _timestamp_to_age_seconds,
)


def _update_runtime_lag(
    runtime: dict[str, object],
    cluster_name: str,
    destination: str,
    partition: int,
    offset: int,
    lag_value: int | None,
) -> None:
    """Record the position of the message just consumed.

    The broker-side lag is refreshed separately (``_refresh_runtime_lag``);
    the per-message value is only a stale fetch-time estimate and is used
    purely to keep the highwater watermark for commit-lag metrics.
    """
    if lag_value is None:
        return

    lag_key = f"{cluster_name}:{destination}:{partition}"
    # Only track the shared highwater watermark here. Lag is refreshed from
    # broker end offsets: per-message values come from a cached fetch response
    # and under-report how far behind the consumer really is.
    highwater_by_partition = dict(
        cast(dict[str, int], runtime.get("highwater_by_partition") or {})
    )
    highwater_by_partition[lag_key] = offset + lag_value + 1
    runtime["highwater_by_partition"] = highwater_by_partition
    consumer_current_offset.labels(
        cluster=cluster_name,
        topic=destination,
        partition=str(partition),
    ).set(offset)


def _refresh_runtime_lag(
    runtime: dict[str, object],
    cluster_name: str,
    snapshot: dict[tuple[str, int], int],
) -> None:
    """Replace cached lag with a broker-side snapshot for assigned partitions."""
    lag_by_partition = {
        f"{cluster_name}:{destination}:{partition}": lag
        for (destination, partition), lag in snapshot.items()
    }
    runtime["lag_by_partition"] = lag_by_partition
    runtime["lag_total"] = sum(lag_by_partition.values())
    for (destination, partition), lag in snapshot.items():
        consumer_lag.labels(
            cluster=cluster_name,
            topic=destination,
            partition=str(partition),
        ).set(lag)


def _record_committed_offsets(
    runtime: dict[str, object],
    cluster_name: str,
    events: list[KafkaEvent],
) -> None:
    """Update offset and commit-lag gauges from the committed batch."""
    highwater_by_partition = dict(
        cast(dict[str, int], runtime.get("highwater_by_partition") or {})
    )
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
    reconnecting = any(
        bool(runtime.get("reconnecting")) for runtime in runtimes.values()
    )
    lag_by_partition: dict[str, int] = {}
    topics: list[str] = []
    errors: list[str] = []
    last_message_at_values: list[str] = []
    last_commit_at_values: list[str] = []
    breaker_states: list[str] = []
    clusters_payload: dict[str, dict[str, object]] = {}

    for cluster_name, runtime in runtimes.items():
        cluster_topics = list(
            cast(list[str], runtime.get("topics") or [])
        )
        topics.extend(cluster_topics)
        lag_by_partition.update(
            cast(dict[str, int], runtime.get("lag_by_partition") or {})
        )
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
            "reconnecting": bool(runtime.get("reconnecting")),
            "reconnect_attempts": int(
                cast(int, runtime.get("reconnect_attempts") or 0)
            ),
            "broker_kind": runtime.get("broker_kind", "kafka"),
            "topics": cluster_topics,
            "last_error": runtime["last_error"],
            "last_message_at": runtime["last_message_at"],
            "last_commit_at": runtime["last_commit_at"],
            "last_commit_age_seconds": _timestamp_to_age_seconds(
                cast(str | None, runtime["last_commit_at"])
            ),
            "lag_by_partition": dict(
            cast(dict[str, int], runtime.get("lag_by_partition") or {})
        ),
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
        "reconnecting": reconnecting,
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
            int(cast(int, runtime.get("lag_total") or 0))
            for runtime in runtimes.values()
        ),
        "dlq_messages_total": max(
            [
                int(cast(int, runtime.get("dlq_messages_total") or 0))
                for runtime in runtimes.values()
            ]
            or [0]
        ),
        "circuit_breaker_state": aggregate_breaker_state,
        "clusters": clusters_payload,
    }
