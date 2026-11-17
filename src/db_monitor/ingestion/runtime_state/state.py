"""Mutable consumer runtime snapshots and state accessors."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def _new_consumer_runtime_state() -> dict[str, object]:
    """Build a clean runtime state structure for a broker consumer."""
    return {
        "running": False,
        "connected": False,
        "reconnecting": False,
        "reconnect_attempts": 0,
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


consumer_runtime: dict[str, object] = _new_consumer_runtime_state()
consumer_runtimes: dict[str, dict[str, object]] = {"default": consumer_runtime}


def _get_consumer_runtime(cluster_name: str) -> dict[str, object]:
    """Return the mutable runtime state for a cluster consumer."""
    if cluster_name == "default":
        return consumer_runtime

    runtime = consumer_runtimes.get(cluster_name)
    if runtime is None:
        runtime = _new_consumer_runtime_state()
        consumer_runtimes[cluster_name] = runtime
    return runtime


def _utc_now_iso() -> str:
    """Render the current UTC timestamp as ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


def _timestamp_to_age_seconds(value: Optional[str]) -> Optional[float]:
    """Return the age of an ISO 8601 timestamp in seconds."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0)
