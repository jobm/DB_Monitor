"""Ingestion quota enforcement for source and tenant identities."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic

from core.config import (
    INGESTION_QUOTA_ENABLED,
    INGESTION_QUOTA_MAX_THROTTLE_SECONDS,
    INGESTION_QUOTA_MODE,
    INGESTION_QUOTA_WINDOW_SECONDS,
    INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW,
    INGESTION_SOURCE_QUOTAS,
    INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW,
    INGESTION_TENANT_QUOTAS,
)
from core.models import KafkaEvent
from source_metadata import extract_source_service_name


@dataclass(frozen=True)
class QuotaDecision:
    """Result for a single event quota evaluation."""

    allowed: bool
    dropped: bool
    throttled: bool
    sleep_seconds: float
    dimension: str | None = None
    key: str | None = None
    limit: int | None = None


class IngestionQuotaLimiter:
    """Apply in-memory quota windows for source and tenant dimensions."""

    def __init__(
        self,
        *,
        enabled: bool,
        window_seconds: float,
        source_default_limit: int,
        source_overrides: dict[str, int],
        tenant_default_limit: int,
        tenant_overrides: dict[str, int],
        mode: str,
        max_throttle_seconds: float,
    ) -> None:
        self.enabled = enabled
        self.window_seconds = window_seconds
        self.source_default_limit = source_default_limit
        self.source_overrides = dict(source_overrides)
        self.tenant_default_limit = tenant_default_limit
        self.tenant_overrides = dict(tenant_overrides)
        self.mode = mode
        self.max_throttle_seconds = max_throttle_seconds
        self._lock = asyncio.Lock()
        self._window_usage: dict[tuple[str, str], tuple[float, int]] = {}

    async def apply(self, event: KafkaEvent) -> QuotaDecision:
        """Check quotas and optionally throttle or drop one event."""
        if not self.enabled:
            return QuotaDecision(
                allowed=True,
                dropped=False,
                throttled=False,
                sleep_seconds=0.0,
            )

        source_name = self._source_name(event)
        tenant_name = self._tenant_name(event)
        checks = [
            (
                "source",
                source_name,
                self.source_overrides.get(source_name, self.source_default_limit),
            )
        ]

        if tenant_name is not None:
            checks.append(
                (
                    "tenant",
                    tenant_name,
                    self.tenant_overrides.get(
                        tenant_name,
                        self.tenant_default_limit,
                    ),
                )
            )

        total_sleep_seconds = 0.0
        throttled = False

        for dimension, key, limit in checks:
            if limit <= 0:
                continue

            while True:
                now = monotonic()
                should_wait = 0.0

                async with self._lock:
                    window_start, count = self._window_usage.get(
                        (dimension, key),
                        (now, 0),
                    )

                    if now - window_start >= self.window_seconds:
                        window_start = now
                        count = 0

                    if count < limit:
                        self._window_usage[(dimension, key)] = (
                            window_start,
                            count + 1,
                        )
                        break

                    if self.mode == "drop":
                        return QuotaDecision(
                            allowed=False,
                            dropped=True,
                            throttled=False,
                            sleep_seconds=total_sleep_seconds,
                            dimension=dimension,
                            key=key,
                            limit=limit,
                        )

                    window_end = window_start + self.window_seconds
                    wait_until_reset = max(window_end - now, 0.0)
                    should_wait = min(
                        wait_until_reset,
                        self.max_throttle_seconds,
                    )

                if should_wait <= 0:
                    continue

                throttled = True
                total_sleep_seconds += should_wait
                await asyncio.sleep(should_wait)

        return QuotaDecision(
            allowed=True,
            dropped=False,
            throttled=throttled,
            sleep_seconds=total_sleep_seconds,
        )

    def _source_name(self, event: KafkaEvent) -> str:
        """Return source identity for source quota checks."""
        if event.service_name:
            return event.service_name
        source_service_name = extract_source_service_name(event.event_data)
        if source_service_name:
            return source_service_name
        if event.kafka_topic:
            return event.kafka_topic
        return "unknown"

    def _tenant_name(self, event: KafkaEvent) -> str | None:
        """Return tenant identity when one can be inferred from payload."""
        payload = event.event_data
        if not isinstance(payload, dict):
            return None

        direct_tenant = _tenant_from_mapping(payload)
        if direct_tenant:
            return direct_tenant

        nested_payload = payload.get("payload")
        if isinstance(nested_payload, dict):
            return _tenant_from_mapping(nested_payload)

        return None


def _tenant_from_mapping(payload: dict[str, object]) -> str | None:
    """Extract one normalized tenant identifier from a payload mapping."""
    for key in ("tenant_id", "tenant", "tenantId"):
        value = payload.get(key)
        if value is None:
            continue
        tenant = str(value).strip()
        if tenant:
            return tenant
    return None


quota_limiter = IngestionQuotaLimiter(
    enabled=INGESTION_QUOTA_ENABLED,
    window_seconds=float(INGESTION_QUOTA_WINDOW_SECONDS),
    source_default_limit=INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW,
    source_overrides=INGESTION_SOURCE_QUOTAS,
    tenant_default_limit=INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW,
    tenant_overrides=INGESTION_TENANT_QUOTAS,
    mode=INGESTION_QUOTA_MODE,
    max_throttle_seconds=INGESTION_QUOTA_MAX_THROTTLE_SECONDS,
)
