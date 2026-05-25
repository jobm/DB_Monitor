"""Ingestion quota enforcement for source and tenant identities."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from core.db import AsyncSessionLocal
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
from core.models import IngestionQuotaWindow, KafkaEvent
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
    """Apply shared quota windows for source and tenant dimensions."""

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
        session_factory: Callable = AsyncSessionLocal,
    ) -> None:
        self.enabled = enabled
        self.window_seconds = window_seconds
        self.source_default_limit = source_default_limit
        self.source_overrides = dict(source_overrides)
        self.tenant_default_limit = tenant_default_limit
        self.tenant_overrides = dict(tenant_overrides)
        self.mode = mode
        self.max_throttle_seconds = max_throttle_seconds
        self.session_factory = session_factory

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
                self.source_overrides.get(
                    source_name,
                    self.source_default_limit,
                ),
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

            allowed, dimension_throttled, sleep_seconds = (
                await self._apply_dimension_quota(
                    dimension=dimension,
                    key=key,
                    limit=limit,
                )
            )
            throttled = throttled or dimension_throttled
            total_sleep_seconds += sleep_seconds

            if not allowed:
                return QuotaDecision(
                    allowed=False,
                    dropped=True,
                    throttled=throttled,
                    sleep_seconds=total_sleep_seconds,
                    dimension=dimension,
                    key=key,
                    limit=limit,
                )

        return QuotaDecision(
            allowed=True,
            dropped=False,
            throttled=throttled,
            sleep_seconds=total_sleep_seconds,
        )

    async def _apply_dimension_quota(
        self,
        *,
        dimension: str,
        key: str,
        limit: int,
    ) -> tuple[bool, bool, float]:
        """Reserve one quota slot, optionally sleeping until the window resets."""
        total_sleep_seconds = 0.0
        throttled = False

        while True:
            now = datetime.now(timezone.utc)
            window_start = self._window_start_for(now)
            statement = (
                pg_insert(IngestionQuotaWindow)
                .values(
                    dimension=dimension,
                    identity=key,
                    window_start=window_start,
                    event_count=1,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[
                        IngestionQuotaWindow.dimension,
                        IngestionQuotaWindow.identity,
                        IngestionQuotaWindow.window_start,
                    ],
                    set_={
                        "event_count": IngestionQuotaWindow.event_count + 1,
                        "updated_at": now,
                    },
                    where=IngestionQuotaWindow.event_count < limit,
                )
                .returning(IngestionQuotaWindow.event_count)
            )

            async with self.session_factory() as session:
                async with session.begin():
                    result = await session.execute(statement)
                    event_count = result.scalar_one_or_none()

            if event_count is not None:
                return True, throttled, total_sleep_seconds

            if self.mode == "drop":
                return False, throttled, total_sleep_seconds

            throttled = True
            wait_seconds = self._sleep_seconds_until_reset(window_start, now)
            if wait_seconds <= 0:
                continue

            total_sleep_seconds += wait_seconds
            await asyncio.sleep(wait_seconds)

    def _window_start_for(self, moment: datetime) -> datetime:
        """Round one timestamp down to the current quota window boundary."""
        window_seconds = float(self.window_seconds)
        window_epoch = (moment.timestamp() // window_seconds) * window_seconds
        return datetime.fromtimestamp(window_epoch, tz=timezone.utc)

    def _sleep_seconds_until_reset(
        self,
        window_start: datetime,
        moment: datetime,
    ) -> float:
        """Return the throttle delay until the shared window can reset."""
        window_end = window_start + timedelta(seconds=self.window_seconds)
        return min(
            max((window_end - moment).total_seconds(), 0.0),
            self.max_throttle_seconds,
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
