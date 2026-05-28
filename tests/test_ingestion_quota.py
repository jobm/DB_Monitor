from __future__ import annotations

from datetime import datetime

import pytest

from ingestion_quota import IngestionQuotaLimiter
from models import KafkaEvent


def _event(
    *,
    service_name: str = "orderdb",
    tenant_id: str | None = None,
) -> KafkaEvent:
    """Build a minimal event for quota tests."""
    event_data: dict[str, object] = {"after": {"id": 1}}
    if tenant_id is not None:
        event_data["tenant_id"] = tenant_id

    return KafkaEvent(
        service_name=service_name,
        kafka_topic=f"{service_name}.public.orders",
        kafka_partition=0,
        kafka_offset=1,
        event_data=event_data,
    )


class FakeQuotaResult:
    def __init__(self, value: int | None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> int | None:
        return self._value


class FakeQuotaTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakeQuotaSession:
    def __init__(self, store: dict[tuple[str, str, datetime], int]) -> None:
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def begin(self):
        return FakeQuotaTransaction()

    async def execute(self, statement):
        params = statement.compile().params
        dimension = params["dimension"]
        identity = params["identity"]
        window_start = params["window_start"]
        limit = statement._post_values_clause.update_whereclause.right.value
        key = (dimension, identity, window_start)
        current_value = self.store.get(key, 0)

        if current_value < limit:
            self.store[key] = current_value + 1
            return FakeQuotaResult(self.store[key])

        return FakeQuotaResult(None)


class FakeQuotaSessionFactory:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str, datetime], int] = {}

    def __call__(self):
        return FakeQuotaSession(self.store)


def _limiter(
    *,
    source_default_limit: int,
    tenant_default_limit: int = 0,
    tenant_overrides: dict[str, int] | None = None,
    mode: str = "drop",
    window_seconds: float = 60.0,
    max_throttle_seconds: float = 1.0,
    session_factory: FakeQuotaSessionFactory | None = None,
) -> IngestionQuotaLimiter:
    """Build a quota limiter backed by one shared fake durable store."""
    return IngestionQuotaLimiter(
        enabled=True,
        window_seconds=window_seconds,
        source_default_limit=source_default_limit,
        source_overrides={},
        tenant_default_limit=tenant_default_limit,
        tenant_overrides=tenant_overrides or {},
        mode=mode,
        max_throttle_seconds=max_throttle_seconds,
        session_factory=session_factory or FakeQuotaSessionFactory(),
    )


@pytest.mark.anyio
async def test_quota_limiter_drops_source_events_when_over_limit() -> None:
    """Source quota mode drop should reject events beyond the window."""
    limiter = _limiter(source_default_limit=1)

    first = await limiter.apply(_event(service_name="orderdb"))
    second = await limiter.apply(_event(service_name="orderdb"))

    assert first.allowed is True
    assert second.allowed is False
    assert second.dropped is True
    assert second.dimension == "source"
    assert second.key == "orderdb"


@pytest.mark.anyio
async def test_quota_limiter_honors_tenant_override() -> None:
    """Tenant-specific quota overrides should apply when tenant is present."""
    limiter = _limiter(
        source_default_limit=0,
        tenant_overrides={"tenant-a": 1},
    )

    first = await limiter.apply(_event(tenant_id="tenant-a"))
    second = await limiter.apply(_event(tenant_id="tenant-a"))

    assert first.allowed is True
    assert second.allowed is False
    assert second.dimension == "tenant"
    assert second.key == "tenant-a"


@pytest.mark.anyio
async def test_quota_limiter_throttles_then_allows() -> None:
    """Throttle mode should delay over-limit events until the window resets."""
    limiter = _limiter(
        source_default_limit=1,
        mode="throttle",
        window_seconds=0.02,
        max_throttle_seconds=0.02,
    )

    first = await limiter.apply(_event(service_name="shippingdb"))
    second = await limiter.apply(_event(service_name="shippingdb"))

    assert first.allowed is True
    assert second.allowed is True
    assert second.throttled is True
    assert second.sleep_seconds > 0


@pytest.mark.anyio
async def test_quota_limiter_persists_across_instances() -> None:
    """A restarted limiter should honor the same shared quota window."""
    session_factory = FakeQuotaSessionFactory()
    first_limiter = _limiter(
        source_default_limit=1,
        session_factory=session_factory,
    )
    second_limiter = _limiter(
        source_default_limit=1,
        session_factory=session_factory,
    )

    first = await first_limiter.apply(_event(service_name="catalogdb"))
    second = await second_limiter.apply(_event(service_name="catalogdb"))

    assert first.allowed is True
    assert second.allowed is False
    assert second.dropped is True
