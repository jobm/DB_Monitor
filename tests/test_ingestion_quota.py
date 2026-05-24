from __future__ import annotations

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


@pytest.mark.anyio
async def test_quota_limiter_drops_source_events_when_over_limit() -> None:
    """Source quota mode drop should reject events beyond the window."""
    limiter = IngestionQuotaLimiter(
        enabled=True,
        window_seconds=60.0,
        source_default_limit=1,
        source_overrides={},
        tenant_default_limit=0,
        tenant_overrides={},
        mode="drop",
        max_throttle_seconds=1.0,
    )

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
    limiter = IngestionQuotaLimiter(
        enabled=True,
        window_seconds=60.0,
        source_default_limit=0,
        source_overrides={},
        tenant_default_limit=0,
        tenant_overrides={"tenant-a": 1},
        mode="drop",
        max_throttle_seconds=1.0,
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
    limiter = IngestionQuotaLimiter(
        enabled=True,
        window_seconds=0.02,
        source_default_limit=1,
        source_overrides={},
        tenant_default_limit=0,
        tenant_overrides={},
        mode="throttle",
        max_throttle_seconds=0.02,
    )

    first = await limiter.apply(_event(service_name="shippingdb"))
    second = await limiter.apply(_event(service_name="shippingdb"))

    assert first.allowed is True
    assert second.allowed is True
    assert second.throttled is True
    assert second.sleep_seconds > 0
