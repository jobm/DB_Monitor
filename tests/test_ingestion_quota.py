from __future__ import annotations

import pytest

from conftest import (
    CI_SAFE_MAX_THROTTLE_SECONDS,
    CI_SAFE_WINDOW_SECONDS,
    FakeQuotaSessionFactory,
    build_quota_limiter,
    quota_event,
)


@pytest.mark.anyio
async def test_quota_limiter_drops_source_events_when_over_limit() -> None:
    """Source quota mode drop should reject events beyond the window."""
    limiter = build_quota_limiter(source_default_limit=1)

    first = await limiter.apply(quota_event(service_name="orderdb"))
    second = await limiter.apply(quota_event(service_name="orderdb"))

    assert first.allowed is True
    assert second.allowed is False
    assert second.dropped is True
    assert second.dimension == "source"
    assert second.key == "orderdb"


@pytest.mark.anyio
async def test_quota_limiter_honors_tenant_override() -> None:
    """Tenant-specific quota overrides should apply when tenant is present."""
    limiter = build_quota_limiter(
        source_default_limit=0,
        tenant_overrides={"tenant-a": 1},
    )

    first = await limiter.apply(quota_event(tenant_id="tenant-a"))
    second = await limiter.apply(quota_event(tenant_id="tenant-a"))

    assert first.allowed is True
    assert second.allowed is False
    assert second.dimension == "tenant"
    assert second.key == "tenant-a"


@pytest.mark.anyio
async def test_quota_limiter_throttles_then_allows() -> None:
    """Throttle mode should delay over-limit events until the window resets."""
    limiter = build_quota_limiter(
        source_default_limit=1,
        mode="throttle",
        window_seconds=CI_SAFE_WINDOW_SECONDS,
        max_throttle_seconds=CI_SAFE_MAX_THROTTLE_SECONDS,
    )

    first = await limiter.apply(quota_event(service_name="shippingdb"))
    second = await limiter.apply(quota_event(service_name="shippingdb"))

    assert first.allowed is True
    assert second.allowed is True
    assert second.throttled is True
    assert second.sleep_seconds > 0


@pytest.mark.anyio
async def test_quota_limiter_persists_across_instances(
    quota_session_factory: FakeQuotaSessionFactory,
) -> None:
    """A restarted limiter should honor the same shared quota window."""
    session_factory = quota_session_factory
    first_limiter = build_quota_limiter(
        source_default_limit=1,
        session_factory=session_factory,
    )
    second_limiter = build_quota_limiter(
        source_default_limit=1,
        session_factory=session_factory,
    )

    first = await first_limiter.apply(quota_event(service_name="catalogdb"))
    second = await second_limiter.apply(quota_event(service_name="catalogdb"))

    assert first.allowed is True
    assert second.allowed is False
    assert second.dropped is True


@pytest.mark.anyio
async def test_quota_limiter_throttle_db_roundtrips(
    quota_session_factory: FakeQuotaSessionFactory,
) -> None:
    """Benchmark: a throttled event should make minimal DB round-trips.

    The optimized inner sleep loop sleeps through the remainder of the
    quota window without touching the database. This test verifies that
    a throttled event triggers exactly:
      - 1 execute call to detect the over-limit condition (fail)
      - 1 execute call to retry after the window rolls over (succeed)

    Before the optimization, with max_throttle_seconds << window_seconds,
    each throttled event would make O(window / max_throttle) failed
    execute calls — up to 5× more in this scenario.
    """
    session_factory = quota_session_factory
    limiter = build_quota_limiter(
        source_default_limit=1,
        mode="throttle",
        window_seconds=CI_SAFE_WINDOW_SECONDS,
        max_throttle_seconds=CI_SAFE_MAX_THROTTLE_SECONDS,
        session_factory=session_factory,
    )

    allowed = await limiter.apply(quota_event(service_name="benchdb"))
    assert allowed.allowed is True
    first_event_executes = session_factory.execute_count
    assert first_event_executes == 1, (
        "First (allowed) event should make exactly 1 execute call"
    )

    throttled = await limiter.apply(quota_event(service_name="benchdb"))
    assert throttled.allowed is True
    assert throttled.throttled is True

    throttled_event_executes = (
        session_factory.execute_count - first_event_executes
    )
    assert throttled_event_executes == 2, (
        f"Throttled event made {throttled_event_executes} execute calls, "
        "expected 2 (one failed UPSERT + one retry after window reset)"
    )


@pytest.mark.anyio
async def test_quota_limiter_throttle_db_roundtrips_large_window(
    quota_session_factory: FakeQuotaSessionFactory,
) -> None:
    """Benchmark: even with max_throttle << window, DB round-trips stay low.

    This is the worst case for the old naive loop: a large window with a
    small max_throttle cap would force many iterations. The optimized
    inner sleep loop eliminates intermediate DB queries.

    With window_seconds=1.0 and max_throttle_seconds=0.1, the old code
    would do up to 10 execute calls per throttled event (one per 0.1s
    sleep chunk). The optimized code should still do exactly 2.
    """
    session_factory = quota_session_factory
    limiter = build_quota_limiter(
        source_default_limit=1,
        mode="throttle",
        window_seconds=1.0,
        max_throttle_seconds=0.1,
        session_factory=session_factory,
    )

    allowed = await limiter.apply(quota_event(service_name="bigwindowdb"))
    assert allowed.allowed is True
    first_event_executes = session_factory.execute_count
    assert first_event_executes == 1

    throttled = await limiter.apply(quota_event(service_name="bigwindowdb"))
    assert throttled.allowed is True
    assert throttled.throttled is True

    throttled_event_executes = (
        session_factory.execute_count - first_event_executes
    )
    # The inner sleep loop sleeps in 0.1s chunks through ~1s of window
    # remaining, but only hits the DB twice: once to discover the limit
    # is exceeded, and once after the window rolls over.
    assert throttled_event_executes == 2, (
        f"Throttled event made {throttled_event_executes} execute calls, "
        "expected 2 (one failed UPSERT + one retry after window reset). "
        "The inner sleep loop should avoid intermediate DB queries."
    )


@pytest.mark.anyio
async def test_quota_limiter_drop_mode_db_roundtrips(
    quota_session_factory: FakeQuotaSessionFactory,
) -> None:
    """Drop mode should never retry — exactly 1 DB round-trip per event."""
    session_factory = quota_session_factory
    limiter = build_quota_limiter(
        source_default_limit=1,
        mode="drop",
        session_factory=session_factory,
    )

    allowed = await limiter.apply(quota_event(service_name="dropdb"))
    assert allowed.allowed is True
    first_event_executes = session_factory.execute_count
    assert first_event_executes == 1

    dropped = await limiter.apply(quota_event(service_name="dropdb"))
    assert dropped.allowed is False
    assert dropped.dropped is True

    dropped_event_executes = (
        session_factory.execute_count - first_event_executes
    )
    assert dropped_event_executes == 1, (
        f"Dropped event made {dropped_event_executes} execute calls, "
        "expected 1 (one failed UPSERT, no retries)"
    )


@pytest.mark.anyio
async def test_quota_limiter_throttle_zero_intermediate_writes(
    quota_session_factory: FakeQuotaSessionFactory,
) -> None:
    """The inner sleep loop must not touch the DB between throttle chunks.

    The optimized loop sleeps through window remainder in
    max_throttle_seconds-sized chunks without querying the database.
    We verify this by starting a throttled event with a window so large
    it cannot reset during the test, cancelling it mid-sleep, and
    asserting exactly 1 failed write (zero retries).
    """
    from anyio import fail_after

    session_factory = quota_session_factory
    limiter = build_quota_limiter(
        source_default_limit=1,
        mode="throttle",
        window_seconds=3600.0,  # huge — never resets during the test
        max_throttle_seconds=0.05,
        session_factory=session_factory,
    )

    # Fill the window
    allowed = await limiter.apply(quota_event(service_name="zerodb"))
    assert allowed.allowed is True
    assert session_factory.execute_count == 1

    # Start a throttled event and cancel it mid-sleep.
    # The inner sleep loop should have made zero DB writes.
    with pytest.raises(TimeoutError), fail_after(0.25):
        await limiter.apply(quota_event(service_name="zerodb"))

    # Exactly 1 additional execute call: the initial failed UPSERT.
    # Zero retries because the window never reset.
    assert session_factory.execute_count == 2, (
        f"Expected 2 execute calls (1 allowed + 1 failed throttled), "
        f"got {session_factory.execute_count}. "
        "The inner sleep loop must not make DB calls between sleeps."
    )


@pytest.mark.anyio
async def test_quota_limiter_throttle_concurrent() -> None:
    """Concurrent throttled events must all eventually succeed and respect
    ``max_throttle_seconds`` bounds.

    With ``limit=1`` and 5 concurrent events hitting the same source
    within one window:
      - Exactly 1 succeeds immediately (non-throttled).
      - The other 4 are throttled, enter the inner sleep loop, and wait
        for the window to reset.
      - After each reset the first to retry claims the slot; the rest
        sleep through another window.
      - All eventually return ``allowed=True``.
      - Each throttled event's ``sleep_seconds`` is positive and within
        a reasonable bound: at most ``max_throttle_seconds`` per window
        crossed.
    """
    import asyncio

    CONCURRENCY = 5
    limiter = build_quota_limiter(
        source_default_limit=1,
        mode="throttle",
        window_seconds=CI_SAFE_WINDOW_SECONDS,
        max_throttle_seconds=CI_SAFE_MAX_THROTTLE_SECONDS,
    )

    events = [quota_event(service_name="concurrentdb") for _ in range(CONCURRENCY)]
    results = await asyncio.gather(*[limiter.apply(e) for e in events])

    allowed = [r for r in results if r.allowed]
    throttled = [r for r in results if r.throttled]
    not_throttled = [r for r in results if not r.throttled]

    # All must eventually be allowed
    assert len(allowed) == CONCURRENCY, (
        f"Expected {CONCURRENCY} allowed, got {len(allowed)}"
    )

    # Exactly 1 was non-throttled (first to get the slot)
    assert len(not_throttled) == 1, (
        f"Expected 1 non-throttled, got {len(not_throttled)}"
    )

    # The rest were throttled
    assert len(throttled) == CONCURRENCY - 1, (
        f"Expected {CONCURRENCY - 1} throttled, got {len(throttled)}"
    )

    for r in throttled:
        assert r.sleep_seconds > 0, (
            f"Throttled event has sleep_seconds={r.sleep_seconds}, expected > 0"
        )
        # Each individual sleep chunk is bounded by max_throttle_seconds.
        # The total sleep is at most max_throttle_seconds per window
        # crossed; with CONCURRENCY events and limit=1, an event may
        # need up to (CONCURRENCY - 1) windows to get its slot.
        assert r.sleep_seconds <= CI_SAFE_MAX_THROTTLE_SECONDS * CONCURRENCY, (
            f"Throttled event sleep_seconds={r.sleep_seconds:.3f} exceeds "
            f"max_throttle_seconds * concurrency "
            f"({CI_SAFE_MAX_THROTTLE_SECONDS} * {CONCURRENCY})"
        )
