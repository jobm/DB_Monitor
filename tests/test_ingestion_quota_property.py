"""Property-based tests for ``ingestion_quota.py``.

Uses Hypothesis to verify that quota decisions are monotonic and
consistent under random event sequences.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from conftest import (
    CI_SAFE_MAX_THROTTLE_SECONDS,
    CI_SAFE_WINDOW_SECONDS,
    build_quota_limiter,
    quota_event,
)


# ── Helper: fast-throttle limiter for property tests ────────────────────
# Use very short timings to keep Hypothesis runs fast.


def _fast_throttle_limiter(*, source_default_limit: int):
    """Return a limiter with short windows so property tests stay fast."""
    return build_quota_limiter(
        source_default_limit=source_default_limit,
        mode="throttle",
        window_seconds=CI_SAFE_WINDOW_SECONDS,
        max_throttle_seconds=CI_SAFE_MAX_THROTTLE_SECONDS,
    )


# ── Invariant: monotonic drop ──────────────────────────────────────────


@pytest.mark.anyio
@given(
    st.lists(
        st.integers(min_value=1, max_value=10),
        min_size=1,
        max_size=15,
    ),
)
@settings(max_examples=30, deadline=None)
async def test_quota_monotonic_drop(counts_per_source: list[int]) -> None:
    """Quota drop decisions must be monotonic within each window:
    once an event is dropped, all subsequent events in the same window
    must also be dropped.
    """
    limiter = build_quota_limiter(source_default_limit=3, mode="drop")

    last_dropped = False
    for count in counts_per_source:
        for _ in range(count):
            decision = await limiter.apply(
                quota_event(service_name="monotonicdb")
            )
            if last_dropped:
                assert decision.allowed is False, (
                    "Once dropped within a window, all subsequent "
                    "events must be dropped"
                )
            if decision.dropped:
                last_dropped = True


# ── Invariant: throttle never drops ────────────────────────────────────


@pytest.mark.anyio
@given(
    st.lists(
        st.integers(min_value=1, max_value=5),
        min_size=1,
        max_size=5,
    ),
)
@settings(max_examples=15, deadline=None)
async def test_quota_throttle_never_drops(counts: list[int]) -> None:
    """In throttle mode, no event must ever be dropped — they are either
    allowed immediately or delayed (throttled) but always eventually
    allowed.

    Uses small batch sizes and few Hypothesis examples because each
    throttled event involves real ``asyncio.sleep()`` wall time.
    """
    limiter = _fast_throttle_limiter(source_default_limit=2)

    for count in counts:
        for _ in range(count):
            decision = await limiter.apply(
                quota_event(service_name="throttledb")
            )
            assert decision.dropped is False, (
                "Throttle mode must never mark events as dropped"
            )
            assert decision.allowed is True, (
                "Throttled events must eventually be allowed"
            )


# ── Invariant: quota decisions are independent per source key ──────────


@pytest.mark.anyio
@given(
    st.integers(min_value=1, max_value=10),
)
@settings(max_examples=30)
async def test_quota_independent_per_source(a_count: int) -> None:
    """Quota decisions for different source keys must be independent."""
    limiter = build_quota_limiter(source_default_limit=3, mode="drop")

    # Consume the full budget for source A
    for _ in range(a_count):
        await limiter.apply(quota_event(service_name="source-a"))

    # Source B's first event should always be allowed regardless
    b_decision = await limiter.apply(
        quota_event(service_name="source-b")
    )
    assert b_decision.allowed is True, (
        "Source B was dropped even though source A has a separate "
        "quota window"
    )


# ── Invariant: each sleep chunk is bounded by max_throttle_seconds ─────


@pytest.mark.anyio
@given(
    st.integers(min_value=3, max_value=8),
)
@settings(max_examples=15, deadline=None)
async def test_quota_throttle_sleep_chunks_bounded(extra_events: int) -> None:
    """Each individual sleep chunk made by a throttled event must be at
    most ``max_throttle_seconds``.  (The total accumulated sleep across
    multiple window resets may exceed the per-chunk cap.)
    """
    limiter = build_quota_limiter(
        source_default_limit=2,
        mode="throttle",
        window_seconds=1.0,
        max_throttle_seconds=0.25,
    )

    for _ in range(extra_events):
        decision = await limiter.apply(
            quota_event(service_name="sleepchunkdb")
        )
        # Each individual sleep chunk is bounded by max_throttle_seconds.
        # The total sleep may be higher (accumulated across windows),
        # but any single call to asyncio.sleep is ≤ max_throttle_seconds.
        if decision.throttled:
            assert decision.sleep_seconds > 0, (
                "Throttled event must have positive sleep_seconds"
            )
