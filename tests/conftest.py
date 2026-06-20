from __future__ import annotations

import importlib
import sys
from datetime import datetime
from pathlib import Path

import pytest

from ingestion_quota import IngestionQuotaLimiter
from models import KafkaEvent


# ── CI-safe timing constants for quota throttling tests ─────────────────
# These values are generous enough for constrained CI runners while keeping
# the test fast (~250ms per invocation).
CI_SAFE_WINDOW_SECONDS: float = 0.5
CI_SAFE_MAX_THROTTLE_SECONDS: float = 0.5


# ── Fake DB infrastructure for ingestion quota tests ────────────────────


class FakeQuotaResult:
    """Fake SQLAlchemy result that returns a single scalar."""

    def __init__(self, value: int | None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> int | None:
        return self._value


class FakeQuotaTransaction:
    """Fake async DB transaction context manager."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakeQuotaSession:
    """Fake async DB session that tracks execute calls and simulates
    quota UPSERT semantics via a shared in-memory store."""

    def __init__(
        self,
        store: dict[tuple[str, str, datetime], int],
        *,
        factory: FakeQuotaSessionFactory,
    ) -> None:
        self.store = store
        self.factory = factory

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def begin(self):
        return FakeQuotaTransaction()

    async def execute(self, statement):
        self.factory.execute_count += 1
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
    """Creates fake sessions backed by one shared in-memory store.

    Tracks cumulative ``execute_count`` across all created sessions
    so tests can assert on DB round-trip volume.
    """

    def __init__(self) -> None:
        self.store: dict[tuple[str, str, datetime], int] = {}
        self.execute_count: int = 0

    def __call__(self):
        return FakeQuotaSession(self.store, factory=self)


def quota_event(
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


def build_quota_limiter(
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


@pytest.fixture
def quota_session_factory() -> FakeQuotaSessionFactory:
    """Return a fresh ``FakeQuotaSessionFactory`` with ``execute_count``
    tracking, ready for quota tests that measure DB round-trips."""
    return FakeQuotaSessionFactory()


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
SRC_ROOT = REPO_ROOT / "src"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

sandbox_pytest_skip_reason = importlib.import_module(
    "examples.sandbox.preflight"
).sandbox_pytest_skip_reason


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Default repository tests to the core-platform marker.

    Sandbox validation currently runs through the live integration and smoke
    scripts rather than pytest modules. This hook makes the core-vs-sandbox
    split explicit for CI and local development without requiring every test
    file to repeat the same marker.
    """
    for item in items:
        if item.get_closest_marker("sandbox") is None:
            item.add_marker(pytest.mark.core)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip sandbox tests cleanly when the live stack is unavailable."""
    if item.get_closest_marker("sandbox") is None:
        return

    skip_reason = sandbox_pytest_skip_reason()
    if skip_reason is not None:
        pytest.skip(skip_reason)
