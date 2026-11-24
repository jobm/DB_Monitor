from __future__ import annotations

import importlib
import sys
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path

import pytest

from db_monitor.ingestion.quota import IngestionQuotaLimiter
from db_monitor.core.models import KafkaEvent


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


# ── Live PostgreSQL fixture ────────────────────────────────────────────


def _postgres_test_url() -> str:
    """Return the configured PostgreSQL URL for live database tests."""
    import os

    return (
        os.environ.get("DB_MONITOR_TEST_URL")
        or os.environ.get("MONITOR_POSTGRES_URL")
        or os.environ.get("POSTGRES_URL")
        or "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
    )


@pytest.fixture
async def migrated_postgres_url() -> AsyncIterator[str]:
    """Create a temporary database with every migration applied.

    Yields the SQLAlchemy URL for the temporary database and drops it
    during teardown.  Tests using this fixture only need PostgreSQL to be
    reachable; they do not need the full HTTP sandbox stack.
    """
    import os

    from asyncpg import create_pool
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import sessionmaker

    from db_monitor.core.migrations import MIGRATIONS, apply_migrations

    admin_url = _postgres_test_url()
    admin_dsn = admin_url.replace("+asyncpg", "")

    probe = create_async_engine(admin_url)
    try:
        async with probe.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        await probe.dispose()
        pytest.skip(f"PostgreSQL at {admin_url} is not reachable: {exc}")
    else:
        await probe.dispose()

    test_db = f"db_monitor_pytest_{os.urandom(6).hex()}"

    pool = await create_pool(admin_dsn, min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            await conn.execute(f'CREATE DATABASE "{test_db}"')
    finally:
        await pool.close()

    test_url = admin_url.rsplit("/", 1)[0] + f"/{test_db}"
    engine = create_async_engine(test_url)
    try:
        session_factory = sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        await apply_migrations(session_factory, engine, MIGRATIONS)
        yield test_url
    finally:
        await engine.dispose()
        pool = await create_pool(admin_dsn, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = $1
                      AND pid <> pg_backend_pid()
                    """,
                    test_db,
                )
                await conn.execute(f'DROP DATABASE IF EXISTS "{test_db}"')
        finally:
            await pool.close()


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
SRC_ROOT = REPO_ROOT / "src"

# Keep sys.path manipulation for legacy app/ modules that haven't been
# migrated to src/db_monitor/ yet. Once all modules are moved, remove these.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

sandbox_pytest_skip_reason = importlib.import_module(
    "examples.sandbox.preflight"
).sandbox_pytest_skip_reason


# ── Module-level state that must be reset between tests ────────────────
# These dicts are mutated by tests in test_consumer_service.py and can
# cause order-dependent failures if not cleaned up.
_CONSUMER_MODULE_STATE_ATTRS = (
    ("consumer.consumer", "consumer_runtime"),
    ("consumer.consumer", "consumer_runtimes"),
    ("consumer.consumer", "cluster_circuit_breakers"),
)


@pytest.fixture(autouse=True)
def _reset_consumer_module_state() -> None:
    """Clear module-level consumer state before each test to prevent
    order-dependent test pollution."""
    import importlib

    for mod_name, attr_name in _CONSUMER_MODULE_STATE_ATTRS:
        mod = importlib.import_module(f"db_monitor.{mod_name}")
        obj = getattr(mod, attr_name, None)
        if obj is not None and isinstance(obj, dict):
            obj.clear()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Default repository tests to the core-platform marker.

    Sandbox validation currently runs through the live integration and smoke
    scripts rather than pytest modules. This hook makes the core-vs-sandbox
    split explicit for CI and local development without requiring every test
    file to repeat the same marker.
    """
    for item in items:
        if (
            item.get_closest_marker("sandbox") is None
            and item.get_closest_marker("slow") is None
        ):
            item.add_marker(pytest.mark.core)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip sandbox tests cleanly when the live stack is unavailable."""
    if item.get_closest_marker("sandbox") is None:
        return

    skip_reason = sandbox_pytest_skip_reason()
    if skip_reason is not None:
        pytest.skip(skip_reason)
