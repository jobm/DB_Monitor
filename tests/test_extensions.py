from __future__ import annotations

import pytest

import extensions


@pytest.mark.anyio
async def test_init_db_applies_migrations_in_apply_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply mode should run tracked migrations instead of validation."""
    called: list[str] = []

    async def fake_apply(session_factory, db_engine) -> list[str]:
        called.append("apply")
        assert session_factory is extensions.AsyncSessionLocal
        assert db_engine is extensions.engine
        return []

    async def fake_validate(db_engine) -> None:
        called.append("validate")

    monkeypatch.setattr(extensions, "DB_SCHEMA_MODE", "apply")
    monkeypatch.setattr("migrations.apply_migrations", fake_apply)
    monkeypatch.setattr("migrations.validate_migrations", fake_validate)

    await extensions.init_db()

    assert called == ["apply"]


@pytest.mark.anyio
async def test_init_db_validates_in_validate_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validate mode should not attempt to mutate schema on startup."""
    called: list[str] = []

    async def fake_apply(session_factory, db_engine) -> list[str]:
        called.append("apply")
        return []

    async def fake_validate(db_engine) -> None:
        called.append("validate")
        assert db_engine is extensions.engine

    monkeypatch.setattr(extensions, "DB_SCHEMA_MODE", "validate")
    monkeypatch.setattr("migrations.apply_migrations", fake_apply)
    monkeypatch.setattr("migrations.validate_migrations", fake_validate)

    await extensions.init_db()

    assert called == ["validate"]


def test_build_engine_uses_pool_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Engine creation should honor the configured async pool settings."""
    captured: dict[str, object] = {}

    def fake_create_async_engine(url: str, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(extensions, "create_async_engine", fake_create_async_engine)
    monkeypatch.setattr(extensions, "POSTGRES_URL", "postgresql+asyncpg://db")
    monkeypatch.setattr(extensions, "SQLALCHEMY_ECHO", True)
    monkeypatch.setattr(extensions, "DB_POOL_PRE_PING", True)
    monkeypatch.setattr(extensions, "DB_POOL_SIZE", 15)
    monkeypatch.setattr(extensions, "DB_MAX_OVERFLOW", 5)
    monkeypatch.setattr(extensions, "DB_POOL_TIMEOUT_SECONDS", 12)
    monkeypatch.setattr(extensions, "DB_POOL_RECYCLE_SECONDS", 120)

    engine = extensions.build_engine()

    assert engine is not None
    assert captured == {
        "url": "postgresql+asyncpg://db",
        "echo": True,
        "future": True,
        "pool_pre_ping": True,
        "pool_size": 15,
        "max_overflow": 5,
        "pool_timeout": 12,
        "pool_recycle": 120,
    }


def test_instrument_pool_metrics_registers_pool_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pool instrumentation should register listeners and update gauges."""
    registered_events: list[str] = []

    class FakePool:
        def size(self) -> int:
            return 10

        def checkedout(self) -> int:
            return 4

        def checkedin(self) -> int:
            return 6

        def overflow(self) -> int:
            return 2

    class FakeSyncEngine:
        pool = FakePool()

    class FakeEngine:
        sync_engine = FakeSyncEngine()

    def fake_listen(pool, event_name: str, callback) -> None:
        del pool, callback
        registered_events.append(event_name)

    monkeypatch.setattr(extensions.event, "listen", fake_listen)

    extensions._instrument_pool_metrics(FakeEngine())

    assert registered_events == [
        "connect",
        "checkout",
        "checkin",
        "close",
        "invalidate",
        "reset",
    ]
    assert extensions.active_connections._value.get() == 4
    assert extensions.db_pool_size._value.get() == 10
    assert extensions.db_pool_checked_in_connections._value.get() == 6
    assert extensions.db_pool_overflow_connections._value.get() == 2
    assert extensions.db_pool_utilization_ratio._value.get() == pytest.approx(
        4 / 12
    )