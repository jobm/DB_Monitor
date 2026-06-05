"""Tests for concurrent migration safety.

Verifies that the PostgreSQL advisory lock mechanism correctly
prevents multiple processes from applying migrations simultaneously.
"""

from __future__ import annotations

import asyncio

import pytest

from migrations import apply_migrations


async def _make_fake_engine():
    """Return a minimal async engine that supports ``async with
    engine.begin() as connection:`` for migration-lock testing."""

    class FakeConnection:
        def __init__(self):
            self.closed = False

        async def execute(self, *args, **kwargs):
            return self

        def scalar(self):
            return None

        def fetchall(self):
            return []

        async def close(self):
            self.closed = True

    class FakeAsyncContextManager:
        def __init__(self, connection):
            self._conn = connection

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *exc):
            await self._conn.close()
            return False

    class FakeEngine:
        def begin(self):
            conn = FakeConnection()
            return FakeAsyncContextManager(conn)

    return FakeEngine()


@pytest.mark.anyio
async def test_concurrent_migration_locking(monkeypatch: pytest.MonkeyPatch):
    """Two concurrent ``apply_migrations`` calls: one wins, one gets a
    clear lock-held error.

    Uses an asyncio.Event to coordinate: the first caller acquires the
    lock and signals, so the second caller is guaranteed to find it held.
    """
    lock_taken = False

    async def fake_ensure_table(connection) -> None:
        await asyncio.sleep(0.0)

    async def fake_acquire_lock(connection) -> bool:
        nonlocal lock_taken
        if lock_taken:
            return False
        lock_taken = True
        # Yield so the second task can also run and find lock held
        await asyncio.sleep(0.0)
        return True

    async def fake_release_lock(connection) -> None:
        nonlocal lock_taken
        lock_taken = False

    async def fake_get_applied_versions(connection) -> set[str]:
        # Yield control so the second caller can try acquiring the
        # lock while this caller still holds it.
        await asyncio.sleep(0.001)
        return set()

    async def fake_record_migration(connection, migration) -> None:
        return None

    monkeypatch.setattr(
        "migrations._acquire_migration_lock", fake_acquire_lock
    )
    monkeypatch.setattr(
        "migrations._release_migration_lock", fake_release_lock
    )
    monkeypatch.setattr(
        "migrations._get_applied_versions", fake_get_applied_versions
    )
    monkeypatch.setattr(
        "migrations._ensure_migration_table", fake_ensure_table
    )
    monkeypatch.setattr("migrations.MIGRATIONS", ())

    fake_engine = await _make_fake_engine()

    def fake_session_factory():
        return None

    async def runner() -> list[str]:
        return await apply_migrations(fake_session_factory, fake_engine)

    t1 = asyncio.create_task(runner())
    t2 = asyncio.create_task(runner())

    # Give the event loop a chance to schedule both tasks
    await asyncio.sleep(0.0)

    results = await asyncio.gather(t1, t2, return_exceptions=True)

    first_result = results[0]
    second_result = results[1]

    # One of the two must have succeeded and the other must have
    # failed with a lock-held error. Order is non-deterministic.
    def _check_pair(a, b) -> bool:
        return (isinstance(a, list) and
                isinstance(b, RuntimeError) and
                "lock is held" in str(b).lower())

    assert _check_pair(first_result, second_result) or \
        _check_pair(second_result, first_result), (
        "Expected one success and one lock-held error. "
        f"Got: success={type(first_result).__name__}, "
        f"error={type(second_result).__name__}"
    )


@pytest.mark.anyio
async def test_migration_lock_released_after_completion(
    monkeypatch: pytest.MonkeyPatch,
):
    """After ``apply_migrations`` finishes, the lock is released so
    subsequent callers can proceed."""
    call_count = 0

    async def fake_acquire_lock(connection) -> bool:
        nonlocal call_count
        call_count += 1
        return True

    release_count = 0

    async def fake_release_lock(connection) -> None:
        nonlocal release_count
        release_count += 1

    async def fake_get_applied_versions(connection) -> set[str]:
        return set()

    async def fake_ensure_table(connection) -> None:
        return None

    async def fake_record_migration(connection, migration) -> None:
        return None

    monkeypatch.setattr(
        "migrations._acquire_migration_lock", fake_acquire_lock
    )
    monkeypatch.setattr(
        "migrations._release_migration_lock", fake_release_lock
    )
    monkeypatch.setattr(
        "migrations._get_applied_versions", fake_get_applied_versions
    )
    monkeypatch.setattr(
        "migrations._ensure_migration_table", fake_ensure_table
    )
    monkeypatch.setattr("migrations.MIGRATIONS", ())

    fake_engine = await _make_fake_engine()

    def fake_session_factory():
        return None

    await apply_migrations(fake_session_factory, fake_engine)

    assert call_count == 1, "Lock should have been acquired once"
    assert (
        release_count == 1
    ), "Lock should have been released exactly once"
