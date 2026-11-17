from __future__ import annotations

import asyncio

import pytest

from db_monitor import audit_log
from db_monitor.metrics import (
    audit_log_flush_failures_total,
    audit_log_records_dropped_total,
)


class _FakeBegin:
    async def __aenter__(self) -> "_FakeBegin":
        return self

    async def __aexit__(
        self,
        exc_type,
        exc,
        tb,
    ) -> bool:
        del exc_type, exc, tb
        return False


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(
        self,
        exc_type,
        exc,
        tb,
    ) -> bool:
        del exc_type, exc, tb
        return False

    def begin(self) -> _FakeBegin:
        return _FakeBegin()

    def add_all(self, items) -> None:
        self.added = list(items)


@pytest.mark.anyio
async def test_audit_log_flush_writes_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Buffered audit logs should be written as structured rows."""
    session = _FakeSession()
    monkeypatch.setattr(audit_log, "AsyncSessionLocal", lambda: session)

    writer = audit_log.AuditLogWriter()
    entry = audit_log.AuditLogEntry(
        api_key_id=7,
        endpoint="/info",
        method="GET",
        status_code=200,
        ip_address="127.0.0.1",
    )

    await writer._flush([entry])

    assert len(session.added) == 1
    flushed = session.added[0]
    assert flushed.api_key_id == 7
    assert flushed.endpoint == "/info"
    assert flushed.method == "GET"
    assert flushed.status_code == 200
    assert flushed.ip_address == "127.0.0.1"


@pytest.mark.anyio
async def test_audit_log_queue_overflow_is_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full audit queues should increment the drop metric and log loudly."""
    writer = audit_log.AuditLogWriter()
    writer._queue = asyncio.Queue(maxsize=1)
    before = audit_log_records_dropped_total._value.get()

    await writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=None,
            endpoint="/health",
            method="GET",
            status_code=200,
            ip_address=None,
        )
    )
    await writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=None,
            endpoint="/metrics",
            method="GET",
            status_code=200,
            ip_address=None,
        )
    )

    assert writer._queue.qsize() == 1
    assert audit_log_records_dropped_total._value.get() == before + 1


@pytest.mark.anyio
async def test_audit_log_queue_block_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Block mode should await space and eventually accept the entry."""
    monkeypatch.setattr(
        audit_log.settings.observability, "audit_log_queue_mode", "block"
    )
    writer = audit_log.AuditLogWriter()
    writer._queue = asyncio.Queue(maxsize=1)

    # Fill the queue
    await writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=None, endpoint="/a", method="GET",
            status_code=200, ip_address=None,
        )
    )
    assert writer._queue.qsize() == 1

    # The second enqueue should block -- consume from the queue in a
    # background task to unblock it.
    async def consumer() -> None:
        await asyncio.sleep(0.05)
        await writer._queue.get()
        writer._queue.task_done()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer())
        await writer.enqueue(
            audit_log.AuditLogEntry(
                api_key_id=None, endpoint="/b", method="GET",
                status_code=200, ip_address=None,
            )
        )

    assert writer._queue.qsize() == 1


@pytest.mark.anyio
async def test_audit_log_queue_spill_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spill mode should persist to api_audit_log_spill when queue is full."""
    monkeypatch.setattr(
        audit_log.settings.observability, "audit_log_queue_mode", "spill"
    )

    executed: list[tuple[str, dict]] = []

    class FakeConn:
        async def __aenter__(self) -> "FakeConn":
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def execute(self, sql, params) -> None:
            executed.append((str(sql), params))

    class FakeEngine:
        def begin(self) -> FakeConn:
            return FakeConn()

    monkeypatch.setattr(audit_log, "engine", FakeEngine())

    writer = audit_log.AuditLogWriter()
    writer._queue = asyncio.Queue(maxsize=1)

    # Fill the queue
    await writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=None, endpoint="/a", method="GET",
            status_code=200, ip_address=None,
        )
    )

    # Second enqueue should spill
    await writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=1, endpoint="/spill", method="POST",
            status_code=201, ip_address="10.0.0.1",
        )
    )

    assert writer._queue.qsize() == 1
    assert len(executed) == 1
    assert "api_audit_log_spill" in executed[0][0]
    assert executed[0][1].get("error_message") == "queue_full_spill"
    assert executed[0][1].get("entry") is not None


@pytest.mark.anyio
async def test_audit_log_queue_spill_mode_fails_gracefully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spill mode should raise when the spill DB insert itself fails."""
    monkeypatch.setattr(
        audit_log.settings.observability, "audit_log_queue_mode", "spill"
    )

    class BrokenConn:
        async def __aenter__(self) -> "BrokenConn":
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def execute(self, sql, params) -> None:
            raise RuntimeError("db unavailable")

    class BrokenEngine:
        def begin(self) -> BrokenConn:
            return BrokenConn()

    monkeypatch.setattr(audit_log, "engine", BrokenEngine())

    writer = audit_log.AuditLogWriter()
    writer._queue = asyncio.Queue(maxsize=1)

    await writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=None, endpoint="/a", method="GET",
            status_code=200, ip_address=None,
        )
    )

    with pytest.raises(RuntimeError, match="db unavailable"):
        await writer.enqueue(
            audit_log.AuditLogEntry(
                api_key_id=None, endpoint="/fail", method="GET",
                status_code=500, ip_address=None,
            )
        )


@pytest.mark.anyio
async def test_audit_log_flush_retries_then_fails_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated database failures should be surfaced after the retry budget."""
    attempts = 0
    sleeps: list[float] = []
    before = audit_log_flush_failures_total._value.get()

    class FailingBegin(_FakeBegin):
        async def __aexit__(self, exc_type, exc, tb) -> bool:
            del exc_type, exc, tb
            return False

    class FlakySession(_FakeSession):
        def begin(self) -> FailingBegin:
            return FailingBegin()

        def add_all(self, items) -> None:
            nonlocal attempts
            del items
            attempts += 1
            raise RuntimeError("db unavailable")

    monkeypatch.setattr(audit_log, "AsyncSessionLocal", lambda: FlakySession())

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(audit_log.asyncio, "sleep", fake_sleep)

    writer = audit_log.AuditLogWriter()
    entry = audit_log.AuditLogEntry(
        api_key_id=None,
        endpoint="/events",
        method="POST",
        status_code=503,
        ip_address=None,
    )

    with pytest.raises(RuntimeError, match="db unavailable"):
        await writer._flush_with_retry([entry])

    assert attempts == audit_log.AUDIT_LOG_FLUSH_RETRIES
    assert sleeps == [0.1, 0.2]
    assert writer.has_flush_failure is True
    assert writer.last_flush_error == "db unavailable"
    assert audit_log_flush_failures_total._value.get() == before + attempts
