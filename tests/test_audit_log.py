from __future__ import annotations

import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest


_APP_CORE_PATH = Path(__file__).resolve().parents[1] / "app" / "core"
if "core" not in sys.modules:
    core_package = types.ModuleType("core")
    core_package.__path__ = [str(_APP_CORE_PATH)]
    sys.modules["core"] = core_package

audit_log = importlib.import_module("audit_log")
metrics = importlib.import_module("metrics")

audit_log_flush_failures_total = metrics.audit_log_flush_failures_total
audit_log_records_dropped_total = metrics.audit_log_records_dropped_total


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

    writer.enqueue(
        audit_log.AuditLogEntry(
            api_key_id=None,
            endpoint="/health",
            method="GET",
            status_code=200,
            ip_address=None,
        )
    )
    writer.enqueue(
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
