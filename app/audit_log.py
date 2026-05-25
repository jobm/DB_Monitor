"""Asynchronous buffering for API audit log persistence."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from core.config import (
    AUDIT_LOG_BATCH_SIZE,
    AUDIT_LOG_FLUSH_INTERVAL_SECONDS,
    AUDIT_LOG_QUEUE_MAXSIZE,
)
from core.db import AsyncSessionLocal
from core.models import ApiAuditLog
from core.db import engine
from sqlalchemy import text
import json
from metrics import (
    audit_log_flush_duration_seconds,
    audit_log_flush_failures_total,
    audit_log_queue_size,
    audit_log_records_dropped_total,
)

logger = logging.getLogger(__name__)

AUDIT_LOG_FLUSH_RETRIES = 3
AUDIT_LOG_RETRY_DELAY_SECONDS = 0.1


@dataclass(slots=True)
class AuditLogEntry:
    """Single buffered audit log write."""

    api_key_id: int | None
    endpoint: str
    method: str
    status_code: int
    ip_address: str | None


class AuditLogWriter:
    """Buffer API audit log writes off the request path."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[AuditLogEntry] = asyncio.Queue(
            maxsize=AUDIT_LOG_QUEUE_MAXSIZE
        )
        self._flush_failure_detected = False
        self._last_flush_error: str | None = None

    def enqueue(self, entry: AuditLogEntry) -> None:
        """Queue an audit log entry without blocking the request path."""
        try:
            self._queue.put_nowait(entry)
            audit_log_queue_size.set(self._queue.qsize())
        except asyncio.QueueFull:
            audit_log_records_dropped_total.inc()
            logger.warning(
                "Audit log queue full; dropping request audit entry",
                extra={
                    "queue_size": self._queue.qsize(),
                    "queue_maxsize": self._queue.maxsize,
                },
            )

    @property
    def has_flush_failure(self) -> bool:
        """Return whether a recent flush attempt failed."""
        return self._flush_failure_detected

    @property
    def last_flush_error(self) -> str | None:
        """Return the most recent audit-log flush error, if any."""
        return self._last_flush_error

    async def run(self) -> None:
        """Flush queued audit logs to the database in small batches."""
        pending: list[AuditLogEntry] = []
        try:
            while True:
                if not pending:
                    try:
                        entry = await asyncio.wait_for(
                            self._queue.get(),
                            timeout=AUDIT_LOG_FLUSH_INTERVAL_SECONDS,
                        )
                        pending.append(entry)
                        audit_log_queue_size.set(self._queue.qsize())
                    except asyncio.TimeoutError:
                        continue

                while len(pending) < AUDIT_LOG_BATCH_SIZE:
                    try:
                        pending.append(self._queue.get_nowait())
                        audit_log_queue_size.set(self._queue.qsize())
                    except asyncio.QueueEmpty:
                        break

                if pending:
                    try:
                        await self._flush_with_retry(pending)
                    except Exception:
                        logger.error(
                            (
                                "Audit log flush failed; retaining batch "
                                "for retry"
                            ),
                            extra={"batch_size": len(pending)},
                        )
                        await asyncio.sleep(AUDIT_LOG_RETRY_DELAY_SECONDS)
                    else:
                        pending = []
        except asyncio.CancelledError:
            while True:
                try:
                    pending.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if pending:
                try:
                    await self._flush_with_retry(pending)
                except Exception:
                    logger.exception(
                        "Audit log flush failed during shutdown",
                        extra={"batch_size": len(pending)},
                    )
            raise

    async def _flush_with_retry(self, entries: list[AuditLogEntry]) -> None:
        """Retry a batch flush before giving up to the caller."""
        delay = AUDIT_LOG_RETRY_DELAY_SECONDS
        last_error: Exception | None = None

        for attempt in range(1, AUDIT_LOG_FLUSH_RETRIES + 1):
            try:
                await self._flush(entries)
                self._flush_failure_detected = False
                self._last_flush_error = None
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self._flush_failure_detected = True
                self._last_flush_error = str(exc)
                audit_log_flush_failures_total.inc()
                logger.error(
                    "Audit log flush attempt failed",
                    extra={
                        "attempt": attempt,
                        "max_attempts": AUDIT_LOG_FLUSH_RETRIES,
                        "batch_size": len(entries),
                    },
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                if attempt < AUDIT_LOG_FLUSH_RETRIES:
                    await asyncio.sleep(delay)
                    delay *= 2

        if last_error is not None:
            # Final failure: try to persist spilled entries to a durable table
            # via the engine directly (bypass AsyncSessionLocal which may be
            # flaky during DB outages). This avoids re-using the same session
            # path that already failed the flush attempts.
            try:
                async with engine.begin() as conn:
                    for e in entries:
                        await conn.execute(
                            text(
                                "INSERT INTO api_audit_log_spill (entry, error_message) VALUES (:entry::jsonb, :error_message)"
                            ),
                            {
                                "entry": json.dumps(
                                    {
                                        "api_key_id": e.api_key_id,
                                        "endpoint": e.endpoint,
                                        "method": e.method,
                                        "status_code": e.status_code,
                                        "ip_address": e.ip_address,
                                    }
                                ),
                                "error_message": str(last_error),
                            },
                        )
                logger.info(
                    "Persisted spilled audit log entries to api_audit_log_spill",
                    extra={"count": len(entries)},
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Failed to persist audit log spill records",
                    extra={"spill_count": len(entries)},
                )
            # re-raise original error to allow callers to observe failure
            raise last_error

    async def _flush(self, entries: list[AuditLogEntry]) -> None:
        """Write a batch of audit log entries to the database."""
        with audit_log_flush_duration_seconds.time():
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    session.add_all(
                        [
                            ApiAuditLog(
                                api_key_id=entry.api_key_id,
                                endpoint=entry.endpoint,
                                method=entry.method,
                                status_code=entry.status_code,
                                ip_address=entry.ip_address,
                            )
                            for entry in entries
                        ]
                    )
        audit_log_queue_size.set(self._queue.qsize())


audit_log_writer = AuditLogWriter()
