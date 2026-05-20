"""Asynchronous buffering for API audit log persistence."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from config import (
    AUDIT_LOG_BATCH_SIZE,
    AUDIT_LOG_FLUSH_INTERVAL_SECONDS,
    AUDIT_LOG_QUEUE_MAXSIZE,
)
from extensions import AsyncSessionLocal
from metrics import (
    audit_log_flush_duration_seconds,
    audit_log_queue_size,
    audit_log_records_dropped_total,
)
from models import ApiAuditLog

logger = logging.getLogger(__name__)


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

    def enqueue(self, entry: AuditLogEntry) -> None:
        """Queue an audit log entry without blocking the request path."""
        try:
            self._queue.put_nowait(entry)
            audit_log_queue_size.set(self._queue.qsize())
        except asyncio.QueueFull:
            audit_log_records_dropped_total.inc()
            logger.warning("Audit log queue full; dropping request audit entry")

    async def run(self) -> None:
        """Flush queued audit logs to the database in small batches."""
        pending: list[AuditLogEntry] = []
        try:
            while True:
                try:
                    entry = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=AUDIT_LOG_FLUSH_INTERVAL_SECONDS,
                    )
                    pending.append(entry)
                    audit_log_queue_size.set(self._queue.qsize())
                    while len(pending) < AUDIT_LOG_BATCH_SIZE:
                        pending.append(self._queue.get_nowait())
                except asyncio.TimeoutError:
                    pass
                except asyncio.QueueEmpty:
                    pass

                if pending:
                    await self._flush(pending)
                    pending = []
        except asyncio.CancelledError:
            while True:
                try:
                    pending.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if pending:
                await self._flush(pending)
            raise

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