"""Postgres LISTEN/NOTIFY backplane implementation.

The Postgres backplane uses ``pg_notify`` for cross-replica communication.
It is the default backend and works without additional infrastructure.
However, it has known limitations at high scale:

- Notifications are not persisted (lost on connection drop)
- All replicas connect to the same Postgres for notifications
- Payload size is practically limited to ~8KB
- Every notification is broadcast to all connected listeners

For deployments exceeding ~2000 ev/s, consider the Redis backplane.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import asyncpg

from db_monitor.core.config import settings
from db_monitor.extensions import engine
from sqlalchemy import text

logger = logging.getLogger(__name__)

BACKPLANE_DSN = settings.db.postgres_url.replace(
    "postgresql+asyncpg://",
    "postgresql://",
    1,
)


class PostgresBackplane:
    """Postgres LISTEN/NOTIFY cross-replica backplane."""

    def __init__(self, channel: str) -> None:
        self._channel = channel
        self._notification_callback: Callable[[str], Awaitable[None]] | None = None
        self._listener_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def start(
        self,
        notification_callback: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        """Start the backplane listener."""
        self._notification_callback = notification_callback
        if self._listener_task and not self._listener_task.done():
            return
        self._stop_event = asyncio.Event()
        self._listener_task = asyncio.create_task(
            self._run_listener(),
            name=f"pg_backplane_{self._channel}",
        )

    async def stop(self) -> None:
        """Stop the backplane listener."""
        if self._listener_task is None:
            return
        self._stop_event.set()
        self._listener_task.cancel()
        try:
            await self._listener_task
        except asyncio.CancelledError:
            pass
        self._listener_task = None
        self._connected = False

    async def publish(self, payload: str, channel: str | None = None) -> None:
        """Publish a payload via pg_notify."""
        ch = channel or self._channel
        async with engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": ch, "payload": payload},
            )

    async def _run_listener(self) -> None:
        while not self._stop_event.is_set():
            connection: asyncpg.Connection | None = None
            try:
                connection = await asyncpg.connect(BACKPLANE_DSN)
                self._connected = True
                await connection.add_listener(
                    self._channel,
                    self._handle_notification,
                )
                logger.info(
                    "Postgres backplane listener subscribed to %s",
                    self._channel,
                )
                await self._stop_event.wait()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Postgres backplane listener error: %s",
                    exc,
                )
                await asyncio.sleep(5.0)
            finally:
                self._connected = False
                if connection is not None:
                    try:
                        await connection.remove_listener(
                            self._channel,
                            self._handle_notification,
                        )
                    except Exception:
                        pass
                    await connection.close()

    def _handle_notification(
        self,
        _connection: asyncpg.Connection,
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        """Handle an incoming backplane notification."""
        if self._notification_callback is not None:
            asyncio.ensure_future(self._notification_callback(payload))
