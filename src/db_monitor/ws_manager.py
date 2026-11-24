"""WebSocket connection management and cross-replica broadcast.

Manages connected WebSocket clients, broadcasts events to all subscribers,
and provides a pluggable backplane (Postgres LISTEN/NOTIFY or Redis Pub/Sub)
for cross-replica event fan-out in horizontally scaled deployments.

Key exports:
    - WebSocketManager: Manages client connections, broadcasting, and backplane
    - ws_manager: Singleton instance for application-wide use

Related modules:
    - :mod:`db_monitor.backplane` — pluggable backplane implementations
    - :mod:`db_monitor.main` — wires ws_manager into the WebSocket route
    - :mod:`db_monitor.lifespan` — starts/stops the backplane listener
"""

from __future__ import annotations

import asyncio
import json
import logging
from uuid import uuid4

from db_monitor.backplane import create_backplane
from db_monitor.backplane.base import BackplaneProtocol
from db_monitor.core.config import settings
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.core.models import KafkaEvent
from fastapi import WebSocket
from sqlalchemy import select
from db_monitor.tracing import start_span

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self) -> None:
        self._connections: dict[int, tuple[WebSocket, str | None]] = {}
        self._lock = asyncio.Lock()
        self._instance_id = uuid4().hex
        self._backplane: BackplaneProtocol | None = None
        self._backplane_task: asyncio.Task | None = None
        self._backplane_enabled = settings.auth.ws_backplane_enabled

    async def connect(
        self,
        websocket: WebSocket,
        client_id: int,
        tenant_id: str | None = None,
    ) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections[client_id] = (websocket, tenant_id)
        logger.info(
            "WebSocket client %d connected for tenant %s (%d total)",
            client_id,
            tenant_id or "all",
            len(self._connections),
        )

    async def disconnect(self, client_id: int) -> None:
        async with self._lock:
            self._connections.pop(client_id, None)
        logger.info(
            "WebSocket client %d disconnected (%d total)",
            client_id,
            len(self._connections),
        )

    async def broadcast(
        self,
        message: dict,
        event_id: int | None = None,
        tenant_id: str | None = None,
    ) -> None:
        with start_span(
            "websocket.broadcast",
            attributes={
                "db_monitor.event_id": event_id,
                "db_monitor.tenant_id": tenant_id,
                "db_monitor.backplane_enabled": self._backplane_enabled,
                "db_monitor.connection_count": len(self._connections),
            },
        ):
            await self._broadcast_local(message, tenant_id=tenant_id)
            if self._backplane_enabled and event_id is not None:
                await self._publish_event(event_id)

    async def _broadcast_local(
        self,
        message: dict,
        *,
        tenant_id: str | None,
    ) -> None:
        payload = json.dumps(message)
        dead = []
        async with self._lock:
            connections = list(self._connections.items())
        for client_id, (ws, client_tenant_id) in connections:
            if (
                client_tenant_id is not None
                and client_tenant_id != tenant_id
            ):
                continue
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(client_id)
        for client_id in dead:
            await self.disconnect(client_id)

    async def _publish_event(self, event_id: int) -> None:
        """Publish a lightweight event notification via active backplane."""
        if self._backplane is None:
            return
        payload = json.dumps(
            {
                "origin_instance_id": self._instance_id,
                "event_id": event_id,
            }
        )
        await self._backplane.publish(payload)

    async def start_backplane_listener(self) -> asyncio.Task | None:
        """Start the backplane listener if clustering is enabled."""
        if not self._backplane_enabled:
            return None
        if self._backplane_task and not self._backplane_task.done():
            return self._backplane_task

        self._backplane = create_backplane()
        await self._backplane.start(
            notification_callback=self._deliver_backplane_notification,
        )

        # The backplane runs its own listener internally, but we return
        # a sentinel task so the lifespan manager can track it.
        self._backplane_task = asyncio.create_task(
            self._wait_for_backplane_stop(),
            name="ws_backplane_watcher",
        )
        return self._backplane_task

    async def stop_backplane_listener(self) -> None:
        """Signal the backplane listener to stop."""
        if self._backplane_task is None:
            return
        self._backplane_task.cancel()
        try:
            await self._backplane_task
        except asyncio.CancelledError:
            pass
        self._backplane_task = None
        if self._backplane is not None:
            await self._backplane.stop()
            self._backplane = None

    async def _wait_for_backplane_stop(self) -> None:
        """Sentinel task that keeps the lifespan manager aware of the backplane."""
        while self._backplane is not None and self._backplane.is_connected:
            await asyncio.sleep(1)

    async def _deliver_backplane_notification(self, payload: str) -> None:
        """Fetch the event referenced by a backplane payload and fan it out."""
        with start_span("websocket.backplane.deliver"):
            try:
                envelope = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning("Ignoring invalid websocket backplane payload")
                return

            if envelope.get("origin_instance_id") == self._instance_id:
                return

            event_id = envelope.get("event_id")
            if event_id is None:
                return

            async with AsyncSessionLocal() as session:  # type: ignore[type-var,attr-defined]
                result = await session.execute(
                    select(KafkaEvent).where(KafkaEvent.id == event_id)
                )
                event = result.scalar_one_or_none()

            if event is None:
                logger.warning(
                    "Could not load event %s from websocket backplane payload",
                    event_id,
                )
                return

            await self._broadcast_local(
                {
                    "type": "new_event",
                    "event": {
                        "id": event.id,
                        "event_type": event.event_type,
                        "event_time": (
                            event.event_time.isoformat()
                            if event.event_time
                            else None
                        ),
                        "user_id": event.user_id,
                        "service_name": event.service_name,
                        "operation": event.operation,
                        "source_table_id": event.source_table_id,
                        "row_identity": event.row_identity,
                        "event_data": event.event_data,
                    },
                },
                tenant_id=event.service_name,
            )

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    @property
    def backplane_backend(self) -> str | None:
        """Return the active backplane backend name, if any."""
        if self._backplane is None:
            return None
        return type(self._backplane).__name__.replace("Backplane", "").lower()


ws_manager = WebSocketManager()
