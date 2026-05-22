import asyncio
import json
import logging
from uuid import uuid4

import asyncpg
from config import POSTGRES_URL, WS_BACKPLANE_CHANNEL, WS_BACKPLANE_ENABLED
from extensions import AsyncSessionLocal, engine
from fastapi import WebSocket
from models import KafkaEvent
from sqlalchemy import select, text
from tracing import start_span

logger = logging.getLogger(__name__)

BACKPLANE_DSN = POSTGRES_URL.replace(
    "postgresql+asyncpg://",
    "postgresql://",
    1,
)


class WebSocketManager:
    def __init__(self):
        self._connections: dict[int, WebSocket] = {}
        self._lock = asyncio.Lock()
        self._instance_id = uuid4().hex
        self._backplane_task: asyncio.Task | None = None
        self._backplane_stop_event = asyncio.Event()

    async def connect(self, websocket: WebSocket, client_id: int) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections[client_id] = websocket
        logger.info(
            "WebSocket client %d connected (%d total)",
            client_id,
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
    ) -> None:
        with start_span(
            "websocket.broadcast",
            attributes={
                "db_monitor.event_id": event_id,
                "db_monitor.backplane_enabled": WS_BACKPLANE_ENABLED,
                "db_monitor.connection_count": len(self._connections),
            },
        ):
            await self._broadcast_local(message)
            if WS_BACKPLANE_ENABLED and event_id is not None:
                await self._publish_event(event_id)

    async def _broadcast_local(self, message: dict) -> None:
        payload = json.dumps(message)
        dead = []
        async with self._lock:
            connections = list(self._connections.items())
        for client_id, ws in connections:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(client_id)
        for client_id in dead:
            await self.disconnect(client_id)

    async def _publish_event(self, event_id: int) -> None:
        """Publish a lightweight websocket event notification to Postgres."""
        payload = json.dumps(
            {
                "origin_instance_id": self._instance_id,
                "event_id": event_id,
            }
        )
        async with engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {
                    "channel": WS_BACKPLANE_CHANNEL,
                    "payload": payload,
                },
            )

    async def start_backplane_listener(self) -> asyncio.Task | None:
        """Start the Postgres LISTEN/NOTIFY backplane listener if enabled."""
        if not WS_BACKPLANE_ENABLED:
            return None
        if self._backplane_task and not self._backplane_task.done():
            return self._backplane_task

        self._backplane_stop_event = asyncio.Event()
        self._backplane_task = asyncio.create_task(
            self._run_backplane_listener(),
            name="ws_backplane_listener",
        )
        return self._backplane_task

    async def stop_backplane_listener(self) -> None:
        """Signal the backplane listener to stop and await its completion."""
        if self._backplane_task is None:
            return
        self._backplane_stop_event.set()
        self._backplane_task.cancel()
        try:
            await self._backplane_task
        except asyncio.CancelledError:
            pass
        self._backplane_task = None

    async def _run_backplane_listener(self) -> None:
        """Listen for remote websocket notifications via Postgres."""
        while not self._backplane_stop_event.is_set():
            connection: asyncpg.Connection | None = None
            try:
                connection = await asyncpg.connect(BACKPLANE_DSN)
                await connection.add_listener(
                    WS_BACKPLANE_CHANNEL,
                    self._handle_backplane_notification,
                )
                logger.info(
                    "WebSocket backplane listener subscribed to %s",
                    WS_BACKPLANE_CHANNEL,
                )
                await self._backplane_stop_event.wait()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "WebSocket backplane listener error: %s",
                    exc,
                )
                await asyncio.sleep(5.0)
            finally:
                if connection is not None:
                    try:
                        await connection.remove_listener(
                            WS_BACKPLANE_CHANNEL,
                            self._handle_backplane_notification,
                        )
                    except Exception:
                        pass
                    await connection.close()

    def _handle_backplane_notification(
        self,
        _connection: asyncpg.Connection,
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        """Schedule handling for a websocket backplane notification."""
        asyncio.create_task(self._deliver_backplane_notification(payload))

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

            async with AsyncSessionLocal() as session:
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
                }
            )

    @property
    def connection_count(self) -> int:
        return len(self._connections)


ws_manager = WebSocketManager()
