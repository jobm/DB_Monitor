import asyncio
import json
import logging
from typing import Optional
from fastapi import WebSocket, WebSocketDisconnect, Query

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        self._connections: dict[int, WebSocket] = {}
        self._lock = asyncio.Lock()
        self._counter = 0

    async def connect(self, websocket: WebSocket, client_id: int) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections[client_id] = websocket
        logger.info(
            f"WebSocket client %d connected (%d total)",
            client_id,
            len(self._connections),
        )

    async def disconnect(self, client_id: int) -> None:
        async with self._lock:
            self._connections.pop(client_id, None)
        logger.info(
            f"WebSocket client %d disconnected (%d total)",
            client_id,
            len(self._connections),
        )

    async def broadcast(self, message: dict) -> None:
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

    @property
    def connection_count(self) -> int:
        return len(self._connections)


ws_manager = WebSocketManager()
