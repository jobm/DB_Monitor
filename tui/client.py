import asyncio
import httpx
import websockets
from typing import Optional, Dict, List, Any, AsyncIterator


class DBMonitorClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.api_key: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._ws_running = False

    def set_credentials(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        if self._client:
            self._client.headers.update(self._get_headers())

    def _get_headers(self) -> Dict[str, str]:
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, headers=self._get_headers(), timeout=10.0
            )
        return self._client

    async def close(self):
        await self._disconnect_ws()
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _disconnect_ws(self):
        self._ws_running = False
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None

    async def _ws_listener(self):
        try:
            async for msg in self._ws:
                import json

                data = json.loads(msg)
                if data.get("type") == "new_event":
                    await self._event_queue.put(data.get("event"))
        except websockets.ConnectionClosed:
            pass
        except Exception:
            pass

    async def connect_ws(self):
        """Connect to WebSocket for real-time events."""
        if self._ws_running and self._ws:
            return
        ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        self._ws = await websockets.connect(
            f"{ws_url}/ws/events?api_key={self.api_key}"
        )
        self._ws_running = True
        self._ws_task = asyncio.create_task(self._ws_listener())

    async def next_event(self, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """Wait for the next real-time event. Returns None on timeout."""
        try:
            return await asyncio.wait_for(self._event_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def check_health(self) -> bool:
        try:
            client = await self.get_client()
            resp = await client.get("/health")
            resp.raise_for_status()
            return True
        except Exception:
            return False

    async def verify_auth(self) -> bool:
        try:
            client = await self.get_client()
            resp = await client.get("/info")
            return resp.status_code == 200
        except Exception:
            return False

    async def get_tables(self) -> List[Dict[str, Any]]:
        client = await self.get_client()
        resp = await client.get("/tables")
        resp.raise_for_status()
        return resp.json().get("tables", [])

    async def get_events(
        self, limit: int = 50, service_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        client = await self.get_client()
        params = {"limit": limit}
        if service_name:
            params["service_name"] = service_name
        resp = await client.get("/events", params=params)
        resp.raise_for_status()
        return resp.json().get("events", [])


api_client = DBMonitorClient()
