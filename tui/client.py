import asyncio
import json
from typing import Any, Dict, List, Optional

import httpx
import websockets


class DBMonitorClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.api_key: Optional[str] = None
        self._access_token: Optional[str] = None
        self._auth_context: Dict[str, Any] = {}
        self._ws_session_token: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._ws_running = False

    def set_credentials(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._access_token = None
        self._auth_context = {}
        self._ws_session_token = None
        if self._client:
            self._client.headers.update(self._get_headers())

    def _get_api_key_headers(self) -> Dict[str, str]:
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def _get_headers(self) -> Dict[str, str]:
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        elif self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _exchange_access_token(self) -> None:
        if not self.api_key or self._access_token:
            return

        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._get_api_key_headers(),
            timeout=10.0,
        ) as client:
            response = await client.post("/auth/token")
            response.raise_for_status()
            payload = response.json()
            self._access_token = payload["access_token"]
            self._auth_context = {
                "owner_name": payload.get("owner_name"),
                "role": payload.get("role"),
                "expires_at": payload.get("expires_at"),
            }

    async def _exchange_ws_session_token(self) -> None:
        if not self.api_key or self._ws_session_token:
            return

        await self._exchange_access_token()
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._get_headers(),
            timeout=10.0,
        ) as client:
            response = await client.post("/auth/ws-token")
            response.raise_for_status()
            self._ws_session_token = response.json()["session_token"]

    async def get_client(self) -> httpx.AsyncClient:
        await self._exchange_access_token()
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self._get_headers(),
                timeout=10.0,
            )
        return self._client

    async def close(self):
        await self._disconnect_ws()
        if self._client:
            await self._client.aclose()
            self._client = None
        self._access_token = None
        self._auth_context = {}
        self._ws_session_token = None

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
        self._clear_event_queue()

    async def _ws_listener(self):
        try:
            async for msg in self._ws:
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
        self._clear_event_queue()
        await self._exchange_ws_session_token()
        ws_url = self.base_url.replace(
            "http://",
            "ws://",
        ).replace("https://", "wss://")
        self._ws = await websockets.connect(
            f"{ws_url}/ws/events?session_token={self._ws_session_token}",
        )
        self._ws_running = True
        self._ws_task = asyncio.create_task(self._ws_listener())

    async def next_event(
        self,
        timeout: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        """Wait for the next real-time event. Returns None on timeout."""
        try:
            return await asyncio.wait_for(
                self._event_queue.get(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            return None

    def _clear_event_queue(self) -> None:
        while not self._event_queue.empty():
            try:
                self._event_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

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

    def get_auth_context(self) -> Dict[str, Any]:
        return dict(self._auth_context)

    def is_admin(self) -> bool:
        return self._auth_context.get("role") == "admin"

    async def get_info(self) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.get("/info")
        resp.raise_for_status()
        return resp.json()

    async def get_readiness(self) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.get("/readyz")
        if resp.status_code not in (200, 503):
            resp.raise_for_status()
        return resp.json()

    async def get_table(
        self,
        service_name: str,
        table_name: str,
    ) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.get(f"/tables/{service_name}/{table_name}")
        resp.raise_for_status()
        return resp.json()

    async def get_events(
        self,
        limit: int = 50,
        offset: int = 0,
        service_name: Optional[str] = None,
        source_table_id: Optional[int] = None,
        row_identity: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        client = await self.get_client()
        params = {"limit": limit, "offset": offset}
        if service_name:
            params["service_name"] = service_name
        if source_table_id is not None:
            params["source_table_id"] = source_table_id
        if row_identity is not None:
            params["row_identity"] = json.dumps(
                row_identity,
                sort_keys=True,
            )
        resp = await client.get("/events", params=params)
        resp.raise_for_status()
        return resp.json().get("events", [])

    async def get_changes(
        self,
        table_name: str,
        service_name: Optional[str] = None,
        column_name: Optional[str] = None,
        row_identity: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        client = await self.get_client()
        params: Dict[str, Any] = {
            "table_name": table_name,
            "limit": limit,
            "offset": offset,
        }
        if service_name:
            params["service_name"] = service_name
        if column_name:
            params["column_name"] = column_name
        if row_identity is not None:
            params["row_identity"] = json.dumps(row_identity, sort_keys=True)

        resp = await client.get("/changes", params=params)
        resp.raise_for_status()
        return resp.json().get("changes", [])

    async def get_consumer_checkpoints(self) -> List[Dict[str, Any]]:
        client = await self.get_client()
        resp = await client.get("/admin/checkpoints")
        resp.raise_for_status()
        return resp.json().get("checkpoints", [])

    async def get_dead_letter_events(
        self,
        limit: int = 100,
        include_replayed: bool = False,
    ) -> List[Dict[str, Any]]:
        client = await self.get_client()
        resp = await client.get(
            "/admin/dlq",
            params={
                "limit": limit,
                "include_replayed": include_replayed,
            },
        )
        resp.raise_for_status()
        return resp.json().get("events", [])

    async def list_api_keys(
        self,
        include_inactive: bool = True,
    ) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.get(
            "/auth/keys",
            params={"include_inactive": include_inactive},
        )
        resp.raise_for_status()
        return resp.json()

    async def create_api_key(
        self,
        owner_name: str,
        role: str = "viewer",
        ttl_days: int = 90,
    ) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.post(
            "/auth/keys",
            params={
                "owner_name": owner_name,
                "role": role,
                "ttl_days": ttl_days,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def rotate_api_key(
        self,
        key_id: int,
        ttl_days: int = 90,
    ) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.post(
            f"/auth/keys/{key_id}/rotate",
            params={"ttl_days": ttl_days},
        )
        resp.raise_for_status()
        return resp.json()

    async def revoke_api_key(self, key_id: int) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.post(f"/auth/keys/{key_id}/revoke")
        resp.raise_for_status()
        return resp.json()

    async def replay_dead_letter_event(
        self,
        dlq_event_id: int,
    ) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.post(f"/admin/dlq/{dlq_event_id}/replay")
        resp.raise_for_status()
        return resp.json()

    async def replay_dead_letter_events(
        self,
        limit: int = 100,
        include_replayed: bool = False,
    ) -> Dict[str, Any]:
        client = await self.get_client()
        resp = await client.post(
            "/admin/dlq/replay",
            params={
                "limit": limit,
                "include_replayed": include_replayed,
            },
        )
        resp.raise_for_status()
        return resp.json()


api_client = DBMonitorClient()
