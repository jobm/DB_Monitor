"""Minimal Python SDK for the DB Monitor HTTP API.

Provides both synchronous (:class:`DBMonitorClient`) and asynchronous
(:class:`AsyncDBMonitorClient`) clients.
"""

from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request
from typing import Any


class DBMonitorClient:
    """Synchronous client for common DB Monitor API workflows.

    Uses only the Python standard library — no third-party dependencies.

    Basic usage::

        client = DBMonitorClient("http://localhost:8000", api_key="<id.secret>")
        token = client.exchange_access_token()
        events = client.get_events(limit=10)

    Args:
        base_url: Root URL of the DB Monitor API (e.g. ``http://localhost:8000``).
        api_key: Optional ``id.secret`` credential for token exchange.
        access_token: Optional pre-exchanged bearer token.
        timeout_seconds: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        access_token: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.access_token = access_token
        self.timeout_seconds = timeout_seconds

    # ── Auth ──────────────────────────────────────────────────────────────

    def exchange_access_token(self) -> str:
        """Exchange the configured API key for a bearer access token.

        Returns:
            The new bearer token string (also stored as ``self.access_token``).

        Raises:
            ValueError: If no ``api_key`` was configured.
        """
        if not self.api_key:
            raise ValueError("An API key is required to exchange a token.")

        payload = self._request_json(
            "/auth/token",
            method="POST",
            headers={"X-API-Key": self.api_key},
        )
        self.access_token = str(payload["access_token"])
        return self.access_token

    # ── Read endpoints ────────────────────────────────────────────────────

    def get_info(self) -> dict[str, Any]:
        """Return basic API metadata (version, status, etc.).

        Returns:
            Info dict from ``/info``.
        """
        return self._request_json("/info")

    def get_tables(self) -> dict[str, Any]:
        """Return discovered tables and their column metadata.

        Returns:
            Tables dict from ``/tables``.
        """
        return self._request_json("/tables")

    def get_events(
        self,
        *,
        limit: int = 100,
        service_name: str | None = None,
        operation: str | None = None,
        cursor: dict[str, Any] | None = None,
        cursor_id: int | None = None,
        cursor_time: str | None = None,
    ) -> dict[str, Any]:
        """Return recent CDC events with optional filters.

        Args:
            limit: Maximum number of events to return.
            service_name: Filter by source service name.
            operation: Filter by operation type (``INSERT``, ``UPDATE``, etc.).
            cursor: Keyset cursor dict for pagination ``{"capture_time": ..., "event_id": ...}``.
            cursor_id: Cursor event ID (alternative to ``cursor`` dict).
            cursor_time: Cursor timestamp (ISO 8601 string, alternative to ``cursor`` dict).

        Returns:
            Events response from ``/events``.
        """
        query: dict[str, Any] = {"limit": limit}
        if service_name:
            query["service_name"] = service_name
        if operation:
            query["operation"] = operation
        if cursor is not None:
            query["cursor"] = json.dumps(cursor, separators=(",", ":"))
        if cursor_id is not None:
            query["cursor_id"] = cursor_id
        if cursor_time is not None:
            query["cursor_time"] = cursor_time
        return self._request_json("/events", query=query)

    def get_changes(
        self,
        *,
        table_name: str,
        service_name: str | None = None,
        row_identity: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
        cursor: dict[str, Any] | None = None,
        cursor_id: int | None = None,
        cursor_time: str | None = None,
    ) -> dict[str, Any]:
        """Return recent column-level changes for one table or record.

        Args:
            table_name: Table identifier (e.g. ``"orderdb.public.orders"``).
            service_name: Optional service name filter.
            row_identity: Optional row identity dict filter (e.g. ``{"id": 42}``).
            limit: Maximum number of changes to return.
            offset: Row offset for offset-based pagination.
            cursor: Keyset cursor dict for pagination.
            cursor_id: Cursor change ID (alternative to ``cursor`` dict).
            cursor_time: Cursor timestamp (ISO 8601 string).

        Returns:
            Changes response from ``/changes``.
        """
        query: dict[str, Any] = {
            "table_name": table_name,
            "limit": limit,
            "offset": offset,
        }
        if service_name:
            query["service_name"] = service_name
        if row_identity is not None:
            query["row_identity"] = json.dumps(row_identity)
        if cursor is not None:
            query["cursor"] = json.dumps(cursor, separators=(",", ":"))
        if cursor_id is not None:
            query["cursor_id"] = cursor_id
        if cursor_time is not None:
            query["cursor_time"] = cursor_time
        return self._request_json("/changes", query=query)

    # ── Admin endpoints ───────────────────────────────────────────────────

    def get_consumer_checkpoints(self) -> dict[str, Any]:
        """Return consumer checkpoint snapshots (admin).

        Returns:
            Checkpoints dict from ``/admin/checkpoints``.
        """
        return self._request_json("/admin/checkpoints")

    def get_dead_letter_events(
        self,
        *,
        limit: int = 100,
        include_replayed: bool = False,
    ) -> dict[str, Any]:
        """Return dead-letter queue entries (admin).

        Args:
            limit: Maximum number of entries to return.
            include_replayed: Whether to include already-replayed entries.

        Returns:
            DLQ entries dict from ``/admin/dlq``.
        """
        return self._request_json(
            "/admin/dlq",
            query={
                "limit": limit,
                "include_replayed": str(include_replayed).lower(),
            },
        )

    # ── Internal request helpers ──────────────────────────────────────────

    def _request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Send one synchronous API request and decode the JSON response."""
        request_headers = self._default_headers()
        if headers:
            request_headers.update(headers)

        request_path = path
        if query:
            encoded_query = urllib.parse.urlencode(query, doseq=True)
            request_path = f"{path}?{encoded_query}"

        request = urllib.request.Request(
            f"{self.base_url}{request_path}",
            method=method,
            headers=request_headers,
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload) if payload else {}

    def _default_headers(self) -> dict[str, str]:
        """Build the best available auth headers from current credentials."""
        if self.access_token:
            return {"Authorization": f"Bearer {self.access_token}"}
        if self.api_key:
            return {"X-API-Key": self.api_key}
        return {}


class AsyncDBMonitorClient:
    """Asynchronous client for DB Monitor API workflows.

    Wraps :class:`DBMonitorClient` via ``asyncio.to_thread`` so HTTP
    requests run in a thread pool without blocking the event loop.
    Requires no third-party dependencies beyond the Python standard
    library.

    Basic usage::

        client = AsyncDBMonitorClient("http://localhost:8000", api_key="<id.secret>")
        token = await client.exchange_access_token()
        events = await client.get_events(limit=10)

    Args:
        base_url: Root URL of the DB Monitor API.
        api_key: Optional ``id.secret`` credential for token exchange.
        access_token: Optional pre-exchanged bearer token.
        timeout_seconds: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        access_token: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._sync = DBMonitorClient(
            base_url,
            api_key=api_key,
            access_token=access_token,
            timeout_seconds=timeout_seconds,
        )

    @property
    def access_token(self) -> str | None:
        """The current bearer access token, if set."""
        return self._sync.access_token

    async def exchange_access_token(self) -> str:
        """Exchange the configured API key for a bearer access token.

        Returns:
            The new bearer token string.
        """
        return await asyncio.to_thread(self._sync.exchange_access_token)

    async def get_info(self) -> dict[str, Any]:
        """Return basic API metadata."""
        return await asyncio.to_thread(self._sync.get_info)

    async def get_tables(self) -> dict[str, Any]:
        """Return discovered tables and columns."""
        return await asyncio.to_thread(self._sync.get_tables)

    async def get_events(
        self,
        *,
        limit: int = 100,
        service_name: str | None = None,
        operation: str | None = None,
        cursor: dict[str, Any] | None = None,
        cursor_id: int | None = None,
        cursor_time: str | None = None,
    ) -> dict[str, Any]:
        """Return recent CDC events with optional filters.

        See :meth:`DBMonitorClient.get_events` for parameter documentation.
        """
        return await asyncio.to_thread(
            self._sync.get_events,
            limit=limit,
            service_name=service_name,
            operation=operation,
            cursor=cursor,
            cursor_id=cursor_id,
            cursor_time=cursor_time,
        )

    async def get_changes(
        self,
        *,
        table_name: str,
        service_name: str | None = None,
        row_identity: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
        cursor: dict[str, Any] | None = None,
        cursor_id: int | None = None,
        cursor_time: str | None = None,
    ) -> dict[str, Any]:
        """Return recent column-level changes.

        See :meth:`DBMonitorClient.get_changes` for parameter documentation.
        """
        return await asyncio.to_thread(
            self._sync.get_changes,
            table_name=table_name,
            service_name=service_name,
            row_identity=row_identity,
            limit=limit,
            offset=offset,
            cursor=cursor,
            cursor_id=cursor_id,
            cursor_time=cursor_time,
        )

    async def get_consumer_checkpoints(self) -> dict[str, Any]:
        """Return consumer checkpoint snapshots (admin)."""
        return await asyncio.to_thread(self._sync.get_consumer_checkpoints)

    async def get_dead_letter_events(
        self,
        *,
        limit: int = 100,
        include_replayed: bool = False,
    ) -> dict[str, Any]:
        """Return dead-letter queue entries (admin).

        See :meth:`DBMonitorClient.get_dead_letter_events` for parameter documentation.
        """
        return await asyncio.to_thread(
            self._sync.get_dead_letter_events,
            limit=limit,
            include_replayed=include_replayed,
        )
