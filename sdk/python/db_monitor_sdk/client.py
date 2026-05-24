"""Minimal Python SDK for the DB Monitor HTTP API."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any


class DBMonitorClient:
    """Small synchronous client for common DB Monitor API workflows."""

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

    def exchange_access_token(self) -> str:
        """Exchange the configured API key for a bearer token."""
        if not self.api_key:
            raise ValueError("An API key is required to exchange a token.")

        payload = self._request_json(
            "/auth/token",
            method="POST",
            headers={"X-API-Key": self.api_key},
        )
        self.access_token = str(payload["access_token"])
        return self.access_token

    def get_info(self) -> dict[str, Any]:
        """Return basic API metadata."""
        return self._request_json("/info")

    def get_tables(self) -> dict[str, Any]:
        """Return discovered tables and columns."""
        return self._request_json("/tables")

    def get_events(
        self,
        *,
        limit: int = 100,
        service_name: str | None = None,
        operation: str | None = None,
        cursor_id: int | None = None,
    ) -> dict[str, Any]:
        """Return recent events with optional filters."""
        query = {"limit": limit}
        if service_name:
            query["service_name"] = service_name
        if operation:
            query["operation"] = operation
        if cursor_id is not None:
            query["cursor_id"] = cursor_id
        return self._request_json("/events", query=query)

    def get_changes(
        self,
        *,
        table_name: str,
        service_name: str | None = None,
        row_identity: dict[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Return recent column changes for one table or record."""
        query: dict[str, Any] = {
            "table_name": table_name,
            "limit": limit,
            "offset": offset,
        }
        if service_name:
            query["service_name"] = service_name
        if row_identity is not None:
            query["row_identity"] = json.dumps(row_identity)
        return self._request_json("/changes", query=query)

    def get_consumer_checkpoints(self) -> dict[str, Any]:
        """Return checkpoint snapshots for admin callers."""
        return self._request_json("/admin/checkpoints")

    def get_dead_letter_events(
        self,
        *,
        limit: int = 100,
        include_replayed: bool = False,
    ) -> dict[str, Any]:
        """Return DLQ entries for admin callers."""
        return self._request_json(
            "/admin/dlq",
            query={
                "limit": limit,
                "include_replayed": str(include_replayed).lower(),
            },
        )

    def _request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Send one API request and decode the JSON response body."""
        request_headers = self._default_headers()
        if headers:
            request_headers.update(headers)

        request_path = path
        if query:
            encoded_query = urllib.parse.urlencode(query)
            request_path = f"{path}?{encoded_query}"

        request = urllib.request.Request(
            f"{self.base_url}{request_path}",
            method=method,
            headers=request_headers,
        )
        with urllib.request.urlopen(
            request,
            timeout=self.timeout_seconds,
        ) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload) if payload else {}

    def _default_headers(self) -> dict[str, str]:
        """Return the best available authentication headers."""
        if self.access_token:
            return {"Authorization": f"Bearer {self.access_token}"}
        if self.api_key:
            return {"X-API-Key": self.api_key}
        return {}
