"""Minimal DB Monitor API example client.

Usage:
    export DB_MONITOR_API_KEY="<id.secret>"
    python examples/python_api_client.py

Optional environment variables:
    DB_MONITOR_BASE_URL=http://localhost:8000
    DB_MONITOR_TABLE=orderdb.orders
    DB_MONITOR_ROW_IDENTITY={"id": 42}
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request


BASE_URL = os.getenv("DB_MONITOR_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("DB_MONITOR_API_KEY")
TABLE_NAME = os.getenv("DB_MONITOR_TABLE", "orderdb.orders")
ROW_IDENTITY = os.getenv("DB_MONITOR_ROW_IDENTITY")


def _request(
    path: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        method=method,
        headers=headers or {},
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def _require_api_key() -> str:
    if not API_KEY:
        raise SystemExit(
            "Set DB_MONITOR_API_KEY to an id.secret credential before running "
            "this example."
        )
    return API_KEY


def _exchange_access_token(api_key: str) -> str:
    payload = _request(
        "/auth/token",
        method="POST",
        headers={"X-API-Key": api_key},
    )
    return payload["access_token"]


def main() -> int:
    api_key = _require_api_key()
    access_token = _exchange_access_token(api_key)
    auth_headers = {"Authorization": f"Bearer {access_token}"}

    tables = _request("/tables", headers=auth_headers)
    events = _request("/events?limit=5", headers=auth_headers)

    print("Tables:")
    print(json.dumps(tables, indent=2))
    print("\nRecent events:")
    print(json.dumps(events, indent=2))

    query = {"table_name": TABLE_NAME, "limit": 5}
    if ROW_IDENTITY:
        query["row_identity"] = ROW_IDENTITY

    encoded_query = urllib.parse.urlencode(query)
    changes = _request(f"/changes?{encoded_query}", headers=auth_headers)

    print("\nRecent changes:")
    print(json.dumps(changes, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())