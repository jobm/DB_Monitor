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
from pathlib import Path


SDK_ROOT = Path(__file__).resolve().parents[1] / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

BASE_URL = os.getenv("DB_MONITOR_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("DB_MONITOR_API_KEY")

# Default target table. Adjust to your custom database table when using in production.
# In the bundled evaluation sandbox environment, this defaults to 'orderdb.public.orders'.
TABLE_NAME = os.getenv("DB_MONITOR_TABLE", "orderdb.public.orders")
ROW_IDENTITY = os.getenv("DB_MONITOR_ROW_IDENTITY")


def _require_api_key() -> str:
    if not API_KEY:
        raise SystemExit(
            "Set DB_MONITOR_API_KEY to an id.secret credential before running "
            "this example."
        )
    return API_KEY


def _exchange_access_token(api_key: str) -> str:
    from db_monitor_sdk import DBMonitorClient

    client = DBMonitorClient(
        BASE_URL,
        api_key=api_key,
    )
    return client.exchange_access_token()


def main() -> int:
    from db_monitor_sdk import DBMonitorClient

    api_key = _require_api_key()
    access_token = _exchange_access_token(api_key)
    client = DBMonitorClient(
        BASE_URL,
        api_key=api_key,
        access_token=access_token,
    )

    tables = client.get_tables()
    events = client.get_events(limit=5)

    print("Tables:")
    print(json.dumps(tables, indent=2))
    print("\nRecent events:")
    print(json.dumps(events, indent=2))

    parsed_row_identity = json.loads(ROW_IDENTITY) if ROW_IDENTITY else None
    changes = client.get_changes(
        table_name=TABLE_NAME,
        row_identity=parsed_row_identity,
        limit=5,
    )

    print("\nRecent changes:")
    print(json.dumps(changes, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
