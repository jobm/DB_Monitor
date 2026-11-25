"""Shared helpers for COPY-based ingestion writes."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession


def jsonb(value: Any) -> str | None:
    """Serialize a JSONB column for asyncpg's COPY codec.

    Values come from parsed CDC payloads, so every non-null value is a Python
    JSON type (including plain strings, which must still be quoted).
    """
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"))


async def asyncpg_connection(session: AsyncSession) -> Any:
    """Return the underlying asyncpg connection for the session's transaction."""
    connection = await session.connection()
    sync_connection = connection.sync_connection
    if sync_connection is None:
        raise RuntimeError("COPY insert requires a connection-backed session")
    return sync_connection.connection.driver_connection
