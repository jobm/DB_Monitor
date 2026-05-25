from __future__ import annotations

from typing import Any

from sqlalchemy import select

from core.db import AsyncSessionLocal
from core.models import ApiAuditLogSpill


async def list_audit_log_spill_snapshot(limit: int = 100) -> list[dict[str, Any]]:
    """Return a snapshot of recent audit log spill entries."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ApiAuditLogSpill).order_by(ApiAuditLogSpill.spilled_at.desc()).limit(limit)
        )
        rows = result.scalars().all()
    return [
        {
            "id": r.id,
            "entry": r.entry,
            "error_message": r.error_message,
            "spilled_at": r.spilled_at.isoformat() if r.spilled_at is not None else None,
        }
        for r in rows
    ]
