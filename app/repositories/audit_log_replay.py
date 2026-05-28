from __future__ import annotations

from sqlalchemy import select

from core.db import AsyncSessionLocal
from core.models import ApiAuditLog, ApiAuditLogSpill


async def replay_spill_entries(
    limit: int = 100,
    *,
    ids: list[int] | None = None,
    actor: str | None = None,
) -> dict[str, int]:
    """Replay spilled audit log entries into `api_audit_logs`.

    If `ids` is provided, replay only those spill rows. Otherwise replay up to
    `limit` most recent unreplayed rows. Mark spill rows as replayed and
    record `replayed_by` and `replayed_at` instead of deleting them.

    Returns a dict with keys: attempted, replayed, failed.
    """
    attempted = 0
    replayed = 0
    failed = 0

    async with AsyncSessionLocal() as session:
        async with session.begin():
            if ids:
                result = await session.execute(
                    select(ApiAuditLogSpill).where(
                        ApiAuditLogSpill.id.in_(ids)
                    )
                )
            else:
                result = await session.execute(
                    select(ApiAuditLogSpill)
                    .where(ApiAuditLogSpill.replayed.is_(False))
                    .order_by(ApiAuditLogSpill.spilled_at.desc())
                    .limit(limit)
                )

            rows = result.scalars().all()
            from datetime import datetime, timezone

            for row in rows:
                attempted += 1
                entry = row.entry or {}
                try:
                    session.add(
                        ApiAuditLog(
                            api_key_id=entry.get("api_key_id"),
                            endpoint=entry.get("endpoint", ""),
                            method=entry.get("method", ""),
                            status_code=entry.get("status_code", 0),
                            ip_address=entry.get("ip_address"),
                        )
                    )
                    # Mark spill row as replayed with metadata
                    row.replayed = True
                    row.replayed_by = actor
                    row.replayed_at = datetime.now(timezone.utc)
                    session.add(row)
                    replayed += 1
                except Exception:
                    failed += 1

    return {"attempted": attempted, "replayed": replayed, "failed": failed}
