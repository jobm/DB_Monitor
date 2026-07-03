"""Dead-letter queue, checkpoint, and audit-log spill admin endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.params import Param

from db_monitor.auth import require_admin_role
from db_monitor.core.models import ApiKey
from db_monitor.ingestion.consumer import (
    list_consumer_checkpoints_snapshot,
    list_dead_letter_events as list_dead_letter_records,
    replay_dead_letter_event_record,
    replay_dead_letter_event_records,
)
from db_monitor.repositories.audit_log_spill import list_audit_log_spill_snapshot
from db_monitor.repositories.audit_log_replay import replay_spill_entries

router = APIRouter()


@router.get("/admin/checkpoints")
async def get_consumer_checkpoints(
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return persisted consumer checkpoint state for operators."""
    del admin_api_key
    checkpoints = await list_consumer_checkpoints_snapshot()
    return {
        "checkpoints": checkpoints,
        "count": len(checkpoints),
    }


@router.get("/admin/audit-log/spill")
async def get_audit_log_spill(
    limit: int = Query(100, ge=1, le=1000),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return recent spilled audit log entries for inspection."""
    del admin_api_key
    rows = await list_audit_log_spill_snapshot(limit=limit)
    return {"events": rows, "count": len(rows)}


@router.post("/admin/audit-log/spill/replay")
async def replay_audit_log_spill(
    limit: int = Query(100, ge=1, le=1000),
    ids: list[int] | None = Query(default=None),
    actor: str | None = Query(default=None),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay spilled audit log entries into the primary audit table."""
    del admin_api_key
    result = await replay_spill_entries(limit=limit, ids=ids, actor=actor)
    return {"result": result}


@router.get("/admin/dlq")
async def get_dead_letter_events(
    limit: int = Query(100, ge=1, le=1000),
    include_replayed: bool = False,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """List persisted DLQ records that are available for replay."""
    del admin_api_key
    dead_letters = await list_dead_letter_records(
        limit=limit,
        include_replayed=include_replayed,
    )
    return {
        "events": dead_letters,
        "count": len(dead_letters),
    }


@router.post("/admin/dlq/replay")
async def replay_dead_letter_events(
    limit: int = Query(100, ge=1, le=1000),
    include_replayed: bool = False,
    customer_id: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay multiple persisted DLQ records through ingestion."""
    del admin_api_key
    customer_scope = None if isinstance(customer_id, Param) else customer_id
    if customer_scope is None or not customer_scope.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header is required for mutating "
                "admin operations."
            ),
        )

    customer_scope = customer_scope.strip()
    result = await replay_dead_letter_event_records(
        limit=limit,
        include_replayed=include_replayed,
    )
    result["customer_id"] = customer_scope
    return result


@router.post("/admin/dlq/{dlq_event_id}/replay")
async def replay_dead_letter_event(
    dlq_event_id: int,
    customer_id: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay a persisted DLQ record back through ingestion."""
    del admin_api_key
    customer_scope = None if isinstance(customer_id, Param) else customer_id
    if customer_scope is None or not customer_scope.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header is required for mutating "
                "admin operations."
            ),
        )

    customer_scope = customer_scope.strip()
    try:
        result = await replay_dead_letter_event_record(dlq_event_id)
        result["customer_id"] = customer_scope
        return result
    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=(
                f"{exc}. Check GET /admin/dlq for currently available "
                "replay targets."
            ),
        ) from exc
