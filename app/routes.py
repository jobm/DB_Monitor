from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request, Security, Depends
from sqlalchemy import func, select
from extensions import AsyncSessionLocal
from lifecycle_manager import lifecycle_manager
from models import KafkaEvent, MonitoredColumn, MonitoredTable, ApiKey
from schema_discovery import SchemaDiscovery
from change_processor import ChangeProcessor
from auth import require_viewer_role, require_admin_role, generate_new_api_key, get_api_key_hash

router = APIRouter()

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)


@router.get("/tables", dependencies=[Depends(require_viewer_role)])
async def get_tables():
    """List all monitored tables."""
    tables = await schema_discovery.get_all_tables()
    return {"tables": tables, "count": len(tables)}


@router.get("/tables/{service_name}/{table_name}", dependencies=[Depends(require_viewer_role)])
async def get_table(service_name: str, table_name: str):
    """Get table details including columns."""
    table = await schema_discovery.get_table_by_name(service_name, table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table {service_name}.{table_name} not found")
    return table


@router.get("/tables/{service_name}/{table_name}/columns", dependencies=[Depends(require_viewer_role)])
async def get_table_columns(service_name: str, table_name: str):
    """Get columns for a specific table."""
    table = await schema_discovery.get_table_by_name(service_name, table_name)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table {service_name}.{table_name} not found")
    return {"columns": table.get("columns", [])}


@router.get("/events", dependencies=[Depends(require_viewer_role)])
async def get_events(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    service_name: Optional[str] = None,
    event_type: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    search_term: Optional[str] = None,
):
    """Get events with pagination and optional filtering."""
    async with AsyncSessionLocal() as session:
        query = select(KafkaEvent).order_by(KafkaEvent.id.desc())
        
        if service_name:
            query = query.where(KafkaEvent.service_name == service_name)
        if event_type:
            query = query.where(KafkaEvent.event_type == event_type)
            
        if start_time:
            from datetime import datetime
            try:
                st = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                query = query.where(KafkaEvent.event_time >= st)
            except ValueError:
                pass
        if end_time:
            from datetime import datetime
            try:
                et = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                query = query.where(KafkaEvent.event_time <= et)
            except ValueError:
                pass
                
        if search_term:
            query = query.where(KafkaEvent.raw_payload.ilike(f"%{search_term}%"))
        
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await session.execute(count_query)
        total = total_result.scalar()
        
        query = query.limit(limit).offset(offset)
        result = await session.execute(query)
        events = result.scalars().all()
        
        return {
            "events": [
                {
                    "id": event.id,
                    "event_type": event.event_type,
                    "event_time": event.event_time.isoformat() if event.event_time else None,
                    "user_id": event.user_id,
                    "service_name": event.service_name,
                    "operation": event.operation,
                    "source_table_id": event.source_table_id,
                    "event_data": event.event_data,
                }
                for event in events
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }


@router.get("/events/stats", dependencies=[Depends(require_viewer_role)])
async def get_event_stats():
    """Get aggregated event statistics."""
    async with AsyncSessionLocal() as session:
        by_service = await session.execute(
            select(KafkaEvent.service_name, func.count(KafkaEvent.id))
            .group_by(KafkaEvent.service_name)
        )
        by_type = await session.execute(
            select(KafkaEvent.event_type, func.count(KafkaEvent.id))
            .group_by(KafkaEvent.event_type)
        )
        by_operation = await session.execute(
            select(KafkaEvent.operation, func.count(KafkaEvent.id))
            .group_by(KafkaEvent.operation)
        )
        total = await session.execute(select(func.count(KafkaEvent.id)))
        
        return {
            "total_events": total.scalar(),
            "by_service": {row[0] or "unknown": row[1] for row in by_service.fetchall()},
            "by_type": {row[0] or "unknown": row[1] for row in by_type.fetchall()},
            "by_operation": {row[0] or "unknown": row[1] for row in by_operation.fetchall()},
        }


@router.get("/changes", dependencies=[Depends(require_viewer_role)])
async def get_changes(
    table_name: str = Query(...),
    column_name: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Get column changes for a table with optional filters."""
    table = await schema_discovery.get_table_by_name(table_name.rsplit(".", 1)[0], table_name.rsplit(".", 1)[-1])
    if not table:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
    
    column_id = None
    if column_name:
        for col in table.get("columns", []):
            if col["column_name"] == column_name:
                column_id = col["id"]
                break
        if not column_id:
            raise HTTPException(status_code=404, detail=f"Column {column_name} not found")
    
    changes = await change_processor.get_changes(
        table_id=table["id"],
        column_id=column_id,
        from_time=from_time,
        to_time=to_time,
        limit=limit,
    )
    return {"changes": changes, "count": len(changes)}


@router.get("/changes/{table_name}/{column_name}/at", dependencies=[Depends(require_viewer_role)])
async def get_value_at_time(
    table_name: str,
    column_name: str,
    timestamp: str = Query(...),
):
    """Get the value of a column at a specific point in time."""
    from datetime import datetime
    
    try:
        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")
    
    value = await change_processor.get_value_at_time(table_name, column_name, ts)
    return {
        "table_name": table_name,
        "column_name": column_name,
        "timestamp": timestamp,
        "value": value,
    }


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy" if lifecycle_manager.is_healthy else "unhealthy",
        "shutting_down": lifecycle_manager.is_shutting_down
    }


@router.get("/info", dependencies=[Depends(require_viewer_role)])
async def app_info(request: Request):
    """Application information endpoint."""
    return {
        "title": request.app.title,
        "description": request.app.description,
        "healthy": lifecycle_manager.is_healthy,
        "shutting_down": lifecycle_manager.is_shutting_down
    }


@router.post("/auth/keys")
async def create_api_key(
    owner_name: str,
    role: str = Query("viewer", regex="^(admin|viewer)$"),
    creator_api_key: ApiKey = Depends(require_admin_role)
):
    """Create a new API Key (requires an existing Admin API key)."""
    raw_key = generate_new_api_key()
    key_hash = get_api_key_hash(raw_key)
    
    async with AsyncSessionLocal() as session:
        async with session.begin():
            new_key = ApiKey(key_hash=key_hash, owner_name=owner_name, role=role, is_active=True)
            session.add(new_key)
            await session.flush()
            # The client needs {id}.{raw_key} to authenticate
            return {
                "api_key": f"{new_key.id}.{raw_key}",
                "owner_name": owner_name,
                "role": role,
                "message": "Store this key securely. It cannot be retrieved again."
            }

@router.post("/auth/bootstrap")
async def bootstrap_admin_key(owner_name: str):
    """Bootstrap endpoint to create the FIRST admin API key. Fails if any keys already exist."""
    async with AsyncSessionLocal() as session:
        # Check if ANY keys exist
        active_api_keys = await session.execute(select(ApiKey).filter(ApiKey.is_active == True))
        if active_api_keys.scalars().first() is not None:
            raise HTTPException(
                status_code=400,
                detail="Cannot bootstrap. System already has active API keys.",
            )

        raw_key = generate_new_api_key()
        key_hash = get_api_key_hash(raw_key)

        new_key = ApiKey(key_hash=key_hash, owner_name=owner_name, role="admin", is_active=True)
        session.add(new_key)
        await session.flush()
        await session.commit()
        
        return {
            "api_key": f"{new_key.id}.{raw_key}",
            "owner_name": owner_name,
            "role": "admin",
            "message": "Store this ADMIN key securely. It cannot be retrieved again."
        }
