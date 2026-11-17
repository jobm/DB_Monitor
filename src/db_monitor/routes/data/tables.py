"""Table metadata routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from db_monitor.auth import get_tenant_filter, require_viewer_role
from db_monitor.core.responses import TableColumnsResponse, TableListResponse
from db_monitor.routes.data.factories import get_schema_discovery

router = APIRouter()


@router.get(
    "/tables",
    dependencies=[Depends(require_viewer_role)],
    response_model=TableListResponse,
)
async def get_tables(
    tenant_filter: str | None = Depends(get_tenant_filter),
):
    """List all monitored tables.

    Tenant-scoped API keys only see tables belonging to their tenant.
    """
    tables = await get_schema_discovery().get_all_tables()
    if tenant_filter:
        tables = [
            t for t in tables
            if t.get("service_name") == tenant_filter
        ]
    return {"tables": tables, "count": len(tables)}


@router.get(
    "/tables/{service_name}/{table_name}",
    dependencies=[Depends(require_viewer_role)],
)
async def get_table(
    service_name: str,
    table_name: str,
    tenant_filter: str | None = Depends(get_tenant_filter),
):
    """Get table details including columns."""
    if tenant_filter and service_name != tenant_filter:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied. This API key is scoped to tenant "
                f"'{tenant_filter}', but service_name '{service_name}' "
                f"was requested."
            ),
        )
    table = await get_schema_discovery().get_table_by_name(
        service_name,
        table_name,
    )
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {service_name}.{table_name} was not found. "
                "Check GET /tables to confirm discovery has completed "
                "for that table."
            ),
        )
    return table


@router.get(
    "/tables/{service_name}/{table_name}/columns",
    dependencies=[Depends(require_viewer_role)],
    response_model=TableColumnsResponse,
)
async def get_table_columns(
    service_name: str,
    table_name: str,
    tenant_filter: str | None = Depends(get_tenant_filter),
):
    """Get columns for a specific table."""
    if tenant_filter and service_name != tenant_filter:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied. This API key is scoped to tenant "
                f"'{tenant_filter}', but service_name '{service_name}' "
                f"was requested."
            ),
        )
    table = await get_schema_discovery().get_table_by_name(
        service_name,
        table_name,
    )
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {service_name}.{table_name} was not found. "
                "Check GET /tables to confirm discovery has completed "
                "for that table."
            ),
        )
    return {"columns": table.get("columns", [])}
