"""FastAPI response models for core DB Monitor endpoints."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class TableListResponse(BaseModel):
    """Response payload for the monitored tables index endpoint."""

    tables: list[dict[str, Any]]
    count: int


class TableColumnsResponse(BaseModel):
    """Response payload for one table's discovered column list."""

    columns: list[dict[str, Any]]


class EventItemResponse(BaseModel):
    """Response model for one rendered event row."""

    id: int
    event_type: str
    event_time: str | None
    user_id: str | None
    service_name: str | None
    operation: str | None
    source_table_id: int | None
    row_identity: dict[str, Any] | None
    event_data: dict[str, Any] | None


class EventsResponse(BaseModel):
    """Response payload for event listing endpoints."""

    events: list[EventItemResponse]
    total: int | None
    limit: int
    offset: int


class ChangesResponse(BaseModel):
    """Response payload for column change listing endpoints."""

    service_name: str
    table_name: str
    row_identity: dict[str, Any] | None
    changes: list[dict[str, Any]]
    count: int
    limit: int
    offset: int


class PointInTimeValueResponse(BaseModel):
    """Response payload for point-in-time value lookups."""

    service_name: str
    table_name: str
    column_name: str
    timestamp: str
    value: Any


class HealthResponse(BaseModel):
    """Response payload for health and liveness checks."""

    model_config = ConfigDict(extra="allow")

    status: str
    shutting_down: bool


class ReadinessResponse(BaseModel):
    """Response payload for readiness checks."""

    model_config = ConfigDict(extra="allow")

    status: str
    checks: dict[str, Any]


class AppInfoResponse(BaseModel):
    """Response payload for app metadata endpoint."""

    title: str
    description: str
    healthy: bool
    shutting_down: bool


class AccessTokenExchangeResponse(BaseModel):
    """Response payload for access token exchange endpoint."""

    access_token: str
    token_type: str
    expires_at: str
    owner_name: str
    role: str


class WebSocketTokenExchangeResponse(BaseModel):
    """Response payload for websocket token exchange endpoint."""

    session_token: str
    expires_at: str
    owner_name: str
    role: str
