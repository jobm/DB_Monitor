"""Bearer and WebSocket session token exchange endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from db_monitor.auth import (
    build_access_token as issue_access_token,
    build_ws_session_token,
    require_viewer_role,
)
from db_monitor.core.models import ApiKey
from db_monitor.core.responses import (
    AccessTokenExchangeResponse,
    WebSocketTokenExchangeResponse,
)

token_router = APIRouter()


@token_router.post("/auth/token", response_model=AccessTokenExchangeResponse)
async def create_access_token_exchange(
    authenticated_key: ApiKey = Depends(require_viewer_role),
):
    """Exchange a valid credential for a short-lived bearer token."""
    token, expires_at = issue_access_token(authenticated_key)
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_at": expires_at.isoformat(),
        "owner_name": authenticated_key.owner_name,
        "role": authenticated_key.role,
    }


@token_router.post(
    "/auth/ws-token",
    response_model=WebSocketTokenExchangeResponse,
)
async def create_websocket_session_token(
    authenticated_key: ApiKey = Depends(require_viewer_role),
):
    """Exchange a valid credential for a short-lived WebSocket token."""
    token, expires_at = build_ws_session_token(authenticated_key)
    return {
        "session_token": token,
        "expires_at": expires_at.isoformat(),
        "owner_name": authenticated_key.owner_name,
        "role": authenticated_key.role,
    }
