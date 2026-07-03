"""API key CRUD, token exchange, and bootstrap endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from db_monitor.routes.auth.key_routes.keys import (
    bootstrap_admin_key,
    create_api_key,
    keys_router,
    list_api_keys,
    revoke_api_key,
    rotate_api_key,
)
from db_monitor.routes.auth.key_routes.tokens import (
    create_access_token_exchange,
    create_websocket_session_token,
    token_router,
)

# Combine sub-routers into one router for the parent package.
router = APIRouter()
router.include_router(token_router)
router.include_router(keys_router)

__all__ = [
    "bootstrap_admin_key",
    "create_access_token_exchange",
    "create_api_key",
    "create_websocket_session_token",
    "list_api_keys",
    "revoke_api_key",
    "rotate_api_key",
    "router",
]
