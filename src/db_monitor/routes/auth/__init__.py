"""Authentication and API key lifecycle endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from db_monitor.core.config import settings

# Import sub-module routers
from db_monitor.routes.auth.jwt_routes import (
    CUSTOMER_JWT_SECRET_STATE,
    bootstrap_customer_auth_scope,
    get_customer_jwt_state,
    recover_customer_jwt_secrets,
    rotate_customer_jwt_secrets,
    router as jwt_router,
)
from db_monitor.routes.auth.key_routes import (
    bootstrap_admin_key,
    create_access_token_exchange,
    create_api_key,
    create_websocket_session_token,
    list_api_keys,
    revoke_api_key,
    rotate_api_key,
    router as key_router,
)

# Combine sub-routers into one master router
router = APIRouter()
router.include_router(key_router)
router.include_router(jwt_router)

__all__ = [
    "CUSTOMER_JWT_SECRET_STATE",
    "router",
    "settings",
    "create_access_token_exchange",
    "create_websocket_session_token",
    "create_api_key",
    "list_api_keys",
    "rotate_api_key",
    "revoke_api_key",
    "bootstrap_admin_key",
    "bootstrap_customer_auth_scope",
    "get_customer_jwt_state",
    "rotate_customer_jwt_secrets",
    "recover_customer_jwt_secrets",
]
