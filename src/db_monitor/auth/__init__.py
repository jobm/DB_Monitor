"""Authentication and RBAC service for API Keys."""

from __future__ import annotations

import jwt as _jwt

from fastapi import HTTPException as _HTTPException

from db_monitor.auth.db import (
    _authenticate_bearer_token,
    _get_api_key_by_id,
    authenticate_credentials,
    bearer_scheme,
    get_current_api_key,
    get_current_bearer_api_key,
    is_api_key_usable,
)
from db_monitor.auth.deps import (
    ensure_customer_tenant_scope,
    get_current_authenticated_api_key,
    get_current_tenant_id,
    get_tenant_filter,
    require_admin_role,
    require_customer_admin_scope,
    require_global_admin,
    require_valid_api_key,
    require_viewer_role,
)
from db_monitor.auth.tokens import (
    _build_session_token,
    build_access_token,
    build_ws_session_token,
    decode_session_token,
)
from db_monitor.auth.utils import (
    _normalize_secret,
    _utc_now,
    api_key_header,
    build_api_key_expiration,
    generate_new_api_key,
    get_api_key_hash,
    password_hasher,
    verify_api_key_hash,
)
from db_monitor.core.config import settings as _settings

# Re-export external names so tests can do auth_mod.jwt.encode(...),
# auth_mod.HTTPException, auth_mod.settings.auth.jwt_secret.
jwt = _jwt
HTTPException = _HTTPException
settings = _settings

__all__ = [
    "HTTPException",
    "_authenticate_bearer_token",
    "_build_session_token",
    "_get_api_key_by_id",
    "_normalize_secret",
    "_utc_now",
    "api_key_header",
    "authenticate_credentials",
    "bearer_scheme",
    "build_access_token",
    "build_api_key_expiration",
    "build_ws_session_token",
    "decode_session_token",
    "ensure_customer_tenant_scope",
    "generate_new_api_key",
    "get_api_key_hash",
    "get_current_api_key",
    "get_tenant_filter",
    "get_current_authenticated_api_key",
    "get_current_tenant_id",
    "get_current_bearer_api_key",
    "is_api_key_usable",
    "jwt",
    "password_hasher",
    "require_admin_role",
    "require_customer_admin_scope",
    "require_global_admin",
    "require_valid_api_key",
    "require_viewer_role",
    "settings",
    "verify_api_key_hash",
]
