"""FastAPI dependency injection for authentication and RBAC."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException

from db_monitor.auth.db import get_current_bearer_api_key, get_current_api_key
from db_monitor.core.models import ApiKey


async def get_current_authenticated_api_key(
    bearer_api_key: Optional[ApiKey] = Depends(get_current_bearer_api_key),
    api_key_record: Optional[ApiKey] = Depends(get_current_api_key),
) -> Optional[ApiKey]:
    """Resolve bearer-token or API-key authentication for HTTP routes."""
    return bearer_api_key or api_key_record


async def require_valid_api_key(
    api_key_record: Optional[ApiKey] = Depends(
        get_current_authenticated_api_key
    ),
) -> ApiKey:
    """Dependency that requires any valid API Key."""
    if not api_key_record:
        raise HTTPException(
            status_code=401,
            detail=(
                "Invalid or missing credentials. "
                "Use Authorization: Bearer <token> "
                "or X-API-Key: <id>.<raw_secret>. "
                "If you only have an API key, first exchange it "
                "at POST /auth/token."
            ),
        )
    return api_key_record


async def require_admin_role(
    api_key_record: ApiKey = Depends(require_valid_api_key),
) -> ApiKey:
    """Dependency that requires an Admin API Key."""
    if api_key_record.role != "admin":
        raise HTTPException(
            status_code=403,
            detail=(
                "Admin role required. Use an admin API key or a bearer token "
                "derived from one."
            ),
        )
    return api_key_record


async def require_viewer_role(
    api_key_record: ApiKey = Depends(require_valid_api_key),
) -> ApiKey:
    """Require at least a Viewer API key.

    Admin credentials also satisfy this dependency.
    """
    if api_key_record.role not in ("admin", "viewer"):
        raise HTTPException(
            status_code=403,
            detail=(
                "Viewer role required. Use a viewer or admin credential to "
                "access this endpoint."
            ),
        )
    return api_key_record
