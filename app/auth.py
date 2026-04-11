"""Authentication and RBAC service for API Keys."""

import os
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security.api_key import APIKeyHeader
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from extensions import AsyncSessionLocal
from models import ApiKey

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key_hash(plain_key: str, hashed_key: str) -> bool:
    """Verify an API key against its hash."""
    return pwd_context.verify(plain_key[:72], hashed_key)

def get_api_key_hash(plain_key: str) -> str:
    """Generate a hash for a new API key."""
    return pwd_context.hash(plain_key[:72])

def generate_new_api_key() -> str:
    """Generate a secure random API key."""
    return secrets.token_urlsafe(32)

async def get_current_api_key(api_key_header_value: str = Security(api_key_header)) -> Optional[ApiKey]:
    """Retrieve the ApiKey record if the provided key is valid."""
    if not api_key_header_value:
        return None
        
    async with AsyncSessionLocal() as session:
        # Since we hash keys, we normally couldn't look them up by hash easily
        # For API keys, a common pattern to allow lookup is prefix.key 
        # But for absolute security, if we only have the raw key, we must check against active keys
        # Optimization: storing an unhashed prefix or ID to lookup the row.
        # For simplicity here, we assume the client passes `{id}.{raw_key}`
        
        try:
            key_id_str, raw_key = api_key_header_value.split(".", 1)
            key_id = int(key_id_str)
        except ValueError:
            # Invalid format
            return None
            
        stmt = select(ApiKey).where(ApiKey.id == key_id, ApiKey.is_active == True)
        result = await session.execute(stmt)
        api_key_record = result.scalar_one_or_none()
        
        if api_key_record and verify_api_key_hash(raw_key, api_key_record.key_hash):
            return api_key_record
            
    return None

async def require_valid_api_key(
    api_key_record: Optional[ApiKey] = Depends(get_current_api_key)
) -> ApiKey:
    """Dependency that requires any valid API Key."""
    if not api_key_record:
        raise HTTPException(
            status_code=401, 
            detail="Invalid or missing X-API-Key header. Format expected: {id}.{raw_secret}"
        )
    return api_key_record

async def require_admin_role(
    api_key_record: ApiKey = Depends(require_valid_api_key)
) -> ApiKey:
    """Dependency that requires an Admin API Key."""
    if api_key_record.role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return api_key_record

async def require_viewer_role(
    api_key_record: ApiKey = Depends(require_valid_api_key)
) -> ApiKey:
    """Dependency that requires at least a Viewer API Key (Admins also pass)."""
    if api_key_record.role not in ("admin", "viewer"):
        raise HTTPException(status_code=403, detail="Viewer role required")
    return api_key_record
