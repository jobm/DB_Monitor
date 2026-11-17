"""Authentication and customer lifecycle ORM models."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from db_monitor.core.models.base import Base


class ApiKey(Base):
    """API Keys for authentication, RBAC, and multi-tenant isolation.

    When ``tenant_id`` is set, the key is scoped to that tenant and
    can only access data belonging to that tenant. Admin keys (with
    ``tenant_id`` set to ``NULL``) can access all tenants.
    """

    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    key_hash = Column(String(255), nullable=False)
    owner_name = Column(String(128), nullable=False)
    role = Column(String(32), nullable=False, default="viewer")
    is_active = Column(Boolean, default=True)
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    tenant_id = Column(String(128), nullable=True, index=True)


class CustomerJWTSecretState(Base):
    """Persist customer JWT secret lifecycle state."""

    __tablename__ = "customer_jwt_secret_state"

    customer_id = Column(String(128), primary_key=True)
    state = Column(JSONB, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CustomerLifecycleState(Base):
    """Persist customer lifecycle control-plane state."""

    __tablename__ = "customer_lifecycle_state"

    customer_id = Column(String(128), primary_key=True)
    state = Column(JSONB, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CustomerProvisionJob(Base):
    """Persist one customer provisioning orchestration job."""

    __tablename__ = "customer_provision_jobs"

    job_id = Column(String(64), primary_key=True)
    customer_id = Column(String(128), nullable=False, index=True)
    state = Column(JSONB, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
