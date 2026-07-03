"""Operational ORM models — ingestion quotas, audit logs, and spill."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from db_monitor.core.models.base import Base


class IngestionQuotaWindow(Base):
    """Persist one shared quota window for a source or tenant identity."""

    __tablename__ = "ingestion_quota_windows"

    id = Column(Integer, primary_key=True, index=True)
    dimension = Column(String(32), nullable=False)
    identity = Column(String(256), nullable=False)
    window_start = Column(DateTime(timezone=True), nullable=False, index=True)
    event_count = Column(Integer, nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        Index(
            "ux_ingestion_quota_windows_dimension_identity_window_start",
            "dimension",
            "identity",
            "window_start",
            unique=True,
        ),
        Index(
            "ix_ingestion_quota_windows_dimension_identity",
            "dimension",
            "identity",
        ),
    )


class ApiAuditLog(Base):
    """Audit logs for API access."""

    __tablename__ = "api_audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    api_key_id = Column(
        Integer,
        ForeignKey("api_keys.id", ondelete="SET NULL"),
        nullable=True,
    )
    endpoint = Column(String(255), nullable=False)
    method = Column(String(16), nullable=False)
    status_code = Column(Integer, nullable=False)
    ip_address = Column(String(64), nullable=True)
    timestamp = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )

    __table_args__ = (
        Index("ix_api_audit_logs_api_key_time", "api_key_id", "timestamp"),
    )


class ApiAuditLogSpill(Base):
    """Persistent spill table for audit logs that could not be flushed."""

    __tablename__ = "api_audit_log_spill"

    id = Column(Integer, primary_key=True, index=True)
    entry = Column(JSONB, nullable=False)
    error_message = Column(Text, nullable=True)
    spilled_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
    replayed = Column(
        Boolean,
        default=False,
        nullable=False,
    )
    replayed_by = Column(String(128), nullable=True)
    replayed_at = Column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
