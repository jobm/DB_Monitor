"""SQLAlchemy models for DB Monitor Server."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class MonitoredTable(Base):
    """Schema catalog - tracks all monitored database tables."""

    __tablename__ = "monitored_tables"

    id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String(128), nullable=False)
    database_name = Column(String(128), nullable=False)
    table_name = Column(String(128), nullable=False)
    topic_name = Column(String(256), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    columns = relationship("MonitoredColumn", back_populates="table", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_monitored_tables_service_db_table", "service_name", "database_name", "table_name", unique=True),
    )


class MonitoredColumn(Base):
    """Schema catalog - tracks columns for monitored tables."""

    __tablename__ = "monitored_columns"

    id = Column(Integer, primary_key=True, index=True)
    table_id = Column(Integer, ForeignKey("monitored_tables.id", ondelete="CASCADE"), nullable=False)
    column_name = Column(String(128), nullable=False)
    data_type = Column(String(64), nullable=False)
    is_primary_key = Column(Boolean, default=False)
    is_nullable = Column(Boolean, default=True)
    audit_enabled = Column(Boolean, default=True)

    table = relationship("MonitoredTable", back_populates="columns")

    __table_args__ = (
        Index("ix_monitored_columns_table_id", "table_id"),
    )


class KafkaEventLegacy(Base):
	"""Legacy raw event table (kept for backward compatibility / migration)."""

	__tablename__ = "kafka_events"

	id = Column(Integer, primary_key=True, index=True)
	value = Column(Text, nullable=False)


class KafkaEvent(Base):
    """Structured event storage.

    Implements High Priority TODO #1:
    - event_type, event_time, user_id, service_name
    - parsed JSON stored in event_data
    - indexes for common query patterns
    """

    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String(64), nullable=False, index=True)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    user_id = Column(String(128), nullable=True, index=True)
    service_name = Column(String(128), nullable=True, index=True)

    source_table_id = Column(Integer, ForeignKey("monitored_tables.id"), nullable=True)
    operation = Column(String(16), nullable=True)  # INSERT, UPDATE, DELETE

    event_data = Column(JSONB, nullable=True)
    raw_payload = Column(Text, nullable=False)
    capture_time = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_events_type_time", "event_type", "event_time"),
        Index("ix_events_source_table_time", "source_table_id", "event_time"),
    )


class ColumnChange(Base):
    """Stores column-level changes for temporal queries."""

    __tablename__ = "column_changes"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False)
    table_id = Column(Integer, ForeignKey("monitored_tables.id", ondelete="CASCADE"), nullable=False)
    column_id = Column(Integer, ForeignKey("monitored_columns.id", ondelete="CASCADE"), nullable=False)

    operation = Column(String(16), nullable=False)
    old_value = Column(JSONB, nullable=True)
    new_value = Column(JSONB, nullable=True)

    changed_at = Column(DateTime(timezone=True), nullable=False, index=True)

    __table_args__ = (
        Index("ix_changes_table_col_time", "table_id", "column_id", "changed_at"),
    )


class ApiKey(Base):
    """API Keys for authentication and RBAC."""

    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    key_hash = Column(String(255), nullable=False)
    owner_name = Column(String(128), nullable=False)
    role = Column(String(32), nullable=False, default="viewer")  # "admin" or "viewer"
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ApiAuditLog(Base):
    """Audit logs for API access."""

    __tablename__ = "api_audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    api_key_id = Column(Integer, ForeignKey("api_keys.id", ondelete="SET NULL"), nullable=True)
    endpoint = Column(String(255), nullable=False)
    method = Column(String(16), nullable=False)
    status_code = Column(Integer, nullable=False)
    ip_address = Column(String(64), nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    __table_args__ = (
        Index("ix_api_audit_logs_api_key_time", "api_key_id", "timestamp"),
    )
