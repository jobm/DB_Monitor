"""SQLAlchemy models for DB Monitor Server."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
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

    columns = relationship(
        "MonitoredColumn",
        back_populates="table",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index(
            "ix_monitored_tables_service_db_table",
            "service_name",
            "database_name",
            "table_name",
            unique=True,
        ),
    )


class MonitoredColumn(Base):
    """Schema catalog - tracks columns for monitored tables."""

    __tablename__ = "monitored_columns"

    id = Column(Integer, primary_key=True, index=True)
    table_id = Column(
        Integer,
        ForeignKey("monitored_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    column_name = Column(String(128), nullable=False)
    data_type = Column(String(64), nullable=False)
    is_primary_key = Column(Boolean, default=False)
    is_nullable = Column(Boolean, default=True)
    audit_enabled = Column(Boolean, default=True)

    table = relationship("MonitoredTable", back_populates="columns")

    __table_args__ = (
        Index("ix_monitored_columns_table_id", "table_id"),
        Index(
            "ux_monitored_columns_table_column",
            "table_id",
            "column_name",
            unique=True,
        ),
    )


class KafkaEventLegacy(Base):
    """Legacy raw event table (kept for backward compatibility)."""

    __tablename__ = "kafka_events"

    id = Column(Integer, primary_key=True, index=True)
    value = Column(Text, nullable=False)


class KafkaEvent(Base):
    """Structured event storage.

    Stores structured CDC fields, parsed event JSON, and indexes
    for common query patterns.
    """

    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String(64), nullable=False, index=True)
    event_time = Column(DateTime(timezone=True), nullable=False, index=True)
    user_id = Column(String(128), nullable=True, index=True)
    service_name = Column(String(128), nullable=True, index=True)
    kafka_topic = Column(String(256), nullable=True)
    kafka_partition = Column(Integer, nullable=True)
    kafka_offset = Column(BigInteger, nullable=True)

    source_table_id = Column(
        Integer,
        ForeignKey("monitored_tables.id"),
        nullable=True,
    )
    operation = Column(String(16), nullable=True)  # INSERT, UPDATE, DELETE
    row_identity = Column(JSONB, nullable=True)

    event_data = Column(JSONB, nullable=True)
    raw_payload = Column(Text, nullable=False)
    capture_time = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_events_type_time", "event_type", "event_time"),
        Index("ix_events_source_table_time", "source_table_id", "event_time"),
        Index(
            "ix_events_row_identity",
            "row_identity",
            postgresql_using="gin",
        ),
        Index(
            "ux_events_kafka_position",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "capture_time",
            unique=True,
        ),
    )


class ConsumerCheckpoint(Base):
    """Tracks the last committed broker position per consumer group."""

    __tablename__ = "consumer_checkpoints"

    id = Column(Integer, primary_key=True, index=True)
    consumer_group = Column(String(128), nullable=False)
    broker_kind = Column(String(32), nullable=False)
    broker_destination = Column(String(256), nullable=False)
    broker_substream = Column(String(128), nullable=False, default="")
    broker_position = Column(String(128), nullable=False)
    kafka_topic = Column(String(256), nullable=True)
    kafka_partition = Column(Integer, nullable=True)
    kafka_offset = Column(BigInteger, nullable=True)
    last_event_time = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index(
            "ux_consumer_checkpoints_group_broker_stream",
            "consumer_group",
            "broker_kind",
            "broker_destination",
            "broker_substream",
            unique=True,
        ),
        Index(
            "ux_consumer_checkpoints_group_topic_partition",
            "consumer_group",
            "kafka_topic",
            "kafka_partition",
            unique=True,
        ),
        Index("ix_consumer_checkpoints_updated_at", "updated_at"),
    )


class DeadLetterEvent(Base):
    """Persists failed consumer messages for operator replay workflows."""

    __tablename__ = "dead_letter_events"

    id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String(128), nullable=True, index=True)
    broker_kind = Column(String(32), nullable=False)
    broker_destination = Column(String(256), nullable=False)
    broker_substream = Column(String(128), nullable=False, default="")
    broker_position = Column(String(128), nullable=False)
    kafka_topic = Column(String(256), nullable=True)
    kafka_partition = Column(Integer, nullable=True)
    kafka_offset = Column(BigInteger, nullable=True)
    operation = Column(String(16), nullable=True)
    raw_payload = Column(Text, nullable=False)
    error_message = Column(Text, nullable=False)
    failed_at = Column(DateTime(timezone=True), server_default=func.now())
    is_replayed = Column(Boolean, default=False, nullable=False)
    replayed_at = Column(DateTime(timezone=True), nullable=True)
    replay_error = Column(Text, nullable=True)

    __table_args__ = (
        Index(
            "ux_dead_letter_events_broker_position",
            "broker_kind",
            "broker_destination",
            "broker_substream",
            "broker_position",
            unique=True,
        ),
        Index(
            "ux_dead_letter_events_kafka_position",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            unique=True,
        ),
        Index("ix_dead_letter_events_failed_at", "failed_at"),
        Index("ix_dead_letter_events_is_replayed", "is_replayed"),
    )


class ColumnChange(Base):
    """Stores column-level changes for temporal queries."""

    __tablename__ = "column_changes"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(
        Integer,
        ForeignKey("events.id", ondelete="CASCADE"),
        nullable=False,
    )
    table_id = Column(
        Integer,
        ForeignKey("monitored_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    column_id = Column(
        Integer,
        ForeignKey("monitored_columns.id", ondelete="CASCADE"),
        nullable=False,
    )

    operation = Column(String(16), nullable=False)
    row_identity = Column(JSONB, nullable=True)
    old_value = Column(JSONB, nullable=True)
    new_value = Column(JSONB, nullable=True)

    changed_at = Column(DateTime(timezone=True), nullable=False, index=True)

    __table_args__ = (
        Index(
            "ix_changes_table_col_time",
            "table_id",
            "column_id",
            "changed_at",
        ),
        Index(
            "ix_changes_row_identity",
            "row_identity",
            postgresql_using="gin",
        ),
    )


class ApiKey(Base):
    """API Keys for authentication and RBAC."""

    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, index=True)
    key_hash = Column(String(255), nullable=False)
    owner_name = Column(String(128), nullable=False)
    role = Column(String(32), nullable=False, default="viewer")
    is_active = Column(Boolean, default=True)
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    revoked_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


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
