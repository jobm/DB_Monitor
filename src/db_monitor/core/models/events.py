"""Event and column-change ORM models."""

from __future__ import annotations


from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from db_monitor.core.models.base import Base


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
    ingested_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_events_type_time", "event_type", "event_time"),
        Index("ix_events_source_table_time", "source_table_id", "event_time"),
        Index(
            "ix_events_cursor_service_type_time",
            "service_name",
            "event_type",
            "capture_time",
            "id",
        ),
        Index(
            "ix_events_cursor_source_table_time",
            "source_table_id",
            "capture_time",
            "id",
        ),
        Index(
            "ix_events_row_identity",
            "row_identity",
            postgresql_using="gin",
        ),
        UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "capture_time",
            name="ux_events_kafka_position",
        ),
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
            "ix_changes_cursor_table_time",
            "table_id",
            "changed_at",
            "id",
        ),
        Index(
            "ix_changes_cursor_column_time",
            "column_id",
            "changed_at",
            "id",
        ),
        Index(
            "ix_changes_row_identity",
            "row_identity",
            postgresql_using="gin",
        ),
    )
