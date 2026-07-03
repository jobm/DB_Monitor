"""Schema catalog ORM models — monitored tables and columns."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.sql import func

from db_monitor.core.models.base import Base


class MonitoredTable(Base):
    """Schema catalog - tracks all monitored database tables."""

    __tablename__ = "monitored_tables"

    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    service_name: Mapped[str] = Column(String(128), nullable=False)
    database_name: Mapped[str] = Column(String(128), nullable=False)
    table_name: Mapped[str] = Column(String(128), nullable=False)
    topic_name: Mapped[str] = Column(String(256), nullable=False)
    is_active: Mapped[bool] = Column(Boolean, default=True)
    created_at: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )

    columns: Mapped[list["MonitoredColumn"]] = relationship(
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

    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    table_id: Mapped[int] = Column(
        Integer,
        ForeignKey("monitored_tables.id", ondelete="CASCADE"),
        nullable=False,
    )
    column_name: Mapped[str] = Column(String(128), nullable=False)
    data_type: Mapped[str] = Column(String(64), nullable=False)
    is_primary_key: Mapped[bool] = Column(Boolean, default=False)
    is_nullable: Mapped[bool] = Column(Boolean, default=True)
    audit_enabled: Mapped[bool] = Column(Boolean, default=True)

    table: Mapped["MonitoredTable"] = relationship(
        "MonitoredTable", back_populates="columns"
    )

    __table_args__ = (
        Index("ix_monitored_columns_table_id", "table_id"),
        Index(
            "ux_monitored_columns_table_column",
            "table_id",
            "column_name",
            unique=True,
        ),
    )
