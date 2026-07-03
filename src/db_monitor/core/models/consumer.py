"""Consumer checkpoint and dead-letter event ORM models."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.sql import func

from db_monitor.core.models.base import Base


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
