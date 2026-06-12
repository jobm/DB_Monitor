"""Retention and archival maintenance for high-volume tables."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.config import (
    API_AUDIT_LOG_RETENTION_DAYS,
    COLUMN_CHANGES_RETENTION_DAYS,
    DEAD_LETTER_RETENTION_DAYS,
    EVENT_RETENTION_DAYS,
    RETENTION_ARCHIVE_BEFORE_DELETE,
    RETENTION_ARCHIVE_DIR,
    RETENTION_CLEANUP_BATCH_SIZE,
    RETENTION_CLEANUP_INTERVAL_SECONDS,
    RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE,
)
from core.db import AsyncSessionLocal
from core.models import ApiAuditLog, ColumnChange, DeadLetterEvent, KafkaEvent
from sqlalchemy import delete, select, text

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Retention policy definition for one persisted table."""

    name: str
    model: Any
    timestamp_column: Any
    retention_days: int


def _utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _serialize_row(row: Any) -> dict[str, Any]:
    """Serialize one SQLAlchemy row to JSON-safe key/value data."""
    payload: dict[str, Any] = {}
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        if isinstance(value, datetime):
            payload[column.name] = value.isoformat()
        else:
            payload[column.name] = value
    return payload


def _archive_file_path(table_name: str, now: datetime) -> Path:
    """Return the JSONL archive file path for one table and UTC date."""
    archive_root = Path(RETENTION_ARCHIVE_DIR)
    stamp = now.strftime("%Y%m%d")
    return archive_root / table_name / f"{stamp}.jsonl"


def _append_archive_rows(
    table_name: str,
    rows: list[Any],
    now: datetime,
) -> None:
    """Append serialized rows to a JSONL archive file."""
    archive_path = _archive_file_path(table_name, now)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with archive_path.open("a", encoding="utf-8") as archive_file:
        for row in rows:
            serialized = _serialize_row(row)
            archive_file.write(json.dumps(serialized, separators=(",", ":")))
            archive_file.write("\n")


def retention_policies() -> list[RetentionPolicy]:
    """Return active retention policies from current runtime config."""
    return [
        RetentionPolicy(
            name="events",
            model=KafkaEvent,
            timestamp_column=KafkaEvent.capture_time,
            retention_days=EVENT_RETENTION_DAYS,
        ),
        RetentionPolicy(
            name="column_changes",
            model=ColumnChange,
            timestamp_column=ColumnChange.changed_at,
            retention_days=COLUMN_CHANGES_RETENTION_DAYS,
        ),
        RetentionPolicy(
            name="dead_letter_events",
            model=DeadLetterEvent,
            timestamp_column=DeadLetterEvent.failed_at,
            retention_days=DEAD_LETTER_RETENTION_DAYS,
        ),
        RetentionPolicy(
            name="api_audit_logs",
            model=ApiAuditLog,
            timestamp_column=ApiAuditLog.timestamp,
            retention_days=API_AUDIT_LOG_RETENTION_DAYS,
        ),
    ]


async def _cleanup_policy_once(
    policy: RetentionPolicy,
    session_factory=AsyncSessionLocal,
) -> int:
    """Cleanup one policy and return a cleanup count.

    For partitioned tables this drops entire child partitions whose data
    is entirely before the cutoff (count = partitions dropped). For
    non-partitioned tables it falls back to batched DELETE (count = rows deleted).
    """
    if policy.retention_days <= 0:
        return 0

    cutoff = _utc_now() - timedelta(days=policy.retention_days)
    table_name = policy.model.__tablename__

    # Check if the table is partitioned
    async with session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT 1 FROM pg_partitioned_table
                WHERE partrelid = :table_name::regclass
                """
            ),
            {"table_name": table_name},
        )
        is_partitioned = result.fetchone() is not None

    if is_partitioned:
        # Partition-aware cleanup: drop old child partitions
        return await _cleanup_partitioned_policy_once(
            policy, table_name, cutoff, session_factory
        )

    # Legacy DELETE-based cleanup for non-partitioned tables
    return await _cleanup_delete_policy_once(
        policy, cutoff, session_factory
    )


async def _cleanup_partitioned_policy_once(
    policy: RetentionPolicy,
    table_name: str,
    cutoff: datetime,
    session_factory,
) -> int:
    """Drop partitions whose upper bound is before the cutoff."""
    from migrations import drop_old_partitions

    async with session_factory() as session:
        async with session.begin():
            connection = await session.connection()
            dropped = await drop_old_partitions(
                connection,
                table_name=table_name,
                partition_column=policy.timestamp_column.name,
                older_than=cutoff,
            )
    return len(dropped)


async def _cleanup_delete_policy_once(
    policy: RetentionPolicy,
    cutoff: datetime,
    session_factory,
) -> int:
    """Batched DELETE for non-partitioned tables (legacy fallback)."""
    deleted_rows = 0

    for _ in range(RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE):
        async with session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(policy.model)
                    .where(policy.timestamp_column < cutoff)
                    .order_by(policy.model.id.asc())
                    .limit(RETENTION_CLEANUP_BATCH_SIZE)
                )
                rows = result.scalars().all()

                if not rows:
                    break

                if RETENTION_ARCHIVE_BEFORE_DELETE:
                    _append_archive_rows(
                        table_name=policy.name,
                        rows=rows,
                        now=_utc_now(),
                    )

                ids = [row.id for row in rows]
                await session.execute(
                    delete(policy.model).where(policy.model.id.in_(ids))
                )

                deleted_rows += len(ids)

                if len(ids) < RETENTION_CLEANUP_BATCH_SIZE:
                    break

    return deleted_rows


async def run_retention_cleanup_once(
    session_factory=AsyncSessionLocal,
) -> dict[str, int]:
    """Run one retention cleanup cycle and return per-table delete counts."""
    summary: dict[str, int] = {}
    for policy in retention_policies():
        deleted = await _cleanup_policy_once(
            policy,
            session_factory=session_factory,
        )
        summary[policy.name] = deleted
    return summary


async def retention_cleanup_task() -> None:
    """Background task that periodically applies retention policies."""
    logger.info(
        (
            "Retention cleanup task started "
            "(interval=%ss, batch=%s, max_batches_per_table=%s, "
            "archive_before_delete=%s)"
        ),
        RETENTION_CLEANUP_INTERVAL_SECONDS,
        RETENTION_CLEANUP_BATCH_SIZE,
        RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE,
        RETENTION_ARCHIVE_BEFORE_DELETE,
    )

    while True:
        try:
            summary = await run_retention_cleanup_once()
            logger.info("Retention cleanup summary: %s", summary)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Retention cleanup cycle failed")

        await asyncio.sleep(RETENTION_CLEANUP_INTERVAL_SECONDS)


async def partition_maintenance_task() -> None:
    """Background task that creates upcoming monthly partitions.

    Runs once per day to ensure the next N months of partitions exist
    for partitioned tables.
    """
    logger.info("Partition maintenance task started (interval=86400s)")

    from core.db import engine
    from migrations import create_monthly_partitions

    while True:
        try:
            async with engine.begin() as connection:
                for table_name, column in (
                    ("events", "capture_time"),
                    ("column_changes", "changed_at"),
                ):
                    created = await create_monthly_partitions(
                        connection,
                        table_name=table_name,
                        partition_column=column,
                        months_ahead=3,
                    )
                    if created:
                        logger.info(
                            "Created partitions for %s: %s",
                            table_name,
                            ", ".join(created),
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Partition maintenance cycle failed")

        await asyncio.sleep(86400)  # Once per day
