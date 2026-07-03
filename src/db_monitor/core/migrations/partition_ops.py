"""Partition management: create monthly partitions, drop old partitions."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)


def _month_start(reference: datetime, months_ahead: int) -> datetime:
    """Return the first instant of a month offset from ``reference``."""
    total_months = reference.year * 12 + (reference.month - 1) + months_ahead
    year = total_months // 12
    month = total_months % 12 + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _extract_upper_bound(bound_expr: str) -> str | None:
    """Extract the upper bound value from a PG partition bound expression."""
    match = re.search(r"TO\s*\(\s*'([^']+)'\s*\)", bound_expr)
    if match:
        return match.group(1)
    return None


async def create_monthly_partitions(
    connection: AsyncConnection,
    table_name: str,
    partition_column: str,
    months_ahead: int = 3,
) -> list[str]:
    """Create monthly range partitions for the upcoming months.

    Idempotent — skips partitions that already exist. Returns the names
    of newly created partitions.
    """
    del partition_column
    if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
        raise ValueError(
            f"Invalid table_name for partition DDL: {table_name!r}"
        )

    now = datetime.now(timezone.utc)
    created: list[str] = []

    for offset in range(months_ahead):
        month_start = _month_start(now, offset)
        month_end = _month_start(now, offset + 1)
        partition_name = (
            f"{table_name}_{month_start.strftime('%Y%m')}"
        )

        exists = await connection.execute(
            text(
                """
                SELECT 1 FROM pg_class
                WHERE relname = :partition_name
                """
            ),
            {"partition_name": partition_name},
        )
        if exists.fetchone() is not None:
            continue

        await connection.execute(
            text(
                f"""
                CREATE TABLE {partition_name}
                PARTITION OF {table_name}
                FOR VALUES FROM (
                '{month_start.isoformat()}') TO
                ('{month_end.isoformat()}')
                """
            )
        )
        created.append(partition_name)

    return created


async def drop_old_partitions(
    connection: AsyncConnection,
    table_name: str,
    partition_column: str,
    older_than: datetime,
) -> list[str]:
    """Drop range partitions whose upper bound is before ``older_than``.

    Only drops partitions that are entirely before the cutoff — does not
    touch the DEFAULT partition. Returns the names of dropped partitions.
    """
    del partition_column
    if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
        raise ValueError(
            f"Invalid table_name for partition DDL: {table_name!r}"
        )

    # Find child partitions older than the cutoff
    rows = await connection.execute(
        text(
            """
            SELECT c.relname AS partition_name,
                   pg_get_expr(c.relpartbound, c.oid) AS bound
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = :table_name
              AND c.relispartition = TRUE
              AND c.relname != :default_name
            """
        ),
        {
            "table_name": table_name,
            "default_name": f"{table_name}_default",
        },
    )

    dropped: list[str] = []
    for partition_name, bound_expr in rows.fetchall():
        if bound_expr is None:
            continue
        # bound_expr is e.g. "FOR VALUES FROM ('2026-01-01')
        # TO ('2026-02-01')"  — extract the upper bound
        upper_str = _extract_upper_bound(bound_expr)
        if upper_str is None:
            continue
        try:
            upper = datetime.fromisoformat(upper_str)
        except ValueError:
            continue

        if upper <= older_than:
            await connection.execute(
                text(f"DROP TABLE IF EXISTS {partition_name}")
            )
            dropped.append(partition_name)
            logger.info(
                "Dropped old partition %s (upper bound %s <= cutoff %s)",
                partition_name,
                upper.isoformat(),
                older_than.isoformat(),
            )

    return dropped
