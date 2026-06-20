"""Regression tests for the events table unique constraint.

PostgreSQL partitioned tables require a UNIQUE CONSTRAINT (not merely a
unique index) for INSERT … ON CONFLICT to work.  These tests verify the
model definition and SQL-level behaviour so the bug does not regress.
"""

from __future__ import annotations

import pytest
from models import KafkaEvent
from sqlalchemy import Index, UniqueConstraint


def test_events_model_has_unique_constraint_for_on_conflict() -> None:
    """The KafkaEvent model must use UniqueConstraint (not Index) for the
    kafka position so that ON CONFLICT works on the partitioned table.

    Asserts both sides in one test: the constraint exists AND no stale
    unique index with the same name shadows it.
    """
    table_args = KafkaEvent.__table_args__

    # ── positive: UniqueConstraint must exist ──────────────────────
    constraint_found = False
    for arg in table_args:
        if isinstance(arg, UniqueConstraint) and arg.name == "ux_events_kafka_position":
            constraint_found = True
            # Also verify columns match the ON CONFLICT target.
            actual_columns = tuple(c.name for c in arg.columns)
            expected = ("kafka_topic", "kafka_partition", "kafka_offset", "capture_time")
            assert actual_columns == expected, (
                f"ux_events_kafka_position columns {actual_columns} "
                f"do not match ON CONFLICT target {expected}"
            )
    assert constraint_found, (
        "KafkaEvent.__table_args__ must include a UniqueConstraint "
        "named 'ux_events_kafka_position' for ON CONFLICT to work "
        "on the partitioned events table."
    )

    # ── negative: no stale Index with the same name ────────────────
    for arg in table_args:
        if isinstance(arg, Index) and arg.name == "ux_events_kafka_position":
            pytest.fail(
                "Found Index named 'ux_events_kafka_position' in "
                "KafkaEvent.__table_args__. This should be a "
                "UniqueConstraint, not an Index."
            )


# ── Sandbox tests (require a live PostgreSQL + app stack) ───────────
#
# These are skipped automatically by conftest.py when the live stack
# is unavailable.


@pytest.mark.sandbox
@pytest.mark.anyio
async def test_pg_constraint_is_unique_type() -> None:
    """After migration 0015 the events table must have a unique
    CONSTRAINT (contype='u') rather than just a unique INDEX.
    """
    from extensions import AsyncSessionLocal
    from sqlalchemy import text

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(
                "SELECT contype FROM pg_constraint "
                "WHERE conname = 'ux_events_kafka_position' "
                "AND conrelid = 'events'::regclass"
            )
        )
        row = result.fetchone()

    assert row is not None, (
        "No constraint named 'ux_events_kafka_position' found on "
        "the events table. Migration 0015 may not have run."
    )
    assert row[0] == "u", (
        f"ux_events_kafka_position has contype='{row[0]}' but "
        "expected 'u' (unique constraint). The table likely has a "
        "unique INDEX instead of a unique CONSTRAINT, which will "
        "cause ON CONFLICT to fail on the partitioned table."
    )


@pytest.mark.sandbox
@pytest.mark.anyio
async def test_on_conflict_insert_succeeds_on_partitioned_events() -> None:
    """INSERT … ON CONFLICT DO NOTHING must succeed on the partitioned
    events table.

    This is the definitive regression test for the bug where
    ``InvalidColumnReferenceError: there is no unique or exclusion
    constraint matching the ON CONFLICT specification`` was raised.
    """
    from datetime import datetime, timezone

    from extensions import AsyncSessionLocal
    from sqlalchemy import text

    now = datetime.now(timezone.utc)
    kafka_offset = 999_999_999

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # First insert — should succeed.
            await session.execute(
                text(
                    "INSERT INTO events "
                    "(event_type, event_time, service_name, "
                    " kafka_topic, kafka_partition, kafka_offset, "
                    " operation, raw_payload, capture_time) "
                    "VALUES ('c', :event_time, 'test', "
                    " 'test.topic', 0, :offset, "
                    " 'INSERT', '{}', :capture_time) "
                    "ON CONFLICT "
                    "(kafka_topic, kafka_partition, kafka_offset, "
                    " capture_time) DO NOTHING"
                ),
                {"event_time": now, "offset": kafka_offset, "capture_time": now},
            )

            # Second insert with same key — ON CONFLICT DO NOTHING
            # should silently skip it (rowcount=0).
            result = await session.execute(
                text(
                    "INSERT INTO events "
                    "(event_type, event_time, service_name, "
                    " kafka_topic, kafka_partition, kafka_offset, "
                    " operation, raw_payload, capture_time) "
                    "VALUES ('c', :event_time, 'test', "
                    " 'test.topic', 0, :offset, "
                    " 'INSERT', '{}', :capture_time) "
                    "ON CONFLICT "
                    "(kafka_topic, kafka_partition, kafka_offset, "
                    " capture_time) DO NOTHING"
                ),
                {"event_time": now, "offset": kafka_offset, "capture_time": now},
            )
            rowcount = result.rowcount

    assert rowcount == 0, (
        f"ON CONFLICT DO NOTHING did not suppress duplicate: "
        f"rowcount={rowcount}"
    )
