"""Event persistence with retry, identity, and change extraction."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Optional

from db_monitor.core.models import KafkaEvent, MonitoredColumn
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.changes import ChangeProcessor
from db_monitor.ingestion.row_identity import extract_row_identity
from db_monitor.ingestion.schema import SchemaDiscovery
from db_monitor.metrics import db_write_duration_seconds
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

logger = logging.getLogger(__name__)

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)


async def _insert_event_record(
    session: object, event_obj: KafkaEvent
) -> bool:
    """Insert an event once, keyed by Kafka topic/partition/offset."""
    now = datetime.now(timezone.utc)
    insert_stmt = (
        pg_insert(KafkaEvent)
        .values(
            event_type=event_obj.event_type,
            event_time=event_obj.event_time,
            user_id=event_obj.user_id,
            service_name=event_obj.service_name,
            kafka_topic=event_obj.kafka_topic,
            kafka_partition=event_obj.kafka_partition,
            kafka_offset=event_obj.kafka_offset,
            source_table_id=event_obj.source_table_id,
            operation=event_obj.operation,
            row_identity=event_obj.row_identity,
            event_data=event_obj.event_data,
            raw_payload=event_obj.raw_payload,
            capture_time=(
                event_obj.event_time
                if event_obj.event_time is not None
                else now
            ),
        )
        .on_conflict_do_nothing(
            index_elements=[
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "capture_time",
            ]
        )
        .returning(KafkaEvent.id)
    )
    result = await session.execute(insert_stmt)  # type: ignore[union-attr]
    inserted_id = result.scalar_one_or_none()
    if inserted_id is None:
        return False

    event_obj.id = inserted_id
    return True


async def _populate_row_identity(
    session: object, event_obj: KafkaEvent
) -> None:
    """Populate row identity on an event before it is inserted."""
    primary_key_columns: list[str] = []
    if event_obj.source_table_id is not None:
        result = await session.execute(  # type: ignore[union-attr]
            select(MonitoredColumn.column_name).where(
                MonitoredColumn.table_id == event_obj.source_table_id,
                MonitoredColumn.is_primary_key.is_(True),
            )
        )
        primary_key_columns = [
            column_name
            for column_name in result.scalars().all()
            if column_name
        ]

    event_obj.row_identity = extract_row_identity(
        event_obj.event_data,
        primary_key_columns=primary_key_columns,
    )


async def _populate_row_identity_batch(
    session: object,
    events: list[KafkaEvent],
) -> None:
    """Populate row identity for a batch of events in one round-trip.

    Fetches primary-key column names once per unique ``source_table_id``
    found in the batch, then applies ``extract_row_identity`` to each
    event in memory.
    """
    table_ids: set[int] = {
        e.source_table_id
        for e in events
        if e.source_table_id is not None
    }
    if not table_ids:
        return

    pk_result = await session.execute(  # type: ignore[union-attr]
        select(
            MonitoredColumn.table_id,
            MonitoredColumn.column_name,
        ).where(
            MonitoredColumn.table_id.in_(table_ids),
            MonitoredColumn.is_primary_key.is_(True),
        )
    )
    pk_by_table: dict[int, list[str]] = {}
    for table_id, col_name in pk_result.fetchall():
        pk_by_table.setdefault(table_id, []).append(col_name)

    for event_obj in events:
        pk_cols = pk_by_table.get(event_obj.source_table_id, [])
        event_obj.row_identity = extract_row_identity(
            event_obj.event_data,
            primary_key_columns=pk_cols,
        )


async def _store_event_graph_with_retries(
    session_factory: object,
    event_obj: KafkaEvent,
    max_retries: int = 5,
) -> bool:
    """Persist an event and its derived records with transaction retries."""
    base_delay = 1.0
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            with db_write_duration_seconds.labels(
                operation=event_obj.operation or "unknown"
            ).time():
                async with session_factory() as session:  # type: ignore[call-overload]
                    async with session.begin():
                        table_id = await schema_discovery.process_event(
                            event_obj,
                            session=session,
                        )
                        if table_id:
                            event_obj.source_table_id = table_id

                        await _populate_row_identity(session, event_obj)

                        inserted = await _insert_event_record(
                            session,
                            event_obj,
                        )
                        if inserted and table_id:
                            await change_processor.process_event(
                                event_obj,
                                session=session,
                            )
            return inserted
        except (OperationalError, DBAPIError) as exc:
            wait = base_delay * (2 ** (attempt - 1)) + random.random() * 0.1
            logger.warning(
                "Transient DB error on attempt %d/%d: %s. Retrying in %.2fs",
                attempt,
                max_retries,
                exc,
                wait,
            )
            await asyncio.sleep(wait)
            last_exc = exc
            continue
        except SQLAlchemyError as exc:
            logger.error(
                "Non-retryable DB error when writing event: %s", exc
            )
            raise
    logger.error(
        "Exhausted DB write retries (%d). Raising last exception",
        max_retries,
    )
    if last_exc is None:
        raise RuntimeError("DB write retries exhausted")
    raise last_exc


async def _store_batch_graph_with_retries(
    session_factory: object,
    events: list[KafkaEvent],
    max_retries: int = 5,
) -> set[tuple[str | None, int | None, int | None]]:
    """Persist one batch via bulk event insert and per-event change extraction.

    Returns a set of event-position keys for newly inserted rows.
    """
    if not events:
        return set()

    base_delay = 1.0
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            with db_write_duration_seconds.labels(
                operation="BATCH"
            ).time():
                async with session_factory() as session:  # type: ignore[call-overload]
                    async with session.begin():
                        table_id_map = (
                            await schema_discovery.process_events_batch(
                                events,
                                session=session,
                            )
                        )
                        for event_obj in events:
                            event_obj.source_table_id = table_id_map.get(
                                id(event_obj)
                            )

                        await _populate_row_identity_batch(session, events)

                        now = datetime.now(timezone.utc)
                        values = [
                            {
                                "event_type": event_obj.event_type,
                                "event_time": event_obj.event_time,
                                "user_id": event_obj.user_id,
                                "service_name": event_obj.service_name,
                                "kafka_topic": event_obj.kafka_topic,
                                "kafka_partition": (
                                    event_obj.kafka_partition
                                ),
                                "kafka_offset": event_obj.kafka_offset,
                                "source_table_id": (
                                    event_obj.source_table_id
                                ),
                                "operation": event_obj.operation,
                                "row_identity": event_obj.row_identity,
                                "event_data": event_obj.event_data,
                                "raw_payload": event_obj.raw_payload,
                                "capture_time": (
                                    event_obj.event_time
                                    if event_obj.event_time is not None
                                    else now
                                ),
                            }
                            for event_obj in events
                        ]
                        insert_stmt = (
                            pg_insert(KafkaEvent)
                            .values(values)
                            .on_conflict_do_nothing(
                                index_elements=[
                                    "kafka_topic",
                                    "kafka_partition",
                                    "kafka_offset",
                                    "capture_time",
                                ]
                            )
                            .returning(
                                KafkaEvent.id,
                                KafkaEvent.kafka_topic,
                                KafkaEvent.kafka_partition,
                                KafkaEvent.kafka_offset,
                            )
                        )

                        result = await session.execute(insert_stmt)
                        inserted_rows = result.all()
                        inserted_lookup = {
                            (
                                row.kafka_topic,
                                row.kafka_partition,
                                row.kafka_offset,
                            ): row.id
                            for row in inserted_rows
                        }

                        inserted_keys = set(inserted_lookup.keys())

                        inserted_events = [
                            e
                            for e in events
                            if (
                                e.kafka_topic,
                                e.kafka_partition,
                                e.kafka_offset,
                            )
                            in inserted_keys
                        ]
                        for event_obj in inserted_events:
                            inserted_id = inserted_lookup.get(
                                (
                                    event_obj.kafka_topic,
                                    event_obj.kafka_partition,
                                    event_obj.kafka_offset,
                                )
                            )
                            if inserted_id is not None:
                                event_obj.id = inserted_id

                        await change_processor.process_events_batch(
                            inserted_events,
                            session=session,
                        )

                        return inserted_keys
        except (OperationalError, DBAPIError) as exc:
            wait = base_delay * (2 ** (attempt - 1)) + random.random() * 0.1
            logger.warning(
                "Transient DB error on batch attempt %d/%d: %s. "
                "Retrying in %.2fs",
                attempt,
                max_retries,
                exc,
                wait,
            )
            await asyncio.sleep(wait)
            last_exc = exc
            continue
        except SQLAlchemyError as exc:
            logger.error(
                "Non-retryable DB error when writing batch: %s", exc
            )
            raise

    logger.error(
        "Exhausted DB batch write retries (%d). Raising last exception",
        max_retries,
    )
    if last_exc is None:
        raise RuntimeError("DB batch write retries exhausted")
    raise last_exc
