"""Event persistence with retry for single events and batches."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Callable, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db_monitor.core.models import KafkaEvent
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.changes import ChangeProcessor
from db_monitor.ingestion.event_storage.identity import (
    _populate_row_identity,
    _populate_row_identity_batch,
)
from db_monitor.ingestion.event_storage.insert import _insert_event_record
from db_monitor.metrics import db_write_duration_seconds
from db_monitor.schema import SchemaDiscovery

logger = logging.getLogger(__name__)

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)


async def _store_event_graph_with_retries(
    session_factory: Callable[[], AsyncSession],
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
                async with session_factory() as session:
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
    session_factory: Callable[[], AsyncSession],
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
                async with session_factory() as session:
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
                        values: list[dict[str, object]] = [
                            {
                                "event_type": event_obj.event_type,
                                "event_time": event_obj.event_time,
                                "user_id": event_obj.user_id,
                                "service_name": (
                                    event_obj.service_name
                                ),
                                "kafka_topic": event_obj.kafka_topic,
                                "kafka_partition": (
                                    event_obj.kafka_partition
                                ),
                                "kafka_offset": event_obj.kafka_offset,
                                "source_table_id": (
                                    event_obj.source_table_id
                                ),
                                "operation": event_obj.operation,
                                "row_identity": (
                                    event_obj.row_identity
                                ),
                                "event_data": event_obj.event_data,
                                "raw_payload": event_obj.raw_payload,
                                "capture_time": (
                                    event_obj.event_time
                                    if event_obj.event_time
                                    is not None
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
                        inserted_lookup: dict[
                            tuple[str | None, int | None, int | None], int | None
                        ] = {}
                        for row in inserted_rows:
                            row_id: int | None = row.id
                            inserted_lookup[
                                (row.kafka_topic, row.kafka_partition, row.kafka_offset)
                            ] = row_id

                        inserted_keys: set[tuple[str | None, int | None, int | None]] = set()
                        for event_obj in events:
                            key = (event_obj.kafka_topic, event_obj.kafka_partition, event_obj.kafka_offset)
                            if key in inserted_lookup:
                                inserted_keys.add(key)

                        inserted_events = [
                            e for e in events
                            if (e.kafka_topic, e.kafka_partition, e.kafka_offset) in inserted_keys
                        ]
                        for event_obj in inserted_events:
                            key = (event_obj.kafka_topic, event_obj.kafka_partition, event_obj.kafka_offset)
                            inserted_id = inserted_lookup.get(key)
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
