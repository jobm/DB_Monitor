"""Event record insert with deduplication via ON CONFLICT DO NOTHING."""

from __future__ import annotations

from datetime import datetime, timezone

from db_monitor.core.models import KafkaEvent
from sqlalchemy.dialects.postgresql import insert as pg_insert


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
