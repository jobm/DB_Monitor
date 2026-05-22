# Broker consumer logic for DB Monitor Server
import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Optional

from change_processor import ChangeProcessor
from config import (
    KAFKA_BROKER,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
)
from event_parser import parse_event_payload
from event_pipeline import event_pipeline
from extensions import AsyncSessionLocal
from message_brokers import (
    BrokerMessage,
    DeadLetterPublisher,
    build_dead_letter_publisher,
    build_message_consumer,
)
from metrics import (
    circuit_breaker_state,
    consumer_lag,
    consumer_last_successful_commit_timestamp_seconds,
    db_write_duration_seconds,
    dlq_messages_total,
    dlq_records_pending,
    events_consumed_total,
    events_failed_total,
    events_processed_total,
    kafka_commit_duration_seconds,
)
from models import (
    ConsumerCheckpoint,
    DeadLetterEvent,
    KafkaEvent,
    MonitoredColumn,
)
from row_identity import extract_row_identity
from schema_discovery import SchemaDiscovery, extract_operation
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from tracing import start_span
from webhooks import webhook_notifier
from ws_manager import ws_manager

logger = logging.getLogger(__name__)

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)

DLQ_TOPIC = "db-monitor-dlq"
DEFAULT_TOPICS = [KAFKA_TOPIC]
MAX_CONNECTION_RETRIES = 60
CONNECTION_RETRY_DELAY = 5.0
CONSUMER_METRIC_NAME = "broker_consumer"


def _new_consumer_runtime_state() -> dict[str, object]:
    """Build a clean runtime state structure for a Kafka consumer."""
    return {
        "running": False,
        "connected": False,
        "topics": [],
        "broker_kind": "kafka",
        "last_error": None,
        "last_message_at": None,
        "last_commit_at": None,
        "lag_by_partition": {},
        "lag_total": 0,
        "dlq_messages_total": 0,
    }


consumer_runtime: dict[str, object] = _new_consumer_runtime_state()
consumer_runtimes: dict[str, dict[str, object]] = {"default": consumer_runtime}


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        metric_name: str = CONSUMER_METRIC_NAME,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"
        self.metric_name = metric_name
        self._update_metric()

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self._update_metric()
            logger.warning("Circuit breaker opened due to repeated failures")

    def record_success(self):
        self.failure_count = 0
        self.state = "closed"
        self._update_metric()

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        if (
            asyncio.get_event_loop().time() - self.last_failure_time
            > self.recovery_timeout
        ):
            self.state = "half-open"
            self._update_metric()
            logger.info("Circuit breaker half-open, allowing test request")
            return True
        return False

    def _update_metric(self):
        state_value = {
            "closed": 0,
            "open": 1,
            "half-open": 2,
        }.get(self.state, 0)
        circuit_breaker_state.labels(breaker=self.metric_name).set(
            state_value
        )


circuit_breaker = CircuitBreaker()
cluster_circuit_breakers: dict[str, CircuitBreaker] = {
    "default": circuit_breaker,
}


def _get_consumer_runtime(cluster_name: str) -> dict[str, object]:
    """Return the mutable runtime state for a cluster consumer."""
    if cluster_name == "default":
        return consumer_runtime

    runtime = consumer_runtimes.get(cluster_name)
    if runtime is None:
        runtime = _new_consumer_runtime_state()
        consumer_runtimes[cluster_name] = runtime
    return runtime


def _get_circuit_breaker(cluster_name: str) -> CircuitBreaker:
    """Return the circuit breaker assigned to a cluster consumer."""
    breaker = cluster_circuit_breakers.get(cluster_name)
    if breaker is None:
        breaker = CircuitBreaker(
            metric_name=f"{CONSUMER_METRIC_NAME}:{cluster_name}"
        )
        cluster_circuit_breakers[cluster_name] = breaker
    return breaker


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp_to_age_seconds(value: Optional[str]) -> Optional[float]:
    """Return the age of an ISO 8601 timestamp in seconds."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0)


def _update_runtime_lag(
    runtime: dict[str, object],
    cluster_name: str,
    destination: str,
    partition: int,
    lag_value: int | None,
) -> None:
    """Update current lag snapshots for readiness and metrics."""
    if lag_value is None:
        return

    lag_key = f"{cluster_name}:{destination}:{partition}"
    lag_by_partition = dict(runtime.get("lag_by_partition") or {})
    lag_by_partition[lag_key] = lag_value
    runtime["lag_by_partition"] = lag_by_partition
    runtime["lag_total"] = sum(lag_by_partition.values())
    consumer_lag.labels(
        cluster=cluster_name,
        topic=destination,
        partition=str(partition),
    ).set(lag_value)


def _record_successful_commit(runtime: dict[str, object]) -> None:
    """Record the latest successful Kafka commit time."""
    timestamp = datetime.now(timezone.utc)
    runtime["last_commit_at"] = timestamp.isoformat()
    consumer_last_successful_commit_timestamp_seconds.set(
        timestamp.timestamp()
    )


def _get_canonical_service_name(
    topic: str,
    parsed_service_name: Optional[str],
) -> str:
    """Normalize service names so APIs and TUI filters use one stable value."""
    topic_prefix = topic.split(".", 1)[0] if topic else ""
    if topic_prefix:
        return topic_prefix
    if parsed_service_name:
        return parsed_service_name
    return "unknown"


def get_consumer_health() -> dict[str, object]:
    runtimes = {
        cluster_name: runtime
        for cluster_name, runtime in consumer_runtimes.items()
    }
    cluster_count = len(runtimes)
    connected = cluster_count > 0 and all(
        bool(runtime["connected"]) for runtime in runtimes.values()
    )
    running = cluster_count > 0 and all(
        bool(runtime["running"]) for runtime in runtimes.values()
    )
    lag_by_partition: dict[str, int] = {}
    topics: list[str] = []
    errors: list[str] = []
    last_message_at_values: list[str] = []
    last_commit_at_values: list[str] = []
    breaker_states: list[str] = []
    clusters_payload: dict[str, dict[str, object]] = {}

    for cluster_name, runtime in runtimes.items():
        cluster_topics = list(runtime.get("topics") or [])
        topics.extend(cluster_topics)
        lag_by_partition.update(dict(runtime.get("lag_by_partition") or {}))
        if runtime.get("last_error"):
            errors.append(f"{cluster_name}: {runtime['last_error']}")
        if runtime.get("last_message_at"):
            last_message_at_values.append(str(runtime["last_message_at"]))
        if runtime.get("last_commit_at"):
            last_commit_at_values.append(str(runtime["last_commit_at"]))

        breaker_state = _get_circuit_breaker(cluster_name).state
        breaker_states.append(breaker_state)
        clusters_payload[cluster_name] = {
            "running": runtime["running"],
            "connected": runtime["connected"],
            "broker_kind": runtime.get("broker_kind", "kafka"),
            "topics": cluster_topics,
            "last_error": runtime["last_error"],
            "last_message_at": runtime["last_message_at"],
            "last_commit_at": runtime["last_commit_at"],
            "last_commit_age_seconds": _timestamp_to_age_seconds(
                runtime["last_commit_at"]
            ),
            "lag_by_partition": dict(runtime.get("lag_by_partition") or {}),
            "lag_total": runtime["lag_total"],
            "dlq_messages_total": runtime["dlq_messages_total"],
            "circuit_breaker_state": breaker_state,
        }

    if any(state == "open" for state in breaker_states):
        aggregate_breaker_state = "open"
    elif any(state == "half-open" for state in breaker_states):
        aggregate_breaker_state = "half-open"
    else:
        aggregate_breaker_state = "closed"

    status = "healthy" if running and connected else "unhealthy"
    return {
        "status": status,
        "running": running,
        "connected": connected,
        "topics": sorted(dict.fromkeys(topics)),
        "last_error": "; ".join(errors) if errors else None,
        "last_message_at": (
            max(last_message_at_values)
            if last_message_at_values
            else None
        ),
        "last_commit_at": (
            max(last_commit_at_values)
            if last_commit_at_values
            else None
        ),
        "last_commit_age_seconds": _timestamp_to_age_seconds(
            max(last_commit_at_values) if last_commit_at_values else None
        ),
        "lag_by_partition": lag_by_partition,
        "lag_total": sum(
            int(runtime.get("lag_total") or 0) for runtime in runtimes.values()
        ),
        "dlq_messages_total": max(
            [
                int(runtime.get("dlq_messages_total") or 0)
                for runtime in runtimes.values()
            ]
            or [0]
        ),
        "circuit_breaker_state": aggregate_breaker_state,
        "clusters": clusters_payload,
    }


async def refresh_dead_letter_backlog_metric(
    session_factory: Callable = AsyncSessionLocal,
) -> int:
    """Refresh the pending DLQ gauge from persisted recovery records."""
    async with session_factory() as session:
        result = await session.execute(
            select(func.count())
            .select_from(DeadLetterEvent)
            .where(DeadLetterEvent.is_replayed.is_(False))
        )
        pending_count = int(result.scalar_one() or 0)

    dlq_records_pending.set(pending_count)
    for runtime in consumer_runtimes.values():
        runtime["dlq_messages_total"] = pending_count
    return pending_count


def _checkpoint_rows_for_events(
    events: list[KafkaEvent],
    consumer_group: str = KAFKA_CONSUMER_GROUP,
    broker_kind: str = "kafka",
) -> list[dict[str, object]]:
    """Return highest committed broker positions for a processed batch."""
    checkpoints: dict[tuple[str, str, str], dict[str, object]] = {}
    for event in events:
        location = _build_broker_location(event, broker_kind)
        if location is None:
            continue

        key = (
            str(location["broker_destination"]),
            str(location["broker_substream"]),
            str(location["broker_kind"]),
        )
        existing = checkpoints.get(key)
        if existing is None or int(location["broker_position"]) > int(
            existing["broker_position"]
        ):
            checkpoints[key] = {
                "consumer_group": consumer_group,
                **location,
                "last_event_time": event.event_time,
            }

    return list(checkpoints.values())


def _build_broker_location(
    event: KafkaEvent,
    broker_kind: str,
) -> dict[str, object] | None:
    """Build broker-neutral recovery metadata for one consumed event."""
    if event.kafka_topic is None or event.kafka_offset is None:
        return None

    if broker_kind == "kafka":
        if event.kafka_partition is None:
            return None
        broker_substream = str(event.kafka_partition)
        kafka_topic = event.kafka_topic
        kafka_partition = event.kafka_partition
        kafka_offset = event.kafka_offset
    else:
        broker_substream = ""
        kafka_topic = None
        kafka_partition = None
        kafka_offset = None

    return {
        "broker_kind": broker_kind,
        "broker_destination": event.kafka_topic,
        "broker_substream": broker_substream,
        "broker_position": str(event.kafka_offset),
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }


def _legacy_event_location(
    broker_kind: str,
    broker_destination: str,
    broker_substream: str,
    broker_position: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> tuple[str, int | None, int | None]:
    """Build the legacy event location fields used by the event table."""
    if kafka_topic is not None:
        return kafka_topic, kafka_partition, kafka_offset

    try:
        position = int(broker_position)
    except (TypeError, ValueError):
        position = None

    if broker_kind == "kafka":
        try:
            partition = int(broker_substream)
        except (TypeError, ValueError):
            partition = None
    else:
        partition = 0

    return broker_destination, partition, position


def _serialize_broker_fields(
    *,
    broker_kind: str,
    broker_destination: str,
    broker_substream: str,
    broker_position: str,
    kafka_topic: str | None,
    kafka_partition: int | None,
    kafka_offset: int | None,
) -> dict[str, object]:
    """Render broker metadata alongside Kafka compatibility aliases."""
    return {
        "broker_kind": broker_kind,
        "broker_destination": broker_destination,
        "broker_substream": broker_substream or None,
        "broker_position": broker_position,
        "kafka_topic": kafka_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }


async def persist_consumer_checkpoints(
    events: list[KafkaEvent],
    consumer_group: str = KAFKA_CONSUMER_GROUP,
    broker_kind: str = "kafka",
    session_factory: Callable = AsyncSessionLocal,
) -> None:
    """Persist the latest committed broker positions per stream."""
    checkpoint_rows = _checkpoint_rows_for_events(
        events,
        consumer_group,
        broker_kind,
    )
    if not checkpoint_rows:
        return

    async with session_factory() as session:
        async with session.begin():
            insert_stmt = pg_insert(ConsumerCheckpoint).values(checkpoint_rows)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "consumer_group",
                    "broker_kind",
                    "broker_destination",
                    "broker_substream",
                ],
                set_={
                    "broker_position": insert_stmt.excluded.broker_position,
                    "kafka_topic": insert_stmt.excluded.kafka_topic,
                    "kafka_partition": insert_stmt.excluded.kafka_partition,
                    "kafka_offset": insert_stmt.excluded.kafka_offset,
                    "last_event_time": insert_stmt.excluded.last_event_time,
                    "updated_at": func.now(),
                },
            )
            await session.execute(upsert_stmt)


async def list_consumer_checkpoints_snapshot(
    session_factory: Callable = AsyncSessionLocal,
) -> list[dict[str, object]]:
    """Return the persisted checkpoint snapshot for operators."""
    async with session_factory() as session:
        result = await session.execute(
            select(ConsumerCheckpoint).order_by(
                ConsumerCheckpoint.consumer_group.asc(),
                ConsumerCheckpoint.broker_kind.asc(),
                ConsumerCheckpoint.broker_destination.asc(),
                ConsumerCheckpoint.broker_substream.asc(),
            )
        )
        checkpoints = result.scalars().all()

    return [
        {
            "consumer_group": checkpoint.consumer_group,
            **_serialize_broker_fields(
                broker_kind=checkpoint.broker_kind,
                broker_destination=checkpoint.broker_destination,
                broker_substream=checkpoint.broker_substream,
                broker_position=checkpoint.broker_position,
                kafka_topic=checkpoint.kafka_topic,
                kafka_partition=checkpoint.kafka_partition,
                kafka_offset=checkpoint.kafka_offset,
            ),
            "last_event_time": (
                checkpoint.last_event_time.isoformat()
                if checkpoint.last_event_time
                else None
            ),
            "updated_at": (
                checkpoint.updated_at.isoformat()
                if checkpoint.updated_at
                else None
            ),
        }
        for checkpoint in checkpoints
    ]


async def _persist_dead_letter_event(
    event: KafkaEvent,
    raw_payload: str,
    error: str,
    broker_kind: str = "kafka",
    session_factory: Callable = AsyncSessionLocal,
) -> int:
    """Store the failed message in the persistent DLQ table."""
    location = _build_broker_location(event, broker_kind)
    if location is None:
        raise ValueError("Dead-letter event is missing broker location.")

    async with session_factory() as session:
        async with session.begin():
            insert_stmt = pg_insert(DeadLetterEvent).values(
                service_name=event.service_name,
                **location,
                operation=event.operation,
                raw_payload=raw_payload,
                error_message=error,
                is_replayed=False,
                replayed_at=None,
                replay_error=None,
            )
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "broker_kind",
                    "broker_destination",
                    "broker_substream",
                    "broker_position",
                ],
                set_={
                    "service_name": insert_stmt.excluded.service_name,
                    "kafka_topic": insert_stmt.excluded.kafka_topic,
                    "kafka_partition": insert_stmt.excluded.kafka_partition,
                    "kafka_offset": insert_stmt.excluded.kafka_offset,
                    "operation": insert_stmt.excluded.operation,
                    "raw_payload": insert_stmt.excluded.raw_payload,
                    "error_message": insert_stmt.excluded.error_message,
                    "failed_at": func.now(),
                    "is_replayed": False,
                    "replayed_at": None,
                    "replay_error": None,
                },
            ).returning(DeadLetterEvent.id)
            result = await session.execute(upsert_stmt)
            return int(result.scalar_one())


async def list_dead_letter_events(
    limit: int = 100,
    include_replayed: bool = False,
    session_factory: Callable = AsyncSessionLocal,
) -> list[dict[str, object]]:
    """Return persisted DLQ records for operator inspection."""
    async with session_factory() as session:
        statement = select(DeadLetterEvent)
        if not include_replayed:
            statement = statement.where(DeadLetterEvent.is_replayed.is_(False))
        statement = statement.order_by(DeadLetterEvent.failed_at.desc()).limit(
            limit
        )
        result = await session.execute(statement)
        events = result.scalars().all()

    return [
        {
            "id": event.id,
            "service_name": event.service_name,
            **_serialize_broker_fields(
                broker_kind=event.broker_kind,
                broker_destination=event.broker_destination,
                broker_substream=event.broker_substream,
                broker_position=event.broker_position,
                kafka_topic=event.kafka_topic,
                kafka_partition=event.kafka_partition,
                kafka_offset=event.kafka_offset,
            ),
            "operation": event.operation,
            "error_message": event.error_message,
            "failed_at": (
                event.failed_at.isoformat() if event.failed_at else None
            ),
            "is_replayed": event.is_replayed,
            "replayed_at": (
                event.replayed_at.isoformat() if event.replayed_at else None
            ),
            "replay_error": event.replay_error,
            "raw_payload": event.raw_payload,
        }
        for event in events
    ]


def _build_replay_event(dlq_event: DeadLetterEvent) -> KafkaEvent:
    """Reconstruct a KafkaEvent from a persisted DLQ record."""
    parsed = parse_event_payload(dlq_event.raw_payload)
    operation = extract_operation(parsed.event_data or {})
    destination, partition, offset = _legacy_event_location(
        dlq_event.broker_kind,
        dlq_event.broker_destination,
        dlq_event.broker_substream,
        dlq_event.broker_position,
        dlq_event.kafka_topic,
        dlq_event.kafka_partition,
        dlq_event.kafka_offset,
    )
    return KafkaEvent(
        event_type=parsed.event_type,
        event_time=parsed.event_time,
        user_id=parsed.user_id,
        service_name=_get_canonical_service_name(
            destination,
            dlq_event.service_name,
        ),
        kafka_topic=destination,
        kafka_partition=partition,
        kafka_offset=offset,
        event_data=parsed.event_data,
        raw_payload=parsed.raw_payload,
        operation=operation,
    )


async def replay_dead_letter_event_record(
    dlq_event_id: int,
    session_factory: Callable = AsyncSessionLocal,
) -> dict[str, object]:
    """Replay a persisted DLQ record through the normal ingestion path."""
    with start_span(
        "dlq.replay.record",
        attributes={"db_monitor.dlq_event_id": dlq_event_id},
    ):
        async with session_factory() as session:
            result = await session.execute(
                select(DeadLetterEvent).where(
                    DeadLetterEvent.id == dlq_event_id
                )
            )
            dlq_event = result.scalar_one_or_none()

        if dlq_event is None:
            raise LookupError("DLQ event not found")

        replay_event = _build_replay_event(dlq_event)
        inserted = False
        replay_status = "skipped"

        try:
            if event_pipeline.should_process(replay_event):
                replay_event = event_pipeline.transform(replay_event)
                inserted = await _store_event_graph_with_retries(
                    session_factory,
                    replay_event,
                    max_retries=5,
                )
                replay_status = "replayed" if inserted else "duplicate"
                if inserted:
                    await _broadcast_event(replay_event)

            async with session_factory() as session:
                async with session.begin():
                    result = await session.execute(
                        select(DeadLetterEvent).where(
                            DeadLetterEvent.id == dlq_event_id
                        )
                    )
                    replay_row = result.scalar_one()
                    replay_row.is_replayed = True
                    replay_row.replayed_at = datetime.now(timezone.utc)
                    replay_row.replay_error = None

            consumer_runtime["dlq_messages_total"] = max(
                await refresh_dead_letter_backlog_metric(session_factory),
                0,
            )
            return {
                "dlq_event_id": dlq_event_id,
                "status": replay_status,
                "inserted": inserted,
                "event_id": replay_event.id,
                "broker_kind": dlq_event.broker_kind,
                "broker_destination": dlq_event.broker_destination,
                "broker_substream": (
                    dlq_event.broker_substream or None
                ),
                "broker_position": dlq_event.broker_position,
                "kafka_topic": replay_event.kafka_topic,
                "kafka_partition": replay_event.kafka_partition,
                "kafka_offset": replay_event.kafka_offset,
            }
        except Exception as exc:
            async with session_factory() as session:
                async with session.begin():
                    result = await session.execute(
                        select(DeadLetterEvent).where(
                            DeadLetterEvent.id == dlq_event_id
                        )
                    )
                    replay_row = result.scalar_one_or_none()
                    if replay_row is not None:
                        replay_row.replay_error = str(exc)
            raise


async def replay_dead_letter_event_records(
    limit: int = 100,
    include_replayed: bool = False,
    session_factory: Callable = AsyncSessionLocal,
) -> dict[str, object]:
    """Replay multiple persisted DLQ records through ingestion."""
    async with session_factory() as session:
        statement = select(DeadLetterEvent.id)
        if not include_replayed:
            statement = statement.where(DeadLetterEvent.is_replayed.is_(False))
        statement = statement.order_by(DeadLetterEvent.failed_at.asc()).limit(
            limit
        )
        result = await session.execute(statement)
        dlq_event_ids = list(result.scalars().all())

    results: list[dict[str, object]] = []
    replayed_count = 0
    duplicate_count = 0
    skipped_count = 0
    failed_count = 0

    for dlq_event_id in dlq_event_ids:
        try:
            replay_result = await replay_dead_letter_event_record(
                dlq_event_id,
                session_factory=session_factory,
            )
            results.append(replay_result)

            status = replay_result.get("status")
            if status == "replayed":
                replayed_count += 1
            elif status == "duplicate":
                duplicate_count += 1
            else:
                skipped_count += 1
        except Exception as exc:
            failed_count += 1
            results.append(
                {
                    "dlq_event_id": dlq_event_id,
                    "status": "failed",
                    "error": str(exc),
                }
            )

    return {
        "results": results,
        "count": len(results),
        "requested_limit": limit,
        "include_replayed": include_replayed,
        "replayed_count": replayed_count,
        "duplicate_count": duplicate_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
    }


async def _store_event_graph_with_retries(
    session_factory: Callable,
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
            logger.error("Non-retryable DB error when writing event: %s", exc)
            raise
    logger.error(
        "Exhausted DB write retries (%d). Raising last exception",
        max_retries,
    )
    if last_exc is None:
        raise RuntimeError("DB write retries exhausted")
    raise last_exc


async def _insert_event_record(session, event_obj: KafkaEvent) -> bool:
    """Insert an event once, keyed by Kafka topic/partition/offset."""
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
        )
        .on_conflict_do_nothing(
            index_elements=["kafka_topic", "kafka_partition", "kafka_offset"]
        )
        .returning(KafkaEvent.id)
    )
    result = await session.execute(insert_stmt)
    inserted_id = result.scalar_one_or_none()
    if inserted_id is None:
        return False

    event_obj.id = inserted_id
    return True


async def _populate_row_identity(session, event_obj: KafkaEvent) -> None:
    """Populate row identity on an event before it is inserted."""
    primary_key_columns: list[str] = []
    if event_obj.source_table_id is not None:
        result = await session.execute(
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


async def send_to_dlq(
    dlq_publisher: Optional[DeadLetterPublisher],
    event: KafkaEvent,
    error: str,
    raw_message: bytes | None = None,
    dlq_destination: str = DLQ_TOPIC,
    broker_kind: str = "kafka",
) -> bool:
    """Persist and optionally forward a failed message to the DLQ topic."""
    raw_payload = (
        raw_message.decode("utf-8")
        if raw_message is not None
        else event.raw_payload
    )
    try:
        dlq_record_id = await _persist_dead_letter_event(
            event,
            raw_payload,
            error,
            broker_kind=broker_kind,
        )
    except Exception as exc:
        logger.exception("Failed to persist dead-letter event: %s", exc)
        return False

    if dlq_publisher:
        try:
            await dlq_publisher.publish_dead_letter(
                raw_payload=raw_payload,
                source_destination=event.kafka_topic,
                source_partition=event.kafka_partition,
                source_offset=event.kafka_offset,
                error=error,
                dlq_record_id=dlq_record_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to forward message to DLQ destination %s: %s",
                dlq_destination,
                exc,
            )

    dlq_messages_total.inc()
    await refresh_dead_letter_backlog_metric()
    logger.info(
        (
            "Message persisted to DLQ record %s for destination=%s "
            "partition=%s offset=%s"
        ),
        dlq_record_id,
        event.kafka_topic,
        event.kafka_partition,
        event.kafka_offset,
    )
    return True


async def consumer_task(
    cluster_name: str = "default",
    broker_kind: str = "kafka",
    bootstrap_servers: str | list[str] | None = None,
    consumer_group: str | None = None,
    topics: list[str] | None = None,
    topic_partitions: dict[str, list[int]] | None = None,
    connection_url: str | None = None,
    queue_names: list[str] | None = None,
    prefetch_count: int = 100,
    dlq_destination: str = DLQ_TOPIC,
    enable_dlq: bool = True,
    enable_batch: bool = True,
    batch_size: int = 100,
):
    """Consume messages from the configured broker with resilience features.

    Features:
    - Multi-destination subscription
    - Manual ack/commit
    - Exponential backoff retry
    - Dead-letter queue
    - Circuit breaker
    - Batch processing
    """
    runtime = _get_consumer_runtime(cluster_name)
    breaker = _get_circuit_breaker(cluster_name)
    if topics is None:
        topics = DEFAULT_TOPICS
    if queue_names is None:
        queue_names = []
    bootstrap_servers = bootstrap_servers or KAFKA_BROKER
    consumer_group = consumer_group or KAFKA_CONSUMER_GROUP
    if topic_partitions is not None:
        topics = sorted(topic_partitions.keys())
    if broker_kind == "rabbitmq":
        destinations = list(queue_names)
    else:
        destinations = list(topics)

    runtime.update(
        {
            "running": True,
            "connected": False,
            "topics": destinations,
            "broker_kind": broker_kind,
            "last_error": None,
            "last_message_at": None,
            "last_commit_at": None,
            "lag_by_partition": {},
            "lag_total": 0,
            "dlq_messages_total": 0,
        }
    )
    try:
        await refresh_dead_letter_backlog_metric()
    except Exception as exc:
        logger.warning(
            (
                "Failed to refresh dead-letter backlog metric on "
                "startup for cluster %s: %s"
            ),
            cluster_name,
            exc,
        )

    consumer = build_message_consumer(
        broker_kind=broker_kind,
        bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group,
        topics=topics,
        topic_partitions=topic_partitions,
        connection_url=connection_url,
        queue_names=queue_names,
        prefetch_count=prefetch_count,
    )

    dlq_publisher = None
    if enable_dlq:
        dlq_publisher = build_dead_letter_publisher(
            broker_kind=broker_kind,
            bootstrap_servers=bootstrap_servers,
            connection_url=connection_url,
            dlq_destination=dlq_destination,
        )
        await dlq_publisher.start()

    batch: list[tuple[KafkaEvent, str, BrokerMessage]] = []
    batch_timeout = 5.0
    last_batch_time: float = 0

    connected = False
    retry_count = 0
    while not connected:
        try:
            await consumer.start()
            if topic_partitions is not None:
                logger.info(
                    "Broker consumer '%s' using explicit topic assignments %s",
                    cluster_name,
                    topic_partitions,
                )
            connected = True
            runtime["connected"] = True
            runtime["last_error"] = None
            if broker_kind == "rabbitmq":
                logger.info(
                    "RabbitMQ consumer '%s' started, subscribed to %s",
                    cluster_name,
                    queue_names,
                )
            elif topic_partitions is None:
                logger.info(
                    "Kafka consumer '%s' started, subscribed to %s",
                    cluster_name,
                    topics,
                )
            else:
                logger.info(
                    (
                        "Kafka consumer '%s' started with partition "
                        "assignments %s"
                    ),
                    cluster_name,
                    topic_partitions,
                )
        except Exception as e:
            retry_count += 1
            runtime["last_error"] = str(e)
            if retry_count >= MAX_CONNECTION_RETRIES:
                logger.error(
                    (
                        "Failed to connect to Kafka cluster '%s' after "
                        "%d attempts: %s. "
                        "Exiting consumer."
                    ),
                    cluster_name,
                    MAX_CONNECTION_RETRIES,
                    e,
                )
                if dlq_publisher:
                    await dlq_publisher.stop()
                runtime["running"] = False
                return
            delay = min(
                CONNECTION_RETRY_DELAY * (2 ** min(retry_count - 1, 5)),
                60,
            )
            logger.warning(
                (
                    "Kafka cluster '%s' connection attempt %d/%d failed: %s. "
                    "Retrying in %.1fs..."
                ),
                cluster_name,
                retry_count,
                MAX_CONNECTION_RETRIES,
                e,
                delay,
            )
            await asyncio.sleep(delay)

    try:
        async for msg in consumer:
            if not breaker.can_execute():
                logger.warning(
                    (
                        "Circuit breaker open for cluster '%s', "
                        "skipping message processing"
                    ),
                    cluster_name,
                )
                await asyncio.sleep(1.0)
                continue

            try:
                data = msg.value.decode("utf-8")
                logger.debug(
                    "Received message destination=%s partition=%d offset=%d",
                    msg.destination,
                    msg.partition,
                    msg.offset,
                )

                parsed = parse_event_payload(data)
                operation = extract_operation(parsed.event_data or {})
                runtime["last_message_at"] = _utc_now_iso()
                _update_runtime_lag(
                    runtime,
                    cluster_name,
                    msg.destination,
                    msg.partition,
                    msg.lag,
                )

                event = KafkaEvent(
                    event_type=parsed.event_type,
                    event_time=parsed.event_time,
                    user_id=parsed.user_id,
                    service_name=_get_canonical_service_name(
                        msg.destination,
                        parsed.service_name,
                    ),
                    kafka_topic=msg.destination,
                    kafka_partition=msg.partition,
                    kafka_offset=msg.offset,
                    event_data=parsed.event_data,
                    raw_payload=parsed.raw_payload,
                    operation=operation,
                )

                events_consumed_total.labels(
                    service=event.service_name or "unknown",
                    operation=event.operation or "unknown",
                ).inc()

                with start_span(
                    "kafka.consumer.message",
                    attributes={
                        "db_monitor.cluster": cluster_name,
                        "messaging.system": broker_kind,
                        "messaging.destination.name": msg.destination,
                        "messaging.destination.partition.id": msg.partition,
                        "messaging.message.id": msg.offset,
                    },
                ):
                    if not event_pipeline.should_process(event):
                        logger.debug(
                            "Event %s dropped by pipeline rules.",
                            parsed.event_type,
                        )
                        breaker.record_success()
                        continue

                    event = event_pipeline.transform(event)

                    if enable_batch:
                        if not batch:
                            last_batch_time = asyncio.get_event_loop().time()
                        batch.append((event, data, msg))
                        batch_elapsed = (
                            asyncio.get_event_loop().time() - last_batch_time
                        )
                        if (
                            len(batch) >= batch_size
                            or batch_elapsed >= batch_timeout
                        ):
                            await _process_batch(
                                batch,
                                consumer.ack_batch,
                                dlq_publisher,
                                dlq_destination,
                                enable_dlq,
                                runtime,
                                consumer_group,
                                broker_kind,
                            )
                            batch = []
                    else:
                        await _process_single_event(
                            event,
                            lambda: msg.ack_callback(),
                            dlq_publisher=dlq_publisher,
                            dlq_destination=dlq_destination,
                            enable_dlq=enable_dlq,
                            raw_data=data,
                            runtime=runtime,
                            consumer_group=consumer_group,
                            broker_kind=broker_kind,
                        )

                breaker.record_success()

            except asyncio.CancelledError:
                logger.info("Consumer task received cancellation signal")
                raise
            except Exception as e:
                logger.exception("Error processing message: %s", e)
                breaker.record_failure()

        if batch:
            await _process_batch(
                batch,
                consumer.ack_batch,
                dlq_publisher,
                dlq_destination,
                enable_dlq,
                runtime,
                consumer_group,
                broker_kind,
            )

    except asyncio.CancelledError:
        logger.info("Consumer task received cancellation signal (outer)")
        raise
    finally:
        logger.info("Stopping %s consumer '%s'", broker_kind, cluster_name)
        runtime["running"] = False
        runtime["connected"] = False
        await consumer.stop()
        if dlq_publisher:
            await dlq_publisher.stop()


async def _process_batch(
    batch: list[tuple[KafkaEvent, str, BrokerMessage]],
    ack_callback: Callable[[list[BrokerMessage]], Awaitable[None]],
    dlq_publisher: Optional[DeadLetterPublisher],
    dlq_destination: str,
    enable_dlq: bool,
    runtime: dict[str, object],
    consumer_group: str,
    broker_kind: str,
):
    """Process a batch of events."""
    with start_span(
        "kafka.process.batch",
        attributes={
            "db_monitor.batch_size": len(batch),
            "db_monitor.dlq_enabled": enable_dlq,
        },
    ):
        for event, raw_data, _message in batch:
            try:
                inserted = await _store_event_graph_with_retries(
                    AsyncSessionLocal,
                    event,
                    max_retries=5,
                )
                if not inserted:
                    logger.info(
                        (
                            "Skipping duplicate Kafka event topic=%s "
                            "partition=%s offset=%s"
                        ),
                        event.kafka_topic,
                        event.kafka_partition,
                        event.kafka_offset,
                    )
                    continue

                await _broadcast_event(event)

                events_processed_total.labels(
                    service=event.service_name or "unknown",
                    operation=event.operation or "unknown",
                ).inc()
            except Exception as exc:
                logger.error("Failed to process event after retries: %s", exc)
                events_failed_total.labels(
                    service=event.service_name or "unknown",
                    error_type=type(exc).__name__,
                ).inc()
                if enable_dlq:
                    handled = await send_to_dlq(
                        dlq_publisher,
                        event,
                        str(exc),
                        raw_data.encode(),
                        dlq_destination=dlq_destination,
                        broker_kind=broker_kind,
                    )
                    if not handled:
                        logger.error(
                            (
                                "Skipping commit because DLQ persistence "
                                "failed "
                                "for %s/%s/%s"
                            ),
                            event.kafka_topic,
                            event.kafka_partition,
                            event.kafka_offset,
                        )
                        return
                else:
                    logger.error(
                        "Skipping commit because DLQ is disabled for %s/%s/%s",
                        event.kafka_topic,
                        event.kafka_partition,
                        event.kafka_offset,
                    )
                    return

    try:
        with kafka_commit_duration_seconds.time():
            batch_messages = [
                message for _event, _raw_data, message in batch
            ]
            await ack_callback(batch_messages)
        await persist_consumer_checkpoints(
            [event for event, _raw_data, _message in batch],
            consumer_group=consumer_group,
            broker_kind=broker_kind,
        )
        _record_successful_commit(runtime)
    except Exception as exc:
        logger.exception("Failed to acknowledge broker messages: %s", exc)


def _event_to_ws_payload(event: KafkaEvent) -> dict:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "event_time": (
            event.event_time.isoformat() if event.event_time else None
        ),
        "user_id": event.user_id,
        "service_name": event.service_name,
        "operation": event.operation,
        "source_table_id": event.source_table_id,
        "row_identity": event.row_identity,
        "event_data": event.event_data,
    }


async def _broadcast_event(event: KafkaEvent) -> None:
    payload = {
        "type": "new_event",
        "event": _event_to_ws_payload(event),
    }
    try:
        with start_span(
            "websocket.broadcast.event",
            attributes={
                "db_monitor.event_id": event.id,
                "db_monitor.service": event.service_name,
                "db_monitor.operation": event.operation,
            },
        ):
            await ws_manager.broadcast(payload, event_id=event.id)
    except Exception as e:
        logger.warning("Failed to broadcast event via WebSocket: %s", e)

    try:
        await webhook_notifier.send_message(payload)
    except Exception as exc:
        logger.warning("Failed to deliver event via webhook: %s", exc)


async def _process_single_event(
    event: KafkaEvent,
    ack_target: Callable[[], Awaitable[None]] | object,
    producer: Optional[DeadLetterPublisher] = None,
    dlq_publisher: Optional[DeadLetterPublisher] = None,
    dlq_destination: str = DLQ_TOPIC,
    enable_dlq: bool = True,
    raw_data: str = "",
    runtime: Optional[dict[str, object]] = None,
    consumer_group: str = KAFKA_CONSUMER_GROUP,
    broker_kind: str = "kafka",
):
    """Process a single event."""
    if runtime is None:
        runtime = consumer_runtime

    if dlq_publisher is None:
        dlq_publisher = producer

    if callable(ack_target):
        ack_callback = ack_target
    elif hasattr(ack_target, "commit") and callable(ack_target.commit):
        ack_callback = ack_target.commit
    else:
        raise TypeError("ack_target must be callable or expose commit().")

    try:
        with start_span(
            "kafka.process.event",
            attributes={
                "messaging.destination.name": event.kafka_topic,
                "messaging.kafka.partition": event.kafka_partition,
                "messaging.kafka.offset": event.kafka_offset,
                "db_monitor.service": event.service_name,
                "db_monitor.operation": event.operation,
            },
        ):
            inserted = await _store_event_graph_with_retries(
                AsyncSessionLocal,
                event,
                max_retries=5,
            )
            if not inserted:
                logger.info(
                    (
                        "Skipping duplicate Kafka event topic=%s "
                        "partition=%s offset=%s"
                    ),
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset,
                )
                return

            await _broadcast_event(event)

            events_processed_total.labels(
                service=event.service_name or "unknown",
                operation=event.operation or "unknown",
            ).inc()
    except Exception as exc:
        logger.error("Failed to process event after retries: %s", exc)
        events_failed_total.labels(
            service=event.service_name or "unknown",
            error_type=type(exc).__name__,
        ).inc()
        if enable_dlq:
            handled = await send_to_dlq(
                dlq_publisher,
                event,
                str(exc),
                raw_data.encode(),
                dlq_destination=dlq_destination,
                broker_kind=broker_kind,
            )
            if not handled:
                return
        else:
            return

    try:
        with kafka_commit_duration_seconds.time():
            await ack_callback()
        await persist_consumer_checkpoints(
            [event],
            consumer_group=consumer_group,
            broker_kind=broker_kind,
        )
        _record_successful_commit(runtime)
    except Exception as exc:
        logger.exception("Failed to acknowledge broker message: %s", exc)
