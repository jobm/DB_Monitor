"""Broker recovery metadata migration — add broker-neutral columns."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def _apply_broker_recovery_metadata(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Add broker-neutral recovery metadata for checkpoints and DLQ rows."""
    del session_factory
    statements = [
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_kind VARCHAR(32)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_destination VARCHAR(256)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_substream VARCHAR(128)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ADD COLUMN IF NOT EXISTS broker_position VARCHAR(128)"
        ),
        (
            "UPDATE consumer_checkpoints "
            "SET broker_kind = COALESCE(broker_kind, 'kafka'), "
            "broker_destination = COALESCE(broker_destination, kafka_topic), "
            "broker_substream = COALESCE("
            "broker_substream, kafka_partition::TEXT, ''), "
            "broker_position = COALESCE("
            "broker_position, kafka_offset::TEXT)"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_kind SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_destination SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_substream SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN broker_position SET NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN kafka_topic DROP NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN kafka_partition DROP NOT NULL"
        ),
        (
            "ALTER TABLE consumer_checkpoints "
            "ALTER COLUMN kafka_offset DROP NOT NULL"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_consumer_checkpoints_group_broker_stream "
            "ON consumer_checkpoints "
            "(consumer_group, broker_kind, broker_destination, "
            "broker_substream)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_kind VARCHAR(32)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_destination VARCHAR(256)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_substream VARCHAR(128)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ADD COLUMN IF NOT EXISTS broker_position VARCHAR(128)"
        ),
        (
            "UPDATE dead_letter_events "
            "SET broker_kind = COALESCE(broker_kind, 'kafka'), "
            "broker_destination = COALESCE(broker_destination, kafka_topic), "
            "broker_substream = COALESCE("
            "broker_substream, kafka_partition::TEXT, ''), "
            "broker_position = COALESCE("
            "broker_position, kafka_offset::TEXT)"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_kind SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_destination SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_substream SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN broker_position SET NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN kafka_topic DROP NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN kafka_partition DROP NOT NULL"
        ),
        (
            "ALTER TABLE dead_letter_events "
            "ALTER COLUMN kafka_offset DROP NOT NULL"
        ),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_dead_letter_events_broker_position "
            "ON dead_letter_events "
            "(broker_kind, broker_destination, broker_substream, "
            "broker_position)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))
